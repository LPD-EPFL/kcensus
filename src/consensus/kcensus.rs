use crate::consensus::kcensus::message::KCensusMsg;
use crate::consensus::kcensus::message::KCensusMsg::{Spread, SpreadValueOnly};
use crate::consensus::kcensus::propagation::{MessageId, PropagationGraphs};
use crate::consensus::kcensus::round_state::KCensusRoundState;
use crate::consensus::message::ConsensusMsg::{Commit, KCensusM};
use crate::consensus::message::{CommandBatch, ConsensusMessage};
use crate::consensus::read_tracker::ReadTracker;
use crate::consensus::{Consensus, ConsensusShard, ConsensusShardTrait};
use crate::multi_sink::{MultiSink, ShardMultiSink};
use log::{debug, trace};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::rc::Rc;

pub(crate) mod message;
mod node_state;
pub mod propagation;
mod round_state;

pub struct KCensusSettings {
    propagation_graphs: Rc<PropagationGraphs>,
}

pub(crate) type KCensusShard = ConsensusShard<KCensusSettings, usize, KCensusRoundState>;

impl KCensusShard {
    pub fn new(
        nb_nodes: usize,
        my_pid: usize,
        sinks: ShardMultiSink,
        leader_priority: Vec<usize>,
        propagation_graphs: Rc<PropagationGraphs>,
    ) -> Self {
        assert!(my_pid < nb_nodes);
        Self {
            nb_nodes,
            my_pid,
            leader_priority,

            sinks,

            next_uid: my_pid,
            slot: 0,
            queued_commands: HashMap::with_capacity(nb_nodes),

            read_tracker: ReadTracker::new(1 + nb_nodes / 2),

            settings: KCensusSettings { propagation_graphs },
            round: 0,
            round_state: KCensusRoundState::new(nb_nodes, my_pid),
        }
    }
}

impl ConsensusShardTrait for KCensusShard {
    async fn process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<CommandBatch>> {
        let msg_v = msg.get_v();
        let src = msg.src;
        let msg = match msg.msg {
            KCensusM(msg) => msg,
            x => panic!("Unexpected message type: {:?}", x),
        };

        // TODO: Ignore some messages if max_seen_slot > slot ?
        // TODO: Handle dead nodes / packet loss ?
        match msg {
            Spread {
                slot,
                round,
                msg_id,
                remote_states,
                with_value,
                new_value,
            } => {
                let msg_v = msg_v.expect("Spread messages should have a value uid");
                if slot < self.slot || round < self.round {
                    if with_value {
                        let msg_id = msg_id.expect("Can't spread value without msg_id");
                        self.graph_spread_value_only(msg_id, msg_v).await?;
                    }
                    return Ok(None);
                } else if round > self.round {
                    self.goto_round(round);
                }
                debug_assert_eq!(slot, self.slot);
                debug_assert_eq!(round, self.round);
                let old_proposer_count = self.round_state.proposers().len();

                let no_v_before = self.round_state.get_my_v().is_none();
                if no_v_before {
                    debug_assert!(old_proposer_count == 0);
                    debug_assert!(!self.round_state.am_i_frozen());
                    // TODO: pick most popular v instead ?
                    self.round_state.set_my_v(msg_v);
                }
                let my_v = self.round_state.get_my_v().unwrap();

                let learned = self.round_state.learn_from(&remote_states);
                let proposer_count = self.round_state.proposers().len();

                if let Some(msg_id) = msg_id {
                    self.round_state.receive_msg(msg_id);
                }

                // Can commit ?
                // TODO: Make can_commit faster when using graph
                if self.round_state.i_am_proposer() && self.round_state.can_commit() {
                    debug_assert!(!with_value); // can't be my value -> there would be a conflict
                    let v = self.round_state.get_my_v().unwrap();
                    self.sinks.broadcast(Commit { slot, v }, None).await?;
                    let value = self.commit_slot(my_v, false);
                    return Ok(Some(value));
                }

                let msg_frozen = remote_states[src].frozen;
                let orig_frozen = self.round_state.am_i_frozen();

                if msg_frozen || msg_v != my_v || orig_frozen {
                    self.round_state.freeze();

                    if with_value {
                        debug_assert!(!msg_frozen); // Can't spread value in frozen messages.
                        let msg_id = msg_id.expect("Can't spread value without msg_id");
                        self.graph_spread_value_only(msg_id, msg_v).await?;
                    }

                    if self.round_state.i_am_proposer() {
                        let min_proposer = *self
                            .leader_priority
                            .iter()
                            .find(|leader| self.round_state.proposers().contains(leader))
                            .unwrap();
                        if min_proposer == self.my_pid {
                            if let Some(adopted_v) = self.round_state.try_adopt() {
                                // Conflict resolved. Adopting...
                                self.goto_round(self.round + 1);
                                self.repropose_start(adopted_v).await?;
                                return Ok(None);
                            }
                        }
                    }

                    if !orig_frozen {
                        if msg_frozen {
                            // Existing conflict. Spreading to proposers.
                            for i in 0..proposer_count {
                                self.spread_to(self.round_state.proposers()[i]).await?;
                            }
                        } else {
                            // New conflict. Freezing others...
                            self.spread_to_all().await?;
                        }
                    } else {
                        // Spread to new proposers only
                        for i in old_proposer_count..proposer_count {
                            self.spread_to(self.round_state.proposers()[i]).await?;
                        }
                    }
                    return Ok(None);
                }

                if let Some(msg_id) = msg_id {
                    if proposer_count == 1 {
                        self.graph_spread(msg_id, new_value).await?;
                        return Ok(None);
                    } else if with_value {
                        self.graph_spread_value_only(msg_id, msg_v).await?;
                    }
                }

                // Multiple proposers of the same value
                debug_assert!(proposer_count > 1);

                if old_proposer_count < 2 {
                    // Transition to multi-proposer strategy
                    self.spread_to_all().await?;
                } else if learned || old_proposer_count < proposer_count {
                    let start_from = if learned { 0 } else { old_proposer_count };
                    // Share knowledge with proposers
                    for i in start_from..proposer_count {
                        let proposer = self.round_state.proposers()[i];
                        self.spread_to(proposer).await?;
                    }
                }
            }
            SpreadValueOnly { msg_id, v } => self.graph_spread_value_only(msg_id, v).await?,
        } // match command
        Ok(None)
    } // fn process_message

    #[inline]
    fn can_forward_proposals(&self) -> bool {
        true
    }

    #[inline]
    async fn propose_start(&mut self, value: CommandBatch, contention: bool) -> io::Result<()> {
        let uid = self.store_new_command(value);
        if contention {
            self.graph_spread_new_value_only(uid).await
        } else {
            self.propose_and_spread(uid, true).await
        }
    }

    #[inline]
    async fn repropose_start(&mut self, v: usize) -> io::Result<()> {
        self.propose_and_spread(v, false).await
    }

    #[inline]
    fn commit_slot(&mut self, v: usize, from_commit_msg: bool) -> CommandBatch {
        let value = self.remove_command(v);
        if from_commit_msg {
            // "<#2FB82F>Commited \"{}\" in slot {}.</>"
            trace!("Commited \"{:?}\" in slot {}.", value, self.slot);
        } else {
            // "<#2FB82F>Commited \"{}\" in slot {} (round {}) from state:</> <#B8E8B8>{}</>"
            trace!(
                "Commited \"{:?}\" in slot {} (round {}) from state: {}",
                value, self.slot, self.round, self.round_state,
            );
        }
        self.slot += 1;
        self.goto_round(0);
        value
    }

    #[inline]
    fn get_my_v(&self) -> Option<usize> {
        self.round_state.get_my_v()
    }

    #[inline]
    fn should_lead(&self) -> bool {
        self.my_pid == self.leader_priority[0]
    }
}

impl KCensusShard {
    #[inline]
    fn goto_round(&mut self, round: usize) {
        if round != 0 {
            debug!(
                // "<#FF4F4F>Can not commit in round {} from state:</> <#EFBFBF>{}</>"
                "Can not commit in round {} from state: {}",
                self.round, self.round_state,
            );
            if round > self.round + 1 {
                // "<yellow>######## Skipping round !!!!</>"
                debug!("######## Skipping round !!!!");
            }
        }
        self.round = round;
        self.round_state.clear();
    }

    #[inline]
    fn value_for_msg(&self, msg: &KCensusMsg) -> Option<CommandBatch> {
        if msg.includes_value() {
            let v = msg.get_v(self.my_pid);
            Some(self.queued_commands[&v].clone())
        } else {
            None
        }
    }

    #[inline]
    async fn broadcast(&self, msg: KCensusMsg) -> io::Result<()> {
        let value = self.value_for_msg(&msg);
        self.sinks.broadcast(KCensusM(msg), value).await
    }

    async fn send_to(&self, msg: KCensusMsg, dest: usize) -> io::Result<()> {
        let value = self.value_for_msg(&msg);
        self.sinks.send(KCensusM(msg), value, dest).await
    }

    async fn spread_to(&self, dest: usize) -> io::Result<()> {
        if self.my_pid == dest {
            return Ok(());
        }
        let msg = Spread {
            slot: self.slot,
            round: self.round,
            msg_id: None,
            remote_states: self.round_state.clone_node_states(),
            with_value: false,
            new_value: false,
        };
        self.send_to(msg, dest).await
    }

    async fn spread_to_all(&self) -> io::Result<()> {
        let msg = Spread {
            slot: self.slot,
            round: self.round,
            msg_id: None,
            remote_states: self.round_state.clone_node_states(),
            with_value: false,
            new_value: false,
        };
        self.broadcast(msg).await
    }

    async fn propose_and_spread(&mut self, v: usize, new_value: bool) -> io::Result<()> {
        self.round_state.set_my_v(v);
        self.round_state.become_proposer();

        for msg_id in self
            .settings
            .propagation_graphs
            .get_start(self.my_pid)
            .iter()
        {
            debug_assert!(
                self.settings
                    .propagation_graphs
                    .get_by_id(msg_id)
                    .get_includes_new_values()
            );
            debug_assert!(
                self.settings
                    .propagation_graphs
                    .get_by_id(msg_id)
                    .get_dependencies()
                    .is_empty()
            );

            let msg = Spread {
                slot: self.slot,
                round: self.round,
                msg_id: Some(*msg_id),
                remote_states: self.round_state.clone_node_states(),
                with_value: new_value,
                new_value,
            };
            self.send_to(msg, msg_id.dest).await?
        }
        Ok(())
    }

    async fn graph_spread(&mut self, prev_msg_id: MessageId, new_value: bool) -> io::Result<()> {
        let prev_msg_info = self.settings.propagation_graphs.get_by_id(&prev_msg_id);
        // Potential follow-up messages:
        for msg_id in prev_msg_info.get_needed_by().iter() {
            if msg_id.src != self.my_pid {
                continue;
            }
            let msg_info = self.settings.propagation_graphs.get_by_id(msg_id);

            if !self.round_state.can_send(msg_info.get_dependencies()) {
                continue;
            }

            self.round_state.receive_msg(*msg_id);

            let msg = Spread {
                slot: self.slot,
                round: self.round,
                msg_id: Some(*msg_id),
                remote_states: self.round_state.clone_node_states(),
                with_value: new_value && msg_info.get_includes_new_values(),
                new_value,
            };
            self.send_to(msg, msg_id.dest).await?;
        }
        Ok(())
    }

    async fn graph_spread_value_only(&self, prev_msg_id: MessageId, v: usize) -> io::Result<()> {
        let prev_msg_info = self.settings.propagation_graphs.get_by_id(&prev_msg_id);
        // Potential follow-up messages:
        for msg_id in prev_msg_info.get_needed_by() {
            if msg_id.src != self.my_pid {
                continue;
            }
            let msg_info = self.settings.propagation_graphs.get_by_id(msg_id);

            if !msg_info.get_includes_new_values() {
                continue;
            }
            debug_assert!(msg_info.get_dependencies().len() == 1);
            debug_assert!(msg_info.get_dependencies().contains(&prev_msg_id));

            let msg = SpreadValueOnly { msg_id: *msg_id, v };
            self.send_to(msg, msg_id.dest).await?;
        }
        Ok(())
    }

    async fn graph_spread_new_value_only(&self, v: usize) -> io::Result<()> {
        for msg_id in self.settings.propagation_graphs.get_start(self.my_pid) {
            debug_assert_eq!(msg_id.src, self.my_pid);
            let msg_info = self.settings.propagation_graphs.get_by_id(msg_id);

            debug_assert!(msg_info.get_includes_new_values());
            debug_assert!(msg_info.get_dependencies().is_empty());

            let msg = SpreadValueOnly { msg_id: *msg_id, v };
            self.send_to(msg, msg_id.dest).await?;
        }
        Ok(())
    }
}

pub(crate) type KCensus = Consensus<KCensusSettings, usize, KCensusRoundState>;

impl KCensus {
    pub fn new(
        nb_nodes: usize,
        my_pid: usize,
        sinks: MultiSink,
        leader_priority: Vec<usize>,
        propagation_graphs: PropagationGraphs,
        shard_count: usize,
    ) -> Self {
        let sinks = Rc::new(RefCell::new(sinks));
        let propagation_graphs = Rc::new(propagation_graphs);
        Self {
            nb_nodes,
            shards: (0..shard_count)
                .map(|shard_id| {
                    KCensusShard::new(
                        nb_nodes,
                        my_pid,
                        ShardMultiSink {
                            shard_id,
                            multi_sink: sinks.clone(),
                        },
                        leader_priority.clone(),
                        propagation_graphs.clone(),
                    )
                })
                .collect(),
            sinks,
        }
    }
}
