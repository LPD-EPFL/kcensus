use crate::connector::DeSink;
use crate::consensus::kcensus::message::KCensusMsg;
use crate::consensus::kcensus::message::KCensusMsg::{Spread, SpreadValueOnly};
use crate::consensus::kcensus::propagation::{MessageId, PropagationGraphs};
use crate::consensus::kcensus::round_state::KCensusRoundState;
use crate::consensus::message::ConsensusMessage;
use crate::consensus::message::ConsensusMsg::{Commit, KCensusM};
use crate::consensus::Consensus;
use crate::message::Message::Done;
use crate::multi_sink::MultiSink;
use crate::value::{KVal, Request};
use log::debug;
use std::collections::HashMap;
use std::io;

pub mod message;
pub mod node_state;
pub mod propagation;
mod round_state;

pub struct KCensus<Sk> {
    // Settings
    nb_nodes: usize,
    my_pid: usize,
    leader_priority: Vec<usize>,

    // Connections
    sinks: MultiSink<Sk>,

    // Propagation graphs
    propagation_graphs: PropagationGraphs,

    // Overall state
    slot: usize,
    round: usize,
    requests: HashMap<usize, Request>,

    round_state: KCensusRoundState,
}

macro_rules! send_msg {
    ($self:ident, $msg:expr, $dest:expr) => {{
        let value = $self.value_for_msg(&$msg);
        $self.sinks.send(KCensusM($msg), value, $dest).await
    }};
}

impl KCensus<DeSink> {
    pub fn new(
        nb_nodes: usize,
        my_pid: usize,
        sinks: MultiSink<DeSink>,
        propagation_graphs: PropagationGraphs,
        leader_priority: Vec<usize>,
    ) -> Self {
        assert!(my_pid < nb_nodes);
        Self {
            nb_nodes,
            my_pid,
            leader_priority,

            sinks,

            propagation_graphs,

            slot: 0,
            round: 0,
            requests: HashMap::with_capacity(nb_nodes),

            round_state: KCensusRoundState::new(nb_nodes, my_pid),
        }
    }
}

impl Consensus for KCensus<DeSink> {
    async fn process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<Request>> {
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
            } => {
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

                if msg_frozen || msg_v != my_v {
                    let orig_frozen = self.round_state.am_i_frozen();
                    self.round_state.freeze();

                    if with_value {
                        debug_assert!(!msg_frozen); // Can't spread value in frozen messages.
                        let msg_id = msg_id.expect("Can't spread value without msg_id");
                        self.graph_spread_value_only(msg_id, msg_v).await?;
                    }

                    // TODO: only try to adopt if I'm the proposer with lowest id ?
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
                                self.round_state.set_my_v(adopted_v); // Needed if we don't repropose
                                // TODO: only repropose if I was the proposer ?
                                //   (potentially need to broadcast adopt in that case ?)
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

                if proposer_count == 1 {
                    let msg_id = msg_id.expect("Single proposer means messages should have ids");
                    self.graph_spread(msg_id, with_value).await?;
                    return Ok(None);
                }

                // Multiple proposers of the same value
                debug_assert!(proposer_count > 1);
                debug_assert!(!with_value);

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
    async fn propose_start(&mut self, req: Request) -> io::Result<()> {
        let v = self.store_new_request(req);

        self.propose_and_spread(v, true).await
    }

    #[inline]
    async fn repropose_start(&mut self, v: usize) -> io::Result<()> {
        self.propose_and_spread(v, false).await
    }

    #[inline]
    fn commit_slot(&mut self, v: usize, from_commit_msg: bool) -> Request {
        let value = self.requests.remove(&v).unwrap();
        if from_commit_msg {
            // "<#2FB82F>Commited \"{}\" in slot {}.</>"
            debug!("Commited \"{:?}\" in slot {}.", value.value.val, self.slot);
        } else {
            // "<#2FB82F>Commited \"{}\" in slot {} (round {}) from state:</> <#B8E8B8>{}</>"
            debug!(
                "Commited \"{:?}\" in slot {} (round {}) from state: {}",
                value.value.val, self.slot, self.round, self.round_state,
            );
        }
        self.slot += 1;
        self.goto_round(0);
        value
    }

    #[inline]
    fn get_nb_nodes(&self) -> usize {
        self.nb_nodes
    }

    #[inline]
    fn get_slot(&self) -> usize {
        self.slot
    }

    #[inline]
    fn get_my_v(&self) -> Option<usize> {
        self.round_state.get_my_v()
    }

    #[inline]
    fn should_lead(&self) -> bool {
        self.my_pid == self.leader_priority[0]
    }

    #[inline]
    async fn announce_done(&mut self) -> io::Result<()> {
        self.sinks.inner_broadcast(Done).await
    }

    #[inline]
    fn store_new_request(&mut self, req: Request) -> usize {
        let v = self.my_pid + (self.slot * self.nb_nodes);
        let inserted = self.requests.insert(v, req);
        debug_assert!(inserted.is_none());
        v
    }

    #[inline]
    fn store_remote_request(&mut self, v: usize, value: KVal) {
        // TODO: Allow forwarding values ? (could the value already be there ?)
        let inserted = self.requests.insert(v, value.into_remote_req());
        debug_assert!(inserted.is_none());
    }

    #[inline]
    fn knows_v(&self, v: usize) -> bool {
        self.requests.contains_key(&v)
    }

    #[inline]
    fn has_queued_requests(&self) -> bool {
        !self.requests.is_empty()
    }

    #[inline]
    fn get_new_batch_to_propose(&self) -> Option<KVal> {
        // TODO: Actually form batch here !
        None
    }

    #[inline]
    fn get_v_to_repropose(&self) -> usize {
        *self.requests.keys().min().unwrap()
    }
}

impl KCensus<DeSink> {
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
    fn value_for_msg(&self, msg: &KCensusMsg) -> Option<KVal> {
        if msg.includes_value() {
            let v = msg.get_v(self.my_pid);
            Some(self.requests[&v].value.clone())
        } else {
            None
        }
    }

    #[inline]
    async fn broadcast(&mut self, msg: KCensusMsg) -> io::Result<()> {
        let value = self.value_for_msg(&msg);
        self.sinks.broadcast(KCensusM(msg), value).await
    }

    async fn spread_to(&mut self, dest: usize) -> io::Result<()> {
        let msg = Spread {
            slot: self.slot,
            round: self.round,
            msg_id: None,
            remote_states: self.round_state.clone_node_states(),
            with_value: false,
        };
        send_msg!(self, msg, dest)
    }

    async fn spread_to_all(&mut self) -> io::Result<()> {
        let msg = Spread {
            slot: self.slot,
            round: self.round,
            msg_id: None,
            remote_states: self.round_state.clone_node_states(),
            with_value: false,
        };
        self.broadcast(msg).await
    }

    async fn propose_and_spread(&mut self, v: usize, with_value: bool) -> io::Result<()> {
        self.round_state.set_my_v(v);
        self.round_state.become_proposer();

        for msg_id in self.propagation_graphs.get_start(self.my_pid).iter() {
            debug_assert!(
                self.propagation_graphs
                    .get_by_id(msg_id)
                    .initial_spreading_tree()
            );
            debug_assert!(
                self.propagation_graphs
                    .get_by_id(msg_id)
                    .get_dependencies()
                    .is_empty()
            );

            let msg = Spread {
                slot: self.slot,
                round: self.round,
                msg_id: Some(*msg_id),
                remote_states: self.round_state.clone_node_states(),
                with_value,
            };
            let dest = msg_id.dest;
            send_msg!(self, msg, dest)?;
        }
        Ok(())
    }

    async fn graph_spread(&mut self, prev_msg_id: MessageId, with_value: bool) -> io::Result<()> {
        let prev_msg_info = self.propagation_graphs.get_by_id(&prev_msg_id);
        // Potential follow-up messages:
        for msg_id in prev_msg_info.get_needed_by().iter() {
            if msg_id.src != self.my_pid {
                continue;
            }
            let msg_info = self.propagation_graphs.get_by_id(msg_id);

            if !self.round_state.can_send(msg_info.get_dependencies()) {
                debug_assert!(!msg_info.initial_spreading_tree());
                continue;
            }

            self.round_state.receive_msg(*msg_id);

            let msg = Spread {
                slot: self.slot,
                round: self.round,
                msg_id: Some(*msg_id),
                remote_states: self.round_state.clone_node_states(),
                with_value: with_value && msg_info.initial_spreading_tree(),
            };
            send_msg!(self, msg, msg_id.dest)?;
        }
        Ok(())
    }

    async fn graph_spread_value_only(
        &mut self,
        prev_msg_id: MessageId,
        v: usize,
    ) -> io::Result<()> {
        let prev_msg_info = self.propagation_graphs.get_by_id(&prev_msg_id);
        // Potential follow-up messages:
        for msg_id in prev_msg_info.get_needed_by() {
            if msg_id.src != self.my_pid {
                continue;
            }
            let msg_info = self.propagation_graphs.get_by_id(msg_id);

            self.round_state.receive_msg(*msg_id);

            if !msg_info.initial_spreading_tree() {
                continue;
            }
            debug_assert!(self.round_state.can_send(msg_info.get_dependencies()));

            let msg = SpreadValueOnly { msg_id: *msg_id, v };
            send_msg!(self, msg, msg_id.dest)?;
        }
        Ok(())
    }
}
