use crate::consensus::kcensus::message::KCensusMsg;
use crate::consensus::kcensus::message::KCensusMsg::{PaxosAccept, Spread, SpreadValueOnly};
use crate::consensus::kcensus::node_state::NodeState;
use crate::consensus::kcensus::propagation::{MessageId, PropagationGraphs};
use crate::consensus::kcensus::round_state::KCensusRoundState;
use crate::consensus::message::ConsensusMsg::{Commit, KCensusM};
use crate::consensus::message::{CommandBatch, ConsensusMessage};
use crate::consensus::read_tracker::ReadTracker;
use crate::consensus::{Consensus, ConsensusShard, ConsensusShardTrait};
use crate::multi_sink::{MultiSink, ShardMultiSink};
use bit_set::BitSet;
use log::{debug, info, trace};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub(crate) mod message;
mod node_state;
pub mod propagation;
mod round_state;

pub struct KCensusSettings {
    graphs: Arc<PropagationGraphs>,
}

pub(crate) type KCensusShard = ConsensusShard<KCensusSettings, KCensusRoundState>;

impl KCensusShard {
    pub fn new(
        process_count: usize,
        replica_count: usize,
        alive_replicas: BitSet,
        my_pid: usize,
        sinks: ShardMultiSink,
        leader_priority: Vec<usize>,
        propagation_graphs: Arc<PropagationGraphs>,
    ) -> Self {
        assert!(my_pid < process_count);
        let majority = 1 + (replica_count / 2);
        let replica = alive_replicas.contains(my_pid);
        Self {
            process_count,
            alive_replicas,
            replica,
            leader_priority,
            my_pid,

            sinks,

            next_uid: my_pid,
            slot: 0,
            queued_commands: HashMap::with_capacity(process_count),
            last_v: None,

            read_tracker: ReadTracker::new(majority),

            settings: KCensusSettings {
                graphs: propagation_graphs,
            },
            round_state: KCensusRoundState::new(process_count, majority, my_pid),
        }
    }
}

impl Debug for KCensusShard {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "slot={}, last_v={:?}, prepared={:?}, my_v={:?}, accepted_at={:?}, could_adopt={}, can_start_accept={}, leaders={:?}, proposers={:?}, queued_commands={:?}",
            self.slot,
            self.last_v,
            self.round_state.prepared_for(),
            self.get_my_v(),
            self.round_state.get_paxos_accept_round(),
            self.round_state.could_adopt(&self.alive_replicas),
            self.round_state
                .can_start_paxos_accept(&self.alive_replicas),
            self.round_state.leaders(),
            self.round_state.proposers(),
            self.queued_commands,
        )
    }
}

impl ConsensusShardTrait for KCensusShard {
    async fn process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<CommandBatch>> {
        let src = msg.src;
        let msg = match msg.msg {
            KCensusM(msg) => msg,
            x => panic!("Unexpected message type: {x:?}"),
        };

        if !matches!(msg, SpreadValueOnly { .. }) {
            debug!(
                "Processing kcensus msg from {src} (shard={}): {msg:?}",
                self.sinks.shard_id
            );
        } else {
            trace!(
                "Processing kcensus msg from {src} (shard={}): {msg:?}",
                self.sinks.shard_id
            );
        }

        // TODO: Ignore some messages if max_seen_slot > slot ?
        // TODO: Handle dead nodes / packet loss ?
        match msg {
            Spread {
                slot,
                v,
                msg_id,
                remote_states,
                with_value,
                new_value,
            } => {
                if slot < self.slot {
                    // TODO: maybe also enter here if paxos-accept ? (only if higher-prio leader ?)
                    if with_value {
                        assert!(new_value);
                        self.graph_spread_value_only(msg_id, v).await?;
                    }
                    return Ok(None);
                }

                debug_assert_eq!(slot, self.slot);
                let old_leader_count = self.round_state.leaders().len();
                let no_v_before = self.round_state.get_my_v().is_none();
                // let had_conflict = self.round_state.has_conflict();
                let proposer = msg_id.proposer;
                let leader = self.settings.graphs.get_leader(proposer);
                let explicit_remote_states = remote_states.is_some();
                let remote_states = match remote_states {
                    Some(x) => x,
                    None => {
                        let remote_state_ids = self.settings.graphs.get_remote_states(msg_id);
                        let mut remote_states: Vec<_> =
                            (0..self.process_count).map(NodeState::new).collect();
                        for i in 0..self.process_count {
                            if remote_state_ids[i] != Duration::ZERO || i == msg_id.proposer {
                                remote_states[i].accept_with_state(
                                    v,
                                    msg_id.proposer,
                                    remote_state_ids[i],
                                    &self.settings.graphs,
                                )
                            }
                        }
                        remote_states
                    }
                };
                assert!(
                    remote_states[proposer].get_v() == Some(v)
                        || remote_states[proposer].get_paxos_accept_round().is_some()
                );
                let _remote_change = self
                    .round_state
                    .store_remote_states(&remote_states, &self.settings.graphs);
                let conflict = self.round_state.has_conflict();
                assert!(!explicit_remote_states || conflict);

                if no_v_before && !conflict {
                    assert_eq!(old_leader_count, 0);
                    assert!(!self.round_state.am_i_frozen());
                    self.round_state.accept_with_state(
                        v,
                        proposer,
                        Duration::ZERO,
                        &self.settings.graphs,
                    );
                }
                let my_v = self.round_state.get_my_v();

                assert!(!self.round_state.am_i_frozen() || conflict);
                if conflict {
                    self.round_state.freeze_and_prepare_leaders();
                }

                // Update propagation state (/v state) and continue propagation
                self.round_state.receive_msg(msg_id);
                let final_state = loop {
                    if let Some((next_state, dependencies)) = self.settings.graphs.next_state(
                        proposer,
                        self.my_pid,
                        self.round_state.get_propagation_state(proposer),
                    ) {
                        if !self.round_state.has_received(dependencies) {
                            break false;
                        }
                        self.round_state.set_propagation_state(proposer, next_state);
                        if !conflict {
                            self.round_state
                                .update_v_state(next_state, &self.settings.graphs)
                        }
                        self.spread(v, proposer, next_state, new_value).await?;
                    } else {
                        break true;
                    }
                };

                if final_state && leader == self.my_pid && !conflict {
                    assert!(self.round_state.can_commit(&self.settings.graphs));
                    let v = my_v.unwrap();
                    if let Some(requester) = self.get_requester(v) {
                        if requester != self.my_pid {
                            self.sinks
                                .send(Commit { slot, v }, None, requester, self.last_v)
                                .await?;
                        }
                        for i in 0..self.process_count {
                            if i == self.my_pid || i == requester {
                                continue;
                            }
                            self.sinks
                                .send(Commit { slot, v }, None, i, self.last_v)
                                .await?;
                        }
                    } else {
                        self.sinks
                            .broadcast(Commit { slot, v }, None, self.last_v)
                            .await?;
                    }
                    info!(
                        "Commit via kcensus: shard={} slot={slot} v={v}",
                        self.sinks.shard_id
                    );
                    let value = self.commit_slot(v, false);
                    return Ok(Some(value));
                }
                if self.round_state.prepared_for() == Some(self.my_pid) {
                    assert!(conflict);
                    assert!(self.replica);
                    if self
                        .round_state
                        .can_start_paxos_accept(&self.alive_replicas)
                    {
                        let adopted_v = self.round_state.adopt(&self.settings.graphs);
                        let (new_value, adopted_v) = match adopted_v {
                            Some(adopted_v) => (false, adopted_v),
                            None => match self.get_new_batch_to_propose() {
                                Some(batch) => (true, self.store_new_command(batch)),
                                None => (false, v),
                            },
                        };
                        self.broadcast_paxos_accept(adopted_v, new_value).await?;
                        self.round_state.paxos_accept(self.my_pid, adopted_v);
                    } else {
                        debug!(
                            "Leader of conflicting state, but not ready to accept. prepared_for: {:?}, last_accepted: {:?}, state: {:?}",
                            self.round_state.prepared_for(),
                            self.round_state.get_paxos_accept_round(),
                            self.round_state.get_node_states(),
                        )
                    }
                }
            }
            SpreadValueOnly { msg_id, v } => self.graph_spread_value_only(msg_id, v).await?,
            PaxosAccept {
                slot, leader, v, ..
            } => {
                if slot < self.slot || !self.replica {
                    return Ok(None);
                }

                self.round_state.freeze_and_prepare(leader);
                if self.round_state.prepared_for() != Some(leader) {
                    return Ok(None);
                }

                if leader != self.my_pid {
                    assert_eq!(leader, src);
                    self.round_state.paxos_accept(leader, v);
                    self.send_to(
                        PaxosAccept {
                            slot,
                            leader,
                            v,
                            new_value: false,
                        },
                        leader,
                    )
                    .await?;
                } else {
                    assert_eq!(v, self.get_my_v().unwrap());
                    self.round_state.recv_paxos_accept(src, v);
                    if self.round_state.can_paxos_commit() {
                        self.sinks
                            .broadcast(Commit { slot, v }, None, self.last_v)
                            .await?;
                        info!(
                            "Commit via paxos: shard={} slot={slot} v={v}",
                            self.sinks.shard_id
                        );
                        let value = self.commit_slot(v, false);
                        return Ok(Some(value));
                    }
                }
            }
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
        self.purge_batches(self.slot);
        if from_commit_msg {
            // "<#2FB82F>Commited \"{}\" in slot {}.</>"
            trace!("Commited \"{:?}\" (v={v}) in slot {}.", value, self.slot);
        } else {
            // "<#2FB82F>Commited \"{}\" in slot {} (round {}) from state:</> <#B8E8B8>{}</>"
            trace!(
                "Commited \"{:?}\" (v={v}) in slot {} (round {:?}) from state: {}",
                value,
                self.slot,
                self.round_state.get_round(),
                self.round_state,
            );
        }
        self.last_v = Some(v);
        self.slot += 1;
        self.round_state.clear();
        value
    }

    #[inline]
    fn get_my_v(&self) -> Option<usize> {
        self.round_state.get_my_v()
    }

    fn ongoing(&self) -> bool {
        !self.round_state.proposers().is_empty()
    }

    #[inline]
    fn should_lead(&self) -> bool {
        self.my_pid == self.leader_priority[0]
    }

    fn can_propose(&self) -> bool {
        true
    }
}

impl KCensusShard {
    #[inline]
    fn value_for_msg(&self, msg: &KCensusMsg) -> Option<CommandBatch> {
        if msg.includes_value() {
            let v = msg.get_v();
            Some(self.queued_commands[&v].clone())
        } else {
            None
        }
    }

    #[inline]
    async fn broadcast(&self, msg: KCensusMsg) -> io::Result<()> {
        let value = self.value_for_msg(&msg);
        self.sinks
            .broadcast(KCensusM(msg), value, self.last_v)
            .await
    }

    async fn broadcast_paxos_accept(&self, v: usize, new_value: bool) -> io::Result<()> {
        // TODO: if not a new value, broadcast to closest majority only ? (small optim.)
        self.broadcast(PaxosAccept {
            slot: self.slot,
            leader: self.my_pid,
            v,
            new_value,
        })
        .await
    }

    async fn send_to(&self, msg: KCensusMsg, dest: usize) -> io::Result<()> {
        let value = self.value_for_msg(&msg);
        self.sinks
            .send(KCensusM(msg), value, dest, self.last_v)
            .await
    }

    async fn inner_spread(
        &self,
        v: usize,
        proposer: usize,
        state_id: Duration,
        new_value: bool,
        value_only: bool,
    ) -> io::Result<()> {
        for msg_id in self
            .settings
            .graphs
            .get_new_messages_to_spread(proposer, self.my_pid, state_id)
            .iter()
            .copied()
        {
            assert!(!value_only || new_value);
            let with_value = new_value && self.settings.graphs.should_include_value(&msg_id);
            if value_only && !with_value {
                continue;
            }
            assert_eq!(msg_id.src, self.my_pid);
            assert_eq!(msg_id.proposer, proposer);
            let dest = msg_id.dest;

            if !value_only {
                let remote_states = if self.round_state.has_conflict() {
                    let node_states = self.round_state.clone_node_states();
                    assert!(
                        node_states[proposer].get_v() == Some(v)
                            || node_states[proposer].get_paxos_accept_round().is_some()
                    );
                    Some(node_states)
                } else {
                    None
                };

                let msg = Spread {
                    slot: self.slot,
                    v,
                    msg_id,
                    remote_states,
                    with_value,
                    new_value,
                };
                self.send_to(msg, dest).await?;
            } else {
                let msg = SpreadValueOnly { v, msg_id };
                self.send_to(msg, dest).await?;
            }
        }
        Ok(())
    }

    async fn spread(
        &self,
        v: usize,
        proposer: usize,
        state_id: Duration,
        new_value: bool,
    ) -> io::Result<()> {
        self.inner_spread(v, proposer, state_id, new_value, false)
            .await
    }

    async fn propose_and_spread(&mut self, v: usize, new_value: bool) -> io::Result<()> {
        let me = self.my_pid;
        let state_id = Duration::ZERO;
        self.round_state
            .accept_with_state(v, me, state_id, &self.settings.graphs);

        assert!(!self.round_state.has_conflict());
        self.spread(v, me, state_id, new_value).await
    }

    async fn graph_spread_value_only(&self, prev_msg_id: MessageId, v: usize) -> io::Result<()> {
        if !self.queued_commands.contains_key(&v) {
            return Ok(());
        }
        let proposer = prev_msg_id.proposer;
        let state_id = self.settings.graphs.msg_arrival_state_id(prev_msg_id);
        self.inner_spread(v, proposer, state_id, true, true).await
    }

    async fn graph_spread_new_value_only(&self, v: usize) -> io::Result<()> {
        let me = self.my_pid;
        let state_id = Duration::ZERO;
        self.inner_spread(v, me, state_id, true, true).await
    }
}

pub(crate) type KCensus = Consensus<KCensusSettings, KCensusRoundState>;

impl KCensus {
    pub fn new(
        process_count: usize,
        replica_count: usize,
        alive_replicas: &BitSet,
        my_pid: usize,
        sinks: MultiSink,
        leader_priority: Vec<usize>,
        propagation_graphs: PropagationGraphs,
        shard_count: usize,
    ) -> Self {
        let sinks = Arc::new(Mutex::new(sinks));
        let propagation_graphs = Arc::new(propagation_graphs);
        Self {
            process_count,
            shards: (0..shard_count)
                .map(|shard_id| {
                    KCensusShard::new(
                        process_count,
                        replica_count,
                        alive_replicas.clone(),
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
