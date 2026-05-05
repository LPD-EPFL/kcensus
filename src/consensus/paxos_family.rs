use crate::consensus::message::ConsensusMsg::{Commit, PaxosM};
use crate::consensus::message::{CommandBatch, ConsensusMessage};
use crate::consensus::paxos_family::message::PaxosMsg::{Accept, ForwardRequest, Prepare};
use crate::consensus::paxos_family::message::{PaxosMsg, PaxosRound};
use crate::consensus::paxos_family::round_state::PaxosFamilyRoundState;
use crate::consensus::paxos_family::PFModeSetting::{
    EPaxos, MultiPaxos, MultiPaxos3P, Pando, SwiftPaxos,
};
use crate::consensus::read_tracker::ReadTracker;
use crate::consensus::{Consensus, ConsensusShard, ConsensusShardTrait};
use crate::multi_sink::{MultiSink, ShardMultiSink};
use crate::topology::Topology;
use bit_set::BitSet;
use log::{debug, info, trace};
use message::RoundV;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) mod message;
mod round_state;

pub struct PaxosFamilySettings {
    // Settings
    mode_setting: PFModeSetting,
    starting_round: Option<PaxosRound>,
    committers: Option<Vec<usize>>,
}

pub type PaxosFamilyShard = ConsensusShard<PaxosFamilySettings, PaxosFamilyRoundState>;

#[derive(Clone, PartialEq, Debug)]
pub enum PFModeSetting {
    Paxos,
    MultiPaxos,
    MultiPaxos3P,
    EPaxos,
    Pando { delegates: Arc<Vec<usize>> },
    SwiftPaxos { quorum: Arc<Option<BitSet>> },
}

impl PaxosFamilyShard {
    pub fn new(
        topology: &Topology,
        my_pid: usize,
        sinks: ShardMultiSink,
        leader_priority: Vec<usize>,
        committers: Option<Vec<usize>>,
        mode_setting: PFModeSetting,
    ) -> Self {
        let process_count = topology.nb_processes;
        let replica_count = topology.nb_replicas;
        assert!(my_pid < process_count);
        let majority = 1 + (replica_count / 2);
        let starting_round = match mode_setting {
            MultiPaxos | MultiPaxos3P => {
                Some(PaxosRound::default().next_leader_round(leader_priority[0]))
            }
            _ => None,
        };
        let replica = topology.alive_replicas.contains(my_pid);
        Self {
            process_count,
            alive_replicas: topology.alive_replicas.clone(),
            replica,
            leader_priority,
            my_pid,

            sinks,

            next_uid: 2 * my_pid,
            slot: 0,
            queued_commands: HashMap::with_capacity(process_count),
            last_v: None,

            read_tracker: ReadTracker::new(majority),

            settings: PaxosFamilySettings {
                mode_setting,
                starting_round,
                committers,
            },

            round_state: PaxosFamilyRoundState::new(
                my_pid,
                replica,
                process_count,
                replica_count,
                starting_round,
            ),
        }
    }
}

impl Debug for PaxosFamilyShard {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "slot={}, last_v={:?}, round={:?}, my_v={:?}, accepted_at={:?}, is_prepared={}, can_commit={}, can_epaxos_commit={:?}, queued_commands={:?}",
            self.slot,
            self.last_v,
            self.round_state.round,
            self.get_my_v(),
            self.round_state.get_last_accepted_round(),
            self.round_state.is_prepared(),
            self.round_state.paxos_can_commit(),
            self.round_state.epaxos_can_commit(),
            self.queued_commands,
        )
    }
}

impl ConsensusShardTrait for PaxosFamilyShard {
    async fn process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<CommandBatch>> {
        if !self.replica && !matches!(self.settings.mode_setting, SwiftPaxos { .. }) {
            return Ok(None);
        }

        let src = msg.src;
        let msg = match msg.msg {
            PaxosM(msg) => {
                if matches!(msg, ForwardRequest { .. }) {
                    trace!(
                        "Processing forward request from {src} (shard={}): {msg:?}",
                        self.sinks.shard_id
                    );
                    return Ok(None);
                } else {
                    msg
                }
            }
            x => panic!("Unexpected message type: {x:?}"),
        };

        debug!(
            "Processing paxos msg from {src} (shard={}): {msg:?}",
            self.sinks.shard_id
        );

        let delegate_or_leader = match &msg {
            Prepare { slot, round, .. } | Accept { slot, round, .. } => {
                if *slot < self.slot || Some(*round) < self.round_state.round {
                    return Ok(None);
                } else if Some(*round) > self.round_state.round {
                    self.goto_round(Some(*round));
                }
                assert_eq!(*slot, self.slot);
                assert_eq!(Some(*round), self.round_state.round);
                assert!(
                    self.alive_replicas.contains(src)
                        || matches!(self.settings.mode_setting, SwiftPaxos { .. })
                );

                match &self.settings.mode_setting {
                    Pando { delegates } => delegates[round.leader],
                    SwiftPaxos { .. } => {
                        if let Prepare {
                            rv: RoundV::FastV { proposer, .. },
                            ..
                        } = &msg
                            && round.leader != self.my_pid
                        {
                            *proposer
                        } else {
                            round.leader
                        }
                    }
                    _ => round.leader,
                }
            }
            _ => panic!("Unexpected message type: {msg:?}"),
        };

        if !self.replica {
            match self.settings.mode_setting {
                SwiftPaxos { .. } => {
                    return Ok(self.process_answers_at_swift_paxos_client(src, msg));
                }
                _ => unreachable!("Non-SwiftPaxos mode should have exited earlier."),
            };
        }

        match msg {
            Prepare { round, rv, .. } => {
                if self.get_my_v().is_none() {
                    self.round_state.paxos_propose_v(rv)
                }

                if delegate_or_leader != self.my_pid {
                    if matches!(self.settings.mode_setting, SwiftPaxos { .. })
                        && delegate_or_leader == round.leader
                        || self.round_state.get_last_accepted_round() == Some(round)
                    {
                        return Ok(None);
                    }
                    self.answer_prepare(delegate_or_leader).await?;
                    assert!(delegate_or_leader == src || delegate_or_leader != round.leader);
                    return Ok(None);
                }

                let was_paxos_prepared = self.round_state.is_prepared();
                self.round_state.receive_promise(src, rv);

                if matches!(self.settings.mode_setting, SwiftPaxos { .. }) {
                    if round.leader != self.my_pid {
                        // I'm the proposer (not the leader). Try to complete the fast-path.
                        return Ok(self.process_answers_at_swift_paxos_client(src, msg));
                    } else {
                        // I'm the leader. Immediately start the accept phase.
                        if self.round_state.get_last_accepted_round().is_none() {
                            self.round_state.self_accept(round);
                            self.broadcast_accept().await?;
                        }
                    }
                } else if self.settings.mode_setting == EPaxos
                    && self.round_state.get_last_accepted_round().is_none()
                {
                    if let RoundV::FastV { proposer, v } = rv {
                        self.round_state.epaxos_answered(src, proposer, v);
                        if self.round_state.epaxos_can_commit() {
                            self.broadcast_commit().await?;
                            info!(
                                "Commit via EPaxos: shard={} slot={} v={v}",
                                self.sinks.shard_id, self.slot
                            );
                            let value = self.commit_slot(v, false);
                            return Ok(Some(value));
                        }

                        let res = self.round_state.epaxos_try_adopt();
                        if let Some(v) = res {
                            // Can go to paxos accept phase
                            assert!(self.round_state.is_prepared());
                            self.round_state.adopt_from_epaxos(round, v);
                            self.broadcast_accept().await?;
                        }
                    } else {
                        unreachable!("Can not receive PaxosV with None round in EPaxos.")
                    }
                } else if !was_paxos_prepared && self.round_state.is_prepared() {
                    self.round_state.self_accept(round);
                    self.broadcast_accept().await?;
                }
            }
            Accept { round, v, .. } => {
                self.round_state.receive_accept(src, round, v);

                if self.my_pid != round.leader {
                    let i_am_3p_commiter =
                        matches!(self.settings.mode_setting, MultiPaxos3P | SwiftPaxos { .. })
                            && self.get_committer(v) == Some(self.my_pid);
                    if src == delegate_or_leader {
                        self.answer_accept().await?;
                        if !i_am_3p_commiter {
                            // nothing else to do (unless I'm a commiter in MultiPaxos3P / SwiftPaxos)
                            return Ok(None);
                        }
                        if let SwiftPaxos { quorum } = &self.settings.mode_setting {
                            self.round_state
                                .swift_answered(src, self.my_pid, v, quorum, true);
                            if self.round_state.swift_can_commit(quorum.is_some()) {
                                return Ok(Some(self.commit_slot(v, false)));
                            }
                        }
                    }
                    assert!(i_am_3p_commiter);
                }

                if self.round_state.paxos_can_commit() {
                    if round.leader == self.my_pid {
                        self.broadcast_commit().await?;
                    } else if self.get_committer(v) == Some(self.my_pid) {
                        let requester = self
                            .get_requester(v)
                            .expect("Should have requester if it has a committer");
                        if requester != self.my_pid {
                            // TODO: Could maybe only send to requester here
                            self.broadcast_commit().await?;
                        }
                    }
                    info!(
                        "Commit via Paxos: shard={} slot={} v={v}",
                        self.sinks.shard_id, self.slot
                    );
                    let value = self.commit_slot(v, false);
                    return Ok(Some(value));
                }
            }
            _ => panic!("Unexpected message type: {msg:?}"),
        }

        Ok(None)
    }

    #[inline]
    fn can_forward_proposals(&self) -> bool {
        // TODO: maybe we want to return false in some cases ?
        //   e.g. return self.is_multi_paxos() || !self.can_propose() || few_requests_in_queue ?
        //   we have to make sure proposers don't stay stuck forever though.
        true
    }

    async fn propose_start(&mut self, value: CommandBatch, contention: bool) -> io::Result<()> {
        if !self.can_propose() || contention || (self.is_multi_paxos() && !self.should_lead()) {
            let v = self.store_new_command(value.clone());
            self.sinks
                .priority_broadcast(
                    PaxosM(ForwardRequest { v }),
                    Some(value),
                    self.last_v,
                    Some(self.get_leader()),
                )
                .await
        } else {
            let v = self.store_new_command(value);
            self.propose(v, true).await
        }
    }

    async fn repropose_start(&mut self, v: usize) -> io::Result<()> {
        self.propose(v, false).await
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
                "Commited \"{:?}\" (v={v}) in slot {} (round {:?})",
                value, self.slot, self.round_state.round
            );
        }
        self.last_v = Some(v);
        self.slot += 1;
        self.goto_round(self.settings.starting_round);
        self.round_state.full_clear();
        value
    }

    #[inline]
    fn get_my_v(&self) -> Option<usize> {
        self.round_state.get_v()
    }

    #[inline]
    fn ongoing(&self) -> bool {
        self.get_my_v().is_some()
    }

    #[inline]
    fn should_lead(&self) -> bool {
        let leader = self.get_leader();
        assert!(
            self.my_pid != leader || self.can_propose(),
            "A non-replica should not have to lead."
        );
        self.my_pid == leader
    }

    #[inline]
    fn can_propose(&self) -> bool {
        self.replica || matches!(self.settings.mode_setting, SwiftPaxos { .. })
    }
}

impl PaxosFamilyShard {
    fn process_answers_at_swift_paxos_client(
        &mut self,
        src: usize,
        msg: PaxosMsg,
    ) -> Option<CommandBatch> {
        if self.get_committer(msg.get_v()) != Some(self.my_pid) {
            return None;
        }
        let quorum = if let SwiftPaxos { quorum } = &self.settings.mode_setting {
            quorum
        } else {
            unreachable!("This method should only be called in SwiftPaxos mode.")
        };
        match msg {
            Prepare { round, rv, .. } => {
                if let RoundV::FastV { proposer, v } = rv {
                    assert_ne!(
                        src, round.leader,
                        "leaders don't respond with prepare messages"
                    );
                    self.round_state
                        .swift_answered(src, proposer, v, quorum, false);
                    if self.round_state.swift_can_commit(quorum.is_some()) {
                        info!(
                            "Commit via SwiftPaxos: shard={} slot={} v={v}",
                            self.sinks.shard_id, self.slot
                        );
                        let value = self.commit_slot(v, false);
                        Some(value)
                    } else {
                        None
                    }
                } else {
                    unreachable!("Can not receive a PaxosV in a Prepare msg in SwiftPaxos.")
                }
            }
            Accept { round, v, .. } => {
                if src == round.leader {
                    self.round_state
                        .swift_answered(src, self.my_pid, v, quorum, true);
                    if self.round_state.swift_can_commit(quorum.is_some()) {
                        return Some(self.commit_slot(v, false));
                    }
                };

                self.round_state.receive_accept(src, round, v);
                if self.round_state.paxos_can_commit() {
                    Some(self.commit_slot(v, false))
                } else {
                    None
                }
            }
            _ => panic!("Unexpected message type: {msg:?}"),
        }
    }

    #[inline]
    fn goto_round(&mut self, round: Option<PaxosRound>) {
        if round.unwrap_or_default()
            > self
                .settings
                .starting_round
                .unwrap_or_default()
                .next_leader_round(self.my_pid)
        {
            if self.round_state.round.is_some()
                || !matches!(self.settings.mode_setting, SwiftPaxos { .. })
            {
                debug!(
                    // "<#FF4F4F>Cannot commit in round {} from state:</> <#EFBFBF>{}</>"
                    "Cannot commit in round {:?}",
                    self.round_state.round,
                );
            }
            if round.unwrap_or_default().round_group
                > self.round_state.round.unwrap_or_default().round_group + 1
            {
                // "<yellow>######## Skipping round !!!!</>"
                debug!("######## Skipping round !!!!");
            }
        }
        self.round_state.round = round;
        self.round_state.next_round();
    }

    #[inline]
    fn value_for_msg(&self, msg: &PaxosMsg, with_value: bool) -> Option<CommandBatch> {
        if with_value {
            Some(self.queued_commands[&msg.get_v()].clone())
        } else {
            None
        }
    }

    #[inline]
    async fn send(&self, msg: PaxosMsg, dest: usize) -> io::Result<()> {
        self.sinks.send(PaxosM(msg), None, dest, self.last_v).await
    }

    #[inline]
    async fn broadcast(&self, msg: PaxosMsg, with_value: bool) -> io::Result<()> {
        let value = self.value_for_msg(&msg, with_value);
        self.sinks.broadcast(PaxosM(msg), value, self.last_v).await
    }

    async fn propose(&mut self, v: usize, with_value: bool) -> io::Result<()> {
        assert_eq!(self.round_state.round, self.settings.starting_round);
        assert!(self.round_state.get_v().is_none());
        let electing = match self.settings.mode_setting {
            SwiftPaxos { .. } => self.leader_priority[0],
            _ => self.my_pid,
        };
        let round = self
            .round_state
            .round
            .unwrap_or_default()
            .next_leader_round(electing);
        self.goto_round(Some(round));
        assert_eq!(
            self.is_multi_paxos(),
            Some(round) == self.settings.starting_round
        );
        let msg = if self.is_multi_paxos()
            || (matches!(self.settings.mode_setting, SwiftPaxos { .. }) && electing == self.my_pid)
        {
            let rv = RoundV::new_paxos_v(None, v);
            self.round_state.paxos_propose_v(rv);
            self.round_state.self_accept(round);
            Accept {
                slot: self.slot,
                round,
                v: rv.get_v(),
            }
        } else {
            let rv = match &self.settings.mode_setting {
                EPaxos => {
                    self.round_state.epaxos_propose_v(v);
                    RoundV::new_fast_v(self.my_pid, v)
                }
                SwiftPaxos { quorum } => {
                    self.round_state
                        .swift_propose_v(v, quorum, electing == self.my_pid);
                    RoundV::new_fast_v(self.my_pid, v)
                }
                _ => RoundV::new_paxos_v(None, v),
            };
            self.round_state.paxos_propose_v(rv);
            Prepare {
                slot: self.slot,
                round,
                rv,
            }
        };
        // TODO: Only send to fastest majority/quorum ?
        self.broadcast(msg, with_value).await
    }

    #[inline]
    async fn answer_prepare(&self, src: usize) -> io::Result<()> {
        let msg = Prepare {
            slot: self.slot,
            round: self.round_state.round.unwrap(),
            rv: self.round_state.get_rv().unwrap(),
        };
        self.send(msg, src).await
    }

    #[inline]
    async fn broadcast_accept(&self) -> io::Result<()> {
        // TODO: Only send to fastest majority/quorum ?
        let msg = Accept {
            slot: self.slot,
            round: self.round_state.round.unwrap(),
            v: self.round_state.get_v().unwrap(),
        };
        self.broadcast(msg, false).await
    }

    #[inline]
    async fn answer_accept(&self) -> io::Result<()> {
        let leader = self.round_state.round.unwrap().leader;
        let v = self.round_state.get_v().unwrap();
        let msg = Accept {
            slot: self.slot,
            round: self.round_state.round.unwrap(),
            v,
        };
        if matches!(self.settings.mode_setting, MultiPaxos3P | SwiftPaxos { .. })
        // && self.round_state.round == self.settings.starting_round // Not correct for SwiftPaxos
        {
            if let Some(commiter) = self.get_committer(v) {
                if commiter != leader && commiter != self.my_pid {
                    // TODO: Only send if self is in the fastest majority (from leader to initiator) ?
                    self.send(msg.clone(), commiter).await?;
                }
            }
        }
        // TODO: Only send if self is in the fastest majority (in leader RTT) ?
        self.send(msg, leader).await
    }

    async fn broadcast_commit(&self) -> io::Result<()> {
        let slot = self.slot;
        let v = self.round_state.get_v().unwrap();
        self.sinks
            .priority_broadcast(Commit { slot, v }, None, self.last_v, self.get_requester(v))
            .await
    }

    #[inline]
    fn is_multi_paxos(&self) -> bool {
        matches!(self.settings.mode_setting, MultiPaxos | MultiPaxos3P)
    }

    fn get_committer(&self, v: usize) -> Option<usize> {
        if let Some(committers) = &self.settings.committers {
            self.get_requester(v).map(|proposer| committers[proposer])
        } else if matches!(self.settings.mode_setting, SwiftPaxos { .. }) {
            self.get_requester(v)
        } else {
            None
        }
    }

    #[inline]
    fn get_leader(&self) -> usize {
        if matches!(self.settings.mode_setting, MultiPaxos | MultiPaxos3P) {
            self.leader_priority[0]
        } else {
            self.leader_priority
                .iter()
                .copied()
                .find(|leader| {
                    self.alive_replicas.contains(*leader)
                        && self
                            .queued_commands
                            .keys()
                            .any(|v| self.get_committer(*v) == Some(*leader))
                })
                .unwrap_or(self.leader_priority[0])
        }
    }
}

pub(crate) type PaxosFamily = Consensus<PaxosFamilySettings, PaxosFamilyRoundState>;

impl PaxosFamily {
    pub fn new(
        topology: &Topology,
        my_pid: usize,
        sinks: MultiSink,
        leader_priority: Vec<usize>,
        committers: Option<Vec<usize>>,
        mode_setting: PFModeSetting,
        shard_count: usize,
    ) -> Self {
        let sinks = Arc::new(Mutex::new(sinks));

        Self {
            process_count: topology.nb_processes,
            shards: (0..shard_count)
                .map(|id| {
                    PaxosFamilyShard::new(
                        topology,
                        my_pid,
                        ShardMultiSink {
                            shard_id: id,
                            multi_sink: sinks.clone(),
                        },
                        leader_priority.clone(),
                        committers.clone(),
                        mode_setting.clone(),
                    )
                })
                .collect(),
            sinks,
        }
    }
}
