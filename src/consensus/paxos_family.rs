use crate::consensus::message::ConsensusMsg::{Commit, PaxosM};
use crate::consensus::message::{CommandBatch, ConsensusMessage};
use crate::consensus::paxos_family::message::PaxosMsg::{Accept, ForwardRequest, Prepare};
use crate::consensus::paxos_family::message::{PaxosMsg, PaxosRound};
use crate::consensus::paxos_family::round_state::PaxosFamilyRoundState;
use crate::consensus::paxos_family::Mode::{EPaxos, MultiPaxos, Paxos};
use crate::consensus::read_tracker::ReadTracker;
use crate::consensus::{Consensus, ConsensusShard, ConsensusShardTrait};
use crate::multi_sink::{MultiSink, ShardMultiSink};
use log::{debug, info, trace};
use message::RoundV;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) mod message;
mod round_state;

pub struct PaxosFamilySettings {
    // Settings
    mode: Mode,
    starting_round: Option<PaxosRound>,
}

pub type PaxosFamilyShard =
    ConsensusShard<PaxosFamilySettings, Option<PaxosRound>, PaxosFamilyRoundState>;

#[derive(Copy, Clone)]
pub enum Mode {
    Paxos,
    MultiPaxos,
    EPaxos,
}

impl PaxosFamilyShard {
    pub fn new(
        nb_nodes: usize,
        my_pid: usize,
        sinks: ShardMultiSink,
        leader_priority: Vec<usize>,
        mode: Mode,
    ) -> Self {
        assert!(my_pid < nb_nodes);
        let starting_round = match mode {
            Paxos => None,
            MultiPaxos => Some(PaxosRound::default().next_proposer_round(leader_priority[0])),
            EPaxos => None,
        };
        Self {
            nb_nodes,
            my_pid,
            leader_priority,

            sinks,

            next_uid: my_pid,
            slot: 0,
            queued_commands: HashMap::with_capacity(nb_nodes),

            read_tracker: ReadTracker::new(1 + nb_nodes / 2),

            settings: PaxosFamilySettings {
                mode,
                starting_round,
            },

            round: starting_round,
            round_state: PaxosFamilyRoundState::new(nb_nodes, my_pid),
        }
    }
}

impl ConsensusShardTrait for PaxosFamilyShard {
    async fn process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<CommandBatch>> {
        let src = msg.src;
        let msg = match msg.msg {
            PaxosM(msg) => msg,
            x => panic!("Unexpected message type: {x:?}"),
        };

        match msg {
            Prepare { slot, round, .. } | Accept { slot, round, .. } => {
                if slot < self.slot || Some(round) < self.round {
                    return Ok(None);
                } else if Some(round) > self.round {
                    debug_assert_ne!(round.proposer, self.my_pid);
                    self.goto_round(Some(round));
                }
                debug_assert_eq!(slot, self.slot);
                debug_assert_eq!(Some(round), self.round);
            }
            _ => (),
        }

        match msg {
            Prepare { round, rv, .. } => {
                if round.proposer != self.my_pid {
                    if self.get_my_v().is_none() {
                        debug_assert!(rv.get_accept_round().is_none());
                        self.round_state.paxos_propose_v(rv)
                    }
                    self.answer_prepare(src).await?;
                    return Ok(None);
                }

                debug_assert!(round.proposer == self.my_pid);
                let was_paxos_prepared = self.round_state.is_prepared();
                self.round_state.receive_promise(src, rv);

                if matches!(self.settings.mode, EPaxos)
                    && self.round_state.get_last_accepted_round().is_none()
                {
                    if let RoundV::EPaxosV { proposer, v } = rv {
                        self.round_state.epaxos_answered(src, proposer, v);
                        if self.round_state.epaxos_can_commit() {
                            self.broadcast_commit().await?;
                            info!("Fast-commited: v={v}");
                            let value = self.commit_slot(v, true);
                            return Ok(Some(value));
                        }

                        let res = self.round_state.epaxos_try_adopt();
                        if let Some(v) = res {
                            // Can go to paxos accept phase
                            debug_assert!(self.round_state.is_prepared());
                            self.round_state.adopt_from_epaxos(round, v);
                            self.broadcast_accept().await?;
                        }
                    } else {
                        panic!("Can not receive PaxosV with None round in EPaxos.")
                    }
                } else if !was_paxos_prepared && self.round_state.is_prepared() {
                    self.round_state.self_accept_v(round);
                    self.broadcast_accept().await?;
                }
            }
            Accept { round, v, .. } => {
                if round.proposer != self.my_pid {
                    self.round_state.accept_v(src, round, v);
                    self.answer_accept().await?;
                    return Ok(None);
                }

                debug_assert!(round.proposer == self.my_pid);
                self.round_state.receive_accepted(src);

                if self.round_state.paxos_can_commit() {
                    self.broadcast_commit().await?;
                    info!("Commited: v={v}");
                    let value = self.commit_slot(v, true);
                    return Ok(Some(value));
                }
            }
            ForwardRequest { .. } => (),
        }

        Ok(None)
    }

    #[inline]
    fn can_forward_proposals(&self) -> bool {
        matches!(self.settings.mode, MultiPaxos)
    }

    async fn propose_start(&mut self, value: CommandBatch, contention: bool) -> io::Result<()> {
        let v = self.store_new_command(value);

        if contention || (matches!(self.settings.mode, MultiPaxos) && !self.should_lead()) {
            self.broadcast(ForwardRequest { v }, true).await
        } else {
            self.propose(v, true).await
        }
    }

    async fn repropose_start(&mut self, v: usize) -> io::Result<()> {
        self.propose(v, false).await
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
                "Commited \"{:?}\" in slot {} (round {:?})",
                value, self.slot, self.round
            );
        }
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
    fn should_lead(&self) -> bool {
        if let MultiPaxos = self.settings.mode {
            self.my_pid == self.leader_priority[0]
        } else {
            let leader = self.leader_priority.iter().copied().find(|leader| {
                self.queued_commands.values().any(|value| {
                    if let CommandBatch::Single(cmd) = value {
                        cmd.proposer == *leader
                    } else {
                        false
                    }
                })
            });
            Some(self.my_pid) == leader
        }
    }
}

impl PaxosFamilyShard {
    #[inline]
    fn goto_round(&mut self, round: Option<PaxosRound>) {
        if round.unwrap_or_default()
            > self
                .settings
                .starting_round
                .unwrap_or_default()
                .next_proposer_round(self.my_pid)
        {
            debug!(
                // "<#FF4F4F>Can not commit in round {} from state:</> <#EFBFBF>{}</>"
                "Can not commit in round {:?}",
                self.round,
            );
            if round.unwrap_or_default().round_group
                > self.round.unwrap_or_default().round_group + 1
            {
                // "<yellow>######## Skipping round !!!!</>"
                debug!("######## Skipping round !!!!");
            }
        }
        self.round = round;
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
        self.sinks.send(PaxosM(msg), None, dest).await
    }

    #[inline]
    async fn broadcast(&self, msg: PaxosMsg, with_value: bool) -> io::Result<()> {
        let value = self.value_for_msg(&msg, with_value);
        self.sinks.broadcast(PaxosM(msg), value).await
    }

    async fn propose(&mut self, v: usize, with_value: bool) -> io::Result<()> {
        debug_assert!(self.round == self.settings.starting_round);
        debug_assert!(self.round_state.get_v().is_none());
        let round = self
            .round
            .unwrap_or_default()
            .next_proposer_round(self.my_pid);
        self.goto_round(Some(round));
        let msg = if Some(round) != self.settings.starting_round {
            debug_assert!(!matches!(self.settings.mode, MultiPaxos));
            let rv = match self.settings.mode {
                EPaxos => {
                    self.round_state.epaxos_propose_v(v);
                    RoundV::new_epaxos_v(self.my_pid, v)
                }
                _ => RoundV::new_paxos_v(None, v),
            };
            self.round_state.paxos_propose_v(rv);
            Prepare {
                slot: self.slot,
                round,
                rv,
            }
        } else {
            debug_assert!(matches!(self.settings.mode, MultiPaxos));
            let rv = RoundV::new_paxos_v(None, v);
            self.round_state.paxos_propose_v(rv);
            self.round_state.self_accept_v(round);
            Accept {
                slot: self.slot,
                round,
                v: rv.get_v(),
            }
        };
        self.broadcast(msg, with_value).await
    }

    async fn answer_prepare(&self, src: usize) -> io::Result<()> {
        let msg = Prepare {
            slot: self.slot,
            round: self.round.unwrap(),
            rv: self.round_state.get_rv().unwrap(),
        };
        self.send(msg, src).await
    }

    async fn broadcast_accept(&self) -> io::Result<()> {
        let msg = Accept {
            slot: self.slot,
            round: self.round.unwrap(),
            v: self.round_state.get_v().unwrap(),
        };
        self.broadcast(msg, false).await
    }

    async fn answer_accept(&self) -> io::Result<()> {
        let src = self.round.unwrap().proposer;
        let msg = Accept {
            slot: self.slot,
            round: self.round.unwrap(),
            v: self.round_state.get_v().unwrap(),
        };
        self.send(msg, src).await
    }

    async fn broadcast_commit(&self) -> io::Result<()> {
        let msg = Commit {
            slot: self.slot,
            v: self.round_state.get_v().unwrap(),
        };
        self.sinks.broadcast(msg, None).await
    }
}

pub(crate) type PaxosFamily =
    Consensus<PaxosFamilySettings, Option<PaxosRound>, PaxosFamilyRoundState>;

impl PaxosFamily {
    pub fn new(
        nb_nodes: usize,
        my_pid: usize,
        sinks: MultiSink,
        leader_priority: Vec<usize>,
        mode: Mode,
        shard_count: usize,
    ) -> Self {
        let sinks = Arc::new(Mutex::new(sinks));

        Self {
            nb_nodes,
            shards: (0..shard_count)
                .map(|id| {
                    PaxosFamilyShard::new(
                        nb_nodes,
                        my_pid,
                        ShardMultiSink {
                            shard_id: id,
                            multi_sink: sinks.clone(),
                        },
                        leader_priority.clone(),
                        mode,
                    )
                })
                .collect(),
            sinks,
        }
    }
}
