use crate::consensus::command::Command;
use crate::consensus::message::ConsensusMessage;
use crate::consensus::message::ConsensusMsg::{Commit, PaxosM};
use crate::consensus::paxos_family::epaxos_round_state::EPaxosRoundState;
use crate::consensus::paxos_family::message::PaxosMsg::{Accept, ForwardRequest, Prepare};
use crate::consensus::paxos_family::message::{PaxosMsg, PaxosRound};
use crate::consensus::paxos_family::paxos_round_state::PaxosRoundState;
use crate::consensus::paxos_family::Mode::{EPaxos, MultiPaxos, Paxos};
use crate::consensus::read_tracker::ReadTracker;
use crate::consensus::Consensus;
use crate::multi_sink::MultiSink;
use log::{debug, info, trace};
use message::RoundV;
use std::collections::HashMap;
use std::io;

mod epaxos_round_state;
pub mod message;
mod paxos_round_state;

pub struct PaxosFamily {
    // Settings
    nb_nodes: usize,
    my_pid: usize,
    leader_priority: Vec<usize>,
    mode: Mode,
    starting_round: Option<PaxosRound>,

    // Connections
    sinks: MultiSink,

    // Overall state
    next_uid: usize,
    slot: usize,
    round: Option<PaxosRound>,
    queued_commands: HashMap<usize, Command>,

    paxos_state: PaxosRoundState,
    epaxos_state: EPaxosRoundState,

    read_tracker: ReadTracker,
}

pub enum Mode {
    Paxos,
    MultiPaxos,
    EPaxos,
}

impl PaxosFamily {
    pub fn new(
        nb_nodes: usize,
        my_pid: usize,
        sinks: MultiSink,
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
            mode,
            starting_round,

            sinks,

            next_uid: my_pid,
            slot: 0,
            round: starting_round,
            queued_commands: HashMap::with_capacity(nb_nodes),

            paxos_state: PaxosRoundState::new(nb_nodes, my_pid),
            epaxos_state: EPaxosRoundState::new(nb_nodes, my_pid),

            read_tracker: ReadTracker::new(1 + nb_nodes / 2),
        }
    }
}

impl Consensus for PaxosFamily {
    async fn process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<Command>> {
        let src = msg.src;
        let msg = match msg.msg {
            PaxosM(msg) => msg,
            x => panic!("Unexpected message type: {:?}", x),
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
                        self.paxos_state.propose_v(rv)
                    }
                    self.answer_prepare(src).await?;
                    return Ok(None);
                }

                debug_assert!(round.proposer == self.my_pid);
                let was_paxos_prepared = self.paxos_state.is_prepared();
                self.paxos_state.receive_promise(src, rv);

                if matches!(self.mode, EPaxos)
                    && self.paxos_state.get_last_accepted_round().is_none()
                {
                    if let RoundV::EPaxosV { proposer, v } = rv {
                        self.epaxos_state.answered(src, proposer, v);
                        if self.epaxos_state.can_commit() {
                            self.broadcast_commit().await?;
                            info!("Fast-commited: v={}", v);
                            let value = self.commit_slot(v, true);
                            return Ok(Some(value));
                        }

                        let res = self.epaxos_state.try_adopt();
                        if let Some(v) = res {
                            // Can go to paxos accept phase
                            debug_assert!(self.paxos_state.is_prepared());
                            self.paxos_state.adopt_from_epaxos(round, v);
                            self.broadcast_accept().await?;
                        }
                    } else {
                        panic!("Can not receive PaxosV with None round in EPaxos.")
                    }
                } else if !was_paxos_prepared && self.paxos_state.is_prepared() {
                    self.paxos_state.self_accept_v(round);
                    self.broadcast_accept().await?;
                }
            }
            Accept { round, v, .. } => {
                if round.proposer != self.my_pid {
                    self.paxos_state.accept_v(src, round, v);
                    self.answer_accept().await?;
                    return Ok(None);
                }

                debug_assert!(round.proposer == self.my_pid);
                self.paxos_state.receive_accepted(src);

                if self.paxos_state.can_commit() {
                    self.broadcast_commit().await?;
                    info!("Commited: v={}", v);
                    let value = self.commit_slot(v, true);
                    return Ok(Some(value));
                }
            }
            ForwardRequest { .. } => (),
        }

        Ok(None)
    }

    #[inline]
    fn can_forward_proposals(&mut self) -> bool {
        matches!(self.mode, MultiPaxos)
    }

    async fn propose_start(&mut self, command: Command, contention: bool) -> io::Result<()> {
        let v = self.store_new_command(command);

        if contention || (matches!(self.mode, MultiPaxos) && !self.should_lead()) {
            self.broadcast(ForwardRequest { v }, true).await
        } else {
            self.propose(v, true).await
        }
    }

    async fn repropose_start(&mut self, v: usize) -> io::Result<()> {
        self.propose(v, false).await
    }

    #[inline]
    fn commit_slot(&mut self, v: usize, from_commit_msg: bool) -> Command {
        let value = self.queued_commands.remove(&v).unwrap();
        if from_commit_msg {
            // "<#2FB82F>Commited \"{}\" in slot {}.</>"
            trace!("Commited \"{:?}\" in slot {}.", value.command, self.slot);
        } else {
            // "<#2FB82F>Commited \"{}\" in slot {} (round {}) from state:</> <#B8E8B8>{}</>"
            trace!(
                "Commited \"{:?}\" in slot {} (round {:?})",
                value.command, self.slot, self.round
            );
        }
        self.slot += 1;
        self.goto_round(self.starting_round);
        self.paxos_state.full_clear();
        self.epaxos_state.full_clear();
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
        self.paxos_state.get_v()
    }

    #[inline]
    fn should_lead(&self) -> bool {
        if let MultiPaxos = self.mode {
            self.my_pid == self.leader_priority[0]
        } else {
            let leader = self.leader_priority.iter().copied().find(|leader| {
                self.queued_commands
                    .values()
                    .any(|value| value.proposer == *leader)
            });
            Some(self.my_pid) == leader
        }
    }

    fn get_next_uid(&mut self) -> usize {
        let uid = self.next_uid;
        self.next_uid += self.nb_nodes;
        uid
    }

    #[inline]
    fn get_new_batch_to_propose(&self) -> Option<Command> {
        // TODO: Actually form batch here !
        None
    }

    #[inline]
    fn get_v_to_repropose(&self) -> usize {
        *self.queued_commands.keys().min().unwrap()
    }

    fn get_queued_commands(&self) -> &HashMap<usize, Command> {
        &self.queued_commands
    }

    fn get_queued_commands_mut(&mut self) -> &mut HashMap<usize, Command> {
        &mut self.queued_commands
    }

    #[inline]
    fn get_read_tracker(&mut self) -> &mut ReadTracker {
        &mut self.read_tracker
    }

    #[inline]
    fn get_sinks(&mut self) -> &mut MultiSink {
        &mut self.sinks
    }
}

impl PaxosFamily {
    #[inline]
    fn goto_round(&mut self, round: Option<PaxosRound>) {
        if round.unwrap_or_default()
            > self
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
        self.paxos_state.next_round();
    }

    #[inline]
    fn value_for_msg(&mut self, msg: &PaxosMsg, with_value: bool) -> Option<Command> {
        if with_value {
            Some(self.queued_commands[&msg.get_v()].clone())
        } else {
            None
        }
    }

    #[inline]
    async fn send(&mut self, msg: PaxosMsg, dest: usize) -> io::Result<()> {
        self.sinks.send(PaxosM(msg), None, dest).await
    }

    #[inline]
    async fn broadcast(&mut self, msg: PaxosMsg, with_value: bool) -> io::Result<()> {
        let value = self.value_for_msg(&msg, with_value);
        self.sinks.broadcast(PaxosM(msg), value).await
    }

    async fn propose(&mut self, v: usize, with_value: bool) -> io::Result<()> {
        debug_assert!(self.round == self.starting_round);
        debug_assert!(self.paxos_state.get_v().is_none());
        let round = self
            .round
            .unwrap_or_default()
            .next_proposer_round(self.my_pid);
        self.goto_round(Some(round));
        let msg = if Some(round) != self.starting_round {
            debug_assert!(!matches!(self.mode, MultiPaxos));
            let rv = match self.mode {
                EPaxos => {
                    self.epaxos_state.propose_v(v);
                    RoundV::new_epaxos_v(self.my_pid, v)
                }
                _ => RoundV::new_paxos_v(None, v),
            };
            self.paxos_state.propose_v(rv);
            Prepare {
                slot: self.slot,
                round,
                rv,
            }
        } else {
            debug_assert!(matches!(self.mode, MultiPaxos));
            let rv = RoundV::new_paxos_v(None, v);
            self.paxos_state.propose_v(rv);
            self.paxos_state.self_accept_v(round);
            Accept {
                slot: self.slot,
                round,
                v: rv.get_v(),
            }
        };
        self.broadcast(msg, with_value).await
    }

    async fn answer_prepare(&mut self, src: usize) -> io::Result<()> {
        let msg = Prepare {
            slot: self.slot,
            round: self.round.unwrap(),
            rv: self.paxos_state.get_rv().unwrap(),
        };
        self.send(msg, src).await
    }

    async fn broadcast_accept(&mut self) -> io::Result<()> {
        let msg = Accept {
            slot: self.slot,
            round: self.round.unwrap(),
            v: self.paxos_state.get_v().unwrap(),
        };
        self.broadcast(msg, false).await
    }

    async fn answer_accept(&mut self) -> io::Result<()> {
        let src = self.round.unwrap().proposer;
        let msg = Accept {
            slot: self.slot,
            round: self.round.unwrap(),
            v: self.paxos_state.get_v().unwrap(),
        };
        self.send(msg, src).await
    }

    async fn broadcast_commit(&mut self) -> io::Result<()> {
        let msg = Commit {
            slot: self.slot,
            v: self.paxos_state.get_v().unwrap(),
        };
        self.sinks.broadcast(msg, None).await
    }
}
