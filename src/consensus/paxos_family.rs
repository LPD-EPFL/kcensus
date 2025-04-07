use crate::connector::DeSink;
use crate::consensus::message::ConsensusMessage;
use crate::consensus::message::ConsensusMsg::PaxosM;
use crate::consensus::paxos_family::epaxos_round_state::EPaxosRoundState;
use crate::consensus::paxos_family::message::PaxosMsg::Commit;
use crate::consensus::paxos_family::message::PaxosMsg::{Accept, Prepare};
use crate::consensus::paxos_family::message::{PaxosMsg, PaxosRound};
use crate::consensus::paxos_family::paxos_round_state::PaxosRoundState;
use crate::consensus::paxos_family::Mode::{EPaxos, MultiPaxos, Paxos};
use crate::consensus::Consensus;
use crate::message::Message::Done;
use crate::multi_sink::MultiSink;
use crate::value::{KVal, Request};
use log::{debug, info, trace};
use message::RoundV;
use std::collections::HashMap;
use std::io;

mod epaxos_round_state;
pub mod message;
mod paxos_round_state;

pub struct PaxosFamily<Sk> {
    // Settings
    nb_nodes: usize,
    my_pid: usize,
    leader: usize,
    mode: Mode,
    starting_round: Option<PaxosRound>,

    // Connections
    sinks: MultiSink<Sk>,

    // Overall state
    slot: usize,
    round: Option<PaxosRound>,
    requests: HashMap<usize, Request>,

    paxos_state: PaxosRoundState,
    epaxos_state: EPaxosRoundState,
}

pub enum Mode {
    Paxos,
    MultiPaxos,
    EPaxos,
}

impl PaxosFamily<DeSink> {
    pub fn new(
        nb_nodes: usize,
        my_pid: usize,
        sinks: MultiSink<DeSink>,
        leader: usize,
        mode: Mode,
    ) -> Self {
        assert!(my_pid < nb_nodes);
        let starting_round = match mode {
            Paxos => Some(PaxosRound::default()),
            MultiPaxos => Some(PaxosRound::default().next_proposer_round(leader)),
            EPaxos => None,
        };
        Self {
            nb_nodes,
            my_pid,
            leader,
            mode,
            starting_round,

            sinks,

            slot: 0,
            round: starting_round,
            requests: HashMap::with_capacity(nb_nodes),

            paxos_state: PaxosRoundState::new(nb_nodes, my_pid),
            epaxos_state: EPaxosRoundState::new(nb_nodes, my_pid),
        }
    }
}

impl Consensus for PaxosFamily<DeSink> {
    async fn process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<Request>> {
        debug_assert!(self.ready_to_process(&msg));
        let src = msg.src;
        let msg = match msg.msg {
            PaxosM(msg) => msg,
            x => panic!("Unexpected message type: {:?}", x),
        };
        trace!("Received message: {:?}", msg);

        match msg {
            Prepare { slot, round, rv } => {
                if slot < self.slot || Some(round) < self.round {
                    return Ok(None);
                } else if Some(round) > self.round {
                    debug_assert_ne!(round.proposer, self.my_pid);
                    self.goto_round(round);
                }
                debug_assert_eq!(slot, self.slot);
                debug_assert!(Some(round) == self.round);

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
                            self.paxos_state.self_accept_v(round);
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
            Accept { slot, round, v } => {
                if slot < self.slot || Some(round) < self.round {
                    return Ok(None);
                } else if Some(round) > self.round {
                    debug_assert_ne!(round.proposer, self.my_pid);
                    self.goto_round(round);
                }
                debug_assert_eq!(slot, self.slot);
                debug_assert_eq!(Some(round), self.round);

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
            Commit { slot, v } => {
                if slot < self.slot {
                    return Ok(None);
                }
                debug_assert_eq!(slot, self.slot);
                info!("Commit msg: v={}", v);
                let value = self.commit_slot(v, true);
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    async fn propose_start(&mut self, req: Request) -> io::Result<()> {
        let v = self.store_new_request(req);

        self.propose(v, true).await
    }

    async fn repropose_start(&mut self, v: usize) -> io::Result<()> {
        self.propose(v, false).await
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
        self.my_pid == self.leader
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

impl PaxosFamily<DeSink> {
    #[inline]
    fn commit_slot(&mut self, v: usize, commit_msg: bool) -> Request {
        let value = self.requests.remove(&v).unwrap();
        if commit_msg {
            // "<#2FB82F>Commited \"{}\" in slot {}.</>"
            debug!("Commited \"{:?}\" in slot {}.", value.value.val, self.slot);
        } else {
            // "<#2FB82F>Commited \"{}\" in slot {} (round {}) from state:</> <#B8E8B8>{}</>"
            debug!(
                "Commited \"{:?}\" in slot {} (round {:?})",
                value.value.val, self.slot, self.round
            );
        }
        self.slot += 1;
        self.goto_round(PaxosRound::default());
        self.paxos_state.full_clear();
        self.epaxos_state.full_clear();
        value
    }

    #[inline]
    fn goto_round(&mut self, round: PaxosRound) {
        if round != PaxosRound::default() {
            debug!(
                // "<#FF4F4F>Can not commit in round {} from state:</> <#EFBFBF>{}</>"
                "Can not commit in round {:?}",
                self.round,
            );
            if round.round_group > self.round.unwrap_or_default().round_group + 1 {
                // "<yellow>######## Skipping round !!!!</>"
                debug!("######## Skipping round !!!!");
            }
        }
        self.round = Some(round);
        self.paxos_state.next_round();
    }

    #[inline]
    fn value_for_msg(&mut self, msg: &PaxosMsg, with_value: bool) -> Option<KVal> {
        if with_value {
            Some(self.requests[&msg.get_v()].value.clone())
        } else {
            None
        }
    }

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
        self.goto_round(round);
        let msg = if Some(round) != self.starting_round {
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
            debug_assert!(!matches!(self.mode, EPaxos));
            let rv = RoundV::new_paxos_v(self.starting_round, v);
            self.paxos_state.propose_v(rv);
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
        self.broadcast(msg, false).await
    }
}
