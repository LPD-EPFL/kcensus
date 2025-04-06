use crate::connector::DeSink;
use crate::consensus::message::ConsensusMessage;
use crate::consensus::message::ConsensusMsg::PaxosM;
use crate::consensus::paxos::message::PaxosMsg::Commit;
use crate::consensus::paxos::message::PaxosMsg::{Accept, Prepare};
use crate::consensus::paxos::message::{PaxosMsg, PaxosRound};
use crate::consensus::paxos::round_state::PaxosRoundState;
use crate::consensus::Consensus;
use crate::message::Message::Done;
use crate::multi_sink::MultiSink;
use crate::value::{KVal, Request};
use log::{debug, info, trace};
use message::RoundValue;
use std::collections::HashMap;
use std::io;

pub mod message;
mod round_state;

pub struct Paxos<Sk> {
    // Settings
    nb_nodes: usize,
    my_pid: usize,

    // Connections
    sinks: MultiSink<Sk>,

    // Overall state
    slot: usize,
    round: PaxosRound,
    values: HashMap<usize, Request>,

    round_state: PaxosRoundState,
}

impl Paxos<DeSink> {
    pub fn new(nb_nodes: usize, my_pid: usize, sinks: MultiSink<DeSink>) -> Self {
        assert!(my_pid < nb_nodes);
        Self {
            nb_nodes,
            my_pid,

            sinks,

            slot: 0,
            round: PaxosRound::default(),
            values: HashMap::with_capacity(nb_nodes),

            round_state: PaxosRoundState::new(nb_nodes, my_pid),
        }
    }

    #[inline]
    fn commit_slot(&mut self, value_uid: usize, commit_msg: bool) -> Request {
        let value = self.values.remove(&value_uid).unwrap();
        if commit_msg {
            // "<#2FB82F>Commited \"{}\" in slot {}.</>"
            debug!("Commited \"{:?}\" in slot {}.", value.value.val, self.slot);
        } else {
            // "<#2FB82F>Commited \"{}\" in slot {} (round {}) from state:</> <#B8E8B8>{}</>"
            debug!(
                "Commited \"{:?}\" in slot {} (round {})",
                value.value.val, self.slot, self.round
            );
        }
        self.slot += 1;
        self.goto_round(PaxosRound::default());
        self.round_state.full_clear();
        value
    }

    #[inline]
    fn goto_round(&mut self, round: PaxosRound) {
        if round != PaxosRound::default() {
            debug!(
                // "<#FF4F4F>Can not commit in round {} from state:</> <#EFBFBF>{}</>"
                "Can not commit in round {}",
                self.round,
            );
            if round.round_group > self.round.round_group + 1 {
                // "<yellow>######## Skipping round !!!!</>"
                debug!("######## Skipping round !!!!");
            }
        }
        self.round = round;
        self.round_state.next_round();
    }

    #[inline]
    fn value_for_msg(&mut self, msg: &PaxosMsg, with_value: bool) -> Option<KVal> {
        if with_value {
            Some(self.values[&msg.get_v()].value.clone())
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

    async fn propose(&mut self, value_uid: usize, with_value: bool) -> io::Result<()> {
        self.goto_round(self.round.next_proposer_round(self.my_pid));
        let round_value = RoundValue::new(self.round, value_uid);
        self.round_state.propose_v(round_value);
        let msg = if self.round != PaxosRound::default() {
            Prepare {
                slot: self.slot,
                round: self.round,
                round_value,
            }
        } else {
            Accept {
                slot: self.slot,
                round: self.round,
                value_uid: round_value.v_uid,
            }
        };
        self.broadcast(msg, with_value).await
    }

    async fn answer_prepare(&mut self, src: usize) -> io::Result<()> {
        let msg = Prepare {
            slot: self.slot,
            round: self.round,
            round_value: self.round_state.get_round_value().unwrap(),
        };
        self.send(msg, src).await
    }

    async fn broadcast_accept(&mut self) -> io::Result<()> {
        let msg = Accept {
            slot: self.slot,
            round: self.round,
            value_uid: self.round_state.get_v().unwrap(),
        };
        self.broadcast(msg, false).await
    }

    async fn answer_accept(&mut self) -> io::Result<()> {
        let src = self.round.proposer;
        let msg = Accept {
            slot: self.slot,
            round: self.round,
            value_uid: self.round_state.get_v().unwrap(),
        };
        self.send(msg, src).await
    }

    async fn broadcast_commit(&mut self) -> io::Result<()> {
        let msg = Commit {
            slot: self.slot,
            value_uid: self.round_state.get_v().unwrap(),
        };
        self.broadcast(msg, false).await
    }
}

impl Consensus for Paxos<DeSink> {
    async fn process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<Request>> {
        debug_assert!(self.ready_to_process(&msg));
        let src = msg.src;
        let msg = match msg.msg {
            PaxosM(msg) => msg,
            x => panic!("Unexpected message type: {:?}", x),
        };
        trace!("Received message: {:?}", msg);

        match msg {
            Prepare {
                slot,
                round,
                round_value,
            } => {
                if slot < self.slot || round < self.round {
                    return Ok(None);
                }
                debug_assert_eq!(slot, self.slot);

                if round.proposer != self.my_pid {
                    debug_assert!(round > self.round);
                    self.goto_round(round);

                    if self.get_my_v().is_none() {
                        self.round_state.propose_v(round_value)
                    }
                    self.answer_prepare(src).await?;
                    return Ok(None);
                }

                debug_assert!(round.proposer == self.my_pid);
                debug_assert!(round == self.round);

                if !self.round_state.is_prepared() {
                    self.round_state.receive_promise(src, round_value);
                    if self.round_state.is_prepared() {
                        self.broadcast_accept().await?;
                        return Ok(None);
                    }
                }
            }
            Accept {
                slot,
                round,
                value_uid,
            } => {
                if slot < self.slot || round < self.round {
                    return Ok(None);
                } else if round > self.round {
                    debug_assert_ne!(round.proposer, self.my_pid);
                    self.goto_round(round);
                }
                debug_assert_eq!(slot, self.slot);
                debug_assert_eq!(round, self.round);

                if round.proposer != self.my_pid {
                    self.round_state
                        .accept_v(src, RoundValue::new(round, value_uid));
                    self.answer_accept().await?;
                    return Ok(None);
                }

                debug_assert!(round.proposer == self.my_pid);
                self.round_state.receive_accepted(src);

                if self.round_state.can_commit() {
                    self.broadcast_commit().await?;
                    info!("Commited: value_uid={}", value_uid);
                    let value = self.commit_slot(value_uid, true);
                    return Ok(Some(value));
                }
            }
            Commit { slot, value_uid } => {
                if slot < self.slot {
                    return Ok(None);
                }
                debug_assert_eq!(slot, self.slot);
                info!("Commit msg: value_uid={}", value_uid);
                let value = self.commit_slot(value_uid, true);
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    async fn propose_start(&mut self, req: Request) -> io::Result<()> {
        let value_uid = self.store_new_value(req);

        self.propose(value_uid, true).await
    }

    async fn repropose_start(&mut self, value_uid: usize) -> io::Result<()> {
        self.propose(value_uid, false).await
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
        self.round_state.get_v()
    }

    #[inline]
    fn should_lead(&self) -> bool {
        self.my_pid == 0
    }

    #[inline]
    async fn announce_done(&mut self) -> io::Result<()> {
        self.sinks.inner_broadcast(Done).await
    }

    #[inline]
    fn store_new_value(&mut self, req: Request) -> usize {
        let value_uid = self.my_pid + (self.slot * self.nb_nodes);
        let inserted = self.values.insert(value_uid, req);
        debug_assert!(inserted.is_none());
        value_uid
    }

    #[inline]
    fn store_remote_value(&mut self, value_uid: usize, v: KVal) {
        // TODO: Allow forwarding values ? (could the value already be there ?)
        let inserted = self.values.insert(value_uid, v.into_remote_req());
        debug_assert!(inserted.is_none());
    }

    #[inline]
    fn knows_value(&self, v_uid: usize) -> bool {
        self.values.contains_key(&v_uid)
    }

    #[inline]
    fn has_queued_values(&self) -> bool {
        !self.values.is_empty()
    }

    #[inline]
    fn get_new_batch_to_propose(&self) -> Option<KVal> {
        // TODO: Actually form batch here !
        None
    }

    #[inline]
    fn get_value_to_repropose(&self) -> usize {
        *self.values.keys().min().unwrap()
    }
}
