use crate::consensus::kcensus::message::KCensusMsg;
use crate::consensus::message::ConsensusMsg::{Commit, KCensusM, PaxosM};
use crate::consensus::paxos_family::message::PaxosMsg;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ConsensusMsg {
    KCensusM(KCensusMsg),
    PaxosM(PaxosMsg),
    Commit { slot: usize, v: usize },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConsensusMessage {
    pub msg: ConsensusMsg,
    pub src: usize,
}

impl ConsensusMessage {
    pub fn can_include_value(&self) -> bool {
        match &self.msg {
            KCensusM(msg) => msg.includes_value(),
            PaxosM(msg) => msg.can_include_value(self.src),
            Commit { .. } => false,
        }
    }

    pub fn should_include_value(&self) -> bool {
        match &self.msg {
            KCensusM(msg) => msg.includes_value(),
            PaxosM(_) => false,
            Commit { .. } => false,
        }
    }

    pub fn get_v(&self) -> usize {
        match &self.msg {
            KCensusM(msg) => msg.get_v(self.src),
            PaxosM(msg) => msg.get_v(),
            Commit { v, .. } => *v,
        }
    }

    pub fn get_slot(&self) -> usize {
        match &self.msg {
            KCensusM(msg) => msg.get_slot(),
            PaxosM(msg) => msg.get_slot(),
            Commit { slot, .. } => *slot,
        }
    }
}
