use crate::consensus::message::ConsensusMsg::{KCensusM, PaxosM};
use crate::kcensus::message::KCensusMsg;
use crate::paxos::message::PaxosMsg;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ConsensusMsg {
    KCensusM(KCensusMsg),
    PaxosM(PaxosMsg),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConsensusMessage {
    pub msg: ConsensusMsg,
    pub src: usize,
}

impl ConsensusMessage {
    pub fn can_include_value(&self) -> bool {
        match &self.msg {
            KCensusM(msg) => msg.should_include_value(),
            PaxosM(msg) => msg.can_include_value(self.src),
        }
    }

    pub fn should_include_value(&self) -> bool {
        match &self.msg {
            KCensusM(msg) => msg.should_include_value(),
            PaxosM(_) => false,
        }
    }

    pub fn get_v(&self) -> usize {
        match &self.msg {
            KCensusM(msg) => msg.get_v(self.src),
            PaxosM(msg) => msg.get_v(),
        }
    }

    pub fn get_slot(&self) -> usize {
        match &self.msg {
            KCensusM(msg) => msg.get_slot(),
            PaxosM(msg) => msg.get_slot(),
        }
    }
}
