use crate::consensus::command::Command;
use crate::consensus::kcensus::message::KCensusMsg;
use crate::consensus::message::ConsensusMsg::{
    Commit, KCensusM, PaxosM, ReadRequest, ReadResponse,
};
use crate::consensus::paxos_family::message::PaxosMsg;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ConsensusMsg {
    KCensusM(KCensusMsg),
    PaxosM(PaxosMsg),
    Commit {
        slot: usize,
        v: usize,
    },
    ReadRequest {
        uid: ReadUid,
    },
    ReadResponse {
        uid: ReadUid,
        next_readable_slot: usize,
    },
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Clone, Copy, Debug)]
pub struct ReadUid {
    pub reader: usize,
    pub id: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum CommandBatch {
    Single(Command),
    Batch(Vec<usize>),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConsensusMessage {
    pub msg: ConsensusMsg,
    pub src: usize,
    pub last_v: Option<usize>,
}

impl ConsensusMessage {
    pub fn can_include_value(&self) -> bool {
        match &self.msg {
            KCensusM(msg) => msg.includes_value(),
            PaxosM(msg) => msg.can_include_value(self.src),
            Commit { .. } => false,
            ReadRequest { .. } => false,
            ReadResponse { .. } => false,
        }
    }

    pub fn should_include_value(&self) -> bool {
        match &self.msg {
            KCensusM(msg) => msg.includes_value(),
            PaxosM(msg) => msg.should_include_value(),
            Commit { .. } => false,
            ReadRequest { .. } => false,
            ReadResponse { .. } => false,
        }
    }

    pub fn get_v(&self) -> Option<usize> {
        match &self.msg {
            KCensusM(msg) => Some(msg.get_v()),
            PaxosM(msg) => Some(msg.get_v()),
            Commit { v, .. } => Some(*v),
            ReadRequest { .. } => None,
            ReadResponse { .. } => None,
        }
    }

    pub fn get_slot(&self) -> Option<usize> {
        match &self.msg {
            KCensusM(msg) => msg.get_slot(),
            PaxosM(msg) => msg.get_slot(),
            Commit { slot, .. } => Some(*slot),
            ReadRequest { .. } => None,
            ReadResponse {
                next_readable_slot, ..
            } => Some(*next_readable_slot),
        }
    }
}
