use crate::consensus::command::Command;
use crate::consensus::deps::message::DepMsg;
use crate::consensus::kcensus::message::KCensusMsg;
use crate::consensus::message::ConsensusMsg::{
    Commit, DepM, KCensusM, PaxosM, ReadRequest, ReadResponse,
};
use crate::consensus::paxos_family::message::PaxosMsg;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ConsensusMsg {
    KCensusM(KCensusMsg),
    PaxosM(PaxosMsg),
    DepM(DepMsg),
    Commit {
        slot: usize,
        v: usize,
    },
    ReadRequest {
        id: ReadId,
    },
    ReadResponse {
        id: ReadId,
        next_readable_slot: usize,
    },
}

/// Identifies a read among those a node issued for one shard. A responder only echoes it
/// back, and a tracker only ever holds the reads its own node issued, so it never has to
/// be unique any wider than that.
#[derive(Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Clone, Copy, Debug)]
pub struct ReadId(pub usize);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum CommandBatch {
    Single(Command),
    Batch { slot: usize, vs: Vec<usize> },
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
            DepM(msg) => msg.includes_value(),
            Commit { .. } => false,
            ReadRequest { .. } => false,
            ReadResponse { .. } => false,
        }
    }

    pub fn should_include_value(&self) -> bool {
        match &self.msg {
            KCensusM(msg) => msg.includes_value(),
            PaxosM(msg) => msg.should_include_value(),
            DepM(msg) => msg.includes_value(),
            Commit { .. } => false,
            ReadRequest { .. } => false,
            ReadResponse { .. } => false,
        }
    }

    pub fn get_v(&self) -> Option<usize> {
        match &self.msg {
            KCensusM(msg) => Some(msg.get_v()),
            PaxosM(msg) => Some(msg.get_v()),
            DepM(msg) => msg.id(),
            Commit { v, .. } => Some(*v),
            ReadRequest { .. } => None,
            ReadResponse { .. } => None,
        }
    }

    pub fn get_slot(&self) -> Option<usize> {
        match &self.msg {
            KCensusM(msg) => msg.get_slot(),
            PaxosM(msg) => msg.get_slot(),
            // Dependency mode has no slots: an instance is identified by its command id.
            DepM(_) => None,
            Commit { slot, .. } => Some(*slot),
            ReadRequest { .. } => None,
            ReadResponse {
                next_readable_slot, ..
            } => Some(*next_readable_slot),
        }
    }
}
