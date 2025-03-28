use crate::kcensus::KVal;
use crate::node_state::NodeState;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RoundCommand {
    Spread {
        remote_states: Vec<NodeState>,
        step: usize,
    },
    Commit,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KCensusMsg {
    pub slot: usize,
    pub round: usize,
    pub value_uid: usize,
    pub command: RoundCommand,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Message {
    Hello {
        pid: usize,
    },
    // TODO: Add path-graph to spread messages
    KCensusMessage {
        msg: KCensusMsg,
        value: Option<KVal>,
    },
    Done,
}

#[derive(Debug)]
pub struct MsgWithSource {
    pub msg: Message,
    pub src: usize,
}

impl Message {
    pub fn with_source(self, src: usize) -> MsgWithSource {
        MsgWithSource { msg: self, src }
    }
}

pub struct MsgWithDeadline {
    pub msg: MsgWithSource,
    pub deadline: Instant,
}

impl MsgWithSource {
    pub fn with_deadline(self, deadline: Instant) -> MsgWithDeadline {
        MsgWithDeadline {
            msg: self,
            deadline,
        }
    }
}

#[derive(Debug)]
pub struct KCensusMsgWithSource {
    pub msg: KCensusMsg,
    pub src: usize,
}

impl KCensusMsg {
    pub fn with_source(self, src: usize) -> KCensusMsgWithSource {
        KCensusMsgWithSource { msg: self, src }
    }
}
