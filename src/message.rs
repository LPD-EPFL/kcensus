use serde::{Deserialize, Serialize};
use crate::kcensus::Value;
use crate::node_state::NodeState;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RoundCommand {
    Spread { remote_states: Vec<NodeState> },
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
    Hello { pid: usize },
    // TODO: Add path-graph to spread messages
    KCensusMessage {
        msg: KCensusMsg,
        value: Option<Value>
    }
}

#[derive(Debug)]
pub struct MsgWithSource {
    pub msg: Message,
    pub src: usize
}

impl Message {
    pub fn with_source(self, src: usize) -> MsgWithSource {
        MsgWithSource {
            msg: self,
            src,
        }
    }
}

#[derive(Debug)]
pub struct KCensusMsgWithSource {
    pub msg: KCensusMsg,
    pub src: usize
}

impl KCensusMsg {
    pub fn with_source(self, src: usize) -> KCensusMsgWithSource {
        KCensusMsgWithSource {
            msg: self,
            src,
        }
    }
}