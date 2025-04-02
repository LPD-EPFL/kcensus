use crate::kcensus::node_state::NodeState;
use crate::kcensus::propagation::MessageId;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RoundCommand {
    // Used to propose & forward, but also to freeze and respond to a freeze
    Spread {
        id: Option<MessageId>,
        remote_states: Vec<NodeState>,
    },
    Commit {
        value_uid: usize,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KCensusMsg {
    pub slot: usize,
    pub round: usize,
    pub command: RoundCommand,
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
