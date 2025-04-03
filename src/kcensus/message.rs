use crate::kcensus::message::RoundCommand::{Commit, Spread, SpreadValueOnly};
use crate::kcensus::node_state::NodeState;
use crate::kcensus::propagation::MessageId;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RoundCommand {
    // Used to propose & forward, but also to freeze and respond to a freeze
    Spread {
        msg_id: Option<MessageId>,
        remote_states: Vec<NodeState>,
        value_spreading: bool,
    },
    SpreadValueOnly {
        msg_id: MessageId,
        value_uid: usize,
    },
    Commit {
        value_uid: usize,
    },
}

impl RoundCommand {
    pub fn message_v_uid(&self, src: usize) -> usize {
        match self {
            Spread {
                msg_id,
                remote_states,
                ..
            } => {
                let v_uid = remote_states[src].v_uid;
                if let Some(msg_id) = msg_id {
                    debug_assert_eq!(
                        remote_states[src].v_uid,
                        remote_states[msg_id.proposer].v_uid
                    )
                }
                v_uid.expect("v_uid of src should not be None")
            }
            SpreadValueOnly { value_uid, .. } => *value_uid,
            Commit { value_uid, .. } => *value_uid,
        }
    }
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

    pub fn message_v_uid(&self, src: usize) -> usize {
        self.command.message_v_uid(src)
    }
}
