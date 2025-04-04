use crate::kcensus::message::KCensusMsg::{Commit, Spread, SpreadValueOnly};
use crate::kcensus::node_state::NodeState;
use crate::kcensus::propagation::MessageId;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KCensusMsg {
    // Used to propose & forward, but also to freeze and respond to a freeze
    Spread {
        slot: usize,
        round: usize,
        msg_id: Option<MessageId>,
        remote_states: Vec<NodeState>,
        with_value: bool,
    },
    Commit {
        slot: usize,
        value_uid: usize,
    },
    SpreadValueOnly {
        msg_id: MessageId,
        value_uid: usize,
    },
}

#[derive(Debug)]
pub struct KCensusMsgWithSource {
    pub msg: KCensusMsg,
    pub src: usize,
}

impl KCensusMsg {
    #[inline]
    pub fn with_source(self, src: usize) -> KCensusMsgWithSource {
        KCensusMsgWithSource { msg: self, src }
    }

    #[inline]
    pub fn get_v(&self, src: usize) -> usize {
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
            Commit { value_uid, .. } => *value_uid,
            SpreadValueOnly { value_uid, .. } => *value_uid,
        }
    }

    #[inline]
    pub fn get_slot(&self) -> usize {
        match self {
            Spread { slot, .. } => *slot,
            Commit { slot, .. } => *slot,
            SpreadValueOnly { .. } => 0,
        }
    }

    #[inline]
    pub fn should_include_value(&self) -> bool {
        match self {
            Spread {
                msg_id, with_value, ..
            } => {
                debug_assert!(!with_value || msg_id.is_some());
                *with_value
            }
            Commit { .. } => false,
            SpreadValueOnly { .. } => true,
        }
    }
}
