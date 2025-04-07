use crate::consensus::kcensus::message::KCensusMsg::{Spread, SpreadValueOnly};
use crate::consensus::kcensus::node_state::NodeState;
use crate::consensus::kcensus::propagation::MessageId;
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
    SpreadValueOnly {
        msg_id: MessageId,
        v: usize,
    },
}

impl KCensusMsg {
    #[inline]
    pub fn get_v(&self, src: usize) -> usize {
        match self {
            Spread {
                msg_id,
                remote_states,
                ..
            } => {
                let v = remote_states[src].v;
                if let Some(msg_id) = msg_id {
                    debug_assert_eq!(remote_states[src].v, remote_states[msg_id.proposer].v)
                }
                v.expect("v of src should not be None")
            }
            SpreadValueOnly { v, .. } => *v,
        }
    }

    #[inline]
    pub fn get_slot(&self) -> usize {
        match self {
            Spread { slot, .. } => *slot,
            SpreadValueOnly { .. } => 0,
        }
    }

    #[inline]
    pub fn includes_value(&self) -> bool {
        match self {
            Spread { with_value, .. } => *with_value,
            SpreadValueOnly { .. } => true,
        }
    }
}
