use crate::consensus::kcensus::message::KCensusMsg::{PaxosAccept, Spread, SpreadValueOnly};
use crate::consensus::kcensus::node_state::NodeState;
use crate::consensus::kcensus::propagation::MessageId;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KCensusMsg {
    // Used to propose & forward, but also to freeze and respond to a freeze
    Spread {
        slot: usize,
        v: usize,
        msg_id: MessageId,
        remote_states: Option<Vec<NodeState>>,
        with_value: bool,
        new_value: bool,
    },
    SpreadValueOnly {
        v: usize,
        msg_id: MessageId,
    },
    PaxosAccept {
        slot: usize,
        leader: usize,
        v: usize,
        new_value: bool,
    },
}

impl KCensusMsg {
    #[inline]
    pub fn get_v(&self) -> usize {
        match self {
            Spread { v, .. } => *v,
            SpreadValueOnly { v, .. } => *v,
            PaxosAccept { v, .. } => *v,
        }
    }

    #[inline]
    pub fn get_slot(&self) -> usize {
        match self {
            Spread { slot, .. } => *slot,
            SpreadValueOnly { .. } => 0,
            PaxosAccept { slot, .. } => *slot,
        }
    }

    #[inline]
    pub fn includes_value(&self) -> bool {
        match self {
            Spread { with_value, .. } => *with_value,
            SpreadValueOnly { .. } => true,
            PaxosAccept { new_value, .. } => *new_value,
        }
    }
}
