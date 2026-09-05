use crate::consensus::kcensus::message::KCensusMsg::{PaxosAccept, Spread, SpreadValueOnly};
use crate::consensus::kcensus::node_state::NodeState;
use crate::consensus::kcensus::propagation::MessageId;
use serde::{Deserialize, Serialize};

/// The paper's Algorithm 2 has four messages; this has two.
///
/// `Accept&Spread`, `DoAdopt`, `Freeze` and `Frozen` all ride in `Spread`, on the
/// propagation graph that spreads knowledge anyway, so none of them costs a round trip of
/// its own. A second proposer among the states it carries is what tells a proposer to adopt
/// and a process to freeze; and a state that is *already* frozen — its last knowledge state
/// reached, or the conflict seen — is that process's `Frozen` report.
///
/// What goes unsaid rather than merged: a process outside the fast quorum, or one already
/// frozen, is never asked to freeze, since a majority of reports is enough; and a process
/// not known to propose is never told to adopt, since it learns from its own quorum if it
/// does not fast-commit. Asking either of them needs a timeout, which is not implemented:
/// it is what would cover a failure rather than a conflict.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KCensusMsg {
    // Used to propose & forward, but also to freeze and respond to a freeze
    Spread {
        slot: usize,
        v: usize,
        msg_id: MessageId,
        /// The census as the sender saw it. `None` while the sender has no conflict: the
        /// propagation graph then says what everyone knew, so the receiver rebuilds it
        /// (`build_remote_state`) instead of paying for it on the wire.
        remote_states: Option<Vec<NodeState>>,
        with_value: bool,
        new_value: bool,
    },
    SpreadValueOnly {
        v: usize,
        msg_id: MessageId,
    },
    /// The fallback's accept phase, in both directions: the leader broadcasts it, and a
    /// replica that votes echoes it back. Its `leader` doubles as the Paxos ballot.
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
    pub fn get_slot(&self) -> Option<usize> {
        match self {
            Spread { slot, .. } => Some(*slot),
            SpreadValueOnly { .. } => None,
            PaxosAccept { slot, .. } => Some(*slot),
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
