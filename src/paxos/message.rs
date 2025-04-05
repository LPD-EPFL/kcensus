use crate::paxos::message::PaxosMsg::{Accept, Commit, Prepare};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Default, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct PaxosRound {
    round_group: usize,
    proposer: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PaxosMsg {
    // Used to propose & forward, but also to freeze and respond to a freeze
    Prepare {
        slot: usize,
        round: PaxosRound,
        value_uid: usize,
    },
    Accept {
        slot: usize,
        round: PaxosRound,
        value_uid: usize,
    },
    Commit {
        slot: usize,
        value_uid: usize,
    },
}

#[derive(Debug)]
pub struct PaxosMsgWithSource {
    pub msg: PaxosMsg,
    pub src: usize,
}

impl PaxosMsg {
    #[inline]
    pub fn with_source(self, src: usize) -> PaxosMsgWithSource {
        PaxosMsgWithSource { msg: self, src }
    }

    #[inline]
    pub fn get_v(&self) -> usize {
        match self {
            Prepare { value_uid, .. } => *value_uid,
            Accept { value_uid, .. } => *value_uid,
            Commit { value_uid, .. } => *value_uid,
        }
    }

    #[inline]
    pub fn get_slot(&self) -> usize {
        match self {
            Prepare { slot, .. } => *slot,
            Accept { slot, .. } => *slot,
            Commit { slot, .. } => *slot,
        }
    }

    #[inline]
    pub fn might_include_value(&self, src: usize) -> bool {
        match self {
            Prepare { round, .. } => round.proposer == src,
            Accept { .. } => false,
            Commit { .. } => false,
        }
    }
}
