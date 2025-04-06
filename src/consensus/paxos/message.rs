use crate::consensus::paxos::message::PaxosMsg::{Accept, Commit, Prepare};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Serialize, Deserialize, Debug, Default, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct PaxosRound {
    pub round_group: usize,
    pub proposer: usize,
}

impl PaxosRound {
    pub(crate) fn next_proposer_round(&self, my_pid: usize) -> PaxosRound {
        let round_group = if my_pid >= self.proposer {
            self.round_group
        } else {
            self.round_group + 1
        };
        PaxosRound {
            round_group,
            proposer: my_pid,
        }
    }
}

impl Display for PaxosRound {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.round_group, self.proposer)
    }
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

impl PaxosMsg {
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
    pub fn can_include_value(&self, src: usize) -> bool {
        match self {
            Prepare { round, .. } => round.proposer == src,
            Accept { .. } => false,
            Commit { .. } => false,
        }
    }
}
