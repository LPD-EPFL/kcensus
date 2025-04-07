use crate::consensus::paxos_family::message::PaxosMsg::{Accept, Commit, Prepare};
use crate::consensus::paxos_family::message::RoundValue::{EPaxosV, PaxosV};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Serialize, Deserialize, Debug, Default, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct PaxosRound {
    pub round_group: usize,
    pub proposer: usize,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum RoundValue {
    EPaxosV {
        proposer: usize,
        v_uid: usize,
    },
    PaxosV {
        accept_round: Option<PaxosRound>,
        v_uid: usize,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PaxosMsg {
    // Used to propose & forward, but also to freeze and respond to a freeze
    Prepare {
        slot: usize,
        round: PaxosRound,
        round_value: RoundValue,
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

impl PaxosRound {
    #[inline]
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

impl RoundValue {
    #[inline]
    pub fn new_paxos_value(accept_round: Option<PaxosRound>, v_uid: usize) -> Self {
        PaxosV {
            accept_round,
            v_uid,
        }
    }

    #[inline]
    pub fn new_epaxos_value(proposer: usize, v_uid: usize) -> Self {
        EPaxosV { proposer, v_uid }
    }

    pub fn get_v(&self) -> usize {
        match self {
            PaxosV { v_uid, .. } => *v_uid,
            EPaxosV { v_uid, .. } => *v_uid,
        }
    }

    pub fn get_accept_round(&self) -> Option<PaxosRound> {
        match self {
            PaxosV { accept_round, .. } => *accept_round,
            EPaxosV { .. } => None,
        }
    }
}

impl PaxosMsg {
    #[inline]
    pub fn get_v(&self) -> usize {
        match self {
            Prepare { round_value, .. } => round_value.get_v(),
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
            Accept { round, .. } => round.proposer == src && *round == PaxosRound::default(),
            _ => false,
        }
    }
}

impl Display for PaxosRound {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.round_group, self.proposer)
    }
}
