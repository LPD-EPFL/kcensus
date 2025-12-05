use crate::consensus::paxos_family::message::PaxosMsg::{Accept, ForwardRequest, Prepare};
use crate::consensus::paxos_family::message::RoundV::{EPaxosV, PaxosV};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Serialize, Deserialize, Debug, Default, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct PaxosRound {
    pub round_group: usize,
    pub leader: usize,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum RoundV {
    EPaxosV {
        leader: usize,
        v: usize,
    },
    PaxosV {
        accept_round: Option<PaxosRound>,
        v: usize,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PaxosMsg {
    // Used to propose & forward, but also to freeze and respond to a freeze
    Prepare {
        slot: usize,
        round: PaxosRound,
        rv: RoundV,
    },
    Accept {
        slot: usize,
        round: PaxosRound,
        v: usize,
    },
    ForwardRequest {
        v: usize,
    },
}

impl PaxosRound {
    #[inline]
    pub(crate) fn next_leader_round(&self, my_pid: usize) -> PaxosRound {
        let round_group = if my_pid >= self.leader {
            self.round_group
        } else {
            self.round_group + 1
        };
        PaxosRound {
            round_group,
            leader: my_pid,
        }
    }
}

impl RoundV {
    #[inline]
    pub fn new_paxos_v(accept_round: Option<PaxosRound>, v: usize) -> Self {
        PaxosV { accept_round, v }
    }

    #[inline]
    pub fn new_epaxos_v(leader: usize, v: usize) -> Self {
        EPaxosV { leader, v }
    }

    pub fn get_v(&self) -> usize {
        match self {
            PaxosV { v, .. } => *v,
            EPaxosV { v, .. } => *v,
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
            Prepare { rv, .. } => rv.get_v(),
            Accept { v, .. } => *v,
            ForwardRequest { v } => *v,
        }
    }

    #[inline]
    pub fn get_slot(&self) -> usize {
        match self {
            Prepare { slot, .. } => *slot,
            Accept { slot, .. } => *slot,
            ForwardRequest { .. } => 0,
        }
    }

    #[inline]
    pub fn can_include_value(&self, src: usize) -> bool {
        match self {
            Prepare { round, .. } => round.leader == src,
            Accept { round, .. } => round.leader == src && round.round_group == 0,
            ForwardRequest { .. } => true,
        }
    }

    #[inline]
    pub fn should_include_value(&self) -> bool {
        matches!(self, ForwardRequest { .. })
    }
}

impl Display for PaxosRound {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.round_group, self.leader)
    }
}
