use crate::consensus::kcensus::propagation::PropagationGraphs;
use bit_set::BitSet;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::time::Duration;

pub type Knowledge = BitSet;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct NodeState {
    id: usize,
    v: Option<usize>,
    k_proposer: Option<usize>,
    k_state_nanos: u64,
    prepared_for: Option<usize>,
    paxos_accept_round: Option<usize>,
}

impl PartialOrd<Self> for NodeState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        assert_eq!(self.id, other.id);
        let accept_ord = self.paxos_accept_round.cmp(&other.paxos_accept_round);
        let prepare_ord = self.prepared_for.cmp(&other.prepared_for);
        if accept_ord != Ordering::Equal {
            assert!(prepare_ord == accept_ord || prepare_ord == Ordering::Equal);
            return Some(accept_ord);
        }

        if prepare_ord != Ordering::Equal {
            return Some(prepare_ord);
        }

        let state_ord = self.k_state_nanos.cmp(&other.k_state_nanos);
        if state_ord != Ordering::Equal {
            return Some(state_ord);
        }

        if self.v.is_some() == other.v.is_some() {
            assert_eq!(self, other);
            return Some(Ordering::Equal);
        }

        let v_ord = self.v.is_some().cmp(&other.v.is_some());
        let prop_ord = self.k_proposer.is_some().cmp(&other.k_proposer.is_some());
        assert_eq!(v_ord, prop_ord);
        Some(v_ord)
    }
}

impl NodeState {
    #[inline]
    pub fn new(id: usize) -> Self {
        Self {
            id,
            v: None,
            k_proposer: None,
            k_state_nanos: 0,
            prepared_for: None,
            paxos_accept_round: None,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.v = None;
        self.k_proposer = None;
        self.k_state_nanos = 0;
        self.prepared_for = None;
        self.paxos_accept_round = None;
    }

    /// True if this is the state of a node that has not taken part in the current slot.
    #[inline]
    pub fn is_clear(&self) -> bool {
        self.v.is_none()
            && self.k_proposer.is_none()
            && self.k_state_nanos == 0
            && self.prepared_for.is_none()
            && self.paxos_accept_round.is_none()
    }

    #[inline]
    pub fn get_v(&self) -> Option<usize> {
        self.v
    }

    #[inline]
    pub fn get_k_proposer(&self) -> Option<usize> {
        self.k_proposer
    }

    #[inline]
    pub fn get_k_state_id(&self) -> Duration {
        Duration::from_nanos(self.k_state_nanos)
    }

    #[inline]
    pub fn is_frozen(&self) -> bool {
        self.prepared_for.is_some()
    }

    #[inline]
    pub fn get_prepared_for(&self) -> Option<usize> {
        self.prepared_for
    }

    #[inline]
    pub fn get_paxos_accept_round(&self) -> Option<usize> {
        self.paxos_accept_round
    }

    #[inline]
    pub fn accept_with_k_state(
        &mut self,
        v: usize,
        proposer: usize,
        duration: Duration,
        graph: &PropagationGraphs,
    ) {
        assert!(self.v.is_none());
        assert!(self.k_proposer.is_none());
        assert_eq!(self.k_state_nanos, 0);
        self.v = Some(v);
        self.k_proposer = Some(proposer);
        self.update_k_state(duration, graph);
    }

    #[inline]
    pub fn update_k_state(&mut self, duration: Duration, graph: &PropagationGraphs) {
        assert!(!self.is_frozen());
        self.k_state_nanos = duration.as_nanos() as u64;
        let proposer = self.k_proposer.unwrap();
        if graph.final_state(proposer, self.id, duration) {
            self.prepared_for = Some(graph.get_leader(proposer));
        }
    }

    #[inline]
    pub fn prepare_for(&mut self, leader: usize) {
        if self.prepared_for < Some(leader) {
            self.prepared_for = Some(leader);
        }
    }

    #[inline]
    pub fn paxos_accept(&mut self, leader: usize, v: usize) -> bool {
        self.prepare_for(leader);
        if self.prepared_for == Some(leader) && self.paxos_accept_round < Some(leader) {
            self.v = Some(v);
            self.paxos_accept_round = Some(leader);

            self.k_proposer = None;
            self.k_state_nanos = 0;
            return true;
        }
        false
    }
}
