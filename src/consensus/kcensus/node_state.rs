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
    v_proposer: Option<usize>,
    v_state_nanos: u64,
    frozen_and_prepared: Option<usize>,
    paxos_accept_round: Option<usize>,
}

impl PartialOrd<Self> for NodeState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        assert_eq!(self.id, other.id);
        let accept_ord = self.paxos_accept_round.cmp(&other.paxos_accept_round);
        let prepare_ord = self.prepared_for().cmp(&other.prepared_for());
        if accept_ord != Ordering::Equal {
            assert!(prepare_ord == accept_ord || prepare_ord == Ordering::Equal);
            return Some(accept_ord);
        } else if prepare_ord != Ordering::Equal {
            return Some(prepare_ord);
        }

        let state_ord = self.v_state_nanos.cmp(&other.v_state_nanos);
        if state_ord != Ordering::Equal {
            return Some(state_ord);
        }

        if self.v.is_some() == other.v.is_some() {
            assert_eq!(self, other);
            return Some(Ordering::Equal);
        }

        let v_ord = self.v.is_some().cmp(&other.v.is_some());
        let prop_ord = self.v_proposer.is_some().cmp(&other.v_proposer.is_some());
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
            v_proposer: None,
            v_state_nanos: 0,
            frozen_and_prepared: None,
            paxos_accept_round: None,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.v = None;
        self.v_proposer = None;
        self.v_state_nanos = 0;
        self.frozen_and_prepared = None;
        self.paxos_accept_round = None;
    }

    pub fn get_v(&self) -> Option<usize> {
        self.v
    }

    pub fn get_proposer(&self) -> Option<usize> {
        self.v_proposer
    }

    pub fn get_state_id(&self) -> Duration {
        Duration::from_nanos(self.v_state_nanos)
    }

    pub fn is_frozen(&self) -> bool {
        self.frozen_and_prepared.is_some()
    }

    pub fn prepared_for(&self) -> Option<usize> {
        self.frozen_and_prepared
    }

    pub fn get_paxos_accept_round(&self) -> Option<usize> {
        self.paxos_accept_round
    }

    pub fn accept_with_state(
        &mut self,
        v: usize,
        proposer: usize,
        duration: Duration,
        graph: &PropagationGraphs,
    ) {
        assert!(self.v.is_none());
        assert!(self.v_proposer.is_none());
        assert_eq!(self.v_state_nanos, 0);
        self.v = Some(v);
        self.v_proposer = Some(proposer);
        self.update_state(duration, graph);
    }

    pub fn update_state(&mut self, duration: Duration, graph: &PropagationGraphs) {
        assert!(!self.is_frozen());
        self.v_state_nanos = duration.as_nanos() as u64;
        let proposer = self.v_proposer.unwrap();
        if graph.get_frozen(proposer, self.id, duration) {
            self.frozen_and_prepared = Some(graph.get_leader(proposer));
        }
    }

    pub fn freeze_and_prepare(&mut self, leader: usize) {
        if self.frozen_and_prepared < Some(leader) {
            self.frozen_and_prepared = Some(leader);
        }
    }

    pub fn paxos_accept(&mut self, leader: usize, v: usize) {
        assert_eq!(self.frozen_and_prepared, Some(leader));
        assert!(self.paxos_accept_round < Some(leader));
        self.v = Some(v);
        self.paxos_accept_round = Some(leader);

        self.v_proposer = None;
        self.v_state_nanos = 0;
    }
}
