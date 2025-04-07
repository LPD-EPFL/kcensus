use crate::consensus::paxos_family::message::{PaxosRound, RoundV};
use bit_set::BitSet;

pub struct PaxosRoundState {
    my_pid: usize,
    majority: usize,

    prepared: usize,
    accepted: usize,
    max_rv: Option<RoundV>,

    // Only used for safety checks
    prepared_set: BitSet,
    accepted_set: BitSet,
}

impl PaxosRoundState {
    pub fn new(nb_nodes: usize, my_pid: usize) -> Self {
        let majority = (nb_nodes / 2) + 1;

        Self {
            my_pid,
            majority,

            prepared: 0,
            accepted: 0,
            max_rv: None,

            prepared_set: BitSet::with_capacity(nb_nodes),
            accepted_set: BitSet::with_capacity(nb_nodes),
        }
    }

    #[inline]
    pub fn next_round(&mut self) {
        self.prepared = 0;
        self.accepted = 0;
        self.prepared_set.clear();
        self.accepted_set.clear();
    }

    #[inline]
    pub fn full_clear(&mut self) {
        self.next_round();
        self.max_rv = None;
    }

    #[inline]
    pub fn get_rv(&self) -> Option<RoundV> {
        self.max_rv
    }

    #[inline]
    pub fn get_v(&self) -> Option<usize> {
        self.max_rv.map(|rv| rv.get_v())
    }

    #[inline]
    pub fn get_last_accepted_round(&self) -> Option<PaxosRound> {
        self.max_rv.map(|rv| rv.get_accept_round()).unwrap_or(None)
    }

    #[inline]
    pub fn propose_v(&mut self, rv: RoundV) {
        debug_assert!(self.max_rv.is_none());
        debug_assert!(self.prepared == 0);
        debug_assert!(self.prepared_set.is_empty());
        self.receive_promise(self.my_pid, rv);
    }

    #[inline]
    pub fn receive_promise(&mut self, src: usize, rv: RoundV) {
        if self.max_rv.is_none() || self.get_last_accepted_round() < rv.get_accept_round() {
            self.max_rv = Some(rv);
        }

        let inserted = self.prepared_set.insert(src);
        debug_assert!(inserted);
        self.prepared += 1;
    }

    pub fn adopt_from_epaxos(&mut self, round: PaxosRound, v: usize) {
        self.max_rv = Some(RoundV::new_paxos_v(Some(round), v));
    }

    #[inline]
    pub fn self_accept_v(&mut self, round: PaxosRound) {
        debug_assert!(self.accepted == 0);
        debug_assert!(self.accepted_set.is_empty());
        self.accept_v(self.my_pid, round, self.get_v().unwrap());
    }

    #[inline]
    pub fn accept_v(&mut self, src: usize, round: PaxosRound, v: usize) {
        debug_assert!(self.get_last_accepted_round() < Some(round));
        self.max_rv = Some(RoundV::new_paxos_v(Some(round), v));
        self.receive_accepted(src);
    }

    #[inline]
    pub fn receive_accepted(&mut self, src: usize) {
        let inserted = self.accepted_set.insert(src);
        debug_assert!(inserted);
        self.accepted += 1;
    }

    #[inline]
    pub fn is_prepared(&self) -> bool {
        self.prepared >= self.majority
    }

    #[inline]
    pub fn can_commit(&self) -> bool {
        self.accepted >= self.majority
    }
}
