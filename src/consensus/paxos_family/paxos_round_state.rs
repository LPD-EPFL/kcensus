use crate::consensus::paxos_family::message::{PaxosRound, RoundValue};
use bit_set::BitSet;

pub struct PaxosRoundState {
    my_pid: usize,
    majority: usize,

    prepared: usize,
    accepted: usize,
    max_round_value: Option<RoundValue>,

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
            max_round_value: None,

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
        self.max_round_value = None;
    }

    #[inline]
    pub fn get_round_value(&self) -> Option<RoundValue> {
        self.max_round_value
    }

    #[inline]
    pub fn get_v(&self) -> Option<usize> {
        self.max_round_value.map(|round_value| round_value.get_v())
    }

    #[inline]
    pub fn get_last_accepted_round(&self) -> Option<PaxosRound> {
        self.max_round_value
            .map(|rv| rv.get_accept_round())
            .unwrap_or(None)
    }

    #[inline]
    pub fn propose_v(&mut self, v_uid: RoundValue) {
        debug_assert!(self.max_round_value.is_none());
        debug_assert!(self.prepared == 0);
        debug_assert!(self.prepared_set.is_empty());
        self.receive_promise(self.my_pid, v_uid);
    }

    #[inline]
    pub fn receive_promise(&mut self, src: usize, round_value: RoundValue) {
        if self.max_round_value.is_none()
            || self.get_last_accepted_round() < round_value.get_accept_round()
        {
            self.max_round_value = Some(round_value);
        }

        let inserted = self.prepared_set.insert(src);
        debug_assert!(inserted);
        self.prepared += 1;
    }

    pub fn adopt_from_epaxos(&mut self, round: PaxosRound, v_uid: usize) {
        self.max_round_value = Some(RoundValue::new_paxos_value(Some(round), v_uid));
    }

    #[inline]
    pub fn self_accept_v(&mut self, round: PaxosRound) {
        debug_assert!(self.accepted == 0);
        debug_assert!(self.accepted_set.is_empty());
        self.accept_v(self.my_pid, round, self.get_v().unwrap());
    }

    #[inline]
    pub fn accept_v(&mut self, src: usize, round: PaxosRound, v_uid: usize) {
        debug_assert!(self.get_last_accepted_round() < Some(round));
        self.max_round_value = Some(RoundValue::new_paxos_value(Some(round), v_uid));
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
