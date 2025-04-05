use crate::consensus::paxos::message::PaxosRound;
use bit_set::BitSet;

#[derive(Copy, Clone, Debug)]
pub struct RoundValue {
    round: PaxosRound,
    v_uid: usize,
}

pub struct PaxosRoundState {
    nb_nodes: usize,
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

        let mut x = Self {
            nb_nodes,
            my_pid,
            majority,

            prepared: 1,
            accepted: 1,
            max_round_value: None,

            prepared_set: BitSet::with_capacity(nb_nodes),
            accepted_set: BitSet::with_capacity(nb_nodes),
        };
        x.full_clear();
        x
    }

    #[inline]
    pub fn next_round(&mut self) {
        self.prepared = 1;
        self.accepted = 1;
        self.prepared_set.clear();
        self.accepted_set.clear();
        self.prepared_set.insert(self.my_pid);
        self.accepted_set.insert(self.my_pid);
    }

    #[inline]
    pub fn full_clear(&mut self) {
        self.next_round();
        self.max_round_value = None;
    }

    #[inline]
    pub fn get_v(&self) -> Option<usize> {
        self.max_round_value.map(|round_value| round_value.v_uid)
    }

    #[inline]
    pub fn propose_v(&mut self, v_uid: RoundValue) {
        debug_assert!(self.max_round_value.is_none());
        self.max_round_value = Some(v_uid);
    }

    #[inline]
    pub fn receive_promise(&mut self, src: usize, round_value: Option<RoundValue>) {
        let inserted = self.prepared_set.insert(src);
        debug_assert!(inserted);
        self.prepared += 1;
        if let Some(round_value) = round_value {
            if self.max_round_value.unwrap_or(round_value).round <= round_value.round {
                self.max_round_value = Some(round_value);
            }
        }
    }

    #[inline]
    pub fn receive_accepted(&mut self, src: usize) {
        let inserted = self.accepted_set.insert(src);
        debug_assert!(inserted);
        self.accepted += 1;
    }

    #[inline]
    pub fn can_commit(&mut self) -> bool {
        self.accepted >= self.majority
    }
}
