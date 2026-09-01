use crate::consensus::paxos_family::message::{PaxosRound, RoundV};
use bit_set::BitSet;
use std::collections::HashMap;

pub struct PaxosFamilyRoundState {
    my_pid: usize,
    replica: bool,
    majority: usize,
    e_paxos_quorum: usize,
    fast_paxos_quorum: usize,

    pub round: Option<PaxosRound>,

    // Paxos / MultiPaxos state
    prepared_set: BitSet,
    accepted_set: BitSet,
    prepared: usize,
    accepted: usize,
    max_rv: Option<RoundV>,

    // EPaxos & SwiftPaxos state
    fast_answer_set: BitSet,
    fast_answers: usize,
    fast_preaccepted: usize,
    fast_proposer_to_v: HashMap<usize, usize>,

    epaxos_proposer_scores: Vec<usize>,
    _swift_proposer_scores: Vec<usize>,
    swift_leader_answer: Option<usize>,
}

impl PaxosFamilyRoundState {
    pub fn new(
        my_pid: usize,
        replica: bool,
        process_count: usize,
        replica_count: usize,
        starting_round: Option<PaxosRound>,
    ) -> Self {
        let majority = (replica_count / 2) + 1;
        let e_paxos_quorum = ((replica_count * 3 - 1) / 4).max(majority);
        let fast_paxos_quorum = ((replica_count * 3 - 1) / 4 + 1).max(majority);

        Self {
            my_pid,
            replica,
            majority,
            e_paxos_quorum,
            fast_paxos_quorum,

            round: starting_round,

            prepared_set: BitSet::with_capacity(process_count),
            accepted_set: BitSet::with_capacity(process_count),
            prepared: 0,
            accepted: 0,
            max_rv: None,

            fast_answer_set: BitSet::with_capacity(process_count),
            fast_answers: 0,
            fast_preaccepted: 0,
            fast_proposer_to_v: HashMap::with_capacity(process_count),
            epaxos_proposer_scores: vec![2; process_count],
            _swift_proposer_scores: vec![0; process_count],
            swift_leader_answer: None,
        }
    }

    #[inline]
    pub fn next_round(&mut self) {
        self.prepared_set.clear();
        self.accepted_set.clear();
        self.prepared = 0;
        self.accepted = 0;
    }

    #[inline]
    pub fn full_clear(&mut self) {
        self.next_round();
        self.max_rv = None;

        self.fast_answer_set.clear();
        self.fast_answers = 0;
        self.fast_preaccepted = 0;
        self.fast_proposer_to_v.clear();
        self.epaxos_proposer_scores.fill(2);
        self._swift_proposer_scores.fill(0);
        self.swift_leader_answer = None;
    }

    /// True if no trace of a round is left, i.e. this is the state `new` and
    /// `full_clear` (followed by `goto_round(starting_round)`) produce.
    pub fn is_clear(&self, starting_round: Option<PaxosRound>) -> bool {
        self.round == starting_round
            && self.max_rv.is_none()
            && self.prepared == 0
            && self.accepted == 0
            && self.prepared_set.is_empty()
            && self.accepted_set.is_empty()
            && self.fast_answers == 0
            && self.fast_preaccepted == 0
            && self.fast_answer_set.is_empty()
            && self.fast_proposer_to_v.is_empty()
            && self.swift_leader_answer.is_none()
            && self.epaxos_proposer_scores.iter().all(|score| *score == 2)
            && self._swift_proposer_scores.iter().all(|score| *score == 0)
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
    pub fn paxos_propose_v(&mut self, rv: RoundV) {
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

        if src != self.my_pid && self.prepared_set.insert(src) {
            self.prepared += 1;
        }
        if self.replica && self.prepared_set.insert(self.my_pid) {
            self.prepared += 1;
        }
        debug_assert_eq!(self.prepared_set.len(), self.prepared);
    }

    pub fn adopt_from_epaxos(&mut self, round: PaxosRound, v: usize) {
        self.max_rv = Some(RoundV::new_paxos_v(None, v));
        self.self_accept(round);
    }

    #[inline]
    pub fn self_accept(&mut self, round: PaxosRound) {
        assert_eq!(self.accepted, 0);
        debug_assert!(self.accepted_set.is_empty());
        self.receive_accept(self.my_pid, round, self.get_v().unwrap());
    }

    #[inline]
    pub fn receive_accept(&mut self, src: usize, round: PaxosRound, v: usize) {
        let rv = Some(RoundV::new_paxos_v(Some(round), v));
        if self.get_last_accepted_round() < Some(round) {
            self.max_rv = rv;
        } else {
            assert_eq!(self.max_rv, rv);
        }

        if src != self.my_pid && self.accepted_set.insert(src) {
            self.accepted += 1;
        }
        if self.replica && self.accepted_set.insert(self.my_pid) {
            self.accepted += 1;
        }
        debug_assert_eq!(self.accepted_set.len(), self.accepted);
    }

    #[inline]
    pub fn is_prepared(&self) -> bool {
        self.prepared >= self.majority
    }

    #[inline]
    pub fn paxos_can_commit(&self) -> bool {
        self.accepted >= self.majority
    }

    #[inline]
    pub fn epaxos_propose_v(&mut self, v: usize) {
        debug_assert!(self.fast_answer_set.is_empty());
        debug_assert_eq!(self.fast_answers, 0);
        debug_assert_eq!(self.fast_preaccepted, 0);
        debug_assert!(self.fast_proposer_to_v.is_empty());
        debug_assert_eq!(self.epaxos_proposer_scores[self.my_pid], 2);
        self.epaxos_answered(self.my_pid, self.my_pid, v);
    }

    pub fn swift_propose_v(&mut self, v: usize, fixed_quorum: &Option<BitSet>, leader: bool) {
        debug_assert!(self.fast_answer_set.is_empty());
        debug_assert_eq!(self.fast_answers, 0);
        debug_assert_eq!(self.fast_preaccepted, 0);
        debug_assert!(self.fast_proposer_to_v.is_empty());
        debug_assert_eq!(self._swift_proposer_scores[self.my_pid], 0);
        debug_assert_eq!(self.swift_leader_answer, None);
        if self.replica {
            self.swift_answered(self.my_pid, self.my_pid, v, fixed_quorum, leader);
        }
    }

    #[inline]
    pub fn epaxos_answered(&mut self, src: usize, proposer: usize, v: usize) {
        if self.fast_answer_set.insert(src) {
            self.fast_proposer_to_v.insert(proposer, v);

            self.fast_answers += 1;
            if proposer == self.my_pid {
                self.fast_preaccepted += 1;
            }

            if src != proposer {
                self.epaxos_proposer_scores[proposer] += 1;
            } else {
                // leader was counted as deduced (+2) before
                self.epaxos_proposer_scores[proposer] -= 1;
            }
        } else if src != self.my_pid {
            unreachable!("Should never receive two preaccept answers from the same process")
        }
    }

    pub fn swift_answered(
        &mut self,
        src: usize,
        proposer: usize,
        v: usize,
        fixed_quorum: &Option<BitSet>,
        leader: bool,
    ) {
        if self.fast_answer_set.insert(src) {
            self.fast_proposer_to_v.insert(proposer, v);

            if let Some(fixed_quorum) = fixed_quorum {
                if !fixed_quorum.contains(src) {
                    assert!(!leader);
                    return;
                }
            }

            self.fast_answers += 1;
            if proposer == self.my_pid {
                self.fast_preaccepted += 1;
            }

            self._swift_proposer_scores[proposer] += 1;
            if leader {
                self.swift_leader_answer = Some(v);
            }
        } else if src != self.my_pid {
            unreachable!("Should never receive two preaccept answers from the same process")
        }
    }

    #[inline]
    pub fn epaxos_can_commit(&self) -> bool {
        self.fast_proposer_to_v.len() == 1 && self.fast_preaccepted >= self.e_paxos_quorum
    }

    pub fn swift_can_commit(&self, fixed_quorum: bool) -> bool {
        let quorum_required = if fixed_quorum {
            self.majority
        } else {
            self.fast_paxos_quorum
        };
        self.fast_proposer_to_v.len() == 1
            && matches!(self.swift_leader_answer, Some(_))
            && self.fast_preaccepted >= quorum_required
    }

    // Note: If I receive an answer from a leader, he will not commit (guaranteed)
    pub fn epaxos_try_adopt(&self) -> Option<usize> {
        debug_assert!(self.fast_proposer_to_v.contains_key(&self.my_pid));
        if self.fast_answers < self.majority {
            None
        } else if self.fast_proposer_to_v.len() < 2 {
            // Don't adopt unless there's more than 1 proposal (for now)
            // TODO: have some form of timeout in case too many died ?
            None
        } else {
            let mut best_score = 0;
            let mut best_v = None;
            for (proposer, v) in self.fast_proposer_to_v.iter() {
                let score = self.epaxos_proposer_scores[*proposer];
                if score > best_score {
                    best_v = Some(*v);
                    best_score = score;
                }
            }
            best_v
        }
    }
}
