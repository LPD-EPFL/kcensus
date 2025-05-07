use crate::consensus::paxos_family::message::{PaxosRound, RoundV};
use bit_set::BitSet;
use std::collections::HashMap;

pub struct PaxosFamilyRoundState {
    my_pid: usize,
    majority: usize,
    e_paxos_quorum: usize,

    // Paxos / MultiPaxos state
    prepared: usize,
    accepted: usize,
    max_rv: Option<RoundV>,

    // Only used for safety checks
    prepared_set: BitSet,
    accepted_set: BitSet,

    // EPaxos state
    epaxos_answers: usize,
    epaxos_preaccepted: usize,
    epaxos_proposer_to_v: HashMap<usize, usize>,
    epaxos_proposer_scores: Vec<usize>,

    // Only used for safety checks
    epaxos_answer_set: BitSet,
}

impl PaxosFamilyRoundState {
    pub fn new(nb_nodes: usize, my_pid: usize) -> Self {
        let majority = (nb_nodes / 2) + 1;
        let e_paxos_quorum = ((nb_nodes * 3) / 4).max(majority);

        Self {
            my_pid,
            majority,
            e_paxos_quorum,

            prepared: 0,
            accepted: 0,
            max_rv: None,

            prepared_set: BitSet::with_capacity(nb_nodes),
            accepted_set: BitSet::with_capacity(nb_nodes),

            epaxos_answers: 0,
            epaxos_preaccepted: 0,
            epaxos_proposer_to_v: HashMap::with_capacity(nb_nodes),
            epaxos_proposer_scores: vec![2; nb_nodes],

            epaxos_answer_set: BitSet::with_capacity(nb_nodes),
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

        self.epaxos_answers = 0;
        self.epaxos_preaccepted = 0;
        self.epaxos_proposer_to_v.clear();
        self.epaxos_proposer_scores.fill(2);
        self.epaxos_answer_set.clear();
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

        let inserted = self.prepared_set.insert(src);
        debug_assert!(inserted);
        self.prepared += 1;
    }

    pub fn adopt_from_epaxos(&mut self, round: PaxosRound, v: usize) {
        self.max_rv = Some(RoundV::new_paxos_v(None, v));
        self.self_accept_v(round);
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
    pub fn paxos_can_commit(&self) -> bool {
        self.accepted >= self.majority
    }

    #[inline]
    pub fn epaxos_propose_v(&mut self, v: usize) {
        debug_assert!(self.epaxos_proposer_to_v.is_empty());
        debug_assert_eq!(self.epaxos_proposer_scores[self.my_pid], 2);
        debug_assert!(self.epaxos_answer_set.is_empty());
        debug_assert_eq!(self.epaxos_answers, 0);
        debug_assert_eq!(self.epaxos_preaccepted, 0);
        self.epaxos_answered(self.my_pid, self.my_pid, v);
    }

    #[inline]
    pub fn epaxos_answered(&mut self, src: usize, proposer: usize, v: usize) {
        self.epaxos_proposer_to_v.insert(proposer, v);
        if src != proposer {
            self.epaxos_proposer_scores[proposer] += 1;
        } else {
            // proposer was counted as deduced (+2) before
            self.epaxos_proposer_scores[proposer] -= 1;
        }

        if proposer == self.my_pid {
            self.epaxos_preaccepted += 1;
        }
        self.epaxos_answers += 1;
        let inserted = self.epaxos_answer_set.insert(src);
        debug_assert!(inserted);
    }

    #[inline]
    pub fn epaxos_can_commit(&self) -> bool {
        debug_assert!(self.epaxos_proposer_to_v.contains_key(&self.my_pid));
        self.epaxos_proposer_to_v.len() == 1 && self.epaxos_preaccepted >= self.e_paxos_quorum
    }

    // Note: If I receive an answer from a proposer, he will not commit (guaranteed)
    pub fn epaxos_try_adopt(&self) -> Option<usize> {
        debug_assert!(self.epaxos_proposer_to_v.contains_key(&self.my_pid));
        if self.epaxos_answers < self.majority {
            None
        } else if self.epaxos_proposer_to_v.len() < 2 {
            // Don't adopt unless there's more than 1 proposal (for now)
            // TODO: have some form of timeout in case too many died ?
            None
        } else {
            let mut best_score = 0;
            let mut best_v = None;
            for (proposer, v) in self.epaxos_proposer_to_v.iter() {
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
