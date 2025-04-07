use bit_set::BitSet;
use std::collections::HashMap;

pub struct EPaxosRoundState {
    my_pid: usize,
    majority: usize,
    e_paxos_quorum: usize,

    answers: usize,
    accepted: usize,
    proposer_to_value: HashMap<usize, usize>,
    proposer_scores: Vec<usize>,

    // Only used for safety checks
    answer_set: BitSet,
}

impl EPaxosRoundState {
    pub fn new(nb_nodes: usize, my_pid: usize) -> Self {
        let majority = (nb_nodes / 2) + 1;
        let e_paxos_quorum = ((nb_nodes * 3) / 4).max(majority);

        Self {
            my_pid,
            majority,
            e_paxos_quorum,

            answers: 0,
            accepted: 0,
            proposer_to_value: HashMap::with_capacity(nb_nodes),
            proposer_scores: vec![2; nb_nodes],

            answer_set: BitSet::with_capacity(nb_nodes),
        }
    }

    #[inline]
    pub fn full_clear(&mut self) {
        self.answers = 0;
        self.accepted = 0;
        self.proposer_to_value.clear();
        self.proposer_scores.fill(2);
        self.answer_set.clear();
    }

    #[inline]
    pub fn propose_v(&mut self, v_uid: usize) {
        debug_assert!(self.proposer_to_value.is_empty());
        debug_assert_eq!(self.proposer_scores[self.my_pid], 1);
        debug_assert!(self.answer_set.is_empty());
        debug_assert_eq!(self.answers, 0);
        debug_assert_eq!(self.accepted, 0);
        self.answered(self.my_pid, self.my_pid, v_uid);
    }

    #[inline]
    pub fn answered(&mut self, src: usize, proposer: usize, v_uid: usize) {
        self.proposer_to_value.insert(proposer, v_uid);
        if src != proposer {
            self.proposer_scores[proposer] += 1;
        } else {
            self.proposer_scores[proposer] -= 1;
        }

        if proposer == self.my_pid {
            self.accepted += 1;
        }
        self.answers += 1;
        let inserted = self.answer_set.insert(src);
        debug_assert!(inserted);
    }

    #[inline]
    pub fn can_commit(&self) -> bool {
        debug_assert!(self.proposer_to_value.contains_key(&self.my_pid));
        self.proposer_to_value.len() == 1 && self.accepted >= self.e_paxos_quorum
    }

    // Note: If I receive an answer from a proposer, he will not commit (guaranteed)
    pub fn try_adopt(&self) -> Option<usize> {
        debug_assert!(self.proposer_to_value.contains_key(&self.my_pid));
        if self.answers < self.majority {
            None
        } else if self.proposer_to_value.len() < 2 {
            // Don't adopt unless there's more than 1 proposal (for now)
            // TODO: have some form of timeout in case too many died ?
            None
        } else {
            let mut best_score = 0;
            let mut best_value = None;
            for (proposer, value) in self.proposer_to_value.iter() {
                let score = self.proposer_scores[*proposer];
                if score > best_score {
                    best_value = Some(*value);
                    best_score = score;
                }
            }
            best_value
        }
    }
}
