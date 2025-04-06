use crate::consensus::paxos::message::RoundValue;
use bit_set::BitSet;
use std::collections::HashMap;

pub struct EPaxosRoundState {
    my_pid: usize,
    majority: usize,
    e_paxos_quorum: usize,

    answers: usize,
    proposer_to_value: HashMap<usize, usize>,
    proposer_to_accepted: Vec<usize>,

    // Only used for safety checks
    answer_set: BitSet,
}

impl EPaxosRoundState {
    pub fn new(nb_nodes: usize, my_pid: usize) -> Self {
        let majority = (nb_nodes / 2) + 1;
        let e_paxos_quorum = ((nb_nodes * 3) / 4).max(majority);

        let mut x = Self {
            my_pid,
            majority,
            e_paxos_quorum,

            answers: 1,
            proposer_to_value: HashMap::with_capacity(nb_nodes),
            proposer_to_accepted: vec![1; nb_nodes],

            answer_set: BitSet::with_capacity(nb_nodes),
        };
        x.full_clear();
        x
    }

    #[inline]
    pub fn full_clear(&mut self) {
        self.answers = 1;
        self.proposer_to_value.clear();
        self.proposer_to_accepted.fill(1);
        self.answer_set.clear();
        self.answer_set.insert(self.my_pid);
    }

    #[inline]
    pub fn propose_v(&mut self, v_uid: usize) {
        debug_assert!(self.proposer_to_value.is_empty());
        self.proposer_to_value.insert(self.my_pid, v_uid);
    }

    pub fn answered(&mut self, src: usize, proposer: usize, v_uid: usize) {
        debug_assert_ne!(src, self.my_pid);
        self.proposer_to_value.insert(proposer, v_uid);
        debug_assert!(self.answer_set.insert(src));
        self.answers += 1;
        if src != proposer {
            self.proposer_to_accepted[proposer] += 1;
        }
    }

    #[inline]
    pub fn can_commit(&self) -> bool {
        debug_assert!(self.proposer_to_value.contains_key(&self.my_pid));
        self.proposer_to_value.len() == 1
            && self.proposer_to_accepted[self.my_pid] >= self.e_paxos_quorum
    }

    // Note: If I receive an answer from a proposer, he will not commit (guaranteed)
    pub fn try_adopt(&self) -> Option<RoundValue> {
        debug_assert!(self.proposer_to_value.contains_key(&self.my_pid));
        if self.answers < self.majority {
            None
        } else if self.proposer_to_value.len() < 2 {
            // TODO: have some form of timeout in case too many died ?
            // Don't adopt unless there's more than 1 proposal
            None
        } else {
            let mut best_score = 0;
            let mut best_value = None;
            for (proposer, value) in self.proposer_to_value.iter() {
                let score = self.proposer_to_accepted[*proposer];
                if score > best_score {
                    best_value = Some(RoundValue::EPaxosV {
                        proposer: *proposer,
                        v_uid: *value,
                    });
                    best_score = score;
                }
            }
            best_value
        }
    }
}
