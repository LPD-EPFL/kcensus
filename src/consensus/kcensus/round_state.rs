use crate::consensus::kcensus::node_state::{Knowledge, NodeState};
use crate::consensus::kcensus::propagation::MessageId;
use std::collections::HashSet;
use std::fmt;

macro_rules! my_state {
    ($self:ident) => {
        $self.node_states[$self.my_pid]
    };
}

pub struct KCensusRoundState {
    nb_nodes: usize,
    my_pid: usize,
    majority: usize,

    node_states: Vec<NodeState>,

    proposers: Vec<usize>,
    received_msgs: HashSet<MessageId>,

    // can_commit optimizations / scratchpads
    my_quorum: Vec<usize>,
    next_combination_pos: Vec<usize>,
    frozen_size_checked: usize,
    _bitset_scratchpad: Knowledge,
}

impl KCensusRoundState {
    pub fn new(nb_nodes: usize, my_pid: usize) -> Self {
        let majority = (nb_nodes / 2) + 1;
        let mut node_states = Vec::with_capacity(nb_nodes);
        for _ in 0..nb_nodes {
            node_states.push(NodeState::new(nb_nodes));
        }
        let mut x = Self {
            nb_nodes,
            my_pid,
            majority,

            node_states,

            proposers: Vec::with_capacity(nb_nodes),
            received_msgs: HashSet::with_capacity(nb_nodes),

            my_quorum: Vec::with_capacity(nb_nodes),
            next_combination_pos: Vec::with_capacity(majority - 1),
            frozen_size_checked: 0,
            _bitset_scratchpad: Knowledge::with_capacity(nb_nodes),
        };
        x.clear();
        x
    }

    #[inline]
    pub fn clear(&mut self) {
        for node_state in self.node_states.iter_mut() {
            node_state.clear();
        }
        // Alternatively, the following could be set in set_my_v
        let inserted = my_state!(self).k.insert(self.my_pid);
        debug_assert!(inserted);

        self.proposers.clear();
        self.received_msgs.clear();

        self.my_quorum.clear();
        self.next_combination_pos.clear();
        self.frozen_size_checked = 0;
    }

    #[inline]
    pub fn get_my_v(&self) -> Option<usize> {
        my_state!(self).v
    }

    #[inline]
    pub fn set_my_v(&mut self, v: usize) {
        debug_assert!(self.get_my_v().is_none());
        my_state!(self).v = Some(v);
    }

    #[inline]
    pub fn am_i_frozen(&self) -> bool {
        my_state!(self).frozen
    }

    #[inline]
    pub fn freeze(&mut self) {
        my_state!(self).frozen = true;
    }

    #[inline]
    pub fn clone_node_states(&self) -> Vec<NodeState> {
        self.node_states.clone()
    }

    #[inline]
    pub fn become_proposer(&mut self) {
        debug_assert!(self.proposers.is_empty());
        my_state!(self).proposer = true;
        self.proposers.push(self.my_pid);
    }

    #[inline]
    pub fn proposers(&self) -> &[usize] {
        &self.proposers
    }

    #[inline]
    pub fn i_am_proposer(&self) -> bool {
        // Note: Can only propose if I didn't see other proposals
        debug_assert_eq!(
            !self.proposers.is_empty() && self.proposers[0] == self.my_pid,
            my_state!(self).proposer
        );
        my_state!(self).proposer
    }

    #[inline]
    pub fn receive_msg(&mut self, msg: MessageId) {
        let inserted = self.received_msgs.insert(msg);
        debug_assert!(inserted);
    }

    #[inline]
    pub fn can_send(&mut self, dependencies: &HashSet<MessageId>) -> bool {
        self.received_msgs.is_superset(dependencies)
    }

    #[inline]
    pub fn learn_from(&mut self, remote_states: &[NodeState]) -> bool {
        let orig_kl = my_state!(self).k.len();
        for (pid, remote_node_state) in remote_states.iter().enumerate() {
            let local_node_state = &mut self.node_states[pid];
            local_node_state.k.union_with(&remote_node_state.k);
            if let Some(node_v) = remote_node_state.v {
                if remote_node_state.proposer && !local_node_state.proposer {
                    debug_assert_ne!(pid, self.my_pid);
                    debug_assert!(local_node_state.v.is_none());
                    self.proposers.push(pid);
                    local_node_state.proposer = true;
                }
                debug_assert_eq!(local_node_state.v.unwrap_or(node_v), node_v);
                local_node_state.v = Some(node_v);
            } else {
                debug_assert!(!remote_node_state.proposer);
            }
            local_node_state.frozen |= remote_node_state.frozen;
            if !self.am_i_frozen() && self.get_my_v() == remote_node_state.v {
                // Only accumulate into your own knowledge if you're not frozen
                my_state!(self).k.union_with(&remote_states[pid].k);
            }
        }
        orig_kl < my_state!(self).k.len()
    }

    #[inline]
    pub fn partial_learn(&mut self, learner: usize, about: usize) {
        self.node_states[learner].k.insert(about);
        my_state!(self).k.insert(about);
    }

    #[inline]
    pub fn learn(&mut self, learner: usize, about: &Knowledge) {
        self.node_states[learner].k.union_with(about);
    }

    #[inline]
    pub fn knows(&self, learner: usize, about: usize) -> bool {
        self.node_states[learner].k.contains(about)
    }

    pub fn try_adopt(&mut self) -> Option<usize> {
        let frozen_count = self.node_states.iter().filter(|x| x.frozen).count();
        if frozen_count < self.majority {
            return None;
        }

        let mut max_score = 0usize;
        let mut max_score_v = None;
        for some_node in self.node_states.iter() {
            if !some_node.frozen {
                continue;
            }
            let v = some_node.v;
            // For all v in the frozen set
            if v == max_score_v {
                continue;
            }

            let mut v_frozen_count = 0;
            self._bitset_scratchpad.clear();
            for node in self.node_states.iter() {
                if !node.frozen || v != node.v {
                    continue;
                }
                v_frozen_count += 1;
                self._bitset_scratchpad.union_with(&node.k);
            }
            let v_known_count = self._bitset_scratchpad.len();
            debug_assert!(v_frozen_count <= v_known_count);

            if v_known_count > self.majority {
                return v;
            }

            let score = v_known_count * 2 - v_frozen_count;

            if score > max_score {
                max_score = score;
                max_score_v = v;
            }
        }
        debug_assert!(max_score_v.is_some());
        max_score_v
    }

    // TODO: Allow can_commit to run for other proposals ?
    pub fn can_commit(&mut self) -> bool {
        // TODO: Filter on v instead of using my k ? (helps if frozen or to allow commiting other props)
        let k_size = my_state!(self).k.len();
        debug_assert!(k_size <= self.nb_nodes);
        if k_size < self.majority {
            return false;
        }
        let e_paxos_quorum = (self.nb_nodes * 3) / 4;
        let everyone_knows_me = my_state!(self)
            .k
            .iter()
            .all(|pid| self.node_states[pid].k.contains(self.my_pid));
        if k_size >= e_paxos_quorum && (everyone_knows_me || k_size > e_paxos_quorum) {
            return true;
        }
        let unknown_nodes = self.nb_nodes - k_size;
        let trivial_frozen = if everyone_knows_me {
            self.majority - 1
        } else {
            self.majority
        };
        let minority = self.nb_nodes - self.majority;
        let min_frozen = (k_size - minority).max(self.frozen_size_checked + 1); // Skip already checked ones

        /* When new nodes appear, if we already checked combinations of up to k-1 frozen,
        then we know that sets of up to k frozen nodes that include some new nodes are fine
        (thanks to the monotonicity of the score function & min_frozen increasing with new nodes)
        thus we only need to refresh my_quorum when reaching k+1 frozen bellow */
        if self.frozen_size_checked + 1 < min_frozen {
            // Note: this will trigger a refresh of my_quorum
            self.next_combination_pos.clear();
            self.frozen_size_checked = min_frozen - 1;
        }

        for frozen in min_frozen..trivial_frozen {
            let max_others_score = 2 * unknown_nodes - (self.majority - frozen);
            let to_know = self.majority.min(((max_others_score + frozen) / 2) + 1);

            if to_know <= frozen {
                self.frozen_size_checked = frozen;
                self.next_combination_pos.clear();
                continue;
            }
            if self.next_combination_pos.is_empty() {
                if self.my_quorum.len() != k_size {
                    self.my_quorum.clear();
                    self.my_quorum.extend(my_state!(self).k.iter());
                    // Optimisation: Put bigger knowledge first to help early skip
                    // Note: !x == (usize::MAX - x)
                    self.my_quorum
                        .sort_by_key(|a| !self.node_states[*a].k.len());

                    // trace!("Reordering my_quorum len: {}", self.my_quorum.len());
                    // for pid in self.my_quorum.iter() {
                    //     trace!("  - Knowledge of {}: {:?}", pid, self.node_states[*pid].k)
                    // }
                }
                self.next_combination_pos.extend(0..frozen);
            }
            debug_assert_eq!(self.next_combination_pos.len(), frozen);
            loop {
                let known = &mut self._bitset_scratchpad;
                known.clear();
                let mut unused_knowledge = frozen;
                for pos in self.next_combination_pos.iter() {
                    let pid = self.my_quorum[*pos];
                    known.union_with(&self.node_states[pid].k);
                    unused_knowledge -= 1;
                    // Optimisation: Early skip
                    if known.len() >= to_know {
                        break;
                    }
                }

                if known.len() < to_know {
                    return false;
                }

                let changed_suffix_size =
                    self.next_combination(self.my_quorum.len(), unused_knowledge);
                if changed_suffix_size == 0 {
                    break;
                }
            }
            // Save combinations that are checked
            self.frozen_size_checked = frozen;
            self.next_combination_pos.clear();
        } // for frozen
        true
    } // fn can_commit

    #[inline]
    fn next_combination(&mut self, positions: usize, unused: usize) -> usize {
        debug_assert!(positions < self.nb_nodes);
        let pos = &mut self.next_combination_pos;
        let len = pos.len();
        debug_assert!(len < self.majority);

        // Optimisation: Skip combinations that only change the unused nodes
        let min_to_move = 1.max(unused + 1);
        for suffix_size in min_to_move..=len {
            let suffix_start = len - suffix_size;
            // Can we move the last "suffix_size" positions ?
            if pos[suffix_start] + suffix_size < positions {
                // Yes: Move them and return
                let new_pos = pos[suffix_start] + 1;
                for j in 0..suffix_size {
                    pos[suffix_start + j] = new_pos + j;
                }
                return suffix_size;
            }
        }
        0
    }
}

impl fmt::Display for KCensusRoundState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // write!(f, "[")?;
        for (i, state) in self.node_states.iter().enumerate() {
            if let Some(v) = state.v {
                write!(f, "\n  {}: v={}, k={:?}", i, v, state.k)?;
            }
        }
        // write!(f, "\n]")?;
        Ok(())
    }
}
