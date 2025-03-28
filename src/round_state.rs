use crate::node_state::{Knowledge, NodeState};
use log::trace;
use std::fmt;

pub struct RoundState {
    nb_nodes: usize,
    my_pid: usize,
    majority: usize,

    node_states: Vec<NodeState>,

    // can_commit optimizations / scratchpads
    my_quorum: Vec<usize>,
    next_combination_pos: Vec<usize>,
    frozen_size_checked: usize,
    _bitset_scratchpad: Knowledge,
}

macro_rules! my_state {
    ($self:ident) => {
        $self.node_states[$self.my_pid]
    };
}

impl RoundState {
    pub fn new(nb_nodes: usize, my_pid: usize) -> Self {
        let majority = (nb_nodes / 2) + 1;
        let mut node_states = Vec::with_capacity(nb_nodes);
        for _ in 0..nb_nodes {
            node_states.push(NodeState::new(nb_nodes));
        }
        Self {
            nb_nodes,
            my_pid,
            majority,

            node_states,

            my_quorum: Vec::with_capacity(nb_nodes),
            next_combination_pos: Vec::with_capacity(majority - 1),
            frozen_size_checked: 0,
            _bitset_scratchpad: Knowledge::with_capacity(nb_nodes),
        }
    }

    pub fn clear(&mut self) {
        for node_state in self.node_states.iter_mut() {
            node_state.clear();
        }
        let inserted = my_state!(self).k.insert(self.my_pid);
        debug_assert!(inserted);
        self.my_quorum.clear();
        self.next_combination_pos.clear();
        self.frozen_size_checked = 0;
    }

    pub fn get_my_v(&self) -> Option<usize> {
        my_state!(self).v_uid
    }

    pub fn set_my_v(&mut self, v_uid: usize) {
        debug_assert!(self.get_my_v() == None);
        my_state!(self).v_uid = Some(v_uid);
    }

    pub fn am_i_frozen(&self) -> bool {
        my_state!(self).frozen
    }

    pub fn freeze(&mut self) {
        my_state!(self).frozen = true;
    }

    pub fn get_node_states(&self) -> &Vec<NodeState> {
        &self.node_states
    }

    pub fn learn_from(
        &mut self,
        remote_states: &[NodeState],
        msg_v_uid: usize,
        src: usize,
    ) -> bool {
        let orig_kl = my_state!(self).k.len();
        for pid in 0..self.nb_nodes {
            let local_node_state = &mut self.node_states[pid];
            let remote_node_state = &remote_states[pid];
            local_node_state.k.union_with(&remote_node_state.k);
            if let Some(node_v_uid) = remote_node_state.v_uid {
                debug_assert_eq!(local_node_state.v_uid.unwrap_or(node_v_uid), node_v_uid);
                local_node_state.v_uid = Some(node_v_uid);
            }
            local_node_state.frozen |= remote_node_state.frozen;
        }
        if msg_v_uid == self.get_my_v().unwrap() && !self.am_i_frozen() {
            my_state!(self).k.union_with(&remote_states[src].k);
        }
        orig_kl < my_state!(self).k.len()
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
            let v = some_node.v_uid;
            // For all v in the frozen set
            if v == max_score_v {
                continue;
            }

            let mut v_frozen_count = 0;
            self._bitset_scratchpad.clear();
            for node in self.node_states.iter() {
                if !node.frozen || v != node.v_uid {
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
    // TODO: Speedup for e-paxos / paxos ?
    pub fn can_commit(&mut self) -> bool {
        let k_size = my_state!(self).k.len();
        debug_assert!(k_size <= self.nb_nodes);
        if k_size < self.majority {
            return false;
        }
        let unknown_nodes = self.nb_nodes - k_size;
        let trivial_frozen = self.majority;
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

            // TODO: If everyone knows a node that knows a majority*, we can:
            //         - Change this condition to to_know <= frozen + 1
            //       OR equivalently:
            //         - Lower trivial_frozen to majority-1
            //         - Immediately return true if k_size >= fast_quorum
            //   *: e.g. we're the sole proposer
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

                    trace!("Reordering my_quorum len: {}", self.my_quorum.len());
                    for pid in self.my_quorum.iter() {
                        trace!("  - Knowledge of {}: {:?}", pid, self.node_states[*pid].k)
                    }
                }
                self.next_combination_pos.extend(0..frozen);
            }
            debug_assert_eq!(self.next_combination_pos.len(), frozen);
            loop {
                // TODO: Save intermediate known set to reduce re-computations ? (is it worth it ?)
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

impl fmt::Display for RoundState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // write!(f, "[")?;
        for (i, state) in self.node_states.iter().enumerate() {
            if let Some(v_uid) = state.v_uid {
                write!(f, "\n  {}: uid={}, k={:?}", i, v_uid, state.k)?;
            }
        }
        // write!(f, "\n]")?;
        Ok(())
    }
}
