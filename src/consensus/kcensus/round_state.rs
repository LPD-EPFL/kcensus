use crate::consensus::kcensus::node_state::NodeState;
use crate::consensus::kcensus::propagation::{MessageId, PropagationGraphs};
use bit_set::BitSet;
use std::collections::HashSet;
use std::fmt;
use std::time::Duration;

macro_rules! my_state {
    ($self:ident) => {
        $self.node_states[$self.my_pid]
    };
}

pub struct KCensusRoundState {
    my_pid: usize,
    majority: usize,

    node_states: Vec<NodeState>,
    propagation_states: Vec<Duration>,

    proposers: Vec<usize>,
    leaders: Vec<usize>,
    received_msgs: HashSet<MessageId>,

    paxos_accept_count: usize,
}

impl KCensusRoundState {
    pub fn new(process_count: usize, majority: usize, my_pid: usize) -> Self {
        Self {
            my_pid,
            majority,

            node_states: (0..process_count).map(NodeState::new).collect(),
            propagation_states: vec![Duration::ZERO; process_count],

            proposers: Vec::with_capacity(process_count),
            leaders: Vec::with_capacity(process_count),
            received_msgs: HashSet::with_capacity(process_count),

            paxos_accept_count: 0,
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        for node_state in self.node_states.iter_mut() {
            node_state.clear();
        }
        for prop_state in self.propagation_states.iter_mut() {
            *prop_state = Duration::ZERO;
        }

        self.proposers.clear();
        self.leaders.clear();
        self.received_msgs.clear();

        self.paxos_accept_count = 0;
    }

    #[inline]
    pub fn get_my_v(&self) -> Option<usize> {
        my_state!(self).get_v()
    }

    #[inline]
    pub fn get_round(&self) -> Option<usize> {
        my_state!(self).get_prepared_for()
    }

    #[inline]
    pub fn kcensus_try_accept(
        &mut self,
        v: usize,
        proposer: usize,
        graph: &PropagationGraphs,
    ) -> bool {
        if self.get_my_v().is_some() || self.has_conflict() {
            return false;
        }
        assert!(!self.am_i_frozen());
        my_state!(self).accept_with_k_state(v, proposer, Duration::ZERO, graph);
        if self.proposers.is_empty() {
            assert!(self.leaders.is_empty());
            self.proposers.push(proposer);
            self.leaders.push(graph.get_leader(proposer));
        } else {
            assert_eq!(self.proposers.len(), 1);
            assert_eq!(self.leaders.len(), 1);
            assert_eq!(self.proposers[0], proposer);
            assert_eq!(self.leaders[0], graph.get_leader(proposer));
        }
        true
    }

    #[inline]
    pub fn update_k_state(&mut self, time: Duration, graph: &PropagationGraphs) {
        assert!(self.get_my_v().is_some());
        my_state!(self).update_k_state(time, graph);
    }

    #[inline]
    pub fn am_i_frozen(&self) -> bool {
        my_state!(self).is_frozen()
    }

    #[inline]
    pub fn freeze_and_prepare_for(&mut self, leader: usize) {
        my_state!(self).prepare_for(leader);
    }

    #[inline]
    pub fn freeze_and_prepare_for_leaders(&mut self) {
        for leader in self.leaders.iter() {
            my_state!(self).prepare_for(*leader);
        }
    }

    #[inline]
    pub fn prepared_for(&self) -> Option<usize> {
        my_state!(self).get_prepared_for()
    }

    #[inline]
    pub fn clone_node_states(&self) -> Vec<NodeState> {
        self.node_states.clone()
    }

    #[inline]
    pub fn proposers(&self) -> &[usize] {
        &self.proposers
    }

    #[inline]
    pub fn leaders(&self) -> &[usize] {
        &self.leaders
    }

    #[inline]
    pub fn has_conflict(&self) -> bool {
        self.proposers().len() > 1 || my_state!(self).get_paxos_accept_round().is_some()
    }

    #[inline]
    pub fn receive_msg(&mut self, msg: MessageId) {
        let inserted = self.received_msgs.insert(msg);
        assert!(inserted);
    }

    #[inline]
    pub fn has_received(&mut self, dependencies: &HashSet<MessageId>) -> bool {
        self.received_msgs.is_superset(dependencies)
    }

    /**
     * This method simply saves new remote states while keeping proposers/leaders lists up to date.
     */
    #[inline]
    pub fn update_remote_states(
        &mut self,
        v: usize,
        msg_id: MessageId,
        remote_states: &Option<Vec<NodeState>>,
        graph: &PropagationGraphs,
    ) -> bool {
        let mut changed = false;
        let my_proposer = my_state!(self).get_k_proposer();
        let process_count = self.node_states.len();
        for pid in 0..process_count {
            // Retrieve local and remote states
            let local_node_state = &mut self.node_states[pid];
            let remote_node_state = match remote_states {
                Some(remote_states) => &remote_states[pid],
                None => &graph.build_remote_state(v, msg_id, pid),
            };

            // Skip remote states if not newer
            if remote_node_state <= local_node_state {
                continue;
            }
            assert_ne!(pid, self.my_pid);

            // Update local copy
            *local_node_state = remote_node_state.clone();
            changed = true;

            // Update lists of proposers and leaders
            if let Some(proposer) = remote_node_state.get_k_proposer() {
                // This node is still in KCensus phase (fast-path)
                assert!(remote_node_state.get_paxos_accept_round().is_none());
                assert!(remote_node_state.get_v().is_some());

                if !self.proposers.contains(&proposer) {
                    assert_ne!(Some(proposer), my_proposer);
                    self.proposers.push(proposer);
                    let leader = graph.get_leader(proposer);
                    if !self.leaders.contains(&leader) {
                        self.leaders.push(leader);
                    }
                }
            }
        }
        changed
    }

    #[inline]
    pub fn get_propagation_state(&self, proposer: usize) -> Duration {
        self.propagation_states[proposer]
    }

    #[inline]
    pub fn set_propagation_state(&mut self, proposer: usize, state_id: Duration) {
        self.propagation_states[proposer] = state_id;
    }

    #[inline]
    pub fn get_node_states(&self) -> &Vec<NodeState> {
        &self.node_states
    }

    #[inline]
    pub fn get_paxos_accept_round(&self) -> Option<usize> {
        my_state!(self).get_paxos_accept_round()
    }

    #[inline]
    pub fn can_start_paxos_accept(&self, alive_replicas: &BitSet) -> bool {
        if self.prepared_for() != Some(self.my_pid)
            || Some(self.my_pid) <= self.get_paxos_accept_round()
        {
            return false;
        };
        let prepared_count = self
            .node_states
            .iter()
            .enumerate()
            .filter(|(id, state)| {
                alive_replicas.contains(*id) && state.get_prepared_for() == Some(self.my_pid)
            })
            .count();
        prepared_count >= self.majority
    }

    pub fn could_adopt(&self, alive_replicas: &BitSet) -> bool {
        self.node_states
            .iter()
            .enumerate()
            .filter(|(id, state)| alive_replicas.contains(*id) && state.is_frozen())
            .count()
            >= self.majority
    }

    pub fn adopt(&self, graph: &PropagationGraphs) -> Option<usize> {
        let mut highest_paxos_accept_round = None;
        let mut highest_paxos_accept_value = None;

        for node in self.node_states.iter() {
            let paxos_accept_round = node.get_paxos_accept_round();
            if paxos_accept_round > highest_paxos_accept_round {
                highest_paxos_accept_round = paxos_accept_round;
                highest_paxos_accept_value = node.get_v();
                assert!(highest_paxos_accept_value.is_some());
            }
        }
        if highest_paxos_accept_round.is_some() {
            return highest_paxos_accept_value;
        }

        'proposer_loop: for proposer in self.proposers.iter().copied() {
            let v = self.node_states[proposer].get_v().unwrap();
            let v_leader = graph.get_leader(proposer);
            let v_required_k = graph.get_final_knowledge(proposer);
            let v_quorum = &v_required_k[v_leader];

            for (pid, node) in self.node_states.iter().enumerate() {
                // TODO: detect failed commits in more cases to batch more ?
                if !node.is_frozen() {
                    continue;
                }

                if node.get_k_proposer() != Some(proposer) {
                    // Node rooting for something else. Check for conflict with v_quorum.
                    let conflict = match node.get_k_proposer() {
                        Some(proposer) => !graph
                            .get_knowledge(proposer, pid, node.get_k_state_id())
                            .is_disjoint(v_quorum),
                        None => v_quorum.contains(pid),
                    };
                    if conflict {
                        continue 'proposer_loop;
                    }
                } else {
                    // Node rooting for v. Check that it reached the required knowledge.
                    if graph.get_final_state_id(proposer, pid) != node.get_k_state_id() {
                        continue 'proposer_loop;
                    }
                }
            }

            return Some(v);
        }

        None // Means nothing was commited, thus we can batch
    }

    #[inline]
    pub fn can_kcensus_commit(&self, graph: &PropagationGraphs) -> bool {
        if let Some(proposer) = my_state!(self).get_k_proposer() {
            if graph.get_leader(proposer) == self.my_pid
                && graph.get_final_state_id(proposer, self.my_pid)
                    == self.node_states[self.my_pid].get_k_state_id()
            {
                return true;
            }
        }
        false
    }

    #[inline]
    pub fn paxos_accept(&mut self, leader: usize, v: usize) {
        if my_state!(self).paxos_accept(leader, v) && leader == self.my_pid {
            self.paxos_accept_count += 1;
        }
    }

    #[inline]
    pub fn recv_paxos_accept(&mut self, src: usize, v: usize) {
        if self.node_states[src].paxos_accept(self.my_pid, v) {
            self.paxos_accept_count += 1;
        }
    }

    #[inline]
    pub fn can_paxos_commit(&self) -> bool {
        self.paxos_accept_count >= self.majority
    }
}

impl fmt::Display for KCensusRoundState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // write!(f, "[")?;
        for (i, state) in self.node_states.iter().enumerate() {
            if let Some(v) = state.get_v() {
                write!(
                    f,
                    "\n  {}: leader={:?}, v={}, ",
                    i,
                    state.get_prepared_for(),
                    v,
                )?;
                if let Some(round) = state.get_paxos_accept_round() {
                    write!(f, "paxos_accept_round={round}")?;
                } else if let Some(proposer) = state.get_k_proposer() {
                    write!(
                        f,
                        "proposer={}, state={:?}",
                        proposer,
                        state.get_k_state_id()
                    )?;
                }
            }
        }
        // write!(f, "\n]")?;
        Ok(())
    }
}
