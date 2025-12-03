use crate::consensus::kcensus::node_state::Knowledge;
use crate::topology::{Topology, FAULTY_LATENCY};
use bit_set::BitSet;
use log::trace;
use petgraph::algo::bellman_ford;
use petgraph::matrix_graph::DiMatrix;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::{Display, Formatter};
use std::time::Duration;

type ProcId = usize;

type NetworkGraph = DiMatrix<(), f64, Option<f64>, usize>;

#[derive(Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Copy, Clone, Serialize, Deserialize)]
pub struct MessageId {
    pub proposer: ProcId,
    pub src: ProcId,
    pub dest: ProcId,
    time: Duration,
}

impl Display for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(p{} at {:?}: {} -> {})",
            self.proposer, self.time, self.src, self.dest
        )
    }
}

#[derive(Debug, Clone)]
pub struct KnowledgeState {
    knowledge: Vec<Knowledge>,
    remote_states: Vec<Duration>,
    dependencies: HashSet<MessageId>,
    needed_by: Vec<MessageId>,
    frozen: BitSet,
}

#[derive(Debug)]
struct PropagationGraph {
    should_include_value: HashSet<MessageId>,
    states: Vec<BTreeMap<Duration, KnowledgeState>>,
    leader: ProcId,
}

#[derive(Debug)]
pub struct PropagationGraphs {
    graphs: Vec<PropagationGraph>,
    topology: Topology,
    pub rtts: Vec<Vec<Duration>>,
    pub path_rtts: Vec<Vec<Duration>>,
    pub kcensus_latencies: Vec<Duration>,
    pub paxos_latencies: Vec<Duration>,
    pub epaxos_latencies: Vec<Duration>,
    pub multi_paxos_latencies: Vec<Vec<Duration>>,
    pub multi_paxos_3p_latencies: Vec<Vec<Duration>>,
}

impl PropagationGraphs {
    #[inline]
    pub fn should_include_value(&self, msg_id: &MessageId) -> bool {
        self.graphs[msg_id.proposer]
            .should_include_value
            .contains(msg_id)
    }

    #[inline]
    pub fn get_start(&self, leader: ProcId) -> &KnowledgeState {
        self.graphs[leader].states[leader]
            .first_key_value()
            .expect("should have at least one state")
            .1
    }

    pub fn can_commit(&self, proposer: ProcId, received: &HashSet<MessageId>) -> bool {
        for state in self.graphs[proposer].states[self.get_leader(proposer)]
            .iter()
            .rev()
        {
            if !state.1.dependencies.is_subset(received) {
                return false;
            }
        }
        true
    }

    fn find_state(&self, node: ProcId, proposer: ProcId, time: Duration) -> &KnowledgeState {
        self.graphs[proposer].states[node]
            .get(&time)
            .expect("state should be found")
    }

    pub fn get_knowledge(&self, node: ProcId, proposer: ProcId, time: Duration) -> &Knowledge {
        &self.find_state(node, proposer, time).knowledge[node]
    }

    pub fn get_frozen(&self, node: ProcId, proposer: ProcId, time: Duration) -> bool {
        self.find_state(node, proposer, time).frozen.contains(node)
    }

    pub fn get_leader(&self, proposer: ProcId) -> ProcId {
        self.graphs[proposer].leader
    }

    pub fn get_final_knowledge(&self, proposer: ProcId) -> &[Knowledge] {
        let leader = self.graphs[proposer].leader;
        &self.graphs[proposer].states[leader]
            .iter()
            .last()
            .unwrap()
            .1
            .knowledge
    }

    pub fn get_final_state_id(&self, proposer: ProcId, node: ProcId) -> Duration {
        *self.graphs[proposer].states[node].iter().last().unwrap().0
    }

    pub fn get_remote_states(&self, msg: MessageId) -> &[Duration] {
        &self.graphs[msg.proposer].states[msg.src][&msg.time].remote_states
    }

    pub fn next_state(
        &self,
        proposer: ProcId,
        node: ProcId,
        time: Duration,
    ) -> Option<(Duration, &HashSet<MessageId>)> {
        self.graphs[proposer].states[node]
            .range(time..)
            .nth(1)
            .map(|(time, ks)| (*time, &ks.dependencies))
    }

    pub fn next_state_from_msg(&self, msg_id: MessageId) -> Duration {
        msg_id.time + self.topology.link_latency(msg_id.src, msg_id.dest)
    }

    pub fn get_new_messages_to_spread(
        &self,
        proposer: ProcId,
        node: ProcId,
        time: Duration,
    ) -> &[MessageId] {
        &self.graphs[proposer].states[node][&time].needed_by
    }
}

#[derive(Debug, Copy, Clone)]
struct TriangularPath {
    first: ProcId,
    second: ProcId,
    total_latency: Duration,
}

impl Display for TriangularPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(p -> {} -> {} -> p: {:?})",
            self.first, self.second, self.total_latency
        )
    }
}

fn triangle_latency(t: &TriangularPath) -> Duration {
    t.total_latency
}

fn are_compatible(knowledge_a: &[Knowledge], knowledge_b: &[Knowledge], f: usize) -> bool {
    let nb_nodes = knowledge_a.len();
    let mut quorum_a: BitSet = BitSet::with_capacity(nb_nodes);
    for k in knowledge_a.iter() {
        quorum_a.union_with(k);
    }
    if quorum_a.len() <= f {
        return false;
    }

    let mut quorum_b: BitSet = BitSet::with_capacity(nb_nodes);
    for k in knowledge_b.iter() {
        quorum_b.union_with(k);
    }
    if quorum_b.len() <= f {
        return false;
    }

    let mut count_deducers: usize = 0;
    for p in 0..nb_nodes {
        if !quorum_a.is_disjoint(&knowledge_b[p]) || !quorum_b.is_disjoint(&knowledge_a[p]) {
            count_deducers += 1;
        }
    }
    count_deducers > f
}

pub fn compute_propagation_graphs(
    topology: Topology,
    kcensus_graph: bool,
    shortest_paths: bool,
) -> PropagationGraphs {
    let nb_nodes = topology.nb_nodes;
    let majority = 1 + nb_nodes / 2;
    let f = majority - 1;

    let alive: Vec<_> = (0..nb_nodes)
        .filter(|x| !topology.faults.contains(*x))
        .collect();

    let mut path_latencies = vec![vec![Duration::default(); nb_nodes]; nb_nodes];
    let mut prev_dest = vec![vec![0; nb_nodes]; nb_nodes];
    let mut next_src = vec![vec![0; nb_nodes]; nb_nodes];

    if shortest_paths {
        // Generate graph nodes
        let mut graph = NetworkGraph::with_capacity(nb_nodes);
        for _ in 0..nb_nodes {
            graph.add_node(());
        }
        for src in 0..nb_nodes {
            for dest in 0..nb_nodes {
                if src != dest {
                    let nanos = topology.link_latency(src, dest).as_nanos();
                    graph.add_edge(src.into(), dest.into(), nanos as f64);
                }
            }
        }

        // Compute the shortest paths
        for src in 0..nb_nodes {
            let paths = bellman_ford(&graph, src.into()).expect("Latencies can not be negative");
            for dest in 0..nb_nodes {
                let mut last_pred = dest;
                let mut pred = paths.predecessors[dest].unwrap_or(src.into()).index();
                prev_dest[src][dest] = pred;
                while pred != src {
                    last_pred = pred;
                    pred = paths.predecessors[last_pred].unwrap_or(src.into()).index();
                }
                next_src[src][dest] = last_pred;
                let nanos = paths.distances[dest];
                if paths.distances[dest] > (u64::MAX / 4) as f64 {
                    path_latencies[src][dest] = FAULTY_LATENCY;
                } else {
                    path_latencies[src][dest] = Duration::from_nanos(nanos.round() as u64);
                }
            }
        }
    } else {
        for src in 0..nb_nodes {
            for dest in 0..nb_nodes {
                path_latencies[src][dest] = topology.link_latency(src, dest);
                prev_dest[src][dest] = src;
                next_src[src][dest] = dest;
            }
        }
    }

    let rtts: Vec<Vec<_>> = (0..nb_nodes)
        .map(|src| {
            (0..nb_nodes)
                .map(|dest| topology.link_latency(src, dest) + topology.link_latency(dest, src))
                .collect()
        })
        .collect();

    let path_rtts: Vec<Vec<_>> = (0..nb_nodes)
        .map(|src| {
            (0..nb_nodes)
                .map(|dest| path_latencies[src][dest] + path_latencies[dest][src])
                .collect()
        })
        .collect();

    let mut paxos_latencies = Vec::with_capacity(nb_nodes);
    let mut epaxos_latencies = Vec::with_capacity(nb_nodes);
    let mut multi_paxos_latencies = Vec::with_capacity(nb_nodes);
    let mut multi_paxos_3p_latencies = Vec::with_capacity(nb_nodes);

    for leader in 0..nb_nodes {
        trace!("leader: {leader}");

        let mut leader_round_trips = rtts[leader].clone();
        leader_round_trips.sort();
        paxos_latencies.push(leader_round_trips[majority - 1] * 2);
        let e_paxos_quorum = ((nb_nodes * 3) / 4).max(majority);
        epaxos_latencies.push(leader_round_trips[e_paxos_quorum - 1]);
        let multi_paxos_latency = (0..nb_nodes)
            .map(|requester| rtts[requester][leader] + leader_round_trips[majority - 1])
            .collect();
        multi_paxos_latencies.push(multi_paxos_latency);

        let multi_paxos_3p_latency: Vec<_> = (0..nb_nodes)
            .map(|requester| {
                let mut quorum_3p_round_trips: Vec<_> = (0..nb_nodes)
                    .map(|acceptor| {
                        topology.link_latency(leader, acceptor)
                            + topology.link_latency(acceptor, requester)
                    })
                    .collect();
                quorum_3p_round_trips.sort();
                topology.link_latency(requester, leader) + quorum_3p_round_trips[majority - 1]
            })
            .collect();
        multi_paxos_3p_latencies.push(multi_paxos_3p_latency);
    }

    let mut propagation_graphs = Vec::with_capacity(nb_nodes);
    let mut kcensus_latencies = Vec::with_capacity(nb_nodes);
    if kcensus_graph {
        let mut triangular_paths: Vec<Vec<TriangularPath>> =
            vec![Vec::with_capacity(alive.len().pow(2)); nb_nodes];
        let mut knowledge_levels: Vec<Vec<(Duration, Vec<Knowledge>, usize)>> =
            vec![Vec::new(); nb_nodes];
        let mut value_only_paths: Vec<Vec<TriangularPath>> =
            vec![Vec::with_capacity(alive.len()); nb_nodes];

        'leader_loop: for leader in 0..nb_nodes {
            if topology.faults.contains(leader) {
                knowledge_levels[leader].push((Duration::ZERO, Vec::new(), 0));
                continue 'leader_loop;
            }

            // Compute triangles
            for first in alive.iter().copied() {
                let latency_to_first = path_latencies[leader][first];
                for second in alive.iter().copied() {
                    let total_latency = latency_to_first
                        + path_latencies[first][second]
                        + path_latencies[second][leader];
                    triangular_paths[leader].push(TriangularPath {
                        first,
                        second,
                        total_latency,
                    });
                }
            }
            triangular_paths[leader].sort_by_key(triangle_latency);
            // trace!("triangular_paths: {}", triangular_paths.len());
            trace!(
                "longest triangular path: {}",
                triangular_paths[leader].last().unwrap()
            );

            // Compute shortest round-trip paths.
            // Used to ensure the value is sent to everyone (not for knowledge spreading).
            for node in alive.iter().copied() {
                let total_latency = path_latencies[leader][node] + path_latencies[node][leader];
                value_only_paths[leader].push(TriangularPath {
                    first: node,
                    second: leader,
                    total_latency,
                })
            }

            value_only_paths[leader].sort_by_key(triangle_latency);
            let min_lat = value_only_paths[leader][f].total_latency;
            let max_lat = min_lat * 2;

            // Simulate propagation of knowledge (from best to worst possible strategy)
            let mut knowledges: Vec<Knowledge> = vec![BitSet::with_capacity(nb_nodes); nb_nodes];
            'triangle_loop: for ti in 0..triangular_paths[leader].len() {
                let t = &triangular_paths[leader][ti];
                if t.total_latency > max_lat {
                    break 'triangle_loop;
                }
                knowledges[t.second].insert(t.first);
                if t.total_latency < min_lat {
                    continue 'triangle_loop;
                }
                let next_t = triangular_paths[leader].get(ti + 1);
                if next_t.is_some_and(|nt| nt.total_latency == t.total_latency) {
                    continue 'triangle_loop;
                }
                knowledge_levels[leader].push((t.total_latency, knowledges.clone(), ti + 1));
            }
        }

        // Truncate knowledge levels (Note: could be merged with precomputation of compatibilities)
        let mut max_levels = vec![0usize; nb_nodes];
        for a in alive.iter().copied() {
            for b in alive.iter().copied() {
                while !are_compatible(
                    &knowledge_levels[a][max_levels[a]].1,
                    &knowledge_levels[b][0].1,
                    f,
                ) {
                    max_levels[a] += 1;
                }
            }
            knowledge_levels[a].truncate(max_levels[a] + 1);
        }

        // Precompute compatible levels
        let mut compatible_levels: Vec<Vec<Vec<usize>>> = vec![Vec::new(); nb_nodes];
        for a in 0..nb_nodes {
            if topology.faults.contains(a) {
                compatible_levels[a].push(vec![0; nb_nodes]);
                continue;
            }

            let mut current_levels: Vec<usize> = max_levels.clone();
            for a_level in 0..knowledge_levels[a].len() {
                let knowledge_a = &knowledge_levels[a][a_level].1;
                for b in alive.iter().copied() {
                    while current_levels[b] > 0
                        && are_compatible(
                            knowledge_a,
                            &knowledge_levels[b][current_levels[b] - 1].1,
                            f,
                        )
                    {
                        current_levels[b] -= 1;
                    }
                }
                compatible_levels[a].push(current_levels.clone());
            }
        }

        // Initialize best to a trivial valid solution
        let mut best_levels = max_levels.clone();
        let mut best_total_time = Duration::ZERO;
        for leader in 0..nb_nodes {
            best_total_time += knowledge_levels[leader][max_levels[leader]].0;
        }

        // Implements recursive search of the optimal solution
        fn best_avg_search(
            best_levels: &mut Vec<usize>,
            best_total_time: &mut Duration,
            pids_done: usize,
            prev_levels: &Vec<usize>,
            prev_partial_total_time: Duration,
            nb_nodes: usize,
            max_levels: &Vec<usize>,
            knowledge_levels: &Vec<Vec<(Duration, Vec<Knowledge>, usize)>>,
            compatible_levels: &Vec<Vec<Vec<usize>>>,
        ) {
            let pid_a = pids_done;
            let pids_done = pids_done + 1;
            'level_loop: for level_a in prev_levels[pid_a]..=max_levels[pid_a] {
                let mut new_levels = prev_levels.clone();
                new_levels[pid_a] = level_a;
                let new_partial_total_time =
                    prev_partial_total_time + knowledge_levels[pid_a][level_a].0;
                let mut new_curr_total_time = new_partial_total_time;
                let mut new_min_total_time = new_partial_total_time;
                for pid_b in pids_done..nb_nodes {
                    new_min_total_time += knowledge_levels[pid_b][prev_levels[pid_b]].0;
                    let req_level_b = compatible_levels[pid_a][level_a][pid_b];
                    if req_level_b > new_levels[pid_b] {
                        new_levels[pid_b] = req_level_b;
                    }
                    new_curr_total_time += knowledge_levels[pid_b][new_levels[pid_b]].0;
                }

                if *best_total_time <= new_min_total_time {
                    // We can not find a better solution with higher level_a
                    break 'level_loop;
                }
                if *best_total_time <= new_curr_total_time {
                    // We can not find a better solution with current level_a
                    continue 'level_loop;
                }

                assert!(new_partial_total_time < *best_total_time);
                if pids_done == nb_nodes {
                    // Found a complete solution that is better !
                    *best_levels = new_levels;
                    *best_total_time = new_partial_total_time;
                } else {
                    // Promising but incomplete solution. Search this branch:
                    best_avg_search(
                        best_levels,
                        best_total_time,
                        pids_done,
                        &new_levels,
                        new_partial_total_time,
                        nb_nodes,
                        max_levels,
                        knowledge_levels,
                        compatible_levels,
                    );
                }
            }
        }

        // Use recursive search to compute the best solution
        best_avg_search(
            &mut best_levels,
            &mut best_total_time,
            0,
            &vec![0usize; nb_nodes],
            Duration::ZERO,
            nb_nodes,
            &max_levels,
            &knowledge_levels,
            &compatible_levels,
        );

        // Build the graph from the solutions for each proposer
        'leader_loop: for leader in 0..nb_nodes {
            // Skip faulty proposers
            if topology.faults.contains(leader) {
                propagation_graphs.push(PropagationGraph {
                    should_include_value: HashSet::new(),
                    states: Vec::new(),
                    leader,
                });
                kcensus_latencies.push(FAULTY_LATENCY);
                continue 'leader_loop;
            }

            // Truncate triangles at commit time
            let (max_latency, _, triangle_count) = knowledge_levels[leader][best_levels[leader]];
            triangular_paths[leader].truncate(triangle_count);
            assert_eq!(
                triangular_paths[leader][triangle_count - 1].total_latency,
                max_latency
            );
            trace!(
                "Left after truncate: {} real triangles, {} total",
                triangular_paths[leader]
                    .iter()
                    .filter(|x| x.first != leader && x.second != leader && x.first != x.second)
                    .count(),
                triangular_paths[leader].len()
            );
            trace!(
                "longest path: {}",
                triangular_paths[leader][triangle_count - 1]
            );

            // TODO: Some knowledge might still not be needed to commit. (but the cost is probably negligible)
            //   Try to check if they are needed for are_compatible?

            // Initialize graph
            let mut message_times: Vec<Vec<BTreeSet<Duration>>> =
                vec![vec![BTreeSet::new(); nb_nodes]; nb_nodes];
            let mut should_include_value: HashSet<MessageId> = HashSet::new();
            let mut states: Vec<BTreeMap<Duration, KnowledgeState>> =
                vec![BTreeMap::new(); nb_nodes];

            // Prepare initial states
            for (i, state) in states.iter_mut().enumerate() {
                let mut knowledge = vec![BitSet::new(); nb_nodes];
                if i == leader {
                    knowledge[leader].insert(leader);
                }
                state.insert(
                    Duration::ZERO,
                    KnowledgeState {
                        knowledge,
                        remote_states: vec![Duration::ZERO; nb_nodes],
                        dependencies: HashSet::new(),
                        needed_by: vec![],
                        frozen: BitSet::new(),
                    },
                );
            }

            // Loop over triangles
            let mut i = value_only_paths[leader].len(); // first: send values (desc order, but does not matter)
            let mut j = triangle_count; // second: triangles to commit, from longest to shortest (desc)
            'triangle_loop: while 0 < j {
                // Pick the next triangle
                let value_only_path = 0 < i;
                let t = if value_only_path {
                    i -= 1;
                    &value_only_paths[leader][i]
                } else {
                    j -= 1;
                    &triangular_paths[leader][j]
                };

                // Skip triangles that would not bring new knowledge
                if !value_only_path {
                    let leaders_final_knowledge = &states[leader]
                        .range(..=max_latency)
                        .last()
                        .expect("leader should have a last state")
                        .1
                        .knowledge;
                    if leaders_final_knowledge[t.second].contains(t.first) {
                        continue 'triangle_loop;
                    }
                }

                // Compute slack (how much delay can add when we reuse messages)
                let mut max_slack = if t.total_latency <= max_latency {
                    max_latency - t.total_latency
                } else {
                    assert!(value_only_path);
                    Duration::ZERO
                };

                // Initialize variables to track time/position/progress along the path
                let mut current_time = Duration::ZERO;
                let mut current = leader;
                let mut shortest_path_from_leader = true;
                let mut _shortest_path_to_leader = false;

                // Loop over the three (/one) checkpoints of the triangle (/value_only_patH)
                let checkpoints = if value_only_path {
                    [t.first, t.first, t.first]
                } else {
                    [t.first, t.second, leader]
                };
                for (step, target) in checkpoints.into_iter().enumerate() {
                    if step > 0 && target == leader {
                        shortest_path_from_leader = false;
                        _shortest_path_to_leader = true;
                    }

                    // For every step towards the checkpoints
                    while current != target {
                        // TODO: if step == 2 (return path) and there's already msgs
                        //   going back to the leader, then avoid creating new ones.
                        //   (Chose one of the existing paths or forward knowledge recursively)

                        // Move towards checkpoint
                        let src = current;
                        current = next_src[current][target];
                        assert_ne!(src, current);

                        // Check if shortest path from/to leader
                        if step > 0 {
                            shortest_path_from_leader &= prev_dest[leader][current] == src;
                            let left = path_latencies[src][target] + path_latencies[target][leader];
                            let to_leader = path_latencies[src][leader];
                            _shortest_path_to_leader |= left == to_leader;
                        }

                        // Find existing compatible message or create new one (and add to source state)
                        let deadline = current_time + max_slack;
                        let next_compatible_msg_time = message_times[src][current]
                            .range(current_time..=deadline)
                            .next();
                        let new_msg = next_compatible_msg_time.is_none();
                        let msg_id = if let Some(compatible_time) = next_compatible_msg_time {
                            assert!(!new_msg);
                            // Reuse existing message
                            let msg_id = MessageId {
                                proposer: leader,
                                src,
                                dest: current,
                                time: *compatible_time,
                            };
                            debug_assert!(
                                states[src]
                                    .get_mut(compatible_time)
                                    .expect("should have state at src")
                                    .needed_by
                                    .contains(&msg_id)
                            );
                            msg_id
                        } else {
                            // New message!
                            assert!(new_msg);
                            // TODO: explore if it can be useful to delay messages ? (for negligible gain)

                            // Add to message_times...
                            let inserted = message_times[src][current].insert(current_time);
                            assert!(inserted);

                            let msg_id = MessageId {
                                proposer: leader,
                                src,
                                dest: current,
                                time: current_time,
                            };

                            // Add msg as derived from the source's state
                            states[src]
                                .get_mut(&current_time)
                                .expect("should have state at src")
                                .needed_by
                                .push(msg_id);

                            // Mark as including a value if needed
                            if value_only_path {
                                should_include_value.insert(msg_id);
                            }
                            assert_eq!(
                                value_only_path, shortest_path_from_leader,
                                "new message should imply value_only_path == shortest_path_from_leader"
                            );

                            msg_id
                        };

                        // Update time and compute remaining slack
                        let src_time = msg_id.time;
                        current_time = src_time + topology.link_latency(src, current);
                        max_slack = deadline - src_time;

                        // if needed, create destination state (with all the previous knowledge)
                        if !states[current].contains_key(&current_time) {
                            assert!(new_msg);
                            let prev_state = states[current]
                                .range(..current_time)
                                .last()
                                .expect("should find a previous state");
                            let mut knowledge = prev_state.1.knowledge.clone();
                            knowledge[current].insert(current);
                            let mut remote_states = prev_state.1.remote_states.clone();
                            remote_states[current] = current_time;
                            let state = KnowledgeState {
                                knowledge,     // fully filled bellow
                                remote_states, // same
                                dependencies: HashSet::with_capacity(1),
                                needed_by: vec![],
                                frozen: BitSet::new(),
                            };
                            let inserted = states[current].insert(current_time, state).is_none();
                            assert!(inserted);
                        }

                        // Update dest state's knowledge
                        let [src_state, cur_state] = states
                            .get_disjoint_mut([src, current])
                            .expect("src should != current");
                        let state = cur_state
                            .get_mut(&current_time)
                            .expect("should have a destination state now");
                        let src_state = src_state.get(&src_time).expect("should have source state");
                        state.dependencies.insert(msg_id); // Note: could be already present
                        state.knowledge[current].union_with(&src_state.knowledge[src]);
                        for i in 0..nb_nodes {
                            // Check source knowledge
                            debug_assert!(
                                src_state.knowledge[i].is_subset(&src_state.knowledge[src])
                            );
                            debug_assert!(
                                state.knowledge[i].is_superset(&src_state.knowledge[i])
                                    || state.knowledge[i].is_subset(&src_state.knowledge[i])
                            );

                            // Merge knowledge
                            state.knowledge[i].union_with(&src_state.knowledge[i]);
                            state.remote_states[i] =
                                max(state.remote_states[i], src_state.remote_states[i]);

                            // Check obtained knowledge
                            debug_assert_eq!(
                                state.knowledge[i] == src_state.knowledge[i],
                                state.remote_states[i] == src_state.remote_states[i]
                            );
                            debug_assert!(state.knowledge[i].is_subset(&state.knowledge[current]));
                        }

                        let ks = state.clone();

                        // Update knowledge of future states at current location.
                        for (time, state) in states[current].range_mut(current_time..).skip(1) {
                            assert!(*time > current_time);
                            for i in 0..nb_nodes {
                                state.knowledge[i].union_with(&ks.knowledge[i]);
                                state.remote_states[i] =
                                    max(state.remote_states[i], ks.remote_states[i]);
                            }
                            // TODO: recursively follow existing paths to propagate knowledge further
                            //   and add checks to continue to next triangle early when possible
                        }
                    }
                }
            }
            // Finished adding messages.

            // Set frozen tags
            for node in 0..nb_nodes {
                let final_node_state = states[node].iter_mut().last().expect("should have state");
                let final_node_time = *final_node_state.0;
                for other_state in states.iter_mut() {
                    for (_, state) in other_state.iter_mut() {
                        if state.remote_states[node] == final_node_time {
                            state.frozen.insert(node);
                        }
                    }
                }
            }

            // Sanity checks:
            assert_eq!(should_include_value.len(), nb_nodes - 1);
            debug_assert!({
                let final_leader_state =
                    states[leader].last_key_value().expect("should have state");
                final_leader_state.0 == &max_latency
                    && final_leader_state.1.knowledge
                        == knowledge_levels[leader][best_levels[leader]].1
            });

            // Add to list of graphs/latencies
            assert_eq!(kcensus_latencies.len(), leader);
            assert_eq!(propagation_graphs.len(), leader);
            kcensus_latencies.push(max_latency);
            propagation_graphs.push(PropagationGraph {
                should_include_value,
                states,
                leader,
            });
        }
    }

    PropagationGraphs {
        graphs: propagation_graphs,
        topology,
        rtts,
        path_rtts,
        kcensus_latencies,
        paxos_latencies,
        epaxos_latencies,
        multi_paxos_latencies,
        multi_paxos_3p_latencies,
    }
}
