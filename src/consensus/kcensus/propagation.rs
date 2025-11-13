use crate::consensus::kcensus::node_state::Knowledge;
use crate::topology::{Topology, FAULTY_LATENCY};
use bit_set::BitSet;
use log::trace;
use petgraph::algo::bellman_ford;
use petgraph::matrix_graph::DiMatrix;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::time::Duration;

type ProcId = usize;

type NetworkGraph = DiMatrix<(), f64, Option<f64>, usize>;

#[derive(Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Copy, Clone, Serialize, Deserialize)]
pub struct MessageId {
    pub leader: ProcId,
    pub src: ProcId,
    pub dest: ProcId,
    time: Duration,
}

impl Display for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(p{} at {:?}: {} -> {})",
            self.leader, self.time, self.src, self.dest
        )
    }
}

#[derive(Debug)]
pub struct MessageInfo {
    dependencies: HashSet<MessageId>,
    needed_by: Vec<MessageId>,
    includes_new_values: bool,
}

impl Display for MessageInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{val={},deps=[", self.includes_new_values)?;
        for (i, x) in self.dependencies.iter().enumerate() {
            write!(f, "{}{}", if i == 0 { "" } else { ", " }, x)?;
        }
        write!(f, "],needed_by=[")?;
        for (i, x) in self.needed_by.iter().enumerate() {
            write!(f, "{}{}", if i == 0 { "" } else { ", " }, x)?;
        }
        write!(f, "]}}")?;
        Ok(())
    }
}

impl MessageInfo {
    #[inline]
    pub fn get_dependencies(&self) -> &HashSet<MessageId> {
        &self.dependencies
    }

    #[inline]
    pub fn get_needed_by(&self) -> &[MessageId] {
        &self.needed_by
    }

    #[inline]
    pub fn get_includes_new_values(&self) -> bool {
        self.includes_new_values
    }
}

#[derive(Debug)]
struct PropagationGraph {
    start_messages: Vec<MessageId>,
    graph: HashMap<MessageId, MessageInfo>,
    end_messages: HashSet<MessageId>,
}

#[derive(Debug)]
pub struct PropagationGraphs {
    graphs: Vec<PropagationGraph>,
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
    pub fn get_by_id(&self, msg_id: &MessageId) -> &MessageInfo {
        &self.graphs[msg_id.leader].graph[msg_id]
    }

    #[inline]
    pub fn get_start(&self, leader: ProcId) -> &[MessageId] {
        &self.graphs[leader].start_messages
    }

    pub fn can_commit(&self, leader: ProcId, received: &HashSet<MessageId>) -> bool {
        self.graphs[leader].end_messages.is_subset(received)
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
    topology: &Topology,
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

        for leader in 0..nb_nodes {
            if topology.faults.contains(leader) {
                knowledge_levels[leader].push((Duration::default(), Vec::new(), 0));
                continue;
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
            for ti in 0..triangular_paths[leader].len() {
                let t = &triangular_paths[leader][ti];
                if t.total_latency > max_lat {
                    break;
                }
                knowledges[t.second].insert(t.first);
                if t.total_latency < min_lat {
                    continue;
                }
                let next_t = triangular_paths[leader].get(ti + 1);
                if next_t.is_some_and(|nt| nt.total_latency == t.total_latency) {
                    continue;
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

        // Implements recursive search of the optimal solution
        fn best_avg_search(
            best_levels: &mut Vec<usize>,
            best_total_time: &mut Duration,
            pids_done: usize,
            prev_levels: &Vec<usize>,
            prev_partial_total_time: Duration,
            nb_nodes: usize,
            max_levels: &Vec<usize>,
            knowledge_levelss: &Vec<Vec<(Duration, Vec<Knowledge>, usize)>>,
            compatible_levelss: &Vec<Vec<Vec<usize>>>,
        ) {
            let pid_a = pids_done;
            let pids_done = pids_done + 1;
            for level_a in prev_levels[pid_a]..=max_levels[pid_a] {
                let mut new_levels = prev_levels.clone();
                new_levels[pid_a] = level_a;
                let new_partial_total_time =
                    prev_partial_total_time + knowledge_levelss[pid_a][level_a].0;
                let mut new_curr_total_time = new_partial_total_time;
                let mut new_min_total_time = new_partial_total_time;
                for pid_b in pids_done..nb_nodes {
                    new_min_total_time += knowledge_levelss[pid_b][prev_levels[pid_b]].0;
                    let req_level_b = compatible_levelss[pid_a][level_a][pid_b];
                    if req_level_b > new_levels[pid_b] {
                        new_levels[pid_b] = req_level_b;
                    }
                    new_curr_total_time += knowledge_levelss[pid_b][new_levels[pid_b]].0;
                }

                if *best_total_time <= new_min_total_time {
                    // We can not find a better solution with higher level_a
                    break;
                }
                if *best_total_time <= new_curr_total_time {
                    // We can not find a better solution with current level_a
                    continue;
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
                        knowledge_levelss,
                        compatible_levelss,
                    );
                }
            }
        }

        // Initialize best to a trivial valid solution
        let mut best_levels = max_levels.clone();
        let mut best_total_time = Duration::default();
        for leader in 0..nb_nodes {
            best_total_time += knowledge_levels[leader][max_levels[leader]].0;
        }

        // Use recursive search to compute the best solution
        best_avg_search(
            &mut best_levels,
            &mut best_total_time,
            0,
            &vec![0usize; nb_nodes],
            Duration::default(),
            nb_nodes,
            &max_levels,
            &knowledge_levels,
            &compatible_levels,
        );

        // Build the graph from the solutions
        for leader in 0..nb_nodes {
            if topology.faults.contains(leader) {
                propagation_graphs.push(PropagationGraph {
                    start_messages: Vec::new(),
                    graph: HashMap::new(),
                    end_messages: HashSet::new(),
                });
                kcensus_latencies.push(FAULTY_LATENCY);
                continue;
            }

            // Truncate at commit time
            let max_latency = knowledge_levels[leader][best_levels[leader]].0;
            let triangle_count = knowledge_levels[leader][best_levels[leader]].2;
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

            // TODO: Some triangles might still not be needed to commit.
            //   Try to check if they are needed for are_compatible? (can be merged with bellow logic?)

            let mut simulated_knowledges: Vec<Knowledge> =
                vec![BitSet::with_capacity(nb_nodes); nb_nodes];

            let mut message_times: Vec<Vec<BTreeSet<Duration>>> =
                vec![vec![BTreeSet::new(); nb_nodes]; nb_nodes];
            let mut message_graph: HashMap<MessageId, MessageInfo> = HashMap::new();
            let mut start_messages: Vec<MessageId> = Vec::new();
            // Remove extra triangles
            let mut i = value_only_paths[leader].len(); // first: send values (desc order, but does not matter)
            let mut j = triangle_count; // second: triangles to commit, from longest to shortest (desc)
            while 0 < j {
                let value_only_path = 0 < i;
                let t = if value_only_path {
                    i -= 1;
                    &value_only_paths[leader][i]
                } else {
                    j -= 1;
                    &triangular_paths[leader][j]
                };

                if !value_only_path && simulated_knowledges[t.second].contains(t.first) {
                    continue;
                }

                // How much delay we can add (Note: "value only" paths can be longer than max)
                let mut max_slack = if t.total_latency < max_latency {
                    max_latency - t.total_latency
                } else {
                    Duration::default()
                };
                let mut current_time = Duration::default();

                let mut k: Knowledge = BitSet::with_capacity(nb_nodes);
                k.insert(leader);

                let mut current = leader;
                let mut prev_msg_id = None;
                let mut shortest_path_from_leader = true;
                let mut shortest_path_to_leader = false;

                let checkpoints = if value_only_path {
                    [t.first, t.first, t.first]
                } else {
                    [t.first, t.second, leader]
                };
                for (step, target) in checkpoints.into_iter().enumerate() {
                    if step > 0 && target == leader {
                        shortest_path_from_leader = false;
                        shortest_path_to_leader = true;
                    }

                    while current != target {
                        let src = current;
                        current = next_src[current][target];
                        if t.total_latency > max_latency && value_only_path && target == leader {
                            // Go back directly to limit message count
                            current = target;
                        }
                        debug_assert!(src != current);
                        if !value_only_path {
                            k.insert(current);
                            simulated_knowledges[current].union_with(&k);
                        }

                        if step > 0 {
                            shortest_path_from_leader &= prev_dest[leader][current] == src;

                            let left = path_latencies[src][target] + path_latencies[target][leader];
                            let to_prop = path_latencies[src][leader];
                            shortest_path_to_leader |= left == to_prop;
                        }

                        let deadline = current_time + max_slack;
                        let msg_id = if let Some(time) = message_times[src][current]
                            .range(current_time..=deadline)
                            .next()
                        {
                            // Reusing a message that is compatible with the time window!
                            // Reconstruct id:
                            let msg_id = MessageId {
                                leader,
                                src,
                                dest: current,
                                time: *time,
                            };
                            // Potentially add dependency links:
                            if let Some(prev_msg_id) = prev_msg_id {
                                let msg_info = message_graph.get_mut(&msg_id).unwrap();
                                if msg_info.dependencies.insert(prev_msg_id) {
                                    debug_assert!(
                                        !message_graph[&prev_msg_id].needed_by.contains(&msg_id)
                                    );
                                    let prev_msg_info =
                                        message_graph.get_mut(&prev_msg_id).unwrap();
                                    prev_msg_info.needed_by.push(msg_id);
                                }
                            }
                            msg_id
                        } else {
                            // New message!

                            let time = if shortest_path_from_leader || !shortest_path_to_leader {
                                current_time
                            } else {
                                // Delay when it might make it more likely to be reused!
                                deadline
                            };

                            // Construct id
                            let msg_id = MessageId {
                                leader,
                                src,
                                dest: current,
                                time,
                            };

                            if time == Duration::default() {
                                debug_assert!(src == leader);
                                debug_assert!(shortest_path_from_leader);
                                start_messages.push(msg_id);
                            }

                            // Potentially add dependency links:
                            let dependencies = match prev_msg_id {
                                None => HashSet::new(),
                                Some(prev_msg_id) => {
                                    debug_assert!(
                                        !message_graph[&prev_msg_id].needed_by.contains(&msg_id)
                                    );
                                    let prev_msg_info =
                                        message_graph.get_mut(&prev_msg_id).unwrap();
                                    prev_msg_info.needed_by.push(msg_id);
                                    let mut dep = HashSet::with_capacity(1);
                                    dep.insert(prev_msg_id);
                                    dep
                                }
                            };
                            debug_assert_eq!(dependencies.is_empty(), time == Duration::default());
                            debug_assert!(!dependencies.is_empty() || src == leader);

                            // Insert new message
                            let inserted = message_graph
                                .insert(
                                    msg_id,
                                    MessageInfo {
                                        dependencies,
                                        needed_by: vec![],
                                        includes_new_values: value_only_path,
                                    },
                                )
                                .is_none();
                            debug_assert!(inserted);
                            let inserted = message_times[src][current].insert(time);
                            debug_assert!(inserted);
                            msg_id
                        };

                        max_slack = deadline - msg_id.time;
                        current_time = msg_id.time + topology.link_latency(src, current);
                        prev_msg_id = Some(msg_id);
                    }
                }
            }

            // TODO: This is only an assertion check
            for dest in 0..nb_nodes {
                if dest == leader {
                    continue;
                }
                let mut first_msg_time = None;
                let mut first_receive_time = None;
                let mut first_src = None;
                for (src, src_message_times) in message_times.iter().enumerate() {
                    let msg_time = src_message_times[dest].first().copied();
                    if let Some(msg_time) = msg_time {
                        let receive_time = msg_time + topology.link_latency(src, dest);
                        if first_receive_time.is_none() || Some(receive_time) < first_receive_time {
                            first_msg_time = Some(msg_time);
                            first_receive_time = Some(receive_time);
                            first_src = Some(src);
                        }
                    }
                }
                let first_msg_id = MessageId {
                    leader,
                    src: first_src.unwrap(),
                    dest,
                    time: first_msg_time.unwrap(),
                };
                assert!(message_graph[&first_msg_id].includes_new_values);
            }

            debug_assert!(kcensus_latencies.len() == leader);
            debug_assert!(propagation_graphs.len() == leader);
            kcensus_latencies.push(max_latency);
            let end_messages = message_graph
                .keys()
                .copied()
                .filter(|id| id.dest == leader)
                .collect();
            propagation_graphs.push(PropagationGraph {
                start_messages,
                graph: message_graph,
                end_messages,
            });
        }
    }

    PropagationGraphs {
        graphs: propagation_graphs,
        rtts,
        path_rtts,
        kcensus_latencies,
        paxos_latencies,
        epaxos_latencies,
        multi_paxos_latencies,
        multi_paxos_3p_latencies,
    }
}
