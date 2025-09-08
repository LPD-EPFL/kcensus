use crate::consensus::kcensus::node_state::Knowledge;
use crate::consensus::kcensus::round_state::KCensusRoundState;
use crate::topology;
use crate::topology::Topology;
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
}

impl PropagationGraphs {
    #[inline]
    pub fn get_by_id(&self, msg_id: &MessageId) -> &MessageInfo {
        &self.graphs[msg_id.proposer].graph[msg_id]
    }

    #[inline]
    pub fn get_start(&self, proposer: ProcId) -> &[MessageId] {
        &self.graphs[proposer].start_messages
    }

    pub fn can_commit(&self, proposer: ProcId, received: &HashSet<MessageId>) -> bool {
        self.graphs[proposer].end_messages.is_subset(&received)
    }
}

#[derive(Debug)]
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

pub fn compute_propagation_graphs(
    topology: &Topology,
    kcensus_graph: bool,
    shortest_paths: bool,
    single_proposer: Option<usize>,
) -> PropagationGraphs {
    let nb_nodes = topology.nb_nodes;
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
                    path_latencies[src][dest] = topology::FAULTY_LATENCY;
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

    let mut propagation_graphs = Vec::with_capacity(nb_nodes);
    let mut kcensus_latencies = Vec::with_capacity(nb_nodes);
    let mut paxos_latencies = Vec::with_capacity(nb_nodes);
    let mut epaxos_latencies = Vec::with_capacity(nb_nodes);
    let mut multi_paxos_latencies = Vec::with_capacity(nb_nodes);

    for proposer in 0..nb_nodes {
        trace!("proposer: {}", proposer);

        let mut proposer_round_trips = rtts[proposer].clone();
        proposer_round_trips.sort();
        let majority = 1 + nb_nodes / 2;
        paxos_latencies.push(proposer_round_trips[majority - 1] * 2);
        let e_paxos_quorum = ((nb_nodes * 3) / 4).max(majority);
        epaxos_latencies.push(proposer_round_trips[e_paxos_quorum - 1]);
        let multi_paxos_latency = (0..nb_nodes)
            .map(|requester| rtts[requester][proposer] + proposer_round_trips[majority - 1])
            .collect();
        multi_paxos_latencies.push(multi_paxos_latency);

        if !kcensus_graph
            || topology.faults.contains(proposer)
            || single_proposer.is_some_and(|p| p != proposer)
        {
            propagation_graphs.push(PropagationGraph {
                start_messages: Vec::new(),
                graph: HashMap::new(),
                end_messages: HashSet::new(),
            });
            kcensus_latencies.push(topology::FAULTY_LATENCY);
            continue;
        }

        let mut triangular_paths: Vec<TriangularPath> = Vec::with_capacity(nb_nodes * nb_nodes);
        for first in 0..nb_nodes {
            let latency_to_first = path_latencies[proposer][first];
            for second in 0..nb_nodes {
                let total_latency = latency_to_first
                    + path_latencies[first][second]
                    + path_latencies[second][proposer];
                triangular_paths.push(TriangularPath {
                    first,
                    second,
                    total_latency,
                });
            }
        }
        triangular_paths.sort_by_key(triangle_latency);
        // trace!("triangular_paths: {}", triangular_paths.len());
        trace!(
            "longest triangular path: {}",
            triangular_paths.last().unwrap()
        );

        // Used to ensure the value is sent to everyone (not for knowledge spreading)
        let mut value_only_paths: Vec<TriangularPath> = Vec::with_capacity(nb_nodes);
        for node in 0..nb_nodes {
            let total_latency = path_latencies[proposer][node] + path_latencies[node][proposer];
            value_only_paths.push(TriangularPath {
                first: node,
                second: proposer,
                total_latency,
            })
        }
        value_only_paths.sort_by_key(triangle_latency);

        // Simulate gossip until commit
        let mut round_state = KCensusRoundState::new(nb_nodes, proposer);
        let mut count = 0;
        while !round_state.can_commit(None) {
            let t = &triangular_paths[count];
            round_state.partial_learn(t.second, t.first);
            count += 1;
        }

        // Truncate at commit time
        triangular_paths.truncate(count);
        let max_latency = triangular_paths[count - 1].total_latency;
        trace!(
            "Left after truncate: {} real triangles, {} total",
            triangular_paths
                .iter()
                .filter(|x| x.first != proposer && x.second != proposer && x.first != x.second)
                .count(),
            triangular_paths.len()
        );
        trace!("longest path: {}", triangular_paths[count - 1]);

        // TODO: Some triangles might still not be needed to commit.
        //   Try to check if they are needed for can_commit? (can be merged with bellow logic?)
        //   (Easy case: detect if e-paxos quorum)

        round_state.clear();
        let mut message_times: Vec<Vec<BTreeSet<Duration>>> =
            vec![vec![BTreeSet::new(); nb_nodes]; nb_nodes];
        let mut message_graph: HashMap<MessageId, MessageInfo> = HashMap::new();
        let mut start_messages: Vec<MessageId> = Vec::new();
        // Remove extra triangles
        let mut i = nb_nodes; // first: send values (desc order, but does not matter)
        let mut j = count; // second: triangles to commit, from longest to shortest (desc)
        while 0 < j {
            let value_only_path = 0 < i;
            let t = if value_only_path {
                i -= 1;
                &value_only_paths[i]
            } else {
                j -= 1;
                &triangular_paths[j]
            };

            if !value_only_path {
                if round_state.can_commit(None) {
                    j = 0;
                    trace!("Messages in graph (before adding \"value only\" paths): ");
                    let mut messages = message_graph.iter().collect::<Vec<_>>();
                    messages.sort_by_key(|(x, _)| (x.time, x.src, x.dest));
                    for msg in messages.into_iter() {
                        trace!("    - {}: {}", msg.0, msg.1);
                    }
                    continue;
                }
                if round_state.knows(t.second, t.first) {
                    continue;
                }
            }

            // How much delay we can add (Note: "value only" paths can be longer than max)
            let mut max_slack = if t.total_latency < max_latency {
                max_latency - t.total_latency
            } else {
                Duration::default()
            };
            let mut current_time = Duration::default();

            let mut k: Knowledge = BitSet::with_capacity(nb_nodes);
            k.insert(proposer);

            let mut current = proposer;
            let mut prev_msg_id = None;
            let mut shortest_path_from_proposer = true;
            let mut shortest_path_to_proposer = false;

            let checkpoints = if value_only_path {
                [t.first, t.first, t.first]
            } else {
                [t.first, t.second, proposer]
            };
            for (step, target) in checkpoints.into_iter().enumerate() {
                if step > 0 && target == proposer {
                    shortest_path_from_proposer = false;
                    shortest_path_to_proposer = true;
                }

                while current != target {
                    let src = current;
                    current = next_src[current][target];
                    if t.total_latency > max_latency && value_only_path && target == proposer {
                        // Go back directly to limit message count
                        current = target;
                    }
                    debug_assert!(src != current);
                    if !value_only_path {
                        k.insert(current);
                        round_state.learn(current, &k);
                    }

                    if step > 0 {
                        shortest_path_from_proposer &= prev_dest[proposer][current] == src;

                        let left = path_latencies[src][target] + path_latencies[target][proposer];
                        let to_prop = path_latencies[src][proposer];
                        shortest_path_to_proposer |= left == to_prop;
                    }

                    let deadline = current_time + max_slack;
                    let msg_id = if let Some(time) = message_times[src][current]
                        .range(current_time..=deadline)
                        .next()
                    {
                        // Reusing a message that is compatible with the time window!
                        // Reconstruct id:
                        let msg_id = MessageId {
                            proposer,
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
                                let prev_msg_info = message_graph.get_mut(&prev_msg_id).unwrap();
                                prev_msg_info.needed_by.push(msg_id);
                            }
                        }
                        msg_id
                    } else {
                        // New message!

                        let time = if shortest_path_from_proposer || !shortest_path_to_proposer {
                            current_time
                        } else {
                            // Delay when it might make it more likely to be reused!
                            deadline
                        };

                        // Construct id
                        let msg_id = MessageId {
                            proposer,
                            src,
                            dest: current,
                            time,
                        };

                        if time == Duration::default() {
                            debug_assert!(src == proposer);
                            debug_assert!(shortest_path_from_proposer);
                            start_messages.push(msg_id);
                        }

                        // Potentially add dependency links:
                        let dependencies = match prev_msg_id {
                            None => HashSet::new(),
                            Some(prev_msg_id) => {
                                debug_assert!(
                                    !message_graph[&prev_msg_id].needed_by.contains(&msg_id)
                                );
                                let prev_msg_info = message_graph.get_mut(&prev_msg_id).unwrap();
                                prev_msg_info.needed_by.push(msg_id);
                                let mut dep = HashSet::with_capacity(1);
                                dep.insert(prev_msg_id);
                                dep
                            }
                        };
                        debug_assert_eq!(dependencies.is_empty(), time == Duration::default());
                        debug_assert!(!dependencies.is_empty() || src == proposer);

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
            if dest == proposer {
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
                proposer,
                src: first_src.unwrap(),
                dest,
                time: first_msg_time.unwrap(),
            };
            assert!(message_graph[&first_msg_id].includes_new_values);
        }

        debug_assert!(kcensus_latencies.len() == proposer);
        debug_assert!(propagation_graphs.len() == proposer);
        kcensus_latencies.push(max_latency);
        let end_messages = message_graph
            .keys()
            .copied()
            .filter(|id| id.dest == proposer)
            .collect();
        propagation_graphs.push(PropagationGraph {
            start_messages,
            graph: message_graph,
            end_messages,
        });
    }

    PropagationGraphs {
        graphs: propagation_graphs,
        rtts,
        path_rtts,
        kcensus_latencies,
        paxos_latencies,
        epaxos_latencies,
        multi_paxos_latencies,
    }
}
