use crate::kcensus::node_state::Knowledge;
use crate::kcensus::round_state::RoundState;
use crate::topology::Topology;
use bit_set::BitSet;
use log::trace;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::time::Duration;

type ProcId = usize;

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
    with_value: bool,
}

impl Display for MessageInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}val={},deps=[", "{", self.with_value)?;
        for (i, x) in self.dependencies.iter().enumerate() {
            write!(f, "{}{}", if i == 0 { "" } else { ", " }, x)?;
        }
        write!(f, "],needed_by=[")?;
        for (i, x) in self.needed_by.iter().enumerate() {
            write!(f, "{}{}", if i == 0 { "" } else { ", " }, x)?;
        }
        write!(f, "]{}", "}")?;
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
    pub fn get_with_value(&self) -> bool {
        self.with_value
    }
}

#[derive(Debug)]
struct PropagationGraph {
    start_messages: Vec<MessageId>,
    graph: HashMap<MessageId, MessageInfo>,
}

#[derive(Debug)]
pub struct PropagationGraphs(Vec<PropagationGraph>);

impl PropagationGraphs {
    #[inline]
    pub fn get_by_id(&self, msg_id: &MessageId) -> &MessageInfo {
        &self.0[msg_id.proposer].graph[&msg_id]
    }

    #[inline]
    pub fn get_start(&self, proposer: ProcId) -> &[MessageId] {
        &self.0[proposer].start_messages
    }

    pub fn from_topology(topology: &Topology) -> Self {
        compute_propagation_graphs(topology)
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

fn compute_propagation_graphs(topology: &Topology) -> PropagationGraphs {
    let mut propagation_graphs = Vec::with_capacity(topology.nb_nodes);
    for proposer in 0..topology.nb_nodes {
        trace!("proposer: {}", proposer);
        let mut triangular_paths: Vec<TriangularPath> =
            Vec::with_capacity(topology.nb_nodes * topology.nb_nodes);
        for first in 0..topology.nb_nodes {
            let latency_to_first = topology.path_latencies[proposer][first];
            for second in 0..topology.nb_nodes {
                let total_latency = latency_to_first
                    + topology.path_latencies[first][second]
                    + topology.path_latencies[second][proposer];
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

        // Used to ensure the raw value is sent to everyone (not for knowledge spreading)
        let mut value_only_paths: Vec<TriangularPath> = Vec::with_capacity(topology.nb_nodes);
        for node in 0..topology.nb_nodes {
            let total_latency =
                topology.path_latencies[proposer][node] + topology.path_latencies[node][proposer];
            value_only_paths.push(TriangularPath {
                first: node,
                second: proposer,
                total_latency,
            })
        }
        value_only_paths.sort_by_key(triangle_latency);

        // Simulate gossip until commit
        let mut round_state = RoundState::new(topology.nb_nodes, proposer);
        let mut count = 0;
        while !round_state.can_commit() {
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
        println!(
            "{}'s expected commit time: {:?}",
            proposer,
            triangular_paths[count - 1].total_latency
        );

        // TODO: Some triangles might still not be needed to commit.
        //   Try to check if they are needed for can_commit? (can be merged with bellow logic?)
        //   (Easy case: detect if e-paxos quorum)

        round_state.clear();
        let mut message_times: Vec<Vec<BTreeSet<Duration>>> =
            vec![vec![BTreeSet::new(); topology.nb_nodes]; topology.nb_nodes];
        let mut message_graph: HashMap<MessageId, MessageInfo> = HashMap::new();
        let mut start_messages: Vec<MessageId> = Vec::new();
        // Remove extra triangles
        let mut i = count; // desc
        let mut j = 0; // asc
        while j < topology.nb_nodes {
            let value_only_path = i == 0;
            let t = if !value_only_path {
                i -= 1;
                &triangular_paths[i]
            } else {
                j += 1;
                &value_only_paths[j - 1]
            };

            if !value_only_path {
                if round_state.can_commit() {
                    i = 0;
                    trace!("Messages in graph (before adding value_only paths): ");
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

            let mut k: Knowledge = BitSet::with_capacity(topology.nb_nodes);
            k.insert(proposer);

            let mut current = proposer;
            let mut prev_msg_id = None;
            let mut shortest_path_from_proposer = true;
            let mut shortest_path_to_proposer = false;

            let steps = [t.first, t.second, proposer];
            for step in 0..3 {
                let goal = steps[step];
                if step > 0 && goal == proposer {
                    shortest_path_from_proposer = false;
                    shortest_path_to_proposer = true;
                }

                while current != goal {
                    let src = current;
                    current = topology.next_src[current][goal];
                    if t.total_latency > max_latency && value_only_path && goal == proposer {
                        // Go back directly to limit message count
                        current = goal;
                    }
                    debug_assert!(src != current);
                    k.insert(current);
                    round_state.learn(current, &k);

                    if step > 0 {
                        shortest_path_from_proposer &= topology.prev_dest[proposer][current] == src;
                        let left = topology.path_latencies[src][goal]
                            + topology.path_latencies[goal][proposer];
                        let to_prop = topology.path_latencies[src][proposer];
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
                        debug_assert_eq!(dependencies.is_empty(), src == proposer);

                        // Insert new message
                        let inserted = message_graph
                            .insert(
                                msg_id,
                                MessageInfo {
                                    dependencies,
                                    needed_by: vec![],
                                    with_value: shortest_path_from_proposer,
                                },
                            )
                            .is_none();
                        debug_assert!(inserted);
                        let inserted = message_times[src][current].insert(time);
                        debug_assert!(inserted);
                        msg_id
                    };

                    max_slack = deadline - msg_id.time;
                    current_time = msg_id.time + topology.link_latencies[src][current];
                    prev_msg_id = Some(msg_id);
                }
            }
        }

        debug_assert!(propagation_graphs.len() == proposer);
        propagation_graphs.push(PropagationGraph {
            start_messages,
            graph: message_graph,
        });
    }
    PropagationGraphs(propagation_graphs)
}
