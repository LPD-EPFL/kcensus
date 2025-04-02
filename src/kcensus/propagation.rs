use crate::kcensus::node_state::Knowledge;
use crate::kcensus::round_state::RoundState;
use crate::topology::Topology;
use bit_set::BitSet;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

type ProcId = usize;

#[derive(Ord, PartialOrd, Eq, PartialEq, Hash, Debug, Copy, Clone)]
pub struct MessageId {
    proposer: ProcId,
    src: ProcId,
    time: Duration,
}

struct MessageInfo {
    dependencies: HashSet<MessageId>,
    needed_by: Vec<MessageId>,
    dest: Vec<ProcId>,
    with_value: bool,
}

pub struct PropagationGraphs(Vec<HashMap<MessageId, MessageInfo>>);

impl PropagationGraphs {
    pub fn get_by_id(&self, msg_id: &MessageId) -> &MessageInfo {
        &self.0[msg_id.proposer][&msg_id]
    }

    pub fn get_start(&self, proposer: ProcId) -> &MessageInfo {
        let start_id = MessageId {
            proposer,
            src: proposer,
            time: Duration::default(),
        };
        &self.0[proposer][&start_id]
    }
}

struct Triangle {
    first: ProcId,
    second: ProcId,
    total_latency: Duration,
}

fn triangle_latency(t: &Triangle) -> Duration {
    t.total_latency
}
pub fn compute_propagation_graphs(topology: &Topology) -> PropagationGraphs {
    let mut propagation_graphs = Vec::with_capacity(topology.nb_nodes);
    for proposer in 0..topology.nb_nodes {
        let mut triangles: Vec<Triangle> =
            Vec::with_capacity(topology.nb_nodes * topology.nb_nodes);
        for first in 0..topology.nb_nodes {
            let latency_to_first = topology.path_latencies[proposer][first];
            for second in 0..topology.nb_nodes {
                let total_latency = latency_to_first
                    + topology.path_latencies[first][second]
                    + topology.path_latencies[second][proposer];
                triangles.push(Triangle {
                    first,
                    second,
                    total_latency,
                });
            }
        }
        triangles.sort_by_key(triangle_latency);

        // Simulate gossip until commit
        let mut round_state = RoundState::new(topology.nb_nodes, proposer);
        let mut count = 0;
        while !round_state.can_commit() {
            let t = &triangles[count];
            round_state.partial_learn(t.second, t.first);
            count += 1;
        }

        // Truncate at commit time
        triangles.truncate(count);
        let max_latency = triangles[count - 1].total_latency;

        // TODO: Some triangles might still not be needed to commit.
        //   Trim ones if not needed for can_commit ? (can be merged with bellow logic)

        round_state.clear();
        let mut message_times: Vec<Vec<BTreeSet<Duration>>> =
            vec![vec![BTreeSet::new(); topology.nb_nodes]; topology.nb_nodes];
        let mut message_graph: HashMap<MessageId, MessageInfo> = HashMap::new();
        // Remove extra triangles
        let mut i = count;
        while i > 0 {
            i -= 1;
            let t = &triangles[i];

            if round_state.knows(t.second, t.first) || round_state.can_commit() {
                triangles.remove(i);
                continue;
            }

            // How much delay we can add
            let mut slack = max_latency - t.total_latency;
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
                if step == 2 || step == 1 && goal == proposer {
                    shortest_path_to_proposer = true;
                }

                while current != *goal {
                    let src = current;
                    current = topology.next_src[current][goal];
                    k.insert(current);
                    round_state.learn(current, &k);

                    shortest_path_from_proposer &= topology.prev_dest[proposer][current] == src;
                    let left = topology.path_latencies[current][goal]
                        + topology.path_latencies[goal][proposer];
                    let to_prop = topology.path_latencies[current][proposer];
                    shortest_path_to_proposer |= step == 1 && left == to_prop;

                    let deadline = current_time + slack;
                    let msg_id = if let Some(time) = message_times[src][current]
                        .range(current_time..=deadline)
                        .next()
                    {
                        // Reusing a message that is compatible with the time window!
                        // Reconstruct id:
                        let msg_id = MessageId {
                            proposer,
                            src,
                            time: *time,
                        };
                        // Potentially add dependency links:
                        if let Some(prev_msg_id) = prev_msg_id {
                            if message_graph[&msg_id].dependencies.insert(prev_msg_id) {
                                debug_assert!(
                                    !message_graph[&prev_msg_id].needed_by.contains(&msg_id)
                                );
                                message_graph[&prev_msg_id].needed_by.push(msg_id);
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
                            time,
                        };

                        // Potentially add dependency links:
                        let dependencies = match prev_msg_id {
                            None => HashSet::new(),
                            Some(prev_msg) => {
                                debug_assert!(
                                    !message_graph[&prev_msg_id].needed_by.contains(&msg_id)
                                );
                                message_graph[&prev_msg].needed_by.push(msg_id);
                                let mut dep = HashSet::with_capacity(1);
                                dep.insert(prev_msg);
                                dep
                            }
                        };

                        let with_value = shortest_path_from_proposer;

                        // Insert new message
                        let inserted = message_graph
                            .insert(
                                msg_id,
                                MessageInfo {
                                    dependencies,
                                    needed_by: vec![],
                                    dest: vec![current],
                                    with_value,
                                },
                            )
                            .is_none();
                        debug_assert!(inserted);
                        let inserted = message_times[src][current].insert(time);
                        debug_assert!(inserted);
                        msg_id
                    };

                    slack = deadline - msg_id.time;
                    current_time = msg_id.time + topology.link_latencies[src][current];
                    prev_msg_id = Some(msg_id);
                }
            }
        }
        debug_assert!(propagation_graphs.len() == proposer);
        propagation_graphs.push(message_graph);
    }
    PropagationGraphs(propagation_graphs)
}
