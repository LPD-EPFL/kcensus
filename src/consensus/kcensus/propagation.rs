use crate::consensus::kcensus::node_state::{Knowledge, NodeState};
use crate::topology::{Topology, FAULTY_LATENCY};
use bit_set::BitSet;
use log::{debug, info, trace};
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
}

#[derive(Debug)]
struct PropagationGraph {
    should_include_value: HashSet<MessageId>,
    states: Vec<BTreeMap<Duration, KnowledgeState>>,
    leader: ProcId,
}

/// KCensus' plan: the propagation graph per proposer, and the latency each process should
/// expect. Produced by [`kcensus_plan`]; the other algorithms have their own plan structs and
/// never build these graphs.
#[derive(Debug)]
pub struct PropagationGraphs {
    graphs: Vec<PropagationGraph>,
    topology: Topology,
    pub latencies: Vec<Duration>,
}

impl PropagationGraphs {
    #[inline]
    pub fn should_include_value(&self, msg_id: &MessageId) -> bool {
        self.graphs[msg_id.proposer]
            .should_include_value
            .contains(msg_id)
    }

    #[inline]
    fn find_state(&self, proposer: ProcId, node: ProcId, state_id: Duration) -> &KnowledgeState {
        self.graphs[proposer].states[node]
            .get(&state_id)
            .expect("state should be found")
    }

    #[inline]
    pub fn get_knowledge(&self, proposer: ProcId, node: ProcId, state_id: Duration) -> &Knowledge {
        &self.find_state(proposer, node, state_id).knowledge[node]
    }

    #[inline]
    pub fn final_state(&self, proposer: ProcId, node: ProcId, state_id: Duration) -> bool {
        *self.graphs[proposer].states[node].iter().last().unwrap().0 == state_id
    }

    #[inline]
    pub fn get_leader(&self, proposer: ProcId) -> ProcId {
        self.graphs[proposer].leader
    }

    #[inline]
    pub fn get_final_knowledge(&self, proposer: ProcId) -> &[Knowledge] {
        let leader = self.graphs[proposer].leader;
        &self.graphs[proposer].states[leader]
            .iter()
            .last()
            .unwrap()
            .1
            .knowledge
    }

    #[inline]
    pub fn get_final_state_id(&self, proposer: ProcId, node: ProcId) -> Duration {
        *self.graphs[proposer].states[node].iter().last().unwrap().0
    }

    #[inline]
    pub fn build_remote_state(&self, v: usize, msg_id: MessageId, pid: usize) -> NodeState {
        let mut remote_state = NodeState::new(pid);

        let remote_state_id =
            self.graphs[msg_id.proposer].states[msg_id.src][&msg_id.time].remote_states[pid];
        if remote_state_id != Duration::ZERO || pid == msg_id.proposer {
            remote_state.accept_with_k_state(v, msg_id.proposer, remote_state_id, self)
        }

        remote_state
    }

    #[inline]
    pub fn next_state(
        &self,
        proposer: ProcId,
        node: ProcId,
        prev_state_id: Duration,
    ) -> Option<(Duration, &HashSet<MessageId>)> {
        self.graphs[proposer].states[node]
            .range(prev_state_id..)
            .nth(1)
            .map(|(time, ks)| (*time, &ks.dependencies))
    }

    pub fn msg_arrival_state_id(&self, msg_id: MessageId) -> Duration {
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

fn is_valid_knowledge(pid: usize, knowledge: &[Knowledge], min_quorum: usize) -> bool {
    let nb_nodes = knowledge.len();
    let mut quorum = BitSet::with_capacity(nb_nodes);
    for k in knowledge.iter() {
        quorum.union_with(k);
    }
    quorum == knowledge[pid] && quorum.len() >= min_quorum
}

fn are_compatible(
    a: usize,
    knowledge_a: &[Knowledge],
    b: usize,
    knowledge_b: &[Knowledge],
    min_quorum: usize,
) -> bool {
    let nb_nodes = knowledge_a.len();
    let quorum_a = &knowledge_a[a];
    let quorum_b = &knowledge_b[b];
    debug_assert!(is_valid_knowledge(a, knowledge_a, min_quorum));
    debug_assert!(is_valid_knowledge(b, knowledge_b, min_quorum));

    if quorum_b.len() < min_quorum {
        return false;
    }

    let mut count_deducers: usize = 0;
    for p in 0..nb_nodes {
        if !quorum_a.is_disjoint(&knowledge_b[p]) || !quorum_b.is_disjoint(&knowledge_a[p]) {
            count_deducers += 1;
        }
    }
    count_deducers >= min_quorum
}

/// The latency tables every planner reads. Several are `O(n³)`, so they are built once and
/// shared.
pub struct LatencyTables {
    pub nb_processes: usize,
    pub max_quorum: usize,
    pub maj_quorum: usize,
    pub min_quorum: usize,
    pub path_latencies: Vec<Vec<Duration>>,
    pub prev_dest: Vec<Vec<usize>>,
    pub next_src: Vec<Vec<usize>>,
    pub link_rtts: Vec<Vec<Duration>>,
    pub path_rtts: Vec<Vec<Duration>>,
    pub quorum_3p_path_rtts: Vec<Vec<Vec<Duration>>>,
    pub quorum_3p_link_rtts: Vec<Vec<Vec<Duration>>>,
}

impl LatencyTables {
    pub fn new(topology: &Topology, shortest_paths: bool) -> Self {
        let nb_processes = topology.nb_processes;
        let max_faults = (topology.nb_replicas - 1) / 2;
        let max_quorum = topology.nb_replicas - max_faults;
        let maj_quorum = max_quorum; // For now, we don't play with quorum sizes
        // TODO: put it back to `max_faults + 1` and instead make sure at least max_quorum of the
        // frozen states always reaches the proposer
        let min_quorum = (max_faults + 1).max(1 + topology.nb_replicas / 2);
        let mut path_latencies = vec![vec![Duration::default(); nb_processes]; nb_processes];
        let mut prev_dest = vec![vec![0; nb_processes]; nb_processes];
        let mut next_src = vec![vec![0; nb_processes]; nb_processes];

        if shortest_paths {
            // Generate graph nodes
            let mut graph = NetworkGraph::with_capacity(nb_processes);
            for _ in 0..nb_processes {
                graph.add_node(());
            }
            for src in 0..nb_processes {
                for dest in 0..nb_processes {
                    if src != dest {
                        let nanos = topology.link_latency(src, dest).as_nanos();
                        graph.add_edge(src.into(), dest.into(), nanos as f64);
                    }
                }
            }

            // Compute the shortest paths
            for src in 0..nb_processes {
                let paths =
                    bellman_ford(&graph, src.into()).expect("Latencies can not be negative");
                for dest in 0..nb_processes {
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
            for src in 0..nb_processes {
                for dest in 0..nb_processes {
                    path_latencies[src][dest] = topology.link_latency(src, dest);
                    prev_dest[src][dest] = src;
                    next_src[src][dest] = dest;
                }
            }
        }
        let link_rtts: Vec<Vec<_>> = (0..nb_processes)
            .map(|src| {
                (0..nb_processes)
                    .map(|dest| topology.link_latency(src, dest) + topology.link_latency(dest, src))
                    .collect()
            })
            .collect();

        let path_rtts: Vec<Vec<_>> = (0..nb_processes)
            .map(|src| {
                (0..nb_processes)
                    .map(|dest| path_latencies[src][dest] + path_latencies[dest][src])
                    .collect()
            })
            .collect();

        let quorum_3p_path_rtts: Vec<Vec<_>> = (0..nb_processes)
            .map(|src| {
                (0..nb_processes)
                    .map(|dest| {
                        let mut replicas_3p_rtts: Vec<Duration> = topology
                            .alive_replicas
                            .iter()
                            .map(|replicas| {
                                path_latencies[src][replicas] + path_latencies[replicas][dest]
                            })
                            .collect();
                        replicas_3p_rtts.sort();
                        replicas_3p_rtts
                    })
                    .collect()
            })
            .collect();

        let quorum_3p_link_rtts: Vec<Vec<_>> = (0..nb_processes)
            .map(|src| {
                (0..nb_processes)
                    .map(|dest| {
                        let mut replicas_3p_rtts: Vec<Duration> = topology
                            .alive_replicas
                            .iter()
                            .map(|replicas| {
                                topology.link_latency(src, replicas)
                                    + topology.link_latency(replicas, dest)
                            })
                            .collect();
                        replicas_3p_rtts.sort();
                        replicas_3p_rtts
                    })
                    .collect()
            })
            .collect();
        Self {
            nb_processes,
            max_quorum,
            maj_quorum,
            min_quorum,
            path_latencies,
            prev_dest,
            next_src,
            link_rtts,
            path_rtts,
            quorum_3p_path_rtts,
            quorum_3p_link_rtts,
        }
    }

    /// `quorum_path_rtts[src]` in the original: the diagonal of the 3-phase table, i.e. the
    /// sorted quorum RTTs when the committer is the source itself. A view, never a copy.
    fn quorum_path(&self, src: usize) -> &Vec<Duration> {
        &self.quorum_3p_path_rtts[src][src]
    }

    fn quorum_link(&self, src: usize) -> &Vec<Duration> {
        &self.quorum_3p_link_rtts[src][src]
    }
}

pub struct MinEffortPlan {
    pub latencies: Vec<Duration>,
    /// Never read; kept because the KCensus assert is stated in terms of the pair.
    _committers: Vec<ProcId>,
}

pub struct PaxosPlan {
    pub latencies: Vec<Duration>,
    pub committers: Vec<ProcId>,
}

pub struct PandoPlan {
    pub latencies: Vec<Duration>,
    pub committers: Vec<ProcId>,
    pub delegates: Vec<ProcId>,
}

pub struct EPaxosPlan {
    pub latencies: Vec<Duration>,
    pub committers: Vec<ProcId>,
}

pub struct MultiPaxosPlan {
    pub latencies: Vec<Vec<Duration>>,
    pub leaders: Vec<usize>,
}

pub struct MultiPaxos3PPlan {
    pub latencies: Vec<Vec<Duration>>,
    pub committers: Vec<Vec<ProcId>>,
    pub leaders: Vec<usize>,
}

pub struct SwiftPaxosPlan {
    pub leader: usize,
    pub fixed_fast_quorum: Option<BitSet>,
    pub latencies: Vec<Duration>,
    /// The processes the leader route is expected to serve faster than the fast quorum, so
    /// they should commit on `SlowAck`s. A prediction, not an instruction: nothing reads it to
    /// decide a route -- both are open to everyone, and whichever completes first wins.
    pub expects_slow_acks: BitSet,
}

/// A process that casts no vote goes through a replica, and takes the best one. The
/// leader-based planners differ only in what they minimise.
fn best_committer(topology: &Topology, nb_processes: usize, mut better: impl FnMut(usize, usize)) {
    for proposer in 0..nb_processes {
        if topology.alive_replicas.contains(proposer) {
            continue;
        }
        for committer in topology.alive_replicas.iter() {
            better(proposer, committer);
        }
    }
}

pub fn min_effort_plan(topology: &Topology, t: &LatencyTables) -> MinEffortPlan {
    let mut latencies = vec![Duration::MAX; t.nb_processes];
    let mut committers = vec![0usize; t.nb_processes];
    for leader in topology.alive_replicas.iter() {
        latencies[leader] = t.quorum_path(leader)[t.min_quorum - 1];
        committers[leader] = leader;
    }
    best_committer(topology, t.nb_processes, |proposer, committer| {
        let latency = t.quorum_3p_path_rtts[proposer][committer][t.min_quorum - 1]
            + topology.link_latency(committer, proposer);
        if latency < latencies[proposer] {
            latencies[proposer] = latency;
            committers[proposer] = committer;
        }
    });
    MinEffortPlan {
        latencies,
        _committers: committers,
    }
}

pub fn paxos_plan(topology: &Topology, t: &LatencyTables) -> PaxosPlan {
    let mut latencies = vec![Duration::MAX; t.nb_processes];
    let mut committers = vec![0usize; t.nb_processes];
    for leader in topology.alive_replicas.iter() {
        latencies[leader] = t.quorum_link(leader)[t.maj_quorum - 1] * 2;
        committers[leader] = leader;
    }
    best_committer(topology, t.nb_processes, |proposer, committer| {
        let latency = t.link_rtts[proposer][committer] + latencies[committer];
        if latency < latencies[proposer] {
            latencies[proposer] = latency;
            committers[proposer] = committer;
        }
    });
    PaxosPlan {
        latencies,
        committers,
    }
}

pub fn pando_plan(topology: &Topology, t: &LatencyTables) -> PandoPlan {
    let mut latencies = vec![Duration::MAX; t.nb_processes];
    let mut committers = vec![0usize; t.nb_processes];
    let mut delegates = vec![0usize; t.nb_processes];
    for leader in topology.alive_replicas.iter() {
        committers[leader] = leader;
        for delegate in topology.alive_replicas.iter() {
            let latency = t.quorum_3p_link_rtts[leader][delegate][t.maj_quorum - 1]
                + t.quorum_3p_link_rtts[delegate][leader][t.maj_quorum - 1];
            if latency < latencies[leader] {
                latencies[leader] = latency;
                delegates[leader] = delegate;
            }
        }
    }
    best_committer(topology, t.nb_processes, |proposer, committer| {
        let latency = t.link_rtts[proposer][committer] + latencies[committer];
        if latency < latencies[proposer] {
            latencies[proposer] = latency;
            committers[proposer] = committer;
            delegates[proposer] = delegates[committer];
        }
    });
    PandoPlan {
        latencies,
        committers,
        delegates,
    }
}

/// Takes `paxos` because EPaxos falls back to the Paxos latency when there are too few live
/// replicas for its own quorum.
pub fn epaxos_plan(topology: &Topology, t: &LatencyTables, paxos: &PaxosPlan) -> EPaxosPlan {
    let mut latencies = vec![Duration::MAX; t.nb_processes];
    let mut committers = vec![0usize; t.nb_processes];
    let e_paxos_quorum = ((topology.nb_replicas * 3 - 1) / 4).max(t.maj_quorum);
    for leader in topology.alive_replicas.iter() {
        if e_paxos_quorum <= topology.alive_replicas.len() {
            latencies[leader] = t.quorum_link(leader)[e_paxos_quorum - 1];
        } else {
            latencies[leader] = paxos.latencies[leader];
        }
        committers[leader] = leader;
    }
    best_committer(topology, t.nb_processes, |proposer, committer| {
        let latency = t.link_rtts[proposer][committer] + latencies[committer];
        if latency < latencies[proposer] {
            latencies[proposer] = latency;
            committers[proposer] = committer;
        }
    });
    EPaxosPlan {
        latencies,
        committers,
    }
}

pub fn multi_paxos_plan(topology: &Topology, t: &LatencyTables) -> MultiPaxosPlan {
    let mut latencies = vec![vec![Duration::MAX; t.nb_processes]; t.nb_processes];
    for leader in topology.alive_replicas.iter() {
        latencies[leader] = (0..t.nb_processes)
            .map(|requester| {
                t.link_rtts[requester][leader] + t.quorum_link(leader)[t.maj_quorum - 1]
            })
            .collect();
    }
    let mut leaders: Vec<_> = topology.alive_replicas.iter().collect();
    leaders.sort_by_key(|leader| latencies[*leader].iter().sum::<Duration>());
    MultiPaxosPlan { latencies, leaders }
}

pub fn multi_paxos_3p_plan(topology: &Topology, t: &LatencyTables) -> MultiPaxos3PPlan {
    let mut latencies = vec![vec![Duration::MAX; t.nb_processes]; t.nb_processes];
    let mut committers = vec![vec![0; t.nb_processes]; t.nb_processes];
    for leader in topology.alive_replicas.iter() {
        for requester in 0..t.nb_processes {
            let best_latency = &mut latencies[leader][requester];
            let best_commiter = &mut committers[leader][requester];

            let is_replicas = topology.alive_replicas.contains(requester);
            for commiter in topology.alive_replicas.iter() {
                if is_replicas && commiter != requester {
                    continue;
                }
                let latency = topology.link_latency(requester, leader)
                    + t.quorum_3p_link_rtts[leader][commiter][t.maj_quorum - 1]
                    + topology.link_latency(commiter, requester);
                if latency < *best_latency {
                    *best_latency = latency;
                    *best_commiter = commiter;
                }
            }
        }
    }
    let mut leaders: Vec<_> = topology.alive_replicas.iter().collect();
    leaders.sort_by_key(|leader| latencies[*leader].iter().sum::<Duration>());
    MultiPaxos3PPlan {
        latencies,
        committers,
        leaders,
    }
}

pub fn swift_paxos_plan(topology: &Topology, t: &LatencyTables) -> SwiftPaxosPlan {
    let nb_processes = t.nb_processes;
    let maj_quorum = t.maj_quorum;
    let link_rtts = &t.link_rtts;
    let quorum_link_rtts: Vec<_> = (0..nb_processes).map(|src| t.quorum_link(src)).collect();

    let mut swift_paxos_leader = 0usize;
    let mut swift_paxos_fixed_fast_quorum: Option<BitSet> = None;
    let mut swift_paxos_latencies = vec![Duration::MAX; nb_processes];
    let mut swift_paxos_best_total = Duration::MAX;
    let mut swift_paxos_slow_acks = BitSet::with_capacity(nb_processes);

    let mut swift_leader_prio: Vec<_> = topology.alive_replicas.iter().collect();
    swift_leader_prio.sort_by_key(|leader| quorum_link_rtts[*leader][maj_quorum - 1]);

    // The route through the leader: a majority acknowledging its accept, three delays.
    // Those acknowledgements are broadcast, as in the reference implementation, so they
    // reach the proposer directly and there is no fourth-delay route relaying them back
    // through the leader — the protocol has no decision message to relay them with.
    //
    // A replica acts on the leader's proposal only once it also holds the command, and the
    // command travels straight from the proposer: the leader's accept carries no value, as
    // in the reference implementation (`afterPropagate`, `swift.go:418`). Adopting the
    // leader's value is therefore gated by the later of the two paths, which is the `max`.
    //
    // Tabulated: the search below wants one per (quorum, leader, requester), tens of millions
    // over `nb_processes²` distinct pairs, and each costs an allocation and a sort.
    let swift_mpaxos_latencies: Vec<Vec<Duration>> = (0..nb_processes)
        .map(|leader| {
            (0..nb_processes)
                .map(|requester| {
                    let mut to_requester: Vec<Duration> = topology
                        .alive_replicas
                        .iter()
                        .map(|replica| {
                            topology.link_latency(requester, replica).max(
                                topology.link_latency(requester, leader)
                                    + topology.link_latency(leader, replica),
                            ) + topology.link_latency(replica, requester)
                        })
                        .collect();
                    to_requester.sort();
                    to_requester[maj_quorum - 1]
                })
                .collect()
        })
        .collect();

    // Find best swift-paxos strategy, checking fast-paxos quorums first
    let fast_paxos_quorum = (((topology.nb_replicas * 3 - 1) / 4) + 1).max(maj_quorum);
    if topology.alive_replicas.len() > fast_paxos_quorum {
        for leader in swift_leader_prio.iter().copied() {
            // Try the full-size fast-paxos quorums approach
            // (Note: if alive.len == fast_paxos_quorum, there cannot be a benefit over a fixed quorum)
            let mut final_latencies = vec![Duration::MAX; nb_processes];
            let mut expects_slow_acks = BitSet::with_capacity(nb_processes);
            for requester in 0..nb_processes {
                let mut requester_quorum_rtts: Vec<_> = topology
                    .alive_replicas
                    .iter()
                    .map(|replica| link_rtts[requester][replica])
                    .collect();
                requester_quorum_rtts.sort();
                let fast_paxos_latency =
                    requester_quorum_rtts[fast_paxos_quorum - 1].max(link_rtts[requester][leader]);
                let mpaxos_latency = swift_mpaxos_latencies[leader][requester];

                final_latencies[requester] = if fast_paxos_latency < mpaxos_latency {
                    fast_paxos_latency
                } else {
                    expects_slow_acks.insert(requester);
                    mpaxos_latency
                }
            }
            let total = final_latencies.iter().sum();
            if total <= swift_paxos_best_total {
                swift_paxos_leader = leader;
                swift_paxos_fixed_fast_quorum = None;
                swift_paxos_latencies = final_latencies;
                swift_paxos_best_total = total;
                swift_paxos_slow_acks = expects_slow_acks;
            }
        }
    }

    // Exhaustive search over every (leader, fixed fast quorum), by branch and bound.
    //
    // `fast(r, Q) = max_{q in Q} link_rtts[r][q]` only grows as `Q` gains members, so a partial
    // quorum bounds the final one from below. That alone prunes almost nothing while the tree is
    // widest, so it is sharpened by what is still to come: the `k` remaining seats are filled
    // from the candidates left, and the cheapest choice for `r` is its `k` nearest, so the final
    // max is at least the `k`-th smallest of those.
    let alive: Vec<usize> = topology.alive_replicas.iter().collect();
    let mut best = SwiftBest {
        total: swift_paxos_best_total,
        leader: swift_paxos_leader,
        quorum: swift_paxos_fixed_fast_quorum,
        latencies: swift_paxos_latencies,
        expects_slow_acks: swift_paxos_slow_acks,
    };
    for leader in alive.iter().copied() {
        // Farthest-from-everyone first. Choosing a far candidate makes `fast` jump, so the
        // bound goes tight at once: trying them first rejects the branches containing one at
        // shallow depth instead of leaf by leaf. Measured at 33 replicas: 1988 nodes, against
        // 5750 unsorted and 67136 for the opposite order.
        //
        // Distance to the leader is a weaker key (10.7k nodes) -- `fast` is a max over every
        // requester, so what counts is distance from all of them. Weighting this against the
        // distance to the replica furthest from the leader does slightly better (1609 at
        // weight 4), but the best weight differs between topologies, and 400 nodes is under a
        // millisecond of a 35ms computation.
        let mut candidates: Vec<usize> = alive.iter().copied().filter(|c| *c != leader).collect();
        candidates.sort_by_key(|c| {
            std::cmp::Reverse(
                (0..nb_processes)
                    .map(|r| link_rtts[r][*c])
                    .sum::<Duration>(),
            )
        });
        let Some(slots) = maj_quorum.checked_sub(1).filter(|s| *s <= candidates.len()) else {
            continue;
        };
        let search = SwiftSearch {
            nb_processes,
            leader,
            // `suffix_sorted[start][r]`: the RTTs from `r` to every candidate from `start` on,
            // sorted, so the `k`-th smallest is a lookup rather than a selection per node.
            suffix_sorted: (0..=candidates.len())
                .map(|start| {
                    (0..nb_processes)
                        .map(|r| {
                            let mut rtts: Vec<Duration> = candidates[start..]
                                .iter()
                                .map(|c| link_rtts[r][*c])
                                .collect();
                            rtts.sort();
                            rtts
                        })
                        .collect()
                })
                .collect(),
            mpaxos: swift_mpaxos_latencies[leader].clone(),
            candidates,
            link_rtts,
        };
        let fast: Vec<Duration> = (0..nb_processes).map(|r| link_rtts[r][leader]).collect();
        search.explore(0, slots, fast, &mut vec![leader], &mut best);
    }

    SwiftPaxosPlan {
        leader: best.leader,
        fixed_fast_quorum: best.quorum,
        latencies: best.latencies,
        expects_slow_acks: best.expects_slow_acks,
    }
}

/// The best (leader, quorum) seen so far, and what it costs every process.
struct SwiftBest {
    total: Duration,
    leader: usize,
    quorum: Option<BitSet>,
    latencies: Vec<Duration>,
    expects_slow_acks: BitSet,
}

/// Everything that does not change while one leader's quorums are enumerated.
struct SwiftSearch<'a> {
    nb_processes: usize,
    leader: usize,
    candidates: Vec<usize>,
    suffix_sorted: Vec<Vec<Vec<Duration>>>,
    /// `mpaxos[r]`: what `r` pays to go through this leader instead of the fast route.
    mpaxos: Vec<Duration>,
    link_rtts: &'a [Vec<Duration>],
}

impl SwiftSearch<'_> {
    /// Chooses `slots` more members from `candidates[start..]`, keeping `best` up to date.
    /// `fast[r]` is `r`'s cost to reach everyone chosen so far.
    fn explore(
        &self,
        start: usize,
        slots: usize,
        fast: Vec<Duration>,
        chosen: &mut Vec<usize>,
        best: &mut SwiftBest,
    ) {
        if slots == 0 {
            let total = (0..self.nb_processes)
                .map(|r| fast[r].min(self.mpaxos[r]))
                .sum();
            if total < best.total {
                *best = SwiftBest {
                    total,
                    leader: self.leader,
                    quorum: Some(BitSet::from_iter(chosen.iter().copied())),
                    latencies: (0..self.nb_processes)
                        .map(|r| fast[r].min(self.mpaxos[r]))
                        .collect(),
                    // Ties go to the fast route.
                    expects_slow_acks: (0..self.nb_processes)
                        .filter(|&r| self.mpaxos[r] < fast[r])
                        .collect(),
                };
            }
            return;
        }

        // Every completion of this partial quorum costs at least this much.
        let bound: Duration = (0..self.nb_processes)
            .map(|r| {
                let still_to_come = self.suffix_sorted[start][r][slots - 1];
                fast[r].max(still_to_come).min(self.mpaxos[r])
            })
            .sum();
        if bound >= best.total {
            return;
        }

        // Leave enough candidates behind to fill the remaining seats.
        for next in start..=self.candidates.len() - slots {
            let member = self.candidates[next];
            let extended = (0..self.nb_processes)
                .map(|r| fast[r].max(self.link_rtts[r][member]))
                .collect();
            chosen.push(member);
            self.explore(next + 1, slots - 1, extended, chosen, best);
            chosen.pop();
        }
    }
}

pub fn kcensus_plan(
    topology: Topology,
    t: &LatencyTables,
    min_effort: &MinEffortPlan,
) -> PropagationGraphs {
    let nb_processes = t.nb_processes;
    let max_quorum = t.max_quorum;
    let min_quorum = t.min_quorum;
    let path_latencies = &t.path_latencies;
    let prev_dest = &t.prev_dest;
    let next_src = &t.next_src;
    let quorum_path_rtts: Vec<_> = (0..nb_processes).map(|src| t.quorum_path(src)).collect();
    let quorum_3p_path_rtts = &t.quorum_3p_path_rtts;
    let min_effort_latencies = &min_effort.latencies;

    #[derive(Debug, Clone)]
    struct KnowledgeLevel {
        time: Duration,
        k: Vec<Knowledge>,
        triangles_count: usize,
    }

    let mut propagation_graphs = Vec::with_capacity(nb_processes);
    let mut kcensus_latencies = Vec::with_capacity(nb_processes);
    let mut triangular_paths: Vec<Vec<Vec<TriangularPath>>> =
        vec![vec![Vec::new(); nb_processes]; nb_processes];
    let mut knowledge_levels: Vec<Vec<Vec<KnowledgeLevel>>> =
        vec![vec![Vec::new(); nb_processes]; nb_processes];
    let mut value_only_paths: Vec<Vec<TriangularPath>> =
        vec![Vec::with_capacity(nb_processes); nb_processes];

    for proposer in 0..nb_processes {
        // Compute shortest round-trip paths.
        // Used to ensure the value is sent to everyone (not for knowledge spreading).
        // TODO: don't actually send the values to non-replicas
        for pid in (0..nb_processes).rev() {
            let total_latency = path_latencies[proposer][pid];
            value_only_paths[proposer].push(TriangularPath {
                first: pid,
                second: pid,
                total_latency,
            })
        }
        value_only_paths[proposer].sort_by_key(triangle_latency);

        let is_replicas = topology.alive_replicas.contains(proposer);
        'leader_loop: for leader in topology.alive_replicas.iter() {
            if is_replicas && leader != proposer {
                continue 'leader_loop;
            }

            // Compute triangles
            triangular_paths[proposer][leader].reserve(topology.alive_replicas.len().pow(2));
            for first in topology.alive_replicas.iter() {
                let latency_to_first = path_latencies[proposer][first];
                for second in topology.alive_replicas.iter() {
                    let total_latency = latency_to_first
                        + path_latencies[first][second]
                        + path_latencies[second][leader];
                    triangular_paths[proposer][leader].push(TriangularPath {
                        first,
                        second,
                        total_latency,
                    });
                }
            }
            triangular_paths[proposer][leader].sort_by_key(triangle_latency);

            // TODO: maybe double-check if this max_lat is good if we start playing with quorums
            let min_lat = quorum_3p_path_rtts[proposer][leader][min_quorum - 1];
            let max_lat = min_lat + quorum_path_rtts[leader][max_quorum - 1];

            // Simulate propagation of knowledge (from best to worst possible strategy)
            let mut knowledges: Vec<Knowledge> =
                vec![BitSet::with_capacity(nb_processes); nb_processes];
            'triangle_loop: for ti in 0..triangular_paths[proposer][leader].len() {
                let t = &triangular_paths[proposer][leader][ti];
                if t.total_latency > max_lat {
                    break 'triangle_loop;
                }
                knowledges[t.second].insert(t.first);
                if t.total_latency < min_lat {
                    continue 'triangle_loop;
                }
                let next_t = triangular_paths[proposer][leader].get(ti + 1);
                if next_t.is_some_and(|nt| nt.total_latency == t.total_latency) {
                    continue 'triangle_loop;
                }
                assert!(knowledges[leader].len() >= min_quorum);
                assert!(is_valid_knowledge(leader, &knowledges, min_quorum));
                knowledge_levels[proposer][leader].push(KnowledgeLevel {
                    time: t.total_latency,
                    k: knowledges.clone(),
                    triangles_count: ti + 1,
                });
            }
        }
    }

    struct BestSol {
        leaders: Vec<usize>,
        levels: Vec<usize>,
        sum_of_latencies: Duration,
    }

    struct LevelSearchData<'a> {
        nb_processes: usize,
        leaders: &'a Vec<usize>,
        knowledge_levels: &'a Vec<Vec<Vec<KnowledgeLevel>>>,
        compatible_levels: Vec<Vec<Vec<usize>>>,
        useful_levels: Vec<Vec<bool>>,
        max_levels: Vec<usize>,
        min_quorum: usize,
    }

    impl LevelSearchData<'_> {
        #[inline]
        fn knowledge_level(&self, pid: usize, level: usize) -> &KnowledgeLevel {
            &self.knowledge_levels[pid][self.leaders[pid]][level]
        }

        #[inline]
        fn are_compatible(&self, a: usize, level_a: usize, b: usize, level_b: usize) -> bool {
            are_compatible(
                self.leaders[a],
                &self.knowledge_level(a, level_a).k,
                self.leaders[b],
                &self.knowledge_level(b, level_b).k,
                self.min_quorum,
            )
        }
    }

    // Implements recursive search of the optimal solution
    fn best_avg_search_inner(
        best: &mut BestSol,
        pids_done: usize,
        min_levels: &[usize],
        partial_total_time: Duration,
        data: &LevelSearchData,
    ) {
        let nb_processes = data.nb_processes;
        let pid_a = pids_done;
        let pids_done = pids_done + 1;
        'level_loop: for level_a in min_levels[pid_a]..=data.max_levels[pid_a] {
            if !data.useful_levels[pid_a][level_a] {
                continue 'level_loop;
            }

            let mut levels = min_levels.to_owned();
            levels[pid_a] = level_a;
            let partial_total_time = partial_total_time + data.knowledge_level(pid_a, level_a).time;
            let mut new_curr_total_time = partial_total_time;
            let mut new_min_total_time = partial_total_time;
            for pid_b in pids_done..nb_processes {
                new_min_total_time += data.knowledge_level(pid_b, min_levels[pid_b]).time;
                let req_level_b = data.compatible_levels[pid_a][level_a][pid_b];
                if req_level_b > levels[pid_b] {
                    levels[pid_b] = req_level_b;
                }
                new_curr_total_time += data.knowledge_level(pid_b, levels[pid_b]).time;
            }

            if best.sum_of_latencies <= new_min_total_time {
                // We cannot find a better solution with higher level_a
                break 'level_loop;
            }
            if best.sum_of_latencies <= new_curr_total_time {
                // We cannot find a better solution with current level_a
                continue 'level_loop;
            }

            assert!(partial_total_time < best.sum_of_latencies);
            if pids_done == nb_processes {
                // Found a complete solution that is better!
                best.levels = levels;
                best.sum_of_latencies = partial_total_time;
                best.leaders = data.leaders.clone();
            } else {
                // Promising but incomplete solution. Search this branch:
                best_avg_search_inner(best, pids_done, &levels, partial_total_time, data);
            }
        }
    }

    fn best_avg_search(
        best: &mut BestSol,
        leaders: &Vec<usize>,
        knowledge_levels: &Vec<Vec<Vec<KnowledgeLevel>>>,
        topology: &Topology,
        min_quorum: usize,
    ) {
        let nb_processes = topology.nb_processes;
        let partial_latency = leaders
            .iter()
            .enumerate()
            .map(|(proposer, leader)| topology.link_latency(*leader, proposer))
            .sum();

        // Early exit if it cannot be better than previous solutions
        let mut min_latency = partial_latency;
        for pid in 0..nb_processes {
            min_latency += knowledge_levels[pid][leaders[pid]][0].time;
        }
        if min_latency > best.sum_of_latencies {
            return;
        }

        // Prepare search data
        let mut data = LevelSearchData {
            nb_processes,
            leaders,
            knowledge_levels,
            compatible_levels: vec![Vec::new(); nb_processes],
            useful_levels: vec![Vec::new(); nb_processes],
            max_levels: vec![0usize; nb_processes],
            min_quorum,
        };

        // Precompute max knowledge levels (Note: could be merged with precomputation of compatibilities)
        for a in 0..nb_processes {
            for b in 0..nb_processes {
                while !data.are_compatible(a, data.max_levels[a], b, 0) {
                    data.max_levels[a] += 1;
                }
            }
        }

        // Precompute compatible levels
        for a in 0..nb_processes {
            let mut current_levels: Vec<usize> = data.max_levels.clone();
            for a_level in 0..knowledge_levels[a][leaders[a]].len() {
                let mut compatibility_changed = false;
                for (b, b_level) in current_levels.iter_mut().enumerate() {
                    while *b_level > 0 && data.are_compatible(a, a_level, b, *b_level - 1) {
                        *b_level -= 1;
                        compatibility_changed = true;
                    }
                }
                data.useful_levels[a]
                    .push(data.compatible_levels[a].is_empty() || compatibility_changed);
                data.compatible_levels[a].push(current_levels.clone());
            }
        }

        trace!("searching with leaders: {:?}", data.leaders);
        trace!(
            "useful levels: {:?}",
            data.useful_levels
                .iter()
                .map(|list| list.iter().filter(|x| **x).count())
                .collect::<Vec<usize>>()
        );

        // Use recursive search to compute the best solution for the given leaders
        best_avg_search_inner(best, 0, &vec![0usize; nb_processes], partial_latency, &data);
    }

    // Implement recursive search of the optimal leaders
    fn best_leaders_search(
        best: &mut BestSol,
        prev_leaders: &Vec<usize>,
        proposer: usize,
        knowledge_levels: &Vec<Vec<Vec<KnowledgeLevel>>>,
        topology: &Topology,
        min_quorum: usize,
    ) {
        if proposer >= topology.nb_processes {
            return best_avg_search(best, prev_leaders, knowledge_levels, topology, min_quorum);
        }

        let mut leaders = prev_leaders.clone();
        let is_replicas = topology.alive_replicas.contains(proposer);
        let mut sorted_leaders: Vec<_> = topology.alive_replicas.iter().collect();
        sorted_leaders.sort_by_key(|leader| topology.link_latency(*leader, proposer));
        for leader in sorted_leaders {
            if is_replicas && leader != proposer {
                continue;
            }

            leaders[proposer] = leader;
            best_leaders_search(
                best,
                &leaders,
                proposer + 1,
                knowledge_levels,
                topology,
                min_quorum,
            );
        }
    }

    let mut best = BestSol {
        leaders: vec![0; nb_processes],
        levels: vec![0; nb_processes],
        sum_of_latencies: Duration::MAX,
    };

    best_leaders_search(
        &mut best,
        &vec![0usize; nb_processes],
        0,
        &knowledge_levels,
        &topology,
        min_quorum,
    );

    assert_ne!(best.sum_of_latencies, Duration::MAX);
    let best = best;

    // Build the graph from the solutions for each proposer
    let mut sum_of_latencies = Duration::ZERO;
    for proposer in 0..nb_processes {
        let leader = best.leaders[proposer];
        let triangular_paths = &mut triangular_paths[proposer][leader];
        let value_only_paths = &value_only_paths[proposer];
        let knowledge_levels = &knowledge_levels[proposer][leader];
        let best_level = &knowledge_levels[best.levels[proposer]];

        // Truncate triangles at commit time
        let leader_latency = best_level.time;
        let triangle_count = best_level.triangles_count;
        let proposer_latency = leader_latency + topology.link_latency(leader, proposer);
        sum_of_latencies += proposer_latency;
        triangular_paths.truncate(triangle_count);
        assert_eq!(
            triangular_paths[triangle_count - 1].total_latency,
            leader_latency
        );

        // Trace for debugging
        if proposer == leader {
            info!("proposer {proposer} ({}):", topology.regions[proposer]);
        } else {
            info!(
                "proposer {proposer} ({}) with leader {leader}:",
                topology.regions[proposer]
            );
        }
        let mut min_proposer_latency = Duration::MAX;
        let mut max_proposer_latency = Duration::MAX;
        for leader in topology.alive_replicas.iter() {
            let lat = quorum_3p_path_rtts[proposer][leader][min_quorum - 1]
                + topology.link_latency(leader, proposer);
            if lat < min_proposer_latency {
                min_proposer_latency = lat;
            }
            let lat = lat + quorum_path_rtts[leader][max_quorum - 1];
            if lat < max_proposer_latency {
                max_proposer_latency = lat;
            }
        }
        assert_eq!(min_effort_latencies[proposer], min_proposer_latency);
        info!(
            "  levels best ({} / {}): {proposer_latency:?} ({:.4}x min, {:.1}% min-max) min: {min_proposer_latency:?}, max: {max_proposer_latency:?}",
            best.levels[proposer],
            knowledge_levels.len(),
            proposer_latency.as_secs_f64() / min_proposer_latency.as_secs_f64(),
            100.0 * (proposer_latency - min_proposer_latency).as_secs_f64()
                / (max_proposer_latency - min_proposer_latency).as_secs_f64()
        );
        debug!(
            "  quorum size: {}, required knowledge: {:?}",
            best_level.k[leader].len(),
            best_level.k
        );
        trace!(
            "  Left after truncate: {} real triangles, {} total, longest path: {}",
            triangular_paths
                .iter()
                .filter(|x| proposer != x.first && x.first != x.second && x.second != leader)
                .count(),
            triangular_paths.len(),
            triangular_paths[triangle_count - 1]
        );

        // TODO: Some knowledge might still not be needed to commit. (but the cost is probably negligible)
        //   Try to check if they are needed for are_compatible?

        // Initialize graph
        let mut message_times: Vec<Vec<BTreeSet<Duration>>> =
            vec![vec![BTreeSet::new(); nb_processes]; nb_processes];
        let mut should_include_value: HashSet<MessageId> = HashSet::new();
        let mut states: Vec<BTreeMap<Duration, KnowledgeState>> =
            vec![BTreeMap::new(); nb_processes];

        // Prepare initial states
        for (i, state) in states.iter_mut().enumerate() {
            let mut knowledge = vec![BitSet::new(); nb_processes];
            if i == proposer {
                knowledge[proposer].insert(proposer);
            }
            state.insert(
                Duration::ZERO,
                KnowledgeState {
                    knowledge,
                    remote_states: vec![Duration::ZERO; nb_processes],
                    dependencies: HashSet::new(),
                    needed_by: vec![],
                },
            );
        }

        // Loop over triangles
        // TODO: Actually, process value_only_paths from quorum first (in desc order)
        let mut i = value_only_paths.len(); // first: send values (desc order, but does not matter)
        let mut j = triangle_count; // second: triangles to commit, from longest to shortest (desc)
        'triangle_loop: while 0 < j {
            // Pick the next triangle
            let value_only_path = 0 < i;
            let t = if value_only_path {
                i -= 1;
                &value_only_paths[i]
            } else {
                j -= 1;
                &triangular_paths[j]
            };

            // Skip triangles that would not bring new knowledge
            if !value_only_path {
                let leaders_final_knowledge = &states[leader]
                    .range(..=leader_latency)
                    .last()
                    .expect("leader should have a last state")
                    .1
                    .knowledge;
                if leaders_final_knowledge[t.second].contains(t.first) {
                    continue 'triangle_loop;
                }
            }

            // Compute slack (how much delay can add when we reuse messages)
            let mut max_slack = if t.total_latency <= leader_latency {
                leader_latency - t.total_latency
            } else {
                assert!(value_only_path);
                Duration::ZERO
            };

            // Initialize variables to track time/position/progress along the path
            let mut current_time = Duration::ZERO;
            let mut current = proposer;
            let mut shortest_path_from_proposer = true;
            let mut _shortest_path_to_leader = false;

            // Loop over the three (/one) checkpoints of the triangle (/value_only_patH)
            let checkpoints = if value_only_path {
                [t.first, t.first, t.first]
            } else {
                [t.first, t.second, leader]
            };
            for (step, target) in checkpoints.into_iter().enumerate() {
                if step > 0 && target == leader {
                    shortest_path_from_proposer = false;
                    _shortest_path_to_leader = true;
                }

                // For every step towards the checkpoints
                while current != target {
                    // TODO: if step == 2 (return path) and there's already messages
                    //   going back to the leader, then avoid creating new ones.
                    //   (Chose one of the existing paths or forward knowledge recursively)

                    // Move towards checkpoint
                    let src = current;
                    current = next_src[current][target];
                    assert_ne!(src, current);

                    // Check if shortest path from/to leader
                    if step > 0 {
                        shortest_path_from_proposer &= prev_dest[proposer][current] == src;
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
                            proposer,
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
                            proposer,
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
                            value_only_path, shortest_path_from_proposer,
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
                    for i in 0..nb_processes {
                        // Check source knowledge
                        debug_assert!(src_state.knowledge[i].is_subset(&src_state.knowledge[src]));
                        // TODO: the following would be true if we always re-propagated knowledge fully
                        // debug_assert!(
                        //     state.knowledge[i].is_superset(&src_state.knowledge[i])
                        //         || state.knowledge[i].is_subset(&src_state.knowledge[i])
                        // );

                        // Merge knowledge
                        state.knowledge[i].union_with(&src_state.knowledge[i]);
                        state.remote_states[i] =
                            max(state.remote_states[i], src_state.remote_states[i]);

                        // Check obtained knowledge
                        // TODO: the following would be true if we always re-propagated knowledge fully
                        // debug_assert_eq!(
                        //     state.knowledge[i] == src_state.knowledge[i],
                        //     state.remote_states[i] == src_state.remote_states[i]
                        // );
                        debug_assert!(state.knowledge[i].is_subset(&state.knowledge[current]));
                    }

                    let ks = state.clone();

                    // Update knowledge of future states at current location.
                    for (time, state) in states[current].range_mut(current_time..).skip(1) {
                        assert!(*time > current_time);
                        for i in 0..nb_processes {
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

        // Sanity checks:
        assert_eq!(should_include_value.len(), nb_processes - 1);
        let final_leader_state = states[leader].last_key_value().expect("should have state");
        debug_assert!(final_leader_state.0 == &leader_latency);
        for pid in 0..nb_processes {
            debug_assert!(final_leader_state.1.knowledge[pid].is_superset(&best_level.k[pid]));
        }

        // Add to list of graphs/latencies
        assert_eq!(kcensus_latencies.len(), proposer);
        assert_eq!(propagation_graphs.len(), proposer);
        kcensus_latencies.push(proposer_latency);
        propagation_graphs.push(PropagationGraph {
            should_include_value,
            states,
            leader,
        });
    }
    assert_eq!(sum_of_latencies, best.sum_of_latencies);

    PropagationGraphs {
        graphs: propagation_graphs,
        topology,
        latencies: kcensus_latencies,
    }
}

#[cfg(test)]
mod swift_search_tests {
    use super::*;
    use itertools::Itertools;

    /// What a (leader, quorum) pair costs everyone, recomputed from the topology rather than
    /// from anything the search builds, so the two are independent.
    fn cost(topology: &Topology, quorum: &[usize], leader: usize) -> Duration {
        let n = topology.nb_processes;
        let maj = topology.nb_replicas - (topology.nb_replicas - 1) / 2;
        (0..n)
            .map(|r| {
                let fast = quorum
                    .iter()
                    .map(|&q| topology.link_latency(r, q) + topology.link_latency(q, r))
                    .max()
                    .unwrap();
                let mut through_leader: Vec<Duration> = topology
                    .alive_replicas
                    .iter()
                    .map(|replica| {
                        topology.link_latency(r, replica).max(
                            topology.link_latency(r, leader)
                                + topology.link_latency(leader, replica),
                        ) + topology.link_latency(replica, r)
                    })
                    .collect();
                through_leader.sort();
                fast.min(through_leader[maj - 1])
            })
            .sum()
    }

    /// The plan must be at least as good as every (leader, quorum) pair there is -- which is
    /// what "exhaustive" means, and what the pruning has to preserve.
    fn assert_optimal(config: &str) {
        let topology = Topology::from_path(&config.to_string(), Vec::new(), None);
        let tables = LatencyTables::new(&topology, false);
        let plan = swift_paxos_plan(&topology, &tables);
        let found: Duration = plan.latencies.iter().sum();

        let maj = topology.nb_replicas - (topology.nb_replicas - 1) / 2;
        let alive: Vec<usize> = topology.alive_replicas.iter().collect();
        let mut brute = Duration::MAX;
        for quorum in alive.iter().copied().combinations(maj) {
            for leader in quorum.iter().copied() {
                brute = brute.min(cost(&topology, &quorum, leader));
            }
        }
        assert!(
            found <= brute,
            "{config}: search found {found:?}, brute force found {brute:?}"
        );
    }

    #[test]
    fn search_matches_brute_force() {
        for config in [
            "configs/aws-ring-7.toml",
            "configs/aws-east-asia-7.toml",
            "configs/aws-europe-7.toml",
            "configs/aws-north-america-7.toml",
        ] {
            assert_optimal(config);
        }
    }
}
