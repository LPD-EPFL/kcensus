use crate::consensus::deps::dep_set::{DepSet, uid_step};
use crate::consensus::deps::instance::Instance;
use petgraph::algo::tarjan_scc;
use petgraph::graph::{DiGraph, NodeIndex};
use std::collections::{HashMap, HashSet};

/// The lowest un-executed uid of every replica, the only commands that can be next.
///
/// Within a shard a command always depends on its own replica's previous command there —
/// a coordinator's `seen` covers everything it proposed before, and FIFO links carry that
/// to the leader in the same order — so execution follows each replica's uid order, and
/// anything ready is one of these `n` uids.
fn heads(executed: &DepSet) -> impl Iterator<Item = usize> + '_ {
    let step = uid_step(executed.process_count());
    (0..executed.process_count()).map(move |replica| {
        executed
            .get(replica)
            .map_or(2 * replica, |done| done + step)
    })
}

/// The next command to execute while the dependency graph stays acyclic, which is the
/// common case and the only case in SwiftPaxos, where every committed dependency set is a
/// prefix of the leader's arrival order.
///
/// This is what [`executable_order`] would return next, found in `O(n)` integer work with
/// no allocation and no graph: a command is ready exactly when it is committed and every
/// dependency it names has already been executed — which is what makes an out-of-order
/// commit wait for its dependencies rather than overtake them. `None` means we are
/// waiting on a commit, or that what is left forms a cycle; see [`cycle_possible`].
pub fn next_executable(instances: &HashMap<usize, Instance>, executed: &DepSet) -> Option<usize> {
    heads(executed)
        .filter(|uid| {
            instances.get(uid).is_some_and(|instance| {
                instance.is_committed()
                    && instance.deps.pending_over(executed).all(|dep| dep == *uid)
            })
        })
        .min()
}

/// Whether a stalled shard might be stalled on a cycle rather than on a missing commit,
/// i.e. whether [`executable_order`] is worth running at all.
///
/// A cycle only blocks execution once everything before it is done, and at that point its
/// members are heads — if a member's replica-predecessor were un-executed it would be a
/// dependency, hence in the cycle's own closure, hence the head instead. So it is enough
/// to ask whether some committed head is blocked purely by *committed* instances. When it
/// is not, which is the usual reason to be waiting, the answer comes back after a lookup
/// or two and the graph is never built.
///
/// It has to be asked on every stall, not only when a commit arrived: the fast path
/// executes as it goes, and an execution can close a set just as a commit can. A gate that
/// only re-examined the command just committed missed exactly that — a cycle whose last
/// outside blocker was executed by the fast path in the same call — and deadlocked.
pub fn cycle_possible(instances: &HashMap<usize, Instance>, executed: &DepSet) -> bool {
    heads(executed).any(|uid| {
        instances.get(&uid).is_some_and(|instance| {
            instance.is_committed()
                && instance.deps.pending_over(executed).all(|dep| {
                    dep == uid || instances.get(&dep).is_some_and(Instance::is_committed)
                })
        })
    })
}

/// The order in which committed commands become executable, following EPaxos\* Figure 1.
///
/// Returns the uids to execute, in order. The caller applies them and advances its
/// `executed` watermark.
///
/// 1. take the largest set `g` of committed instances that transitively depend only on
///    committed (or already executed) instances;
/// 2. split `g` into strongly connected components in topological order;
/// 3. execute each component in uid order.
///
/// Everything here is per shard, because a dependency never leaves its shard, so these
/// graphs stay small — bounded by the commands outstanding on a single key.
pub fn executable_order(instances: &HashMap<usize, Instance>, executed: &DepSet) -> Vec<usize> {
    // Step 1: start from every committed instance and drop the ones that (transitively)
    // wait on something not committed yet. A dependency that is missing entirely — known
    // only as a watermark, its `PreAccept` not yet delivered — blocks in exactly the same
    // way, which is how we wait out a reordering rather than needing a Nop.
    let mut g: HashSet<usize> = instances
        .iter()
        .filter(|(_, instance)| instance.is_committed())
        .map(|(uid, _)| *uid)
        .collect();

    loop {
        let mut removed = false;
        for uid in g.clone() {
            let deps = &instances[&uid].deps;
            if deps
                .pending_over(executed)
                .any(|dep| dep != uid && !g.contains(&dep))
            {
                g.remove(&uid);
                removed = true;
            }
        }
        if !removed {
            break;
        }
    }

    if g.is_empty() {
        return Vec::new();
    }

    // Step 2: build the graph over `g`, with an edge from a command to each dependency it
    // still has to wait for, and take the strongly connected components. `tarjan_scc`
    // returns them in reverse topological order, i.e. dependencies first, which is the
    // order we want to execute in.
    // Nodes are added in uid order so that the result depends only on the graph and not
    // on hash iteration order: every process must produce the same sequence.
    let mut ordered: Vec<usize> = g.iter().copied().collect();
    ordered.sort_unstable();
    let mut graph = DiGraph::<usize, ()>::new();
    let mut nodes: HashMap<usize, NodeIndex> = HashMap::with_capacity(g.len());
    for uid in &ordered {
        nodes.insert(*uid, graph.add_node(*uid));
    }
    for uid in &ordered {
        for dep in instances[uid].deps.pending_over(executed) {
            if dep != *uid && g.contains(&dep) {
                graph.add_edge(nodes[uid], nodes[&dep], ());
            }
        }
    }

    // Step 3: uid order inside a component, which every process computes identically.
    let mut order = Vec::with_capacity(g.len());
    for component in tarjan_scc(&graph) {
        let mut component: Vec<usize> = component.into_iter().map(|node| graph[node]).collect();
        component.sort_unstable();
        order.extend(component);
    }
    order
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::command::Command;
    use crate::consensus::deps::instance::Instance;

    const N: usize = 3;

    fn uid(replica: usize, k: usize) -> usize {
        2 * (k * N + replica)
    }

    fn command() -> Command {
        Command {
            requester: 0,
            shard: 0,
            command: vec![],
            read_only: false,
            arrival_slot: 0,
            report: None,
        }
    }

    /// A committed instance depending on `deps`.
    fn committed(deps: &[usize]) -> Instance {
        let mut dep_set = DepSet::new(N);
        for dep in deps {
            dep_set.insert(*dep);
        }
        let mut instance = Instance::new(0, 0, command(), dep_set.clone(), N);
        instance.commit(dep_set);
        instance
    }

    fn uncommitted() -> Instance {
        Instance::new(0, 0, command(), DepSet::new(N), N)
    }

    /// The shard's drain, with the graph forced rather than gated on [`cycle_possible`], so
    /// that the graph-free path can be compared against the graph command for command.
    fn drain(instances: &mut HashMap<usize, Instance>, executed: &mut DepSet) -> Vec<usize> {
        let mut order = Vec::new();
        let execute = |instances: &mut HashMap<usize, Instance>,
                       executed: &mut DepSet,
                       order: &mut Vec<usize>,
                       uid: usize| {
            instances.remove(&uid);
            executed.insert(uid);
            order.push(uid);
        };
        while let Some(uid) = next_executable(instances, executed) {
            execute(instances, executed, &mut order, uid);
        }
        for uid in executable_order(instances, executed) {
            execute(instances, executed, &mut order, uid);
        }
        assert!(next_executable(instances, executed).is_none());
        order
    }

    #[test]
    fn independent_commands_execute_in_uid_order() {
        let mut instances = HashMap::new();
        instances.insert(uid(1, 0), committed(&[]));
        instances.insert(uid(0, 0), committed(&[]));
        let order = executable_order(&instances, &DepSet::new(N));
        assert_eq!(order, vec![uid(0, 0), uid(1, 0)]);
    }

    #[test]
    fn a_dependency_executes_before_its_dependent() {
        let mut instances = HashMap::new();
        instances.insert(uid(0, 0), committed(&[]));
        instances.insert(uid(1, 0), committed(&[uid(0, 0)]));
        let order = executable_order(&instances, &DepSet::new(N));
        assert_eq!(order, vec![uid(0, 0), uid(1, 0)]);
    }

    #[test]
    fn an_uncommitted_dependency_blocks_the_whole_chain() {
        let mut instances = HashMap::new();
        instances.insert(uid(0, 0), uncommitted());
        instances.insert(uid(1, 0), committed(&[uid(0, 0)]));
        instances.insert(uid(2, 0), committed(&[uid(1, 0)]));
        assert!(executable_order(&instances, &DepSet::new(N)).is_empty());
    }

    #[test]
    fn a_missing_dependency_blocks_too() {
        // uid(0, 0) is named by a watermark but no instance exists for it yet.
        let mut instances = HashMap::new();
        instances.insert(uid(1, 0), committed(&[uid(0, 0)]));
        assert!(executable_order(&instances, &DepSet::new(N)).is_empty());
    }

    #[test]
    fn an_already_executed_dependency_does_not_block() {
        let mut executed = DepSet::new(N);
        executed.insert(uid(0, 0));
        let mut instances = HashMap::new();
        instances.insert(uid(1, 0), committed(&[uid(0, 0)]));
        assert_eq!(executable_order(&instances, &executed), vec![uid(1, 0)]);
    }

    #[test]
    fn a_cycle_becomes_one_component_ordered_by_uid() {
        // Visibility only requires one direction; when both hold, the two commands form a
        // component and every process breaks the tie the same way.
        let mut instances = HashMap::new();
        instances.insert(uid(1, 0), committed(&[uid(0, 0)]));
        instances.insert(uid(0, 0), committed(&[uid(1, 0)]));
        let order = executable_order(&instances, &DepSet::new(N));
        assert_eq!(order, vec![uid(0, 0), uid(1, 0)]);
    }

    #[test]
    fn a_component_still_waits_for_what_precedes_it() {
        let mut instances = HashMap::new();
        instances.insert(uid(2, 0), committed(&[]));
        instances.insert(uid(1, 0), committed(&[uid(0, 0), uid(2, 0)]));
        instances.insert(uid(0, 0), committed(&[uid(1, 0)]));
        let order = executable_order(&instances, &DepSet::new(N));
        assert_eq!(order, vec![uid(2, 0), uid(0, 0), uid(1, 0)]);
    }
    #[test]
    fn the_graph_free_path_produces_the_order_the_graph_would() {
        // Every scenario above, driven the way the shard drives it. The graph-free path
        // has to agree with `executable_order` command for command, or two processes
        // could execute the same conflicting commands in different orders.
        let scenarios: Vec<Vec<(usize, Instance)>> = vec![
            vec![(uid(1, 0), committed(&[])), (uid(0, 0), committed(&[]))],
            vec![
                (uid(0, 0), committed(&[])),
                (uid(1, 0), committed(&[uid(0, 0)])),
            ],
            vec![
                (uid(0, 0), uncommitted()),
                (uid(1, 0), committed(&[uid(0, 0)])),
                (uid(2, 0), committed(&[uid(1, 0)])),
            ],
            vec![
                (uid(2, 0), committed(&[])),
                (uid(1, 0), committed(&[uid(0, 0), uid(2, 0)])),
                (uid(0, 0), committed(&[uid(1, 0)])),
            ],
        ];
        for scenario in scenarios {
            let instances: HashMap<usize, Instance> = scenario.into_iter().collect();
            let expected = executable_order(&instances, &DepSet::new(N));
            let mut instances = instances;
            let mut executed = DepSet::new(N);
            assert_eq!(drain(&mut instances, &mut executed), expected);
        }
    }

    #[test]
    fn a_cycle_is_broken_by_the_graph() {
        let mut instances = HashMap::new();
        instances.insert(uid(1, 0), committed(&[uid(0, 0)]));
        instances.insert(uid(0, 0), committed(&[uid(1, 0)]));
        let mut executed = DepSet::new(N);
        assert_eq!(next_executable(&instances, &executed), None);
        assert!(cycle_possible(&instances, &executed));
        assert_eq!(
            drain(&mut instances, &mut executed),
            vec![uid(0, 0), uid(1, 0)]
        );
    }

    #[test]
    fn a_cycle_freed_by_an_execution_is_still_broken() {
        // The cycle {1, 2} also waits on 0, which commits last. The fast path executes 0
        // in the same call, and only then is the cycle blocked by nothing but itself — so
        // the trigger has to look at the shard's state after that execution, not at the
        // command that was just committed, which by then is gone.
        let mut instances = HashMap::new();
        instances.insert(uid(0, 0), committed(&[]));
        instances.insert(uid(1, 0), committed(&[uid(0, 0), uid(2, 0)]));
        instances.insert(uid(2, 0), committed(&[uid(0, 0), uid(1, 0)]));
        let mut executed = DepSet::new(N);

        assert_eq!(next_executable(&instances, &executed), Some(uid(0, 0)));
        instances.remove(&uid(0, 0));
        executed.insert(uid(0, 0));

        assert_eq!(next_executable(&instances, &executed), None);
        assert!(cycle_possible(&instances, &executed));
        assert_eq!(
            drain(&mut instances, &mut executed),
            vec![uid(1, 0), uid(2, 0)]
        );
    }

    #[test]
    fn waiting_for_a_commit_never_builds_the_graph() {
        // The common stall: a dependency we have not committed yet. Nothing to break, so
        // the expensive path must stay untouched.
        let mut instances = HashMap::new();
        instances.insert(uid(0, 0), uncommitted());
        instances.insert(uid(1, 0), committed(&[uid(0, 0)]));
        assert!(!cycle_possible(&instances, &DepSet::new(N)));
        // Nor when the dependency is not even known here.
        let mut instances = HashMap::new();
        instances.insert(uid(1, 0), committed(&[uid(0, 0)]));
        assert!(!cycle_possible(&instances, &DepSet::new(N)));
    }

    #[test]
    fn a_self_dependency_does_not_block() {
        // A watermark cannot say "all of this replica but me", so a set can name the
        // command it belongs to. Execution ignores that, on both paths.
        let mut instances = HashMap::new();
        instances.insert(uid(0, 0), committed(&[uid(0, 0)]));
        let mut executed = DepSet::new(N);
        assert_eq!(next_executable(&instances, &executed), Some(uid(0, 0)));
        assert_eq!(drain(&mut instances, &mut executed), vec![uid(0, 0)]);
    }

    #[test]
    fn only_the_lowest_uid_of_a_replica_is_a_candidate() {
        // Later commands of a replica wait for its earlier ones, so they are not heads.
        let mut instances = HashMap::new();
        instances.insert(uid(0, 1), committed(&[]));
        assert_eq!(next_executable(&instances, &DepSet::new(N)), None);
        let mut executed = DepSet::new(N);
        executed.insert(uid(0, 0));
        assert_eq!(next_executable(&instances, &executed), Some(uid(0, 1)));
    }
}
