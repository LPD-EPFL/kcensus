use crate::consensus::deps::dep_set::{Executed, uid_step};
use crate::consensus::deps::instance::Instance;
use crate::consensus::deps::instance_table::InstanceTable;

/// The committed commands that could run next: each replica's log from its watermark up to
/// the first uid that has not committed, with the ones already executed skipped.
///
/// A per-key dependency set does not order two commands of one replica on different keys,
/// so the next to run need not be the lowest — the reference scan stops only at an
/// uncommitted instance and steps over the executed ones (`epaxos.go:397`).
fn candidates<'a>(
    instances: &'a InstanceTable,
    executed: &'a Executed,
) -> impl Iterator<Item = usize> + 'a {
    let process_count = executed.process_count();
    let step = uid_step(process_count);
    (0..process_count).flat_map(move |replica| {
        let mut uid = executed.next_unexecuted(replica);
        std::iter::from_fn(move || {
            while executed.contains(uid) {
                uid += step;
            }
            let instance = instances.get(uid)?;
            if !instance.is_committed() {
                return None;
            }
            let ready = uid;
            uid += step;
            Some(ready)
        })
    })
}

/// The next command to execute while the dependency graph stays acyclic, which is the
/// common case and the only case in SwiftPaxos, where every committed dependency set is a
/// prefix of the leader's arrival order.
///
/// This is what [`ExecutionScratch::executable_order`] would return next, found in `O(n)`
/// integer work with
/// no allocation and no graph: a command is ready exactly when it is committed and every
/// dependency it names has already been executed — which is what makes an out-of-order
/// commit wait for its dependencies rather than overtake them. `None` means we are
/// waiting on a commit, or that what is left forms a cycle; see [`cycle_possible`].
pub fn next_executable(instances: &InstanceTable, executed: &Executed) -> Option<usize> {
    candidates(instances, executed)
        .filter(|uid| {
            instances[*uid]
                .deps
                .pending_over(executed)
                .all(|dep| dep == *uid)
        })
        .min()
}

/// Whether a stalled shard might be stalled on a cycle rather than on a missing commit,
/// i.e. whether [`ExecutionScratch::executable_order`] is worth running at all.
///
/// A cycle only blocks execution once everything before it is done, and at that point its
/// members are heads — if a member's replica-predecessor were un-executed it would be a
/// dependency, hence in the cycle's own closure, hence the head instead. So it is enough
/// to ask whether some committed head is blocked purely by *committed* instances. When it
/// is not, which is the usual reason to be waiting, the answer comes back after a lookup
/// or two and the graph is never built.
///
/// It has to be asked on every stall, not only when a commit arrived: the fast path
/// executes as it goes, and an execution can close a set just as a commit can.
pub fn cycle_possible(instances: &InstanceTable, executed: &Executed) -> bool {
    candidates(instances, executed).any(|uid| {
        instances[uid]
            .deps
            .pending_over(executed)
            .all(|dep| dep == uid || instances.get(dep).is_some_and(Instance::is_committed))
    })
}

/// Reusable state for the rooted, in-place Tarjan traversal used by `executable_order`.
/// The reference EPaxos executor likewise keeps one stack across attempts rather than
/// materializing a separate graph.
#[derive(Default)]
pub(super) struct ExecutionScratch {
    stack: Vec<usize>,
    roots: Vec<usize>,
    touched: Vec<usize>,
}

impl ExecutionScratch {
    /// The order in which committed commands become executable, following EPaxos\* Figure 1.
    ///
    /// Traverse dependencies directly from each committed instance, aborting a root as soon
    /// as its closure reaches a missing or uncommitted command. Completed dependency SCCs are
    /// retained even if a later branch blocks the root, just as in the reference executor.
    /// SCCs are emitted dependencies-first and commands within one SCC are ordered by uid.
    pub fn executable_order(
        &mut self,
        instances: &mut InstanceTable,
        executed: &Executed,
    ) -> Vec<usize> {
        debug_assert!(self.stack.is_empty());
        debug_assert!(self.roots.is_empty());
        debug_assert!(self.touched.is_empty());

        // The reference gets deterministic roots from its replica/instance arrays. Our
        // instances are indexed per replica, so sort their ids before the same traversal.
        self.roots.extend(
            instances
                .iter()
                .filter(|(_, instance)| instance.is_committed())
                .map(|(uid, _)| uid),
        );
        self.roots.sort_unstable();

        let mut next_index = 1;
        let mut order = Vec::with_capacity(self.roots.len());
        for root_index in 0..self.roots.len() {
            let root = self.roots[root_index];
            if instances[root].execution_index != 0 {
                continue;
            }
            if !self.strongconnect(root, instances, executed, &mut next_index, &mut order) {
                // A missing/uncommitted dependency leaves precisely the unfinished DFS path
                // on the stack. Forget it so another root can explore it independently.
                for uid in self.stack.drain(..) {
                    let instance = instances.get_mut(uid).expect("visited instance exists");
                    instance.execution_index = 0;
                    instance.execution_lowlink = 0;
                }
            }
        }
        debug_assert!(self.stack.is_empty());

        // Successful nodes are executed and removed by the caller, but clear their scratch
        // too so this function remains repeatable for tests and for any future caller.
        for uid in self.touched.drain(..) {
            let instance = instances.get_mut(uid).expect("visited instance exists");
            instance.execution_index = 0;
            instance.execution_lowlink = 0;
        }
        self.roots.clear();
        order
    }

    fn strongconnect(
        &mut self,
        uid: usize,
        instances: &mut InstanceTable,
        executed: &Executed,
        next_index: &mut usize,
        order: &mut Vec<usize>,
    ) -> bool {
        let index = *next_index;
        *next_index += 1;
        let instance = instances.get_mut(uid).expect("visited instance exists");
        instance.execution_index = index;
        instance.execution_lowlink = index;
        self.stack.push(uid);
        self.touched.push(uid);

        // Enumerate the same per-replica dependency prefixes as `pending_over`, one mark at
        // a time, so no borrow of `uid` and no copied dependency set survives the recursion.
        let step = uid_step(executed.process_count());
        for replica in 0..executed.process_count() {
            let Some(mark) = instances[uid].deps.get(replica) else {
                continue;
            };
            let first = executed.next_unexecuted(replica);
            if first > mark {
                continue;
            }
            for dep in (first..=mark).step_by(step) {
                if dep == uid || executed.contains(dep) {
                    continue;
                }
                let Some(dependency) = instances.get(dep) else {
                    return false;
                };
                if !dependency.is_committed() {
                    return false;
                }

                if dependency.execution_index == 0 {
                    if !self.strongconnect(dep, instances, executed, next_index, order) {
                        return false;
                    }
                    let dependency_lowlink = instances[dep].execution_lowlink;
                    let instance = instances.get_mut(uid).expect("visited instance exists");
                    instance.execution_lowlink = instance.execution_lowlink.min(dependency_lowlink);
                } else if self.stack.contains(&dep) {
                    let dependency_index = dependency.execution_index;
                    let instance = instances.get_mut(uid).expect("visited instance exists");
                    instance.execution_lowlink = instance.execution_lowlink.min(dependency_index);
                }
            }
        }

        if instances[uid].execution_lowlink == index {
            let component_start = self
                .stack
                .iter()
                .rposition(|candidate| *candidate == uid)
                .expect("an active Tarjan root is on the stack");
            let mut component = self.stack.split_off(component_start);
            component.sort_unstable();
            order.extend(component);
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::command::Command;
    use crate::consensus::deps::dep_set::DepSet;
    use crate::consensus::deps::instance::Instance;
    use petgraph::algo::tarjan_scc;
    use petgraph::graph::{DiGraph, NodeIndex};
    use std::collections::HashMap;
    use std::collections::HashSet;

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
        let mut instance = Instance::new(0, 0, vec![command()], dep_set.clone(), N);
        instance.commit(dep_set);
        instance
    }

    fn uncommitted() -> Instance {
        Instance::new(0, 0, vec![command()], DepSet::new(N), N)
    }

    fn executable_order(instances: &mut InstanceTable, executed: &Executed) -> Vec<usize> {
        ExecutionScratch::default().executable_order(instances, executed)
    }

    /// The materialized-graph implementation retained only as an equivalence oracle.
    fn previous_executable_order(instances: &InstanceTable, executed: &Executed) -> Vec<usize> {
        let mut closed: HashSet<usize> = instances
            .iter()
            .filter(|(_, instance)| instance.is_committed())
            .map(|(uid, _)| uid)
            .collect();
        loop {
            let mut removed = false;
            for uid in closed.clone() {
                if instances[uid]
                    .deps
                    .pending_over(executed)
                    .any(|dep| dep != uid && !closed.contains(&dep))
                {
                    closed.remove(&uid);
                    removed = true;
                }
            }
            if !removed {
                break;
            }
        }

        let mut ordered: Vec<usize> = closed.iter().copied().collect();
        ordered.sort_unstable();
        let mut graph = DiGraph::<usize, ()>::new();
        let mut nodes: HashMap<usize, NodeIndex> = HashMap::with_capacity(closed.len());
        for uid in &ordered {
            nodes.insert(*uid, graph.add_node(*uid));
        }
        for uid in &ordered {
            for dep in instances[*uid].deps.pending_over(executed) {
                if dep != *uid && closed.contains(&dep) {
                    graph.add_edge(nodes[uid], nodes[&dep], ());
                }
            }
        }

        let mut order = Vec::with_capacity(closed.len());
        for component in tarjan_scc(&graph) {
            let mut component: Vec<usize> = component.into_iter().map(|node| graph[node]).collect();
            component.sort_unstable();
            order.extend(component);
        }
        order
    }

    /// The shard's drain, with the graph forced rather than gated on [`cycle_possible`], so
    /// that the graph-free path can be compared against the graph command for command.
    fn drain(instances: &mut InstanceTable, executed: &mut Executed) -> Vec<usize> {
        let mut order = Vec::new();
        let execute = |instances: &mut InstanceTable,
                       executed: &mut Executed,
                       order: &mut Vec<usize>,
                       uid: usize| {
            instances.remove(uid);
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
        let mut instances = InstanceTable::new(N);
        instances.insert(uid(1, 0), committed(&[]));
        instances.insert(uid(0, 0), committed(&[]));
        let order = executable_order(&mut instances, &Executed::new(N));
        assert_eq!(order, vec![uid(0, 0), uid(1, 0)]);
    }

    #[test]
    fn a_dependency_executes_before_its_dependent() {
        let mut instances = InstanceTable::new(N);
        instances.insert(uid(0, 0), committed(&[]));
        instances.insert(uid(1, 0), committed(&[uid(0, 0)]));
        let order = executable_order(&mut instances, &Executed::new(N));
        assert_eq!(order, vec![uid(0, 0), uid(1, 0)]);
    }

    #[test]
    fn an_uncommitted_dependency_blocks_the_whole_chain() {
        let mut instances = InstanceTable::new(N);
        instances.insert(uid(0, 0), uncommitted());
        instances.insert(uid(1, 0), committed(&[uid(0, 0)]));
        instances.insert(uid(2, 0), committed(&[uid(1, 0)]));
        assert!(executable_order(&mut instances, &Executed::new(N)).is_empty());
    }

    #[test]
    fn an_aborted_traversal_leaves_reusable_scratch_clean() {
        let dependency = uid(0, 0);
        let dependent = uid(1, 0);
        let mut instances = InstanceTable::new(N);
        instances.insert(dependency, uncommitted());
        instances.insert(dependent, committed(&[dependency]));
        let executed = Executed::new(N);
        let mut scratch = ExecutionScratch::default();

        assert!(
            scratch
                .executable_order(&mut instances, &executed)
                .is_empty()
        );
        assert!(
            scratch
                .executable_order(&mut instances, &executed)
                .is_empty()
        );

        instances
            .get_mut(dependency)
            .expect("dependency exists")
            .commit(DepSet::new(N));
        assert_eq!(
            scratch.executable_order(&mut instances, &executed),
            vec![dependency, dependent]
        );
    }

    #[test]
    fn a_missing_dependency_blocks_too() {
        // uid(0, 0) is named by a watermark but no instance exists for it yet.
        let mut instances = InstanceTable::new(N);
        instances.insert(uid(1, 0), committed(&[uid(0, 0)]));
        assert!(executable_order(&mut instances, &Executed::new(N)).is_empty());
    }

    #[test]
    fn an_already_executed_dependency_does_not_block() {
        let mut executed = Executed::new(N);
        executed.insert(uid(0, 0));
        let mut instances = InstanceTable::new(N);
        instances.insert(uid(1, 0), committed(&[uid(0, 0)]));
        assert_eq!(executable_order(&mut instances, &executed), vec![uid(1, 0)]);
    }

    #[test]
    fn a_cycle_becomes_one_component_ordered_by_uid() {
        // Visibility only requires one direction; when both hold, the two commands form a
        // component and every process breaks the tie the same way.
        let mut instances = InstanceTable::new(N);
        instances.insert(uid(1, 0), committed(&[uid(0, 0)]));
        instances.insert(uid(0, 0), committed(&[uid(1, 0)]));
        let order = executable_order(&mut instances, &Executed::new(N));
        assert_eq!(order, vec![uid(0, 0), uid(1, 0)]);
    }

    #[test]
    fn a_component_still_waits_for_what_precedes_it() {
        let mut instances = InstanceTable::new(N);
        instances.insert(uid(2, 0), committed(&[]));
        instances.insert(uid(1, 0), committed(&[uid(0, 0), uid(2, 0)]));
        instances.insert(uid(0, 0), committed(&[uid(1, 0)]));
        let order = executable_order(&mut instances, &Executed::new(N));
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
            let mut instances = InstanceTable::new(N);
            for (uid, instance) in scenario {
                instances.insert(uid, instance);
            }
            let expected = executable_order(&mut instances, &Executed::new(N));
            let mut executed = Executed::new(N);
            assert_eq!(drain(&mut instances, &mut executed), expected);
        }
    }

    #[test]
    fn rooted_tarjan_matches_the_previous_graph_implementation() {
        let uids = [uid(0, 0), uid(1, 0), uid(2, 0)];
        // Every directed graph over three instances, under every combination of committed
        // and uncommitted phases. Self edges are included because watermarks can contain them.
        for edges in 0usize..(1 << (N * N)) {
            for committed_mask in 0usize..(1 << N) {
                let mut instances = InstanceTable::new(N);
                for (from, uid) in uids.iter().copied().enumerate() {
                    let mut deps = DepSet::new(N);
                    for (to, dependency) in uids.iter().copied().enumerate() {
                        if edges & (1 << (from * N + to)) != 0 {
                            deps.insert(dependency);
                        }
                    }
                    let mut instance = Instance::new(0, 0, vec![command()], deps.clone(), N);
                    if committed_mask & (1 << from) != 0 {
                        instance.commit(deps);
                    }
                    instances.insert(uid, instance);
                }

                let executed = Executed::new(N);
                let previous = previous_executable_order(&instances, &executed);
                let order = executable_order(&mut instances, &executed);
                let mut ordered_set = order.clone();
                let mut previous_set = previous.clone();
                ordered_set.sort_unstable();
                previous_set.sort_unstable();
                assert_eq!(
                    ordered_set, previous_set,
                    "edges={edges:#011b}, committed={committed_mask:#05b}"
                );

                // With an edge in at least one direction between every pair, SCCs form a
                // total order. That is our one-write-key workload, and there the complete
                // execution order must remain identical. Incomparable SCCs in other graphs
                // commute, so the reference-style traversal may visit them in a different
                // valid order than petgraph's reverse adjacency iteration.
                let all_conflict = (0..N).all(|left| {
                    (left + 1..N).all(|right| {
                        edges & (1 << (left * N + right)) != 0
                            || edges & (1 << (right * N + left)) != 0
                    })
                });
                if all_conflict {
                    assert_eq!(
                        order, previous,
                        "edges={edges:#011b}, committed={committed_mask:#05b}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_cycle_is_broken_by_the_graph() {
        let mut instances = InstanceTable::new(N);
        instances.insert(uid(1, 0), committed(&[uid(0, 0)]));
        instances.insert(uid(0, 0), committed(&[uid(1, 0)]));
        let mut executed = Executed::new(N);
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
        let mut instances = InstanceTable::new(N);
        instances.insert(uid(0, 0), committed(&[]));
        instances.insert(uid(1, 0), committed(&[uid(0, 0), uid(2, 0)]));
        instances.insert(uid(2, 0), committed(&[uid(0, 0), uid(1, 0)]));
        let mut executed = Executed::new(N);

        assert_eq!(next_executable(&instances, &executed), Some(uid(0, 0)));
        instances.remove(uid(0, 0));
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
        let mut instances = InstanceTable::new(N);
        instances.insert(uid(0, 0), uncommitted());
        instances.insert(uid(1, 0), committed(&[uid(0, 0)]));
        assert!(!cycle_possible(&instances, &Executed::new(N)));
        // Nor when the dependency is not even known here.
        let mut instances = InstanceTable::new(N);
        instances.insert(uid(1, 0), committed(&[uid(0, 0)]));
        assert!(!cycle_possible(&instances, &Executed::new(N)));
    }

    #[test]
    fn a_self_dependency_does_not_block() {
        // A watermark cannot say "all of this replica but me", so a set can name the
        // command it belongs to. Execution ignores that, on both paths.
        let mut instances = InstanceTable::new(N);
        instances.insert(uid(0, 0), committed(&[uid(0, 0)]));
        let mut executed = Executed::new(N);
        assert_eq!(next_executable(&instances, &executed), Some(uid(0, 0)));
        assert_eq!(drain(&mut instances, &mut executed), vec![uid(0, 0)]);
    }

    #[test]
    fn only_the_lowest_uid_of_a_replica_is_a_candidate() {
        // Later commands of a replica wait for its earlier ones, so they are not heads.
        let mut instances = InstanceTable::new(N);
        instances.insert(uid(0, 1), committed(&[]));
        assert_eq!(next_executable(&instances, &Executed::new(N)), None);
        let mut executed = Executed::new(N);
        executed.insert(uid(0, 0));
        assert_eq!(next_executable(&instances, &executed), Some(uid(0, 1)));
    }
}
