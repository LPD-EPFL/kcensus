use crate::consensus::deps::dep_set::DepSet;
use crate::consensus::deps::instance::Instance;
use petgraph::algo::tarjan_scc;
use petgraph::graph::{DiGraph, NodeIndex};
use std::collections::{HashMap, HashSet};

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
pub fn executable_order(
    instances: &HashMap<usize, Instance>,
    executed: &DepSet,
) -> Vec<usize> {
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
                .into_iter()
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
}
