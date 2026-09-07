use crate::consensus::command::{Command, CommitReport};
use crate::consensus::deps::dep_set::DepSet;
use crate::consensus::message::ReadId;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};

/// A read waiting for the replicas to say what they have seen.
struct PendingRead {
    command: Command,
    /// The union of the answers counted so far. Everything in it has to be executed here
    /// before the read can be served.
    union: DepSet,
    /// How many answers went into `union` — the first quorum to reply, and no more. Each
    /// replica answers a request exactly once, so counting is enough to know who has.
    answers: usize,
    /// SwiftPaxos: the leader's own answer, which bounds the union. `None` until it comes.
    leader_seen: Option<DepSet>,
    /// Filled in the first time the read's requirement is known, i.e. when the quorum (and,
    /// in SwiftPaxos, the leader) has answered. A read is fast when that requirement is
    /// already executed then: the answers alone were enough, with nothing to wait for.
    report: Option<CommitReport>,
}

/// The reads this node has issued for one shard and has not served yet.
///
/// A read takes no instance and no dependency set of its own. It is served once the union
/// of what a majority of replicas had seen — bounded, in SwiftPaxos, by what the leader
/// had seen — is executed here. See `DepShard::submit_read` for why that is enough.
pub struct ReadTracker {
    next_id: usize,
    read_quorum: usize,
    /// SwiftPaxos' leader, whose answer bounds the union. `None` in EPaxos, which has no
    /// leader and whose own read dependencies are a quorum union like ours.
    leader: Option<usize>,
    reads: BTreeMap<ReadId, PendingRead>,
}

/// Reports every read still waiting, so that a stuck shard says what it is waiting for. A
/// shard holding a pending read cannot sleep, so the deadlock detector always reaches it.
impl Debug for ReadTracker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "quorum={}, pending=[", self.read_quorum)?;
        for (i, (id, read)) in self.reads.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{id:?}: {} answer(s)", read.answers)?;
            if self.leader.is_some() && read.leader_seen.is_none() {
                write!(f, ", waiting on the leader")?;
            }
            write!(f, ", needs {:?}", read.union)?;
        }
        write!(f, "]")
    }
}

impl ReadTracker {
    pub fn new(read_quorum: usize, leader: Option<usize>) -> Self {
        Self {
            next_id: 0,
            read_quorum,
            leader,
            reads: BTreeMap::new(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.reads.is_empty()
    }

    #[inline]
    pub fn next_id(&self) -> usize {
        self.next_id
    }

    /// Restores the id counter of a shard that was put to sleep, so that a late answer to
    /// a read issued before it slept cannot match a new one.
    #[inline]
    pub fn set_next_id(&mut self, next_id: usize) {
        debug_assert!(self.is_empty());
        self.next_id = next_id;
    }

    /// Registers a read and returns the id the answers will carry. Ids are never reused,
    /// including across a sleep (`set_next_id`), so an answer matches at most one read.
    pub fn insert(&mut self, command: Command, process_count: usize) -> ReadId {
        debug_assert!(command.read_only);
        let id = ReadId(self.next_id);
        self.next_id += 1;
        let old = self.reads.insert(
            id,
            PendingRead {
                command,
                union: DepSet::new(process_count),
                answers: 0,
                leader_seen: None,
                report: None,
            },
        );
        debug_assert!(old.is_none());
        id
    }

    /// Records one replica's answer. Answers for an unknown id are late ones for a read
    /// already served, and are dropped.
    ///
    /// The union stops growing at the first quorum to answer. Any majority meets any
    /// commit quorum, so one is all the read needs; merging the answers that arrive after
    /// it would only make the read wait for commands it is free to miss. Which majority it
    /// is settles itself — the fastest to reply, which is what the latency model assumes.
    ///
    /// The leader's answer is kept whenever it comes, because it *bounds* the union rather
    /// than adding to it.
    pub fn receive(&mut self, id: ReadId, src: usize, seen: &DepSet) {
        let quorum = self.read_quorum;
        let leader = self.leader;
        let Some(read) = self.reads.get_mut(&id) else {
            return;
        };
        if read.answers < quorum {
            read.answers += 1;
            read.union.union_with(seen);
        }
        if leader == Some(src) {
            read.leader_seen.get_or_insert_with(|| seen.clone());
        }
    }

    /// The reads whose quorum is complete and whose answers `executed` now covers, in the
    /// order they were issued.
    pub fn take_ready(&mut self, executed: &DepSet) -> Vec<Command> {
        let quorum = self.read_quorum;
        let leader = self.leader;
        let ready: Vec<ReadId> = self
            .reads
            .iter_mut()
            .filter(|(_, read)| read.answers >= quorum)
            .filter_map(|(id, read)| {
                let mut required = read.union.clone();
                match (leader, read.leader_seen.as_ref()) {
                    // SwiftPaxos: bound by the leader's view. Waiting for it is what makes
                    // the bound sound as well as tighter.
                    (Some(_), Some(leader_seen)) => required.intersect_with(leader_seen),
                    (Some(_), None) => return None,
                    (None, _) => {}
                }
                let covered = required.is_covered_by(executed);
                read.report.get_or_insert_with(|| CommitReport {
                    fast: covered,
                    waited_for: required.pending_over(executed).count(),
                });
                covered.then_some(*id)
            })
            .collect();
        ready
            .iter()
            .map(|id| {
                let read = self.reads.remove(id).expect("listed above");
                let mut command = read.command;
                command.report = read.report;
                command
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const N: usize = 3;
    const LEADER: usize = 0;

    fn uid(replica: usize, k: usize) -> usize {
        2 * (k * N + replica)
    }

    fn deps(uids: &[usize]) -> DepSet {
        let mut set = DepSet::new(N);
        for uid in uids {
            set.insert(*uid);
        }
        set
    }

    fn read() -> Command {
        Command {
            requester: 0,
            shard: 0,
            command: Vec::new(),
            read_only: true,
            arrival_slot: 0,
            report: None,
        }
    }

    #[test]
    fn a_read_waits_for_a_quorum_then_for_execution() {
        let mut tracker = ReadTracker::new(2, None);
        let id = tracker.insert(read(), N);
        tracker.receive(id, 0, &deps(&[uid(0, 0)]));
        // One answer is not a quorum, however much has been executed.
        assert!(tracker.take_ready(&deps(&[uid(0, 5)])).is_empty());
        tracker.receive(id, 1, &deps(&[uid(1, 1)]));
        // Now a quorum, but the union is not executed here yet.
        assert!(tracker.take_ready(&deps(&[uid(0, 0)])).is_empty());
        assert_eq!(tracker.take_ready(&deps(&[uid(0, 0), uid(1, 1)])).len(), 1);
        assert!(tracker.is_empty());
    }

    #[test]
    fn swift_paxos_bounds_the_union_by_the_leader_and_waits_for_it() {
        let mut tracker = ReadTracker::new(2, Some(LEADER));
        let id = tracker.insert(read(), N);
        // A quorum of followers, one of which has seen a command the leader has not.
        tracker.receive(id, 1, &deps(&[uid(1, 0)]));
        tracker.receive(id, 2, &deps(&[uid(1, 3)]));
        // The leader's answer is missing, so nothing is served even though we executed it.
        assert!(tracker.take_ready(&deps(&[uid(1, 3)])).is_empty());
        // The leader has only seen the older one: the union is cut back to it, and the
        // read no longer waits for `uid(1, 3)`.
        tracker.receive(id, LEADER, &deps(&[uid(1, 0)]));
        assert_eq!(tracker.take_ready(&deps(&[uid(1, 0)])).len(), 1);
    }

    #[test]
    fn the_union_stops_at_the_first_quorum() {
        let mut tracker = ReadTracker::new(2, None);
        let id = tracker.insert(read(), N);
        tracker.receive(id, 0, &deps(&[uid(0, 0)]));
        tracker.receive(id, 1, &deps(&[uid(1, 0)]));
        // A third replica has seen more, but the quorum is already settled: the read is
        // free to miss it, and waiting for it would be waiting for nothing.
        tracker.receive(id, 2, &deps(&[uid(2, 7)]));
        assert_eq!(tracker.take_ready(&deps(&[uid(0, 0), uid(1, 0)])).len(), 1);
    }

    #[test]
    fn a_late_answer_to_a_served_read_is_dropped() {
        let mut tracker = ReadTracker::new(1, None);
        let id = tracker.insert(read(), N);
        tracker.receive(id, 0, &DepSet::new(N));
        assert_eq!(tracker.take_ready(&DepSet::new(N)).len(), 1);
        tracker.receive(id, 1, &deps(&[uid(1, 9)]));
        assert!(tracker.is_empty());
    }
}
