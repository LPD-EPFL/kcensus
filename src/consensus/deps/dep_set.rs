use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// The uid of the `k`-th command issued by replica `r` on a shard is `2 * (k * n + r)`,
/// so consecutive uids of one replica are `2 * n` apart. Everything in this module relies
/// on that, which is what lets a dependency set be a watermark per replica rather than a
/// set of uids. Batching (which is what the odd uids were for) does not exist in
/// dependency mode, so every uid here is even.
#[inline]
pub fn uid_step(process_count: usize) -> usize {
    2 * process_count
}

/// The replica that issued `uid`.
#[inline]
pub fn requester_of(uid: usize, process_count: usize) -> usize {
    debug_assert_eq!(uid % 2, 0, "dependency mode does not use batch uids");
    (uid >> 1) % process_count
}

/// The position of `uid` in its replica's sequence, the form a mark is stored in.
#[inline]
fn index_of(uid: usize, process_count: usize) -> u32 {
    let index = (uid >> 1) / process_count;
    assert!(
        index < u32::MAX as usize,
        "a run never issues 4G commands per replica"
    );
    index as u32
}

/// The uid `replica` gave its `index`-th command.
#[inline]
fn uid_at(index: u32, replica: usize, process_count: usize) -> usize {
    2 * (index as usize * process_count + replica)
}

/// A dependency set, compressed as one watermark per replica.
///
/// `marks[r] == k + 1` means "every command of replica `r` on this shard up to and
/// including its `k`-th", so a set of arbitrary size costs `n` entries and never grows.
/// `0` means no dependency on that replica at all.
///
/// A mark holds the position rather than the uid: the slot already says which replica it
/// is, and the smaller number is what `bincode`'s varint encoding charges for. The
/// reference stores the same thing, `Deps []int32` indexed by replica (`epaxos/defs.go`).
///
/// The representation is only sound because all commands of a shard conflict with each
/// other (`command.shard = key % shards`), so a dependency set never has to name a
/// command from another shard, and depending on a command implies depending on every
/// earlier command of the same replica.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepSet {
    marks: Vec<u32>,
}

// Some of these are exercised only by the unit tests below, but they are the natural
// operations on a watermark and belong to the type.
#[allow(dead_code)]
impl DepSet {
    #[inline]
    pub fn new(process_count: usize) -> Self {
        Self {
            marks: vec![0; process_count],
        }
    }

    #[inline]
    pub fn process_count(&self) -> usize {
        self.marks.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.marks.iter().all(|mark| *mark == 0)
    }

    #[inline]
    pub fn get(&self, replica: usize) -> Option<usize> {
        let mark = self.marks[replica];
        (mark != 0).then(|| uid_at(mark - 1, replica, self.marks.len()))
    }

    /// Adds `uid` (and, implicitly, every earlier command of its replica).
    #[inline]
    pub fn insert(&mut self, uid: usize) {
        let replica = requester_of(uid, self.marks.len());
        let mark = index_of(uid, self.marks.len()) + 1;
        if self.marks[replica] < mark {
            self.marks[replica] = mark;
        }
    }

    /// Elementwise max: the union of the two sets, used by the slow path to merge the
    /// dependency sets reported by a quorum.
    #[inline]
    pub fn union_with(&mut self, other: &DepSet) {
        debug_assert_eq!(self.marks.len(), other.marks.len());
        for (mark, other_mark) in self.marks.iter_mut().zip(other.marks.iter()) {
            if *mark < *other_mark {
                *mark = *other_mark;
            }
        }
    }

    #[inline]
    pub fn union(mut self, other: &DepSet) -> Self {
        self.union_with(other);
        self
    }

    /// Elementwise min: the intersection of the two sets. Both are downward closed, so the
    /// smaller watermark per replica *is* the intersection.
    ///
    /// SwiftPaxos reads use it to bound a quorum's union by the leader's own view. See
    /// `DepShard::submit_read`.
    #[inline]
    pub fn intersect_with(&mut self, other: &DepSet) {
        debug_assert_eq!(self.marks.len(), other.marks.len());
        for (mark, other_mark) in self.marks.iter_mut().zip(other.marks.iter()) {
            if *mark > *other_mark {
                *mark = *other_mark;
            }
        }
    }

    /// True if `uid` is in the set.
    #[inline]
    pub fn contains(&self, uid: usize) -> bool {
        index_of(uid, self.marks.len()) < self.marks[requester_of(uid, self.marks.len())]
    }

    /// True if everything this set depends on is already covered by `other`, i.e. `other`
    /// is a superset. Used against the shard's `executed` watermark to decide whether a
    /// command's dependencies are all done.
    #[inline]
    pub fn is_covered_by(&self, other: &DepSet) -> bool {
        debug_assert_eq!(self.marks.len(), other.marks.len());
        self.marks
            .iter()
            .zip(other.marks.iter())
            .all(|(mark, other_mark)| mark <= other_mark)
    }

    /// Removes `uid` when it is the watermark for its replica.
    ///
    /// Used so that a command never proposes a dependency on itself: learning that a
    /// command exists (from someone else's ack) puts it in `seen` before its own
    /// `PreAccept` is processed. A watermark cannot express "all of this replica but this
    /// one", so if a *later* command of the same replica is also in the set the uid stays;
    /// execution filters self-dependencies out anyway.
    /// Drops what this set says about `replica`, leaving the mark to come from elsewhere.
    #[inline]
    pub fn clear_replica(&mut self, replica: usize) {
        self.marks[replica] = 0;
    }

    pub fn without_uid(&mut self, uid: usize) {
        let replica = requester_of(uid, self.marks.len());
        let index = index_of(uid, self.marks.len());
        if self.marks[replica] == index + 1 {
            self.marks[replica] = index;
        }
    }

    /// The uids this set depends on that `executed` has not covered yet, in increasing
    /// order per replica.
    ///
    /// A watermark names commands implicitly, and this is where they are made explicit
    /// again: everything of replica `r` above `executed[r]` and up to `marks[r]`. The
    /// enumeration is bounded by what is still unexecuted, not by the whole history, and
    /// it is exact because a replica's uids on a shard form the arithmetic sequence
    /// `2r, 2r + 2n, 2r + 4n, …`.
    ///
    /// Lazy, so that a caller that only wants to know whether *some* dependency blocks
    /// it stops at the first one instead of walking the whole backlog.
    pub fn pending_over<'a>(&'a self, executed: &'a Executed) -> impl Iterator<Item = usize> + 'a {
        debug_assert_eq!(self.marks.len(), executed.watermark.marks.len());
        let step = uid_step(self.marks.len());
        self.marks
            .iter()
            .enumerate()
            .filter_map(move |(replica, mark)| {
                let mark = *mark;
                // `done` counts what the prefix covers, so it is also the next position.
                let done = executed.watermark.marks[replica];
                if mark == 0 || done >= mark {
                    return None;
                }
                let first = uid_at(done, replica, self.marks.len());
                let last = uid_at(mark - 1, replica, self.marks.len());
                Some((first..=last).step_by(step))
            })
            .flatten()
            .filter(move |uid| !executed.ahead.contains(uid))
    }
}

/// What has been executed: a contiguous prefix per replica, plus whatever ran ahead of it.
///
/// A watermark alone is enough while a replica's commands execute in uid order, which a
/// per-shard dependency set guarantees — everything the coordinator had seen in that shard
/// is a dependency. Once dependencies are per key, two commands from one replica on
/// different keys do not order each other and either may go first. The reference executor
/// behaves the same way: it marks each instance `EXECUTED` on its own and only advances
/// `ExecedUpTo[q]` when the instance it just ran closed the gap (`epaxos.go:397`).
#[derive(Debug, Clone)]
pub struct Executed {
    /// The prefix every replica has finished, the form a dependency range starts from.
    watermark: DepSet,
    /// Executed past `watermark`, waiting for the gap below to close. Holds only the
    /// reordering window, so it stays small.
    ahead: HashSet<usize>,
}

impl Executed {
    pub fn new(process_count: usize) -> Self {
        Self {
            watermark: DepSet::new(process_count),
            ahead: HashSet::new(),
        }
    }

    #[inline]
    pub fn process_count(&self) -> usize {
        self.watermark.process_count()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.watermark.is_empty() && self.ahead.is_empty()
    }

    #[inline]
    pub fn contains(&self, uid: usize) -> bool {
        self.watermark.contains(uid) || self.ahead.contains(&uid)
    }

    /// The lowest uid of `replica` that is not covered by the prefix.
    #[inline]
    pub fn next_unexecuted(&self, replica: usize) -> usize {
        uid_at(self.watermark.marks[replica], replica, self.process_count())
    }

    /// The prefix alone, which is what an idle shard carries while it sleeps.
    #[inline]
    pub fn watermark(&self) -> &DepSet {
        debug_assert!(self.ahead.is_empty(), "an idle shard has no gap to close");
        &self.watermark
    }

    /// Consumes this into its prefix, which an idle shard keeps while it sleeps.
    pub fn into_watermark(self) -> DepSet {
        debug_assert!(self.ahead.is_empty(), "an idle shard has no gap to close");
        self.watermark
    }

    pub fn set_watermark(&mut self, watermark: DepSet) {
        debug_assert!(self.ahead.is_empty());
        self.watermark = watermark;
    }

    /// Records `uid`, closing whatever gap that opens.
    pub fn insert(&mut self, uid: usize) {
        let step = uid_step(self.process_count());
        let replica = requester_of(uid, self.process_count());
        if uid != self.next_unexecuted(replica) {
            self.ahead.insert(uid);
            return;
        }
        self.watermark.insert(uid);
        let mut next = uid + step;
        while self.ahead.remove(&next) {
            self.watermark.insert(next);
            next += step;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const N: usize = 3;

    /// The `k`-th uid of replica `r`, matching `ConsensusShard::get_next_uid`.
    fn uid(replica: usize, k: usize) -> usize {
        2 * (k * N + replica)
    }

    #[test]
    fn uid_layout_matches_the_allocator() {
        for replica in 0..N {
            for k in 0..5 {
                assert_eq!(requester_of(uid(replica, k), N), replica);
            }
            assert_eq!(uid(replica, 1) - uid(replica, 0), uid_step(N));
        }
    }

    #[test]
    fn intersect_keeps_the_lower_watermark_of_each_replica() {
        let mut mine = DepSet::new(N);
        mine.insert(uid(0, 3));
        mine.insert(uid(1, 1));
        let mut leaders = DepSet::new(N);
        leaders.insert(uid(0, 1));
        leaders.insert(uid(1, 4));
        leaders.insert(uid(2, 0)); // not in `mine`: stays out
        mine.intersect_with(&leaders);
        assert_eq!(mine.get(0), Some(uid(0, 1)));
        assert_eq!(mine.get(1), Some(uid(1, 1)));
        assert_eq!(mine.get(2), None);
        // Intersecting is idempotent, and bounded by both sides.
        let bounded = mine.clone();
        mine.intersect_with(&leaders);
        assert_eq!(mine, bounded);
        assert!(mine.is_covered_by(&leaders));
    }

    #[test]
    fn insert_keeps_the_highest_and_covers_everything_below() {
        let mut deps = DepSet::new(N);
        assert!(deps.is_empty());
        deps.insert(uid(1, 2));
        deps.insert(uid(1, 0)); // older: must not lower the watermark
        assert_eq!(deps.get(1), Some(uid(1, 2)));
        assert!(deps.contains(uid(1, 0)));
        assert!(deps.contains(uid(1, 2)));
        assert!(!deps.contains(uid(1, 3)));
        assert!(!deps.contains(uid(0, 0)));
    }

    #[test]
    fn union_is_elementwise_max() {
        let mut left = DepSet::new(N);
        left.insert(uid(0, 3));
        left.insert(uid(1, 1));
        let mut right = DepSet::new(N);
        right.insert(uid(1, 4));
        right.insert(uid(2, 0));

        let merged = left.clone().union(&right);
        assert_eq!(merged.get(0), Some(uid(0, 3)));
        assert_eq!(merged.get(1), Some(uid(1, 4)));
        assert_eq!(merged.get(2), Some(uid(2, 0)));

        // union is commutative and absorbs both sides
        assert_eq!(merged, right.union(&left));
        assert!(left.is_covered_by(&merged));
    }

    #[test]
    fn coverage_is_the_superset_test() {
        let mut small = DepSet::new(N);
        small.insert(uid(0, 1));
        let mut big = DepSet::new(N);
        big.insert(uid(0, 2));
        big.insert(uid(2, 0));

        assert!(small.is_covered_by(&big));
        assert!(!big.is_covered_by(&small));
        assert!(DepSet::new(N).is_covered_by(&small));
    }

    #[test]
    fn without_uid_drops_only_the_top_of_a_watermark() {
        let mut deps = DepSet::new(N);
        deps.insert(uid(1, 2));
        deps.without_uid(uid(1, 2));
        assert_eq!(deps.get(1), Some(uid(1, 1)));

        // Down to nothing once the first command of that replica is removed.
        let mut deps = DepSet::new(N);
        deps.insert(uid(1, 0));
        deps.without_uid(uid(1, 0));
        assert_eq!(deps.get(1), None);

        // A later command of the same replica keeps the watermark where it is.
        let mut deps = DepSet::new(N);
        deps.insert(uid(1, 3));
        deps.without_uid(uid(1, 1));
        assert_eq!(deps.get(1), Some(uid(1, 3)));
    }

    #[test]
    fn pending_enumerates_only_what_is_left() {
        let mut deps = DepSet::new(N);
        deps.insert(uid(0, 2));
        deps.insert(uid(1, 0));

        // Nothing executed: every implied command shows up.
        let mut pending: Vec<usize> = deps.pending_over(&Executed::new(N)).collect();
        pending.sort();
        assert_eq!(
            pending,
            vec![uid(0, 0), uid(0, 1), uid(0, 2), uid(1, 0)].tap_sorted()
        );

        // A prefix executed: only the tail is left.
        let mut executed = Executed::new(N);
        executed.insert(uid(0, 0));
        executed.insert(uid(0, 1));
        executed.insert(uid(1, 0));
        assert_eq!(
            deps.pending_over(&executed).collect::<Vec<_>>(),
            vec![uid(0, 2)]
        );

        // Everything executed.
        executed.insert(uid(0, 2));
        assert_eq!(deps.pending_over(&executed).count(), 0);
    }

    /// A command can run before its replica's earlier one once dependencies are per key,
    /// so the prefix has to tolerate a gap and close it later.
    #[test]
    fn executed_tolerates_a_gap_and_closes_it() {
        let mut executed = Executed::new(N);
        executed.insert(uid(0, 1));
        assert!(executed.contains(uid(0, 1)));
        assert!(!executed.contains(uid(0, 0)));
        assert_eq!(executed.next_unexecuted(0), uid(0, 0));

        // What ran ahead is not reported as still pending.
        let mut deps = DepSet::new(N);
        deps.insert(uid(0, 1));
        assert_eq!(
            deps.pending_over(&executed).collect::<Vec<_>>(),
            vec![uid(0, 0)]
        );

        executed.insert(uid(0, 0));
        assert_eq!(executed.next_unexecuted(0), uid(0, 2));
        assert_eq!(deps.pending_over(&executed).count(), 0);
    }

    /// `0` is a mark meaning "nothing", and it is also replica 0's first uid. The two must
    /// not be confused: a mark holds the position plus one.
    #[test]
    fn the_first_uid_is_not_the_empty_mark() {
        let first = uid(0, 0);
        assert_eq!(first, 0);

        let empty = DepSet::new(N);
        assert!(empty.is_empty());
        assert_eq!(empty.get(0), None);
        assert!(!empty.contains(first));

        let mut set = DepSet::new(N);
        set.insert(first);
        assert!(!set.is_empty());
        assert_eq!(set.get(0), Some(first));
        assert!(set.contains(first));
        assert!(!set.contains(uid(0, 1)));

        // And removing it goes back to empty rather than to "position zero".
        set.without_uid(first);
        assert!(set.is_empty());
        assert_eq!(set.get(0), None);
    }

    trait TapSorted {
        fn tap_sorted(self) -> Self;
    }
    impl TapSorted for Vec<usize> {
        fn tap_sorted(mut self) -> Self {
            self.sort();
            self
        }
    }
}
