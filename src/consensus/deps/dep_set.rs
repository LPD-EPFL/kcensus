use serde::{Deserialize, Serialize};

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

/// A dependency set, compressed as one watermark per replica.
///
/// `marks[r] == Some(uid)` means "every command of replica `r` on this shard up to and
/// including `uid`", so a set of arbitrary size costs `n` entries and never grows. `None`
/// means no dependency on that replica at all.
///
/// The representation is only sound because all commands of a shard conflict with each
/// other (`command.shard = key % shards`), so a dependency set never has to name a
/// command from another shard, and depending on a command implies depending on every
/// earlier command of the same replica.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepSet {
    marks: Vec<Option<usize>>,
}

// Some of these are exercised only by the unit tests below, but they are the natural
// operations on a watermark and belong to the type.
#[allow(dead_code)]
impl DepSet {
    #[inline]
    pub fn new(process_count: usize) -> Self {
        Self {
            marks: vec![None; process_count],
        }
    }

    #[inline]
    pub fn process_count(&self) -> usize {
        self.marks.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.marks.iter().all(Option::is_none)
    }

    #[inline]
    pub fn get(&self, replica: usize) -> Option<usize> {
        self.marks[replica]
    }

    /// Adds `uid` (and, implicitly, every earlier command of its replica).
    #[inline]
    pub fn insert(&mut self, uid: usize) {
        let replica = requester_of(uid, self.marks.len());
        let mark = &mut self.marks[replica];
        if mark.is_none_or(|current| current < uid) {
            *mark = Some(uid);
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

    /// True if `uid` is in the set.
    #[inline]
    pub fn contains(&self, uid: usize) -> bool {
        self.marks[requester_of(uid, self.marks.len())].is_some_and(|mark| uid <= mark)
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
    pub fn without_uid(&mut self, uid: usize) {
        let step = uid_step(self.marks.len());
        let replica = requester_of(uid, self.marks.len());
        if self.marks[replica] == Some(uid) {
            self.marks[replica] = uid.checked_sub(step).filter(|lower| *lower >= 2 * replica);
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
    pub fn pending_over<'a>(
        &'a self,
        executed: &'a DepSet,
    ) -> impl Iterator<Item = usize> + 'a {
        debug_assert_eq!(self.marks.len(), executed.marks.len());
        let step = uid_step(self.marks.len());
        self.marks
            .iter()
            .enumerate()
            .filter_map(move |(replica, mark)| {
                let mark = (*mark)?;
                let first = match executed.marks[replica] {
                    Some(done) if done >= mark => return None,
                    Some(done) => done + step,
                    None => 2 * replica,
                };
                Some((first..=mark).step_by(step))
            })
            .flatten()
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
        let mut pending: Vec<usize> = deps.pending_over(&DepSet::new(N)).collect();
        pending.sort();
        assert_eq!(
            pending,
            vec![uid(0, 0), uid(0, 1), uid(0, 2), uid(1, 0)].tap_sorted()
        );

        // A prefix executed: only the tail is left.
        let mut executed = DepSet::new(N);
        executed.insert(uid(0, 1));
        executed.insert(uid(1, 0));
        assert_eq!(
            deps.pending_over(&executed).collect::<Vec<_>>(),
            vec![uid(0, 2)]
        );

        // Everything executed.
        executed.insert(uid(0, 2));
        assert_eq!(deps.pending_over(&executed).count(), 0);
        assert!(deps.is_covered_by(&executed));
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
