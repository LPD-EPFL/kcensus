use crate::consensus::command::Command;
use crate::consensus::deps::dep_set::DepSet;
use bit_set::BitSet;

/// Where a command is in its lifecycle. Executed instances are dropped rather than kept
/// in a phase, so that the instance table only holds unfinished work — that is what lets
/// a shard go back to sleep.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    /// A proposal exists (ours or the coordinator's) but nothing is agreed.
    PreAccepted,
    /// A value has been accepted — EPaxos\*'s union, or SwiftPaxos' leader's proposal.
    /// Our vote is cast: nothing heard afterwards can take it back.
    Accepted,
    /// Agreed. Still waiting for its dependencies before it can be executed.
    Committed,
}

/// One consensus instance: a single command and the dependency set being agreed for it.
///
/// This is the unit that replaces the slot. A shard holds one of these per unfinished
/// command instead of a single round state.
///
/// EPaxos\* and SwiftPaxos keep the *same* state here. They differ in who is told about
/// each piece — EPaxos' `PreAcceptOk` reaches only the coordinator, SwiftPaxos' `FastAck`
/// reaches everyone — and in the rule that reads it, not in what is worth remembering.
/// Every process keeps all of it rather than only the coordinator, following EPaxos\*
/// Fig. 2: that is exactly the state a recovery would have to read, and recovery is the one
/// part of both protocols this repository does not implement.
#[derive(Debug)]
pub struct Instance {
    /// Who submitted the command, derived from its uid. Also read through `Debug` in the
    /// deadlock report.
    pub coordinator_pid: usize,
    /// The payload. Always present: an instance is not created until it arrives, because
    /// a command we do not hold conflicts with nothing (`cmd[id] = ⊥` in both papers).
    pub command: Command,
    /// The one process that sends `Accept` for this instance: SwiftPaxos' designated
    /// leader, or — EPaxos\* having none — the coordinator, which plays the part on the
    /// ballot-0 slow path. Its pre-accept is what a fast commit agrees on: `dep_init` in
    /// EPaxos\* (Fig. 3 lines 15 and 22), the leader's `Accept`, which is its own
    /// `FastAck`, in SwiftPaxos (Fig. 4 line 25). Either way that set lives in `preaccepts`
    /// like everyone else's — it is one, after all.
    leader: usize,
    /// EPaxos\* `dep`: our own proposal while `PreAccepted`, the accepted set from
    /// `Accepted` on, the agreed set once `Committed`.
    pub deps: DepSet,
    pub phase: Phase,
    /// Each process's *own* proposal, as it reported it, indexed by pid: EPaxos'
    /// `PreAcceptOk`, SwiftPaxos' `FastAck`, and our own. Kept rather than folded away
    /// because the leader's can arrive after some of them, and only then can they be
    /// classified.
    preaccepts: Vec<Option<DepSet>>,
    /// The processes that pre-accepted exactly `leader_deps()`. Both fast paths are a quorum
    /// condition on this set; EPaxos additionally requires that no one else has replied at
    /// all.
    endorsers: BitSet,
    /// Union of every proposal in `proposals`, maintained as they arrive — what EPaxos'
    /// slow path accepts.
    union: DepSet,
    /// The processes known to have accepted: they acknowledged an `Accept`, or sent one.
    pub accept_acked: BitSet,
    /// Whether this instance has left the fast route. Read into `CommitReport::fast`.
    /// SwiftPaxos sets it outright, EPaxos infers it from the `Accept`.
    left_fast_path: bool,
}

impl Instance {
    pub fn new(
        coordinator_pid: usize,
        leader: usize,
        command: Command,
        deps: DepSet,
        process_count: usize,
    ) -> Self {
        Self {
            coordinator_pid,
            command,
            leader,
            deps,
            phase: Phase::PreAccepted,
            preaccepts: vec![None; process_count],
            endorsers: BitSet::with_capacity(process_count),
            union: DepSet::new(process_count),
            accept_acked: BitSet::with_capacity(process_count),
            left_fast_path: false,
        }
    }

    #[inline]
    pub fn is_committed(&self) -> bool {
        self.phase == Phase::Committed
    }

    /// Whether our vote is cast. From `Accepted` on we have backed a value, and a
    /// proposal arriving afterwards — a `PreAccept` overtaken by the leader's `Accept`,
    /// say — no longer makes us propose one of our own.
    #[inline]
    pub fn is_settled(&self) -> bool {
        self.phase > Phase::PreAccepted
    }

    /// The set a fast commit would agree on, and so the one every fast-quorum member has
    /// to have pre-accepted. `None` until the leader's own pre-accept arrives — another
    /// process's can reach us first.
    #[inline]
    pub fn leader_deps(&self) -> Option<&DepSet> {
        self.preaccepts[self.leader].as_ref()
    }

    /// Records `src`'s own proposal. Returns false if we already had one from it, which is
    /// how both protocols ignore a duplicate ack.
    pub fn record_preaccept(&mut self, src: usize, deps: DepSet) -> bool {
        if self.preaccepts[src].is_some() {
            return false;
        }
        self.union.union_with(&deps);
        if src == self.leader {
            // The leader's own: classify everything that arrived before it.
            self.endorsers.insert(src);
            for (other, proposal) in self.preaccepts.iter().enumerate() {
                if proposal.as_ref() == Some(&deps) {
                    self.endorsers.insert(other);
                }
            }
        } else if self.leader_deps() == Some(&deps) {
            self.endorsers.insert(src);
        }
        self.preaccepts[src] = Some(deps);
        true
    }

    /// How many processes have reported their own proposal. Counted rather than tracked:
    /// it is one pass over `n` options, on the same order as the union it sits next to.
    #[inline]
    pub fn preaccepts_count(&self) -> usize {
        self.preaccepts.iter().flatten().count()
    }

    /// EPaxos\*: every pre-accept received so far was the coordinator's own set, so the
    /// fast path is still reachable (Fig. 3 line 22).
    #[inline]
    pub fn all_preaccepts_endorse(&self) -> bool {
        // `preaccepts` is indexed by pid, so its `len()` is the process count: compare
        // against how many entries are actually filled.
        self.endorsers.len() == self.preaccepts_count()
    }

    /// Union of every proposal received — EPaxos\*'s slow-path value. Visibility holds for
    /// it because every proposal is the coordinator's set plus what that process had seen.
    #[inline]
    pub fn union(&self) -> &DepSet {
        &self.union
    }

    /// The value we accepted, if any. In SwiftPaxos that is always the leader's proposal:
    /// it is the only thing anyone accepts, and it overrides whatever we had proposed.
    #[inline]
    pub fn accepted_deps(&self) -> Option<&DepSet> {
        self.is_settled().then_some(&self.deps)
    }

    /// SwiftPaxos: the processes holding the leader's value — those that pre-accepted the
    /// same set on their own, plus those that accepted it when the leader spoke. That is
    /// the reference implementation's `acceptFastAndSlowAck` (`swift.go:665-673`): a
    /// follower's ack counts if its dependencies equal the leader's, and a `SlowAck` — an
    /// `AcceptOk` here — always counts, because sending one means adopting the leader's set.
    ///
    /// `None` until the leader has spoken, since its pre-accept is what its `Accept`
    /// carries.
    pub fn fast_endorsers(&self) -> Option<BitSet> {
        self.leader_deps()?;
        let mut endorsers = self.accept_acked.clone();
        endorsers.union_with(&self.endorsers);
        Some(endorsers)
    }

    /// Backs `deps`: EPaxos\*'s union at the coordinator and everyone it reaches, or
    /// SwiftPaxos' leader's proposal, which overrides whatever we had proposed ourselves.
    pub fn accept(&mut self, deps: DepSet) {
        debug_assert!(
            !self.is_committed(),
            "an agreed instance must not be reopened"
        );
        self.deps = deps;
        self.phase = Phase::Accepted;
    }

    /// Deliberately not folded into `accept`: SwiftPaxos' leader sends an `Accept` for *every*
    /// instance -- it doubles as its own `FastAck` -- so accepting says nothing about the route
    /// there. EPaxos only accepts on the slow path, and there it does.
    #[inline]
    pub fn mark_left_fast_path(&mut self) {
        self.left_fast_path = true;
    }

    #[inline]
    pub fn set_fast_path(&mut self, fast: bool) {
        self.left_fast_path = !fast;
    }

    #[inline]
    pub fn on_fast_path(&self) -> bool {
        !self.left_fast_path
    }

    /// Moves to `Committed` with the agreed dependencies. Agreement (EPaxos\* Invariant 1)
    /// means this can only ever be called with the same set at every process, which the
    /// debug assertion checks locally.
    pub fn commit(&mut self, deps: DepSet) {
        debug_assert!(
            !self.is_committed() || self.deps == deps,
            "a command must not commit with two different dependency sets"
        );
        self.deps = deps;
        self.phase = Phase::Committed;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const N: usize = 3;
    /// The instance's leader, whose pre-accept a fast commit agrees on.
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

    fn instance() -> Instance {
        let command = Command {
            requester: 0,
            shard: 0,
            command: vec![],
            read_only: false,
            arrival_slot: 0,
            report: None,
        };
        Instance::new(0, LEADER, command, DepSet::new(N), N)
    }

    /// The ordinary order: the leader's pre-accept first, then the answers to it.
    #[test]
    fn preaccepts_after_the_leaders_are_classified_on_arrival() {
        let mut instance = instance();
        instance.record_preaccept(LEADER, deps(&[uid(0, 0)]));
        assert!(instance.record_preaccept(1, deps(&[uid(0, 0)])));
        assert!(instance.record_preaccept(2, deps(&[uid(0, 0), uid(1, 0)])));
        assert_eq!(instance.preaccepts_count(), 3);
        assert!(!instance.all_preaccepts_endorse());
        assert_eq!(instance.union(), &deps(&[uid(0, 0), uid(1, 0)]));
    }

    /// In SwiftPaxos a pre-accept is broadcast by whichever replica formed it, so it can
    /// reach us over a shorter link than the leader's `Accept`. Classifying has to wait
    /// for the leader's, and must then happen retroactively.
    #[test]
    fn preaccepts_before_the_leaders_are_classified_retroactively() {
        let mut instance = instance();
        assert!(instance.record_preaccept(1, deps(&[uid(0, 0)])));
        assert!(instance.record_preaccept(2, deps(&[uid(0, 0), uid(1, 0)])));
        // Nothing can endorse a set we have not seen yet.
        assert!(instance.leader_deps().is_none());
        assert!(instance.fast_endorsers().is_none());

        instance.record_preaccept(LEADER, deps(&[uid(0, 0)]));
        assert_eq!(instance.preaccepts_count(), 3);
        assert!(!instance.all_preaccepts_endorse());
        instance.accept(deps(&[uid(0, 0)]));
        // Replica 1 proposed the leader's set, replica 2 did not.
        let endorsers = instance.fast_endorsers().expect("the leader has spoken");
        assert!(endorsers.contains(LEADER));
        assert!(endorsers.contains(1));
        assert!(!endorsers.contains(2));
    }

    /// Unanimity is over the pre-accepts *received*, not over every process. `preaccepts`
    /// is indexed by pid, so its `len()` is the process count and must not be mistaken for
    /// the number of replies — doing so silently confines EPaxos to the slow path.
    #[test]
    fn a_partial_set_of_matching_preaccepts_is_unanimous() {
        let mut instance = instance();
        instance.record_preaccept(LEADER, deps(&[uid(0, 0)]));
        instance.record_preaccept(1, deps(&[uid(0, 0)]));
        assert_eq!(instance.preaccepts_count(), 2);
        assert!(instance.all_preaccepts_endorse());

        // And process 2 disagreeing is what ends it.
        instance.record_preaccept(2, deps(&[uid(0, 0), uid(2, 0)]));
        assert_eq!(instance.preaccepts_count(), 3);
        assert!(!instance.all_preaccepts_endorse());
    }

    /// Whichever order they arrive in, the state is the same.
    #[test]
    fn arrival_order_does_not_change_the_outcome() {
        let mut early = instance();
        early.record_preaccept(1, deps(&[uid(0, 0)]));
        early.record_preaccept(LEADER, deps(&[uid(0, 0)]));
        early.record_preaccept(2, deps(&[uid(0, 0)]));

        let mut late = instance();
        late.record_preaccept(LEADER, deps(&[uid(0, 0)]));
        late.record_preaccept(1, deps(&[uid(0, 0)]));
        late.record_preaccept(2, deps(&[uid(0, 0)]));

        for instance in [&early, &late] {
            assert_eq!(instance.preaccepts_count(), 3);
            assert!(instance.all_preaccepts_endorse());
            assert_eq!(instance.union(), &deps(&[uid(0, 0)]));
        }
    }

    /// A second pre-accept from the same process — a `PreAccept` overtaken by the leader's
    /// `Accept`, say — must not move the leader's: it is the set the fast path commits.
    #[test]
    fn the_leaders_preaccept_is_set_once() {
        let mut instance = instance();
        instance.record_preaccept(LEADER, deps(&[uid(0, 0)]));
        assert!(!instance.record_preaccept(LEADER, deps(&[uid(1, 0)])));
        assert_eq!(instance.leader_deps(), Some(&deps(&[uid(0, 0)])));
    }

    #[test]
    fn a_duplicate_preaccept_is_ignored() {
        let mut instance = instance();
        instance.record_preaccept(LEADER, deps(&[]));
        assert!(instance.record_preaccept(1, deps(&[])));
        assert!(!instance.record_preaccept(1, deps(&[uid(0, 0)])));
        assert_eq!(instance.preaccepts_count(), 2);
        assert_eq!(instance.union(), &deps(&[]));
    }
}
