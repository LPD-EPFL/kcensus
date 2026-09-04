//! Dependency-based ordering: the consensus value is a command's *dependency set*, and
//! there is one instance per command instead of one per slot.
//!
//! This is a second ordering layer, running beside the slot-based one rather than
//! replacing it: the two share the shard pool, the sinks, the command types and the eval
//! logging, but nothing of their instance state or their commit-to-execution path. See
//! `docs/dependency-ordering-plan.md`.

use crate::consensus::command::Command;
use crate::consensus::deps::dep_set::{requester_of, DepSet};
use crate::consensus::deps::execution::{cycle_possible, executable_order, next_executable};
use crate::consensus::deps::instance::{Instance, Phase};
use crate::consensus::deps::message::DepMsg;
use crate::consensus::message::{CommandBatch, ConsensusMessage, ConsensusMsg};
use crate::consensus::shard_pool::{PooledShard, ShardPool};
use crate::eval;
use crate::message::Message::{ConsensusM, Done};
use crate::message::MsgWithSource;
use crate::multi_sink::{MultiSink, ShardMultiSink};
use crate::topology::Topology;
use bit_set::BitSet;
use log::{debug, trace};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::{pin, select};
use tokio_timerfd::Delay;

pub(crate) mod dep_set;
pub(crate) mod execution;
pub(crate) mod instance;
pub(crate) mod message;

/// Which protocol drives agreement on the dependency set.
#[derive(Clone, Debug)]
pub enum DepMode {
    /// EPaxos\*, non-thrifty: the fast quorum is any `n − e` responders.
    ///
    /// `coordinator` is the replica this process hands its commands to — itself when it is
    /// one, and otherwise the replica `epaxos_committers` picked, i.e. the one whose round
    /// trip plus fast quorum is shortest from here. EPaxos has no leader for a non-replica
    /// to address, and running a quorum requires being counted in it, so a non-voting
    /// process cannot coordinate its own commands.
    EPaxos { coordinator: usize },
    /// SwiftPaxos: every fast quorum contains `leader`, which is what gives the leader the
    /// last word and keeps the committed graph acyclic. `quorum` is the paper's C2 — a
    /// single fixed fast quorum — or `None` for C1, any set of more than 3/4 of replicas.
    SwiftPaxos {
        leader: usize,
        quorum: Arc<Option<BitSet>>,
    },
}

/// The proposers for which the leader's `Accept` should carry the command.
///
/// The reference implementation never puts one in a replica-to-replica message: a replica
/// that has not received the client's `Propagate` simply waits for it (`afterPropagate`,
/// `swift.go:418`), and the paper recovers a genuinely missing payload by asking the leader
/// to retransmit. Carrying it costs bytes on every link, and buys nothing unless it beats
/// the `PreAccept` — which needs the network to violate the triangle inequality:
/// `proposer → leader → replica` shorter than `proposer → replica`. Attach it only for the
/// proposers where some replica actually gains, which is what `force_mpaxos` is about in
/// the slot layer.
fn spread_value_for(topology: &Topology, leader: usize) -> BitSet {
    let mut proposers = BitSet::with_capacity(topology.nb_processes);
    for proposer in 0..topology.nb_processes {
        let via_leader = topology.link_latency(proposer, leader);
        if topology.alive_replicas.iter().any(|replica| {
            replica != leader
                && replica != proposer
                && via_leader + topology.link_latency(leader, replica)
                    < topology.link_latency(proposer, replica)
        }) {
            proposers.insert(proposer);
        }
    }
    proposers
}

/// The compressed state of an idle dependency-mode shard.
///
/// With no epochs, the dependency watermark has to survive sleep — it is what a freshly
/// submitted command depends on. Once every instance is executed, "seen" and "executed"
/// coincide, so one vector is enough. This is `O(n)` per logical shard rather than the
/// `O(1)` of the slot layer; see §5 and §12 of the plan.
#[derive(Clone, Debug)]
pub(crate) struct SleepingDepShard {
    next_uid: usize,
    watermark: DepSet,
}

impl SleepingDepShard {
    fn new(my_pid: usize, process_count: usize) -> Self {
        Self {
            next_uid: 2 * my_pid,
            watermark: DepSet::new(process_count),
        }
    }
}

pub(crate) struct DepShard {
    // Settings
    process_count: usize,
    my_pid: usize,
    alive_replicas: BitSet,
    /// `n − e`: unanimity over this many proposals commits on the fast path.
    fast_quorum: usize,
    /// `n − f`: enough to take the slow path, and to commit it.
    slow_quorum: usize,
    mode: DepMode,
    /// SwiftPaxos: the proposers whose command the leader's `Accept` should carry. See
    /// `spread_value_for`.
    spread_value: BitSet,

    sinks: ShardMultiSink,

    // Overall state
    next_uid: usize,
    /// Every command of this shard we have heard of. A new command depends on all of it.
    seen: DepSet,
    /// Every command of this shard we have executed. A dependency at or below this needs
    /// no waiting, which is what keeps the execution graph bounded by unfinished work.
    executed: DepSet,
    /// One entry per unfinished command whose payload we hold. Executed instances are
    /// removed, so an empty table is (with `deferred`) the condition for the shard to sleep.
    instances: HashMap<usize, Instance>,
    /// Messages held back, keyed by what they are waiting for and in arrival order.
    /// A message waits either for the command it is about — nothing can be acted on before
    /// the payload — or for a dependency the leader named to be accepted here. See
    /// `handle` and `blocked_by`.
    deferred: HashMap<usize, Vec<(usize, DepMsg)>>,
    /// The instances whose leader's `Accept` is one of the messages held in `deferred`,
    /// waiting for either reason. Empty, which is the normal state, is what lets
    /// `blocked_by` answer without looking at anything: no `Accept` waiting means every
    /// dependency the leader can name is already accepted here.
    parked_accepts: HashSet<usize>,
    /// Commands executed since the last drain, in execution order.
    ready: Vec<Command>,
}

impl DepShard {
    pub fn new(topology: &Topology, my_pid: usize, sinks: ShardMultiSink, mode: DepMode) -> Self {
        let process_count = topology.nb_processes;
        let replica_count = topology.nb_replicas;
        assert!(my_pid < process_count);
        let slow_quorum = 1 + (replica_count / 2);
        let fast_quorum = match &mode {
            // EPaxos: n − e.
            DepMode::EPaxos { .. } => ((replica_count * 3 - 1) / 4).max(slow_quorum),
            // SwiftPaxos C2: the fixed quorum is a majority including the leader.
            DepMode::SwiftPaxos { quorum, .. } if quorum.is_some() => slow_quorum,
            // SwiftPaxos C1: more than 3/4 of the replicas, including the leader.
            DepMode::SwiftPaxos { .. } => ((replica_count * 3 - 1) / 4 + 1).max(slow_quorum),
        };
        Self {
            process_count,
            my_pid,
            alive_replicas: topology.alive_replicas.clone(),
            fast_quorum,
            slow_quorum,
            spread_value: match &mode {
                DepMode::EPaxos { .. } => BitSet::with_capacity(process_count),
                DepMode::SwiftPaxos { leader, .. } => spread_value_for(topology, *leader),
            },
            mode,
            sinks,
            next_uid: 2 * my_pid,
            seen: DepSet::new(process_count),
            executed: DepSet::new(process_count),
            instances: HashMap::new(),
            deferred: HashMap::new(),
            parked_accepts: HashSet::new(),
            ready: Vec::new(),
        }
    }

    /// The instance's leader: the one process that sends `Accept` for it, and so the one
    /// whose pre-accept a fast commit agrees on.
    ///
    /// SwiftPaxos has a designated one, shared by every instance. EPaxos\* has none, but
    /// the coordinator plays the part — it is what drives the ballot-0 slow path, and its
    /// `PreAccept` is the `dep_init` a fast commit agrees on (Fig. 3 lines 15 and 22).
    #[inline]
    fn leader(&self, coordinator_pid: usize) -> usize {
        match self.mode {
            DepMode::EPaxos { .. } => coordinator_pid,
            DepMode::SwiftPaxos { leader, .. } => leader,
        }
    }

    /// Whether `pid` counts towards quorums.
    #[inline]
    fn is_replica(&self, pid: usize) -> bool {
        self.alive_replicas.contains(pid)
    }

    /// The proposer of `id` when it is not a replica, as a priority destination.
    ///
    /// A non-voting proposer plays the SwiftPaxos client's part: it decides the fast route
    /// for itself, so the votes and the leader's proposal have to reach it even though the
    /// replica-only filter would otherwise drop them. `None` — the common case — leaves the
    /// broadcast untouched.
    #[inline]
    fn requester_of(&self, id: usize) -> Option<usize> {
        Some(requester_of(id, self.process_count))
    }

    #[inline]
    fn next_uid(&mut self) -> usize {
        let uid = self.next_uid;
        self.next_uid += 2 * self.process_count;
        uid
    }

    /// How many proposals we can hope to collect.
    #[inline]
    fn reachable(&self) -> usize {
        self.alive_replicas.len()
    }

    #[inline]
    async fn broadcast(&self, msg: DepMsg, value: Option<CommandBatch>) -> io::Result<()> {
        self.priority_broadcast(msg, value, None).await
    }

    #[inline]
    async fn priority_broadcast(
        &self,
        msg: DepMsg,
        value: Option<CommandBatch>,
        priority: Option<usize>,
    ) -> io::Result<()> {
        self.sinks
            .priority_broadcast(ConsensusMsg::DepM(msg), value, None, priority)
            .await
    }

    #[inline]
    async fn send(&self, msg: DepMsg, dest: usize) -> io::Result<()> {
        self.send_with_value(msg, None, dest).await
    }

    #[inline]
    async fn send_with_value(
        &self,
        msg: DepMsg,
        value: Option<CommandBatch>,
        dest: usize,
    ) -> io::Result<()> {
        if dest == self.my_pid {
            return Ok(());
        }
        self.sinks
            .send(ConsensusMsg::DepM(msg), value, dest, None)
            .await
    }

    /// Submits a locally issued command: allocate an id, propose every command we have
    /// seen so far as its dependencies, and pre-accept it ourselves.
    ///
    /// Unlike the slot layer there is no contention path — a new command never waits for
    /// another to finish, it just starts its own instance. That parallelism is the whole
    /// point of dependency ordering.
    pub async fn submit(&mut self, command: Command) -> io::Result<()> {
        if let DepMode::EPaxos { coordinator } = self.mode
            && coordinator != self.my_pid
        {
            // Not a replica: hand the command over
            return self
                .send_with_value(
                    DepMsg::Forward,
                    Some(CommandBatch::Single(command)),
                    coordinator,
                )
                .await;
        }
        let id = self.next_uid();
        // Dependencies are computed before the command is added to `seen`, so a command
        // never depends on itself.
        let init_deps = self.seen.clone();
        self.seen.insert(id);

        let value = CommandBatch::Single(command.clone());
        let mut instance = Instance::new(
            self.my_pid,
            self.leader(self.my_pid),
            command,
            init_deps.clone(),
            self.process_count,
        );
        // "Self-addressed messages are delivered immediately": we are already one of the
        // proposals, and our own trivially equals the coordinator's. A non-voting proposer
        // is the exception — it proposes a value but casts no vote for it, so it needs one
        // more replica than a proposer that is in the fast quorum itself.
        if self.in_fast_quorum(self.my_pid) {
            instance.record_preaccept(self.my_pid, init_deps.clone());
        }
        self.instances.insert(id, instance);

        debug!(
            "Submitting id={id} (shard={}) with deps={:?}",
            self.sinks.shard_id, init_deps
        );
        self.broadcast(
            DepMsg::PreAccept {
                id,
                deps: init_deps.clone(),
            },
            Some(value),
        )
        .await?;

        if let DepMode::SwiftPaxos { leader, .. } = self.mode
            && self.my_pid == leader
        {
            self.swift_leader_accept(id, init_deps).await?;
        }

        self.try_commit(id).await
    }

    /// The instance `id`, which by then always exists: `handle` creates it as soon as the
    /// payload arrives and holds every earlier message back until it does.
    fn instance_mut(&mut self, id: usize) -> &mut Instance {
        self.instances
            .get_mut(&id)
            .expect("an instance exists once its payload has arrived")
    }

    /// Records that `id` exists, now that we hold its command.
    ///
    /// The coordinator is derived from the uid rather than from whoever sent the message:
    /// a `Proposal` or a `Commit` about a command says nothing about who submitted it.
    fn create_instance(&mut self, id: usize, command: Command) {
        debug_assert!(
            !self.executed.contains(id),
            "an executed instance must never be recreated"
        );
        // Only now does the command count as one of ours: until the payload is here it
        // conflicts with nothing, which is `cmd[id] = ⊥` in both papers.
        self.seen.insert(id);
        let coordinator_pid = requester_of(id, self.process_count);
        self.instances.insert(
            id,
            Instance::new(
                coordinator_pid,
                self.leader(coordinator_pid),
                command,
                DepSet::new(self.process_count),
                self.process_count,
            ),
        );
    }

    /// Delivers one message, holding it back until the command it is about is known.
    ///
    /// Both papers gate every handler on that: EPaxos\* stores `cmd[id]` in the
    /// `PreAccept` handler and every later precondition names a phase that only that
    /// handler can set, and SwiftPaxos Fig. 4 line 23 requires `id ∈ Preaccept` before the
    /// leader's ack may be processed — which the reference implementation enforces by
    /// deferring it behind `afterPropagate` (`swift.go:418`). It is also the only sound
    /// reading: our answer to a command is "everything I hold that conflicts with it", and
    /// there is nothing to compare against until we hold it.
    ///
    /// Only SwiftPaxos ever waits here. Every message about an EPaxos instance either comes
    /// from its coordinator — behind the `PreAccept` on a FIFO link — or goes to it, so the
    /// payload is always already in hand. In SwiftPaxos a `Proposal` is broadcast by
    /// whichever replica formed it and can arrive over a shorter link than the proposer's
    /// `PreAccept`; the leader's `Accept` carries the payload precisely so that it never
    /// has to wait here itself.
    pub async fn handle(
        &mut self,
        src: usize,
        msg: DepMsg,
        value: Option<CommandBatch>,
    ) -> io::Result<()> {
        trace!(
            "Processing dep msg from {src} (shard={}): {msg:?}",
            self.sinks.shard_id
        );
        let Some(id) = msg.id() else {
            // A forwarded request: it *is* the command, and has no instance yet.
            let Some(CommandBatch::Single(command)) = value else {
                panic!("a forwarded request must carry its command");
            };
            debug_assert!(
                self.is_replica(self.my_pid),
                "only a replica is ever forwarded to"
            );
            return self.submit(command).await;
        };
        // Nothing about an already-executed command can change anything.
        if self.executed.contains(id) {
            return Ok(());
        }
        if !self.instances.contains_key(&id) {
            let Some(CommandBatch::Single(command)) = value else {
                self.defer(id, src, msg);
                return Ok(());
            };
            self.create_instance(id, command);
        }
        self.deliver(src, msg).await
    }

    /// Delivers one message and then whatever its delivery released, in arrival order.
    ///
    /// A message can be held twice — once for the command it is about, once for a
    /// dependency it names (`blocked_by`) — and releasing one can hand back messages that
    /// have to wait again, so this is a queue rather than the single pass it used to be.
    /// It terminates because a message only ever waits on something strictly ahead of it
    /// in the leader's order, which no message of ours can hold up.
    async fn deliver(&mut self, src: usize, msg: DepMsg) -> io::Result<()> {
        let mut queue = VecDeque::from([(src, msg)]);
        while let Some((src, msg)) = queue.pop_front() {
            let id = msg.id().expect("a held message is about an instance");
            // Executed while it waited: nothing it has to say can matter any more, and its
            // instance is gone.
            if self.executed.contains(id) {
                continue;
            }
            if let Some(dep) = self.blocked_by(&msg) {
                self.defer(dep, src, msg);
                continue;
            }
            self.dispatch(src, msg).await?;
            queue.extend(self.take_deferred(id));
        }
        Ok(())
    }

    /// Holds `msg` back until `key` — the command it is about, or a dependency of it — has
    /// moved on.
    fn defer(&mut self, key: usize, src: usize, msg: DepMsg) {
        if let DepMsg::Accept { id, .. } = msg {
            self.parked_accepts.insert(id);
        }
        self.deferred.entry(key).or_default().push((src, msg));
    }

    /// Everything that was waiting on `key`, in arrival order.
    fn take_deferred(&mut self, key: usize) -> Vec<(usize, DepMsg)> {
        let held = self.deferred.remove(&key).unwrap_or_default();
        for (_, msg) in &held {
            if let DepMsg::Accept { id, .. } = msg {
                self.parked_accepts.remove(id);
            }
        }
        held
    }

    /// What a message has to wait for before it can be delivered, if anything: SwiftPaxos
    /// Fig. 4 line 23's `D ⊆ Accept ∪ Commit` as a precondition.
    ///
    /// The leader accepts in its own arrival order and FIFO delivers its `Accept`s in that
    /// order, so by the time it names `D` we have received its `Accept` for every member of
    /// it. *Received*, not necessarily processed: an `Accept` that travels without the
    /// command (`spread_value`) waits for the proposer's `PreAccept`, and one waiting there
    /// holds up the leader's later `Accept`s in turn, so a dependency can be un-accepted
    /// for either reason. Both leave an `Accept` of ours parked, which is why
    /// `parked_accepts` settles it: nothing parked means every dependency is accepted, and
    /// the answer is no without a single lookup.
    ///
    /// Waiting here does reorder the leader's own link: a `Commit` that follows a parked
    /// `Accept` overtakes it. Nothing depends on that order — `on_accept` on a settled
    /// instance is already a no-op, since the network reorders across sources anyway — and
    /// the reference implementation reorders in exactly the same way, running a message
    /// whose condition holds while an earlier one of the same instance sits in
    /// `afterPropagate` (`hook/cond.go`).
    ///
    /// Replicas only, and the leader never sees its own `Accept`. A non-voting process is
    /// served the leader's `Accept` for its own commands and nothing else, so its view is
    /// deliberately partial: waiting for the rest would be waiting forever.
    fn blocked_by(&self, msg: &DepMsg) -> Option<usize> {
        let DepMode::SwiftPaxos { .. } = self.mode else {
            return None;
        };
        // Nothing is waiting: by FIFO every dependency the leader can name is accepted.
        if self.parked_accepts.is_empty() {
            return None;
        }
        if !self.is_replica(self.my_pid) {
            return None;
        }
        // Only the leader ever sends `Accept` in SwiftPaxos, so being one is enough.
        let DepMsg::Accept { deps, id, .. } = msg else {
            return None;
        };
        let id = *id;
        let blocking = deps
            .pending_over(&self.executed)
            .find(|dep| *dep != id && !self.instances.get(dep).is_some_and(Instance::is_settled))?;
        debug_assert!(
            self.parked_accepts.contains(&blocking),
            "the leader's order reaches us over a FIFO link, so a dependency we have not \
             accepted must be one whose `Accept` is already waiting here"
        );
        Some(blocking)
    }

    async fn dispatch(&mut self, src: usize, msg: DepMsg) -> io::Result<()> {
        match msg {
            DepMsg::Forward => unreachable!("a forwarded request has no instance"),
            DepMsg::PreAccept { id, deps } => self.on_pre_accept(src, id, deps).await,
            DepMsg::PreAcceptOk { id, deps } => self.on_pre_accept_ok(src, id, deps).await,
            DepMsg::Accept { id, deps, .. } => self.on_accept(src, id, deps).await,
            DepMsg::AcceptOk { id } => self.on_accept_ok(src, id).await,
            DepMsg::Commit { id, deps } => {
                self.on_commit(id, deps);
                Ok(())
            }
        }
    }

    /// The coordinator's proposal reached us. Our own proposal completes it with every
    /// conflicting command we know of, which is what makes visibility hold: any command
    /// we have already seen ends up in one of the two dependency sets.
    async fn on_pre_accept(&mut self, src: usize, id: usize, deps: DepSet) -> io::Result<()> {
        let coordinator_deps = deps;
        // Our own proposal. EPaxos\* Fig. 3 line 16 completes the coordinator's set with
        // everything else we have seen; SwiftPaxos Fig. 4 line 11 is purely local, because
        // there the client's `Propagate` carries no dependencies at all. Unioning the
        // coordinator's view in would let the leader name a command it has not accepted
        // itself, which line 23 forbids and which can close a dependency cycle.
        let mut my_deps = match self.mode {
            DepMode::EPaxos { .. } => {
                let mut deps = coordinator_deps.clone();
                deps.union_with(&self.seen);
                deps
            }
            DepMode::SwiftPaxos { .. } => self.seen.clone(),
        };
        // Another replica's ack may have told us this command exists before its own
        // PreAccept arrived, which would otherwise make us propose it as its own
        // dependency and disagree with the coordinator for no reason.
        my_deps.without_uid(id);

        let my_pid = self.my_pid;
        let votes = self.in_fast_quorum(my_pid);
        let src_votes = self.in_fast_quorum(src);
        let instance = self.instance_mut(id);
        // The coordinator's `PreAccept` doubles as its own pre-accept: it is a replica's
        // local proposal like any other, and both protocols keep everyone's. In EPaxos it
        // is also the leader's, and so the set a fast commit would agree on; in SwiftPaxos
        // that one waits for the leader's `Accept`.
        if src_votes {
            instance.record_preaccept(src, coordinator_deps);
        }
        if instance.is_settled() {
            // We have already backed a value — the leader's accept overtook this message,
            // or the decision did. It only had to deliver the payload.
            return self.try_commit(id).await;
        }
        instance.deps = my_deps.clone();

        if votes {
            instance.record_preaccept(my_pid, my_deps.clone());

            match self.mode {
                // EPaxos: only the coordinator decides, so the answer goes to it alone.
                DepMode::EPaxos { .. } => {
                    self.send(DepMsg::PreAcceptOk { id, deps: my_deps }, src)
                        .await?
                }
                // We are the leader: our proposal is the one everyone else adopts, and its
                // `Accept` doubles as the answer a `Proposal` would have carried.
                DepMode::SwiftPaxos { leader, .. } if my_pid == leader => {
                    self.swift_leader_accept(id, my_deps).await?
                }
                // Broadcast, as in the reference implementation: every replica evaluates the
                // fast route for itself and commits at two message delays, instead of waiting
                // out the leader's Commit at four. A non-voting proposer decides too, so it is
                // served first.
                DepMode::SwiftPaxos { .. } => {
                    self.priority_broadcast(
                        DepMsg::PreAcceptOk { id, deps: my_deps },
                        None,
                        // Send in priority to the requester (can be a non-voting proposer)
                        self.requester_of(id),
                    )
                    .await?
                }
            }
        }

        self.try_commit(id).await
    }

    /// Both protocols' decision rule, run after anything that could complete a quorum —
    /// either of them, in either phase. EPaxos only ever decides at the coordinator;
    /// SwiftPaxos decides everywhere.
    async fn try_commit(&mut self, id: usize) -> io::Result<()> {
        match self.mode {
            DepMode::EPaxos { .. } => self.epaxos_try_commit(id).await,
            DepMode::SwiftPaxos { .. } => self.swift_try_commit(id).await,
        }
    }

    /// A process's own proposal — EPaxos' `PreAcceptOk`, SwiftPaxos' `FastAck`. The same
    /// state either way; the two protocols differ only in who receives it.
    async fn on_pre_accept_ok(&mut self, src: usize, id: usize, deps: DepSet) -> io::Result<()> {
        let instance = self.instance_mut(id);
        if instance.is_committed() || !instance.record_preaccept(src, deps) {
            return Ok(());
        }
        self.try_commit(id).await
    }

    /// The coordinator's decision point (EPaxos\* Fig. 3, lines 19-25). Nobody else has
    /// the answers — `PreAcceptOk` is unicast in this mode — so it runs only there.
    async fn epaxos_try_commit(&mut self, id: usize) -> io::Result<()> {
        let fast_quorum = self.fast_quorum;
        let slow_quorum = self.slow_quorum;
        // With fewer replicas reachable than a fast quorum we can never be unanimous over
        // one, so the slow path has to trigger on what we can actually collect.
        let fast_target = fast_quorum.min(self.reachable());
        let my_pid = self.my_pid;

        let instance = self
            .instances
            .get_mut(&id)
            .expect("decided instance exists");
        if instance.coordinator_pid != my_pid {
            return Ok(());
        }
        if instance.is_settled() {
            // Past the pre-accept phase: the only quorum still open is the accept one.
            return self.epaxos_try_commit_accepted(id).await;
        }
        let unanimous = instance.all_preaccepts_endorse();
        let preaccepts = instance.preaccepts_count();

        if unanimous && preaccepts >= fast_quorum {
            let deps = instance.leader_deps().expect("we proposed it").clone();
            debug!("Fast-commit id={id} (shard={})", self.sinks.shard_id);
            self.commit_and_broadcast(id, deps).await
        } else if preaccepts >= slow_quorum && (!unanimous || preaccepts >= fast_target) {
            // Slow path: accept the union of what the quorum reported. There is no
            // arbitration to do — the union is the answer.
            let deps = instance.union().clone();
            instance.accept(deps.clone());
            instance.accept_acked.insert(my_pid);
            debug!("Slow path for id={id} (shard={})", self.sinks.shard_id);
            self.broadcast(
                DepMsg::Accept {
                    id,
                    deps,
                    with_value: false,
                },
                None,
            )
            .await?;
            self.epaxos_try_commit_accepted(id).await
        } else {
            Ok(())
        }
    }

    /// Someone's `Accept`: EPaxos' coordinator announcing the union it will commit, or
    /// SwiftPaxos' leader announcing the value that overrides every proposal.
    ///
    /// It takes priority over anything we had proposed — that is SwiftPaxos' double
    /// voting, and it is safe because the leader belongs to every fast quorum, so a replica
    /// that disagreed with it thereby knows the command cannot have committed on the fast
    /// path. Only the reverse is forbidden: once we have accepted, a `PreAccept` arriving
    /// later does not take us back to a proposal of our own.
    async fn on_accept(&mut self, src: usize, id: usize, deps: DepSet) -> io::Result<()> {
        let my_pid = self.my_pid;
        let swift = matches!(self.mode, DepMode::SwiftPaxos { .. });
        debug_assert!(
            !swift || !self.is_replica(my_pid) || self.deps_are_settled(id, &deps),
            "SwiftPaxos Fig. 4 line 23: the leader's dependencies must already be accepted"
        );
        let instance = self.instance_mut(id);
        if instance.is_settled() {
            return self.try_commit(id).await;
        }
        instance.accept(deps.clone());
        // The sender has obviously accepted its own value. In SwiftPaxos it is also the
        // leader's *own proposal* — the paper sends it as a `FastAck` — so it counts on the
        // fast route too, and it is the set a fast commit agrees on. EPaxos' union is not
        // anyone's proposal and is not what its fast path would have committed, so neither
        // applies there.
        instance.accept_acked.insert(src);
        if swift {
            instance.record_preaccept(src, deps);
        }
        let proposer = instance.coordinator_pid;
        if !self.is_replica(my_pid) {
            // A non-voting process adopts the value — it executes the command — but casts
            // no vote, so it neither counts itself nor acknowledges. It decides from the
            // replicas' answers, or from the `Commit`.
            return self.try_commit(id).await;
        }
        self.instances
            .get_mut(&id)
            .expect("created above")
            .accept_acked
            .insert(my_pid);
        // Back to the sender, which is the coordinator in EPaxos and the leader in
        // SwiftPaxos. There the proposer needs it as well, to finish
        // `proposer -> leader -> majority -> proposer`, while the leader's own majority
        // drives the `Commit` that tells replicas busy with a conflicting command that this
        // one is decided — without it they could wait forever on a dependency ordered
        // before their own. In EPaxos the two are the same process and the second send is
        // skipped.
        self.send(DepMsg::AcceptOk { id }, src).await?;
        if proposer != src {
            self.send(DepMsg::AcceptOk { id }, proposer).await?;
        }
        self.try_commit(id).await
    }

    async fn on_accept_ok(&mut self, src: usize, id: usize) -> io::Result<()> {
        let instance = self.instance_mut(id);
        if instance.is_committed() || !instance.accept_acked.insert(src) {
            return Ok(());
        }
        self.try_commit(id).await
    }

    /// EPaxos' slow-path commit: a majority has accepted the union, at the coordinator.
    async fn epaxos_try_commit_accepted(&mut self, id: usize) -> io::Result<()> {
        let slow_quorum = self.slow_quorum;
        let my_pid = self.my_pid;
        let instance = self
            .instances
            .get_mut(&id)
            .expect("accepted instance exists");
        if instance.coordinator_pid != my_pid
            || instance.phase != Phase::Accepted
            || instance.accept_acked.len() < slow_quorum
        {
            return Ok(());
        }
        let deps = instance.deps.clone();
        self.commit_and_broadcast(id, deps).await
    }

    async fn commit_and_broadcast(&mut self, id: usize, deps: DepSet) -> io::Result<()> {
        self.on_commit(id, deps.clone());
        self.broadcast(DepMsg::Commit { id, deps }, None).await
    }

    fn on_commit(&mut self, id: usize, deps: DepSet) {
        let instance = self.instance_mut(id);
        if instance.is_committed() {
            return;
        }
        instance.commit(deps);
        self.execute_ready();
    }

    /// Moves every command whose dependencies are settled into `ready`, in the order
    /// every process computes identically.
    ///
    /// The graph is the fallback, not the normal path. Dependency sets are watermarks, so
    /// they are downward closed and the graph over what is pending is dense — in
    /// SwiftPaxos it is the leader's whole arrival order, `Θ(P²)` edges to rediscover an
    /// order that was already fixed. [`next_executable`] walks that same order in `O(n)`
    /// per command without allocating, and only a real cycle needs Tarjan.
    fn execute_ready(&mut self) {
        while let Some(uid) = next_executable(&self.instances, &self.executed) {
            self.execute(uid);
        }
        if cycle_possible(&self.instances, &self.executed) {
            for uid in executable_order(&self.instances, &self.executed) {
                self.execute(uid);
            }
            // `executable_order` returns the largest set closed under "depends only on
            // what is in the set or already executed", so what it leaves behind is still
            // blocked by something it did not execute: there is never a second round.
            debug_assert!(next_executable(&self.instances, &self.executed).is_none());
        }
    }

    /// Hands one committed command over to the application and advances the watermark.
    fn execute(&mut self, uid: usize) {
        let command = self
            .instances
            .remove(&uid)
            .expect("ordered instance exists")
            .command;
        self.executed.insert(uid);
        self.ready.push(command);
    }

    /// Commands executed since the last call, to be handed to the application.
    #[inline]
    pub fn drain_ready(&mut self) -> Vec<Command> {
        std::mem::take(&mut self.ready)
    }

    // ---------------------------------------------------------------- SwiftPaxos

    /// Whether `pid` proposes in the fast path. With C1 every replica does; with C2 only
    /// the members of the one fixed quorum do.
    fn in_fast_quorum(&self, pid: usize) -> bool {
        self.is_replica(pid)
            && match &self.mode {
                DepMode::EPaxos { .. } => true,
                DepMode::SwiftPaxos { quorum, .. } => {
                    quorum.as_ref().as_ref().is_none_or(|q| q.contains(pid))
                }
            }
    }

    /// The leader broadcasts its own proposal as an `Accept`, carrying the command so that
    /// a replica closer to the leader than to the proposer can act on it straight away.
    async fn swift_leader_accept(&mut self, id: usize, my_deps: DepSet) -> io::Result<()> {
        let my_pid = self.my_pid;
        let instance = self.instances.get_mut(&id).expect("instance exists");
        // The leader accepts its own value, and that accept doubles as its own proposal and
        // its own acknowledgement — the paper sends it as a `FastAck` — so it counts on
        // both routes without a separate message.
        instance.record_preaccept(my_pid, my_deps.clone());
        instance.accept(my_deps.clone());
        instance.accept_acked.insert(my_pid);
        let spread = self.spread_value.contains(instance.coordinator_pid);
        let value = spread.then(|| CommandBatch::Single(instance.command.clone()));
        self.priority_broadcast(
            DepMsg::Accept {
                id,
                deps: my_deps,
                with_value: value.is_some(),
            },
            value,
            // Send in priority to the requester (can be a non-voting proposer)
            self.requester_of(id),
        )
        .await
    }

    /// Commit as soon as *either* route completes, which is what makes the latency the
    /// minimum of the two:
    ///
    /// 1. **fast quorum** — a whole fast quorum backing the proposer's own proposal, which
    ///    requires the leader to have agreed with it (the leader is in every fast quorum,
    ///    so this is also why the two routes can never commit different values). Every
    ///    replica evaluates this one, because `FastAck`s are broadcast;
    /// 2. **MultiPaxos through the leader** — a majority having acknowledged the leader's
    ///    accept, i.e. `proposer → leader → majority → proposer`. Only the proposer and the
    ///    leader see those acknowledgements; everyone else learns the outcome from the
    ///    leader's `Commit`.
    ///
    /// Only acknowledgements of the leader's accept count towards (2): a replica's own
    /// proposal is a fast-path vote and belongs to (1).
    async fn swift_try_commit(&mut self, id: usize) -> io::Result<()> {
        let slow_quorum = self.slow_quorum;
        let my_pid = self.my_pid;
        let Some(instance) = self.instances.get(&id) else {
            return Ok(());
        };
        if instance.is_committed() {
            return Ok(());
        }
        let leader = self.leader(instance.coordinator_pid);
        // Whatever we accepted is the leader's proposal: it is the only value anyone
        // accepts in this mode, and it is the one both routes commit.
        let Some(leader_deps) = instance.accepted_deps().cloned() else {
            return Ok(());
        };
        let am_proposer = instance.coordinator_pid == my_pid;
        let fast = instance
            .fast_endorsers()
            .is_some_and(|endorsers| self.swift_fast_quorum_reached(&endorsers));
        // A majority having accepted the leader's value is enough for both the proposer
        // (3 delays, answers came straight to it) and the leader (4 delays, but its Commit
        // can still be the shorter route when the requester sits close to it — networks do
        // not have to obey the triangle inequality).
        let via_majority =
            (am_proposer || my_pid == leader) && instance.accept_acked.len() >= slow_quorum;
        if !fast && !via_majority {
            return Ok(());
        }
        self.on_commit(id, leader_deps.clone());
        // Only the leader announces the decision. It always gets there — every replica
        // acknowledges its accept — so one announcement is enough, and it is what unblocks
        // replicas whose own commands are ordered after this one. Having the proposer
        // announce as well would only delay its own client's response behind a broadcast.
        if my_pid != leader {
            return Ok(());
        }
        self.broadcast(
            DepMsg::Commit {
                id,
                deps: leader_deps,
            },
            None,
        )
        .await
    }

    /// True if `endorsers` covers a fast quorum: the fixed one under the paper's C2, or
    /// any set of more than 3/4 of the replicas including the leader under C1.
    fn swift_fast_quorum_reached(&self, endorsers: &BitSet) -> bool {
        let DepMode::SwiftPaxos { leader, quorum } = &self.mode else {
            unreachable!("SwiftPaxos quorum check in another mode");
        };
        match quorum.as_ref() {
            Some(quorum) => quorum.iter().all(|member| endorsers.contains(member)),
            None => endorsers.contains(*leader) && endorsers.len() >= self.fast_quorum,
        }
    }

    /// SwiftPaxos Fig. 4 line 23's `D ⊆ Accept ∪ Commit`: every dependency the leader
    /// names is accepted here. `blocked_by` is what makes it true — this checks that it
    /// did, which is also how a broken FIFO or a leader naming a command out of its own
    /// order would show up.
    ///
    /// Replicas only. A non-voting process is served the leader's `Accept` for its own
    /// commands and nothing else, so its view is deliberately partial: it casts no vote,
    /// and it learns every other decision from the `Commit`.
    fn deps_are_settled(&self, id: usize, deps: &DepSet) -> bool {
        deps.pending_over(&self.executed)
            .all(|dep| dep == id || self.instances.get(&dep).is_some_and(Instance::is_settled))
    }

    /// True if a message about `id` cannot change anything at a sleeping shard, i.e. the
    /// command has already been executed here. Mirrors the slot layer's stale-message
    /// guard: waking a shard up only to drop the message wastes a pool slot.
    fn is_noop_when_asleep(state: &SleepingDepShard, id: usize) -> bool {
        state.watermark.contains(id)
    }
}

impl PooledShard for DepShard {
    type Sleeping = SleepingDepShard;

    fn wake_up(&mut self, shard_id: usize, state: SleepingDepShard) {
        debug_assert!(self.instances.is_empty());
        debug_assert!(self.ready.is_empty());
        debug_assert!(self.seen.is_empty());
        debug_assert!(self.executed.is_empty());

        self.sinks.shard_id = shard_id;
        self.next_uid = state.next_uid;
        self.seen = state.watermark.clone();
        self.executed = state.watermark;
    }

    fn fall_asleep(&mut self) -> SleepingDepShard {
        debug_assert!(self.can_sleep());
        debug_assert_eq!(
            self.seen, self.executed,
            "an idle shard has executed everything it has seen"
        );
        let watermark = std::mem::replace(&mut self.executed, DepSet::new(self.process_count));
        self.seen = DepSet::new(self.process_count);
        SleepingDepShard {
            next_uid: self.next_uid,
            watermark,
        }
    }

    /// Nothing unfinished is left: every instance has been executed and handed over, and
    /// nothing is waiting for a payload or for a dependency — sleeping would drop it.
    fn can_sleep(&self) -> bool {
        let idle = self.instances.is_empty() && self.deferred.is_empty() && self.ready.is_empty();
        debug_assert!(
            !self.deferred.is_empty() || self.parked_accepts.is_empty(),
            "an empty hold-back table holds no `Accept`s"
        );
        idle
    }
}

pub(crate) struct DepConsensus {
    process_count: usize,
    pool: ShardPool<DepShard>,
    sinks: Arc<Mutex<MultiSink>>,
}

impl DepConsensus {
    pub fn new(
        topology: &Topology,
        my_pid: usize,
        sinks: MultiSink,
        mode: DepMode,
        shard_count: usize,
        shard_pool_size: usize,
    ) -> Self {
        let process_count = topology.nb_processes;
        let sinks = Arc::new(Mutex::new(sinks));
        let shard_sinks = sinks.clone();
        let topology = topology.clone();
        let new_shard = Box::new(move || {
            let mode = mode.clone();
            DepShard::new(
                &topology,
                my_pid,
                ShardMultiSink {
                    // Placeholder: assigned when the shard is taken out of the pool.
                    shard_id: usize::MAX,
                    multi_sink: shard_sinks.clone(),
                },
                mode,
            )
        });
        Self {
            process_count,
            pool: ShardPool::new(
                shard_count,
                shard_pool_size,
                SleepingDepShard::new(my_pid, process_count),
                new_shard,
            ),
            sinks,
        }
    }

    pub async fn run(
        &mut self,
        mut msg_rx: Receiver<MsgWithSource>,
        mut new_client_commands_rx: Receiver<Command>,
        committed_commands_tx: Sender<Command>,
        deadlock_deadline: Duration,
    ) -> io::Result<()> {
        let mut count_done = 0usize;
        let mut done = false;

        let deadlock_deadline =
            Delay::new(Instant::now() + deadlock_deadline).expect("should init timer");
        pin!(deadlock_deadline);

        'main_loop: while count_done < self.process_count {
            let touched: Option<usize> = select! {
                res = &mut deadlock_deadline => {
                    res.expect("should wait until deadlock_deadline");
                    eprintln!("deadlock detected ! Checking all active shards...");
                    for (shard_id, shard) in self.pool.iter_active() {
                        if !shard.instances.is_empty() {
                            eprintln!(
                                "shard={shard_id} is stuck with {} unfinished instance(s), executed={:?}",
                                shard.instances.len(), shard.executed
                            );
                            for (id, instance) in shard.instances.iter() {
                                eprintln!("  id={id}: {instance:?}");
                            }
                        }
                    }
                    eprintln!("checked all active shards ({} asleep).", self.pool.shard_count() - self.pool.active_count());
                    panic!("deadlock detected, terminating.");
                },
                command = new_client_commands_rx.recv(), if !done => {
                    match command {
                        Some(command) => {
                            let shard_id = command.shard;
                            self.pool.wake(shard_id).submit(command).await?;
                            Some(shard_id)
                        }
                        None => {
                            done = true;
                            self.sinks.lock().await.broadcast(Done, None).await?;
                            count_done += 1;
                            None
                        }
                    }
                }
                opt_msg = msg_rx.recv() => {
                    let msg = opt_msg.unwrap();
                    match msg.msg {
                        ConsensusM { shard: shard_id, msg, value } => {
                            let ConsensusMessage { msg, src, .. } = msg;
                            let ConsensusMsg::DepM(msg) = msg else {
                                panic!("Unexpected message type in dependency mode: {msg:?}");
                            };
                            if !self.pool.is_active(shard_id)
                                && msg.id().is_some_and(|id| {
                                    DepShard::is_noop_when_asleep(
                                        self.pool.sleeping_state(shard_id),
                                        id,
                                    )
                                })
                            {
                                continue 'main_loop;
                            }
                            self.pool.wake(shard_id).handle(src, msg, value).await?;
                            Some(shard_id)
                        }
                        Done => {
                            count_done += 1;
                            None
                        }
                        _ => panic!("Unexpected message type"),
                    }
                },
            };

            let Some(shard_id) = touched else {
                continue 'main_loop;
            };

            for command in self.pool.active_shard(shard_id).drain_ready() {
                committed_commands_tx
                    .send(command)
                    .await
                    .expect("Sending committed value");
            }
            self.pool.try_sleep(shard_id);
        }

        self.pool.report();

        let sinks = self.sinks.lock().await;
        eval::log(
            "network-done",
            &format!(
                "Sent {} messages ({} bytes)",
                sinks.stats.msg_count, sinks.stats.byte_count
            ),
            &sinks.stats,
        );

        new_client_commands_rx.close();
        msg_rx.close();
        Ok(())
    }
}
