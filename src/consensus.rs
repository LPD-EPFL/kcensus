use crate::consensus::message::ConsensusMsg::{Commit, PaxosM, ReadRequest, ReadResponse};
use crate::consensus::message::{CommandBatch, ConsensusMessage};
use crate::consensus::paxos_family::message::PaxosMsg::{Accept, Prepare};
use crate::consensus::read_tracker::ReadTracker;
use crate::consensus::shard_pool::{PooledShard, ShardPool};
use crate::eval;
use crate::message::Message::{ConsensusM, Done};
use crate::message::MsgWithSource;
use crate::multi_sink::{MultiSink, ShardMultiSink};
use bit_set::BitSet;
use command::{Command, CommitReport};
use log::{info, trace, warn};
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::{pin, select};
use tokio_timerfd::Delay;

pub(crate) mod command;
pub(crate) mod deps;
pub mod kcensus;
pub(crate) mod message;
pub(crate) mod paxos_family;
mod read_tracker;
pub(crate) mod shard_pool;

pub use shard_pool::DEFAULT_SHARD_POOL_SIZE;

/// Stuck shards printed in full before the report is summarised:
/// Throughput saturation can cause thousands of false reports.
pub(crate) const DEADLOCK_REPORT_LIMIT: usize = 10;

/// Capacity a pooled shard's tables are shrunk back to when it falls asleep, so a shard
/// that served a hot key does not carry its tables into the cold keys that reuse it.
const IDLE_CAPACITY: usize = 3;

pub(crate) struct ConsensusShard<AlgoSettings, AlgoRoundState> {
    // Settings
    process_count: usize,
    alive_replicas: BitSet,
    replica: bool,
    leader_priority: Vec<usize>,
    my_pid: usize,

    // Connections
    sinks: ShardMultiSink,

    // Overall state
    next_uid: usize,
    slot: usize,
    queued_commands: HashMap<usize, CommandBatch>,
    last_v: Option<usize>,

    /// Whether the current slot has been seen to leave the fast route. Cleared when the slot
    /// advances, read into `CommitReport::fast`.
    slot_left_fast_path: bool,

    // Messages that can not be processed yet, and local commands that can not be proposed yet.
    queued_messages: VecDeque<ConsensusMessage>,
    my_queued_commands: VecDeque<Command>,

    read_tracker: ReadTracker,

    settings: AlgoSettings,
    round_state: AlgoRoundState,
}

/// The compressed state of a logical shard that currently owns no physical shard.
///
/// Everything else about an idle shard is either empty or rebuilt from the settings,
/// so these few fields are all that has to survive while the shard sleeps.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SleepingShard {
    next_uid: usize,
    slot: usize,
    last_v: Option<usize>,
    next_read_id: usize,
}

impl SleepingShard {
    #[inline]
    fn new(my_pid: usize) -> Self {
        Self {
            next_uid: 2 * my_pid,
            slot: 0,
            last_v: None,
            next_read_id: 0,
        }
    }
}

pub(crate) struct Consensus<AlgoSettings, AlgoRoundState>
where
    ConsensusShard<AlgoSettings, AlgoRoundState>: ConsensusShardTrait,
{
    process_count: usize,
    pool: ShardPool<ConsensusShard<AlgoSettings, AlgoRoundState>>,
    sinks: Arc<Mutex<MultiSink>>,
}

pub(crate) trait ConsensusShardTrait {
    async fn process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<CommandBatch>>;

    fn can_forward_proposals(&self) -> bool;

    async fn propose_start(&mut self, value: CommandBatch, contention: bool) -> io::Result<()>;

    async fn repropose_start(&mut self, v: usize) -> io::Result<()>;

    fn commit_slot(&mut self, v: usize, from_commit_msg: bool) -> CommandBatch;

    /// Whether this algorithm has a fast route at all. Leader-based modes report
    /// `CommitReport::fast` false outright: Pando does not put an `Accept` in front of every
    /// replica, so not seeing one proves nothing there.
    fn has_fast_path(&self) -> bool;

    /// True if the algorithm state holds nothing about an unfinished round, i.e. it is
    /// back to what it was at the start of the slot. Asserted, in debug builds only,
    /// before a shard sleeps and after it wakes up.
    fn round_state_is_clear(&self) -> bool;

    fn get_my_v(&self) -> Option<usize>;

    fn ongoing(&self) -> bool;

    fn should_lead(&self) -> bool;

    fn can_propose(&self) -> bool;
}

impl<AS, ARS> Consensus<AS, ARS>
where
    ConsensusShard<AS, ARS>: ConsensusShardTrait,
{
    fn with_pool(
        process_count: usize,
        my_pid: usize,
        shard_count: usize,
        pool_size: usize,
        sinks: Arc<Mutex<MultiSink>>,
        new_shard: Box<dyn Fn() -> ConsensusShard<AS, ARS> + Send>,
    ) -> Self {
        Self {
            process_count,
            pool: ShardPool::new(
                shard_count,
                pool_size,
                SleepingShard::new(my_pid),
                new_shard,
            ),
            sinks,
        }
    }

    #[inline]
    fn wake_shard(&mut self, shard_id: usize) -> &mut ConsensusShard<AS, ARS> {
        self.pool.wake(shard_id)
    }

    /// True if `msg` is provably a no-op for `shard_id`, so that it can be dropped
    /// instead of claiming a physical shard for a round-trip that changes nothing.
    ///
    /// Waking up is cheap but not free: it costs two `active_shards` operations and the
    /// field copies of `wake_up` and `fall_asleep`, plus, in debug builds, the linear
    /// `round_state_is_clear` scan of `can_sleep`. A wake-up that the pool can not serve
    /// is the expensive one: it grows the pool for nothing, and inflates the size the
    /// end-of-run report tells experiments to preallocate.
    ///
    /// Only ever called on messages that carry no value: a value has to be stored (and,
    /// in kcensus, kept travelling along the propagation graph) whatever slot it belongs
    /// to, otherwise peers wait forever for a value that no one relays any more.
    fn is_noop_for_sleeping_shard(&self, shard_id: usize, msg: &ConsensusMessage) -> bool {
        if self.pool.is_active(shard_id) {
            return false;
        }
        // A sleeping shard has no pending read (`can_sleep` requires it), and its uid
        // counter is preserved precisely so that a late answer can not match a future
        // read, so `receive_ready` is guaranteed to find nothing.
        if matches!(msg.msg, ReadResponse { .. }) {
            return true;
        }
        // Messages of an already decided slot are dropped by every algorithm. Only the
        // slot can be filtered on: the round is not part of `SleepingShard`, and a
        // sleeping shard has cleared its round state anyway.
        // `is_some_and` matters here: `None < Some(slot)` holds, and the messages with no
        // slot (ReadRequest, SpreadValueOnly, ForwardRequest) all still have to be served.
        msg.get_slot()
            .is_some_and(|slot| slot < self.pool.sleeping_state(shard_id).slot)
    }

    #[inline]
    fn active_shard(&mut self, shard_id: usize) -> &mut ConsensusShard<AS, ARS> {
        self.pool.active_shard(shard_id)
    }

    #[inline]
    fn try_sleep(&mut self, shard_id: usize) {
        self.pool.try_sleep(shard_id)
    }
}

impl<AS, ARS> Consensus<AS, ARS>
where
    ConsensusShard<AS, ARS>: ConsensusShardTrait + Debug,
{
    pub async fn run(
        &mut self,
        mut msg_rx: Receiver<MsgWithSource>,
        mut new_client_commands_rx: Receiver<Command>,
        committed_commands_tx: Sender<Command>,
        deadlock_deadline: Duration,
    ) -> io::Result<()> {
        let mut count_done = 0usize;
        let mut done = false;

        {
            let sample = self.pool.sample();
            assert!(sample.can_forward_proposals() || sample.can_propose());
        }

        let deadlock_deadline =
            Delay::new(Instant::now() + deadlock_deadline).expect("should init timer");
        pin!(deadlock_deadline);

        'main_loop: while count_done < self.process_count {
            // Read new messages and/or new local command.
            // `None` means no shard was touched, `false` means the shard has nothing
            // left to propose or repropose.
            let touched: Option<(usize, bool)> = select! {
                res = &mut deadlock_deadline => {
                    res.expect("should wait until deadlock_deadline");
                    eprintln!("deadlock detected ! Checking all active shards...");
                    let mut stuck = 0usize;
                    for (shard_id, shard) in self.pool.iter_active() {
                        // A read still short of its quorum keeps a shard awake without
                        // showing up in any of the other three, so it is checked here too.
                        let awaiting_read = !shard.read_tracker.is_empty();
                        if shard.ongoing() || shard.has_queued_commands() || !shard.queued_messages.is_empty() || awaiting_read {
                            stuck += 1;
                            if stuck <= DEADLOCK_REPORT_LIMIT {
                                eprintln!("shard={shard_id} is stuck. Shard state: {shard:?}");
                                eprintln!("Queued messages for shard={shard_id}: {:?}", shard.queued_messages);
                                if awaiting_read {
                                    eprintln!("Awaited reads for shard={shard_id}: {:?}", shard.read_tracker);
                                }
                            }
                        }
                    }
                    if stuck > DEADLOCK_REPORT_LIMIT {
                        eprintln!("... and {} more stuck shard(s), not printed.", stuck - DEADLOCK_REPORT_LIMIT);
                    }
                    eprintln!("{stuck} stuck shard(s) of {} active ({} asleep).", self.pool.active_count(), self.pool.shard_count() - self.pool.active_count());
                    panic!("deadlock detected, terminating.");
                },
                command = new_client_commands_rx.recv(), if !done => {
                    match command {
                        Some(mut command) =>  {
                            let shard_id = command.shard;
                            let shard = self.wake_shard(shard_id);
                            command.arrival_slot = shard.slot;
                            if command.read_only {
                                shard.start_read(command).await?;
                            } else {
                                let ongoing = shard.ongoing() || !shard.queued_messages.is_empty();
                                let contention = ongoing || shard.has_queued_commands();
                                if shard.can_forward_proposals() || !contention {
                                    shard.propose_start(CommandBatch::Single(command), contention).await?;
                                } else {
                                    shard.my_queued_commands.push_back(command);
                                }
                            }
                            Some((shard_id, true))
                        }
                        None => {
                            debug_assert!(self
                                .pool
                                .iter_active()
                                .all(|(_, shard)| shard.my_queued_commands.is_empty()));
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
                        ConsensusM { shard: shard_id, msg, value } => 'shard_msg: {
                            trace!("received: {msg:?}");
                            if value.is_none() && self.is_noop_for_sleeping_shard(shard_id, &msg) {
                                debug_assert!(!msg.should_include_value());
                                break 'shard_msg None;
                            }
                            let shard = self.wake_shard(shard_id);
                            let new_value = value.is_some();
                            if let Some(value) = value {
                                debug_assert!(msg.can_include_value());
                                let v = msg.get_v().expect("A value should travel with its uid");
                                shard.store_remote_command(v, value);
                            } else {
                                debug_assert!(!msg.should_include_value());
                            };

                            if !shard.ready_to_process(&msg) {
                                shard.queued_messages.push_back(msg);
                                break 'shard_msg Some((shard_id, false));
                            }

                            let res_command = shard.full_process_message(msg, false).await?;
                            let commited = shard
                                .commit_commands(&committed_commands_tx, res_command)
                                .await;

                            if !commited && !new_value {
                                // No need to process queue nor repropose.
                                break 'shard_msg Some((shard_id, false));
                            }

                            // Process queued messages (if ready)
                            let mut i = 0usize;
                            while i < shard.queued_messages.len() {
                                // TODO: (Optim.) check Commit messages first ?
                                if shard.ready_to_process(&shard.queued_messages[i]) {
                                    let msg = shard.queued_messages.remove(i).unwrap();
                                    let batch = shard.full_process_message(msg, true).await?;
                                    if shard
                                        .commit_commands(&committed_commands_tx, batch)
                                        .await
                                    {
                                        i = 0; // Restart from the beginning of the queue
                                    }
                                } else {
                                    i += 1;
                                }
                            }

                            Some((shard_id, true))
                        }
                        Done => {
                            count_done += 1;
                            None
                        }
                        _ => panic!("Unexpected message type"),
                    }
                },
            };

            let Some((shard_id, may_propose)) = touched else {
                continue 'main_loop;
            };

            if may_propose {
                let shard = self.active_shard(shard_id);
                let ongoing = shard.ongoing() || !shard.queued_messages.is_empty();
                let should_repropose = !ongoing && shard.has_queued_commands();
                let contention = ongoing || should_repropose;

                if ongoing && !shard.ongoing() {
                    warn!(
                        "Consensus is not running but messages are still queued. my slot: {:?}, queue: {:?}",
                        shard.slot, shard.queued_messages
                    );
                }

                // Process queued commands
                // TODO: (Optim.) peak connection first ?
                if should_repropose && shard.should_lead() {
                    if let Some(batch) = shard.get_new_batch_to_propose() {
                        shard.propose_start(batch, false).await?;
                    } else {
                        let v = shard.get_v_to_repropose();
                        shard.repropose_start(v).await?;
                    }
                } else if !contention && !shard.my_queued_commands.is_empty() {
                    let command = shard.my_queued_commands.pop_front().unwrap();
                    assert!(!command.read_only, "read_only command wrongly queued");
                    shard
                        .propose_start(CommandBatch::Single(command), false)
                        .await?;
                }
            }

            // Release the physical shard if the logical shard has nothing left to do.
            self.try_sleep(shard_id);
        } // 'main_loop: loop

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
    } // run
}

impl<AS, ARS> PooledShard for ConsensusShard<AS, ARS>
where
    ConsensusShard<AS, ARS>: ConsensusShardTrait,
{
    type Sleeping = SleepingShard;

    /// Assigns this (pooled, hence pristine) physical shard to a logical shard.
    #[inline]
    fn wake_up(&mut self, shard_id: usize, state: SleepingShard) {
        debug_assert!(self.queued_commands.is_empty());
        debug_assert!(self.queued_messages.is_empty());
        debug_assert!(self.my_queued_commands.is_empty());
        debug_assert!(self.read_tracker.is_empty());
        debug_assert!(self.round_state_is_clear());

        self.sinks.shard_id = shard_id;
        self.next_uid = state.next_uid;
        self.slot = state.slot;
        self.last_v = state.last_v;
        self.slot_left_fast_path = false;
        self.read_tracker.set_next_id(state.next_read_id);
    }

    /// Compresses the state of an idle logical shard, leaving the physical shard pristine.
    #[inline]
    fn fall_asleep(&mut self) -> SleepingShard {
        debug_assert!(self.can_sleep());
        self.queued_commands.shrink_to(IDLE_CAPACITY);
        self.queued_messages.shrink_to(IDLE_CAPACITY);
        self.my_queued_commands.shrink_to(IDLE_CAPACITY);
        SleepingShard {
            next_uid: self.next_uid,
            slot: self.slot,
            last_v: self.last_v,
            next_read_id: self.read_tracker.next_id(),
        }
    }

    /// A shard can only be released once nothing is waiting to be ordered in it.
    ///
    /// An idle shard has cleared its round state by construction, so `round_state_is_clear`
    /// is only asserted in debug builds: it is a linear scan over the node states, which is
    /// not worth paying on every message to confirm an invariant that holds anyway. What it
    /// guards against is a shard falling asleep on a leftover proposal or accepted value,
    /// which `fall_asleep` would silently drop.
    #[inline]
    fn can_sleep(&self) -> bool {
        let idle = !self.ongoing()
            && self.queued_commands.is_empty()
            && self.queued_messages.is_empty()
            && self.my_queued_commands.is_empty()
            && self.read_tracker.is_empty();
        if !idle {
            return false;
        }
        debug_assert!(
            self.round_state_is_clear(),
            "an idle shard should have cleared its round state already"
        );
        true
    }
}

impl<AS, ARS> ConsensusShard<AS, ARS>
where
    ConsensusShard<AS, ARS>: ConsensusShardTrait,
{
    #[inline]
    async fn start_read(&mut self, command: Command) -> io::Result<()> {
        let local_ready = self.get_my_v().is_none();
        let id = self.read_tracker.insert(command, local_ready);
        self.sinks
            .broadcast(ReadRequest { id }, None, self.last_v)
            .await
    }

    #[inline]
    fn ready_to_process(&self, msg: &ConsensusMessage) -> bool {
        if msg.get_slot() == Some(self.slot) {
            assert!(
                msg.last_v.is_none() || msg.last_v == self.last_v,
                "last_v should be the same in shard {} for slot {}, but msg indicates {:?} while local state is {:?}. msg:{:?}",
                self.sinks.shard_id,
                self.slot,
                msg.last_v,
                self.last_v,
                msg
            );
            let value_ready = match msg.get_v() {
                None => true,
                Some(v) => match self.queued_commands.get(&v) {
                    None => false,
                    Some(CommandBatch::Single(_)) => true,
                    Some(CommandBatch::Batch { slot, vs }) => {
                        assert_eq!(*slot, self.slot);
                        vs.iter().all(|v| self.queued_commands.contains_key(v))
                    }
                },
            };
            value_ready
                || matches!(msg.msg, PaxosM(Prepare { round, .. } | Accept { round, .. }) if round.leader != self.my_pid)
        } else {
            msg.get_slot() < Some(self.slot) // "Process" messages from lower slots, regardless of value
        }
    }

    /// `from_queue` says whether this message was held in `queued_messages` rather than acted
    /// on when it arrived. Only `ReadResponse` cares; see there.
    async fn full_process_message(
        &mut self,
        msg: ConsensusMessage,
        from_queue: bool,
    ) -> io::Result<Option<CommandBatch>> {
        debug_assert!(self.ready_to_process(&msg));
        if let Commit { slot, v } = msg.msg {
            if slot < self.slot {
                return Ok(None);
            }
            debug_assert_eq!(slot, self.slot);
            info!(
                "Commit via msg: shard={} slot={slot} v={v}",
                self.sinks.shard_id
            );
            return Ok(Some(self.commit_slot(v, true)));
        };
        if let ReadRequest { id } = msg.msg {
            // Slot-layer reads only: there is no slot for this to name in dependency mode,
            // which answers reads with its own `DepMsg::ReadRequest` instead.
            let next_readable_slot = self.slot + self.get_my_v().is_some() as usize;
            self.sinks
                .send(
                    ReadResponse {
                        id,
                        next_readable_slot,
                    },
                    None,
                    msg.src,
                    None,
                )
                .await?;
            return Ok(None);
        }
        if let ReadResponse { id, .. } = msg.msg {
            // A read costs one round trip to a majority. It is fast exactly when the answer
            // completing that majority cost nothing beyond its own flight: it was not itself
            // held back waiting for a slot, and no other answer was queued behind it, which
            // would mean a majority had already arrived and we were the ones lagging. Answers
            // that waited *earlier* do not count -- they were released before the majority
            // formed, so they cost the read nothing.
            let fast = !from_queue
                && !self.queued_messages.iter().any(
                    |queued| matches!(queued.msg, ReadResponse { id: other, .. } if other == id),
                );
            return Ok(self
                .read_tracker
                .receive_ready(id, self.slot, fast)
                .map(CommandBatch::Single));
        }
        self.process_message(msg).await
    }

    #[inline]
    async fn commit_commands(
        &mut self,
        committed_commands_tx: &Sender<Command>,
        batch: Option<CommandBatch>,
    ) -> bool {
        let Some(batch) = batch else {
            return false;
        };

        // A read served by its own quorum arrives here as its own batch, but nothing committed
        // a slot for it. None of the bookkeeping below applies: it would clear the conflict
        // flag of a round still in progress, hand the read tracker a slot commit that did not
        // happen, and underflow `self.slot - 1` on the very first slot.
        if matches!(&batch, CommandBatch::Single(command) if command.read_only) {
            if let CommandBatch::Single(command) = batch {
                committed_commands_tx
                    .send(command)
                    .await
                    .expect("Sending commited value");
            }
            return true;
        }

        {
            // `commit_slot` has already advanced past the slot that just committed.
            let committed_slot = self.slot - 1;
            let fast = self.has_fast_path() && !self.slot_left_fast_path;
            let my_pid = self.my_pid;
            self.slot_left_fast_path = false;
            // Writes only: a read is never proposed into a slot, so it never reaches here.
            let commit = async |mut command: Command| {
                assert!(!command.read_only);
                if command.requester == my_pid {
                    command.report = Some(CommitReport {
                        fast,
                        waited_for: committed_slot.saturating_sub(command.arrival_slot),
                    });
                }
                committed_commands_tx
                    .send(command)
                    .await
                    .expect("Sending commited value");
            };

            match batch {
                CommandBatch::Single(command) => {
                    commit(command).await;
                }
                CommandBatch::Batch { vs, .. } => {
                    for v in vs {
                        let command = self.remove_command(v);
                        if let CommandBatch::Single(command) = command {
                            commit(command).await;
                        } else {
                            panic!("Batches should not include batches.")
                        }
                    }
                }
            }

            // These have already be stamped. The
            // slot's own fast/slow says nothing about it.
            for read_only_command in self.read_tracker.commit_slot(self.slot) {
                info!(
                    "Commit read: shard={} slot={}",
                    self.sinks.shard_id, self.slot
                );
                committed_commands_tx
                    .send(read_only_command)
                    .await
                    .expect("Sending commited value");
            }
        }
        true
    }

    #[inline]
    fn store_new_command(&mut self, value: CommandBatch) -> usize {
        let v = self.get_next_uid(matches!(value, CommandBatch::Batch { .. }));
        let old = self.queued_commands.insert(v, value);
        debug_assert!(old.is_none());
        v
    }

    #[inline]
    fn store_remote_command(&mut self, v: usize, value: CommandBatch) {
        // TODO: Allow forwarding values ? (could the value already be there ?)
        if let CommandBatch::Batch { slot, .. } = value {
            if slot < self.slot {
                return;
            }
        }
        let inserted = self.queued_commands.insert(v, value);
        debug_assert!(inserted.is_none());
    }

    #[inline]
    fn has_queued_commands(&self) -> bool {
        !self.queued_commands.is_empty()
    }

    fn get_new_batch_to_propose(&self) -> Option<CommandBatch> {
        if self.queued_commands.len() <= 1 {
            return None;
        }
        let vs: Vec<_> = self
            .queued_commands
            .iter()
            .filter_map(|(v, cmd)| match cmd {
                CommandBatch::Single(_) => Some(*v),
                _ => None,
            })
            .collect();
        if vs.len() <= 1 {
            return None;
        }
        let slot = self.slot;
        Some(CommandBatch::Batch { slot, vs })
    }

    #[inline]
    fn get_v_to_repropose(&self) -> usize {
        *self
            .queued_commands
            .iter()
            .filter_map(|(v, value)| match value {
                CommandBatch::Single(_) => Some(v),
                _ => None,
            })
            .min()
            .unwrap()
    }

    fn purge_batches(&mut self, commited_slot: usize) {
        self.queued_commands.retain(|_, value| match value {
            CommandBatch::Single(_) => true,
            CommandBatch::Batch { slot, .. } => commited_slot < *slot,
        });
    }

    fn remove_command(&mut self, v: usize) -> CommandBatch {
        let out = self.queued_commands.remove(&v);
        out.expect("Removing command that does not exist")
    }

    #[inline]
    fn get_requester(&self, v: usize) -> Option<usize> {
        if v % 2 == 0 {
            Some((v >> 1) % self.process_count)
        } else {
            None
        }
    }

    fn get_next_uid(&mut self, batch: bool) -> usize {
        let uid = self.next_uid;
        self.next_uid += 2 * self.process_count;
        if batch { uid + 1 } else { uid }
    }
}
