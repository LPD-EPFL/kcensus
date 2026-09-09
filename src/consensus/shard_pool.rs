use crate::eval;
use log::warn;
use std::collections::HashMap;

/// Number of physical shards preallocated when no explicit pool size is requested.
pub const DEFAULT_SHARD_POOL_SIZE: usize = 1000;

/// A shard that a [`ShardPool`] can recycle: it can compress itself into a small
/// `Sleeping` state, hand its physical slot to another logical shard, and be restored
/// later. Both ordering layers (slot-based and dependency-based) implement this, which is
/// what lets them share the pool.
pub(crate) trait PooledShard {
    /// Everything about an idle logical shard that has to survive while it sleeps.
    /// Kept small: one of these exists per *logical* shard, for every shard.
    type Sleeping: Clone;

    /// Assigns this (pooled, hence pristine) physical shard to a logical shard.
    fn wake_up(&mut self, shard_id: usize, state: Self::Sleeping);

    /// Compresses the state of an idle logical shard, leaving the physical shard pristine.
    fn fall_asleep(&mut self) -> Self::Sleeping;

    /// A shard can only be released once nothing is waiting to be ordered in it.
    fn can_sleep(&self) -> bool;
}

/// End-of-run report on the physical shard pool.
#[derive(serde::Serialize)]
struct ShardPoolStats {
    /// Number of logical shards sharing this pool.
    logical_shards: usize,
    /// Size of the pool at the end of the run.
    pool_size: usize,
    /// Size it was preallocated with. A larger `pool_size` means it was too small.
    initial_pool_size: usize,
}

/// Builds an unassigned physical shard, used to fill (and, if needed, grow) the pool.
type ShardFactory<S> = Box<dyn Fn() -> S + Send>;

/// A fixed set of physical shards, reused by whichever logical shards are active.
///
/// Together, `active` and `free` cover each index of `shards` exactly once, which is what
/// guarantees a physical shard is never used by two logical shards at the same time.
pub(crate) struct ShardPool<S: PooledShard> {
    /// Compressed state of every logical shard, indexed by shard id.
    /// Only meaningful for the shards that are not in `active`.
    sleeping: Vec<S::Sleeping>,
    /// The physical shards. They are allocated once and never moved out of the pool, so
    /// waking a logical shard up only overwrites the few fields of `Sleeping`.
    shards: Vec<S>,
    /// Logical shard id -> index in `shards` of the shard currently serving it.
    active: HashMap<usize, usize>,
    /// Indices in `shards` that no logical shard is currently using.
    free: Vec<usize>,
    /// Only used when more shards are active at once than the pool was sized for.
    new_shard: ShardFactory<S>,
    /// Size the pool was preallocated with, kept to report how far it had to grow.
    initial_pool_size: usize,
}

impl<S: PooledShard> ShardPool<S> {
    pub fn new(
        shard_count: usize,
        pool_size: usize,
        initial_sleeping: S::Sleeping,
        new_shard: ShardFactory<S>,
    ) -> Self {
        assert!(shard_count > 0);
        let pool_size = pool_size.clamp(1, shard_count);
        Self {
            sleeping: vec![initial_sleeping; shard_count],
            shards: (0..pool_size).map(|_| new_shard()).collect(),
            active: HashMap::with_capacity(pool_size),
            // Reversed so that the lowest indices are handed out first.
            free: (0..pool_size).rev().collect(),
            new_shard,
            initial_pool_size: pool_size,
        }
    }

    /// Returns the physical shard serving `shard_id`, waking the logical shard up
    /// (i.e. claiming a free physical shard and restoring its state) if needed.
    pub fn wake(&mut self, shard_id: usize) -> &mut S {
        let physical = if let Some(&physical) = self.active.get(&shard_id) {
            physical
        } else {
            let physical = match self.free.pop() {
                Some(physical) => physical,
                None => self.grow(shard_id),
            };
            self.shards[physical].wake_up(shard_id, self.sleeping[shard_id].clone());
            let previously_serving = self.active.insert(shard_id, physical);
            debug_assert!(previously_serving.is_none());
            self.debug_assert_invariant();
            physical
        };
        &mut self.shards[physical]
    }

    /// Appends one physical shard to the pool, because more logical shards are awake at
    /// once than it was sized for, and returns its index.
    ///
    /// One at a time is deliberate: the cost is dominated by building the shard (its own
    /// maps and vectors), which no batching would avoid, while the `Vec` reallocation it
    /// may trigger is already amortised. Growing in bigger steps would only make the rare
    /// hiccup bigger. Experiments should size the pool up front.
    #[cold]
    fn grow(&mut self, shard_id: usize) -> usize {
        let pool_size = self.shards.len();
        // Only the first growth is reported here: one line per added shard would be a
        // problem of its own with many shards. The end-of-run report gives the size the
        // pool had to reach, which is the number an experiment should be resized with.
        if pool_size == self.initial_pool_size {
            warn!(
                "the pool of {pool_size} physical shards is too small: growing it to \
                 serve logical shard {shard_id} (raise --shard-pool to avoid this)"
            );
        }
        self.shards.push((self.new_shard)());
        pool_size
    }

    #[inline]
    pub fn active_shard(&mut self, shard_id: usize) -> &mut S {
        let physical = *self
            .active
            .get(&shard_id)
            .expect("shard should still be awake");
        &mut self.shards[physical]
    }

    #[inline]
    pub fn is_active(&self, shard_id: usize) -> bool {
        self.active.contains_key(&shard_id)
    }

    /// The compressed state of a logical shard. Only meaningful while it is asleep.
    #[inline]
    pub fn sleeping_state(&self, shard_id: usize) -> &S::Sleeping {
        &self.sleeping[shard_id]
    }

    /// Puts `shard_id` back to sleep if it has nothing left to do, freeing its
    /// physical shard for any other logical shard to claim.
    pub fn try_sleep(&mut self, shard_id: usize) {
        let Some(&physical) = self.active.get(&shard_id) else {
            return;
        };
        let shard = &mut self.shards[physical];
        if !shard.can_sleep() {
            return;
        }
        self.sleeping[shard_id] = shard.fall_asleep();
        self.active.remove(&shard_id);
        debug_assert!(
            !self.free.contains(&physical),
            "physical shard {physical} freed twice"
        );
        self.free.push(physical);
        self.debug_assert_invariant();
    }

    /// Every physical shard is either free or serving exactly one logical shard.
    #[inline]
    fn debug_assert_invariant(&self) {
        debug_assert_eq!(
            self.active.len() + self.free.len(),
            self.shards.len(),
            "a physical shard is either used twice or lost"
        );
    }

    #[inline]
    pub fn shard_count(&self) -> usize {
        self.sleeping.len()
    }

    #[inline]
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    /// (logical shard id, physical shard) for every awake shard.
    pub fn iter_active(&self) -> impl Iterator<Item = (usize, &S)> {
        self.active
            .iter()
            .map(|(shard_id, &physical)| (*shard_id, &self.shards[physical]))
    }

    #[inline]
    pub fn sample(&self) -> &S {
        self.shards.first().expect("pool should not be empty")
    }

    /// Warns if the pool had to grow, and logs the size an experiment should preallocate.
    pub fn report(&self) {
        let pool_size = self.shards.len();
        if pool_size > self.initial_pool_size {
            warn!(
                "the shard pool had to grow from {} to {pool_size} physical shards: \
                 pass --shard-pool {pool_size} to preallocate it",
                self.initial_pool_size
            );
        }
        eval::log(
            "shard-pool-done",
            &format!(
                "shard pool has {pool_size} physical shards ({} preallocated)",
                self.initial_pool_size
            ),
            &ShardPoolStats {
                logical_shards: self.shard_count(),
                pool_size,
                initial_pool_size: self.initial_pool_size,
            },
        );
    }
}
