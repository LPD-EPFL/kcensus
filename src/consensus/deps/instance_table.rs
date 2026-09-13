use crate::consensus::deps::dep_set::{requester_of, uid_step};
use crate::consensus::deps::instance::Instance;
use std::collections::VecDeque;
use std::ops::Index;

/// The unfinished instances, laid out the way the reference lays out `InstanceSpace`: one
/// log per replica, addressed by position, so stepping a dependency range is an array
/// index rather than a hash lookup.
///
/// A slot holds `None` until its payload arrives and again once it has executed. The
/// instance itself is boxed so a vacated slot costs a pointer, which is what lets a log
/// keep positions for a whole run without holding the commands.
pub(crate) struct InstanceTable {
    process_count: usize,
    logs: Vec<VecDeque<Option<Box<Instance>>>>,
    /// The uid each log's first slot stands for.
    front: Vec<usize>,
    len: usize,
}

impl InstanceTable {
    pub fn new(process_count: usize) -> Self {
        Self {
            process_count,
            logs: (0..process_count).map(|_| VecDeque::new()).collect(),
            front: (0..process_count).map(|replica| 2 * replica).collect(),
            len: 0,
        }
    }

    /// Which log and which slot `uid` lives in, or `None` when it is behind the front and
    /// so already gone.
    #[inline]
    fn position(&self, uid: usize) -> Option<(usize, usize)> {
        let replica = requester_of(uid, self.process_count);
        let front = self.front[replica];
        (uid >= front).then(|| (replica, (uid - front) / uid_step(self.process_count)))
    }

    #[inline]
    pub fn get(&self, uid: usize) -> Option<&Instance> {
        let (replica, index) = self.position(uid)?;
        self.logs[replica].get(index)?.as_deref()
    }

    #[inline]
    pub fn get_mut(&mut self, uid: usize) -> Option<&mut Instance> {
        let (replica, index) = self.position(uid)?;
        self.logs[replica].get_mut(index)?.as_deref_mut()
    }

    #[inline]
    pub fn contains_key(&self, uid: usize) -> bool {
        self.get(uid).is_some()
    }

    pub fn insert(&mut self, uid: usize, instance: Instance) {
        let step = uid_step(self.process_count);
        let replica = requester_of(uid, self.process_count);
        // An empty log has no position to keep, so it starts wherever the next instance
        // does. A shard that slept and woke resumes at a uid far above `2 * replica`.
        if self.logs[replica].is_empty() {
            self.front[replica] = uid;
        } else if uid < self.front[replica] {
            let missing = (self.front[replica] - uid) / step;
            for _ in 0..missing {
                self.logs[replica].push_front(None);
            }
            self.front[replica] = uid;
        }
        let index = (uid - self.front[replica]) / step;
        let log = &mut self.logs[replica];
        if index >= log.len() {
            log.resize_with(index + 1, || None);
        }
        let previous = log[index].replace(Box::new(instance));
        debug_assert!(previous.is_none(), "an instance is never created twice");
        self.len += 1;
    }

    pub fn remove(&mut self, uid: usize) -> Option<Instance> {
        let (replica, index) = self.position(uid)?;
        let taken = self.logs[replica].get_mut(index)?.take()?;
        self.len -= 1;
        Some(*taken)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// (uid, instance) for every live slot, each replica's in uid order.
    pub fn iter(&self) -> impl Iterator<Item = (usize, &Instance)> + '_ {
        let step = uid_step(self.process_count);
        self.logs
            .iter()
            .enumerate()
            .flat_map(move |(replica, log)| {
                let front = self.front[replica];
                log.iter()
                    .enumerate()
                    .filter_map(move |(index, slot)| Some((front + index * step, slot.as_deref()?)))
            })
    }

    /// Drops the positions of a shard with nothing left, so the pool can hand it on.
    pub fn reset(&mut self) {
        debug_assert!(self.is_empty());
        for (replica, log) in self.logs.iter_mut().enumerate() {
            log.clear();
            log.shrink_to_fit();
            self.front[replica] = 2 * replica;
        }
    }
}

impl Index<usize> for InstanceTable {
    type Output = Instance;

    #[inline]
    fn index(&self, uid: usize) -> &Instance {
        self.get(uid).expect("indexed instance exists")
    }
}
