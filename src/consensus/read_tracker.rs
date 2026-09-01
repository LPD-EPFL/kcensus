use crate::consensus::command::Command;
use crate::consensus::message::ReadId;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};

pub struct ReadOnlyCommand {
    pub command: Command,
    pub ready_count: usize,
    pub local_ready: bool,
}

pub struct ReadTracker {
    next_id: usize,
    read_quorum: usize,
    read_commands: BTreeMap<ReadId, ReadOnlyCommand>,
}

/// Reports every read still short of its quorum, so that a stuck shard says what it is
/// waiting for. A shard holding a pending read can not sleep, so it is always reachable
/// through `active_shards` when the deadlock detector goes looking for it.
impl Debug for ReadTracker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "quorum={}, pending=[", self.read_quorum)?;
        for (i, (id, roc)) in self.read_commands.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{id:?}: {} ready", roc.ready_count)?;
            if !roc.local_ready {
                write!(f, ", waiting on the local slot")?;
            }
        }
        write!(f, "]")
    }
}

impl ReadTracker {
    pub fn new(read_quorum: usize) -> Self {
        Self {
            next_id: 0,
            read_quorum,
            read_commands: BTreeMap::new(),
        }
    }
}

impl ReadTracker {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.read_commands.is_empty()
    }

    #[inline]
    pub fn next_id(&self) -> usize {
        self.next_id
    }

    /// Restores the id counter of a shard that was put to sleep, so that late answers
    /// to reads issued before the shard fell asleep can not match a new read.
    #[inline]
    pub fn set_next_id(&mut self, next_id: usize) {
        debug_assert!(self.is_empty());
        self.next_id = next_id;
    }

    pub fn insert(&mut self, command: Command, local_ready: bool) -> ReadId {
        debug_assert!(command.read_only);
        let id = ReadId(self.next_id);
        self.next_id += 1;
        let old = self.read_commands.insert(
            id,
            ReadOnlyCommand {
                command,
                ready_count: local_ready as usize,
                local_ready,
            },
        );
        debug_assert!(old.is_none());
        id
    }

    pub fn commit_slot(&mut self) -> Vec<Command> {
        let mut commited_reads = Vec::new();
        for (id, roc) in self.read_commands.iter_mut() {
            if !roc.local_ready {
                roc.ready_count += 1;
                roc.local_ready = true;
            }
            if roc.ready_count >= self.read_quorum {
                commited_reads.push(*id);
            }
        }
        commited_reads
            .iter()
            .map(|id| self.read_commands.remove(id).unwrap().command)
            .collect()
    }

    pub fn receive_ready(&mut self, id: ReadId) -> Option<Command> {
        let roc = self.read_commands.get_mut(&id)?;
        roc.ready_count += 1;
        if roc.ready_count >= self.read_quorum {
            self.read_commands.remove(&id).map(|roc| roc.command)
        } else {
            None
        }
    }
}
