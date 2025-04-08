use crate::consensus::command::Command;
use crate::consensus::message::ReadUid;
use std::collections::BTreeMap;

pub struct ReadOnlyCommand {
    pub command: Command,
    pub ready_count: usize,
    pub local_ready: bool,
}

pub struct ReadTracker {
    next_id: usize,
    read_quorum: usize,
    read_commands: BTreeMap<ReadUid, ReadOnlyCommand>,
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
    pub fn insert(&mut self, command: Command, local_ready: bool) -> ReadUid {
        debug_assert!(command.read_only);
        let uid = ReadUid {
            reader: self.next_id,
            id: self.next_id,
        };
        self.next_id += 1;
        let old = self.read_commands.insert(
            uid,
            ReadOnlyCommand {
                command,
                ready_count: local_ready as usize,
                local_ready,
            },
        );
        debug_assert!(old.is_none());
        uid
    }

    pub fn commit_slot(&mut self) -> Vec<Command> {
        let mut commited_reads = Vec::new();
        for (uid, roc) in self.read_commands.iter_mut() {
            if !roc.local_ready {
                roc.ready_count += 1;
                roc.local_ready = true;
            }
            if roc.ready_count >= self.read_quorum {
                commited_reads.push(*uid);
            }
        }
        commited_reads
            .iter()
            .map(|uid| self.read_commands.remove(uid).unwrap().command)
            .collect()
    }

    pub fn receive_ready(&mut self, uid: ReadUid) -> Option<Command> {
        let roc = self.read_commands.get_mut(&uid)?;
        roc.ready_count += 1;
        if roc.ready_count > self.read_quorum {
            self.read_commands.remove(&uid).map(|roc| roc.command)
        } else {
            None
        }
    }
}
