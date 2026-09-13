use crate::consensus::deps::message::ShardAcks;
use crate::consensus::message::{CommandBatch, ConsensusMessage};
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Message {
    Hello {
        pid: usize,
    },
    ConsensusM {
        shard: usize,
        msg: ConsensusMessage,
        value: Option<CommandBatch>,
    },
    /// Several shards' worth of `PreAcceptOk`/`AcceptOk` in one message. The acks a
    /// process owes span shards, and a `ConsensusM` is tagged with exactly one, so
    /// bundling needs an envelope of its own.
    DepAcks {
        src: usize,
        shards: Vec<(usize, ShardAcks)>,
    },
    Ready,
    Done,
}

#[derive(Debug)]
pub struct MsgWithSource {
    pub msg: Message,
    pub src: usize,
}

pub struct MsgWithDeadline {
    pub msg: MsgWithSource,
    pub deadline: Instant,
}

impl Message {
    #[inline]
    pub fn with_source(self, src: usize) -> MsgWithSource {
        MsgWithSource { msg: self, src }
    }

    pub fn is_consensus_msg(&self) -> bool {
        matches!(self, Message::ConsensusM { .. } | Message::DepAcks { .. })
    }
}

impl MsgWithSource {
    #[inline]
    pub fn with_deadline(self, deadline: Instant) -> MsgWithDeadline {
        MsgWithDeadline {
            msg: self,
            deadline,
        }
    }
}
