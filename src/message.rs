use crate::consensus::message::{CommandBatch, ConsensusMessage};
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Message {
    Hello {
        pid: usize,
    },
    ConsensusM {
        msg: ConsensusMessage,
        value: Option<CommandBatch>,
    },
    Done,
    RoundRobin,
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
        match self {
            Message::ConsensusM { .. } => true,
            _ => false,
        }
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
