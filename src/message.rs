use crate::consensus::message::ConsensusMessage;
use crate::value::KVal;
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Message {
    Hello {
        pid: usize,
    },
    ConsensusM {
        msg: ConsensusMessage,
        value: Option<KVal>,
    },
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
