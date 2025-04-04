use crate::kcensus::message::KCensusMsg;
use crate::value::KVal;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Message {
    Hello {
        pid: usize,
    },
    // TODO: Add path-graph to spread messages
    KCensusMessage {
        msg: KCensusMsg,
        value: Option<KVal>,
    },
    Done,
}

#[derive(Debug)]
pub struct MsgWithSource {
    pub msg: Message,
    pub src: usize,
}

impl Message {
    #[inline]
    pub fn with_source(self, src: usize) -> MsgWithSource {
        MsgWithSource { msg: self, src }
    }
}

pub struct MsgWithDeadline {
    pub msg: MsgWithSource,
    pub deadline: Instant,
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
