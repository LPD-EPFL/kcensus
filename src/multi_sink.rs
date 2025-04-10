use crate::connector::DeSink;
use crate::consensus::command::Command;
use crate::consensus::message::{ConsensusMessage, ConsensusMsg};
use crate::message::Message;
use futures::SinkExt;
use log::debug;
use std::collections::HashMap;
use std::io;

pub struct MultiSink {
    pub my_pid: usize,
    pub sinks: HashMap<usize, DeSink>,
}

impl MultiSink {
    #[inline]
    pub async fn broadcast(&mut self, msg: ConsensusMsg, value: Option<Command>) -> io::Result<()> {
        debug!("Broadcasting {:?} with_value={}", msg, value.is_some());
        self.inner_broadcast(self.build_msg(msg, value)).await
    }

    #[inline]
    pub async fn send(
        &mut self,
        msg: ConsensusMsg,
        value: Option<Command>,
        pid: usize,
    ) -> io::Result<()> {
        debug!("Sending to {pid}: {:?} with_value={}", msg, value.is_some());
        self.inner_send(self.build_msg(msg, value), pid).await
    }

    #[inline]
    pub async fn inner_broadcast(&mut self, msg: Message) -> io::Result<()> {
        for (_, sink) in self.sinks.iter_mut() {
            sink.send(msg.clone()).await?;
        }
        Ok(())
    }

    #[inline]
    pub async fn inner_send(&mut self, msg: Message, pid: usize) -> io::Result<()> {
        debug_assert!(pid != self.my_pid);
        let sink = self.sinks.get_mut(&pid).unwrap();
        sink.send(msg.clone()).await
    }

    #[inline]
    fn build_msg(&self, msg: ConsensusMsg, value: Option<Command>) -> Message {
        Message::ConsensusM {
            msg: ConsensusMessage {
                msg,
                src: self.my_pid,
            },
            value,
        }
    }
}
