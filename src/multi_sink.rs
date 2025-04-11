use crate::connector::DeSink;
use crate::consensus::message::{CommandBatch, ConsensusMessage, ConsensusMsg};
use crate::message::Message;
use bit_set::BitSet;
use futures::SinkExt;
use log::debug;
use std::collections::HashMap;
use std::io;

pub struct MultiSink {
    pub my_pid: usize,
    pub sinks: HashMap<usize, DeSink>,
    pub faults: BitSet,
}

impl MultiSink {
    #[inline]
    pub async fn broadcast(
        &mut self,
        msg: ConsensusMsg,
        value: Option<CommandBatch>,
    ) -> io::Result<()> {
        self.inner_broadcast(self.build_msg(msg, value)).await
    }

    #[inline]
    pub async fn send(
        &mut self,
        msg: ConsensusMsg,
        value: Option<CommandBatch>,
        pid: usize,
    ) -> io::Result<()> {
        self.inner_send(self.build_msg(msg, value), pid).await
    }

    #[inline]
    pub async fn inner_broadcast(&mut self, msg: Message) -> io::Result<()> {
        debug!("Broadcasting {:?}", msg);
        for (dest, sink) in self.sinks.iter_mut() {
            if self.faults.contains(*dest) && msg.delayed() {
                continue;
            }
            sink.send(msg.clone()).await?;
        }
        Ok(())
    }

    #[inline]
    pub async fn inner_send(&mut self, msg: Message, pid: usize) -> io::Result<()> {
        debug!("Sending to {pid}: {:?}", msg);
        debug_assert!(pid != self.my_pid);
        if self.faults.contains(pid) && msg.delayed() {
            return Ok(());
        }
        let sink = self.sinks.get_mut(&pid).unwrap();
        sink.send(msg.clone()).await
    }

    #[inline]
    fn build_msg(&self, msg: ConsensusMsg, value: Option<CommandBatch>) -> Message {
        Message::ConsensusM {
            msg: ConsensusMessage {
                msg,
                src: self.my_pid,
            },
            value,
        }
    }
}
