use crate::connector::DeSink;
use crate::consensus::message::{ConsensusMessage, ConsensusMsg};
use crate::message::Message;
use crate::value::KVal;
use futures::SinkExt;
use std::collections::HashMap;
use std::io;

pub struct MultiSink<Sk> {
    pub my_pid: usize,
    pub sinks: HashMap<usize, Sk>,
}

impl MultiSink<DeSink> {
    #[inline]
    pub async fn broadcast(&mut self, msg: ConsensusMsg, value: Option<KVal>) -> io::Result<()> {
        self.inner_broadcast(self.build_msg(msg, value)).await
    }

    #[inline]
    pub async fn send(
        &mut self,
        msg: ConsensusMsg,
        value: Option<KVal>,
        pid: usize,
    ) -> io::Result<()> {
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
    async fn inner_send(&mut self, msg: Message, pid: usize) -> io::Result<()> {
        debug_assert!(pid != self.my_pid);
        let sink = self.sinks.get_mut(&pid).unwrap();
        sink.send(msg.clone()).await
    }

    fn build_msg(&self, msg: ConsensusMsg, value: Option<KVal>) -> Message {
        Message::ConsensusM {
            msg: ConsensusMessage {
                msg,
                src: self.my_pid,
            },
            value,
        }
    }
}
