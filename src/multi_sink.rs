use crate::connector::WrappedSink;
use crate::consensus::message::{CommandBatch, ConsensusMessage, ConsensusMsg};
use crate::message::Message;
use bincode::Options;
use bit_set::BitSet;
use futures::SinkExt;
use log::debug;
use serde::Serialize;
use std::collections::HashMap;
use std::io;
use tokio_util::bytes::Bytes;

pub struct MultiSink {
    my_pid: usize,
    sinks: HashMap<usize, WrappedSink>,
    pub faults: BitSet,
    pub stats: Stats,
}

#[derive(Serialize, Default)]
pub struct Stats {
    pub msg_count: usize,
    pub byte_count: usize,
}

pub fn encode(msg: &Message) -> Bytes {
    bincode::DefaultOptions::new()
        .serialize(msg)
        .expect("Serializer failure")
        .into()
}

impl MultiSink {
    pub fn new(my_pid: usize, nb_nodes: usize) -> Self {
        Self {
            my_pid,
            sinks: HashMap::with_capacity(nb_nodes - 1),
            faults: BitSet::with_capacity(nb_nodes),
            stats: Stats::default(),
        }
    }

    pub fn new_with_faults(
        my_pid: usize,
        nb_nodes: usize,
        faults: impl Iterator<Item = usize>,
    ) -> Self {
        let mut x = Self::new(my_pid, nb_nodes);
        x.faults.extend(faults);
        x
    }

    pub fn insert_sink(&mut self, pid: usize, sink: WrappedSink) {
        let out = self.sinks.insert(pid, sink);
        assert!(out.is_none(), "There should be no 2 sinks with same pid");
    }

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
        let bytes = encode(&msg);
        for (dest, sink) in self.sinks.iter_mut() {
            if msg.is_consensus_msg() {
                if self.faults.contains(*dest) {
                    continue;
                }
                self.stats.msg_count += 1;
                self.stats.byte_count += bytes.len();
            }

            sink.send(bytes.clone()).await?;
        }
        Ok(())
    }

    #[inline]
    pub async fn inner_send(&mut self, msg: Message, pid: usize) -> io::Result<()> {
        debug!("Sending to {pid}: {:?}", msg);
        debug_assert!(pid != self.my_pid);
        if msg.is_consensus_msg() && self.faults.contains(pid) {
            return Ok(());
        }
        let bytes = encode(&msg);
        if msg.is_consensus_msg() {
            self.stats.msg_count += 1;
            self.stats.byte_count += bytes.len();
        }
        let sink = self.sinks.get_mut(&pid).unwrap();
        sink.send(bytes).await
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
