use crate::connector::WrappedSink;
use crate::consensus::message::{CommandBatch, ConsensusMessage, ConsensusMsg};
use crate::message::Message;
use bincode::Options;
use bit_set::BitSet;
use futures::SinkExt;
use log::trace;
use serde::Serialize;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::bytes::Bytes;

pub struct MultiSink {
    my_pid: usize,
    nb_nodes: usize,
    sinks: HashMap<usize, WrappedSink>,
    pub faults: BitSet,
    pub stats: Stats,
}

pub struct ShardMultiSink {
    pub multi_sink: Arc<Mutex<MultiSink>>,
    pub shard_id: usize,
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
            nb_nodes,
            sinks: HashMap::with_capacity(nb_nodes - 1),
            faults: BitSet::new(),
            stats: Stats::default(),
        }
    }

    pub fn insert_sink(&mut self, pid: usize, sink: WrappedSink) {
        assert!(pid < self.nb_nodes);
        assert_ne!(pid, self.my_pid);
        let out = self.sinks.insert(pid, sink);
        assert!(out.is_none(), "There should be no 2 sinks with same pid");
    }

    #[inline]
    pub async fn broadcast(&mut self, msg: Message) -> io::Result<()> {
        trace!("Broadcasting {msg:?}");
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
    pub async fn send(&mut self, msg: Message, pid: usize) -> io::Result<()> {
        trace!("Sending to {pid}: {msg:?}");
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
}

impl ShardMultiSink {
    #[inline]
    pub async fn broadcast(
        &self,
        msg: ConsensusMsg,
        value: Option<CommandBatch>,
        last_v: Option<usize>,
    ) -> io::Result<()> {
        let mut multi_sink = self.multi_sink.lock().await;
        let msg = self.build_msg(msg, value, multi_sink.my_pid, last_v);
        multi_sink.broadcast(msg).await
    }

    #[inline]
    pub async fn send(
        &self,
        msg: ConsensusMsg,
        value: Option<CommandBatch>,
        pid: usize,
        last_v: Option<usize>,
    ) -> io::Result<()> {
        let mut multi_sink = self.multi_sink.lock().await;
        let msg = self.build_msg(msg, value, multi_sink.my_pid, last_v);
        multi_sink.send(msg, pid).await
    }

    #[inline]
    fn build_msg(
        &self,
        msg: ConsensusMsg,
        value: Option<CommandBatch>,
        my_pid: usize,
        last_v: Option<usize>,
    ) -> Message {
        Message::ConsensusM {
            shard: self.shard_id,
            msg: ConsensusMessage {
                msg,
                src: my_pid,
                last_v,
            },
            value,
        }
    }
}
