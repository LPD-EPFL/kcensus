use crate::connector::WrappedSink;
use crate::consensus::deps::dep_set::DepSet;
use crate::consensus::deps::message::ShardAcks;
use crate::consensus::kcensus::message::KCensusMsg;
use crate::consensus::message::{CommandBatch, ConsensusMessage, ConsensusMsg};
use crate::message::Message;
use crate::message::Message::ConsensusM;
use bincode::Options;
use bit_set::BitSet;
use futures::SinkExt;
use log::trace;
use serde::Serialize;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;
use tokio_util::bytes::Bytes;

pub struct MultiSink {
    my_pid: usize,
    nb_nodes: usize,
    sinks: HashMap<usize, WrappedSink>,
    pub alive_replicas: BitSet,
    pub stats: Stats,
    send_paxos_messages_to_all: bool,
    /// Acks formed but not sent yet, by shard, and the non-replica proposers owed one of
    /// them. Only SwiftPaxos fills this; see [`MultiSink::flush_acks`].
    pending_acks: HashMap<usize, ShardAcks>,
    /// Shared so that the run loop can tell there is nothing to flush without taking the
    /// lock, which is most iterations: only `PreAccept` and `Accept` produce an ack.
    pending_ack_count: Arc<AtomicUsize>,
    ack_recipients: BitSet,
}

pub struct ShardMultiSink {
    pub multi_sink: Arc<Mutex<MultiSink>>,
    pub shard_id: usize,
}

impl ShardMultiSink {
    pub async fn queue_fast_ack(&self, id: usize, deps: Option<DepSet>, requester: Option<usize>) {
        self.multi_sink
            .lock()
            .await
            .queue_fast_ack(self.shard_id, id, deps, requester);
    }

    pub async fn queue_slow_ack(&self, id: usize, requester: Option<usize>) {
        self.multi_sink
            .lock()
            .await
            .queue_slow_ack(self.shard_id, id, requester);
    }
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
    pub fn new(
        my_pid: usize,
        nb_nodes: usize,
        alive_replicas: BitSet,
        send_paxos_messages_to_all: bool,
    ) -> Self {
        Self {
            my_pid,
            nb_nodes,
            sinks: HashMap::with_capacity(nb_nodes - 1),
            alive_replicas,
            stats: Stats::default(),
            send_paxos_messages_to_all,
            pending_acks: HashMap::new(),
            pending_ack_count: Arc::new(AtomicUsize::new(0)),
            ack_recipients: BitSet::with_capacity(nb_nodes),
        }
    }

    /// Holds one ack back for [`MultiSink::flush_acks`]. `requester` is served even when it
    /// casts no vote, the way a `priority_broadcast` of a single ack would serve it.
    /// Holds one ack back for [`MultiSink::flush_acks`]. `requester` is served even when it
    /// casts no vote, the way a `priority_broadcast` of a single ack would serve it.
    fn ack_bucket(&mut self, shard: usize, requester: Option<usize>) -> &mut ShardAcks {
        self.pending_ack_count.fetch_add(1, Ordering::Relaxed);
        if let Some(requester) = requester {
            self.ack_recipients.insert(requester);
        }
        self.pending_acks.entry(shard).or_default()
    }

    pub fn queue_fast_ack(
        &mut self,
        shard: usize,
        id: usize,
        deps: Option<DepSet>,
        requester: Option<usize>,
    ) {
        self.ack_bucket(shard, requester).fast.push((id, deps));
    }

    pub fn queue_slow_ack(&mut self, shard: usize, id: usize, requester: Option<usize>) {
        self.ack_bucket(shard, requester).slow.push(id);
    }

    /// Sends everything queued as one message.
    ///
    /// Acks are replica business, so the bundle goes to the replicas plus whichever
    /// non-voting proposers are owed one of the acks it carries.
    pub async fn flush_acks(&mut self) -> io::Result<()> {
        if self.pending_ack_count.swap(0, Ordering::Relaxed) == 0 {
            return Ok(());
        }
        let shards: Vec<(usize, ShardAcks)> = self.pending_acks.drain().collect();
        let bytes = encode(&Message::DepAcks {
            src: self.my_pid,
            shards,
        });
        for (dest, sink) in self.sinks.iter_mut() {
            if !self.alive_replicas.contains(*dest) && !self.ack_recipients.contains(*dest) {
                continue;
            }
            self.stats.msg_count += 1;
            self.stats.byte_count += bytes.len();
            sink.send(bytes.clone()).await?;
        }
        self.ack_recipients.clear();
        Ok(())
    }

    /// Reads how many acks are waiting, without taking the lock.
    pub fn pending_acks_handle(&self) -> Arc<AtomicUsize> {
        self.pending_ack_count.clone()
    }

    pub fn insert_sink(&mut self, pid: usize, sink: WrappedSink) {
        assert!(pid < self.nb_nodes);
        assert_ne!(pid, self.my_pid);
        let out = self.sinks.insert(pid, sink);
        assert!(out.is_none(), "There should be no 2 sinks with same pid");
    }

    #[inline]
    pub async fn broadcast(&mut self, msg: Message, priority: Option<usize>) -> io::Result<()> {
        trace!("Broadcasting {msg:?}");
        let replica_only = self.should_only_send_to_replicas(&msg);
        let bytes = encode(&msg);
        if let Some(priority_dest) = priority
            && priority_dest != self.my_pid
        {
            if msg.is_consensus_msg() {
                self.stats.msg_count += 1;
                self.stats.byte_count += bytes.len();
            }

            // The priority destination is always served, and served first — even when the
            // message is otherwise replica-only. That is how a non-voting proposer gets the
            // answers it needs to decide for itself.
            self.sinks
                .get_mut(&priority_dest)
                .unwrap()
                .send(bytes.clone())
                .await?;
        }
        for (dest, sink) in self.sinks.iter_mut() {
            if replica_only && !self.alive_replicas.contains(*dest) {
                continue;
            }

            if Some(*dest) == priority {
                // Already sent
                continue;
            }

            if msg.is_consensus_msg() {
                self.stats.msg_count += 1;
                self.stats.byte_count += bytes.len();
            }

            sink.send(bytes.clone()).await?;
        }
        Ok(())
    }

    #[inline]
    pub async fn send(&mut self, msg: Message, dest: usize) -> io::Result<()> {
        trace!("Sending to {dest}: {msg:?}");
        debug_assert!(dest != self.my_pid);
        if self.should_only_send_to_replicas(&msg) && !self.alive_replicas.contains(dest) {
            return Ok(());
        }
        let bytes = encode(&msg);
        if msg.is_consensus_msg() {
            self.stats.msg_count += 1;
            self.stats.byte_count += bytes.len();
        }
        let sink = self.sinks.get_mut(&dest).unwrap();
        sink.send(bytes).await
    }

    fn should_only_send_to_replicas(&self, msg: &Message) -> bool {
        if let ConsensusM { msg, value, .. } = msg {
            match &msg.msg {
                ConsensusMsg::Commit { .. } => false,
                ConsensusMsg::ReadRequest { .. } => true,
                ConsensusMsg::ReadResponse { .. } => false,
                ConsensusMsg::KCensusM(msg) => match msg {
                    KCensusMsg::Spread { .. } => false,
                    KCensusMsg::SpreadValueOnly { .. } => false,
                    KCensusMsg::PaxosAccept { .. } => value.is_none(),
                },
                ConsensusMsg::PaxosM(_) => value.is_none() && !self.send_paxos_messages_to_all,
                ConsensusMsg::DepM(msg) => msg.replicas_only(),
            }
        } else {
            false
        }
    }
}

impl ShardMultiSink {
    #[inline]
    pub async fn priority_broadcast(
        &self,
        msg: ConsensusMsg,
        value: Option<CommandBatch>,
        last_v: Option<usize>,
        priority: Option<usize>,
    ) -> io::Result<()> {
        let mut multi_sink = self.multi_sink.lock().await;
        let msg = self.build_msg(msg, value, multi_sink.my_pid, last_v);
        multi_sink.broadcast(msg, priority).await
    }

    pub async fn broadcast(
        &self,
        msg: ConsensusMsg,
        value: Option<CommandBatch>,
        last_v: Option<usize>,
    ) -> io::Result<()> {
        self.priority_broadcast(msg, value, last_v, None).await
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
        ConsensusM {
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
