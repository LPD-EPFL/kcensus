use crate::connector::WrappedSink;
use crate::consensus::deps::message::DepMsg;
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
    /// SwiftPaxos acks formed but not sent yet, each with its shard: `PreAcceptOk` and the
    /// leader's `Accept` (the reference's `MFastAck`), and `AcceptOk` (its `MLightSlowAck`).
    pending_fast: Vec<(usize, DepMsg)>,
    pending_slow: Vec<(usize, DepMsg)>,
    /// How many of each are pending, readable without taking the lock.
    pending_counts: Arc<PendingAcks>,
    /// The non-voting proposers owed one of the pending acks.
    ack_recipients: BitSet,
}

/// How many acks of one kind a bundle holds when it is sent. Each of the reference batcher's
/// channels holds 16 (`NewBatcher(r, 16, ..)`) and its replica blocks on the 17th; once the
/// batcher takes one, that 17th moves into the channel before the batcher counts what is
/// left, so the bundle carries all 17.
pub const ACK_BATCH_LIMIT: usize = 17;

/// How many acks of each kind a [`MultiSink`] is holding.
#[derive(Default)]
pub struct PendingAcks {
    fast: AtomicUsize,
    slow: AtomicUsize,
}

impl PendingAcks {
    pub fn any(&self) -> bool {
        self.fast.load(Ordering::Relaxed) > 0 || self.slow.load(Ordering::Relaxed) > 0
    }

    /// How many more acks guarantee that the pending bundle is sent, however they split
    /// between the two kinds: one fewer could leave both a single ack short of
    /// [`ACK_BATCH_LIMIT`]. With nothing pending, this is the largest bundle there is.
    pub fn room(&self) -> usize {
        let fast = ACK_BATCH_LIMIT - self.fast.load(Ordering::Relaxed);
        let slow = ACK_BATCH_LIMIT - self.slow.load(Ordering::Relaxed);
        fast + slow - 1
    }
}

pub struct ShardMultiSink {
    pub multi_sink: Arc<Mutex<MultiSink>>,
    pub shard_id: usize,
}

impl ShardMultiSink {
    pub async fn queue_ack(&self, msg: DepMsg, requester: Option<usize>) -> io::Result<()> {
        self.multi_sink
            .lock()
            .await
            .queue_ack(self.shard_id, msg, requester)
            .await
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
            pending_fast: Vec::new(),
            pending_slow: Vec::new(),
            pending_counts: Arc::default(),
            ack_recipients: BitSet::with_capacity(nb_nodes),
        }
    }

    /// Holds one ack back for [`MultiSink::flush_acks`], and sends the bundle once it holds
    /// [`ACK_BATCH_LIMIT`] acks of this kind. `requester` is served even when it casts no
    /// vote, the way a `priority_broadcast` of a single ack would serve it.
    pub async fn queue_ack(
        &mut self,
        shard: usize,
        msg: DepMsg,
        requester: Option<usize>,
    ) -> io::Result<()> {
        let (queue, count) = match msg {
            DepMsg::PreAcceptOk { .. } | DepMsg::Accept { .. } => {
                (&mut self.pending_fast, &self.pending_counts.fast)
            }
            DepMsg::AcceptOk { .. } => (&mut self.pending_slow, &self.pending_counts.slow),
            ref other => panic!("not an ack: {other:?}"),
        };
        queue.push((shard, msg));
        let full = count.fetch_add(1, Ordering::Relaxed) + 1 >= ACK_BATCH_LIMIT;
        if let Some(requester) = requester {
            self.ack_recipients.insert(requester);
        }
        if full {
            self.flush_acks().await?;
        }
        Ok(())
    }

    /// Reads how many acks are pending, without taking the lock.
    pub fn pending_acks_handle(&self) -> Arc<PendingAcks> {
        self.pending_counts.clone()
    }

    /// Sends every pending ack as one bundle, fast acks first: the order the receiver
    /// applies an `MAcks` in (`swift.go:307`).
    pub async fn flush_acks(&mut self) -> io::Result<()> {
        if !self.pending_counts.any() {
            return Ok(());
        }
        self.pending_counts.fast.store(0, Ordering::Relaxed);
        self.pending_counts.slow.store(0, Ordering::Relaxed);
        let mut acks = std::mem::take(&mut self.pending_fast);
        acks.append(&mut self.pending_slow);
        let recipients = std::mem::take(&mut self.ack_recipients);
        let result = self.send_acks(acks, &recipients).await;
        self.ack_recipients = recipients;
        self.ack_recipients.clear();
        result
    }

    /// Sends one bundle of acks.
    ///
    /// Acks are replica business, so the bundle goes to the replicas plus whichever
    /// non-voting proposers in `requesters` are owed one of the acks it carries.
    async fn send_acks(
        &mut self,
        acks: Vec<(usize, DepMsg)>,
        requesters: &BitSet,
    ) -> io::Result<()> {
        let bundle = Message::DepAcks {
            src: self.my_pid,
            acks,
        };
        let bytes = encode(&bundle);
        if let Message::DepAcks { mut acks, .. } = bundle {
            acks.clear();
            self.pending_fast = acks;
        }
        for (dest, sink) in self.sinks.iter_mut() {
            if !self.alive_replicas.contains(*dest) && !requesters.contains(*dest) {
                continue;
            }
            self.stats.msg_count += 1;
            self.stats.byte_count += bytes.len();
            sink.send(bytes.clone()).await?;
        }
        Ok(())
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
