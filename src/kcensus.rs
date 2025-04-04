use crate::connector::DeSink;
use crate::kcensus::message::KCensusMsg::{Commit, Spread, SpreadValueOnly};
use crate::kcensus::message::{KCensusMsg, KCensusMsgWithSource};
use crate::kcensus::propagation::{MessageId, PropagationGraphs};
use crate::kcensus::round_state::RoundState;
use crate::message::Message::{Done, KCensusMessage};
use crate::message::{Message, MsgWithSource};
use crate::value::Request;
use futures::{SinkExt, StreamExt};
use log::{debug, info};
use std::collections::HashMap;
use std::io;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;

pub mod message;
pub mod node_state;
pub mod propagation;
mod round_state;

pub struct KCensus<St, Sk> {
    // Settings
    nb_nodes: usize,
    my_pid: usize,

    // Connections
    in_stream: St,
    out_sinks: HashMap<usize, Sk>,

    // Propagation graphs
    propagation_graphs: PropagationGraphs,

    // Overall state
    slot: usize,
    max_seen_slot: usize,
    round: usize,
    queued_messages: Vec<KCensusMsgWithSource>,
    values: HashMap<usize, Request>,

    round_state: RoundState,
}

pub struct NbNodes(pub usize);
pub struct Pid(pub usize);

impl KCensus<ReceiverStream<MsgWithSource>, DeSink> {
    pub fn new(
        nb_nodes: NbNodes,
        my_pid: Pid,
        in_stream: ReceiverStream<MsgWithSource>,
        out_sinks: HashMap<usize, DeSink>,
        propagation_graphs: PropagationGraphs,
    ) -> Self {
        let nb_nodes = nb_nodes.0;
        Self {
            nb_nodes,
            my_pid: my_pid.0,

            in_stream,
            out_sinks,

            propagation_graphs,

            slot: 0,
            max_seen_slot: 0,
            round: 0,
            queued_messages: Vec::with_capacity(nb_nodes),
            values: HashMap::with_capacity(nb_nodes),

            round_state: RoundState::new(nb_nodes, my_pid.0),
        }
    }

    pub async fn run(
        mut self,
        mut rx: Receiver<Option<Request>>,
        tx: Sender<Request>,
    ) -> io::Result<()> {
        self.goto_round(0);
        let mut count_done = 0usize;
        let mut done = false;

        'main_loop: loop {
            // Process queued messages (if possible)
            let mut i = 0usize;
            while i < self.queued_messages.len() {
                let msg = &self.queued_messages[i];
                if self.ready_to_process(&msg.msg, msg.src) {
                    let msg = self.queued_messages.remove(i);
                    match self.process_message(msg.msg, msg.src).await? {
                        Some(value) => {
                            tx.send(value).await.expect("Sending commited value");
                            continue 'main_loop; // Restart from the beginning of the queue
                        }
                        None => (),
                    }
                } else {
                    i += 1;
                }
            }

            if count_done == self.nb_nodes {
                break 'main_loop;
            }

            // TODO: (Optim.) peak connection first ?
            if self.round_state.get_my_v().is_none()
                && self.max_seen_slot == self.slot
                && !self.values.is_empty()
            {
                // TODO: Leader election / only leader should repropose !!!!!!!!!!!!!!!!!!
                let v_uid = *self.values.keys().min().unwrap();
                self.repropose_start(v_uid).await?;
            }

            // Read new messages and/or new local request
            let msg = select! {
                req = rx.recv(), if self.round_state.get_my_v().is_none()
                && !done && self.max_seen_slot == self.slot => {
                    match req.unwrap() {
                        Some(req) =>  {
                            debug_assert!(req.start_time.is_some());
                            let start_time = req.start_time.unwrap();
                            self.propose_start(req).await?;
                            debug!("local request started after {:?}", start_time.elapsed());
                        }
                        None => {
                            done = true;
                            self.inner_broadcast(Done).await?;
                            count_done += 1;
                            if count_done == self.nb_nodes {
                                break 'main_loop;
                            }
                        }
                    }
                    self.in_stream.next().await.unwrap()
                }
                opt_msg = self.in_stream.next() => opt_msg.unwrap(),
            };
            let src = msg.src;

            match msg.msg {
                KCensusMessage { msg, value } => {
                    if let Some(v) = value {
                        debug_assert!(msg.should_include_value());
                        let value_uid = msg.get_v(src);
                        let inserted = self.values.insert(value_uid, v.into_remote_req());
                        // TODO: Allow forwarding values ? (could the value already be there ?)
                        debug_assert!(inserted.is_none());
                    } else {
                        debug_assert!(!msg.should_include_value());
                    }

                    if !self.ready_to_process(&msg, src) {
                        self.max_seen_slot = self.max_seen_slot.max(msg.get_slot());
                        self.queued_messages.push(msg.with_source(src));
                        // TODO: recheck queued messages only if
                        //   "ready_to_process" might have changed
                        continue 'main_loop;
                    }

                    match self.process_message(msg, src).await? {
                        Some(value) => {
                            tx.send(value).await.expect("Sending commited value");
                            continue 'main_loop; // Restart from the beginning of the queue
                        }
                        None => (),
                    }
                }
                Done => count_done += 1,
                _ => panic!("Unexpected message type"),
            }
        } // 'main_loop: loop

        rx.close();
        self.in_stream.close();
        Ok(())
    }

    fn ready_to_process(&self, msg: &KCensusMsg, src: usize) -> bool {
        msg.get_slot() <= self.slot && self.values.contains_key(&msg.get_v(src))
    }

    async fn process_message(
        &mut self,
        msg: KCensusMsg,
        src: usize,
    ) -> io::Result<Option<Request>> {
        debug_assert!(self.ready_to_process(&msg, src));
        let msg_v_uid = msg.get_v(src);

        // TODO: Ignore some messages if max_seen_slot > slot ?
        // TODO: Handle dead nodes / packet loss ?
        match msg {
            Spread {
                slot,
                round,
                msg_id,
                remote_states,
                with_value,
            } => {
                if slot < self.slot || round < self.round {
                    if with_value {
                        let msg_id = msg_id.expect("Can't spread value without msg_id");
                        self.graph_spread_value_only(msg_id, msg_v_uid).await?;
                    }
                    return Ok(None);
                } else if round > self.round {
                    self.goto_round(round);
                }
                debug_assert_eq!(slot, self.slot);
                debug_assert_eq!(round, self.round);
                let old_proposer_count = self.round_state.proposers().len();

                let no_val_before = self.round_state.get_my_v().is_none();
                if no_val_before {
                    debug_assert!(old_proposer_count == 0);
                    debug_assert!(!self.round_state.am_i_frozen());
                    // TODO: pick most popular v_uid instead ?
                    self.round_state.set_my_v(msg_v_uid);
                }
                let my_v_uid = self.round_state.get_my_v().unwrap();

                let learned = self.round_state.learn_from(&remote_states);
                let proposer_count = self.round_state.proposers().len();

                if let Some(msg_id) = msg_id {
                    self.round_state.receive_msg(msg_id);
                }

                // Can commit ?
                // TODO: Make can_commit faster when using graph
                if self.round_state.i_am_proposer() && self.round_state.can_commit() {
                    debug_assert!(!with_value); // can't be my value -> there would be a conflict
                    let value_uid = self.round_state.get_my_v().unwrap();
                    self.broadcast(Commit { slot, value_uid }).await?;
                    let value = self.commit_slot(my_v_uid, false);
                    return Ok(Some(value));
                }

                let msg_frozen = remote_states[src].frozen;

                if msg_frozen || msg_v_uid != my_v_uid {
                    let orig_frozen = self.round_state.am_i_frozen();
                    self.round_state.freeze();

                    if with_value {
                        debug_assert!(!msg_frozen); // Can't spread value in frozen messages.
                        let msg_id = msg_id.expect("Can't spread value without msg_id");
                        self.graph_spread_value_only(msg_id, msg_v_uid).await?;
                    }

                    // TODO: only try to adopt if I'm the proposer with lowest id ?
                    if self.round_state.i_am_proposer() {
                        if let Some(adopted_v) = self.round_state.try_adopt() {
                            // Conflict resolved. Adopting...
                            self.goto_round(self.round + 1);
                            self.round_state.set_my_v(adopted_v); // Needed if we don't repropose
                            // TODO: only repropose if I was the proposer ?
                            //   (potentially need to broadcast adopt in that case ?)
                            self.repropose_start(adopted_v).await?;
                            return Ok(None);
                        }
                    }

                    if !orig_frozen {
                        if msg_frozen {
                            // Existing conflict. Spreading to proposers.
                            for i in 0..proposer_count {
                                self.spread_to(self.round_state.proposers()[i]).await?;
                            }
                        } else {
                            // New conflict. Freezing others...
                            self.spread_to_all().await?;
                        }
                    } else {
                        // Spread to new proposers only
                        for i in old_proposer_count..proposer_count {
                            self.spread_to(self.round_state.proposers()[i]).await?;
                        }
                    }
                    return Ok(None);
                }

                if proposer_count == 1 {
                    let msg_id = msg_id.expect("Single proposer means messages should have ids");
                    self.graph_spread(msg_id, with_value).await?;
                    return Ok(None);
                }

                // Multiple proposers of the same value
                debug_assert!(proposer_count > 1);
                debug_assert!(!with_value);

                if old_proposer_count < 2 {
                    // Transition to multi-proposer strategy
                    self.spread_to_all().await?;
                } else if learned || old_proposer_count < proposer_count {
                    let start_from = if learned { 0 } else { old_proposer_count };
                    // Share knowledge with proposers
                    for i in start_from..proposer_count {
                        let proposer = self.round_state.proposers()[i];
                        self.spread_to(proposer).await?;
                    }
                }
            }
            SpreadValueOnly { msg_id, value_uid } => {
                self.graph_spread_value_only(msg_id, value_uid).await?
            }
            Commit { slot, value_uid } => {
                if slot < self.slot {
                    return Ok(None);
                }
                info!("######## Commit msg: value_uid={}", value_uid);
                let value = self.commit_slot(value_uid, true);
                return Ok(Some(value));
            }
        } // match command
        Ok(None)
    } // fn process_message

    #[inline]
    fn commit_slot(&mut self, value_uid: usize, commit_msg: bool) -> Request {
        let value = self.values.remove(&value_uid).unwrap();
        if commit_msg {
            // "<#2FB82F>Commited \"{}\" in slot {}.</>"
            debug!("Commited \"{}\" in slot {}.", value.value.val, self.slot);
        } else {
            // "<#2FB82F>Commited \"{}\" in slot {} (round {}) from state:</> <#B8E8B8>{}</>"
            debug!(
                "Commited \"{}\" in slot {} (round {}) from state: {}",
                value.value.val, self.slot, self.round, self.round_state,
            );
        }
        self.slot += 1;
        self.max_seen_slot = self.max_seen_slot.max(self.slot);
        self.goto_round(0);
        value
    }

    #[inline]
    fn goto_round(&mut self, round: usize) {
        if round != 0 {
            debug!(
                // "<#FF4F4F>Can not commit in round {} from state:</> <#EFBFBF>{}</>"
                "Can not commit in round {} from state: {}",
                self.round, self.round_state,
            );
            if round > self.round + 1 {
                // "<yellow>######## Skipping round !!!!</>"
                info!("######## Skipping round !!!!");
            }
        }
        self.round = round;
        self.round_state.clear();
    }

    #[inline]
    async fn propose_start(&mut self, req: Request) -> io::Result<()> {
        let value_uid = self.my_pid + (self.slot * self.nb_nodes);
        self.values.insert(value_uid, req);

        self.inner_propose_start(value_uid, true).await
    }

    #[inline]
    async fn repropose_start(&mut self, value_uid: usize) -> io::Result<()> {
        self.inner_propose_start(value_uid, false).await
    }

    #[inline]
    async fn inner_propose_start(&mut self, value_uid: usize, with_value: bool) -> io::Result<()> {
        self.round_state.set_my_v(value_uid);
        self.round_state.become_proposer();

        self.start_spread(with_value).await
    }

    #[inline]
    async fn inner_broadcast(&mut self, msg: Message) -> io::Result<()> {
        for (_, sink) in self.out_sinks.iter_mut() {
            sink.send(msg.clone()).await?;
        }
        Ok(())
    }

    #[inline]
    async fn inner_send_to(&mut self, msg: Message, pid: usize) -> io::Result<()> {
        if pid == self.my_pid {
            return Ok(());
        }
        let sink = self.out_sinks.get_mut(&pid).unwrap();
        sink.send(msg.clone()).await
    }

    #[inline]
    async fn broadcast(&mut self, msg: KCensusMsg) -> io::Result<()> {
        self.inner_broadcast(KCensusMessage { msg, value: None })
            .await
    }

    #[inline]
    async fn send_to(&mut self, msg: KCensusMsg, pid: usize) -> io::Result<()> {
        let value = if msg.should_include_value() {
            let value_uid = msg.get_v(self.my_pid);
            Some(self.values[&value_uid].value.clone())
        } else {
            None
        };

        self.inner_send_to(KCensusMessage { msg, value }, pid).await
    }

    #[inline]
    async fn spread_to(&mut self, pid: usize) -> io::Result<()> {
        let msg = Spread {
            slot: self.slot,
            round: self.round,
            msg_id: None,
            remote_states: self.round_state.clone_node_states(),
            with_value: false,
        };
        self.send_to(msg, pid).await
    }

    #[inline]
    async fn spread_to_all(&mut self) -> io::Result<()> {
        let msg = Spread {
            slot: self.slot,
            round: self.round,
            msg_id: None,
            remote_states: self.round_state.clone_node_states(),
            with_value: false,
        };
        self.broadcast(msg).await
    }

    #[inline]
    async fn start_spread(&mut self, with_value: bool) -> io::Result<()> {
        let msg_count = self.propagation_graphs.get_start(self.my_pid).len();
        for m_i in 0..msg_count {
            // Reborrow
            let msg_id = self.propagation_graphs.get_start(self.my_pid)[m_i];
            debug_assert!(self.propagation_graphs.get_by_id(&msg_id).get_with_value());
            debug_assert!(
                self.propagation_graphs
                    .get_by_id(&msg_id)
                    .get_dependencies()
                    .is_empty()
            );

            let msg = Spread {
                slot: self.slot,
                round: self.round,
                msg_id: Some(msg_id),
                remote_states: self.round_state.clone_node_states(),
                with_value,
            };
            let dest = msg_id.dest;
            self.send_to(msg, dest).await?
        }
        Ok(())
    }

    #[inline]
    async fn graph_spread(&mut self, prev_msg_id: MessageId, with_value: bool) -> io::Result<()> {
        let prev_msg_info = self.propagation_graphs.get_by_id(&prev_msg_id);
        let msg_count = prev_msg_info.get_needed_by().len();
        for m_i in 0..msg_count {
            // Reborrow
            let prev_msg_info = self.propagation_graphs.get_by_id(&prev_msg_id);
            // Potential follow-up message:
            let msg_id = prev_msg_info.get_needed_by()[m_i];
            if msg_id.src != self.my_pid {
                continue;
            }
            let msg_info = self.propagation_graphs.get_by_id(&msg_id);

            if !self.round_state.can_send(msg_info.get_dependencies()) {
                debug_assert!(!msg_info.get_with_value());
                continue;
            }

            self.round_state.receive_msg(msg_id);

            let msg = Spread {
                slot: self.slot,
                round: self.round,
                msg_id: Some(msg_id),
                remote_states: self.round_state.clone_node_states(),
                with_value: with_value && msg_info.get_with_value(),
            };
            let dest = msg_id.dest;
            self.send_to(msg, dest).await?;
        }
        Ok(())
    }

    #[inline]
    async fn graph_spread_value_only(
        &mut self,
        prev_msg_id: MessageId,
        value_uid: usize,
    ) -> io::Result<()> {
        let prev_msg_info = self.propagation_graphs.get_by_id(&prev_msg_id);
        let msg_count = prev_msg_info.get_needed_by().len();
        for msg_i in 0..msg_count {
            // Reborrow
            let prev_msg_info = self.propagation_graphs.get_by_id(&prev_msg_id);
            // Potential follow-up message:
            let msg_id = prev_msg_info.get_needed_by()[msg_i];
            if msg_id.src != self.my_pid {
                continue;
            }
            let msg_info = self.propagation_graphs.get_by_id(&msg_id);

            self.round_state.receive_msg(msg_id);

            if !msg_info.get_with_value() {
                continue;
            }
            debug_assert!(self.round_state.can_send(msg_info.get_dependencies()));

            let dest = msg_id.dest;
            let msg = SpreadValueOnly { msg_id, value_uid };
            self.send_to(msg, dest).await?;
        }
        Ok(())
    }
}
