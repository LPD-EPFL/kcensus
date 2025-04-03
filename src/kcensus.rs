use crate::connector::DeSink;
use crate::kcensus::message::RoundCommand::{Commit, Spread, SpreadValueOnly};
use crate::kcensus::message::{KCensusMsg, KCensusMsgWithSource, RoundCommand};
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
            if self.round_state.get_my_v() == None
                && self.max_seen_slot == self.slot
                && !self.values.is_empty()
            {
                let v_uid = *self.values.keys().min().unwrap();
                self.repropose_start(v_uid).await?;
            }

            // Read new messages and/or new local request
            let msg = select! {
                req = rx.recv(), if self.round_state.get_my_v() == None
                && !done && self.max_seen_slot == self.slot => {
                    match req.unwrap() {
                        Some(req) =>  {
                            self.propose_start(req).await?;
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
                        if let Commit { .. } = &msg.command {
                            panic!("Values should never be sent in a commit message");
                        } else {
                            match &msg.command {
                                Spread {
                                    value_spreading: false,
                                    ..
                                } => panic!(
                                    "Values should never be sent in a spread message with value_included set to false"
                                ),
                                Spread { msg_id: None, .. } => panic!(
                                    "Values should never be sent in a spread message with no msg_id"
                                ),
                                _ => {}
                            }
                        }
                        let value_uid = msg.message_v_uid(src);
                        let inserted = self.values.insert(value_uid, v.into_remote_req());

                        // TODO: Allow forwarding values ? (could the value already be there ?)
                        debug_assert!(inserted.is_none());
                    }

                    if !self.ready_to_process(&msg, src) {
                        self.max_seen_slot = self.max_seen_slot.max(msg.slot);
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
        msg.slot <= self.slot && self.values.contains_key(&msg.message_v_uid(src))
    }

    async fn process_message(
        &mut self,
        msg: KCensusMsg,
        src: usize,
    ) -> io::Result<Option<Request>> {
        debug_assert!(self.ready_to_process(&msg, src));
        let slot = msg.slot;
        let round = msg.round;
        let command = msg.command;

        // TODO: Ignore some messages if max_seen_slot > slot ?
        // TODO: Handle dead nodes / packet loss ?
        match command {
            Spread {
                msg_id,
                remote_states,
                value_spreading,
            } => {
                let msg_v_uid = remote_states[src]
                    .v_uid
                    .expect("Can't spread without v_uid");
                if slot < self.slot || round < self.round {
                    if value_spreading {
                        let msg_id = msg_id.expect("Can't spread value without msg_id");
                        debug_assert_eq!(Some(msg_v_uid), remote_states[msg_id.proposer].v_uid);
                        self.spread_value_only_from(msg_id).await?;
                    }
                    return Ok(None);
                } else if round > self.round {
                    self.goto_round(round);
                }
                debug_assert_eq!(slot, self.slot);
                debug_assert_eq!(round, self.round);

                let no_val_before = self.round_state.get_my_v() == None;
                if no_val_before {
                    debug_assert!(!self.round_state.am_i_frozen());
                    self.round_state.set_my_v(msg_v_uid);
                }
                let my_v_uid = self.round_state.get_my_v().unwrap();

                let learned = self.round_state.learn_from(&remote_states, msg_v_uid, src);
                let new_prop = if let Some(msg_id) = msg_id {
                    self.round_state.receive_msg(msg_id);
                    self.round_state.new_proposer(msg_id.proposer)
                } else {
                    false
                };
                debug_assert!(!(no_val_before && self.round_state.multiple_proposers()));

                // Can commit ?
                if self.round_state.am_i_proposer() && self.round_state.can_commit() {
                    debug_assert!(!value_spreading);
                    let value_uid = self.round_state.get_my_v().unwrap();
                    self.broadcast_command(Commit { value_uid }).await?;
                    let value = self.commit_slot(my_v_uid, false);
                    return Ok(Some(value));
                }

                let msg_frozen = remote_states[src].frozen;

                if msg_frozen || msg_v_uid != my_v_uid {
                    let orig_frozen = self.round_state.am_i_frozen();
                    self.round_state.freeze();

                    if value_spreading {
                        let msg_id = msg_id.expect("Can't spread value without msg_id");
                        debug_assert_eq!(Some(msg_v_uid), remote_states[msg_id.proposer].v_uid);
                        self.spread_value_only_from(msg_id).await?;
                    }

                    if self.round_state.am_i_proposer() {
                        if let Some(adopted_v) = self.round_state.try_adopt() {
                            // Conflict resolved. Adopting...
                            self.goto_round(self.round + 1);
                            self.round_state.set_my_v(adopted_v); // Needed if we don't repropose
                            // TODO: only repropose if I was the proposer ?
                            self.repropose_start(adopted_v).await?;
                            return Ok(None);
                        }
                    }

                    if !orig_frozen {
                        if msg_frozen {
                            // Existing conflict. Spreading to proposers.
                            for i in 0..self.round_state.proposers().len() {
                                self.spread_to(self.round_state.proposers()[i]).await?;
                            }
                        } else {
                            // New conflict. Freezing others...
                            self.spread_to_all().await?;
                        }
                        return Ok(None);
                    }
                }

                if let Some(msg_id) = msg_id {
                    if new_prop && self.round_state.multiple_proposers() {
                        if value_spreading {
                            self.spread_value_only_from(msg_id).await?;
                        }
                        // Only share knowledge with new proposer
                        self.spread_to(msg_id.proposer).await?;
                    } else {
                        self.spread_from(msg_id, value_spreading).await?;
                    }
                } else if no_val_before {
                    panic!("Weird: new value from msg without id ?");
                } else if learned {
                    // Answer proposers only
                    for i in 0..self.round_state.proposers().len() {
                        self.spread_to(self.round_state.proposers()[i]).await?;
                    }
                }
            }
            SpreadValueOnly { msg_id, value_uid } => {
                self.round_state.set_v(src, value_uid);
                self.spread_value_only_from(msg_id).await?
            }
            Commit { value_uid } => {
                info!("######## Commit msg (from round {}):", round);
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
        let new = self.round_state.new_proposer(self.my_pid);
        debug_assert!(new);

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
        debug_assert_ne!(pid, self.my_pid);
        let sink = self.out_sinks.get_mut(&pid).unwrap();
        sink.send(msg.clone()).await
    }

    #[inline]
    async fn broadcast_command(&mut self, command: RoundCommand) -> io::Result<()> {
        self.inner_broadcast(KCensusMessage {
            msg: KCensusMsg {
                slot: self.slot,
                round: self.round,
                command: command.clone(),
            },
            value: None,
        })
        .await
    }

    #[inline]
    async fn send_command_to(&mut self, command: RoundCommand, pid: usize) -> io::Result<()> {
        let value = match &command {
            Spread {
                value_spreading: true,
                ..
            } => {
                let value_uid = self.round_state.get_my_v().unwrap();
                Some(self.values[&value_uid].value.clone())
            }
            SpreadValueOnly { value_uid, .. } => Some(self.values[value_uid].value.clone()),
            _ => None,
        };

        self.inner_send_to(
            KCensusMessage {
                msg: KCensusMsg {
                    slot: self.slot,
                    round: self.round,
                    command: command.clone(),
                },
                value,
            },
            pid,
        )
        .await
    }

    #[inline]
    async fn spread_to(&mut self, pid: usize) -> io::Result<()> {
        let cmd = Spread {
            msg_id: None,
            remote_states: self.round_state.get_node_states().clone(),
            value_spreading: false,
        };
        self.send_command_to(cmd, pid).await
    }

    #[inline]
    async fn spread_to_all(&mut self) -> io::Result<()> {
        let cmd = Spread {
            msg_id: None,
            remote_states: self.round_state.get_node_states().clone(),
            value_spreading: false,
        };
        self.broadcast_command(cmd).await
    }

    #[inline]
    async fn start_spread(&mut self, value_spreading: bool) -> io::Result<()> {
        let (_, msg_info) = self.propagation_graphs.get_start(self.my_pid);
        for i in 0..msg_info.get_destinations().len() {
            // Reborrow
            let (msg_id, msg_info) = self.propagation_graphs.get_start(self.my_pid);
            let dest = msg_info.get_destinations()[i];
            debug_assert!(msg_info.get_with_value()[i]);
            let cmd = Spread {
                msg_id: Some(msg_id),
                remote_states: self.round_state.get_node_states().clone(),
                value_spreading,
            };
            self.send_command_to(cmd, dest).await?
        }
        Ok(())
    }

    #[inline]
    async fn spread_from(
        &mut self,
        prev_msg_id: MessageId,
        value_spreading: bool,
    ) -> io::Result<()> {
        let prev_msg_info = self.propagation_graphs.get_by_id(&prev_msg_id);
        for m in 0..prev_msg_info.follow_up_messages().len() {
            // Reborrow
            let prev_msg_info = self.propagation_graphs.get_by_id(&prev_msg_id);
            let msg_id = prev_msg_info.follow_up_messages()[m];
            if msg_id.src != self.my_pid {
                continue;
            }
            let msg_info = self.propagation_graphs.get_by_id(&msg_id);

            if !self.round_state.can_send(msg_info.get_dependencies()) {
                debug_assert!(!msg_info.get_with_value().contains(&true));
                continue;
            }

            self.round_state.receive_msg(msg_id);
            for i in 0..msg_info.get_destinations().len() {
                // Reborrow
                let msg_info = self.propagation_graphs.get_by_id(&msg_id);
                let dest = msg_info.get_destinations()[i];
                let cmd = Spread {
                    msg_id: Some(msg_id),
                    remote_states: self.round_state.get_node_states().clone(),
                    value_spreading: value_spreading && msg_info.get_with_value()[i],
                };
                self.send_command_to(cmd, dest).await?;
            }
        }
        Ok(())
    }

    #[inline]
    async fn spread_value_only_from(&mut self, prev_msg_id: MessageId) -> io::Result<()> {
        let value_uid = self
            .round_state
            .get_v(prev_msg_id.proposer)
            .expect("Should have received value from proposer when spreading value");
        let prev_msg_info = self.propagation_graphs.get_by_id(&prev_msg_id);
        for msg_i in 0..prev_msg_info.follow_up_messages().len() {
            // Reborrow
            let prev_msg_info = self.propagation_graphs.get_by_id(&prev_msg_id);
            let msg_id = prev_msg_info.follow_up_messages()[msg_i];
            if msg_id.src != self.my_pid {
                continue;
            }
            let msg_info = self.propagation_graphs.get_by_id(&msg_id);

            self.round_state.receive_msg(msg_id);

            for dest_i in 0..msg_info.get_destinations().len() {
                // Reborrow
                let msg_info = self.propagation_graphs.get_by_id(&msg_id);
                let dest = msg_info.get_destinations()[dest_i];
                if !msg_info.get_with_value()[dest_i] {
                    continue;
                }
                debug_assert!(self.round_state.can_send(msg_info.get_dependencies()));

                let cmd = SpreadValueOnly { msg_id, value_uid };
                self.send_command_to(cmd, dest).await?;
            }
        }
        Ok(())
    }
}
