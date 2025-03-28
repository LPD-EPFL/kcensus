use crate::connector::DeSink;
use crate::message::Message::{Done, KCensusMessage};
use crate::message::RoundCommand::{Commit, Spread};
use crate::message::{KCensusMsg, KCensusMsgWithSource, Message, MsgWithSource, RoundCommand};
use crate::round_state::RoundState;
pub(crate) use crate::value::{KVal, Request};
use futures::{SinkExt, StreamExt};
use log::{debug, info};
use std::collections::HashMap;
use std::io;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;

pub struct KCensus<St, Sk> {
    // Settings
    nb_nodes: usize,
    my_pid: usize,

    // Connections
    in_stream: St,
    out_sinks: HashMap<usize, Sk>,

    // Overall state
    slot: usize,
    max_seen_slot: usize,
    round: usize,
    step: usize,
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
    ) -> Self {
        let nb_nodes = nb_nodes.0;
        Self {
            nb_nodes,
            my_pid: my_pid.0,

            in_stream,
            out_sinks,

            slot: 0,
            max_seen_slot: 0,
            round: 0,
            step: 0,
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
                if self.ready_to_process(&msg.msg) {
                    let msg = self.queued_messages.remove(i);
                    match self.process_message(msg.msg, msg.src).await? {
                        Some(value) => {
                            tx.send(value).await.expect("Sending value");
                            continue 'main_loop; // Restart from the beginning of the queue
                        }
                        None => (),
                    }
                    continue; // Don't increment i here !
                }
                i += 1;
            }

            if count_done == self.nb_nodes {
                break 'main_loop;
            }

            // TODO: (Optim.) peak connection first ?
            if self.round_state.get_my_v() == None
                && self.max_seen_slot == self.slot
                && !self.values.is_empty()
            {
                let v_uid = self.values.keys().min().unwrap();
                self.repropose_start(*v_uid).await?;
            }

            // Read new messages
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
                        let inserted = self.values.insert(msg.value_uid, v.into_remote_req());
                        // TODO: Allow forwarding values ? (could the value already be there ?)
                        debug_assert!(inserted.is_none());
                    }

                    if !self.ready_to_process(&msg) {
                        self.max_seen_slot = self.max_seen_slot.max(msg.slot);
                        self.queued_messages.push(msg.with_source(src));
                        // TODO: recheck queued messages only if
                        //   "ready_to_process" might have changed
                        continue 'main_loop;
                    }

                    match self.process_message(msg, src).await? {
                        Some(value) => {
                            tx.send(value).await.expect("Sending value");
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

    fn ready_to_process(&self, msg: &KCensusMsg) -> bool {
        msg.slot == self.slot && self.values.contains_key(&msg.value_uid)
    }

    #[inline]
    async fn inner_broadcast(&mut self, msg: Message) -> io::Result<()> {
        for (_, sink) in self.out_sinks.iter_mut() {
            sink.send(msg.clone()).await?;
        }
        Ok(())
    }

    #[inline]
    async fn inner_round_broadcast(
        &mut self,
        command: RoundCommand,
        value: Option<KVal>,
    ) -> io::Result<()> {
        self.inner_broadcast(KCensusMessage {
            msg: KCensusMsg {
                slot: self.slot,
                round: self.round,
                value_uid: self.round_state.get_my_v().unwrap(),
                command: command.clone(),
            },
            value: value.clone(),
        })
        .await
    }

    #[inline]
    async fn spread_with_value(&mut self, value: &KVal) -> io::Result<()> {
        let remote_states = self.round_state.get_node_states().clone();
        self.inner_round_broadcast(
            Spread {
                remote_states,
                step: 0,
            },
            Some(value.clone()),
        )
        .await
    }

    #[inline]
    async fn round_broadcast(&mut self, command: RoundCommand) -> io::Result<()> {
        self.inner_round_broadcast(command, None).await
    }

    #[inline]
    async fn spread(&mut self) -> io::Result<()> {
        let remote_states = self.round_state.get_node_states().clone();
        let step = self.step + 1;
        if step > 2 {
            return Ok(());
        }
        let cmd = Spread {
            remote_states,
            step,
        };
        if step == 2 {
            let val_uid = self.round_state.get_my_v().unwrap();
            let proposer = self.values.get(&val_uid).unwrap().value.proposer;
            if proposer == self.my_pid {
                return Ok(());
            }
            self.out_sinks
                .get_mut(&proposer)
                .unwrap()
                .send(KCensusMessage {
                    msg: KCensusMsg {
                        slot: self.slot,
                        round: self.round,
                        value_uid: val_uid,
                        command: cmd,
                    },
                    value: None,
                })
                .await
        } else {
            self.round_broadcast(cmd).await
        }
    }

    async fn process_message(
        &mut self,
        msg: KCensusMsg,
        src: usize,
    ) -> io::Result<Option<Request>> {
        debug_assert!(self.ready_to_process(&msg)); // Redundant with the following asserts...
        let slot = msg.slot;
        let round = msg.round;
        let msg_v_uid = msg.value_uid;
        let command = msg.command;
        debug_assert!(self.values.contains_key(&msg_v_uid));
        debug_assert_eq!(slot, self.slot);

        // TODO: Ignore some messages if max_seen_slot > slot ?
        // TODO: Handle dead nodes / packet loss ?
        match command {
            Spread {
                remote_states,
                step,
            } => {
                if round < self.round {
                    return Ok(None);
                } else if round > self.round {
                    self.goto_round(round);
                }
                if self.step < step {
                    self.step = step;
                }
                debug_assert!(round == self.round);

                if self.round_state.get_my_v() == None {
                    debug_assert!(!self.round_state.am_i_frozen());
                    self.round_state.set_my_v(msg_v_uid);
                }
                let my_v_uid = self.round_state.get_my_v().unwrap();

                let learned = self.round_state.learn_from(&remote_states, msg_v_uid, src);

                // Can commit ?
                if self.round_state.can_commit() {
                    self.round_broadcast(Commit).await?;
                    let value = self.commit_slot(my_v_uid, false);
                    return Ok(Some(value));
                }

                let msg_frozen = remote_states[src].frozen;

                if msg_frozen || msg_v_uid != my_v_uid {
                    let orig_frozen = self.round_state.am_i_frozen();
                    self.round_state.freeze();

                    if let Some(adopted_v) = self.round_state.try_adopt() {
                        // Conflict resolved. Adopting...
                        self.goto_round(self.round + 1);
                        self.round_state.set_my_v(adopted_v);
                        self.spread().await?;
                        return Ok(None);
                    }

                    if !orig_frozen {
                        // New conflict. Freezing others...
                        self.spread().await?;
                        return Ok(None);
                    }
                }

                if learned {
                    self.spread().await?;
                }
            }
            Commit => {
                info!("######## Commit msg (from round {}):", round);
                let value = self.commit_slot(msg_v_uid, true);
                return Ok(Some(value));
            }
        } // match command
        Ok(None)
    } // fn process_message

    #[inline]
    async fn propose_start(&mut self, req: Request) -> io::Result<()> {
        let value_uid = self.my_pid + (self.slot * self.nb_nodes);
        self.round_state.set_my_v(value_uid);

        self.spread_with_value(&req.value).await?;

        self.values.insert(value_uid, req);

        Ok(())
    }

    async fn repropose_start(&mut self, value_uid: usize) -> io::Result<()> {
        self.round_state.set_my_v(value_uid);
        self.spread().await
    }

    #[inline]
    fn commit_slot(&mut self, value_uid: usize, commit_msg: bool) -> Request {
        let value = self.values.remove(&value_uid).unwrap();
        if commit_msg {
            // "<#2FB82F>Commited \"{}\" in slot {}.</>"
            debug!("Commited \"{}\" in slot {}.", value.value.val, self.slot);
        } else {
            debug!(
                // "<#2FB82F>Commited \"{}\" in slot {} (round {}) from state:</> <#B8E8B8>{}</>"
                "Commited \"{}\" in slot {} (round {}) from state: {}",
                value.value.val, self.slot, self.round, self.round_state,
            );
        }
        self.slot += 1;
        self.max_seen_slot = self.max_seen_slot.max(self.slot);
        self.goto_round(0);
        self.queued_messages.retain(|msg| msg.msg.slot >= self.slot);
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
        self.step = 0;
        self.round_state.clear();
    }
}
