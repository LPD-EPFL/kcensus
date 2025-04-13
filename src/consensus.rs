use crate::consensus::message::ConsensusMsg::{Commit, ReadRequest, ReadResponse};
use crate::consensus::message::{CommandBatch, ConsensusMessage};
use crate::consensus::read_tracker::ReadTracker;
use crate::eval;
use crate::message::Message::{ConsensusM, Done};
use crate::message::MsgWithSource;
use crate::multi_sink::MultiSink;
use command::Command;
use log::{debug, info};
use std::collections::{HashMap, VecDeque};
use std::io;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};

pub(crate) mod command;
pub mod kcensus;
pub(crate) mod message;
pub(crate) mod paxos_family;
mod read_tracker;

pub(crate) trait Consensus {
    async fn run(
        &mut self,
        mut msg_rx: Receiver<MsgWithSource>,
        mut new_client_commands_rx: Receiver<Command>,
        committed_commands_tx: Sender<Command>,
    ) -> io::Result<()> {
        let mut queued_messages: VecDeque<ConsensusMessage> =
            VecDeque::with_capacity(self.get_nb_nodes());
        let mut count_done = 0usize;
        let mut done = false;
        let mut max_queued_slot = 0;

        'main_loop: loop {
            // Process queued messages (if possible)
            let mut i = 0usize;
            while i < queued_messages.len() {
                let msg = &queued_messages[i];
                if self.ready_to_process(msg) {
                    let msg = queued_messages.remove(i).unwrap();
                    let batch = self.full_process_message(msg).await?;
                    if self.commit_commands(&committed_commands_tx, batch).await {
                        continue 'main_loop; // Restart from the beginning of the queue
                    }
                } else {
                    i += 1;
                }
            }

            if count_done == self.get_nb_nodes() {
                let sinks = self.get_sinks();
                eval::log(
                    "network-done",
                    &format!(
                        "Sent {} messages ({} bytes)",
                        sinks.stats.msg_count, sinks.stats.byte_count
                    ),
                    &sinks.stats,
                );
                break 'main_loop;
            }

            let ongoing = self.get_my_v().is_some() || max_queued_slot > self.get_slot();
            let should_repropose = !ongoing && self.has_queued_commands();
            // TODO: (Optim.) peak connection first ?
            if should_repropose && self.should_lead() {
                if let Some(batch) = self.get_new_batch_to_propose() {
                    self.propose_start(batch, false).await?;
                } else {
                    let v = self.get_v_to_repropose();
                    self.repropose_start(v).await?;
                }
            }

            // Read new messages and/or new local command
            let msg = select! {
                command = new_client_commands_rx.recv(), if (!ongoing || self.can_forward_proposals()) && !should_repropose && !done => {
                    match command {
                        Some(command) =>  {
                            if command.read_only {
                                self.start_read(command).await?;
                            } else {
                                self.propose_start(CommandBatch::Single(command), ongoing).await?;
                            }
                        }
                        None => {
                            done = true;
                            self.get_sinks().inner_broadcast(Done).await?;
                            count_done += 1;
                            if count_done == self.get_nb_nodes() {
                                break 'main_loop;
                            }
                        }
                    }
                    msg_rx.recv().await.unwrap()
                }
                opt_msg = msg_rx.recv() => opt_msg.unwrap(),
            };

            match msg.msg {
                ConsensusM { msg, value } => {
                    if let Some(value) = value {
                        debug_assert!(msg.can_include_value());
                        let v = msg.get_v().expect("A value should travel with its uid");
                        self.store_remote_command(v, value);
                    } else {
                        debug_assert!(!msg.should_include_value());
                    }

                    if !self.ready_to_process(&msg) {
                        max_queued_slot = max_queued_slot.max(msg.get_slot());
                        queued_messages.push_back(msg);
                        // TODO: recheck queued messages only if
                        //   "ready_to_process" might have changed
                        continue 'main_loop;
                    }

                    let res_command = self.full_process_message(msg).await?;
                    if self
                        .commit_commands(&committed_commands_tx, res_command)
                        .await
                    {
                        continue 'main_loop; // Restart from the beginning of the queue
                    }
                }
                Done => count_done += 1,
                _ => panic!("Unexpected message type"),
            }
        } // 'main_loop: loop

        new_client_commands_rx.close();
        msg_rx.close();
        Ok(())
    } // run

    #[inline]
    async fn start_read(&mut self, command: Command) -> io::Result<()> {
        let local_ready = self.get_my_v().is_none();
        let uid = self.get_read_tracker().insert(command, local_ready);
        self.get_sinks().broadcast(ReadRequest { uid }, None).await
    }

    #[inline]
    fn ready_to_process(&self, msg: &ConsensusMessage) -> bool {
        if msg.get_slot() > self.get_slot() {
            false
        } else {
            match msg.get_v() {
                None => true,
                Some(v) => match self.get_queued_commands().get(&v) {
                    None => false,
                    Some(CommandBatch::Single(_)) => true,
                    Some(CommandBatch::Batch(vs)) => vs
                        .iter()
                        .all(|v| self.get_queued_commands().contains_key(v)),
                },
            }
        }
    }

    async fn full_process_message(
        &mut self,
        msg: ConsensusMessage,
    ) -> io::Result<Option<CommandBatch>> {
        debug_assert!(self.ready_to_process(&msg));
        if let Commit { slot, v } = msg.msg {
            if slot < self.get_slot() {
                return Ok(None);
            }
            debug_assert_eq!(slot, self.get_slot());
            info!("Commit msg: v={}", v);
            return Ok(Some(self.commit_slot(v, true)));
        };
        if let ReadRequest { uid } = msg.msg {
            let next_readable_slot = self.get_slot() + self.get_my_v().is_some() as usize;
            self.get_sinks()
                .send(
                    ReadResponse {
                        uid,
                        next_readable_slot,
                    },
                    None,
                    msg.src,
                )
                .await?;
            return Ok(None);
        }
        if let ReadResponse { uid, .. } = msg.msg {
            return Ok(self
                .get_read_tracker()
                .receive_ready(uid)
                .map(|c| CommandBatch::Single(c)));
        }
        debug!("Processing message: {:?}", msg);
        self.process_message(msg).await
    }

    #[inline]
    async fn commit_commands(
        &mut self,
        committed_commands_tx: &Sender<Command>,
        batch: Option<CommandBatch>,
    ) -> bool {
        if let Some(batch) = batch {
            match batch {
                CommandBatch::Single(command) => {
                    commit_command(committed_commands_tx, command).await;
                }
                CommandBatch::Batch(vs) => {
                    for v in vs {
                        let command = self.remove_command(v);
                        if let CommandBatch::Single(command) = command {
                            commit_command(committed_commands_tx, command).await;
                        } else {
                            panic!("Batches should not include batches.")
                        }
                    }
                }
            }

            let result = self.get_read_tracker().commit_slot();
            for read_only_command in result.into_iter() {
                commit_command(committed_commands_tx, read_only_command).await;
            }

            true
        } else {
            false
        }
    }

    async fn process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<CommandBatch>>;

    fn can_forward_proposals(&mut self) -> bool;

    async fn propose_start(&mut self, value: CommandBatch, contention: bool) -> io::Result<()>;

    async fn repropose_start(&mut self, v: usize) -> io::Result<()>;

    fn commit_slot(&mut self, v: usize, from_commit_msg: bool) -> CommandBatch;

    fn get_nb_nodes(&self) -> usize;

    fn get_slot(&self) -> usize;

    fn get_my_v(&self) -> Option<usize>;

    fn should_lead(&self) -> bool;

    fn get_next_uid(&mut self) -> usize;

    #[inline]
    fn store_new_command(&mut self, value: CommandBatch) -> usize {
        let v = self.get_next_uid();
        let old = self.get_queued_commands_mut().insert(v, value);
        debug_assert!(old.is_none());
        v
    }

    #[inline]
    fn store_remote_command(&mut self, v: usize, value: CommandBatch) {
        // TODO: Allow forwarding values ? (could the value already be there ?)
        let inserted = self.get_queued_commands_mut().insert(v, value);
        debug_assert!(inserted.is_none());
    }

    #[inline]
    fn has_queued_commands(&self) -> bool {
        !self.get_queued_commands().is_empty()
    }

    fn get_new_batch_to_propose(&self) -> Option<CommandBatch> {
        if self.get_queued_commands().is_empty() {
            return None;
        }
        let mut vs: Vec<_> = self.get_queued_commands().keys().copied().collect();
        vs.retain(|v| matches!(self.get_queued_commands()[v], CommandBatch::Single(_)));
        Some(CommandBatch::Batch(vs))
    }

    fn get_v_to_repropose(&self) -> usize;

    fn get_queued_commands(&self) -> &HashMap<usize, CommandBatch>;

    fn get_queued_commands_mut(&mut self) -> &mut HashMap<usize, CommandBatch>;

    fn remove_command(&mut self, v: usize) -> CommandBatch {
        let out = self.get_queued_commands_mut().remove(&v);
        self.get_queued_commands_mut()
            .retain(|_, value| match value {
                CommandBatch::Single(_) => true,
                CommandBatch::Batch(vs) => !vs.contains(&v),
            });
        out.expect("Removing command that does not exist")
    }

    fn get_read_tracker(&mut self) -> &mut ReadTracker;

    fn get_sinks(&mut self) -> &mut MultiSink;
}

async fn commit_command(committed_commands_tx: &Sender<Command>, command: Command) {
    committed_commands_tx
        .send(command)
        .await
        .expect("Sending commited value");
}
