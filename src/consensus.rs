use crate::consensus::message::ConsensusMessage;
use crate::consensus::message::ConsensusMsg::Commit;
use crate::message::Message::{ConsensusM, Done};
use crate::message::MsgWithSource;
use command::Command;
use log::{debug, info};
use std::collections::VecDeque;
use std::io;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};

pub mod command;
pub mod kcensus;
pub mod message;
pub mod paxos_family;

pub trait Consensus {
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
                    let result = self.full_process_message(msg).await?;
                    if let Some(command) = result {
                        committed_commands_tx
                            .send(command)
                            .await
                            .expect("Sending commited value");
                        continue 'main_loop; // Restart from the beginning of the queue
                    }
                } else {
                    i += 1;
                }
            }

            if count_done == self.get_nb_nodes() {
                break 'main_loop;
            }

            let ongoing = self.get_my_v().is_some() || max_queued_slot > self.get_slot();
            let should_repropose = !ongoing && self.has_queued_commands();
            // TODO: (Optim.) peak connection first ?
            if should_repropose && self.should_lead() {
                // TODO: Leader election / only leader should repropose !!!!!!!!!!!!!!!!!!
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
                            self.propose_start(command, ongoing).await?;
                        }
                        None => {
                            done = true;
                            self.announce_done().await?;
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

                    let result = self.full_process_message(msg).await?;
                    if let Some(value) = result {
                        committed_commands_tx
                            .send(value)
                            .await
                            .expect("Sending commited value");
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
    fn ready_to_process(&self, msg: &ConsensusMessage) -> bool {
        msg.get_slot() <= self.get_slot() && msg.get_v().iter().all(|v| self.knows_v(*v))
    }

    async fn full_process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<Command>> {
        debug_assert!(self.ready_to_process(&msg));
        if let Commit { slot, v } = msg.msg {
            if slot < self.get_slot() {
                return Ok(None);
            }
            debug_assert_eq!(slot, self.get_slot());
            info!("Commit msg: v={}", v);
            let value = self.commit_slot(v, true);
            return Ok(Some(value));
        };
        debug!("Processing message: {:?}", msg);
        self.process_message(msg).await
    }

    async fn process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<Command>>;

    fn can_forward_proposals(&mut self) -> bool;

    async fn propose_start(&mut self, command: Command, contention: bool) -> io::Result<()>;

    async fn repropose_start(&mut self, v: usize) -> io::Result<()>;

    fn commit_slot(&mut self, v: usize, from_commit_msg: bool) -> Command;

    fn get_nb_nodes(&self) -> usize;

    fn get_slot(&self) -> usize;

    fn get_my_v(&self) -> Option<usize>;

    fn should_lead(&self) -> bool;

    async fn announce_done(&mut self) -> io::Result<()>;

    fn store_new_command(&mut self, command: Command) -> usize;

    fn store_remote_command(&mut self, v: usize, command: Command);

    fn knows_v(&self, v: usize) -> bool;

    fn has_queued_commands(&self) -> bool;

    fn get_new_batch_to_propose(&self) -> Option<Command>;

    fn get_v_to_repropose(&self) -> usize;
}
