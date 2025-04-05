use crate::consensus::message::ConsensusMessage;
use crate::message::Message::{ConsensusM, Done};
use crate::message::{Message, MsgWithSource};
use crate::value::{KVal, Request};
use log::debug;
use std::collections::VecDeque;
use std::io;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};

pub mod message;

pub trait Consensus {
    async fn run(
        &mut self,
        mut msg_rx: Receiver<MsgWithSource>,
        mut req_rx: Receiver<Option<Request>>,
        resp_tx: Sender<Request>,
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
                    let result = self.process_message(msg).await?;
                    if let Some(value) = result {
                        resp_tx.send(value).await.expect("Sending commited value");
                        continue 'main_loop; // Restart from the beginning of the queue
                    }
                } else {
                    i += 1;
                }
            }

            if count_done == self.get_nb_nodes() {
                break 'main_loop;
            }

            let nothing_ongoing = self.get_my_v().is_none() && max_queued_slot <= self.get_slot();
            let should_repropose = nothing_ongoing && self.has_queued_values();
            // TODO: (Optim.) peak connection first ?
            if should_repropose && self.should_lead() {
                // TODO: Leader election / only leader should repropose !!!!!!!!!!!!!!!!!!
                let (v_uid, opt_value) = self.get_value_to_propose();
                self.repropose_start(v_uid, opt_value).await?;
            }

            // Read new messages and/or new local request
            let msg = select! {
                req = req_rx.recv(), if nothing_ongoing && !should_repropose && !done => {
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
                    if let Some(v) = value {
                        debug_assert!(msg.should_include_value());
                        let value_uid = msg.get_v();
                        self.store_remote_value(value_uid, v);
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

                    let result = self.process_message(msg).await?;
                    if let Some(value) = result {
                        resp_tx.send(value).await.expect("Sending commited value");
                        continue 'main_loop; // Restart from the beginning of the queue
                    }
                }
                Done => count_done += 1,
                _ => panic!("Unexpected message type"),
            }
        } // 'main_loop: loop

        req_rx.close();
        msg_rx.close();
        Ok(())
    } // run

    fn ready_to_process(&self, msg: &ConsensusMessage) -> bool {
        msg.get_slot() <= self.get_slot() && self.knows_value(msg.get_v())
    }

    async fn process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<Request>>;

    async fn propose_start(&mut self, req: Request) -> io::Result<()>;

    async fn repropose_start(&mut self, value_uid: usize, value: Option<KVal>) -> io::Result<()>;

    fn get_nb_nodes(&self) -> usize;

    fn get_slot(&self) -> usize;

    fn get_my_v(&self) -> Option<usize>;

    fn should_lead(&self) -> bool;

    async fn inner_broadcast(&mut self, msg: Message) -> io::Result<()>;

    fn store_new_value(&mut self, req: Request) -> usize;

    fn store_remote_value(&mut self, value_uid: usize, v: KVal);

    fn knows_value(&self, v_uid: usize) -> bool;

    fn has_queued_values(&self) -> bool;

    fn get_value_to_propose(&self) -> (usize, Option<KVal>);
}
