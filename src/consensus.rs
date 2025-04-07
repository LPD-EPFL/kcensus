use crate::consensus::message::ConsensusMessage;
use crate::consensus::message::ConsensusMsg::Commit;
use crate::message::Message::{ConsensusM, Done};
use crate::message::MsgWithSource;
use crate::value::{CommittedRequest, KVal, Request};
use log::{debug, info};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::VecDeque;
use std::io;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};

pub mod kcensus;
pub mod message;
pub mod paxos_family;

pub trait Consensus {
    async fn run<ApplicationRequest: Serialize + DeserializeOwned>(
        &mut self,
        mut msg_rx: Receiver<MsgWithSource>,
        mut client_request_rx: Receiver<Option<ApplicationRequest>>,
        committed_request_tx: Sender<CommittedRequest<ApplicationRequest>>,
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
                    if let Some(request) = result {
                        committed_request_tx
                            .send(request.into())
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
            let should_repropose = !ongoing && self.has_queued_requests();
            // TODO: (Optim.) peak connection first ?
            if should_repropose && self.should_lead() {
                // TODO: Leader election / only leader should repropose !!!!!!!!!!!!!!!!!!
                if let Some(batch) = self.get_new_batch_to_propose() {
                    self.propose_start(batch.into_remote_req(), false).await?;
                } else {
                    let v = self.get_v_to_repropose();
                    self.repropose_start(v).await?;
                }
            }

            // Read new messages and/or new local request
            let msg = select! {
                req = client_request_rx.recv(), if (!ongoing || self.can_forward_proposals()) && !should_repropose && !done => {
                    match req.unwrap() {
                        Some(req) =>  {
                            self.propose_start(KVal::new(self.get_my_pid(), &req).into_local_req(), ongoing).await?;
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
                        let v = msg.get_v();
                        self.store_remote_request(v, value);
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
                        committed_request_tx
                            .send(value.into())
                            .await
                            .expect("Sending commited value");
                        continue 'main_loop; // Restart from the beginning of the queue
                    }
                }
                Done => count_done += 1,
                _ => panic!("Unexpected message type"),
            }
        } // 'main_loop: loop

        client_request_rx.close();
        msg_rx.close();
        Ok(())
    } // run

    #[inline]
    fn ready_to_process(&self, msg: &ConsensusMessage) -> bool {
        msg.get_slot() <= self.get_slot() && self.knows_v(msg.get_v())
    }

    async fn full_process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<Request>> {
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

    async fn process_message(&mut self, msg: ConsensusMessage) -> io::Result<Option<Request>>;

    fn can_forward_proposals(&mut self) -> bool;

    async fn propose_start(&mut self, req: Request, contention: bool) -> io::Result<()>;

    async fn repropose_start(&mut self, v: usize) -> io::Result<()>;

    fn commit_slot(&mut self, v: usize, from_commit_msg: bool) -> Request;

    fn get_my_pid(&self) -> usize;

    fn get_nb_nodes(&self) -> usize;

    fn get_slot(&self) -> usize;

    fn get_my_v(&self) -> Option<usize>;

    fn should_lead(&self) -> bool;

    async fn announce_done(&mut self) -> io::Result<()>;

    fn store_new_request(&mut self, req: Request) -> usize;

    fn store_remote_request(&mut self, v: usize, value: KVal);

    fn knows_v(&self, v: usize) -> bool;

    fn has_queued_requests(&self) -> bool;

    fn get_new_batch_to_propose(&self) -> Option<KVal>;

    fn get_v_to_repropose(&self) -> usize;
}
