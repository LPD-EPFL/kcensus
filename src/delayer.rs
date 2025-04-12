use crate::message::{MsgWithDeadline, MsgWithSource};
use crate::topology::Topology;
use futures::Stream;
use log::trace;
use std::collections::VecDeque;
use std::io;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::{pin, select};
use tokio_stream::StreamExt;
use tokio_timerfd::Delay;

pub struct Delayer {
    delayed_msg_tx: Sender<MsgWithSource>,
}

impl Delayer {
    pub fn new() -> (Self, Receiver<MsgWithSource>) {
        let (delayed_msg_tx, delayed_msg_rx) = mpsc::channel(1);
        (Self { delayed_msg_tx }, delayed_msg_rx)
    }

    pub async fn run(
        self,
        topology: Topology,
        speedup: u32,
        my_pid: usize,
        mut input_stream: impl Stream<Item = io::Result<MsgWithSource>> + Unpin,
    ) {
        let dead = topology.faults.contains(my_pid);
        let delay = Delay::new(Instant::now()).expect("Delayer failed to init delay");
        pin!(delay);

        let mut queues: Vec<VecDeque<MsgWithDeadline>> = Vec::with_capacity(topology.nb_nodes);
        for _ in 0..topology.nb_nodes {
            queues.push(VecDeque::with_capacity(
                topology.nb_nodes * topology.nb_nodes,
            ));
        }

        let mut stream_ended = false;

        loop {
            let opt_deadline = queues
                .iter()
                .filter_map(|q| q.front())
                .map(|msg| msg.deadline)
                .min();
            if let Some(deadline) = opt_deadline {
                delay.as_mut().reset(deadline)
            }
            let is_empty = opt_deadline.is_none();
            if is_empty && stream_ended {
                break;
            }
            select! {
                opt_res = input_stream.next(), if !stream_ended => {
                    let msg = match opt_res {
                        Some(Ok(msg)) => msg,
                        Some(Err(err)) => { panic!("{}", err); }
                        None => {
                            stream_ended = true;
                            continue
                        }
                    };

                    if !msg.msg.delayed() {
                        self.delayed_msg_tx.send(msg).await.expect(
                            "Channel should not be closed yet"
                        );
                        continue
                    }

                    if dead || topology.faults.contains(msg.src) {
                        continue
                    }

                    let deadline = Instant::now() + topology.link_latency(msg.src,my_pid) / speedup;

                    queues[msg.src].push_back(
                        msg.with_deadline(deadline)
                    );
                }
                res = &mut delay, if !is_empty => {
                    res.expect("Delayed message stream ended early");
                    let now = Instant::now();
                    trace!("Overslept by {:?}. {} queued messages. Consuming...", now.duration_since(delay.deadline()), queues.iter()
                        .map(|q| q.len()).sum::<usize>());
                    for q in queues.iter_mut() {
                        if let Some(m) = q.front() {
                            if m.deadline < now {
                                let res = self.delayed_msg_tx.send(q.pop_front().unwrap().msg).await;
                                if res.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    trace!("New queue_size = {}", queues.iter()
                         .map(|q| q.len()).sum::<usize>());
                }
                () = self.delayed_msg_tx.closed() => {
                    return;
                }
            } // select!
        } // loop
    }
}
