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
        simulate_delays: bool,
    ) {
        let jitter: f64 = std::env::var("KC_JITTER")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0);
        let delay = Delay::new(Instant::now()).expect("Delayer failed to init delay");
        pin!(delay);

        let mut queues: Vec<VecDeque<MsgWithDeadline>> = Vec::with_capacity(topology.nb_processes);
        for _ in 0..topology.nb_processes {
            queues.push(VecDeque::with_capacity(
                topology.nb_processes * topology.nb_processes,
            ));
        }

        let mut stream_ended = false;
        let mut consumer_gone = false;

        loop {
            let opt_deadline = if consumer_gone {
                None
            } else {
                queues
                    .iter()
                    .filter_map(|q| q.front())
                    .map(|msg| msg.deadline)
                    .min()
            };
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

                    if consumer_gone {
                        continue
                    }

                    if !msg.msg.is_consensus_msg() {
                        consumer_gone |= self.delayed_msg_tx.send(msg).await.is_err();
                        continue
                    }

                    if simulate_delays {
                        // XJITTER (experiment only): scale each link latency by a random
                        // factor in [1 - j, 1 + j]. FIFO per source is preserved by never
                        // scheduling before the message already queued behind it.
                        let base = topology.link_latency(msg.src, my_pid) / speedup;
                        let latency = match jitter {
                            0.0 => base,
                            j => base.mul_f64(1.0 - j + 2.0 * j * rand::random::<f64>()),
                        };
                        let mut deadline = Instant::now() + latency;
                        if let Some(last) = queues[msg.src].back() {
                            deadline = deadline.max(last.deadline);
                        }
                        queues[msg.src].push_back(
                            msg.with_deadline(deadline)
                        );
                    } else {
                        consumer_gone |= self.delayed_msg_tx.send(msg).await.is_err();
                    }
                }

                res = &mut delay, if !is_empty => {
                    res.expect("Delayed message stream ended early");
                    let now = Instant::now();
                    trace!("Overslept by {:?}. {} queued messages. Consuming...", now.duration_since(delay.deadline()), queues.iter()
                        .map(|q| q.len()).sum::<usize>());
                    for q in queues.iter_mut() {
                        if let Some(m) = q.front() {
                            if m.deadline < now {
                                let msg = q.pop_front().unwrap().msg;
                                if self.delayed_msg_tx.send(msg).await.is_err() {
                                    consumer_gone = true;
                                    break;
                                }
                            }
                        }
                    }
                    trace!("New queue_size = {}", queues.iter()
                         .map(|q| q.len()).sum::<usize>());
                }

                () = self.delayed_msg_tx.closed(), if !consumer_gone => {
                    consumer_gone = true;
                }
            } // select!
        } // loop
    }
}
