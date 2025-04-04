use crate::message::{MsgWithDeadline, MsgWithSource};
use crate::topology::Topology;
use chrono::Utc;
use futures::Stream;
use log::trace;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::io;
use std::time::Instant;
use tokio::sync::mpsc::Sender;
use tokio::{pin, select};
use tokio_stream::StreamExt;
use tokio_timerfd::Delay;

pub async fn delayer<St: Stream<Item = io::Result<MsgWithSource>> + Unpin>(
    topology: Topology,
    my_pid: usize,
    mut input_stream: St,
    delayed_output: Sender<MsgWithSource>,
) -> io::Result<()> {
    let delay = Delay::new(Instant::now())?;
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
                if opt_res.is_none() {
                    stream_ended = true;
                    continue
                }
                let msg = opt_res.unwrap()?;

                let deadline = Instant::now() + topology.link_latencies[msg.src][my_pid];

                trace!("Queuing message from {}", msg.src);
                queues[msg.src].push_back(
                    msg.with_deadline(deadline)
                );
            }
            res = &mut delay, if !is_empty => {
                res?;
                let now = Instant::now();
                trace!("Overslept by {:?}", now.duration_since(delay.deadline()));
                trace!("Slept enough. {} queued messages. Consuming...", queues.iter()
                    .map(|q| q.len()).sum::<usize>());
                for q in queues.iter_mut() {
                    if let Some(m) = q.front() {
                        if m.deadline < now {
                            if delayed_output.send(q.pop_front().unwrap().msg).await.is_err() {
                                return Ok(())
                            }
                        }
                    }
                }
                trace!("New queue_size = {}", queues.iter()
                     .map(|q| q.len()).sum::<usize>());
            }
            () = delayed_output.closed() => {
                return Ok(())
            }
        } // select!
    } // loop
    Ok(())
}
