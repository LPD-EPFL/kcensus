use std::collections::VecDeque;
use std::collections::HashMap;
use futures::{SinkExt, StreamExt, TryStreamExt};
use std::io;
use std::time::Duration;
use futures::prelude::stream::select_all;
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf}, TcpListener,
    TcpStream,
};
use tokio_serde::{formats::Bincode, Framed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use clap::Parser;
use tokio::{pin, select};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{sleep_until, Instant};
use tokio_stream::wrappers::ReceiverStream;
use message::Message;
use message::Message::{Hello};
use crate::kcensus::{KCensus, NbNodes, Pid};
use crate::message::{MsgWithDeadline};
use crate::message::Message::Done;

mod message;
mod kcensus;
mod node_state;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 3)]
    nb_nodes: usize,
    #[arg(short, long)]
    pid: usize,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();
    let nb_nodes = args.nb_nodes;
    let my_pid = args.pid;

    let my_address = format!("127.0.0.1:{}", 9876 + my_pid);
    let listener = TcpListener::bind(my_address).await?;

    let mut sinks = HashMap::with_capacity(nb_nodes - 1);
    let mut streams = Vec::with_capacity(nb_nodes - 1);

    let wrap_with_source_pid = |pid: usize| { move |m: Message| {
        m.with_source(pid)
    }};

    for pid in 0..my_pid {
        let address = format!("127.0.0.1:{}", 9876 + pid);
        let connection = loop {
            if let Ok(c) = TcpStream::connect(address.clone()).await {
                break c;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        println!("Connection initiated to: {}", address);
        connection.set_nodelay(true)?;
        let (stream, mut sink) = wrap_stream(connection);
        sink.send(Hello { pid: my_pid }).await?;
        sinks.insert(pid, sink);
        streams.push(stream.map_ok(wrap_with_source_pid(pid)))
    }

    for _ in (my_pid+1)..nb_nodes {
        let (socket, address) = listener.accept().await?;
        println!("Connection received from: {:?}", address);
        socket.set_nodelay(true)?;
        let (mut stream, sink) = wrap_stream(socket);
        match stream.next().await.unwrap()? {
            Hello { pid } => {
                sinks.insert(pid, sink);
                streams.push(stream.map_ok(wrap_with_source_pid(pid)));
            }
            _ => panic!("First message should be Hello")
        };
    }

    let mut streams = select_all(streams);

    // TODO: Channel buffer size ?
    let (tx, rx) = mpsc::channel(nb_nodes * nb_nodes);

    let delayer: JoinHandle<io::Result<()>> = tokio::task::spawn(async move {
        let sleep = sleep_until(Instant::now());
        pin!(sleep);

        let mut queues: Vec<VecDeque<MsgWithDeadline>> = Vec::with_capacity(nb_nodes);
        for _ in 0..nb_nodes {
            queues.push(VecDeque::with_capacity(nb_nodes*nb_nodes));
        }

        let mut stream_ended = false;

        loop {
            let opt_deadline = queues.iter()
                .filter_map(|q| q.front())
                .map(|msg| msg.deadline)
                .min();
            if let Some(deadline) = opt_deadline {
                sleep.as_mut().reset(deadline)
            }
            let is_empty = opt_deadline.is_none();
            if is_empty && stream_ended {
                break;
            }
            select! {
                opt_msg = streams.next(), if !stream_ended => {
                    if opt_msg.is_none() {
                        stream_ended = true;
                        continue
                    }
                    let msg = opt_msg.unwrap()?;

                    // Line topology
                    let pid_diff = my_pid as i64 - msg.src as i64;
                    let pid_diff = if pid_diff > 0 { pid_diff } else { - pid_diff } as u64;
                    let deadline = Instant::now() + Duration::from_millis(100*pid_diff);

                    queues[msg.src].push_back(
                        msg.with_deadline(deadline)
                    );
                    // println!("queue_size++ = {}", queues.iter()
                    //     .map(|q| q.len()).sum::<usize>());
                }
                () = &mut sleep, if !is_empty => {
                    let now = Instant::now();
                    // println!("Slept enough. Consuming messages...");
                    for q in queues.iter_mut() {
                        if let Some(m) = q.front() {
                            if m.deadline < now {
                                if tx.send(q.pop_front().unwrap().msg).await.is_err() {
                                    return Ok(())
                                }
                            }
                        }
                    }
                    // println!("New queue_size = {}", queues.iter()
                    //     .map(|q| q.len()).sum::<usize>());
                }
                () = tx.closed() => {
                    return Ok(())
                }
            } // select end
        }
        Ok(())
    });

    let mut kcensus = 
        KCensus::new(NbNodes(nb_nodes), Pid(my_pid), ReceiverStream::new(rx), sinks);

    kcensus.run().await?;

    delayer.await??;

    Ok(())
}

type WrappedStream = FramedRead<OwnedReadHalf, LengthDelimitedCodec>;
type WrappedSink = FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>;

type SerStream = Framed<WrappedStream, Message, (), Bincode<Message, ()>>;
type DeSink = Framed<WrappedSink, (), Message, Bincode<(), Message>>;

fn wrap_stream(stream: TcpStream) -> (SerStream, DeSink) {
    let (read, write) = stream.into_split();
    let stream = WrappedStream::new(read, LengthDelimitedCodec::new());
    let sink = WrappedSink::new(write, LengthDelimitedCodec::new());
    (
        SerStream::new(stream, Bincode::default()),
        DeSink::new(sink, Bincode::default()),
    )
}