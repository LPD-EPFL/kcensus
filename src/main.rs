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
use message::Message;
use message::Message::{Hello};
use crate::kcensus::{KCensus, NbNodes, Pid};

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

    let streams = select_all(streams);
    
    let mut kcensus = 
        KCensus::new(NbNodes(nb_nodes), Pid(my_pid), streams, sinks);
    
    kcensus.run().await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

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