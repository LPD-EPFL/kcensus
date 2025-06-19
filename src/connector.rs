use crate::message::Message::Hello;
use crate::message::{Message, MsgWithSource};
use crate::multi_sink::MultiSink;
use bit_set::BitSet;
use futures::stream::select_all;
use futures::{Stream, TryStreamExt};
use log::debug;
use std::time::Duration;
use tokio::io;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio_serde::formats::Bincode;
use tokio_serde::Framed;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use crate::topology::Topology;

pub struct Connector {
    listener: TcpListener,
    addresses: Vec<String>,
}

type WrappedStream = FramedRead<OwnedReadHalf, LengthDelimitedCodec>;
pub type WrappedSink = FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>;
type SerStream = Framed<WrappedStream, Message, (), Bincode<Message, ()>>;

impl Connector {
    pub async fn new(my_pid: usize, addresses: &[String]) -> io::Result<Self> {
        let my_address = &addresses[my_pid];
        Ok(Self {
            listener: TcpListener::bind(my_address).await?,
            addresses: addresses.to_vec(),
        })
    }

    pub async fn connect_to(&self, pid: usize) -> io::Result<(SerStream, WrappedSink)> {
        let address = &self.addresses[pid];
        let connection = loop {
            if let Ok(c) = TcpStream::connect(address.clone()).await {
                break c;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        debug!("Connection initiated to: {}", address);
        connection.set_nodelay(true)?;
        Ok(wrap_stream(connection))
    }

    pub async fn accept_connection(&self) -> io::Result<(SerStream, WrappedSink)> {
        let (socket, address) = self.listener.accept().await?;
        debug!("Connection received from: {:?}", address);
        socket.set_nodelay(true)?;
        Ok(wrap_stream(socket))
    }
}

fn wrap_stream(stream: TcpStream) -> (SerStream, WrappedSink) {
    let (read, write) = stream.into_split();
    let stream = WrappedStream::new(read, LengthDelimitedCodec::new());
    let sink = WrappedSink::new(write, LengthDelimitedCodec::new());
    let stream = SerStream::new(stream, Bincode::default());
    (stream, sink)
}

pub async fn connect_all(
    my_pid: usize,
    topology: Topology,
    faults: Option<BitSet>,
) -> (
    MultiSink,
    impl Stream<Item = Result<MsgWithSource, io::Error>>,
) {
    let nb_nodes = topology.nb_nodes;
    let mut sinks = MultiSink::new_with_faults(my_pid, nb_nodes, faults.iter().flatten());
    let mut streams = Vec::with_capacity(nb_nodes);

    let wrap_with_source_pid = |pid: usize| move |m: Message| m.with_source(pid);

    let connector = Connector::new(my_pid, &topology.addresses)
        .await
        .expect("Connector failed to init");
    for pid in 0..my_pid {
        let (stream, sink) = connector.connect_to(pid).await.expect("Failed to connect");
        sinks.insert_sink(pid, sink);
        sinks
            .inner_send(Hello { pid: my_pid }, pid)
            .await
            .expect("Should send hello msg");
        streams.push(stream.map_ok(wrap_with_source_pid(pid)))
    }
    for _ in (my_pid + 1)..nb_nodes {
        'retry: loop {
            let (mut stream, sink) = connector
                .accept_connection()
                .await
                .expect("Failed to accept connection");
            let first_msg = stream.next().await;
            let pid = match first_msg {
                Some(Ok(Hello { pid })) => pid,
                _ => continue 'retry,
            };
            assert!(pid < nb_nodes);
            sinks.insert_sink(pid, sink);
            streams.push(stream.map_ok(wrap_with_source_pid(pid)));
            break;
        }
    }

    let streams = select_all(streams);
    (sinks, streams)
}
