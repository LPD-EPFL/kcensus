use crate::message::Message::Hello;
use crate::message::{Message, MsgWithSource};
use crate::multi_sink::MultiSink;
use futures::stream::{select_all, SelectAll};
use futures::{SinkExt, Stream, StreamExt, TryStreamExt};
use log::debug;
use std::collections::HashMap;
use std::time::Duration;
use tokio::io;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio_serde::formats::Bincode;
use tokio_serde::Framed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

pub struct Connector {
    my_pid: usize,
    listener: TcpListener,
    base_port: u16,
}

type WrappedStream = FramedRead<OwnedReadHalf, LengthDelimitedCodec>;
type WrappedSink = FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>;
type SerStream = Framed<WrappedStream, Message, (), Bincode<Message, ()>>;
pub type DeSink = Framed<WrappedSink, (), Message, Bincode<(), Message>>;

impl Connector {
    pub async fn new(my_pid: usize, base_port: u16) -> io::Result<Self> {
        // TODO: Load addresses from config
        let my_address = format!("127.0.0.1:{}", base_port + my_pid as u16);
        Ok(Self {
            my_pid,
            listener: TcpListener::bind(my_address).await?,
            base_port,
        })
    }

    pub async fn connect_to(&self, pid: usize) -> io::Result<(DeSink, SerStream)> {
        let address = format!("127.0.0.1:{}", self.base_port + pid as u16);
        let connection = loop {
            if let Ok(c) = TcpStream::connect(address.clone()).await {
                break c;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        debug!("Connection initiated to: {}", address);
        connection.set_nodelay(true)?;
        let (stream, mut sink) = wrap_stream(connection);
        sink.send(Hello { pid: self.my_pid }).await?;
        Ok((sink, stream))
    }

    pub async fn accept_connection(&self) -> io::Result<(usize, DeSink, SerStream)> {
        let (socket, address) = self.listener.accept().await?;
        debug!("Connection received from: {:?}", address);
        socket.set_nodelay(true)?;
        let (mut stream, sink) = wrap_stream(socket);
        match stream.next().await.unwrap()? {
            Hello { pid } => Ok((pid, sink, stream)),
            _ => panic!("First message should be Hello"),
        }
    }
}

fn wrap_stream(stream: TcpStream) -> (SerStream, DeSink) {
    let (read, write) = stream.into_split();
    let stream = WrappedStream::new(read, LengthDelimitedCodec::new());
    let sink = WrappedSink::new(write, LengthDelimitedCodec::new());
    (
        SerStream::new(stream, Bincode::default()),
        DeSink::new(sink, Bincode::default()),
    )
}

pub async fn connect_all(
    my_pid: usize,
    nb_nodes: usize,
    base_port: u16,
) -> (
    MultiSink,
    SelectAll<impl Stream<Item = Result<MsgWithSource, std::io::Error>>>,
) {
    let mut sinks = HashMap::with_capacity(nb_nodes - 1);
    let mut streams = Vec::with_capacity(nb_nodes - 1);

    let wrap_with_source_pid = |pid: usize| move |m: Message| m.with_source(pid);

    let connector = Connector::new(my_pid, base_port)
        .await
        .expect("Connector failed to init");
    for pid in 0..my_pid {
        let (sink, stream) = connector.connect_to(pid).await.expect("Failed to connect");
        sinks.insert(pid, sink);
        streams.push(stream.map_ok(wrap_with_source_pid(pid)))
    }
    for _ in (my_pid + 1)..nb_nodes {
        let (pid, sink, stream) = connector
            .accept_connection()
            .await
            .expect("Failed to accept connection");
        sinks.insert(pid, sink);
        streams.push(stream.map_ok(wrap_with_source_pid(pid)))
    }

    let sinks = MultiSink { sinks, my_pid };
    let streams = select_all(streams);
    (sinks, streams)
}
