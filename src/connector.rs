use crate::message::Message;
use crate::message::Message::Hello;
use futures::{SinkExt, StreamExt};
use log::debug;
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
}

type WrappedStream = FramedRead<OwnedReadHalf, LengthDelimitedCodec>;
type WrappedSink = FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>;
type SerStream = Framed<WrappedStream, Message, (), Bincode<Message, ()>>;
pub type DeSink = Framed<WrappedSink, (), Message, Bincode<(), Message>>;

impl Connector {
    pub async fn new(my_pid: usize) -> io::Result<Self> {
        // TODO: Load addresses from config
        let my_address = format!("127.0.0.1:{}", 9876 + my_pid);
        Ok(Self {
            my_pid,
            listener: TcpListener::bind(my_address).await?,
        })
    }

    pub async fn connect_to(&self, pid: usize) -> io::Result<(DeSink, SerStream)> {
        let address = format!("127.0.0.1:{}", 9876 + pid);
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
