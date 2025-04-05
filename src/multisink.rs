use crate::connector::DeSink;
use crate::message::Message;
use futures::SinkExt;
use std::collections::HashMap;
use std::io;

pub struct MultiSink<Sk>(pub HashMap<usize, Sk>);

impl MultiSink<DeSink> {
    #[inline]
    pub async fn broadcast(&mut self, msg: Message) -> io::Result<()> {
        for (_, sink) in self.0.iter_mut() {
            sink.send(msg.clone()).await?;
        }
        Ok(())
    }

    #[inline]
    pub async fn send(&mut self, msg: Message, pid: usize) -> io::Result<()> {
        let sink = self.0.get_mut(&pid).unwrap();
        sink.send(msg.clone()).await
    }
}
