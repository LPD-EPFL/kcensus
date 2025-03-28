use crate::connector::Connector;
use crate::kcensus::{KCensus, NbNodes, Pid};
use crate::value::Request;
use clap::Parser;
use futures::prelude::stream::select_all;
use futures::TryStreamExt;
use log::info;
use message::Message;
use std::collections::HashMap;
use std::io;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;

mod connector;
mod delayer;
mod kcensus;
mod message;
mod node_state;
mod requester;
mod round_state;
mod value;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 3)]
    nb_nodes: usize,
    #[arg(short, long)]
    pid: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let nb_nodes = args.nb_nodes;
    let my_pid = args.pid;

    let mut sinks = HashMap::with_capacity(nb_nodes - 1);
    let mut streams = Vec::with_capacity(nb_nodes - 1);

    let wrap_with_source_pid = |pid: usize| move |m: Message| m.with_source(pid);

    let connector = Connector::new(my_pid).await?;
    for pid in 0..my_pid {
        let (sink, stream) = connector.connect_to(pid).await?;
        sinks.insert(pid, sink);
        streams.push(stream.map_ok(wrap_with_source_pid(pid)))
    }
    for _ in (my_pid + 1)..nb_nodes {
        let (pid, sink, stream) = connector.accept_connection().await?;
        sinks.insert(pid, sink);
        streams.push(stream.map_ok(wrap_with_source_pid(pid)))
    }

    let input_stream = select_all(streams);

    // TODO: Channel buffer size ?
    let (delayed_tx, delayed_rx) = mpsc::channel(1);

    let delayer_task =
        tokio::task::spawn(delayer::delayer(nb_nodes, my_pid, input_stream, delayed_tx));

    let kcensus = KCensus::new(
        NbNodes(nb_nodes),
        Pid(my_pid),
        ReceiverStream::new(delayed_rx),
        sinks,
    );

    let (request_tx, request_rx) = mpsc::channel(1);
    let (response_tx, mut response_rx) = mpsc::channel(1);

    let kcensus_task = tokio::task::spawn(kcensus.run(request_rx, response_tx));

    let requester_task =
        tokio::task::spawn(requester::simple_requester(nb_nodes, my_pid, request_tx));

    let start = Instant::now();

    loop {
        match response_rx.recv().await {
            None => break,
            Some(Request {
                value,
                start_time: Some(t),
            }) => {
                println!("Decided {} in {:?}.", value.val, t.elapsed());
            }
            Some(Request {
                value,
                start_time: None,
            }) => {
                info!("Received val {}.", value.val)
            }
        }
    }

    kcensus_task.await??;
    println!("Total duration: {:?}", start.elapsed());

    requester_task.await?;
    delayer_task.await??;
    Ok(())
}
