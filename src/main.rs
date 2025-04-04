use crate::connector::Connector;
use crate::kcensus::propagation::PropagationGraphs;
use crate::kcensus::{KCensus, NbNodes, Pid};
use crate::message::Message;
use crate::topology::Topology;
use crate::value::Request;
use chrono::prelude::*;
use clap::Parser;
use env_logger::fmt::style;
use futures::prelude::stream::select_all;
use futures::TryStreamExt;
use log::{debug, info};
use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

mod connector;
mod delayer;
mod kcensus;
mod message;
mod requester;
mod topology;
mod value;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    pid: usize,
    #[arg(short, long)]
    config: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    env_logger::builder()
        .format(|buf, record| {
            let time = Utc::now();
            let level = record.level();
            let level_style = buf.default_level_style(level);
            let header_style = style::AnsiColor::BrightBlack.on_default();
            writeln!(
                buf,
                "{header_style}{}{header_style:#} {level_style}{level:<5}{level_style:#} {}",
                time.format("%S%.6f"),
                record.args()
            )
        })
        .init();

    let args = Args::parse();
    let my_pid = args.pid;
    let topology = Topology::from_toml(&args.config);
    debug!("Loaded topology:{}", topology);
    let nb_nodes = topology.regions.len();
    let propagation_graphs = PropagationGraphs::from_topology(&topology);

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
        tokio::task::spawn(delayer::delayer(topology, my_pid, input_stream, delayed_tx));

    let kcensus = KCensus::new(
        NbNodes(nb_nodes),
        Pid(my_pid),
        ReceiverStream::new(delayed_rx),
        sinks,
        propagation_graphs,
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
            Some(Request { value, .. }) => {
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
