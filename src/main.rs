use crate::connector::Connector;
use crate::consensus::paxos::Paxos;
use crate::consensus::Consensus;
use crate::message::Message;
use crate::multi_sink::MultiSink;
use crate::topology::Topology;
use chrono::prelude::*;
use clap::Parser;
use consensus::kcensus::propagation::PropagationGraphs;
use consensus::kcensus::KCensus;
use env_logger::fmt::style;
use futures::prelude::stream::select_all;
use futures::TryStreamExt;
use log::{debug, trace};
use std::collections::HashMap;
use std::io;
use std::io::Write;
use std::time::Instant;
use tokio::sync::mpsc;
use crate::value::CommittedRequest;

mod connector;
mod consensus;
mod delayer;
mod message;
mod multi_sink;
mod topology;
mod value;
mod cassandra;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    pid: usize,
    #[arg(short, long)]
    config: String,
    #[arg(short, long)]
    db: Option<String>,
    #[arg(short, long)]
    algo: Algo,
}

#[derive(clap::ValueEnum, Clone, Default, Debug)]
enum Algo {
    #[default]
    KCensus,
    Paxos,
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
    let topology = Topology::from(&args.config);
    debug!("Loaded topology:{}", topology);
    let nb_nodes = topology.regions.len();
    let propagation_graphs = PropagationGraphs::from(&topology);

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

    let sinks = MultiSink { sinks, my_pid };

    let input_stream = select_all(streams);

    // TODO: Channel buffer size ?
    let (delayed_tx, delayed_rx) = mpsc::channel(1);

    let delayer_task =
        tokio::task::spawn(delayer::delayer(topology, my_pid, input_stream, delayed_tx));

    let (client_request_tx, client_request_rx) = mpsc::channel(1);
    let (client_response_tx, client_response_rx) = mpsc::channel(1);
    let (committed_request_tx, mut committed_request_rx) = mpsc::channel::<CommittedRequest<cassandra::Request>>(1);

    let cassandra = if let Some(uri) = args.db {
        // docker run --name cassandra -p 9042:9042 -d cassandra
        // -db 127.0.0.1:9042
        // docker stop cassandra && docker rm cassandra
        let cassandra = cassandra::Handler::new(&uri).await;
        cassandra.reset_database().await;
        cassandra.prepare().await.into()
    } else {
        None
    };

    let start = Instant::now();

    let client_task =
        tokio::task::spawn(async move {
            let client = cassandra::Client {};
            client.run(nb_nodes, my_pid, client_request_tx, client_response_rx).await;
        });

    let app = async {
        while let Some(req) = committed_request_rx.recv().await {
            trace!("About to execute committed request: {:?}", req);
            let response = if let Some(cassandra) = cassandra.as_ref() {
                cassandra.execute(req.request).await
            } else { // We mock Cassandra
                match req.request {
                    cassandra::Request::Put { key, value } => { cassandra::Response::Put { key, value } }
                    cassandra::Request::Get { key } => { cassandra::Response::Get { key, value: None } }
                }
            };
            if req.local {
                client_response_tx.send(response).await.expect("Server failed to enqueue client Response");
            }
        }
    };

    match args.algo {
        Algo::KCensus => {
            let mut kcensus_obj = KCensus::new(nb_nodes, my_pid, sinks, propagation_graphs);
            let kcensus = kcensus_obj.run(delayed_rx, client_request_rx, committed_request_tx);
            let _ = tokio::join!(app, kcensus);
        }
        Algo::Paxos => {
            let mut paxos_obj = Paxos::new(nb_nodes, my_pid, sinks);
            let paxos = paxos_obj.run(delayed_rx, client_request_rx, committed_request_tx);
            let _ = tokio::join!(app, paxos);
        }
    };

    println!("Total duration: {:?}", start.elapsed());

    client_task.await?;
    delayer_task.await??;
    Ok(())
}
