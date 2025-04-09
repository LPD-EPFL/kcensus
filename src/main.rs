use crate::cassandra::Request;
use crate::connector::Connector;
use crate::consensus::paxos_family::{Mode, PaxosFamily};
use crate::consensus::Consensus;
use crate::message::Message;
use crate::multi_sink::MultiSink;
use crate::topology::Topology;
use chrono::prelude::*;
use clap::Parser;
use consensus::command::{Command, CommittedCommand};
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
use tokio::sync::{mpsc, watch};

mod cassandra;
mod connector;
mod consensus;
mod delayer;
mod message;
mod multi_sink;
mod topology;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    pid: usize,
    #[arg(short, long)]
    config: String,
    #[arg(short, long, help = "Cassandra URI, mocked otherwise")]
    db: Option<String>,
    #[arg(short, long, default_value_t = Algo::KCensus, value_enum)]
    algo: Algo,
    #[arg(short, long, default_value_t = 10)]
    requests: usize,
    #[arg(short, long, default_value_t = Ingress::RoundRobin, value_enum)]
    ingress: Ingress,
    #[arg(short, long, default_value_t = 10f32, value_name = "TARGET_REQ/S")]
    throughput: f32,
    #[arg(short, long, default_value_t = 0.5f32, value_name = "WRITE_RATIO")]
    writes: f32,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum Algo {
    KCensus,
    Paxos,
    EPaxos,
    MultiPaxos,
    Unreplicated,
    WeakReplication,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum Ingress {
    RoundRobin,
    Exponential,
    Constant,
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
    let start = Instant::now();
    let propagation_graphs = PropagationGraphs::from(&topology);
    println!("Computed propagation graphs in {:?}", start.elapsed());
    let nb_nodes = topology.regions.len();

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
    let (delayed_msg_tx, delayed_msg_rx) = mpsc::channel(1);

    let delayer_task = tokio::task::spawn(delayer::delayer(
        topology.clone(),
        my_pid,
        input_stream,
        delayed_msg_tx,
    ));

    let (new_client_request_tx, mut new_client_request_rx) = mpsc::channel(1);
    let (committed_request_tx, mut committed_request_rx) = mpsc::channel::<Command>(1);
    let (client_response_tx, client_response_rx) = mpsc::channel(1);
    let (num_committed_watch_tx, num_committed_watch_rx) = watch::channel(0usize);

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

    let client_task = tokio::task::spawn(async move {
        let client = cassandra::Client {
            my_pid,
            new_client_request_tx,
            client_response_rx,
            num_committed_watch_rx,
        };
        client
            .run(cassandra::Workload {
                nb_requests: args.requests,
                rw_ratio: args.writes,
                interval: match args.ingress {
                    Ingress::RoundRobin => cassandra::RequestInterval::RoundRobin { nb_nodes },
                    Ingress::Exponential => {
                        cassandra::RequestInterval::new_exponential(args.throughput)
                    }
                    Ingress::Constant => cassandra::RequestInterval::Constant {
                        reqs_per_second: args.throughput,
                    },
                },
            })
            .await;
    });

    let app = async {
        let mut num_committed = if let Algo::Unreplicated | Algo::WeakReplication = args.algo {
            my_pid
        } else {
            0
        };
        num_committed_watch_tx.send(num_committed).ok(); // Client not listening for back pressure
        while let Some(command) = committed_request_rx.recv().await {
            let command: CommittedCommand<Request> = command.into();
            num_committed += if let Algo::Unreplicated | Algo::WeakReplication = args.algo {
                nb_nodes
            } else {
                1
            };
            trace!("About to execute committed request: {:?}", command);
            let response = if let Some(cassandra) = cassandra.as_ref() {
                cassandra.execute(command.app_request).await
            } else {
                // We mock Cassandra
                match command.app_request {
                    Request::Put { key, value } => cassandra::Response::Put { key, value },
                    Request::Get { key } => cassandra::Response::Get { key, value: None },
                }
            };
            if command.proposer == my_pid {
                client_response_tx
                    .send(response)
                    .await
                    .expect("Server failed to enqueue client Response");
            }
            num_committed_watch_tx.send(num_committed).ok(); // Client not listening for back pressure
        }
    };

    match args.algo {
        Algo::KCensus => {
            println!(
                "Expected local latency (no-contention): {:?}",
                propagation_graphs.kcensus_latencies[my_pid]
            );
            let mut leader_prio: Vec<_> = (0..nb_nodes).collect();
            leader_prio.sort_by_key(|pid| propagation_graphs.kcensus_latencies[*pid]);
            let mut consensus_obj =
                KCensus::new(nb_nodes, my_pid, sinks, propagation_graphs, leader_prio);
            let consensus =
                consensus_obj.run(delayed_msg_rx, new_client_request_rx, committed_request_tx);
            let _ = tokio::join!(app, consensus);
        }
        Algo::Paxos => {
            let mut leader_prio: Vec<_> = (0..nb_nodes).collect();
            leader_prio.sort_by_key(|pid| propagation_graphs.paxos_latencies[*pid]);
            let leader = leader_prio[0] == my_pid;
            println!(
                "Expected local latency (no-contention): {:?}",
                propagation_graphs.paxos_latencies[my_pid] / if leader { 2 } else { 1 }
            );
            let mut consensus_obj =
                PaxosFamily::new(nb_nodes, my_pid, sinks, leader_prio, Mode::Paxos);
            let consensus =
                consensus_obj.run(delayed_msg_rx, new_client_request_rx, committed_request_tx);
            let _ = tokio::join!(app, consensus);
        }
        Algo::EPaxos => {
            let mut leader_prio: Vec<_> = (0..nb_nodes).collect();
            leader_prio.sort_by_key(|pid| propagation_graphs.epaxos_latencies[*pid]);
            println!(
                "Expected local latency (no-contention): {:?}",
                propagation_graphs.epaxos_latencies[my_pid]
            );
            let mut consensus_obj =
                PaxosFamily::new(nb_nodes, my_pid, sinks, leader_prio, Mode::EPaxos);
            let consensus =
                consensus_obj.run(delayed_msg_rx, new_client_request_rx, committed_request_tx);
            let _ = tokio::join!(app, consensus);
        }
        Algo::MultiPaxos => {
            let mut leader_prio: Vec<_> = (0..nb_nodes).collect();
            leader_prio.sort_by_key(|pid| propagation_graphs.multi_paxos_latencies[*pid]);
            let leader = leader_prio[0];
            println!(
                // TODO: Provide expected local latency
                "Expected AVERAGE latency for leader {} (no-contention): {:?}",
                leader, propagation_graphs.multi_paxos_latencies[leader]
            );
            let mut consensus_obj =
                PaxosFamily::new(nb_nodes, my_pid, sinks, leader_prio, Mode::MultiPaxos);
            let consensus =
                consensus_obj.run(delayed_msg_rx, new_client_request_rx, committed_request_tx);
            let _ = tokio::join!(app, consensus);
        }
        Algo::Unreplicated | Algo::WeakReplication => {
            let unreplicated = async {
                // For simplicity, requests will be executed locally after a ping delay.
                // This is a lower bound as this consumes no network + compute is sharded.
                let rtt = match args.algo {
                    Algo::Unreplicated => {
                        // The leader is the node with the lowest median ping.
                        let leader = (0..topology.nb_nodes)
                            .min_by_key(|&potential_leader| {
                                let mut rtts = topology.rtts[potential_leader].clone();
                                rtts.sort();
                                rtts[rtts.len() / 2 + 1]
                            })
                            .expect("There should be a leader");
                        topology.rtts[leader][my_pid]
                    }
                    Algo::WeakReplication => {
                        let mut rtts = topology.rtts[my_pid].clone();
                        rtts.sort();
                        rtts[rtts.len() / 2 + 1]
                    }
                    _ => unreachable!("Algo::Unreplicated | Algo::WeakReplication"),
                };
                while let Some(command) = new_client_request_rx.recv().await {
                    tokio_timerfd::sleep(rtt)
                        .await
                        .expect("Unreplicated or weakly replicated server failed to sleep");
                    committed_request_tx.send(command).await.expect(
                        "Unreplicated or weakly replicated server failed to send CommittedRequest",
                    );
                }
                drop(committed_request_tx); // So the app stops
                drop(delayed_msg_rx); // So the delayer stops
            };
            let _ = tokio::join!(app, unreplicated);
        }
    };

    println!("Total duration: {:?}", start.elapsed());

    client_task.await?;
    delayer_task.await??;
    Ok(())
}
