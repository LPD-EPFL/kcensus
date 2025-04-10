use crate::connector::connect_all;
use crate::consensus::paxos_family::{Mode, PaxosFamily};
use crate::consensus::Consensus;
use crate::delayer::Delayer;
use crate::topology::Topology;
use chrono::prelude::*;
use clap::{arg, Parser};
use consensus::kcensus::propagation::PropagationGraphs;
use consensus::kcensus::KCensus;
use env_logger::fmt::style;
use log::debug;
use std::io;
use std::io::Write;
use std::time::{Duration, Instant};

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
    #[arg(short, long, num_args = 0.., value_delimiter = ',')]
    faults: Vec<usize>,
    #[arg(short, long, default_value_t = 1u32, value_name = "SIMULATION_SPEED")]
    speedup: u32,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum Algo {
    KCensus,
    Paxos,
    EPaxos,
    MultiPaxos,
    NoReplication,
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
    let topology = Topology::from_path(&args.config, args.faults);

    let epaxos_max_faults = topology.nb_nodes - ((topology.nb_nodes * 3) / 4);
    let algo = match args.algo {
        Algo::EPaxos => {
            if topology.faults.len() > epaxos_max_faults {
                Algo::Paxos
            } else {
                Algo::EPaxos
            }
        }
        x => x,
    };
    let faulty = topology.faults.contains(my_pid);
    debug!("Loaded topology:{}", topology);
    let start = Instant::now();
    let propagation_graphs = PropagationGraphs::from(&topology);
    println!("Computed propagation graphs in {:?}", start.elapsed());
    let nb_nodes = topology.regions.len();

    let (consensus_msg_sinks, consensus_msg_streams) =
        connect_all(my_pid, nb_nodes, 9876, Some(topology.faults.clone())).await;
    let (delayer, delayed_msg_rx) = Delayer::new();
    let delayer_task = tokio::task::spawn(delayer.run(
        topology.clone(),
        args.speedup,
        my_pid,
        consensus_msg_streams,
    ));

    let ((client, mut new_client_request_rx), (app, committed_request_tx)) =
        cassandra::App::new(args.db, args.speedup, my_pid).await;

    let start = Instant::now();

    let client_task = tokio::task::spawn(client.run(cassandra::Workload {
        nb_requests: args.requests,
        rw_ratio: args.writes,
        interval: match args.ingress {
            Ingress::RoundRobin => {
                cassandra::RequestInterval::new_round_robin(
                    my_pid,
                    nb_nodes,
                    topology.rtts[(my_pid + nb_nodes - 1) % nb_nodes] // predecessor
                        .iter()
                        .max()
                        .expect("There should be a maximum RTT.")
                        .to_owned()
                        / args.speedup,
                )
                .await
            }
            Ingress::Exponential => {
                cassandra::RequestInterval::new_exponential(args.throughput * args.speedup as f32)
            }
            Ingress::Constant => cassandra::RequestInterval::Constant {
                reqs_per_second: args.throughput * args.speedup as f32,
            },
        },
        faulty,
    }));

    match algo {
        Algo::KCensus => {
            println!(
                "Expected local latency (no-contention): {:?}",
                propagation_graphs.kcensus_latencies[my_pid]
            );
            let mut leader_prio: Vec<_> = (0..nb_nodes).collect();
            leader_prio.sort_by_key(|pid| propagation_graphs.kcensus_latencies[*pid]);
            let mut consensus_obj = KCensus::new(
                nb_nodes,
                my_pid,
                consensus_msg_sinks,
                propagation_graphs,
                leader_prio,
            );
            let consensus =
                consensus_obj.run(delayed_msg_rx, new_client_request_rx, committed_request_tx);
            let _ = tokio::join!(app.run(), consensus);
        }
        Algo::Paxos => {
            let mut leader_prio: Vec<_> = (0..nb_nodes).collect();
            leader_prio.sort_by_key(|pid| propagation_graphs.paxos_latencies[*pid]);
            let leader = leader_prio[0] == my_pid;
            println!(
                "Expected local latency (no-contention): {:?}",
                propagation_graphs.paxos_latencies[my_pid] / if leader { 2 } else { 1 }
            );
            let mut consensus_obj = PaxosFamily::new(
                nb_nodes,
                my_pid,
                consensus_msg_sinks,
                leader_prio,
                Mode::Paxos,
            );
            let consensus =
                consensus_obj.run(delayed_msg_rx, new_client_request_rx, committed_request_tx);
            let _ = tokio::join!(app.run(), consensus);
        }
        Algo::EPaxos => {
            let mut leader_prio: Vec<_> = (0..nb_nodes).collect();
            leader_prio.sort_by_key(|pid| propagation_graphs.epaxos_latencies[*pid]);
            println!(
                "Expected local latency (no-contention): {:?}",
                propagation_graphs.epaxos_latencies[my_pid]
            );
            let mut consensus_obj = PaxosFamily::new(
                nb_nodes,
                my_pid,
                consensus_msg_sinks,
                leader_prio,
                Mode::EPaxos,
            );
            let consensus =
                consensus_obj.run(delayed_msg_rx, new_client_request_rx, committed_request_tx);
            let _ = tokio::join!(app.run(), consensus);
        }
        Algo::MultiPaxos => {
            let mut leader_prio: Vec<_> = (0..nb_nodes).collect();
            leader_prio.sort_by_cached_key(|pid| {
                propagation_graphs.multi_paxos_latencies[*pid]
                    .iter()
                    .sum::<Duration>()
            });
            let leader = leader_prio[0];
            println!(
                // TODO: Provide expected local latency
                "Expected local latency with leader {} (no-contention): {:?}",
                leader, propagation_graphs.multi_paxos_latencies[leader][my_pid]
            );
            let mut consensus_obj = PaxosFamily::new(
                nb_nodes,
                my_pid,
                consensus_msg_sinks,
                leader_prio,
                Mode::MultiPaxos,
            );
            let consensus =
                consensus_obj.run(delayed_msg_rx, new_client_request_rx, committed_request_tx);
            let _ = tokio::join!(app.run(), consensus);
        }
        Algo::NoReplication | Algo::WeakReplication => {
            let latency_mock = async {
                // For simplicity, requests will be executed locally after a ping delay.
                // This is a lower bound as this consumes no network + compute is sharded.
                let rtt = match algo {
                    Algo::NoReplication => {
                        // The leader is the node with the lowest median ping.
                        let leader = (0..topology.nb_nodes)
                            .min_by_key(|&potential_leader| {
                                let mut rtts = topology.rtts[potential_leader].clone();
                                rtts.sort();
                                rtts[rtts.len() / 2]
                            })
                            .expect("There should be a leader");
                        topology.rtts[leader][my_pid] / args.speedup
                    }
                    Algo::WeakReplication => {
                        let mut rtts = topology.rtts[my_pid].clone();
                        rtts.sort();
                        rtts[rtts.len() / 2] / args.speedup
                    }
                    _ => unreachable!("Algo::(No|Weak)Replication"),
                };
                while let Some(command) = new_client_request_rx.recv().await {
                    tokio_timerfd::sleep(rtt)
                        .await
                        .expect("Unreplicated or weakly replicated server failed to sleep");
                    committed_request_tx.send(command).await.expect(
                        "Unreplicated or weakly replicated server failed to send committed request",
                    );
                }
                drop(committed_request_tx); // So the app stops
                drop(delayed_msg_rx); // So the delayer stops
            };
            let _ = tokio::join!(app.run(), latency_mock);
        }
    };

    println!("Total duration: {:?}", start.elapsed());

    client_task.await?;
    delayer_task.await?;
    Ok(())
}
