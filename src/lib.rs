use crate::connector::connect_all;
use crate::consensus::command::Command;
use crate::consensus::kcensus::propagation::compute_propagation_graphs;
use crate::consensus::kcensus::KCensus;
use crate::consensus::paxos_family::{Mode, PaxosFamily};
use crate::delayer::Delayer;
use crate::topology::Topology;
use bincode::Options;
use chrono::prelude::*;
use clap::{arg, Parser};
use env_logger::fmt::style;
use log::debug;
use std::collections::VecDeque;
use std::io;
use std::io::Write;
use std::time::{Duration, Instant};

mod cassandra;
mod connector;
pub mod consensus;
mod delayer;
pub mod eval;
mod message;
mod multi_sink;
pub mod topology;

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
    #[arg(long, value_parser = humantime::parse_duration, default_value = "10s", value_name = "EXP_DURATION")]
    duration: Duration,
    #[arg(long, value_parser = humantime::parse_duration, default_value = "2s", value_name = "WARMUP")]
    warmup: Duration,
    #[arg(long, value_parser = humantime::parse_duration, default_value = "2s", value_name = "WARMDOWN")]
    warmdown: Duration,
    #[arg(short, long, default_value_t = Ingress::Exponential, value_enum)]
    ingress: Ingress,
    #[arg(short, long, default_value_t = 10f32, value_name = "TARGET_REQ/S")]
    throughput: f32,
    #[arg(short, long, default_value_t = 1f32, value_name = "WRITE_RATIO")]
    writes: f32,
    #[arg(short, long, num_args = 0.., value_delimiter = ',')]
    faults: Vec<usize>,
    #[arg(short, long, num_args = 0.., value_delimiter = ',', short_alias = 'v')]
    non_voting: Vec<usize>,
    #[arg(short, long, default_value_t = 1u32, value_name = "SIMULATION_SPEED")]
    speedup: u32,
    #[arg(long, help = "Simulate link delays. (Default: only if localhost)")]
    simulate_delays: Option<bool>,
    #[arg(short, long, default_value_t = 1usize, value_name = "KEY_COUNT")]
    keys: usize,
    #[arg(
        long,
        default_value_t = 0f64,
        value_name = "ZIPFIAN_SKEW",
        help = "0 is uniform.",
        short_alias = 'z'
    )]
    skew: f64,
    #[arg(long, value_name = "SHARD_COUNT", help = "Defaults to the key count")]
    shards: Option<usize>,
}

#[derive(clap::ValueEnum, Copy, Clone, Debug, PartialEq)]
enum Algo {
    #[value(name = "kcensus", alias = "KCensus")]
    KCensus,
    #[value(name = "paxos", alias = "Paxos")]
    Paxos,
    #[value(name = "epaxos", alias = "EPaxos")]
    EPaxos,
    #[value(name = "multi-paxos", alias = "Multi-Paxos")]
    MultiPaxos,
    #[value(name = "multi-paxos-3p", alias = "Multi-Paxos-3P")]
    MultiPaxos3P,
    #[value(name = "no-replication", alias = "NoReplication")]
    NoReplication,
    #[value(name = "weak-replication", alias = "WeakReplication")]
    WeakReplication,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum Ingress {
    Exponential,
    Constant,
}

pub async fn run() -> io::Result<()> {
    env_logger::builder()
        .format(|buf, record| {
            let time = Utc::now();
            let level = record.level();
            let level_style = buf.default_level_style(level);
            let header_style = style::AnsiColor::BrightBlack.on_default();
            writeln!(
                buf,
                "{header_style}{}{header_style:#} {level_style}{level:<5}{level_style:#} {}",
                time.format("%M:%S%.6f"),
                record.args()
            )
        })
        .init();

    let args = Args::parse();
    let my_pid = args.pid;
    let topology = Topology::from_path(&args.config, args.non_voting, Some(args.faults));
    let delay_mode = args.simulate_delays.unwrap_or_else(|| {
        topology
            .addresses
            .iter()
            .all(|(address, _)| address == "localhost" || address == "127.0.0.1")
    });

    let epaxos_quorum = (topology.nb_replicas * 3) / 4;
    let algo = match args.algo {
        Algo::EPaxos => {
            if topology.alive_replicas.len() < epaxos_quorum {
                Algo::Paxos
            } else {
                Algo::EPaxos
            }
        }
        x => x,
    };
    debug!("Loaded topology:{topology}");
    let start = Instant::now();
    let propagation_graphs = compute_propagation_graphs(
        topology.clone(),
        algo == Algo::KCensus,
        matches!(algo, Algo::KCensus | Algo::WeakReplication),
    );
    println!("Computed propagation graphs in {:?}", start.elapsed());
    let process_count = topology.regions.len();

    let (consensus_msg_sinks, consensus_msg_streams) =
        connect_all(my_pid, topology.nb_processes, topology.addresses.clone()).await;
    let (delayer, delayed_msg_rx) = Delayer::new();
    let delayer_task = tokio::task::spawn(delayer.run(
        topology.clone(),
        args.speedup,
        my_pid,
        consensus_msg_streams,
        delay_mode,
    ));

    let ((client, mut new_client_request_rx), (app, committed_request_tx)) =
        cassandra::App::new(args.db, args.speedup, my_pid, args.keys).await;

    let start = Instant::now();

    let client_task = tokio::task::spawn(client.run(cassandra::Workload {
        key_distribution:
            rand_distr::Zipf::new(args.keys as f64, args.skew).expect("Incorrect skew"),
        shards: args.shards.unwrap_or(args.keys),
        duration: args.duration,
        warmup: args.warmup,
        warmdown: args.warmdown,
        rw_ratio: args.writes,
        interval: match args.ingress {
            Ingress::Exponential => {
                cassandra::RequestInterval::new_exponential(args.throughput * args.speedup as f32)
            }
            Ingress::Constant => cassandra::RequestInterval::Constant {
                reqs_per_second: args.throughput * args.speedup as f32,
            },
        },
    }));

    match algo {
        Algo::KCensus => {
            println!(
                "Expected local latency (no-contention): {:?}",
                propagation_graphs.kcensus_latencies[my_pid]
            );
            let mut leader_prio: Vec<_> = (0..process_count).collect();
            leader_prio.sort_by_key(|pid| propagation_graphs.kcensus_latencies[*pid]);
            let mut consensus_obj = KCensus::new(
                process_count,
                topology.alive_replicas.len(),
                &topology.alive_replicas,
                my_pid,
                consensus_msg_sinks,
                leader_prio,
                propagation_graphs,
                args.keys,
            );
            let consensus =
                consensus_obj.run(delayed_msg_rx, new_client_request_rx, committed_request_tx);
            let _ = tokio::join!(app.run(), consensus);
        }
        Algo::Paxos => {
            let mut leader_prio: Vec<_> = topology.alive_replicas.iter().collect();
            leader_prio.sort_by_key(|pid| propagation_graphs.paxos_latencies[*pid]);
            let leader = leader_prio[0] == my_pid;
            println!(
                "Expected local latency (no-contention): {:?}",
                propagation_graphs.paxos_latencies[my_pid] / if leader { 2 } else { 1 }
            );
            let mut consensus_obj = PaxosFamily::new(
                process_count,
                topology.nb_replicas,
                &topology.alive_replicas,
                my_pid,
                consensus_msg_sinks,
                leader_prio,
                None,
                Mode::Paxos,
                args.keys,
            );
            let consensus =
                consensus_obj.run(delayed_msg_rx, new_client_request_rx, committed_request_tx);
            let _ = tokio::join!(app.run(), consensus);
        }
        Algo::EPaxos => {
            let mut leader_prio: Vec<_> = topology.alive_replicas.iter().collect();
            leader_prio.sort_by_key(|pid| propagation_graphs.epaxos_latencies[*pid]);
            println!(
                "Expected local latency (no-contention): {:?}",
                propagation_graphs.epaxos_latencies[my_pid]
            );
            let mut consensus_obj = PaxosFamily::new(
                process_count,
                topology.nb_replicas,
                &topology.alive_replicas,
                my_pid,
                consensus_msg_sinks,
                leader_prio,
                None,
                Mode::EPaxos,
                args.keys,
            );
            let consensus =
                consensus_obj.run(delayed_msg_rx, new_client_request_rx, committed_request_tx);
            let _ = tokio::join!(app.run(), consensus);
        }
        Algo::MultiPaxos | Algo::MultiPaxos3P => {
            let is_3p = algo == Algo::MultiPaxos3P;
            let multi_paxos_latencies = if is_3p {
                &propagation_graphs.multi_paxos_3p_latencies
            } else {
                &propagation_graphs.multi_paxos_latencies
            };
            let mut leader_prio: Vec<_> = topology.alive_replicas.iter().collect();
            leader_prio
                .sort_by_cached_key(|pid| multi_paxos_latencies[*pid].iter().sum::<Duration>());
            let leader = leader_prio[0];
            let committers = if is_3p {
                Some(propagation_graphs.multi_paxos_3p_committers[leader].clone())
            } else {
                None
            };
            println!(
                "Expected local latency with leader {} (no-contention): {:?}",
                leader, multi_paxos_latencies[leader][my_pid]
            );
            let mut consensus_obj = PaxosFamily::new(
                process_count,
                topology.nb_replicas,
                &topology.alive_replicas,
                my_pid,
                consensus_msg_sinks,
                leader_prio,
                committers,
                if is_3p {
                    Mode::MultiPaxos3P
                } else {
                    Mode::MultiPaxos
                },
                args.keys,
            );
            let consensus =
                consensus_obj.run(delayed_msg_rx, new_client_request_rx, committed_request_tx);
            let _ = tokio::join!(app.run(), consensus);
        }
        Algo::NoReplication | Algo::WeakReplication => {
            let latency_mock = async {
                // For simplicity, requests will be executed locally after a ping delay.
                // This is a lower bound as this consumes no network + compute is sharded.
                let (rtt, quorum) = match algo {
                    Algo::NoReplication => {
                        // The leader is the node with the lowest median ping.
                        let leader = topology
                            .alive_replicas
                            .iter()
                            .min_by_key(|&potential_leader| {
                                let mut rtts = propagation_graphs.rtts[potential_leader].clone();
                                rtts.sort();
                                rtts[rtts.len() / 2]
                            })
                            .expect("There should be a leader");
                        (propagation_graphs.rtts[leader][my_pid] / args.speedup, 1)
                    }
                    Algo::WeakReplication => {
                        let mut rtts = propagation_graphs.rtts[my_pid].clone();
                        rtts.sort();
                        (rtts[rtts.len() / 2] / args.speedup, rtts.len() / 2)
                    }
                    _ => unreachable!("Algo::(No|Weak)Replication"),
                };
                let mut network_stats = multi_sink::Stats::default();
                let serializer = bincode::DefaultOptions::new();

                // We will send the messages to ourselves.
                use tokio::io::AsyncReadExt;
                use tokio::io::AsyncWriteExt;
                // :0 tells the OS to pick an open port.
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let mut writer = tokio::net::TcpStream::connect(addr).await.unwrap();
                let (mut reader, _addr) = listener.accept().await.unwrap();
                let mut read_buffer = vec![];

                let mut queue: VecDeque<(Command, Instant)> = VecDeque::new();
                let timer = tokio_timerfd::sleep(Duration::ZERO);
                tokio::pin!(timer);
                loop {
                    tokio::select! {
                        maybe_cmd = new_client_request_rx.recv() => {
                            if let Some(cmd) = maybe_cmd {
                                if rtt.is_zero() {
                                    committed_request_tx
                                        .send(cmd)
                                        .await
                                        .expect("Unreplicated server failed to send committed request");
                                    continue;
                                } // Purely local operation
                                let serialized = serializer
                                    .serialize(&cmd)
                                    .expect("Local server failed to serialize command");
                                read_buffer.resize(serialized.len(), 0);
                                for _ in 0..1.max(quorum - 1) {
                                    // No need to send to ourselves.
                                    writer
                                        .write_all(&serialized)
                                        .await
                                        .expect("Local server failed to write command");
                                    network_stats.msg_count += 1;
                                    network_stats.byte_count += serialized.len();
                                    let read = reader
                                        .read(&mut read_buffer)
                                        .await
                                        .expect("Remote server failed to read command");
                                    assert_eq!(read, serialized.len(), "Read a partial command.");
                                    writer
                                        .write_all(&serialized)
                                        .await
                                        .expect("Remote server failed to write reply");
                                    network_stats.msg_count += 1;
                                    network_stats.byte_count += serialized.len();
                                    let read = reader
                                        .read(&mut read_buffer)
                                        .await
                                        .expect("Local server failed to read reply");
                                    assert_eq!(read, serialized.len(), "Read a partial reply.");
                                }
                                let completes_at = Instant::now() + rtt;
                                queue.push_back((cmd, completes_at));
                                timer.as_mut().reset(queue.front().unwrap().1);
                            } else { // The client is done, exit.
                                break;
                            }
                        }
                        _ = &mut timer, if !queue.is_empty() => {
                            let now = Instant::now();
                            // Complete all commands whose timer has expired
                            while let Some((_, completes_at)) = queue.front() {
                                if *completes_at <= now {
                                    let (cmd, _) = queue.pop_front().unwrap();
                                    committed_request_tx.send(cmd).await.expect(
                                        "Unreplicated or weakly replicated server failed to send committed request",
                                    );
                                } else {
                                    break;
                                }
                            }
                            // Reset the timer for the next command in the queue
                            if let Some((_, next_completion)) = queue.front() {
                                timer.as_mut().reset(*next_completion);
                            }
                        }
                    }
                }
                eval::log(
                    "network-done",
                    &format!(
                        "Sent+Received {} messages ({} bytes)",
                        network_stats.msg_count, network_stats.byte_count
                    ),
                    &network_stats,
                );
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
