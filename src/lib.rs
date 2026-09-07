use crate::consensus::command::Command;
use crate::consensus::deps::{DepConsensus, DepMode};
use crate::consensus::kcensus::KCensus;
use crate::consensus::kcensus::propagation::{
    LatencyTables, epaxos_plan, kcensus_plan, min_effort_plan, multi_paxos_3p_plan,
    multi_paxos_plan, pando_plan, paxos_plan, swift_paxos_plan,
};
use crate::consensus::paxos_family::{PFModeSetting, PaxosFamily};
use crate::delayer::Delayer;
use crate::topology::Topology;
use bincode::Options;
use chrono::prelude::*;
use clap::Parser;
use cpu_time::ProcessTime;
use env_logger::fmt::style;
use log::debug;
use std::collections::VecDeque;
use std::io;
use std::io::Write;
use std::sync::Arc;
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
    #[arg(long, value_parser = humantime::parse_duration, value_name = "WARMUP")]
    warmup: Option<Duration>,
    #[arg(long, value_parser = humantime::parse_duration, value_name = "SUSTAIN")]
    sustain: Option<Duration>,
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
    #[arg(
        long,
        value_name = "SHARD_POOL_SIZE",
        help = "Number of preallocated physical shards, reused by the active logical shards. \
                Defaults to min(shard count, 64)."
    )]
    shard_pool: Option<usize>,
    #[arg(
        long,
        help = "Avoids conflicts by partitioning the keyspace between requesters"
    )]
    conflicts: Option<bool>,
}

#[derive(clap::ValueEnum, Copy, Clone, Debug, PartialEq)]
enum Algo {
    #[value(name = "kcensus", alias = "KCensus")]
    KCensus,
    #[value(name = "paxos", alias = "Paxos")]
    Paxos,
    /// EPaxos*, agreeing on a dependency set per command.
    #[value(name = "epaxos", aliases = ["EPaxos", "epaxos-deps", "EPaxosDeps"])]
    EPaxos,
    /// SwiftPaxos, agreeing on a dependency set per command.
    #[value(
        name = "swift-paxos",
        aliases = ["SwiftPaxos", "swiftpaxos", "swift-paxos-deps", "SwiftPaxosDeps"]
    )]
    SwiftPaxos,
    /// EPaxos encoded over the slot layer: agreement is on which command takes a slot,
    /// with one instance per slot. Superseded by `epaxos`; kept to compare the encodings.
    #[value(name = "epaxos-slots", alias = "EPaxosSlots")]
    EPaxosSlots,
    /// SwiftPaxos over the slot layer. Superseded by `swift-paxos`.
    #[value(name = "swift-paxos-slots", alias = "SwiftPaxosSlots")]
    SwiftPaxosSlots,
    #[value(name = "multi-paxos", alias = "Multi-Paxos")]
    MultiPaxos,
    #[value(name = "multi-paxos-3p", alias = "Multi-Paxos-3P")]
    MultiPaxos3P,
    #[value(name = "pando", alias = "Pando")]
    Pando,
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

pub fn init_logger() {
    env_logger::builder()
        .format(|buf, record| {
            let _time = Utc::now();
            let level = record.level();
            let _level_style = buf.default_level_style(level);
            let _header_style = style::AnsiColor::BrightBlack.on_default();
            writeln!(
                buf,
                "{_header_style}{}{_header_style:#} {_level_style}{level:<5}{_level_style:#} {}",
                _time.format("%M:%S%.6f"),
                record.args()
            )
        })
        .init();
}

pub async fn run() -> io::Result<()> {
    init_logger();
    let args = Args::parse();
    let my_pid = args.pid;
    let topology = Topology::from_path(&args.config, args.non_voting, Some(args.faults));
    let delay_mode = args.simulate_delays.unwrap_or_else(|| {
        topology
            .addresses
            .iter()
            .all(|(address, _)| address == "localhost" || address == "127.0.0.1")
    });
    let shards = args.shards.unwrap_or(args.keys);
    let shard_pool = args
        .shard_pool
        .unwrap_or_else(|| shards.min(consensus::DEFAULT_SHARD_POOL_SIZE));

    let epaxos_quorum = (topology.nb_replicas * 3) / 4;
    let algo = match args.algo {
        Algo::EPaxosSlots => {
            if topology.alive_replicas.len() < epaxos_quorum {
                Algo::Paxos
            } else {
                Algo::EPaxosSlots
            }
        }
        x => x,
    };
    debug!("Loaded topology:{topology}");
    // The latency matrix decides the leaders and quorums, and on AWS it is measured fresh at
    // deploy time, so record it in the log. Pid 0 only: every process holds the same matrix, and
    // 31 copies per run would dominate the log bundle. The delimiters make it easy to lift out --
    // everything between them is a valid config file.
    if my_pid == 0 {
        println!("--- BEGIN topology config.toml ---");
        print!("{}", topology.to_config_toml());
        println!("--- END topology config.toml ---");
    }
    println!("Region: {}", topology.regions[my_pid]);
    let start = Instant::now();
    let tables = LatencyTables::new(
        &topology,
        matches!(algo, Algo::KCensus | Algo::WeakReplication),
    );
    // The only two expensive plans (35ms and 150ms at 31 replicas, against ~1ms for the rest,
    // which are planned in their own arm). They go before `connect_all`, the barrier where
    // processes wait for each other's "Ready", or the run starts desynchronised.
    let kcensus_graphs = (algo == Algo::KCensus).then(|| {
        kcensus_plan(
            topology.clone(),
            &tables,
            &min_effort_plan(&topology, &tables),
        )
    });
    let swift = matches!(algo, Algo::SwiftPaxos | Algo::SwiftPaxosSlots)
        .then(|| swift_paxos_plan(&topology, &tables));
    println!("Computed propagation graphs in {:?}", start.elapsed());
    let process_count = topology.regions.len();

    let ((client, mut new_client_request_rx), (app, committed_request_tx)) =
        cassandra::App::new(args.db, args.speedup, my_pid, shards).await;

    let (consensus_msg_sinks, consensus_msg_streams) = connector::connect_all(
        my_pid,
        topology.clone(),
        matches!(algo, Algo::SwiftPaxosSlots),
    )
    .await;
    let (delayer, delayed_msg_rx) = Delayer::new();

    let delayer_task = tokio::task::spawn(delayer.run(
        topology.clone(),
        args.speedup,
        my_pid,
        consensus_msg_streams,
        delay_mode,
    ));

    let duration = args.duration;
    let warmup = args.warmup.unwrap_or(args.duration / 4);
    let sustain = args.sustain.unwrap_or((warmup * 3) / 4);
    let exp_length = warmup + duration + sustain;
    let deadlock_deadline = ((exp_length * 3) / 2).max(Duration::from_secs(5));
    let no_conflicts = !args.conflicts.unwrap_or(false);
    let partition_keys = if no_conflicts {
        args.keys / process_count
    } else {
        args.keys
    };
    let partition_start = if no_conflicts {
        partition_keys * my_pid
    } else {
        0
    };

    let workload = cassandra::Workload {
        key_distribution: rand_distr::Zipf::new(args.keys as f64, args.skew)
            .expect("Incorrect skew"),
        shards,
        duration,
        warmup,
        sustain,
        rw_ratio: args.writes,
        interval: match args.ingress {
            Ingress::Exponential => {
                if args.throughput != 0f32 {
                    cassandra::RequestInterval::new_exponential(
                        args.throughput * args.speedup as f32,
                    )
                } else {
                    cassandra::RequestInterval::Constant {
                        reqs_per_second: 0f32,
                    }
                }
            }
            Ingress::Constant => cassandra::RequestInterval::Constant {
                reqs_per_second: args.throughput * args.speedup as f32,
            },
        },
        no_conflicts,
        partition_keys,
        partition_start,
        last_key: 0,
    };

    let start = Instant::now();
    let process_start = ProcessTime::try_now().expect("Getting process time failed");

    let expected_latency = match algo {
        Algo::KCensus => {
            let mut leader_prio: Vec<_> = (0..process_count).collect();
            let graphs = kcensus_graphs.expect("planned before the barrier");
            leader_prio.sort_by_key(|pid| graphs.latencies[*pid]);
            let expected_latency = graphs.latencies[my_pid];
            let mut consensus_obj = KCensus::new(
                &topology,
                my_pid,
                consensus_msg_sinks,
                leader_prio,
                graphs,
                shards,
                shard_pool,
            );
            let consensus = consensus_obj.run(
                delayed_msg_rx,
                new_client_request_rx,
                committed_request_tx,
                deadlock_deadline,
            );
            let _ = tokio::join!(app.run(), client.run(workload), consensus);
            expected_latency
        }
        Algo::Paxos => {
            let mut leader_prio: Vec<_> = topology.alive_replicas.iter().collect();
            let paxos = paxos_plan(&topology, &tables);
            leader_prio.sort_by_key(|pid| paxos.latencies[*pid]);
            let committers = paxos.committers;
            let mut consensus_obj = PaxosFamily::new(
                &topology,
                my_pid,
                consensus_msg_sinks,
                leader_prio,
                Some(committers),
                PFModeSetting::Paxos,
                shards,
                shard_pool,
            );
            let consensus = consensus_obj.run(
                delayed_msg_rx,
                new_client_request_rx,
                committed_request_tx,
                deadlock_deadline,
            );
            let _ = tokio::join!(app.run(), client.run(workload), consensus);
            paxos.latencies[my_pid]
        }
        Algo::Pando => {
            let mut leader_prio: Vec<_> = topology.alive_replicas.iter().collect();
            let pando = pando_plan(&topology, &tables);
            leader_prio.sort_by_key(|pid| pando.latencies[*pid]);
            let committers = pando.committers;
            let mut consensus_obj = PaxosFamily::new(
                &topology,
                my_pid,
                consensus_msg_sinks,
                leader_prio,
                Some(committers),
                PFModeSetting::Pando {
                    delegates: Arc::new(pando.delegates.clone()),
                },
                shards,
                shard_pool,
            );
            let consensus = consensus_obj.run(
                delayed_msg_rx,
                new_client_request_rx,
                committed_request_tx,
                deadlock_deadline,
            );
            let _ = tokio::join!(app.run(), client.run(workload), consensus);
            pando.latencies[my_pid]
        }
        Algo::EPaxos | Algo::SwiftPaxos => {
            let (mode, expected_latency) = if algo == Algo::EPaxos {
                let epaxos = epaxos_plan(&topology, &tables, &paxos_plan(&topology, &tables));
                (
                    DepMode::EPaxos {
                        coordinator: epaxos.committers[my_pid],
                    },
                    epaxos.latencies[my_pid],
                )
            } else {
                let swift = swift.as_ref().expect("planned before the barrier");
                (
                    DepMode::SwiftPaxos {
                        leader: swift.leader,
                        quorum: Arc::new(swift.fixed_fast_quorum.clone()),
                    },
                    swift.latencies[my_pid],
                )
            };
            let mut consensus_obj = DepConsensus::new(
                &topology,
                my_pid,
                consensus_msg_sinks,
                mode,
                shards,
                shard_pool,
            );
            let consensus = consensus_obj.run(
                delayed_msg_rx,
                new_client_request_rx,
                committed_request_tx,
                deadlock_deadline,
            );
            let _ = tokio::join!(app.run(), client.run(workload), consensus);
            expected_latency
        }
        Algo::EPaxosSlots => {
            let mut leader_prio: Vec<_> = topology.alive_replicas.iter().collect();
            let epaxos = epaxos_plan(&topology, &tables, &paxos_plan(&topology, &tables));
            leader_prio.sort_by_key(|pid| epaxos.latencies[*pid]);
            let committers = epaxos.committers;
            let mut consensus_obj = PaxosFamily::new(
                &topology,
                my_pid,
                consensus_msg_sinks,
                leader_prio,
                Some(committers),
                PFModeSetting::EPaxos,
                shards,
                shard_pool,
            );
            let consensus = consensus_obj.run(
                delayed_msg_rx,
                new_client_request_rx,
                committed_request_tx,
                deadlock_deadline,
            );
            let _ = tokio::join!(app.run(), client.run(workload), consensus);
            epaxos.latencies[my_pid]
        }
        Algo::SwiftPaxosSlots => {
            let swift = swift.expect("planned before the barrier");
            let mut consensus_obj = PaxosFamily::new(
                &topology,
                my_pid,
                consensus_msg_sinks,
                vec![swift.leader],
                None,
                PFModeSetting::SwiftPaxos {
                    quorum: Arc::new(swift.fixed_fast_quorum.clone()),
                },
                shards,
                shard_pool,
            );
            let consensus = consensus_obj.run(
                delayed_msg_rx,
                new_client_request_rx,
                committed_request_tx,
                deadlock_deadline,
            );
            let _ = tokio::join!(app.run(), client.run(workload), consensus);
            println!("SwiftPaxos fixed quorum: {:?}", swift.fixed_fast_quorum);
            println!("SwiftPaxos leader: {:?}", swift.leader);
            println!(
                "Force MPaxos3P at this replica: {:?}",
                swift.force_mpaxos.contains(my_pid)
            );
            swift.latencies[my_pid]
        }
        Algo::MultiPaxos | Algo::MultiPaxos3P => {
            let is_3p = algo == Algo::MultiPaxos3P;
            let three_phase = is_3p.then(|| multi_paxos_3p_plan(&topology, &tables));
            let two_phase = (!is_3p).then(|| multi_paxos_plan(&topology, &tables));
            let (multi_paxos_latencies, leader_prio) = match (&three_phase, &two_phase) {
                (Some(plan), _) => (&plan.latencies, plan.leaders.clone()),
                (_, Some(plan)) => (&plan.latencies, plan.leaders.clone()),
                _ => unreachable!("exactly one of the two is planned"),
            };
            let leader = leader_prio[0];
            let committers = three_phase
                .as_ref()
                .map(|plan| plan.committers[leader].clone());
            let mut consensus_obj = PaxosFamily::new(
                &topology,
                my_pid,
                consensus_msg_sinks,
                leader_prio,
                committers,
                if is_3p {
                    PFModeSetting::MultiPaxos3P
                } else {
                    PFModeSetting::MultiPaxos
                },
                shards,
                shard_pool,
            );
            let consensus = consensus_obj.run(
                delayed_msg_rx,
                new_client_request_rx,
                committed_request_tx,
                deadlock_deadline,
            );
            let _ = tokio::join!(app.run(), client.run(workload), consensus);
            println!("Leader: {leader}");
            multi_paxos_latencies[leader][my_pid]
        }
        Algo::NoReplication | Algo::WeakReplication => {
            let (rtt, messages_to_send, leader) = match algo {
                Algo::NoReplication => {
                    // The leader is the node with the lowest average ping.
                    let leader = topology
                        .alive_replicas
                        .iter()
                        .min_by_key(|&potential_leader| {
                            tables.link_rtts[potential_leader].iter().sum::<Duration>()
                        })
                        .expect("There should be a leader");
                    (
                        tables.link_rtts[leader][my_pid] / args.speedup,
                        (leader != my_pid) as usize,
                        Some(leader),
                    )
                }
                Algo::WeakReplication => {
                    let majority = 1 + (topology.nb_replicas / 2);
                    let to_send = majority - topology.alive_replicas.contains(my_pid) as usize;
                    (
                        min_effort_plan(&topology, &tables).latencies[my_pid],
                        to_send,
                        None,
                    )
                }
                _ => unreachable!("Algo::(No|Weak)Replication"),
            };

            let latency_mock = async {
                // For simplicity, requests will be executed locally after a ping delay.
                // This is a lower bound as this consumes no network + compute is sharded.
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
                                if rtt.is_zero() && committed_request_tx.capacity() > 0 {
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

                                // TODO: this is for tput, but in latency we should only pay 1 send/recv
                                for _ in 0..messages_to_send {
                                    writer
                                        .write_all(&serialized)
                                        .await
                                        .expect("Local server failed to write command");
                                    network_stats.msg_count += 1;
                                    network_stats.byte_count += serialized.len();
                                }
                                for _ in 0..messages_to_send {
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
                                }
                                for _ in 0..messages_to_send {
                                    let read = reader
                                        .read(&mut read_buffer)
                                        .await
                                        .expect("Local server failed to read reply");
                                    assert_eq!(read, serialized.len(), "Read a partial reply.");
                                }
                                let completes_at = Instant::now() + rtt;
                                queue.push_back((cmd, completes_at));
                                if queue.len() == 1 {
                                    timer.as_mut().reset(queue.front().unwrap().1);
                                }
                            } else { // The client is done, exit.
                                break;
                            }
                        }
                        res = &mut timer, if !queue.is_empty() => {
                            res.expect("failed to wait");
                            let now = Instant::now();
                            // Complete all commands whose timer has expired
                            while let Some((_, completes_at)) = queue.front() {
                                if *completes_at <= now && committed_request_tx.capacity() > 0 {
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
                                // We need to give the app the opportunity to process the request,
                                // otherwise we might re-trigger the timer directly and prevent progress.
                                if committed_request_tx.capacity() == 0 {
                                    tokio::task::yield_now().await
                                }
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
            let _ = tokio::join!(app.run(), client.run(workload), latency_mock);
            if let Some(leader) = leader {
                println!("Leader: {leader}");
            }
            rtt
        }
    };
    let process_runtime = process_start
        .try_elapsed()
        .expect("Getting process time failed");
    println!(
        "[log=time] process time (in seconds) | {{\"user\": {}}}",
        process_runtime.as_secs_f64()
    );
    println!("Expected local latency (no-contention): {expected_latency:?}",);
    println!("Total duration: {:?}", start.elapsed());
    println!("Region: {}", topology.regions[my_pid]);

    delayer_task.await?;
    Ok(())
}
