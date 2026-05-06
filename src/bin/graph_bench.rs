use bit_set::BitSet;
use clap::Parser;
use kcensus::consensus::kcensus::propagation::compute_propagation_graphs;
use kcensus::eval;
use kcensus::topology::Topology;
use log::info;
use serde::Serialize;
use std::io::Write;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config: String,
    #[arg(short, long, num_args = 0.., value_delimiter = ',', short_alias = 'v')]
    non_voting: Vec<usize>,
    #[clap(short, long, default_value = "0")]
    fault_count: usize,
    #[clap(long, default_value = "0", short_alias = 'w')]
    min_warmup: u32,
    #[clap(short, long, default_value = "1", short_alias = 's')]
    min_samples: u32,
}

fn main() {
    env_logger::builder()
        .format(|buf, record| {
            let level = record.level();
            let _level_style = buf.default_level_style(level);
            writeln!(
                buf,
                "{_level_style}{level:<5}{_level_style:#} {}",
                record.args()
            )
        })
        .init();
    let args = Args::parse();
    let base_topology = Topology::from_path(&args.config, args.non_voting, None);
    let nb_processes = base_topology.nb_processes;
    let mut faults = Vec::with_capacity(args.fault_count);
    let mut start = Instant::now();
    let mut count: u32 = 0;
    for target in [args.min_warmup, args.min_samples] {
        start = Instant::now();
        count = 0;
        while count < target {
            faults.clear();
            faults.extend(0..args.fault_count);
            loop {
                let mut topology = base_topology.clone();
                topology
                    .alive_replicas
                    .difference_with(&BitSet::from_iter(faults.iter().copied()));
                // println!("faults: {:?}", topology.faults);
                let graph = compute_propagation_graphs(topology, true, true);

                let mp_leader = graph.multi_paxos_leaders[0];
                let mp3p_leader = graph.multi_paxos_3p_leaders[0];
                for proposer in 0..nb_processes {
                    let min_effort = graph.min_effort_latencies[proposer];
                    let kcensus = graph.kcensus_latencies[proposer];
                    let paxos = graph.paxos_latencies[proposer];
                    let swift_paxos = graph.swift_paxos_latencies[proposer];
                    let pando = graph.pando_latencies[proposer];
                    let pando_delegate = graph.pando_delegates[proposer];
                    let epaxos = graph.epaxos_latencies[proposer];
                    let mpaxos = graph.multi_paxos_latencies[mp_leader][proposer];
                    let mpaxos_3p = graph.multi_paxos_3p_latencies[mp3p_leader][proposer];
                    info!("proposer {proposer} ({})", base_topology.regions[proposer],);
                    info!(
                        "  min-effort: {min_effort:?}, kcensus: {kcensus:?}, swift_paxos: {swift_paxos:?}, pando: {pando:?} (delegate: {pando_delegate}), epaxos: {epaxos:?}, multi-paxos: {mpaxos:?}, multi-paxos-3p: {mpaxos_3p:?}, paxos: {paxos:?}",
                    );
                }
                info!(
                    "swift-paxos leader: {} and fixed quorum: {:?}",
                    graph.swift_paxos_leader, graph.swift_paxos_fixed_fast_quorum
                );
                info!("multi-paxos leader: {mp_leader}");
                info!("multi-paxos-3p leader: {mp_leader}");

                let avg_millis = |durations: &[Duration]| {
                    1_000.0 * durations.iter().sum::<Duration>().as_secs_f64()
                        / durations.len() as f64
                };
                info!(
                    "Averages:  min-effort: {:.2}ms, kcensus: {:.2}ms, swift_paxos: {:.2}ms, pando: {:.2}ms, epaxos: {:.2}ms, multi-paxos: {:.2}ms, multi-paxos-3p: {:.2}ms, paxos: {:.2}ms",
                    avg_millis(&graph.min_effort_latencies),
                    avg_millis(&graph.kcensus_latencies),
                    avg_millis(&graph.swift_paxos_latencies),
                    avg_millis(&graph.pando_latencies),
                    avg_millis(&graph.epaxos_latencies),
                    avg_millis(&graph.multi_paxos_latencies[mp_leader]),
                    avg_millis(&graph.multi_paxos_3p_latencies[mp_leader]),
                    avg_millis(&graph.paxos_latencies),
                );
                count += 1;
                if !next_combination(&mut faults, nb_processes) {
                    break;
                }
            }
        }
    }
    let total = start.elapsed();
    eval::log(
        "graph-generation",
        &format!(
            "Average graph generation time: {:?} ({} samples)",
            total / count,
            count
        ),
        &GraphGenerationEvent {
            num_faults: args.fault_count,
            average: total / count,
            count,
        },
    )
}

#[derive(Serialize)]
struct GraphGenerationEvent {
    num_faults: usize,
    average: Duration,
    count: u32,
}

#[inline]
fn next_combination(faults: &mut [usize], nb_nodes: usize) -> bool {
    debug_assert!(faults.len() < nb_nodes);
    let len = faults.len();

    // Optimization: Skip combinations that only change the unused nodes
    for suffix_size in 1..=len {
        let suffix_start = len - suffix_size;
        // Can we move the last "suffix_size" positions?
        if faults[suffix_start] + suffix_size < nb_nodes {
            // Yes: Move them and return
            let new_pos = faults[suffix_start] + 1;
            for j in 0..suffix_size {
                faults[suffix_start + j] = new_pos + j;
            }
            return true;
        }
    }
    false
}
