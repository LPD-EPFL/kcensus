use bit_set::BitSet;
use clap::Parser;
use kcensus::consensus::kcensus::propagation::compute_propagation_graphs;
use kcensus::eval;
use kcensus::topology::Topology;
use log::info;
use serde::Serialize;
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
    env_logger::init();
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
                let mut total_kcensus = Duration::ZERO;
                let mut total_paxos = Duration::ZERO;
                let mut total_epaxos = Duration::ZERO;
                let mut total_mpaxos = Duration::ZERO;
                let mut total_mpaxos_3p = Duration::ZERO;
                for proposer in 0..nb_processes {
                    let leader = graph.multi_paxos_leaders[0];
                    let leader_3p = graph.multi_paxos_3p_leaders[0];
                    let committer_3p = graph.multi_paxos_3p_committers[leader_3p][proposer];

                    let kcensus = graph.kcensus_latencies[proposer];
                    total_kcensus += kcensus;
                    let paxos = graph.paxos_latencies[proposer];
                    total_paxos += paxos;
                    let epaxos = graph.epaxos_latencies[proposer];
                    total_epaxos += epaxos;
                    let mpaxos = graph.multi_paxos_latencies[leader][proposer];
                    total_mpaxos += mpaxos;
                    let mpaxos_3p = graph.multi_paxos_3p_latencies[leader_3p][proposer];
                    total_mpaxos_3p += mpaxos_3p;
                    info!("proposer {proposer} ({})", base_topology.regions[proposer],);
                    info!(
                        "  paxos: {paxos:?}, epaxos: {epaxos:?}, multi-paxos: {mpaxos:?}, multi-paxos-3p: {mpaxos_3p:?} (committer {committer_3p})"
                    );
                }
                let to_avg_millis =
                    |total: Duration| (total.as_nanos() as f64) / 10000000.0 / nb_processes as f64;
                info!(
                    "Averages: kcensus: {:.2}ms paxos: {:.2} epaxos: {:.2}ms multi-paxos: {:.2}ms multi-paxos-3p: {:.2}ms",
                    to_avg_millis(total_kcensus),
                    to_avg_millis(total_paxos),
                    to_avg_millis(total_epaxos),
                    to_avg_millis(total_mpaxos),
                    to_avg_millis(total_mpaxos_3p),
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

    // Optimisation: Skip combinations that only change the unused nodes
    for suffix_size in 1..=len {
        let suffix_start = len - suffix_size;
        // Can we move the last "suffix_size" positions ?
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
