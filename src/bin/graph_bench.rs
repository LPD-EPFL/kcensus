use bit_set::BitSet;
use clap::Parser;
use kcensus::consensus::kcensus::propagation::{
    LatencyTables, epaxos_plan, kcensus_plan, min_effort_plan, multi_paxos_3p_plan,
    multi_paxos_plan, pando_plan, paxos_plan, swift_paxos_plan,
};
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
    /// Plan every algorithm and report all of their latencies. Off by default: this benchmark
    /// measures KCensus' graph generation (Figure 12)
    #[clap(long, alias = "all")]
    all_algos: bool,
    /// Leave SwiftPaxos out of `--all-algos`. Its quorum search is the expensive one -- ~150ms
    /// at 31 replicas against ~1ms for the others -- so it is worth skipping when the run is
    /// repeated for timing rather than for the latencies themselves.
    #[clap(long)]
    skip_swiftpaxos: bool,
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
                let tables = LatencyTables::new(&topology, true);
                let min_effort = min_effort_plan(&topology, &tables);
                let kcensus = kcensus_plan(topology.clone(), &tables, &min_effort);

                if args.all_algos {
                    let paxos = paxos_plan(&topology, &tables);
                    let epaxos = epaxos_plan(&topology, &tables, &paxos);
                    let pando = pando_plan(&topology, &tables);
                    let multi_paxos = multi_paxos_plan(&topology, &tables);
                    let multi_paxos_3p = multi_paxos_3p_plan(&topology, &tables);
                    // `None` rather than a plan full of `Duration::MAX`: the averages below
                    // sum these, and summing `MAX` panics with an overflow.
                    let swift =
                        (!args.skip_swiftpaxos).then(|| swift_paxos_plan(&topology, &tables));
                    let or_skipped = |value: Option<String>| value.unwrap_or("skipped".into());

                    let mp_leader = multi_paxos.leaders[0];
                    let mp3p_leader = multi_paxos_3p.leaders[0];
                    for proposer in 0..nb_processes {
                        let min_effort = min_effort.latencies[proposer];
                        let kcensus = kcensus.latencies[proposer];
                        let paxos = paxos.latencies[proposer];
                        let swift_paxos = or_skipped(
                            swift
                                .as_ref()
                                .map(|p| format!("{:?}", p.latencies[proposer])),
                        );
                        let pando_latency = pando.latencies[proposer];
                        let pando_delegate = pando.delegates[proposer];
                        let epaxos = epaxos.latencies[proposer];
                        let mpaxos = multi_paxos.latencies[mp_leader][proposer];
                        let mpaxos_3p = multi_paxos_3p.latencies[mp3p_leader][proposer];
                        info!("proposer {proposer} ({})", base_topology.regions[proposer],);
                        info!(
                            "  min-effort: {min_effort:?}, kcensus: {kcensus:?}, swift_paxos: {swift_paxos}, pando: {pando_latency:?} (delegate: {pando_delegate}), epaxos: {epaxos:?}, multi-paxos: {mpaxos:?}, multi-paxos-3p: {mpaxos_3p:?}, paxos: {paxos:?}",
                        );
                    }
                    match &swift {
                        Some(plan) => info!(
                            "swift-paxos leader: {} and fixed quorum: {:?}",
                            plan.leader, plan.fixed_fast_quorum
                        ),
                        None => info!("swift-paxos: skipped"),
                    }
                    info!("multi-paxos leader: {mp_leader}");
                    info!("multi-paxos-3p leader: {mp_leader}");

                    let avg_millis = |durations: &[Duration]| {
                        1_000.0 * durations.iter().sum::<Duration>().as_secs_f64()
                            / durations.len() as f64
                    };
                    if args.min_warmup == 0 && args.min_samples == 1 {
                        info!("Faults: {faults:?}");
                        info!(
                            "Averages:  min-effort: {:.2}ms, kcensus: {:.2}ms, swift_paxos: {}, pando: {:.2}ms, epaxos: {:.2}ms, multi-paxos: {:.2}ms, multi-paxos-3p: {:.2}ms, paxos: {:.2}ms",
                            avg_millis(&min_effort.latencies),
                            avg_millis(&kcensus.latencies),
                            or_skipped(
                                swift
                                    .as_ref()
                                    .map(|p| format!("{:.2}ms", avg_millis(&p.latencies)))
                            ),
                            avg_millis(&pando.latencies),
                            avg_millis(&epaxos.latencies),
                            avg_millis(&multi_paxos.latencies[mp_leader]),
                            avg_millis(&multi_paxos_3p.latencies[mp_leader]),
                            avg_millis(&paxos.latencies),
                        );
                    }
                } else {
                    for proposer in 0..nb_processes {
                        info!(
                            "proposer {proposer} ({}): kcensus: {:?}",
                            base_topology.regions[proposer], kcensus.latencies[proposer]
                        );
                    }
                }
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
