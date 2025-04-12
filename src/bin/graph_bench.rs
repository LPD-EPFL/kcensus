use std::time::Instant;
use clap::Parser;
use kcensus::consensus::kcensus::propagation::PropagationGraphs;
use kcensus::topology::Topology;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config: String,
    #[clap(short, long)]
    faults: usize,
    #[clap(long, default_value = "1000")]
    min_warmup: u32,
    #[clap(short, long, default_value = "1000")]
    min_samples: u32,
}

fn main() {
    env_logger::init();
    let args = Args::parse();
    let mut topology = Topology::from_path(&args.config, None);
    let nb_nodes = topology.nb_nodes;
    let mut faults = Vec::with_capacity(args.faults);
    let mut start = Instant::now();
    let mut count: u32 = 0;
    for target in [args.min_warmup, args.min_samples] {
        start = Instant::now();
        count = 0;
        while count < target {
            faults.clear();
            faults.extend(0..args.faults);
            loop {
                topology.faults.clear();
                topology.faults.extend(faults.iter().copied());
                // println!("faults: {:?}", topology.faults);
                let _: PropagationGraphs = (&topology).into();
                count += 1;
                if !next_combination(&mut faults, nb_nodes) {
                    break
                }
            }
        }
    }
    println!("Average graph generation time: {:?} ({} samples)", start.elapsed() / count, count);
}

#[inline]
fn next_combination(faults: &mut Vec<usize>, nb_nodes: usize) -> bool {
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