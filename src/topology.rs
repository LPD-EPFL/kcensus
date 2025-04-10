use bit_set::BitSet;
use log::debug;
use petgraph::algo::bellman_ford;
use petgraph::matrix_graph::DiMatrix;
use serde::Deserialize;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::Read;
use std::time::Duration;

type NetworkGraph = DiMatrix<(), f64, Option<f64>, usize>;

const FAULTY_LATENCY_SECS: u64 = 1000000;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub regions: Vec<String>,
    pub raw_latencies: Vec<Vec<f64>>,
}

#[derive(Debug, Clone)]
pub struct Topology {
    pub nb_nodes: usize,
    pub regions: Vec<String>,
    pub link_latencies: Vec<Vec<Duration>>,
    pub path_latencies: Vec<Vec<Duration>>,
    pub prev_dest: Vec<Vec<usize>>,
    pub next_src: Vec<Vec<usize>>,
    pub rtts: Vec<Vec<Duration>>,
    pub faults: BitSet,
}

impl Display for Topology {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for i in 0..self.regions.len() {
            if self.faults.contains(i) {
                write!(f, "\n{i}: \"{}\" (DEAD):", self.regions[i])?;
            } else {
                write!(f, "\n{i}: \"{}\":", self.regions[i])?;
            }
            write!(f, "\n  - link_latencies: {:?}", self.link_latencies[i])?;
            write!(f, "\n  - path_latencies: {:?}", self.path_latencies[i])?;
            write!(f, "\n  - next_src: {:?}", self.next_src[i])?;
            write!(f, "\n  - prev_dest: {:?}", self.prev_dest[i])?;
            write!(f, "\n  - rtts: {:?}", self.rtts[i])?;
        }
        Ok(())
    }
}

impl Topology {
    pub fn from_path(toml_path: &String, faults: Vec<usize>) -> Self {
        let mut file = File::open(toml_path).expect("Failed to open toml config");
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .expect("Failed to read toml config");
        debug!("Loaded config:\n{}", contents);
        let config: Config = toml::from_str(&contents).expect("Failed to parse toml config");
        Self::from_config(config, faults)
    }

    fn from_config(config: Config, faults: Vec<usize>) -> Self {
        let faults = BitSet::from_iter(faults);
        let Config {
            raw_latencies,
            regions,
        } = config;
        let nb_nodes = regions.len();
        assert_eq!(nb_nodes, raw_latencies.len());
        // Generate graph nodes
        let mut graph = NetworkGraph::with_capacity(nb_nodes);
        for _ in 0..nb_nodes {
            graph.add_node(());
        }

        // Compute raw_latencies and generate graph edges
        let mut link_latencies = vec![vec![Duration::default(); nb_nodes]; nb_nodes];
        for src in 0..nb_nodes {
            assert_eq!(nb_nodes, raw_latencies[src].len());
            for dest in 0..nb_nodes {
                if faults.contains(src) || faults.contains(dest) {
                    link_latencies[src][dest] = Duration::from_secs(FAULTY_LATENCY_SECS);
                    continue;
                }
                let nanos = (raw_latencies[src][dest] * 1000. * 1000.).round();
                link_latencies[src][dest] = Duration::from_nanos(nanos as u64);
                if src != dest {
                    graph.add_edge(src.into(), dest.into(), nanos);
                }
            }
        }

        let mut path_latencies = vec![vec![Duration::default(); nb_nodes]; nb_nodes];
        let mut prev_dest = vec![vec![0; nb_nodes]; nb_nodes];
        let mut next_src = vec![vec![0; nb_nodes]; nb_nodes];

        // Compute the shortest paths
        for src in 0..nb_nodes {
            let paths = bellman_ford(&graph, src.into()).expect("Latencies can not be negative");
            for dest in 0..nb_nodes {
                let mut last_pred = dest;
                let mut pred = paths.predecessors[dest].unwrap_or(src.into()).index();
                prev_dest[src][dest] = pred;
                while pred != src {
                    last_pred = pred;
                    pred = paths.predecessors[last_pred].unwrap_or(src.into()).index();
                }
                next_src[src][dest] = last_pred;
                let nanos = paths.distances[dest];
                if paths.distances[dest] > u64::MAX as f64 {
                    path_latencies[src][dest] = Duration::from_secs(FAULTY_LATENCY_SECS);
                } else {
                    path_latencies[src][dest] = Duration::from_nanos(nanos.round() as u64);
                }
            }
        }

        let rtts = (0..nb_nodes)
            .map(|src| {
                (0..nb_nodes)
                    .map(|dest| link_latencies[src][dest] + link_latencies[dest][src])
                    .collect()
            })
            .collect();

        Topology {
            nb_nodes,
            regions,
            link_latencies,
            path_latencies,
            prev_dest,
            next_src,
            rtts,
            faults,
        }
    }
}
