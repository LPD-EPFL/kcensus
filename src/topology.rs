use bit_set::BitSet;
use log::debug;
use serde::Deserialize;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::Read;
use std::time::Duration;

const FAULTY_LATENCY_SECS: u64 = 1000000;
pub const FAULTY_LATENCY: Duration = Duration::from_secs(FAULTY_LATENCY_SECS);

#[derive(Deserialize, Debug)]
pub struct Config {
    pub regions: Vec<String>,
    pub raw_latencies: Vec<Vec<f64>>,
    pub addresses: Option<Vec<(String, u16)>>,
}

#[derive(Debug, Clone)]
pub struct Topology {
    pub nb_nodes: usize,
    pub regions: Vec<String>,
    link_latencies: Vec<Vec<Duration>>,
    pub faults: BitSet,
    pub addresses: Vec<(String, u16)>,
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
        }
        Ok(())
    }
}

impl Topology {
    pub fn from_path(toml_path: &String, faults: Option<Vec<usize>>) -> Self {
        let mut file = File::open(toml_path).expect("Failed to open toml config");
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .expect("Failed to read toml config");
        debug!("Loaded config:\n{contents}");
        let config: Config = toml::from_str(&contents).expect("Failed to parse toml config");
        Self::from_config(config, faults)
    }

    fn from_config(config: Config, faults: Option<Vec<usize>>) -> Self {
        let Config {
            raw_latencies,
            regions,
            addresses,
        } = config;
        let nb_nodes = regions.len();
        assert_eq!(nb_nodes, raw_latencies.len());
        let addresses: Vec<(String, u16)> = match addresses {
            Some(x) => x,
            None => (0..nb_nodes)
                .map(|i| ("localhost".into(), (8000 + i) as u16))
                .collect(),
        };
        assert_eq!(nb_nodes, addresses.len());

        // Compute raw_latencies
        let mut link_latencies = vec![vec![Duration::default(); nb_nodes]; nb_nodes];
        for src in 0..nb_nodes {
            for dest in 0..nb_nodes {
                let nanos = raw_latencies[src][dest] * 1000. * 1000.;
                link_latencies[src][dest] = Duration::from_nanos(nanos.round() as u64);
            }
        }

        let faults = match faults {
            Some(faults) => BitSet::from_iter(faults),
            None => BitSet::with_capacity(nb_nodes),
        };

        Topology {
            nb_nodes,
            regions,
            link_latencies,
            faults,
            addresses,
        }
    }

    pub fn link_latency(&self, src: usize, dest: usize) -> Duration {
        if self.faults.contains(src) || self.faults.contains(dest) {
            FAULTY_LATENCY
        } else {
            self.link_latencies[src][dest]
        }
    }
}
