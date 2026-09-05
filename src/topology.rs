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
    pub nb_processes: usize,
    pub nb_replicas: usize,
    pub alive_replicas: BitSet,
    pub regions: Vec<String>,
    link_latencies: Vec<Vec<Duration>>,
    pub addresses: Vec<(String, u16)>,
    /// The topology as a pasteable config file -- see `to_config_toml`.
    config_toml: String,
}

impl Display for Topology {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for i in 0..self.regions.len() {
            if self.alive_replicas.contains(i) {
                write!(f, "\n{i}: \"{}\" (alive replicas):", self.regions[i])?;
            } else {
                write!(f, "\n{i}: \"{}\":", self.regions[i])?;
            }
            write!(f, "\n  - link_latencies: {:?}", self.link_latencies[i])?;
        }
        Ok(())
    }
}

impl Topology {
    pub fn from_path(
        toml_path: &String,
        non_voting: Vec<usize>,
        faults: Option<Vec<usize>>,
    ) -> Self {
        let mut file = File::open(toml_path).expect("Failed to open toml config");
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .expect("Failed to read toml config");
        debug!("Loaded config:\n{contents}");
        let config: Config = toml::from_str(&contents).expect("Failed to parse toml config");
        Self::from_config(config, non_voting, faults)
    }

    fn from_config(config: Config, non_voting: Vec<usize>, faults: Option<Vec<usize>>) -> Self {
        let Config {
            raw_latencies,
            regions,
            addresses,
        } = config;
        let nb_processes = regions.len();
        assert_eq!(nb_processes, raw_latencies.len());
        let config_toml = render_config_toml(&regions, &raw_latencies);
        let addresses: Vec<(String, u16)> = match addresses {
            Some(x) => x,
            None => (0..nb_processes)
                .map(|i| ("localhost".into(), (8000 + i) as u16))
                .collect(),
        };
        assert_eq!(nb_processes, addresses.len());

        // Compute raw_latencies
        let mut link_latencies = vec![vec![Duration::default(); nb_processes]; nb_processes];
        for src in 0..nb_processes {
            for dest in 0..nb_processes {
                let nanos = raw_latencies[src][dest] * 1000. * 1000.;
                link_latencies[src][dest] = Duration::from_nanos(nanos.round() as u64);
            }
        }

        let faults = match faults {
            Some(faults) => BitSet::from_iter(faults),
            None => BitSet::with_capacity(nb_processes),
        };

        let replicas = BitSet::from_iter((0..nb_processes).filter(|i| !non_voting.contains(i)));
        let nb_replicas = replicas.len();
        assert_eq!(nb_replicas, nb_processes - non_voting.len());

        let mut alive_replicas = replicas;
        alive_replicas.difference_with(&faults);

        Topology {
            nb_processes,
            nb_replicas,
            alive_replicas,
            regions,
            link_latencies,
            addresses,
            config_toml,
        }
    }

    pub fn link_latency(&self, src: usize, dest: usize) -> Duration {
        self.link_latencies[src][dest]
    }

    /// The topology as a config file, ready to be pasted into `configs/`.
    ///
    /// On AWS the latency matrix is measured by pinging the instances at deploy time
    /// (`deployment/ansible/02-generate-configs.yml`), so it is different for every deployment and
    /// exists nowhere once the instances are gone. Recording it in the run's log is what lets the
    /// same delays be replayed locally afterwards -- and the protocols pick their leaders and
    /// quorums by taking a minimum over this matrix, so a few percent of difference is enough to
    /// flip a choice and move the results.
    pub fn to_config_toml(&self) -> &str {
        &self.config_toml
    }
}

/// Render `regions` and `raw_latencies` as a config file, in the same shape as the files in
/// `configs/`.
///
/// Built from the values as they were parsed, before any conversion to `Duration`, so the output
/// round-trips exactly rather than by luck of float formatting.
///
/// `addresses` is deliberately left out. Without it every process defaults to
/// `localhost:8000+pid`, which is what a local run wants -- whereas the `config.toml` generated on
/// an AWS node names that deployment's instances, and pasting those into `configs/` would send a
/// local replay off to hosts that no longer exist.
fn render_config_toml(regions: &[String], raw_latencies: &[Vec<f64>]) -> String {
    let mut out = String::from("regions = [\n");
    for region in regions {
        // Single quotes are TOML literal strings: nothing to escape. Region names come from the
        // AWS region list and contain no quote anyway.
        out.push_str(&format!("    '{}',\n", region.replace('\'', "")));
    }
    out.push_str("]\nraw_latencies = [\n");
    for row in raw_latencies {
        let cells: Vec<String> = row.iter().map(|ms| ms.to_string()).collect();
        out.push_str(&format!("    [{}],\n", cells.join(", ")));
    }
    out.push_str("]\n");
    out
}
