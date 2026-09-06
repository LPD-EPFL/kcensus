# Source map

The `kcensus` executable starts in [main.rs](main.rs); [lib.rs](lib.rs) parses the command-line
options and connects the topology, protocol, workload, and application.

| Component | Source | Relation to the paper |
|---|---|---|
| KCensus protocol | [consensus/kcensus.rs](consensus/kcensus.rs) | Fast-path execution and fallback to Paxos (§4). |
| Knowledge and per-slot state | [consensus/kcensus/node_state.rs](consensus/kcensus/node_state.rs), [consensus/kcensus/round_state.rs](consensus/kcensus/round_state.rs) | Tracks acceptance evidence and implements commit/adopt checks (§4). |
| Requirements optimizer | [consensus/kcensus/propagation.rs](consensus/kcensus/propagation.rs) | Computes compatible knowledge requirements and propagation schedules (§5). |
| Replication runtime | [consensus.rs](consensus.rs) | Shared sharding, log slots, command ordering, reads, and recovery machinery used by KSMR (§6) and the baselines. |
| Baseline protocols | [consensus/paxos_family.rs](consensus/paxos_family.rs) | Shared implementation of the Paxos-family comparison protocols (§7). |
| Key-value application and workload | [cassandra.rs](cassandra.rs) | Generates requests and executes committed commands through Cassandra (deprecated), or mocks application execution when no database is configured as in the paper (§6–7). |
| Optimizer benchmark | [bin/graph_bench.rs](bin/graph_bench.rs) | Measures requirements computation time for Figure 12. |

[topology.rs](topology.rs) loads regions, addresses, and latency matrices.
[connector.rs](connector.rs) establishes TCP connections, [multi_sink.rs](multi_sink.rs)
handles message sending and traffic accounting, and [delayer.rs](delayer.rs) simulates link delays
when all replicas run on a single machine.
[eval.rs](eval.rs) emits structured events consumed by the plotting scripts.

Experiment definitions live in [../lib.sh](../lib.sh), deployment tooling in
[../deployment/](../deployment/), topology inputs in [../configs/](../configs/), and analysis
scripts in [../graphs/](../graphs/). See the [main README](../README.md) for build and run commands.
