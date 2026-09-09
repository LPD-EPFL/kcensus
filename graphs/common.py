lavanda = "#a084d1"
blue = "#194881"
green = "#2b7941"
yellow = "#bca01b"
orange = "#a95c30"
red = "#811d27"

ALGORITHMS = {
    # "no-replication": {
    #     "label": "No Rep.",
    #     "color": lavanda,
    #     "lw": 0.7,
    #     "linestyle": "--",
    #     "marker": "",
    #     "markersize": 3.5,
    # },
    # "weak-replication": {
    #     "label": "Min. Effort",
    #     "color": blue,
    #     "lw": 0.9,
    #     "linestyle": ":",
    #     "marker": "",
    #     "markersize": 2.5,
    # },
    "kcensus": {
        "label": "KCensus",
        "color": green,
        "lw": 0.7,
        "linestyle": "-",
        "marker": "x",
        "markersize": 3.2,
        "markeredgewidth": 0.7,
    },
    "swift-paxos": {
        "label": "SwiftPaxos",
        "color": yellow,
        "lw": 0.7,
        "linestyle": "-",
        "marker": "+",
        "markersize": 4,
        "markeredgewidth": 0.7,
    },
    "epaxos": {
        "label": "EPaxos",
        "color": orange,
        "lw": 0.7,
        "linestyle": "-",
        "marker": "o",
        "markersize": 1.8,
        "markeredgewidth": None,
    },
    "pando": {
        "label": "Pando",
        "color": red,
        "lw": 0.7,
        "linestyle": "-",
        "marker": "s",
        "markersize": 1.6,
        "markeredgewidth": None,
    },
    "multi-paxos": {
        "label": "Multi-Paxos",
        "color": lavanda,
        "lw": 0.7,
        "linestyle": "-",
        "marker": "d",
        "markersize": 2.5,
        "markeredgewidth": None,
    },
}

EXTRA_ALGORITHMS = {
    "kcensus conflicts": {
        "label": "KCensus with Conflicts",
        "color": blue,
        "lw": 0.7,
        "linestyle": "-",
        "marker": "p",
        "markersize": 2.4,
    },
}

import argparse

import logparser

parser = argparse.ArgumentParser()
parser.add_argument(
    "-c", "--config", type=str, default="aws-europe-7.toml", help="Topology"
)
parser.add_argument("-w", "--writes", type=float, default=0.5, help="Ratio of writes")
parser.add_argument(
    "-d", "--duration", type=str, default="10s", help="Duration of the experiment in seconds"
)
parser.add_argument(
    "--baseline-duration", type=str, default=None,
    help="Duration of the fault-free runs, when it differs from --duration (exp-2)",
)
parser.add_argument(
    "-i", "--ingress", type=str, default="exponential", help="Type of ingress"
)
parser.add_argument(
    "-t", "--throughput", type=float, default="10", help="Target req/s per client"
)
parser.add_argument(
    "-s", "--speedup", type=int, default="1", help="How much the network was sped up"
)
parser.add_argument(
    "-f", "--faults", type=str, default="", help="Comma-separated list of faults"
)
parser.add_argument(
    "-g", "--geo", type=int, default=0, choices=[0, 1],
    help="Setting this to 1 takes into account that experiments are done on aws servers."
)
parser.add_argument(
    "-k", "--keys", type=int, default=100, help="Key count"
)
parser.add_argument(
    "--nonvoting_weak_replication", type=lambda x: list(map(int, x.split(","))), default=[],
    help="Non-voting processes for weak-replication"
)
parser.add_argument(
    "--nonvoting_paxos", type=lambda x: list(map(int, x.split(","))), default=[], help="Non-voting processes for paxos"
)
parser.add_argument(
    "--nonvoting_epaxos", type=lambda x: list(map(int, x.split(","))), default=[],
    help="Non-voting processes for epaxos"
)
parser.add_argument(
    "--nonvoting_multi_paxos_3p", type=lambda x: list(map(int, x.split(","))), default=[],
    help="Non-voting processes for multi-paxos-3p"
)
parser.add_argument(
    "--nonvoting_kcensus", type=lambda x: list(map(int, x.split(","))), default=[],
    help="Non-voting processes for kcensus"
)
parser.add_argument(
    "--skew", type=float, default=0, help="Zipfian skew"
)
parser.add_argument(
    "--shards", type=int, default=100, help="Shard count"
)
parser.add_argument(
    "--local", action="store_true",
    help="Read ../local-logs (produced by eval.sh) instead of ../logs, and expect the reduced "
         "throughput a local run uses.",
)
parser.add_argument(
    "--log-subdir", type=str, default="",
    help="Optional experiment namespace below the log root (for example, exp-4).",
)
args = parser.parse_args()
if args.baseline_duration is None:
    args.baseline_duration = args.duration

# eval.sh writes to a separate root so a local run can never overwrite the AWS results.
logparser.LOG_DIR = "../local-logs" if args.local else "../logs"
if args.log_subdir:
    logparser.LOG_DIR = f"{logparser.LOG_DIR}/{args.log_subdir.strip('/')}"
# Local figures are prefixed so a local run never overwrites the real ones.
PLOT_PREFIX = "local-" if args.local else ""


def local_duration(nominal, num_replicas, sample_divisor=2):
    """Measurement window a local run used, e.g. "40s".

    Local throughput falls with the replica count, so a fixed window would collect ever fewer
    requests as the deployment grows. eval.sh stretches it to compensate.

    `sample_divisor` says how much of an AWS run's request count is reproduced: 2 (default) for
    half, which is plenty for latency percentiles, and 1 for the resource figures, where compute
    scales with the number of requests processed. Must match `local_duration` in lib.sh exactly.
    """
    if not args.local:
        return nominal
    base = int(str(nominal).rstrip("s"))
    target = (1000 * base) // sample_divisor
    rate = local_throughput(1000, num_replicas)
    return f"{max(base, -(-target // rate))}s"       # ceiling division


def local_throughput(nominal, num_replicas):
    """Total throughput a run actually used.

    Local runs are throttled by (f+1)/2 because every replica shares one machine; with
    f = (n-1)/2 that is a divisor of (n+1)/4. Must match `local_throughput` in lib.sh exactly,
    including the integer division, or the path built here will not exist.
    """
    if not args.local:
        return nominal
    return (nominal * 4) // (num_replicas + 1)


def k_formatter(x, _):
    if x < 1000:
        return int(x)
    return f"{int(x / 1000)}k"


def ki_formatter(x, _):
    if x > (1024 ** 3):
        return f"{int(x / 1024 ** 3)}Gi"
    if x > (1024 ** 2):
        return f"{int(x / 1024 ** 2)}Mi"
    if x > (1024 ** 1):
        return f"{int(x / 1024 ** 1)}Ki"
    return int(x)
