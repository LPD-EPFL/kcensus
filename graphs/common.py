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

parser = argparse.ArgumentParser()
parser.add_argument(
    "-c", "--config", type=str, default="aws-europe-7.toml", help="Topology"
)
parser.add_argument("-w", "--writes", type=float, default=0.5, help="Ratio of writes")
parser.add_argument(
    "-d", "--duration", type=str, default="10s", help="Duration of the experiment in seconds"
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
args = parser.parse_args()


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
