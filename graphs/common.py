lavanda = "#a084d1"
blue = "#194881"
green = "#2b7941"
yellow = "#bca01b"
orange = "#a95c30"
red = "#811d27"

ALGORITHMS = {
    "no-replication": {
        "label": "No Rep.",
        "color": lavanda,
        "lw": 0.7,
        "linestyle": "--",
        "marker": "",
        "markersize": 3.5,
    },
    "weak-replication": {
        "label": "Min. Effort",
        "color": blue,
        "lw": 0.9,
        "linestyle": ":",
        "marker": "",
        "markersize": 2.5,
    },
    "kcensus": {
        "label": "KCensus",
        "color": green,
        "lw": 0.7,
        "linestyle": "-",
        "marker": "d",
        "markersize": 2.4,
    },
    "epaxos": {
        "label": "EPaxos",
        "color": yellow,
        "lw": 0.7,
        "linestyle": "-",
        "marker": "o",
        "markersize": 2.1,
    },
    "multi-paxos": {
        "label": "Multi-Paxos",
        "color": orange,
        "lw": 0.7,
        "linestyle": "-",
        "marker": "x",
        "markersize": 4,
        "markeredgewidth": 0.7,
    },
    "paxos": {
        "label": "Paxos",
        "color": red,
        "lw": 0.7,
        "linestyle": "-",
        "marker": "s",
        "markersize": 1.4,
    },
}

import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "-c", "--config", type=str, default="aws-europe-7.toml", help="Topology"
)
parser.add_argument("-w", "--writes", type=float, default=0.5, help="Ratio of writes")
parser.add_argument(
    "-r", "--requests", type=int, default=100, help="Number of requests per client"
)
parser.add_argument(
    "-i", "--ingress", type=str, default="round-robin", help="Type of ingress"
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
args = parser.parse_args()
serialized_args = f'-c={args.config.replace("/", "-")}-w={args.writes:g}-r={args.requests}-i={args.ingress}-t={args.throughput:g}-s={args.speedup}-f={args.faults}-g={args.geo}'

def k_formatter(x, _):
    if x < 1000:
        return int(x)
    return f"{int(x / 1000)}k"


def ki_formatter(x, _):
    if x > (1024**3):
        return f"{int(x / 1024 ** 3)}Gi"
    if x > (1024**2):
        return f"{int(x / 1024 ** 2)}Mi"
    if x > (1024**1):
        return f"{int(x / 1024 ** 1)}Ki"
    return int(x)
