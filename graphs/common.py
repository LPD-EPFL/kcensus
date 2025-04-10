blue = '#4C72B0'
red = '#C44E52'
orange = '#DD8452'
yellow = '#E2C44C'
green = '#55A868'
lavanda = '#BEA9DF'

ALGORITHMS = {
    'no-replication': {'label': 'No Rep.', 'color': blue, 'lw': 0.8, 'marker': '', 'markersize': 3.5},
    'paxos': {'label': 'Paxos', 'color': red, 'lw': 0.8, 'marker': 'o', 'markersize': 3.5},
    'multi-paxos': {'label': 'Multi-Paxos', 'color': orange, 'lw': 0.8, 'marker': 's', 'markersize': 3.5},
    'e-paxos': {'label': 'EPaxos', 'color': yellow, 'lw': 0.8, 'marker': 'x', 'markersize': 3.5},
    'k-census': {'label': 'K-Census', 'color': green, 'lw': 0.8, 'marker': '>', 'markersize': 3.5},
    'weak-replication': {'label': 'Weak Rep.', 'color': lavanda, 'lw': 0.8, 'marker': '^', 'markersize': 3.5},
}

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', type=str, default='aws-europe-7.toml', help='Topology')
parser.add_argument('-w', '--writes', type=float, default=0.5, help='Ratio of writes')
parser.add_argument('-r', '--requests', type=int, default=100, help='Number of requests per client')
parser.add_argument('-i', '--ingress', type=str, default='round-robin', help='Type of ingress')
parser.add_argument('-t', '--throughput', type=float, default='10', help='Target req/s per client')
parser.add_argument('-s', '--speedup', type=int, default='1', help='How much the network was sped up')
args = parser.parse_args()
serialized_args = f'-c={args.config}-w={args.writes:g}-r={args.requests}-i={args.ingress}-t={args.throughput:g}-s={args.speedup}'


def k_formatter(x, _):
    if x < 1000: return int(x)
    return f'{int(x / 1000)}k'


def ki_formatter(x, _):
    if x > (1024 ** 3): return f'{int(x / 1024 ** 3)}Gi'
    if x > (1024 ** 2): return f'{int(x / 1024 ** 2)}Mi'
    if x > (1024 ** 1): return f'{int(x / 1024 ** 1)}Ki'
    return int(x)
