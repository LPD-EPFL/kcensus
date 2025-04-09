blue = '#4C72B0'
red = '#C44E52'
orange = '#DD8452'
yellow = '#E2C44C'
green = '#55A868'
grey = '#FAFAFA'

ALGORITHMS = {
    'unreplicated': {'label': 'Unreplicated', 'color': blue},
    'paxos': {'label': 'Paxos', 'color': red},
    'multi-paxos': {'label': 'Multi-Paxos', 'color': orange},
    'e-paxos': {'label': 'EPaxos', 'color': yellow},
    'k-census': {'label': 'K-Census', 'color': green},
    'weak-replication': {'label': 'Weak', 'color': grey},
}

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', type=str, default='aws-europe-7.toml', help='Topology')
parser.add_argument('-w', '--writes', type=float, default=0.5, help='Ratio of writes')
parser.add_argument('-r', '--requests', type=int, default=100, help='Number of requests per client')
parser.add_argument('-i', '--ingress', type=str, default='round-robin', help='Type of ingress')
parser.add_argument('-t', '--throughput', type=float, default='10', help='Target req/s per client')
args = parser.parse_args()


def k_formatter(x, _):
    if x < 1000: return int(x)
    return f'{int(x / 1000)}k'


def ki_formatter(x, _):
    if x > (1024 ** 3): return f'{int(x / 1024 ** 3)}Gi'
    if x > (1024 ** 2): return f'{int(x / 1024 ** 2)}Mi'
    if x > (1024 ** 1): return f'{int(x / 1024 ** 1)}Ki'
    return int(x)
