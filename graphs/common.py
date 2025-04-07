blue = '#4C72B0'
red = '#C44E52'
orange = '#DD8452'
yellow = '#E2C44C'
green = '#55A868'

ALGORITHMS = {
    'unreplicated': {'label': 'Unreplicated', 'color': blue},
    'paxos': {'label': 'Paxos', 'color': red},
    'multi-paxos': {'label': 'Multi-Paxos', 'color': orange},
    'e-paxos': {'label': 'EPaxos', 'color': yellow},
    'k-census': {'label': 'K-Census', 'color': green},
}


def k_formatter(x, _):
    if x < 1000: return int(x)
    return f'{int(x / 1000)}k'


def ki_formatter(x, _):
    if x > (1024 ** 3): return f'{int(x / 1024 ** 3)}Gi'
    if x > (1024 ** 2): return f'{int(x / 1024 ** 2)}Mi'
    if x > (1024 ** 1): return f'{int(x / 1024 ** 1)}Ki'
    return int(x)
