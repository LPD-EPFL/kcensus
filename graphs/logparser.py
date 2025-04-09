import json
import re
from collections import defaultdict

LOG_DIR = '../logs'


def parse(pids=None, config='aws-europe-7.toml', algo='k-census', writes=0.5, requests=100, ingress='round-robin',
          throughput=0):
    if not pids:
        num_replicas = int(''.join([char for char in config if char.isdigit()]))
        pids = list(range(num_replicas))
    output = defaultdict(list)
    for pid in pids:
        file_path = f'{LOG_DIR}/c={config}/a={algo}/w={writes:g}/r={requests}/i={ingress}/t={throughput:g}/{pid}.stdout'
        with open(file_path) as file:
            for key, items in parse_file(file).items():
                output[key] += items
    return output


def parse_file(file):
    output = defaultdict(list)
    log_pattern = r"\[log=(.*?)\] ([^\|]*) \| (.*)"
    for line in file:
        match = re.match(log_pattern, line)
        if match:
            output[match.group(1)].append(json.loads(match.group(3)))
    return output


def compute_percentiles(items, accessor=lambda x: x):
    sorted_mapped = sorted([accessor(item) for item in items])
    return [sorted_mapped[int(p * (len(sorted_mapped) / 100))] for p in range(0, 100)] + [sorted_mapped[-1]]


def duration_to_ms(duration):
    return duration['secs'] * 1_000 + duration['nanos'] / 1_000_000
