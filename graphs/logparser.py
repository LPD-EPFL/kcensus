import json
import re
from collections import defaultdict

LOG_DIR = "../logs"


def parse(
        pids=None,
        config="aws-europe-7.toml",
        algo="kcensus",
        writes=0.5,
        duration="10s",
        ingress="exponential",
        throughput=10,
        speedup=1,
        faults="",
        keys=100,
        skew=0,
        shards=100,
        std="out",
        stop_at=0,  # 0 means take all requests
        conflicts="conflicts=false"
):
    if not pids:
        num_replicas = int("".join([char for char in config if char.isdigit()]))
        pids = list(range(num_replicas))
    output = defaultdict(lambda: defaultdict(list))
    for pid in pids:
        file_path = f"{LOG_DIR}/c={config}/a={algo}/w={writes:g}/d={duration}/i={ingress}/t={throughput:g}/s={speedup}/f={faults}/k={keys}/skew={skew:g}/shards={shards}/{conflicts}/{pid}.std{std}"
        print(file_path)
        with open(file_path) as file:
            for key, items in parse_file(file).items():
                if stop_at:
                    output[key][pid] = items[:stop_at]
                else:
                    output[key][pid] = items
    return output


def parse_file(file):
    output = defaultdict(list)
    log_pattern = r"\[log=(.*?)\] ([^\|]*) \| (.*)"
    # Older exp-3 logs predate the structured resource line above. Keep them usable for
    # Figure 11 instead of requiring an expensive cloud rerun.
    legacy_memory_pattern = r"\[time\] Memory\(KB\): (\d+)"
    for line in file:
        match = re.match(log_pattern, line)
        if match:
            output[match.group(1)].append(json.loads(match.group(3)))
            continue
        match = re.match(legacy_memory_pattern, line)
        if match:
            output["time"].append({"memory": int(match.group(1))})
    return output


def compute_percentiles(items, accessor=lambda x: x):
    sorted_mapped = sorted([accessor(item) for item in items])
    return [
        sorted_mapped[int(p * (len(sorted_mapped) / 100))] for p in range(0, 100)
    ] + [sorted_mapped[-1]]


def compute_average(items, accessor=lambda x: x):
    return sum(accessor(item) for item in items) / len(items)


def duration_to_ms(duration):
    return duration["secs"] * 1_000 + duration["nanos"] / 1_000_000


def compute_replica_averages(replica_data, accessor=lambda x: x):
    replica_averages = []
    for pid, items in replica_data.items():
        if items:
            avg = sum(accessor(item) for item in items) / len(items)
            replica_averages.append(avg)
    return replica_averages
