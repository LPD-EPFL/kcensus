#!/usr/bin/env python3
from matplotlib.lines import Line2D
from matplotlib.ticker import *

from common import ALGORITHMS, EXTRA_ALGORITHMS, args
from logparser import *
from prelude import plt

ALL_ALGOS = ALGORITHMS | EXTRA_ALGORITHMS

algos = ["kcensus", "kcensus conflicts", "swift-paxos", "multi-paxos"]
ALL_ALGOS["kcensus"]["label"] = "KCensus without Conflicts"

# to update
# python3 2-cdfs.py -c=aws-world-ring-13.toml -w=1 -r=100 -i=round-robin -t=0
# python3 2-cdfs.py -c=aws-world-ring-13.toml -w=1 -r=100 -i=exponential -t=0.05
# python3 2-cdfs.py -c=aws-world-ring-13.toml -w=1 -r=100 -i=exponential -t=0.1

config = "aws-ring-7" if args.geo == 1 else "aws-ring-7.toml"
writes = 1.0
duration = "10s"
ingress = "exponential"
throughput = 1000
speedup = 1
faults = ""
keys = 10000
shards = 10000

fig, plot = plt.subplots(figsize=(3.11, 2.7), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))
fig.subplots_adjust(
    # wspace=0.22,
    hspace=0.5,
    # top=.80,
)

title = "Request Latency CDF"
plot.set_title(title, pad=0)
print(
    "##################################################################################"
)
print(title + ":")
plot.set_xlabel("Request Latency (ms)", labelpad=1)
plot.set_ylabel("Percentile", labelpad=0)
plot.grid(axis="y", which="major", linestyle="--", linewidth="0.5")
plot.grid(axis="y", which="minor", linestyle=":", linewidth="0.25")
plot.grid(axis="x", which="major", linestyle="--", linewidth="0.5")
plot.grid(axis="x", which="minor", linestyle=":", linewidth="0.25")
plot.tick_params(axis="both", which="major", pad=0.5)
plot.tick_params(axis="both", which="minor", pad=0.5)
plot.yaxis.set_minor_locator(MultipleLocator(25))
plot.yaxis.set_major_locator(MultipleLocator(50))
plot.xaxis.set_minor_locator(MultipleLocator(25))
plot.xaxis.set_major_locator(MultipleLocator(100))
plot.set_axisbelow(True)

max_x = 0
for experiment in algos:
    algo = experiment
    conflicts = "conflicts=false"
    skew = 0.0
    if experiment == "kcensus conflicts":
        algo = "kcensus"
        conflicts = "conflicts=true"
        skew = 1.0
    logs = parse(
        algo=algo,
        config=config,
        writes=writes,
        duration=duration,
        ingress=ingress,
        throughput=throughput,
        speedup=speedup,
        faults=faults,
        keys=keys,
        skew=skew,
        shards=shards,
        conflicts=conflicts
    )

    flattened_output = defaultdict(list)
    for category, pid_data in logs.items():
        for pid, items in pid_data.items():
            flattened_output[category].extend(items)
    logs = flattened_output

    average = compute_average(
        logs["executed"], lambda log: duration_to_ms(log["latency"])
    )
    percentiles = compute_percentiles(
        logs["executed"], lambda log: duration_to_ms(log["latency"])
    )
    print(algo, conflicts, list(enumerate(percentiles)))
    nice_percentiles = [-999999] + percentiles + [999999]
    plot.plot(
        nice_percentiles,
        [0, 0.0001] + list(range(1, 100)) + [99.9999, 100],
        **(ALL_ALGOS[experiment]),
        markevery=[2, 26, 51, 76, 100],
    )
    if percentiles[-1] > max_x:
        max_x = percentiles[-1]
        plot.set_xlim(0, percentiles[-1])

    if args.geo == 1:
        plot.set_xlim(0, 475)
    else:
        plot.sex_xlim(0, 475)

#
# logs_a = parse(
#     algo="weak-replication",
#     config=config,
#     writes=writes,
#     duration=duration,
#     ingress=ingress,
#     throughput=throughput,
#     speedup=speedup,
#     faults=faults,
#     keys=keys,
#     skew=skew,
#     shards=shards,
# )
#
# flattened_output = defaultdict(list)
# for category, pid_data in logs_a.items():
#     for pid, items in pid_data.items():
#         flattened_output[category].extend(items)
# logs_a = flattened_output
#
# logs_b = parse(
#     algo="kcensus",
#     config=config,
#     writes=writes,
#     duration=duration,
#     ingress=ingress,
#     throughput=throughput,
#     speedup=speedup,
#     faults=faults,
#     keys=keys,
#     skew=skew,
#     shards=shards,
# )
#
# flattened_output = defaultdict(list)
# for category, pid_data in logs_b.items():
#     for pid, items in pid_data.items():
#         flattened_output[category].extend(items)
# logs_b = flattened_output
#
# percentiles_a = compute_percentiles(
#     logs_a["executed"], lambda log: duration_to_ms(log["latency"])
# )
# percentiles_b = compute_percentiles(
#     logs_b["executed"], lambda log: duration_to_ms(log["latency"])
# )
# ratios = {i: percentiles_b[i] / percentiles_a[i] for i in range(0, 16)}
# print(f"ratios: {ratios}")
# print(f"min:{min(ratios.values())} max:{max(ratios.values())}")

legends = [Line2D([0], [0], **ALL_ALGOS[algo]) for algo in algos]

fig.legend(
    handles=legends,
    bbox_to_anchor=(0, 1.09, 1, 0.28),
    loc="center",
    edgecolor="black",
    borderaxespad=0,
    ncols=2,
    borderpad=0.3,
    labelspacing=0.1,
    # mode="expand",
    handlelength=0.8,
    handletextpad=0.5,
)

pdf_path = f"plots/2-cdfs.pdf"
plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
print(pdf_path)
