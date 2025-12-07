#!/usr/bin/env python3
from itertools import combinations
from collections import defaultdict

import matplotlib as mpl
from matplotlib import patches
from matplotlib.ticker import NullLocator, NullFormatter, MultipleLocator

from common import ALGORITHMS, args, serialized_args
from logparser import *
from prelude import lighten_color, plt

algorithms = ["weak-replication", "kcensus", "epaxos", "multi-paxos-3p", "paxos"]

mpl.rcParams["hatch.linewidth"] = 0.5
HATCHES = {
    "weak-replication": "",
    "kcensus": "xxxx",
    "epaxos": "\\\\\\\\\\",
    "multi-paxos-3p": "----",
    "paxos": "////",
}

fig, plots = plt.subplots(1, 5, figsize=(3.22, 0.95), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))

for num_faults, plot in enumerate(plots):
    plot.set_title(f'{num_faults} Fault{["s", ""][num_faults == 1]}', pad=0)
    if num_faults == 0:
        plot.set_ylabel("Latency (ms)", labelpad=1)
    else:
        plot.yaxis.set_major_formatter(NullFormatter())
    plot.grid(axis="y", which="major", linestyle="--", linewidth="0.5")
    plot.grid(axis="y", which="minor", linestyle=":", linewidth="0.25")
    if num_faults == 0:
        plot.tick_params(axis="both", which="major", pad=0.5)
        plot.tick_params(axis="both", which="minor", pad=0.5)
    else:
        plot.tick_params(
            axis="both", which="major", pad=0.5, left=False, labelleft=False
        )
        plot.tick_params(
            axis="both", which="minor", pad=0.5, left=False, labelleft=False
        )
    plot.xaxis.set_major_locator(NullLocator())
    plot.xaxis.set_minor_locator(NullLocator())
    plot.yaxis.set_major_locator(MultipleLocator(400))
    plot.yaxis.set_minor_locator(MultipleLocator(100))
    plt.gca().xaxis.set_tick_params(pad=10)
    plot.set_axisbelow(True)
    plt.xticks(ha="center", va="center")

    xs = []
    ys = []
    delta_ys_top = []
    delta_ys_bottom = []
    labels = []
    colors = []
    hatches = []
    for i, experiment in enumerate(algorithms):
        all_executed_by_replica = defaultdict(list)
        num_replicas = int("".join([char for char in args.config if char.isdigit()]))
        all_faults = [
            ",".join(map(str, comb))
            for comb in combinations(range(num_replicas), num_faults)
        ]
        for faults in all_faults:
            logs = parse(
                algo=experiment,
                config=args.config,
                writes=args.writes,
                requests=args.requests,
                ingress=args.ingress,
                throughput=args.throughput,
                speedup=args.speedup,
                faults=faults,
            )
            for pid, pid_data in logs["executed"].items():
                all_executed_by_replica[pid].extend(pid_data)
        
        all_executed = []
        for pid_data in all_executed_by_replica.values():
            all_executed.extend(pid_data)
        average = compute_average(
            all_executed, lambda log: duration_to_ms(log["latency"])
        )
        replica_averages = compute_replica_averages(
            all_executed_by_replica, lambda log: duration_to_ms(log["latency"])
        )
        min_replica_avg = min(replica_averages) if replica_averages else average
        max_replica_avg = max(replica_averages) if replica_averages else average
        percentiles = compute_percentiles(
            all_executed, lambda log: duration_to_ms(log["latency"])
        )
        print(
            experiment,
            average,
            (min_replica_avg, max_replica_avg),
            (percentiles[1], percentiles[5], percentiles[95], percentiles[99])
        )
        regions = ["us-west-2", "ca-central-1", "eu-west-3", "eu-west-1",
                   "me-south-1", "ap-south-2", "ap-southeast-1",
                   "ap-northeast-1", "ap-east-1"]
        regions.sort()
        for pid, region in enumerate(regions):
            if pid in logs["executed"] and logs["executed"][pid]:
                region_avg = compute_average(
                    logs["executed"][pid], lambda log: duration_to_ms(log["latency"])
                )
                print(region, region_avg)
        xs.append(i)
        labels.append(i)
        ys.append(average)
        delta_ys_top.append(max_replica_avg - average)
        delta_ys_bottom.append(average - min_replica_avg)
        colors.append(lighten_color(ALGORITHMS[experiment]["color"]))
        hatches.append(HATCHES[experiment])

    plot.bar(labels, ys, lw=0, color=colors, hatch=hatches, edgecolor="black")
    plot.errorbar(
        xs,
        ys,
        [delta_ys_bottom, delta_ys_top],
        ls="none",
        color="black",
        solid_capstyle="projecting",
        capsize=1.5,
    )

max_y = max(plot.get_ylim()[1] for plot in plots)
for plot in plots:
    plot.set_ylim(0, max_y * 1.1)
fig.subplots_adjust(wspace=0, hspace=0)

legends = [
    patches.Patch(
        facecolor=lighten_color(ALGORITHMS[algo]["color"]),
        label=ALGORITHMS[algo]["label"],
        edgecolor="black",
        hatch=HATCHES[algo],
        lw=0,
    )
    for algo in algorithms
]

fig.legend(
    handles=legends,
    bbox_to_anchor=(-0.01, 1.22, 1, 0.01),
    loc="center",
    edgecolor="black",
    borderaxespad=0,
    ncols=5,
    borderpad=0.3,
    labelspacing=0.1,
    mode="expand",
    handlelength=0.8,
    handletextpad=0.5,
)

pdf_path = f"plots/4-faults{serialized_args}.pdf"
plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
print(pdf_path)
