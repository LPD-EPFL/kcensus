#!/usr/bin/env python3
import matplotlib as mpl
# from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from matplotlib.ticker import MultipleLocator, NullFormatter

from common import ALGORITHMS, args, local_throughput, PLOT_PREFIX, local_duration
from logparser import *
from prelude import lighten_color, plt

mpl.rcParams["hatch.linewidth"] = 0.5
HATCHES = {
    "kcensus": "",
    "swift-paxos": "xxxx",
    "epaxos": "\\\\\\\\\\",
    "pando": "----",
    "multi-paxos": "////",
}

# python3 1-bars.py -c=aws-east-asia-9.toml -w=1
# python3 1-bars.py -c=aws-europe-8.toml -w=1
# python3 1-bars.py -c=aws-north-america-7 -w=1
writes = 1.0
duration = "10s"
ingress = "exponential"
throughput = 1000
speedup = 1
faults = ""
keys = 10000
skew = 0.0
shards = 10000

algos = list(ALGORITHMS)
x_middle = len(algos) // 2
x_offset = len(algos) + 1

fig, plots = plt.subplots(1, 2, figsize=(3.26, 1), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))
fig.subplots_adjust(
    wspace=0,
    # hspace=0.3,
    # top=.80,
)

plots[0].set_title("Average", pad=0)
plots[1].set_title("99th Percentile", pad=0)
plots[0].set_ylabel("  Request Latency (ms)", labelpad=1)
for plot in plots:
    plot.set_axisbelow(True)
    # Both graphs, y-axis
    plot.grid(axis="y", which="major", linestyle="--", linewidth="0.5")
    plot.grid(axis="y", which="minor", linestyle=":", linewidth="0.25")
    plot.yaxis.set_major_locator(MultipleLocator(20))
    plot.yaxis.set_minor_locator(MultipleLocator(10))
    plot.set_ylim(30, 100)
    plot.set_xticks(
        [x_middle + i * x_offset for i in range(2)],
        labels=["East Asia", "North America"],
    )
    plot.tick_params(axis="x", which="major", pad=0.5)
# Left graph, y-axis
plots[0].tick_params(axis="y", which="major", pad=0.5)
plots[0].tick_params(axis="y", which="minor", pad=0.5)
# Right graph, y-axis
plots[1].tick_params(axis="y", which="both", left=False, labelleft=False)
plots[1].tick_params(axis="y", which="both", left=False, labelleft=False)

for c in range(2):
    config = [
        "aws-east-asia-7",
        # "aws-europe-7",
        "aws-north-america-7",
    ][c]
    if args.geo != 1:
        config += ".toml"

    # Regions sorted by longitude
    # with minor exceptions in NA/EU to keep close-by datacenters next to each other
    regions = [[
        "ap-southeast-5",
        "ap-southeast-1",
        "ap-east-1",
        "ap-east-2",
        "ap-northeast-2",
        "ap-northeast-3",
        "ap-northeast-1",
        # ], [
        #     "eu-south-2",
        #     "eu-west-1",
        #     "eu-west-2",
        #     "eu-west-3",
        #     "eu-central-1",
        #     "eu-central-2",
        #     "eu-south-1",
    ], [
        "ca-west-1",
        "us-west-2",
        "us-west-1",
        "mx-central-1",
        "us-east-2",
        "us-east-1",
        "ca-central-1",
    ]][c]
    alpha_regions = regions.copy()
    alpha_regions.sort()

    for i, experiment in enumerate(e for e in ALGORITHMS.keys() if e != "weak-replication"):
        logs = parse(
            algo=experiment,
            config=config,
            writes=writes,
            duration=local_duration(duration, int(''.join(c for c in config if c.isdigit()))),
            ingress=ingress,
            throughput=local_throughput(throughput, int(''.join(c for c in config if c.isdigit()))),
            speedup=speedup,
            faults=faults,
            keys=keys,
            skew=skew,
            shards=shards,
        )

        all_executed = []
        for pid_data in logs["executed"].values():
            all_executed.extend(pid_data)
        average = compute_average(
            all_executed, lambda log: duration_to_ms(log["latency"])
        )
        replica_averages = compute_replica_averages(
            logs["executed"], lambda log: duration_to_ms(log["latency"])
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
            (percentiles[1], percentiles[5], percentiles[50], percentiles[95], percentiles[99])
        )

        x_pos = i + x_offset * c
        plots[0].bar(
            x_pos,
            average,
            hatch=HATCHES[experiment],
            color=lighten_color(ALGORITHMS[experiment]["color"]),
            edgecolor="black",
            lw=0,
            # width=0.8,
        )
        plots[1].bar(
            x_pos,
            percentiles[99],
            hatch=HATCHES[experiment],
            color=lighten_color(ALGORITHMS[experiment]["color"]),
            edgecolor="black",
            lw=0,
            # width=0.8,
        )

# legends = [Line2D([0], [0], **ALGORITHMS[algo]) for algo in ALGORITHMS]
legends = [
    Patch(
        facecolor=lighten_color(ALGORITHMS[algo]["color"]),
        label=ALGORITHMS[algo]["label"],
        edgecolor="black",
        hatch=HATCHES[algo],
        lw=0,
    ) for algo in ALGORITHMS
]
fig.legend(
    handles=legends,
    bbox_to_anchor=(0.03, 1.17, 0.95, 0.07),
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

# plt.xticks(ha="center", va="center", rotation=45)
pdf_path = f"plots/{PLOT_PREFIX}exp-1-figure-1-intro.pdf"
plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
print(pdf_path)
