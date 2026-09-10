#!/usr/bin/env python3

from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator, NullFormatter

from common import ALGORITHMS, args, local_throughput, PLOT_PREFIX, local_duration
from logparser import *
from prelude import lighten_color, plt

writes = 1.0
duration = "10s"
ingress = "exponential"
throughput = 1000
speedup = 1
faults = ""
keys = 100000
skew = 0.0

fig, subplots = plt.subplots(4, 2, figsize=(3.26, 3.5), tight_layout=True, width_ratios=[4, 2])
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))
fig.subplots_adjust(
    wspace=0,
    hspace=0.3,
    # top=.80,
)

for c, row in enumerate(subplots):
    config = [
        "aws-ring-7",
        "aws-east-asia-7",
        "aws-europe-7",
        "aws-north-america-7",
    ][c]
    if args.geo != 1:
        config += ".toml"

    title = [
        "Northern Hemisphere (NH) Deployments",
        "East Asia (EA) Deployments",
        "Europe (EU) Deployments",
        "North America (NA) Deployments",
    ][c]
    row[0].set_title(title, pad=0)
    row[1].set_title("Avg & P1/99", pad=0)

    # Regions sorted by longitude
    # with minor exceptions in NA/EU to keep close-by datacenters next to each other
    regions = [[
        'us-west-2',
        'ca-central-1',
        'eu-west-1',
        'eu-south-1',
        'ap-south-1',
        'ap-southeast-1',
        'ap-northeast-1',
    ], [
        "ap-southeast-5",
        "ap-southeast-1",
        "ap-east-1",
        "ap-east-2",
        "ap-northeast-2",
        "ap-northeast-3",
        "ap-northeast-1",
    ], [
        "eu-south-2",
        "eu-west-1",
        "eu-west-2",
        "eu-west-3",
        "eu-central-1",
        "eu-central-2",
        "eu-south-1",
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

    ymax = [350, 105, 30, 105][c]
    if c == 2:
        row[0].set_ylabel("                        Request Latency (ms)", labelpad=1)
    for plot in row:
        plot.set_axisbelow(True)
        # Both graphs, y-axis
        plot.grid(axis="y", which="major", linestyle="--", linewidth="0.5")
        plot.grid(axis="y", which="minor", linestyle=":", linewidth="0.25")
        plot.yaxis.set_major_locator(MultipleLocator([100, 40, 10, 40][c]))
        plot.yaxis.set_minor_locator(MultipleLocator([50, 20, 5, 20][c]))
        plot.set_ylim(0, ymax)
    # Left graph, x-axis
    row[0].xaxis.set_major_locator(MultipleLocator(1))
    # row[0].xaxis.set_major_formatter(NullFormatter())
    row[0].tick_params(axis="x", which="major", bottom=False, labelbottom=False)
    row[0].grid(axis="x", which="major", linestyle=":", linewidth="0.3")
    # Left graph, y-axis
    row[0].tick_params(axis="y", which="major", pad=0.5)
    row[0].tick_params(axis="y", which="minor", pad=0.5)
    # Right graph, x-axis
    row[1].set_xticks([])
    # Right graph, y-axis
    row[1].tick_params(axis="y", which="both", left=False, labelleft=False)
    row[1].tick_params(axis="y", which="both", left=False, labelleft=False)

    xs = []
    ys = []
    delta_ys_top = []
    delta_ys_bottom = []
    labels = []
    colors = []
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
            shards=keys,
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
        region_averages = {}
        for pid, region in enumerate(alpha_regions):
            if pid in logs["executed"] and logs["executed"][pid]:
                print(region, replica_averages[pid])
                region_averages[region] = replica_averages[pid]

        xs.append(i)
        ys.append(average)
        delta_ys_top.append(percentiles[99] - average)
        delta_ys_bottom.append(average - percentiles[1])

        labels.append(
            ALGORITHMS[experiment]["label"].replace(" ", "\n").replace("-", "-\n")
        )
        colors.append(lighten_color(ALGORITHMS[experiment]["color"]))

        if i == 0:
            row[0].set_xlim(-0.6, len(regions) - 1 + 0.4)
            for r, region in enumerate(regions):
                region = region.replace("north", "N")
                region = region.replace("south", "S")
                region = region.replace("east", "E")
                region = region.replace("west", "W")
                region = region.replace("central", "C")
                region = region[:3] + region[3:].replace("-", "")
                region = region.upper()
                row[0].text(
                    r - 0.15,
                    ymax / 20,
                    f"{region}",
                    horizontalalignment="right",
                    verticalalignment="bottom",
                    rotation=90,
                    size=6,
                    color="#222",
                    # backgroundcolor="white",
                    bbox=dict(facecolor='white', alpha=0.5, linewidth=0, edgecolor=None, pad=0.1),
                    zorder=100,
                )

        row[0].plot(
            list(range(len(regions))),
            [region_averages[region] for region in regions],
            **ALGORITHMS[experiment],
            markevery=1,
            zorder=(2 - i / 100),
        )
        # plot.text(
        #     i,
        #     0,
        #     f"{round(average) if average >= 99.95 else round(average, 1)}\N{THIN SPACE}ms",
        #     horizontalalignment="center",
        #     verticalalignment="bottom",
        # )

    row[1].bar(list(range(len(ys))), ys, lw=0, color=colors, width=0.9)
    for i, experiment in enumerate(ALGORITHMS):
        style = ALGORITHMS[experiment]
        row[1].plot(
            [i],
            [ymax * 0.185],
            marker=style["marker"],
            markersize=style["markersize"],
            markeredgewidth=style["markeredgewidth"],
            color="black",
            linewidth=0,
        )
        average = ys[i]
        plot.text(
            i,
            0,
            f"{round(average) if average >= 99.95 else round(average, 1)}",
            horizontalalignment="center",
            verticalalignment="bottom",
            size=6,
        )
    row[1].errorbar(
        list(range(len(ys))),
        ys,
        [delta_ys_bottom, delta_ys_top],
        ls="none",
        color="black",
        solid_capstyle="projecting",
        capsize=2.5,
    )

legends = [Line2D([0], [0], **ALGORITHMS[algo]) for algo in ALGORITHMS]
fig.legend(
    handles=legends,
    bbox_to_anchor=(-0.012, 1.03, 0.99, 0.07),
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
pdf_path = f"plots/{PLOT_PREFIX}exp-1-figure-7-latency.pdf"
plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
print(pdf_path)
