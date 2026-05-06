#!/usr/bin/env python3

from matplotlib.ticker import MultipleLocator

from common import ALGORITHMS, args
from logparser import *
from prelude import lighten_color, plt

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

fig, subplots = plt.subplots(4, 1, figsize=(3.26, 2.7), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))
fig.subplots_adjust(
    # wspace=0.22,
    hspace=0.40,
    # top=.80,
)

for c in range(4):
    config = [
        "aws-ring-7",
        "aws-east-asia-7",
        "aws-europe-7",
        "aws-north-america-7",
    ][c] if args.geo == 1 else [
        "aws-ring-7.toml",
        "aws-east-asia-7.toml",
        "aws-europe-7.toml",
        "aws-north-america-7.toml",
    ][c]
    plot = subplots[c]
    title = [
        "Northern Hemisphere (NH) Deployments",
        "East Asia (EA) Deployments",
        "Europe (EU) Deployments",
        "North America (NA) Deployments",
    ][c]
    plot.set_title(title, pad=0)
    if c == 2:
        plot.set_ylabel("                     Request Latency (ms)", labelpad=1)
    plot.grid(axis="y", which="major", linestyle="--", linewidth="0.5")
    plot.grid(axis="y", which="minor", linestyle=":", linewidth="0.25")
    plot.tick_params(axis="both", which="major", pad=0.5)
    plot.tick_params(axis="both", which="minor", pad=0.5)
    plot.yaxis.set_major_locator(MultipleLocator([100, 40, 10, 40][c]))
    plot.yaxis.set_minor_locator(MultipleLocator([50, 20, 5, 20][c]))
    plot.xaxis.set_tick_params(pad=10)
    plot.set_axisbelow(True)

    # # Min. effort average latency
    # def compute_wr_latency():
    #     logs = parse(
    #         algo="weak-replication",
    #         config=config,
    #         writes=writes,
    #         duration=duration,
    #         ingress=ingress,
    #         throughput=throughput,
    #         speedup=speedup,
    #         faults=faults,
    #         keys=keys,
    #         skew=skew,
    #         shards=shards,
    #     )
    #     all_executed = []
    #     for pid_data in logs["executed"].values():
    #         all_executed.extend(pid_data)
    #     return compute_average(all_executed, lambda log: duration_to_ms(log["latency"]))
    #
    #
    # wr_latency = compute_wr_latency()
    # wr_style = ALGORITHMS["weak-replication"]
    # plot.axhline(y=wr_latency, linestyle="--", color=wr_style["color"], linewidth=1, zorder=2)

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
            duration=duration,
            ingress=ingress,
            throughput=throughput,
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
        if config == "aws-east-asia-7":
            regions = [
                "ap-east-1",
                "ap-east-2",
                "ap-northeast-1",
                "ap-northeast-2",
                "ap-northeast-3",
                "ap-southeast-1",
                "ap-southeast-5",
            ]
        elif config == "aws-europe-7":
            regions = [
                "eu-central-1",
                "eu-central-2",
                "eu-south-1",
                "eu-south-2",
                "eu-west-1",
                "eu-west-2",
                "eu-west-3",
            ]
        elif config == "aws-north-america-7":
            regions = [
                "ca-central-1",
                "ca-west-1",
                "mx-central-1",
                "us-east-1",
                "us-east-2",
                "us-west-1",
                "us-west-2",
            ]
        elif config == "aws-ring-7":
            regions = [
                "ap-northeast-1",
                "ap-south-1",
                "ap-southeast-1",
                "ca-central-1",
                "eu-south-1",
                "eu-west-1",
                "us-west-2",
            ]
        regions.sort()
        for pid, region in enumerate(regions):
            if pid in logs["executed"] and logs["executed"][pid]:
                region_avg = compute_average(
                    logs["executed"][pid], lambda log: duration_to_ms(log["latency"])
                )
                print(region, region_avg)
        xs.append(i)
        ys.append(average)
        delta_ys_top.append(max_replica_avg - average)
        delta_ys_bottom.append(average - min_replica_avg)

        labels.append(
            ALGORITHMS[experiment]["label"].replace(" ", "\n").replace("-", "-\n")
        )
        colors.append(lighten_color(ALGORITHMS[experiment]["color"]))
        # text_y = max_replica_avg  # average / 2
        # plot.text(
        #     i,
        #     text_y,
        #     f"{round(average)}\N{THIN SPACE}ms",
        #     horizontalalignment="center",
        #     verticalalignment="bottom",
        # )
        text_y = 0  # min(average / 2, min_replica_avg, wr_latency)
        plot.text(
            i,
            text_y,
            f"{round(average) if average >= 99.95 else round(average, 1)}\N{THIN SPACE}ms",
            horizontalalignment="center",
            verticalalignment="bottom",
        )

    plot.bar(labels, ys, lw=0, color=colors)
    plot.errorbar(
        xs,
        ys,
        [delta_ys_bottom, delta_ys_top],
        ls="none",
        color="black",
        solid_capstyle="projecting",
        capsize=2.5,
    )

    if c != 3:
        plot.set_xticks([])

    # For the average text to fit
    ymin, ymax = plot.get_ylim()
    ymax = [350, 110, 35, 110][c]
    plot.set_ylim(ymin, ymax)
plt.xticks(ha="center", va="center")
pdf_path = f"plots/1-bars.pdf"
plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
print(pdf_path)
