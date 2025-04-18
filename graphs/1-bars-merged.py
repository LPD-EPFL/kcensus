#!/usr/bin/env python3

from matplotlib.ticker import MultipleLocator
from common import ALGORITHMS
from logparser import *
from prelude import plt

# python3 1-bars.py -c=aws-world-ring-13.toml -w=1 -r=100 -i=round-robin -t=0
# python3 1-bars.py -c=aws-north-america-7.toml -w=1 -r=100 -i=round-robin -t=0
# python3 1-bars.py -c=aws-europe-7-alt.toml -w=1 -r=100 -i=round-robin -t=0
writes = 1.0
requests = 100
ingress = "round-robin"
throughput = 0.0
speedup = 1
faults = ""

fig, subplots = plt.subplots(3, 1, figsize=(3.26, 2.3), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))
fig.subplots_adjust(
    # wspace=0.22,
    hspace=0.40,
    # top=.80,
)

for c in range(3):
    config = [
        "aws-world-ring-13.toml",
        "aws-europe-7-alt.toml",
        "aws-north-america-7.toml",
    ][c]
    plot = subplots[c]
    title = [
        "13-Machine Northern Hemisphere (NH) Deployments",
        "7-Machine Europe (EU) Deployments",
        "7-Machine North America (NA) Deployments",
    ][c]
    plot.set_title(title, pad=0)
    if c == 1:
        plot.set_ylabel("Request Latency (ms)", labelpad=1)
    plot.grid(axis="y", which="major", linestyle="--", linewidth="0.5")
    plot.grid(axis="y", which="minor", linestyle=":", linewidth="0.25")
    plot.tick_params(axis="both", which="major", pad=0.5)
    plot.tick_params(axis="both", which="minor", pad=0.5)
    plot.yaxis.set_major_locator(MultipleLocator([400, 50, 100][c]))
    plot.yaxis.set_minor_locator(MultipleLocator([100, 25, 50][c]))
    plot.xaxis.set_tick_params(pad=10)
    plot.set_axisbelow(True)

    xs = []
    ys = []
    delta_ys_top = []
    delta_ys_bottom = []
    labels = []
    colors = []
    for i, experiment in enumerate(ALGORITHMS.keys()):
        logs = parse(
            algo=experiment,
            config=config,
            writes=writes,
            requests=requests,
            ingress=ingress,
            throughput=throughput,
            speedup=speedup,
            faults=faults,
        )
        average = compute_average(
            logs["executed"], lambda log: duration_to_ms(log["latency"])
        )
        percentiles = compute_percentiles(
            logs["executed"], lambda log: duration_to_ms(log["latency"])
        )
        MOUSTACHES = (5, 95)
        print(
            experiment,
            average,
            (percentiles[MOUSTACHES[0]], percentiles[MOUSTACHES[1]]),
        )
        xs.append(i)
        ys.append(average)
        delta_ys_top.append(percentiles[MOUSTACHES[1]] - average)
        delta_ys_bottom.append(average - percentiles[MOUSTACHES[0]])

        labels.append(
            ALGORITHMS[experiment]["label"].replace(" ", "\n").replace("-", "-\n")
        )
        colors.append(ALGORITHMS[experiment]["color"])
        text_y = percentiles[MOUSTACHES[1]]  # average / 2
        plot.text(
            i,
            text_y,
            f"{int(average)}\N{THIN SPACE}ms",
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

    if c != 2:
        plot.set_xticks([])

    # For the average text to fit
    ymin, ymax = plot.get_ylim()
    plot.set_ylim(ymin, ymax * 1.3)
plt.xticks(ha="center", va="center")
pdf_path = f"plots/1-bars.pdf"
plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
print(pdf_path)
