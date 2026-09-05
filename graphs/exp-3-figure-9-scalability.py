#!/usr/bin/env python3
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator, ScalarFormatter

from common import ALGORITHMS, local_throughput, PLOT_PREFIX
from logparser import *
from prelude import plt

# python3 3-scalability.py -c=aws-random/@.toml -w=1 -r=10 -i=round-robin -t=0
# python3 3-scalability.py -c=aws-from-paris/@.toml -w=1 -r=10 -i=round-robin -t=0
writes = 1.0
duration = "10s"
ingress = "exponential"
throughput = 1000
speedup = 1
faults = ""
keys = 10000
skew = 0.0
shards = 10000

fig, subplots = plt.subplots(2, 1, figsize=(3.155, 2.265), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))
fig.subplots_adjust(
    # wspace=0.22,
    hspace=0.35,
    # top=.80,
)

for p in range(2):
    config = [
        "aws-random/@.toml",
        "aws-from-paris/@.toml",
    ][p]
    plot = subplots[p]

    title = "Average Latency for " + ["Random", "Parisian"][p] + " Deployments"
    plot.set_title(title, pad=0)
    if p == 1:
        plot.set_xlabel("Number of Replicas", labelpad=0.2)
    plot.set_ylabel(" ", labelpad=1)
    plot.grid(axis="y", which="major", linestyle="--", linewidth="0.5")
    plot.grid(axis="y", which="minor", linestyle=":", linewidth="0.25")
    plot.grid(axis="x", which="major", linestyle="--", linewidth="0.5")
    plot.grid(axis="x", which="minor", linestyle=":", linewidth="0.25")
    plot.tick_params(axis="both", which="major", pad=0.5)
    plot.tick_params(axis="both", which="minor", pad=0.5)
    plot.xaxis.set_major_locator(MultipleLocator(4, 3))
    plot.xaxis.set_minor_locator(MultipleLocator(2, 3))
    if p == 0:
        plot.yaxis.set_minor_locator(MultipleLocator(25))
    else:
        plot.yaxis.set_minor_locator(MultipleLocator(50))
    plot.yaxis.set_major_formatter(ScalarFormatter())
    plot.set_xlim(3, 31)

    min_y = 1000
    for i, experiment in enumerate(ALGORITHMS):
        xs = []
        ys = []
        # percentiles_ys = ([], [])
        for num_replicas in range(3, 31 + 2, 2):
            logs = parse(
                algo=experiment,
                config=config.replace("@", str(num_replicas)),
                writes=writes,
                duration=duration,
                ingress=ingress,
                throughput=local_throughput(throughput, num_replicas),
                speedup=speedup,
                faults=faults,
                keys=keys,
                skew=skew,
                shards=shards,
                conflicts="conflicts=false",
            )

            flattened_output = defaultdict(list)
            for category, pid_data in logs.items():
                for pid, items in pid_data.items():
                    flattened_output[category].extend(items)
            logs = flattened_output
            print(experiment, num_replicas, config)
            average = compute_average(
                logs["executed"], lambda log: duration_to_ms(log["latency"])
            )
            percentiles = compute_percentiles(
                logs["executed"], lambda log: duration_to_ms(log["latency"])
            )
            MOUSTACHES = (5, 95)
            print(
                experiment,
                num_replicas,
                average,
                (percentiles[MOUSTACHES[0]], percentiles[MOUSTACHES[1]]),
            )
            xs.append(num_replicas)
            ys.append(average)
            # percentiles_ys[0].append(percentiles[MOUSTACHES[0]])
            # percentiles_ys[1].append(percentiles[MOUSTACHES[1]])
            min_y = min(min_y, average)

        plot.plot(xs, ys, **ALGORITHMS[experiment], markevery=(1, 3), zorder=(2 - i / 100))
        # for ys in percentiles_ys:
        #     plot.plot(xs, ys, color=ALGORITHMS[experiment]['color'], linestyle="--")

    # if p == 1:
    #     plot.set_ylim(min_y, 1000)
subplots[0].set_ylim(100, 300)
subplots[1].set_ylim(0, 250)

legends = [Line2D([0], [0], **algo) for algo in ALGORITHMS.values()]

fig.legend(
    handles=legends,
    bbox_to_anchor=(0, 1.12, 1, 0.01),
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
plt.axes(frameon=False)
plt.xticks([])
plt.yticks([])
plt.ylabel("Latency (ms)", labelpad=20)
pdf_path = f"plots/{PLOT_PREFIX}exp-3-figure-9-scalability.pdf"
plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
print(pdf_path)
