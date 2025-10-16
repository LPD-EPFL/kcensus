#!/usr/bin/env python3
from matplotlib.lines import Line2D
from matplotlib.ticker import *

from common import ALGORITHMS, args
from logparser import *
from prelude import plt

# python3 2-cdfs.py -c=aws-world-ring-13.toml -w=1 -r=100 -i=round-robin -t=0
# python3 2-cdfs.py -c=aws-world-ring-13.toml -w=1 -r=100 -i=exponential -t=0.05
# python3 2-cdfs.py -c=aws-world-ring-13.toml -w=1 -r=100 -i=exponential -t=0.1
config = "aws-world-ring-13" if args.geo == 1 else "aws-world-ring-13.toml"
writes = 1.0
requests = 100
stop_at = 60
speedup = 1
faults = ""

fig, subplots = plt.subplots(3, 1, figsize=(3.11, 2.82), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))
fig.subplots_adjust(
    # wspace=0.22,
    hspace=0.5,
    # top=.80,
)

for p in range(3):
    throughput = [0.0, 0.1, 0.2][p] if args.geo == 1 else [0.0, 0.05, 0.1][p]
    ingress = "exponential" if throughput > 0 else "round-robin"
    plot = subplots[p]

    title = "Request Latency CDF Under " + ["No", "Moderate", "High"][p] + " Contention"
    plot.set_title(title, pad=0)
    print(
        "##################################################################################"
    )
    print(title + ":")
    if p == 2:
        plot.set_xlabel("Request Latency (ms)", labelpad=1)
    if p == 1:
        plot.set_ylabel("Percentile")
    plot.grid(axis="y", which="major", linestyle="--", linewidth="0.5")
    plot.grid(axis="y", which="minor", linestyle=":", linewidth="0.25")
    plot.grid(axis="x", which="major", linestyle="--", linewidth="0.5")
    plot.grid(axis="x", which="minor", linestyle=":", linewidth="0.25")
    plot.tick_params(axis="both", which="major", pad=0.5)
    plot.tick_params(axis="both", which="minor", pad=0.5)
    plot.yaxis.set_minor_locator(MultipleLocator(25))
    plot.yaxis.set_major_locator(MultipleLocator(50))
    if args.geo == 1:
        plot.xaxis.set_minor_locator(MultipleLocator(25))
        plot.xaxis.set_major_locator(MultipleLocator(100))
    else:
        plot.xaxis.set_minor_locator(MultipleLocator(50))
        plot.xaxis.set_major_locator(MultipleLocator(200))
    plot.set_axisbelow(True)

    max_x = 0
    for experiment in ALGORITHMS:
        logs = parse(
            algo=experiment,
            config=config,
            writes=writes,
            requests=requests,
            ingress=ingress,
            throughput=throughput,
            speedup=speedup,
            faults=faults,
            stop_at=stop_at,
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
        print(experiment, list(enumerate(percentiles)))
        nice_percentiles = [-999999] + percentiles + [999999]
        plot.plot(
            nice_percentiles,
            [0, 0.1] + list(range(1, 100)) + [99.9, 100],
            **ALGORITHMS[experiment],
            markevery=(26, 25),
        )
        if percentiles[-1] > max_x:
            max_x = percentiles[-1]
            plot.set_xlim(0, percentiles[-1])

        if args.geo == 1:
            plot.set_xlim(0, 500)
        else:
            plot.sex_xlim(0, 1000)


logs_a = parse(
    algo="weak-replication",
    config=config,
    writes=writes,
    requests=requests,
    ingress="round-robin",
    throughput=0.0,
    speedup=speedup,
    faults=faults,
    stop_at=stop_at,
)

flattened_output = defaultdict(list)
for category, pid_data in logs_a.items():
    for pid, items in pid_data.items():
        flattened_output[category].extend(items)
logs_a = flattened_output

logs_b = parse(
    algo="kcensus",
    config=config,
    writes=writes,
    requests=requests,
    ingress="round-robin",
    throughput=0.0,
    speedup=speedup,
    faults=faults,
    stop_at=stop_at,
)

flattened_output = defaultdict(list)
for category, pid_data in logs_b.items():
    for pid, items in pid_data.items():
        flattened_output[category].extend(items)
logs_b = flattened_output

percentiles_a = compute_percentiles(
    logs_a["executed"], lambda log: duration_to_ms(log["latency"])
)
percentiles_b = compute_percentiles(
    logs_b["executed"], lambda log: duration_to_ms(log["latency"])
)
ratios = {i: percentiles_b[i] / percentiles_a[i] for i in range(0, 16)}
print(f"ratios: {ratios}")
print(f"min:{min(ratios.values())} max:{max(ratios.values())}")

legends = [Line2D([0], [0], **algo) for algo in ALGORITHMS.values()]


fig.legend(
    handles=legends,
    bbox_to_anchor=(0.14, 1.09, 0.75, 0.01),
    loc="center",
    edgecolor="black",
    borderaxespad=0,
    ncols=3,
    borderpad=0.3,
    labelspacing=0.1,
    mode="expand",
    handlelength=0.8,
    handletextpad=0.5,
)

pdf_path = f"plots/2-cdfs.pdf"
plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
print(pdf_path)
