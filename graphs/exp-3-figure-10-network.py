#!/usr/bin/env python3
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator

from common import ALGORITHMS, args, k_formatter, local_throughput, PLOT_PREFIX, local_duration
from logparser import *
from prelude import plt

fig, plots = plt.subplots(1, 2, figsize=(3.2, 0.95), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))
plots[0].set_title("Traffic", pad=0)
plots[0].set_ylabel("Bytes per App Req.", labelpad=1)
plots[0].yaxis.set_major_locator(MultipleLocator(2000))
plots[0].yaxis.set_minor_locator(MultipleLocator(1000))
plots[0].set_ylim(0, 6000)
# plots[0].set_yscale("log")
plots[0].yaxis.set_major_formatter(k_formatter)
plots[1].set_title("Communication", pad=0)
plots[1].set_ylabel("Msg. per App Req.", labelpad=1)
plots[1].yaxis.set_major_formatter(k_formatter)
plots[1].yaxis.set_major_locator(MultipleLocator(50))
plots[1].yaxis.set_minor_locator(MultipleLocator(25))
plots[1].set_ylim(0, 200)
for plot in plots:
    plot.set_xlabel("Number of Servers", labelpad=1)
    plot.grid(axis="y", which="major", linestyle="--", linewidth="0.5")
    plot.grid(axis="y", which="minor", linestyle=":", linewidth="0.25")
    plot.grid(axis="x", which="major", linestyle="--", linewidth="0.5")
    plot.grid(axis="x", which="minor", linestyle=":", linewidth="0.25")
    plot.tick_params(axis="both", which="major", pad=0.5)
    plot.tick_params(axis="both", which="minor", pad=0.5)
    plot.xaxis.set_major_locator(MultipleLocator(4, 3))
    plot.set_xlim(3, 31)

for i, experiment in enumerate(ALGORITHMS):
    if experiment == "weak-replication": continue
    xs = []
    ys_bytes = []
    ys_msgs = []
    for num_replicas in range(3, 31 + 2, 2):
        config = args.config.replace("@", str(num_replicas))
        logs = parse(
            algo=experiment,
            config=config,
            writes=args.writes,
            ingress=args.ingress,
            duration=local_duration(args.duration, num_replicas, 1),
            throughput=local_throughput(args.throughput, num_replicas),
            faults=args.faults,
            speedup=args.speedup,
            keys=args.keys,
            skew=args.skew,
            shards=args.shards,
            conflicts="conflicts=false",
        )
        assert (
                len(logs["network-done"]) == num_replicas
        ), f'Some replicas ({num_replicas - len(logs["network-done"])} out of {num_replicas}) did not report networking stats'

        flattened_output = defaultdict(list)
        for category, pid_data in logs.items():
            for pid, items in pid_data.items():
                flattened_output[category].extend(items)
        logs = flattened_output

        total_requests = sum(log["requests"] for log in logs["client-done"])
        total_bytes = sum(log["byte_count"] for log in logs["network-done"])
        total_msgs = sum(log["msg_count"] for log in logs["network-done"])
        xs.append(num_replicas)
        ys_bytes.append(total_bytes / total_requests)
        ys_msgs.append(total_msgs / total_requests)
        print(experiment, num_replicas, ys_bytes[-1], ys_msgs[-1])
    plots[0].plot(xs, ys_bytes, **ALGORITHMS[experiment], markevery=(1, 3), zorder=(2 - i / 100))
    plots[1].plot(xs, ys_msgs, **ALGORITHMS[experiment], markevery=(1, 3), zorder=(2 - i / 100))
for plot in plots:
    plot.set_ylim(0, None)

fig.subplots_adjust(wspace=0.4, hspace=0)

legends = [Line2D([0], [0], **algo) for (k, algo) in ALGORITHMS.items() if k != "weak-replication"]

fig.legend(
    handles=legends,
    bbox_to_anchor=(0, 1.29, 1, 0.01),
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

pdf_path = f"plots/{PLOT_PREFIX}exp-3-figure-10-network.pdf"
plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
print(pdf_path)
