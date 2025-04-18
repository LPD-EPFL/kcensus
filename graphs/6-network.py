#!/usr/bin/env python3
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator

from common import ALGORITHMS, args, serialized_args, k_formatter
from logparser import *
from prelude import plt

fig, plots = plt.subplots(1, 2, figsize=(3.2, 0.9), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))
plots[0].set_title("Traffic", pad=0)
plots[0].set_ylabel("Bytes per Req. (log)", labelpad=1)
plots[0].set_yscale("log")
plots[0].yaxis.set_major_formatter(k_formatter)
plots[1].set_title("Communication", pad=0)
plots[1].set_ylabel("Msg. per Req.", labelpad=1)
plots[1].yaxis.set_major_formatter(k_formatter)
plots[1].yaxis.set_major_locator(MultipleLocator(50))
plots[1].yaxis.set_minor_locator(MultipleLocator(25))
for plot in plots:
    plot.set_xlabel("Number of Servers", labelpad=1)
    plot.grid(axis="y", which="major", linestyle="--", linewidth="0.5")
    plot.grid(axis="y", which="minor", linestyle=":", linewidth="0.25")
    plot.grid(axis="x", which="major", linestyle="--", linewidth="0.5")
    plot.grid(axis="x", which="minor", linestyle=":", linewidth="0.25")
    plot.tick_params(axis="both", which="major", pad=0.5)
    plot.tick_params(axis="both", which="minor", pad=0.5)
    plot.xaxis.set_major_locator(MultipleLocator(4, 3))
    # plot.xaxis.set_minor_locator(MultipleLocator(3, 3))
    plot.set_xlim(3, 31)

for experiment in ALGORITHMS:
    xs = []
    ys_bytes = []
    ys_msgs = []
    for num_replicas in range(3, 33, 2):
        req_per_replica = args.requests // num_replicas
        config = args.config.replace("@", str(num_replicas))
        logs = parse(
            algo=experiment,
            config=config,
            writes=args.writes,
            requests=req_per_replica,
            ingress=args.ingress,
            throughput=args.throughput,
            speedup=args.speedup,
        )
        assert (
            len(logs["network-done"]) == num_replicas
        ), f'Some replicas ({num_replicas - len(logs["network-done"])} out of {num_replicas}) did not report networking stats'
        total_bytes = sum(log["byte_count"] for log in logs["network-done"])
        total_msgs = sum(log["msg_count"] for log in logs["network-done"])
        xs.append(num_replicas)
        ys_bytes.append(total_bytes / (req_per_replica * num_replicas))
        ys_msgs.append(total_msgs / (req_per_replica * num_replicas))
        print(experiment, num_replicas, ys_bytes[-1], ys_msgs[-1])
    plots[0].plot(xs, ys_bytes, **ALGORITHMS[experiment], markevery=(1, 3))
    plots[1].plot(xs, ys_msgs, **ALGORITHMS[experiment], markevery=(1, 3))

fig.subplots_adjust(wspace=0.4, hspace=0)

legends = [Line2D([0], [0], **algo) for algo in ALGORITHMS.values()]

fig.legend(
    handles=legends,
    bbox_to_anchor=(0.14, 1.29, 0.75, 0.01),
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

pdf_path = f"plots/6-network{serialized_args}.pdf"
plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
print(pdf_path)
