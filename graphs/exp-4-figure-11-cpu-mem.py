#!/usr/bin/env python3
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator

from common import ALGORITHMS, args, ki_formatter
from logparser import *
from prelude import plt

fig, plots = plt.subplots(1, 2, figsize=(3.26, 0.93), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))
plots[0].set_title("Average Compute", pad=0)
plots[0].set_ylabel("CPU time (s)", labelpad=1)
# plots[0].yaxis.set_major_locator(MultipleLocator(1))
# plots[0].yaxis.set_major_formatter(k_formatter)
plots[1].set_title("Average Shard Memory", pad=0)
plots[1].set_ylabel("Memory (KiB)", labelpad=1)
plots[1].yaxis.set_major_formatter(ki_formatter)
plots[1].yaxis.set_minor_locator(MultipleLocator(2.5))
plots[1].yaxis.set_major_locator(MultipleLocator(5))
# if args.geo ==1:
#     plots[1].set_ylim(5.3 * 1024 * 1024, 10 * 1024 * 1024)
# else:
#     plots[1].set_ylim(7.2 * 1024 * 1024, 10.9 * 1024 * 1024)
plots[0].yaxis.set_minor_locator(MultipleLocator(1))
plots[0].yaxis.set_major_locator(MultipleLocator(2))
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

for i, experiment in enumerate(ALGORITHMS):
    if experiment == "weak-replication": continue
    xs = []
    ys_cpu = []
    ys_mem = []
    for num_replicas in range(3, 31 + 2, 2):
        config = args.config.replace("@", str(num_replicas))
        logs = {}
        for source in ["err", "out"]:
            raw_output = parse(
                algo=experiment,
                config=config,
                writes=args.writes,
                ingress=args.ingress,
                duration=args.duration,
                throughput=args.throughput,
                faults=args.faults,
                speedup=args.speedup,
                keys=args.keys,
                skew=args.skew,
                shards=args.shards,
                std=source,
                conflicts="conflicts=false",
            )
            assert (
                    len(raw_output["time"]) == num_replicas
            ), f'Some replicas ({num_replicas - len(logs["err"]["time"])} out of {num_replicas}) did not report CPU+mem stats'
            flattened_output = defaultdict(list)
            for category, pid_data in raw_output.items():
                for pid, items in pid_data.items():
                    flattened_output[category].extend(items)
            logs[source] = flattened_output

        cpu = sum(map(lambda log: log["user"], logs["out"]["time"])) / num_replicas
        # cpu = compute_average(logs['time'], lambda log: log['user'] + log['system'])
        mem = compute_average(logs["err"]["time"], lambda log: log["memory"] * 1024) / args.shards
        xs.append(num_replicas)
        ys_cpu.append(cpu)
        ys_mem.append(mem / 1024)
        print(experiment, num_replicas, ys_cpu[-1], ys_mem[-1])
    plots[0].plot(xs, ys_cpu, **ALGORITHMS[experiment], markevery=(1, 3), zorder=(2 - i / 100))
    plots[1].plot(xs, ys_mem, **ALGORITHMS[experiment], markevery=(1, 3), zorder=(2 - i / 100))

plots[0].set_ylim(0, 6)
plots[1].set_ylim(0, 15)
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

pdf_path = f"plots/exp-4-figure-11-cpu-mem.pdf"
plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
print(pdf_path)
