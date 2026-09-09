#!/usr/bin/env python3
from matplotlib.lines import Line2D
from matplotlib.ticker import MaxNLocator, MultipleLocator

from common import ALGORITHMS, args, local_throughput, PLOT_PREFIX, local_duration
from logparser import *
from prelude import plt

EXPECTED_INITIAL_POOL_SIZE = min(args.shards, 1000)

fig, plots = plt.subplots(1, 2, figsize=(3.26, 0.93), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))
plots[0].set_title("Average Compute", pad=0)
plots[0].set_ylabel("CPU time (s)", labelpad=1)
plots[1].set_title("Average Peak Memory", pad=0)
plots[1].set_ylabel("Memory (MiB)", labelpad=1)
plots[0].yaxis.set_major_locator(MultipleLocator(2))
plots[1].yaxis.set_major_locator(MultipleLocator(20))
plots[0].set_ylim(0, 8)
# for plot in plots:
    # plot.yaxis.set_major_locator(MaxNLocator(nbins=5, min_n_ticks=3))
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
                duration=local_duration(args.duration, num_replicas, 1),
                throughput=local_throughput(args.throughput, num_replicas),
                faults=args.faults,
                speedup=args.speedup,
                keys=args.keys,
                skew=args.skew,
                shards=args.shards,
                std=source,
                conflicts="conflicts=false",
            )
            assert len(raw_output["time"]) == num_replicas, (
                f'{num_replicas - len(raw_output["time"])} out of {num_replicas} replicas '
                f'did not report {"memory" if source == "err" else "CPU"} stats'
            )
            flattened_output = defaultdict(list)
            for category, pid_data in raw_output.items():
                for pid, items in pid_data.items():
                    flattened_output[category].extend(items)
            logs[source] = flattened_output

        pool_reports = logs["out"]["shard-pool-done"]
        assert len(pool_reports) == num_replicas, (
            f'{num_replicas - len(pool_reports)} out of {num_replicas} replicas did not '
            'report their Rust shard-pool allocation'
        )
        assert all(log["initial_pool_size"] == EXPECTED_INITIAL_POOL_SIZE for log in pool_reports), (
            f"expected every Rust shard pool to start with {EXPECTED_INITIAL_POOL_SIZE} physical "
            "shards"
        )
        assert all(log.get("logical_shards", args.shards) == args.shards for log in pool_reports), (
            f"expected every Rust shard pool to serve {args.shards} logical shards"
        )

        # ProcessTime is process-wide CPU time (user + system), measured over the workload.
        # `user` is accepted for old logs, where this same value had a misleading field name.
        cpu = compute_average(
            logs["out"]["time"],
            lambda log: log["cpu"] if "cpu" in log else log["user"],
        )
        # GNU time's %M is the directly measured peak resident set size in KiB. Plot the whole
        # process RSS: dividing it by the logical shard count was only valid when every logical
        # shard was physically preallocated, and substantially understates a pooled deployment.
        mem = compute_average(logs["err"]["time"], lambda log: log["memory"]) / 1024
        total_pool_size = sum(log["pool_size"] for log in pool_reports)
        total_initial_pool_size = sum(log["initial_pool_size"] for log in pool_reports)
        average_pool_size = total_pool_size / len(pool_reports)
        xs.append(num_replicas)
        ys_cpu.append(cpu)
        ys_mem.append(mem)
        print(
            experiment,
            num_replicas,
            f"cpu_seconds={ys_cpu[-1]}",
            f"peak_memory_mib={ys_mem[-1]}",
            f"preallocated_physical_shards={total_initial_pool_size}",
            f"physical_shards={total_pool_size}",
            f"pool_growth={total_pool_size - total_initial_pool_size}",
            f"average_pool_size={average_pool_size}",
        )
    plots[0].plot(xs, ys_cpu, **ALGORITHMS[experiment], markevery=(1, 3), zorder=(2 - i / 100))
    plots[1].plot(xs, ys_mem, **ALGORITHMS[experiment], markevery=(1, 3), zorder=(2 - i / 100))

plots[0].set_ylim(0, None)
plots[1].set_ylim(0, None)
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

pdf_path = f"plots/{PLOT_PREFIX}exp-3-figure-11-cpu-mem.pdf"
plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
print(pdf_path)
