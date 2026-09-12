#!/usr/bin/env python3
"""Figure 14: put, read, and all-request mean latency against achieved throughput.

Every run uses estimated achieved throughput; regressing points are omitted.

  ./exp-5-figure-14-load.py              # all
  ./exp-5-figure-14-load.py --skew 0.99  # one
"""
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator, ScalarFormatter

from common import PLOT_PREFIX, args
from exp5 import (
    EXP5_ALGORITHMS as ALGORITHMS,
    LOAD_WORKLOADS,
    THROUGHPUTS,
    run_stats,
    selected_workloads,
)
from logparser import *
from prelude import plt

workloads = selected_workloads(LOAD_WORKLOADS)
plotted_throughputs = tuple(throughput for throughput in THROUGHPUTS if throughput <= 35000)
latency_graphs = (
    ("put", "Put", ""),
    ("read", "Read", "-read"),
    ("average", "All-request", "-average"),
)
run_results = {workload: {} for workload in workloads}


def mean_latency(stats, latency_kind):
    """Mean latency for one operation type or for all requests pooled together."""
    if latency_kind == "put":
        samples = stats.put_latencies
    elif latency_kind == "read":
        samples = stats.read_latencies
    else:
        samples = stats.put_latencies + stats.read_latencies
    return sum(samples) / len(samples) if samples else None


for workload in workloads:
    for algo in ALGORITHMS:
        results = []
        for throughput in plotted_throughputs:
            stats = run_stats(
                algo=algo,
                workload=workload,
                throughput=throughput,
                fallback_to_failed=True,
            )
            results.append(stats)
        run_results[workload][algo] = results

for latency_kind, latency_label, filename_suffix in latency_graphs:
    series = {workload: {} for workload in workloads}
    drawn = set()
    for workload in workloads:
        print("#" * 82)
        print(f"Load -- {workload.label} -- {latency_label} latency:")
        for algo in ALGORITHMS:
            points = []
            reported_latencies = []
            previous_throughput = None
            for throughput, stats in zip(plotted_throughputs, run_results[workload][algo]):
                if stats is None:
                    reported_latencies.append("missing logs")
                    continue
                mean = mean_latency(stats, latency_kind)
                if mean is None:
                    reported_latencies.append("no samples")
                    continue
                achieved_throughput = stats.estimated_throughput
                if achieved_throughput is None:
                    reported_latencies.append("no throughput estimate")
                    continue
                if (
                    previous_throughput is not None
                    and achieved_throughput < previous_throughput
                ):
                    reported_latencies.append(
                        f"dropped@{achieved_throughput:.0f}req/s"
                    )
                    continue
                points.append((achieved_throughput, mean))
                previous_throughput = achieved_throughput
                status = "aborted" if stats.aborted else "achieved"
                if stats.from_failed_logs:
                    status += "[failed/]"
                reported_latencies.append(
                    f"{status}@{achieved_throughput:.0f}req/s->{mean:.1f}ms"
                )
                drawn.add(algo)
            plot_throughputs = [point[0] for point in points]
            latencies = [point[1] for point in points]
            series[workload][algo] = (plot_throughputs, latencies)
            print(f"  {algo}: " + ", ".join(
                f"{throughput:g}->{reported}"
                for throughput, reported in zip(plotted_throughputs, reported_latencies)
            ))

    if not drawn:
        print(f"nothing to plot for {latency_kind} latency, skipping graph")
        continue

    fig, subplot_grid = plt.subplots(
        len(workloads),
        1,
        figsize=(3.26, 3.5),
        sharex=True,
        sharey=True,
        squeeze=False,
        tight_layout=True,
    )
    fig.tight_layout(pad=0, w_pad=0, h_pad=0)
    fig.subplots_adjust(hspace=0.35)
    plots = subplot_grid[:, 0]

    for index, (plot, workload) in enumerate(zip(plots, workloads)):
        plot.set_title(f"{latency_label} latency under load, {workload.label}", pad=0)
        plot.grid(axis="y", which="major", linestyle="--", linewidth="0.5")
        plot.grid(axis="y", which="minor", linestyle=":", linewidth="0.25")
        plot.grid(axis="x", which="major", linestyle="--", linewidth="0.5")
        plot.grid(axis="x", which="minor", linestyle=":", linewidth="0.25")
        plot.tick_params(axis="both", which="major", pad=0.5)
        plot.tick_params(axis="both", which="minor", pad=0.5)
        plot.set_axisbelow(True)
        plot.xaxis.set_major_formatter(ScalarFormatter())
        plot.xaxis.set_major_locator(MultipleLocator(10000))
        plot.xaxis.set_minor_locator(MultipleLocator(5000))
        plot.yaxis.set_major_locator(MultipleLocator(200))
        plot.yaxis.set_minor_locator(MultipleLocator(100))
        plot.set_xlim(0, 35000)
        plot.set_ylim(0, 450)
        if index == len(workloads) - 1:
            plot.set_xlabel("Achieved throughput (req/s)", labelpad=0.2)
        if index == len(workloads) // 2:
            plot.set_ylabel("Mean latency (ms)", labelpad=1)

        for algo, (throughputs, latencies) in series[workload].items():
            plot.plot(throughputs, latencies, clip_on=True, **ALGORITHMS[algo])

    legends = [Line2D([0], [0], **ALGORITHMS[a]) for a in ALGORITHMS if a in drawn]
    fig.legend(
        handles=legends,
        bbox_to_anchor=(-0.012, 1.03, 0.99, 0.07),
        loc="center",
        edgecolor="black",
        borderaxespad=0,
        ncols=len(legends),
        borderpad=0.3,
        labelspacing=0.1,
        mode="expand",
        handlelength=0.8,
        handletextpad=0.5,
    )

    pdf_path = f"plots/{PLOT_PREFIX}exp-5-figure-14-load{filename_suffix}.pdf"
    fig.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
    plt.close(fig)
    print(pdf_path)
