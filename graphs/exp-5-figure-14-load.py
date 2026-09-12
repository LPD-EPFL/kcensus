#!/usr/bin/env python3
"""Figure 14: all-request mean latency against achieved throughput.

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
latency_graphs = (("All-request", ""),)
run_results = {workload: {} for workload in workloads}


def latency_summary(stats):
    """Average, p50, and p99 over all put and read requests pooled together."""
    samples = stats.put_latencies + stats.read_latencies
    if not samples:
        return None
    percentiles = compute_percentiles(samples)
    return sum(samples) / len(samples), percentiles[50], percentiles[99]


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

for latency_label, filename_suffix in latency_graphs:
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
                    reported_latencies.append(
                        f"input={throughput:g} req/s; status=missing logs"
                    )
                    continue
                summary = latency_summary(stats)
                if summary is None:
                    reported_latencies.append(
                        f"input={throughput:g} req/s; status=no samples"
                    )
                    continue
                mean, p50, p99 = summary
                achieved_throughput = stats.estimated_throughput
                if achieved_throughput is None:
                    reported_latencies.append(
                        f"input={throughput:g} req/s; average={mean:.2f} ms; "
                        f"p50={p50:.2f} ms; p99={p99:.2f} ms; "
                        "status=no throughput estimate"
                    )
                    continue
                measurement = (
                    f"input={throughput:g} req/s; "
                    f"achieved={achieved_throughput:.2f} req/s; "
                    f"average={mean:.2f} ms; p50={p50:.2f} ms; p99={p99:.2f} ms"
                )
                if (
                    previous_throughput is not None
                    and achieved_throughput < previous_throughput
                ):
                    reported_latencies.append(
                        f"{measurement}; status=dropped (below previous plotted throughput)"
                    )
                    continue
                points.append((achieved_throughput, mean))
                previous_throughput = achieved_throughput
                details = ["plotted", "aborted" if stats.aborted else "completed"]
                if stats.from_failed_logs:
                    details.append("source=logs/failed")
                reported_latencies.append(
                    f"{measurement}; status={', '.join(details)}"
                )
                drawn.add(algo)
            plot_throughputs = [point[0] for point in points]
            latencies = [point[1] for point in points]
            series[workload][algo] = (plot_throughputs, latencies)
            print(f"  {algo}:")
            for reported in reported_latencies:
                print(f"    {reported}")

    if not drawn:
        print("nothing to plot, skipping graph")
        continue

    fig, subplot_grid = plt.subplots(
        len(workloads),
        1,
        figsize=(3.26, 1.95),
        sharex=True,
        sharey=True,
        squeeze=False,
        tight_layout=True,
    )
    fig.tight_layout(pad=0, w_pad=0, h_pad=0)
    fig.subplots_adjust(hspace=0.04)
    plots = subplot_grid[:, 0]

    for index, (plot, workload) in enumerate(zip(plots, workloads)):
        plot.text(
            0.97,
            0.05,
            workload.label,
            transform=plot.transAxes,
            horizontalalignment="right",
            verticalalignment="bottom",
            multialignment="center",
            fontsize=8,
            bbox={
                "boxstyle": "square,pad=0.15",
                "facecolor": "white",
                "edgecolor": "black",
                "linewidth": 0.5,
            },
            zorder=10,
        )
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
        plot.yaxis.set_minor_locator(MultipleLocator(50))
        plot.set_xlim(0, 35000)
        plot.set_ylim(0, 450)
        if index != len(workloads) - 1:
            plot.tick_params(axis="x", which="both", labelbottom=False, length=0)

        for algo, (throughputs, latencies) in series[workload].items():
            plot.plot(throughputs, latencies, clip_on=True, **ALGORITHMS[algo])

    fig.supylabel("Mean latency (ms)", x=-0.04)
    fig.supxlabel("Achieved throughput (req/s)", y=-0.05)
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
