#!/usr/bin/env python3
"""Figure 13: PUT and GET latency CDFs side by side for each skew.

  ./exp-5-figure-13-conflict.py              # both skews
  ./exp-5-figure-13-conflict.py --skew 0.99  # only that skew
  ./exp-5-figure-13-conflict.py -t 2000      # a different load rung
"""
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator

from common import PLOT_PREFIX
from exp5 import (
    CDF_WORKLOADS,
    CDF_THROUGHPUT,
    EXP5_CDF_DURATION,
    EXP5_DURATION,
    EXP5_ALGORITHMS as ALGORITHMS,
    run_stats,
    selected_throughput,
    selected_workloads,
)
from logparser import compute_percentiles
from prelude import plt

throughput = selected_throughput()
duration = EXP5_CDF_DURATION if throughput == CDF_THROUGHPUT else EXP5_DURATION
workloads = selected_workloads(CDF_WORKLOADS)
operations = (
    ("PUTs", "put_latencies"),
    ("GETs", "read_latencies"),
)

run_results = {workload: {} for workload in workloads}
for workload in workloads:
    for algo in ALGORITHMS:
        run_results[workload][algo] = run_stats(
            algo=algo, workload=workload, throughput=throughput, duration=duration
        )


def print_latency_distribution(algo, samples, percentiles):
    """Print the average and p01-p99 in compact, human-readable groups."""
    print(f"  {algo}:")
    print(f"    average: {sum(samples) / len(samples):.2f} ms")
    for first in range(1, 100, 10):
        last = min(first + 10, 100)
        values = ", ".join(
            f"p{percentile:02d}: {percentiles[percentile]:.2f} ms"
            for percentile in range(first, last)
        )
        print(f"    {values}")


fig, plots = plt.subplots(
    len(workloads),
    len(operations),
    figsize=(3.26, 1.95),
    sharey=True,
    squeeze=False,
    tight_layout=True,
    gridspec_kw={"width_ratios": (1.15, 1)},
)
fig.tight_layout(pad=0, w_pad=0, h_pad=0)
fig.subplots_adjust(wspace=0, hspace=0.04)
drawn = set()

for row, workload in enumerate(workloads):
    for column, (operation_label, latency_field) in enumerate(operations):
        plot = plots[row][column]
        title = f"{operation_label}, {workload.label}"
        plot.text(
            0.97,
            0.05,
            f"{operation_label}\n{workload.label}",
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
        print("#" * 82)
        print(f"Request Latency CDF -- {title}:")

        plot.grid(axis="y", which="major", linestyle="--", linewidth="0.5")
        plot.grid(axis="y", which="minor", linestyle=":", linewidth="0.25")
        plot.grid(axis="x", which="major", linestyle="--", linewidth="0.5")
        plot.grid(axis="x", which="minor", linestyle=":", linewidth="0.25")
        plot.tick_params(axis="both", which="major", pad=0.5)
        plot.tick_params(axis="both", which="minor", pad=0.5)
        plot.yaxis.set_major_locator(MultipleLocator(50))
        plot.yaxis.set_minor_locator(MultipleLocator(25))
        plot.xaxis.set_major_locator(MultipleLocator(100))
        plot.xaxis.set_minor_locator(MultipleLocator(50))
        plot.set_axisbelow(True)
        plot.set_xlim(0, workload.latency_xlim if column == 0 else 370)

        if row != len(workloads) - 1:
            plot.tick_params(axis="x", which="both", labelbottom=False, length=0)
        if column != 0:
            plot.tick_params(axis="y", which="both", left=False, labelleft=False)

        for algo in ALGORITHMS:
            stats = run_results[workload][algo]
            samples = (
                getattr(stats, latency_field)
                if stats is not None and stats.sustained
                else []
            )
            if not samples:
                print(f"  {algo}: no sustained data, skipped")
                continue
            percentiles = compute_percentiles(samples)
            print_latency_distribution(algo, samples, percentiles)
            plot.plot(
                [-999999] + percentiles + [999999],
                [0, 0.0001] + list(range(1, 100)) + [99.9999, 100],
                **ALGORITHMS[algo],
                markevery=[2, 26, 51, 76, 100],
            )
            drawn.add(algo)

if not drawn:
    print("nothing to plot, skipping figure")
    plt.close(fig)
    raise SystemExit(0)

fig.supylabel("Percentile", x=-0.04)
fig.supxlabel("Request Latency (ms)", y=-0.05)
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

pdf_path = f"plots/{PLOT_PREFIX}exp-5-figure-13-conflict.pdf"
fig.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
plt.close(fig)
print(pdf_path)
