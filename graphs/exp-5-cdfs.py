#!/usr/bin/env python3
"""Latency CDF, one plot per skew.

  ./exp-5-cdfs.py              # all three
  ./exp-5-cdfs.py --skew 0.99  # only that skew
  ./exp-5-cdfs.py -t 2000      # a different rung, from the load sub-experiment
"""
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator

from common import PLOT_PREFIX, args
from exp5 import (
    CDF_WORKLOADS,
    CDF_THROUGHPUT,
    EXP5_CDF_DURATION,
    EXP5_DURATION,
    EXP5_ALGORITHMS as ALGORITHMS,
    WRITES,
    run_stats,
    selected_throughput,
    selected_workloads,
)
from logparser import *
from prelude import plt

throughput = selected_throughput()
duration = EXP5_CDF_DURATION if throughput == CDF_THROUGHPUT else EXP5_DURATION

for workload in selected_workloads(CDF_WORKLOADS):
    fig, plot = plt.subplots(figsize=(3.11, 2.7), tight_layout=True)
    plt.tight_layout(pad=0, w_pad=0, h_pad=0)

    title = f"Request Latency CDF -- {workload.label}"
    plot.set_title(title, pad=0)
    print("#" * 82)
    print(title + ":")
    plot.set_xlabel("Request Latency (ms)", labelpad=1)
    plot.set_ylabel("Percentile", labelpad=0)
    plot.grid(axis="y", which="major", linestyle="--", linewidth="0.5")
    plot.grid(axis="y", which="minor", linestyle=":", linewidth="0.25")
    plot.grid(axis="x", which="major", linestyle="--", linewidth="0.5")
    plot.grid(axis="x", which="minor", linestyle=":", linewidth="0.25")
    plot.tick_params(axis="both", which="major", pad=0.5)
    plot.tick_params(axis="both", which="minor", pad=0.5)
    plot.yaxis.set_minor_locator(MultipleLocator(25))
    plot.yaxis.set_major_locator(MultipleLocator(50))
    plot.set_axisbelow(True)

    drawn = []
    for algo in ALGORITHMS:
        stats = run_stats(
            algo=algo, workload=workload, throughput=throughput, duration=duration
        )
        latencies = stats[0] if stats else []
        if not latencies:
            print(f"  {algo}: no data, skipped")
            continue
        percentiles = compute_percentiles(latencies)
        print(f"  {algo} avg={sum(latencies) / len(latencies):.2f}ms "
              f"p50={percentiles[50]:.2f} p99={percentiles[99]:.2f}")
        # Flat ends, so the step reaches 0 and 100 rather than the extreme samples.
        plot.plot(
            [-999999] + percentiles + [999999],
            [0, 0.0001] + list(range(1, 100)) + [99.9999, 100],
            **ALGORITHMS[algo],
            markevery=[2, 26, 51, 76, 100],
        )
        drawn.append(algo)

    if not drawn:
        print("  nothing to plot, skipping figure")
        plt.close(fig)
        continue

    plot.set_xlim(0, workload.latency_xlim)
    fig.legend(
        handles=[Line2D([0], [0], **ALGORITHMS[a]) for a in drawn],
        bbox_to_anchor=(0, 1.09, 1, 0.28),
        loc="center",
        edgecolor="black",
        borderaxespad=0,
        ncols=2,
        borderpad=0.3,
        labelspacing=0.1,
        handlelength=0.8,
        handletextpad=0.5,
    )

    pdf_path = (
        f"plots/{PLOT_PREFIX}exp-5-cdf-{workload.slug}-t{throughput:g}.pdf"
    )
    plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
    plt.close(fig)
    print(pdf_path)
