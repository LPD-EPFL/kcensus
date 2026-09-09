#!/usr/bin/env python3
"""Figure 13: latency under conflicts, with one CDF panel per skew.

  ./exp-5-figure-13-conflict.py              # all three
  ./exp-5-figure-13-conflict.py --skew 0.99  # only that skew
  ./exp-5-figure-13-conflict.py -t 2000      # a different load rung
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
workloads = selected_workloads(CDF_WORKLOADS)

fig, subplot_grid = plt.subplots(
    len(workloads),
    1,
    figsize=(3.26, 3.5),
    sharey=True,
    squeeze=False,
    tight_layout=True,
)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)
fig.subplots_adjust(hspace=0.35)
plots = subplot_grid[:, 0]
drawn = set()

for index, (plot, workload) in enumerate(zip(plots, workloads)):
    title = "Latency CDF with conflicts, " + workload.label
    plot.set_title(title, pad=0)
    print("#" * 82)
    print(f"Request Latency CDF -- {title}:")
    if index == len(workloads) - 1:
        plot.set_xlabel("Request Latency (ms)", labelpad=0.2)
    if index == len(workloads) // 2:
        plot.set_ylabel("Percentile", labelpad=1)
    plot.grid(axis="y", which="major", linestyle="--", linewidth="0.5")
    plot.grid(axis="y", which="minor", linestyle=":", linewidth="0.25")
    plot.grid(axis="x", which="major", linestyle="--", linewidth="0.5")
    plot.grid(axis="x", which="minor", linestyle=":", linewidth="0.25")
    plot.tick_params(axis="both", which="major", pad=0.5)
    plot.tick_params(axis="both", which="minor", pad=0.5)
    plot.yaxis.set_minor_locator(MultipleLocator(25))
    plot.yaxis.set_major_locator(MultipleLocator(50))
    plot.set_axisbelow(True)

    for algo in ALGORITHMS:
        stats = run_stats(
            algo=algo, workload=workload, throughput=throughput, duration=duration
        )
        if stats is None or not stats.sustained or not stats.latencies:
            print(f"  {algo}: no sustained data, skipped")
            continue
        percentiles = compute_percentiles(stats.latencies)
        print(f"  {algo} avg={sum(stats.latencies) / len(stats.latencies):.2f}ms "
              f"p50={percentiles[50]:.2f} p99={percentiles[99]:.2f}")
        # Flat ends, so the step reaches 0 and 100 rather than the extreme samples.
        plot.plot(
            [-999999] + percentiles + [999999],
            [0, 0.0001] + list(range(1, 100)) + [99.9999, 100],
            **ALGORITHMS[algo],
            markevery=[2, 26, 51, 76, 100],
        )
        drawn.add(algo)

    plot.set_xlim(0, workload.latency_xlim)

if not drawn:
    print("nothing to plot, skipping figure")
    plt.close(fig)
    raise SystemExit(0)

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
plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
plt.close(fig)
print(pdf_path)
