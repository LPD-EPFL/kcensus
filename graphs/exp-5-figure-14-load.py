#!/usr/bin/env python3
"""Figure 14: mean latency against throughput, with one panel per skew.

An incomplete, missing, or latency-drifting run terminates that algorithm's curve.

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
offscreen_latency = 800
series = {workload: {} for workload in workloads}
drawn = set()

for workload in workloads:
    print("#" * 82)
    print(f"Load -- {workload.label}:")
    for algo in ALGORITHMS:
        plot_throughputs = []
        latencies = []
        reported_latencies = []
        sustained = True
        for throughput in plotted_throughputs:
            if not sustained:
                reported_latencies.append("—")
                continue
            stats = run_stats(
                algo=algo,
                workload=workload,
                throughput=throughput,
                reject_latency_drift=True,
            )
            if stats is None or not stats.sustained or not stats.latencies:
                sustained = False
                # Duplicate the last sustained x-coordinate above the visible range. Its marker
                # is clipped, while the incoming segment represents infinite latency vertically.
                if plot_throughputs:
                    plot_throughputs.append(plot_throughputs[-1])
                    latencies.append(offscreen_latency)
                reason = stats.failure_reason if stats is not None else "missing logs"
                reported_latencies.append(f"unsustained ({reason})")
            else:
                mean = sum(stats.latencies) / len(stats.latencies)
                plot_throughputs.append(throughput)
                latencies.append(mean)
                reported_latencies.append(f"{mean:.1f}ms")
                drawn.add(algo)
        series[workload][algo] = (plot_throughputs, latencies)
        print(f"  {algo}: " + ", ".join(
            f"{throughput:g}->{reported}"
            for throughput, reported in zip(plotted_throughputs, reported_latencies)
        ))

if not drawn:
    print("nothing to plot, skipping figure")
    raise SystemExit(0)

fig, subplot_grid = plt.subplots(
    len(workloads),
    1,
    figsize=(3.26, 3.5),
    sharex=True,
    sharey=True,
    squeeze=False,
    tight_layout=True,
)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)
fig.subplots_adjust(hspace=0.35)
plots = subplot_grid[:, 0]

for index, (plot, workload) in enumerate(zip(plots, workloads)):
    plot.set_title("Average latency under load, " + workload.label, pad=0)
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
        plot.set_xlabel("Throughput (req/s)", labelpad=0.2)
    if index == len(workloads) // 2:
        plot.set_ylabel("Mean latency (ms)", labelpad=1)

    for algo, (throughputs, latencies) in series[workload].items():
        plot.plot(throughputs, latencies, clip_on=True, **ALGORITHMS[algo])

legends = [Line2D([0], [0], **ALGORITHMS[algo]) for algo in ALGORITHMS if algo in drawn]
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

pdf_path = f"plots/{PLOT_PREFIX}exp-5-figure-14-load.pdf"
plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
plt.close(fig)
print(pdf_path)
