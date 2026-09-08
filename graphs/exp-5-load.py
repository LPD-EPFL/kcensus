#!/usr/bin/env python3
"""Latency against offered load: four plots, `LOAD_SKEWS` by key count -- the only
combinations given the full ladder.

Upper panel: mean latency. Lower: achieved against offered throughput, with the diagonal.

  ./exp-5-load.py              # both
  ./exp-5-load.py --skew 0.99  # one
"""
from matplotlib.lines import Line2D
from matplotlib.ticker import ScalarFormatter

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

for workload in selected_workloads(LOAD_WORKLOADS):
    fig, subplots = plt.subplots(2, 1, figsize=(3.155, 2.9), tight_layout=True, sharex=True)
    plt.tight_layout(pad=0, w_pad=0, h_pad=0)
    fig.subplots_adjust(hspace=0.28)
    latency_plot, throughput_plot = subplots

    title = f"Load -- {workload.label}"
    latency_plot.set_title(title, pad=0)
    print("#" * 82)
    print(title + ":")
    for plot in subplots:
        plot.grid(axis="y", which="major", linestyle="--", linewidth="0.5")
        plot.grid(axis="x", which="major", linestyle="--", linewidth="0.5")
        plot.tick_params(axis="both", which="major", pad=0.5)
        plot.set_axisbelow(True)
        plot.set_xscale("log")
        plot.xaxis.set_major_formatter(ScalarFormatter())
    latency_plot.set_ylim(0, 400)
    latency_plot.set_ylabel("Mean latency (ms)", labelpad=1)
    throughput_plot.set_ylabel("Achieved (req/s)", labelpad=1)
    throughput_plot.set_xlabel("Offered throughput (req/s)", labelpad=0.2)

    drawn = []
    for algo in ALGORITHMS:
        xs, latencies, achieved = [], [], []
        for offered in THROUGHPUTS:
            stats = run_stats(
                algo=algo, workload=workload, throughput=offered
            )
            if not stats or not stats[0]:
                continue
            samples, completed = stats
            xs.append(offered)
            latencies.append(sum(samples) / len(samples))
            achieved.append(completed)
        if not xs:
            print(f"  {algo}: no data, skipped")
            continue
        print(f"  {algo}: " + ", ".join(
            f"{x:g}->{y:.1f}ms" for x, y in zip(xs, latencies)
        ))
        latency_plot.plot(xs, latencies, **ALGORITHMS[algo])
        throughput_plot.plot(xs, achieved, **ALGORITHMS[algo])
        drawn.append(algo)

    if not drawn:
        print("  nothing to plot, skipping figure")
        plt.close(fig)
        continue

    throughput_plot.plot(
        list(THROUGHPUTS), list(THROUGHPUTS),
        color="black", lw=0.5, linestyle=":", label="offered",
    )
    fig.legend(
        handles=[Line2D([0], [0], **ALGORITHMS[a]) for a in drawn],
        bbox_to_anchor=(0, 1.06, 1, 0.28),
        loc="center",
        edgecolor="black",
        borderaxespad=0,
        ncols=3,
        borderpad=0.3,
        labelspacing=0.1,
        handlelength=0.8,
        handletextpad=0.5,
    )

    pdf_path = f"plots/{PLOT_PREFIX}exp-5-load-{workload.slug}.pdf"
    plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
    plt.close(fig)
    print(pdf_path)
