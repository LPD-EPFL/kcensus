#!/usr/bin/env python3
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator

from logparser import *
from prelude import plt

config_lists = {
    "aws-random/@.toml": {
        "label": "Random Deployments",
        "marker": "o",
        "markersize": 3.2,
        "color": "#808078",
        "linewidth": 1.2,
    },
    "aws-from-paris/@.toml": {
        "label": "Parisian Deployments",
        "marker": "x",
        "markersize": 4.4,
        "color": "#000008",
        "linewidth": 0.8,
        "linestyle": "--",
        "markeredgewidth": 0.7,
    },
}

fig, plot = plt.subplots(figsize=(3.235, 0.75), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))
plot.set_title("Computing Optimal Requirements", pad=0)
plot.set_ylabel("Duration (ms)", labelpad=1)
plot.set_xlabel("Number of Replicas", labelpad=1)
plot.grid(axis="y", which="major", linestyle="--", linewidth="0.5")
plot.grid(axis="y", which="minor", linestyle=":", linewidth="0.25")
plot.grid(axis="x", which="major", linestyle="--", linewidth="0.5")
plot.grid(axis="x", which="minor", linestyle=":", linewidth="0.25")
plot.tick_params(axis="both", which="major", pad=0.5)
plot.tick_params(axis="both", which="minor", pad=0.5)
plot.xaxis.set_major_locator(MultipleLocator(4, 3))
plot.xaxis.set_minor_locator(MultipleLocator(2, 3))
plot.yaxis.set_major_locator(MultipleLocator(50))
plot.yaxis.set_minor_locator(MultipleLocator(25))
plot.set_xlim(3, 31)

for config_list, cl_style in config_lists.items():
    xs = []
    ys = []
    # percentiles_ys = ([], [])
    for num_replicas in range(3, 31 + 2, 2):
        config = config_list.replace("@", str(num_replicas))
        with open(f"../logs/c={config}/graph_bench.stdout") as file:
            logs = parse_file(file)["graph-generation"]
            assert len(logs) == 1
            xs.append(num_replicas)
            ys.append(duration_to_ms(logs[0]["average"]))
            print(config, xs[-1], ys[-1])
    plot.plot(xs, ys, **cl_style, markevery=(1, 3))

legends = [Line2D([0], [0], **cl_style) for cl_style in config_lists.values()]

fig.legend(
    handles=legends,
    bbox_to_anchor=(0.09, 1.27, 0.85, 0.01),
    loc="center",
    edgecolor="black",
    borderaxespad=0,
    ncols=3,
    borderpad=0.3,
    labelspacing=0.1,
    mode="expand",
    handlelength=1.2,
    handletextpad=0.5,
)

pdf_path = f"plots/5-propagation.pdf"
plt.savefig(pdf_path, format="pdf", bbox_inches="tight", pad_inches=0.01)
print(pdf_path)
