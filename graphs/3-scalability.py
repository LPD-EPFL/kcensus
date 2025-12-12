#!/usr/bin/env python3
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator, ScalarFormatter

from common import ALGORITHMS, args, serialized_args
from logparser import *
from prelude import plt

fig, plot = plt.subplots(figsize=(2.975, 0.8), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))
plot.set_title("Average Latency", pad=0)
plot.set_ylabel('Latency (ms)', labelpad=1)
plot.set_xlabel('Number of Replicas', labelpad=1)
plot.grid(axis='y', which='major', linestyle='--', linewidth='0.5')
plot.grid(axis='y', which='minor', linestyle=':', linewidth='0.25')
plot.tick_params(axis='both', which='major', pad=0.5)
plot.tick_params(axis='both', which='minor', pad=0.5)
plot.xaxis.set_major_locator(MultipleLocator(4, 3))
plot.xaxis.set_minor_locator(MultipleLocator(2, 3))
plot.set_yscale("log")
plot.yaxis.set_major_formatter(ScalarFormatter())
plot.set_xlim(3, 19)

min_y = 1000
for experiment in ALGORITHMS:
    xs = []
    ys = []
    # percentiles_ys = ([], [])
    for num_replicas in range(3, 19+2, 2):
        config = args.config.replace('@', str(num_replicas))
        logs = parse(algo=experiment, config=config, writes=args.writes, requests=args.requests,
                     ingress=args.ingress, throughput=args.throughput, speedup=args.speedup)
        average = compute_average(logs['executed'], lambda log: duration_to_ms(log['latency']))
        percentiles = compute_percentiles(logs['executed'], lambda log: duration_to_ms(log['latency']))
        MOUSTACHES = (5, 95)
        print(experiment, num_replicas, average, (percentiles[MOUSTACHES[0]], percentiles[MOUSTACHES[1]]))
        xs.append(num_replicas)
        ys.append(average)
        # percentiles_ys[0].append(percentiles[MOUSTACHES[0]])
        # percentiles_ys[1].append(percentiles[MOUSTACHES[1]])
        min_y = min(min_y, average)

    plot.plot(xs, ys, **ALGORITHMS[experiment], markevery=(1, 3))
    # for ys in percentiles_ys:
    #     plot.plot(xs, ys, color=ALGORITHMS[experiment]['color'], linestyle="--")

plot.set_ylim(min_y, 1000)

legends = [
    Line2D([0], [0], **algo)
    for algo in ALGORITHMS.values()
]

fig.legend(handles=legends, bbox_to_anchor=(0.1, 1.29, 0.75, 0.01),
           loc='center', edgecolor='black', borderaxespad=0, ncols=3,
           borderpad=.3, labelspacing=0.1, mode='expand',
           handlelength=0.8, handletextpad=0.5)

pdf_path = f'plots/3-scalability{serialized_args}.pdf'
plt.savefig(pdf_path,
            format='pdf', bbox_inches='tight', pad_inches=0.01)
print(pdf_path)
