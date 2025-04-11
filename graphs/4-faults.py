#!/usr/bin/env python3
from itertools import combinations

from matplotlib import patches
from matplotlib.ticker import NullLocator, NullFormatter, MultipleLocator

from common import ALGORITHMS, args, serialized_args
from logparser import *
from prelude import plt

algorithms = ['weak-replication', 'k-census', 'e-paxos', 'multi-paxos', 'paxos']

fig, plots = plt.subplots(1, 5, figsize=(2.975, 0.8), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))

for num_faults, plot in enumerate(plots):
    plot.set_title(f'{num_faults} Fault{["s", ""][num_faults == 1]}', pad=0)
    if num_faults == 0:
        plot.set_ylabel('Latency (ms)', labelpad=1)
    else:
        plot.yaxis.set_major_formatter(NullFormatter())
    plot.grid(axis='y', which='major', linestyle='--', linewidth='0.5')
    plot.grid(axis='y', which='minor', linestyle=':', linewidth='0.25')
    plot.tick_params(axis='both', which='major', pad=0.5)
    plot.tick_params(axis='both', which='minor', pad=0.5)
    plot.xaxis.set_major_locator(NullLocator())
    plot.xaxis.set_minor_locator(NullLocator())
    plot.yaxis.set_major_locator(MultipleLocator(1000))
    plot.yaxis.set_minor_locator(MultipleLocator(250))
    plt.gca().xaxis.set_tick_params(pad=10)
    plot.set_axisbelow(True)
    plt.xticks(ha='center', va='center')

    xs = []
    ys = []
    delta_ys_top = []
    delta_ys_bottom = []
    labels = []
    colors = []
    for i, experiment in enumerate(algorithms):
        executed = []
        num_replicas = int(''.join([char for char in args.config if char.isdigit()]))
        all_faults = [','.join(map(str, comb)) for comb in combinations(range(num_replicas), num_faults)]
        for faults in all_faults:
            logs = parse(algo=experiment, config=args.config, writes=args.writes, requests=args.requests,
                         ingress=args.ingress,
                         throughput=args.throughput, speedup=args.speedup, faults=faults)
            executed += logs['executed']
        average = compute_average(executed, lambda log: duration_to_ms(log['latency']))
        percentiles = compute_percentiles(executed, lambda log: duration_to_ms(log['latency']))
        MOUSTACHES = (5, 95)
        print(experiment, average, (percentiles[MOUSTACHES[0]], percentiles[MOUSTACHES[1]]))
        xs.append(i)
        labels.append(i)
        ys.append(average)
        delta_ys_top.append(percentiles[MOUSTACHES[1]] - average)
        delta_ys_bottom.append(average - percentiles[MOUSTACHES[0]])
        colors.append(ALGORITHMS[experiment]['color'])

    plot.bar(labels, ys, lw=0, color=colors)
    plot.errorbar(xs, ys, [delta_ys_bottom, delta_ys_top], ls='none', color='black', solid_capstyle='projecting',
                  capsize=1.5)

max_y = max(plot.get_ylim()[1] for plot in plots)
for plot in plots:
    plot.set_ylim(0, max_y * 1.1)
fig.subplots_adjust(wspace=0, hspace=0)

legends = [
    patches.Patch(color=ALGORITHMS[algo]['color'], label=ALGORITHMS[algo]['label'])
    for algo in algorithms
]

fig.legend(handles=legends, bbox_to_anchor=(0, 1.29, 1, 0.01),
           loc='center', edgecolor='black', borderaxespad=0, ncols=5,
           borderpad=.3, labelspacing=0.1, mode='expand',
           handlelength=0.8, handletextpad=0.5)

pdf_path = f'plots/4-faults{serialized_args}.pdf'
plt.savefig(pdf_path,
            format='pdf', bbox_inches='tight', pad_inches=0.01)
print(pdf_path)
