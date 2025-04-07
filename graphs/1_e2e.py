#!/usr/bin/env python3

from matplotlib.ticker import *

from common import ALGORITHMS
from logparser import *
from prelude import plt

fig, plot = plt.subplots(figsize=(2.975, 0.8), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))

experiments = ('paxos', 'multi-paxos', 'e-paxos', 'k-census', 'unreplicated')

plot.yaxis.set_minor_locator(MultipleLocator(50))
plot.yaxis.set_major_locator(MultipleLocator(100))
plot.grid(axis='y', which='major', linestyle='--', linewidth='0.5')
plot.grid(axis='y', which='minor', linestyle=':', linewidth='0.25')
plot.set_axisbelow(True)
plot.tick_params(axis='both', which='major', pad=0.5)
plot.tick_params(axis='both', which='minor', pad=0.5)

xs = []
ys = []
delta_ys_top = []
delta_ys_bottom = []
labels = []
colors = []
for i, experiment in enumerate(experiments):
    logs = parse(algo=experiment)
    percentiles = compute_percentiles(logs['executed'], lambda log: duration_to_ms(log['latency']))
    percentiles = percentiles[5], percentiles[50], percentiles[95]
    print(experiment, percentiles)
    xs.append(i)
    ys.append(percentiles[1])
    delta_ys_top.append(percentiles[2] - percentiles[1])
    delta_ys_bottom.append(percentiles[1] - percentiles[0])
    labels.append(ALGORITHMS[experiment]['label'])
    colors.append(ALGORITHMS[experiment]['color'])
    plot.text(i, percentiles[1] / 2, f'{int(percentiles[1])}\N{thin space}ms', horizontalalignment='center',
              verticalalignment='center')

plot.bar(labels, ys, label=labels, lw=0, color=colors)
plot.errorbar(xs, ys, [delta_ys_bottom, delta_ys_top], ls='none', color='black', solid_capstyle='projecting',
              capsize=2.5)
plot.set_title('Median Request Latency', pad=0)

plot.yaxis.set_major_locator(MultipleLocator(80))
plot.yaxis.set_minor_locator(MultipleLocator(20))
plt.gca().xaxis.set_tick_params(pad=10)
plt.xticks(ha='center', va='center')

plot.set_ylabel('Duration (ms)', labelpad=1)

plt.savefig(f'plots/1-e2e.pdf', format='pdf', bbox_inches='tight', pad_inches=0.01)
