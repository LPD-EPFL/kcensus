#!/usr/bin/env python3
from matplotlib.lines import Line2D
from matplotlib.ticker import *

from common import ALGORITHMS, args
from logparser import *
from prelude import plt

fig, plot = plt.subplots(figsize=(2.975, 0.8), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))

plot.set_title('Request Latency CDF', pad=0)
plot.set_xlabel('Latency (ms)', labelpad=1)
plot.set_ylabel('Percentile')
plot.grid(axis='y', which='major', linestyle='--', linewidth='0.5')
plot.grid(axis='y', which='minor', linestyle=':', linewidth='0.25')
plot.grid(axis='x', which='major', linestyle='--', linewidth='0.5')
plot.grid(axis='x', which='minor', linestyle=':', linewidth='0.25')
plot.tick_params(axis='both', which='major', pad=0.5)
plot.tick_params(axis='both', which='minor', pad=0.5)
plot.yaxis.set_minor_locator(MultipleLocator(25))
plot.yaxis.set_major_locator(MultipleLocator(50))
plot.xaxis.set_minor_locator(MultipleLocator(10))
plot.xaxis.set_major_locator(MultipleLocator(20))
plot.set_axisbelow(True)
plot.set_xlim(0, 70)

legends = []
for experiment in ALGORITHMS.keys():
    logs = parse(algo=experiment, config=args.config, writes=args.writes, requests=args.requests, ingress=args.ingress,
                 throughput=args.throughput)

    average = compute_average(logs['executed'], lambda log: duration_to_ms(log['latency']))
    percentiles = [-999999] + compute_percentiles(logs['executed'], lambda log: duration_to_ms(log['latency'])) + [
        999999]
    plot.plot(percentiles, [0, 0.1] + list(range(1, 100)) + [99.9, 100], **ALGORITHMS[experiment],
              markevery=(26, 25))
    legends.append(Line2D([0], [0], **ALGORITHMS[experiment]))

fig.legend(handles=legends, bbox_to_anchor=(0.1, 1.29, 0.75, 0.01),
           loc='center', edgecolor='black', borderaxespad=0, ncols=3,
           borderpad=.3, labelspacing=0.1, mode='expand',
           handlelength=0.8, handletextpad=0.5)

plt.savefig(
    f'plots/2-cdfs-c={args.config}-w={args.writes:g}-r={args.requests}-i={args.ingress}-t={args.throughput:g}.pdf',
    format='pdf', bbox_inches='tight', pad_inches=0.01)
