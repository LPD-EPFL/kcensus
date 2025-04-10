#!/usr/bin/env python3
from matplotlib.lines import Line2D
from matplotlib.ticker import *

from common import ALGORITHMS, args, serialized_args
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
plot.set_axisbelow(True)

max_x = 0
legends = []
for experiment in ALGORITHMS.keys():
    logs = parse(algo=experiment, config=args.config, writes=args.writes, requests=args.requests, ingress=args.ingress,
                 throughput=args.throughput, speedup=args.speedup)
    average = compute_average(logs['executed'], lambda log: duration_to_ms(log['latency']))
    percentiles = compute_percentiles(logs['executed'], lambda log: duration_to_ms(log['latency']))
    nice_percentiles = [-999999] + percentiles + [999999]
    plot.plot(nice_percentiles, [0, 0.1] + list(range(1, 100)) + [99.9, 100], **ALGORITHMS[experiment],
              markevery=(26, 25))
    legends.append(Line2D([0], [0], **ALGORITHMS[experiment]))
    if percentiles[-1] > max_x:
        max_x = percentiles[-1]
        plot.set_xlim(0, percentiles[-1])

fig.legend(handles=legends, bbox_to_anchor=(0.1, 1.29, 0.75, 0.01),
           loc='center', edgecolor='black', borderaxespad=0, ncols=3,
           borderpad=.3, labelspacing=0.1, mode='expand',
           handlelength=0.8, handletextpad=0.5)

pdf_path = f'plots/2-cdfs{serialized_args}.pdf'
plt.savefig(pdf_path,
            format='pdf', bbox_inches='tight', pad_inches=0.01)
print(pdf_path)
