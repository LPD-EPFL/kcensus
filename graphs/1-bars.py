#!/usr/bin/env python3

from common import ALGORITHMS, args, serialized_args
from logparser import *
from prelude import plt

fig, plot = plt.subplots(figsize=(2.975, 0.8), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))

plot.set_title('Average Request Latency', pad=0)
plot.set_ylabel('Latency (ms)', labelpad=1)
plot.grid(axis='y', which='major', linestyle='--', linewidth='0.5')
plot.grid(axis='y', which='minor', linestyle=':', linewidth='0.25')
plot.tick_params(axis='both', which='major', pad=0.5)
plot.tick_params(axis='both', which='minor', pad=0.5)
# plot.yaxis.set_major_locator(MultipleLocator(50))
# plot.yaxis.set_minor_locator(MultipleLocator(25))
plt.gca().xaxis.set_tick_params(pad=10)
plot.set_axisbelow(True)
plt.xticks(ha='center', va='center')

xs = []
ys = []
delta_ys_top = []
delta_ys_bottom = []
labels = []
colors = []
for i, experiment in enumerate(ALGORITHMS.keys()):
    logs = parse(algo=experiment, config=args.config, writes=args.writes, requests=args.requests, ingress=args.ingress,
                 throughput=args.throughput, speedup=args.speedup, faults=args.faults)
    average = compute_average(logs['executed'], lambda log: duration_to_ms(log['latency']))
    percentiles = compute_percentiles(logs['executed'], lambda log: duration_to_ms(log['latency']))
    MOUSTACHES = (5, 95)
    print(experiment, average, (percentiles[MOUSTACHES[0]], percentiles[MOUSTACHES[1]]))
    xs.append(i)
    ys.append(average)
    delta_ys_top.append(percentiles[MOUSTACHES[1]] - average)
    delta_ys_bottom.append(average - percentiles[MOUSTACHES[0]])

    labels.append(ALGORITHMS[experiment]['label'].replace(' ', '\n').replace('-', '-\n'))
    colors.append(ALGORITHMS[experiment]['color'])
    text_y = percentiles[MOUSTACHES[1]]  # average / 2
    plot.text(i, text_y, f'{int(average)}\N{thin space}ms', horizontalalignment='center',
              verticalalignment='bottom')

plot.bar(labels, ys, lw=0, color=colors)
plot.errorbar(xs, ys, [delta_ys_bottom, delta_ys_top], ls='none', color='black', solid_capstyle='projecting',
              capsize=2.5)

# For the average text to fit
ymin, ymax = plot.get_ylim()
plot.set_ylim(ymin, ymax * 1.3)

pdf_path = f'plots/1-bars{serialized_args}.pdf'
plt.savefig(pdf_path,
            format='pdf', bbox_inches='tight', pad_inches=0.01)
print(pdf_path)
