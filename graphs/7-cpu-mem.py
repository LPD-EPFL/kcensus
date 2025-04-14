#!/usr/bin/env python3
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator

from common import ALGORITHMS, args, serialized_args, ki_formatter
from logparser import *
from prelude import plt

fig, plots = plt.subplots(1, 2, figsize=(2.975, 0.8), tight_layout=True)
plt.tight_layout(pad=0, w_pad=0, h_pad=0)  # , rect=(0,0,.80,1))
plots[0].set_title("Total Compute", pad=0)
plots[0].set_ylabel('CPU time (s, sys.+user)', labelpad=1)
# plots[0].yaxis.set_major_locator(MultipleLocator(10))
# plots[0].yaxis.set_major_formatter(k_formatter)
plots[1].set_title("Average Memory", pad=0)
plots[1].set_ylabel('Memory (B)', labelpad=1)
plots[1].yaxis.set_major_formatter(ki_formatter)
plots[1].yaxis.set_minor_locator(MultipleLocator(2 * 1024 * 1024))
plots[1].yaxis.set_major_locator(MultipleLocator(4 * 1024 * 1024))
plots[1].set_ylim(6 * 1024 * 1024, 12 * 1024 * 1024)
for plot in plots:
    plot.set_xlabel('Number of Servers', labelpad=1)
    plot.grid(axis='y', which='major', linestyle='--', linewidth='0.5')
    plot.grid(axis='y', which='minor', linestyle=':', linewidth='0.25')
    plot.tick_params(axis='both', which='major', pad=0.5)
    plot.tick_params(axis='both', which='minor', pad=0.5)
    plot.xaxis.set_major_locator(MultipleLocator(4, 3))
    # plot.xaxis.set_minor_locator(MultipleLocator(3, 3))
    plot.set_xlim(3, 31)

for experiment in ALGORITHMS:
    xs = []
    ys_cpu = []
    ys_mem = []
    for num_replicas in range(3, 33, 2):
        config = args.config.replace('@', str(num_replicas))
        logs = parse(algo=experiment, config=config, writes=args.writes, requests=args.requests // num_replicas,
                     ingress=args.ingress, throughput=args.throughput, speedup=args.speedup, std='err')
        assert len(logs[
                       'time']) == num_replicas, f'Some replicas ({num_replicas - len(logs["time"])} out of {num_replicas}) did not report CPU+mem stats'
        cpu = sum(map(lambda log: log['user'] + log['system'], logs['time']))
        # cpu = compute_average(logs['time'], lambda log: log['user'] + log['system'])
        mem = compute_average(logs['time'], lambda log: log['memory'] * 1024)
        xs.append(num_replicas)
        ys_cpu.append(cpu)
        ys_mem.append(mem)
        print(experiment, num_replicas, ys_cpu[-1], ys_mem[-1])
    plots[0].plot(xs, ys_cpu, **ALGORITHMS[experiment], markevery=(1, 3))
    plots[1].plot(xs, ys_mem, **ALGORITHMS[experiment], markevery=(1, 3))

fig.subplots_adjust(wspace=0.4, hspace=0)

legends = [
    Line2D([0], [0], **algo)
    for algo in ALGORITHMS.values()
]

fig.legend(handles=legends, bbox_to_anchor=(0.1, 1.29, 0.75, 0.01),
           loc='center', edgecolor='black', borderaxespad=0, ncols=3,
           borderpad=.3, labelspacing=0.1, mode='expand',
           handlelength=0.8, handletextpad=0.5)

pdf_path = f'plots/7-cpu-mem{serialized_args}.pdf'
plt.savefig(pdf_path,
            format='pdf', bbox_inches='tight', pad_inches=0.01)
print(pdf_path)
