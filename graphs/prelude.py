from math import sqrt
import matplotlib as mpl
import matplotlib.pyplot as plt
from os.path import expanduser

mpl.rcParams["font.family"] = "Linux Libertine O"
mpl.rcParams["text.usetex"] = False
mpl.rcParams["hatch.linewidth"] = 0.6

EXTRA_SMALL_SIZE = 6
SMALL_SIZE = 8
MEDIUM_SIZE = 9
NORMAL_SIZE = 10
BIGGER_SIZE = 12

plt.rc("font", size=MEDIUM_SIZE)  # controls default text sizes
plt.rc("legend", fontsize=SMALL_SIZE)  # legend fontsize
plt.rc(
    "figure", titlesize=MEDIUM_SIZE
)  # Figure title (used when having multiple subplots)
plt.rc("axes", titlesize=MEDIUM_SIZE)  # Subplot title
plt.rc("axes", labelsize=MEDIUM_SIZE)  # fontsize of the x and y labels
plt.rc("xtick", labelsize=SMALL_SIZE)  # fontsize of the tick labels
plt.rc("ytick", labelsize=SMALL_SIZE)  # fontsize of the tick labels


def lighten_color(color):
    """
    Lightens the given color by multiplying (1-luminosity) by the given amount.
    Input can be matplotlib color string, hex string, or RGB tuple.

    Examples:
    >> lighten_color('g', 0.3)
    >> lighten_color('#F034A3', 0.6)
    >> lighten_color((.3,.55,.1), 0.5)
    """
    import colorsys

    try:
        c = mpl.colors.cnames[color]
    except:
        c = color
    c = colorsys.rgb_to_hls(*mpl.colors.to_rgb(c))
    return colorsys.hls_to_rgb(c[0], (1 - (1 - c[1] * 0.58) * 0.66), c[2] * 0.75)
