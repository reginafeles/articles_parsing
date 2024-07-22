"""
Visualizer module for visualizing PosFrequencyPipeline results
"""

from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt


def visualize(statistics: dict, path_to_save: Path):
    """
    param: statistics is a dictionary with keys:POS tags, values:frequencies
    """
    number_of_tags = len(statistics)
    sorted_frequencies = sorted(statistics.values(), reverse=True)
    sorted_tags = sorted(statistics, key=statistics.get, reverse=True)

    pos_tags = np.arange(number_of_tags)
    colors = ('b', 'g', 'r', 'c')

    figure = plt.figure()
    axis = figure.add_subplot(1, 1, 1)
    for i in range(0, number_of_tags):
        axis.bar(pos_tags[i], sorted_frequencies[i],
                 align='center', width=0.5,
                 color=colors[i % len(colors)])

    axis.set_xticks(pos_tags)
    axis.set_xticklabels(sorted_tags)
    plt.setp(sorted_tags)
    plt.xticks(rotation=20)
    y_max = max(sorted_frequencies) + 1
    plt.ylim(0, y_max)

    plt.savefig(path_to_save)


if __name__ == "__main__":
    pass
