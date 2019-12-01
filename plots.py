import logging

import pandas as pd
import matplotlib.pyplot as plt
import pygal

from utils import get_all_cleaned_files
from loadconfig import config


def interarrival_box_plot(in_ar_ti, plot_title):
    title = "Box plot for {}".format(plot_title)
    plot_title = plot_title + ".png"
    graph_path = "./{}/{}".format(config.config_["GRAPH_FOLDER"], plot_title)
    # box_plot = pygal.Box()
    # box_plot.title = title
    # box_plot.add(plot_title, in_ar_ti["InterarrivalTime"].tolist()[1:])
    # box_plot.render()

    figure = plt.figure(1, figsize=(10, 12))
    plt.boxplot(in_ar_ti["InterarrivalTime"].tolist()[1:])
    plt.savefig(graph_path)
    logging.info("Saved file {}".format(graph_path))


def run():
    all_files_list = get_all_cleaned_files()
    for file_ in all_files_list:
        logging.info("Starting box plot for {}".format(file_))
        file_name = file_.split("/")[-1]
        file_name = file_name.split(".")[0]
        data_frame = pd.read_csv(file_)
        print(max(data_frame["InterarrivalTime"].tolist()[1:]))
        print(min(data_frame["InterarrivalTime"].tolist()[1:]))

        interarrival_box_plot(in_ar_ti=data_frame, plot_title=file_name)


if __name__ == '__main__':
    run()
