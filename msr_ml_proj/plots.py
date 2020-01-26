import logging

import pandas as pd
import matplotlib.pyplot as plt

from msr_ml_proj.utils import get_all_cleaned_files
from loadconfig import config


def interarrival_hist_plot(in_ar_ti, plot_title):
    title = "Histogram for {}".format(plot_title)
    plot_title = plot_title + "hist.png"
    graph_path = "./{}/{}".format(config.config_["GRAPH_FOLDER"], plot_title)

    dataframe = pd.DataFrame(columns=["InterarrivalTime"])
    dataframe["InterarrivalTime"] = in_ar_ti["InterarrivalTime"]
    dataframe.dropna()

    max_ = max(dataframe["InterarrivalTime"].tolist()[1:])
    min_ = min(dataframe["InterarrivalTime"].tolist()[1:])
    logging.info("For file {}, Max value is {}, Min value is {}".format(plot_title, max_, min_))

    dataframe.plot.hist(alpha=0.5, bins=15, grid=True, legend=None, log=True)
    plt.xlabel("Interarrival Times [LOG SCALE]")
    plt.title(title)
    plt.savefig(graph_path)
    logging.info("Saved file {}".format(graph_path))


def size_hist_plot(size_dataset, plot_title):

    title = "Size Histogram for {}".format(plot_title)
    plot_title = plot_title + "sizehist.png"
    graph_path = "./{}/{}".format(config.config_["GRAPH_FOLDER"], plot_title)

    dataframe = pd.DataFrame(columns=["Size"])
    dataframe["Size"] = size_dataset["Size"]
    dataframe.dropna()

    # print(dataframe.head())
    max_ = max(dataframe["Size"].tolist()[1:])
    min_ = min(dataframe["Size"].tolist()[1:])
    logging.info("For file {}, Max value is {}, Min value is {}".format(plot_title, max_, min_))

    dataframe.plot.hist(alpha=0.5, bins=15, grid=True, legend=None, log=True)
    plt.xlabel("Size of Block [LOG SCALE]")
    plt.title(title)
    plt.savefig(graph_path)
    logging.info("Saved Size Histogram {}".format(graph_path))


def unique_block_access_histogram():
    title = "Unique Workload Access Histogram"


def run():
    all_files_list = get_all_cleaned_files()
    for file_ in all_files_list:
        logging.info("Starting box plot for {}".format(file_))
        file_name = file_.split("/")[-1]
        file_name = file_name.split(".")[0]
        data_frame = pd.read_csv(file_)

        size_hist_plot(size_dataset=data_frame, plot_title=file_name)


if __name__ == '__main__':
    run()
