import logging

import pandas as pd
import matplotlib.pyplot as plt

from utils import get_all_cleaned_files
from loadconfig import config


def interarrival_hist_plot(in_ar_ti, plot_title):
    title = "Histogram for {}".format(plot_title)
    plot_title = plot_title + "hist.png"
    graph_path = "./{}/{}".format(config.config_["GRAPH_FOLDER"], plot_title)

    dataframe = pd.DataFrame(columns=["InterarrivalTime"])
    dataframe["InterarrivalTime"] = in_ar_ti["InterarrivalTime"]
    dataframe.dropna()
    dataframe.plot.hist(alpha=0.5, bins=15, grid=True, legend=None, log=True)
    plt.xlabel("Interarrival Times")
    plt.title(title)
    plt.savefig(graph_path)
    logging.info("Saved file {}".format(graph_path))


def run():
    all_files_list = get_all_cleaned_files()
    for file_ in all_files_list:
        logging.info("Starting box plot for {}".format(file_))
        file_name = file_.split("/")[-1]
        file_name = file_name.split(".")[0]
        data_frame = pd.read_csv(file_)
        max_ = max(data_frame["InterarrivalTime"].tolist()[1:])
        min_ = min(data_frame["InterarrivalTime"].tolist()[1:])
        logging.info("For file {}, Max value is {}, Min value is {}".format(file_name, max_, min_))
        interarrival_hist_plot(in_ar_ti=data_frame, plot_title=file_name)


if __name__ == '__main__':
    run()
