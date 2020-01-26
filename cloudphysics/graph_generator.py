import matplotlib

matplotlib.use('TkAgg')
import json
import logging
import time
import csv
import plotly.express as px
import plotly.graph_objects as go
import numpy

import matplotlib.pyplot as plt
from cloudphysics.utils import ret_server_result_json, return_iat_graph_path, ret_all_csv_trace_files, \
    workload_unique_block_path


def generate_read_write_graph():
    file_path = ret_server_result_json()
    json_fd = open(file_path, 'r')
    dataset = json.load(json_fd)
    file_names_list = [x["filename"] for x in dataset]
    rw_ratio_list = [x["read_count"] / x["write_count"] for x in dataset]
    rw_ratio_list = sorted(rw_ratio_list)
    # print(file_names_list)
    # print(rw_ratio_list)
    range_ = numpy.arange(len(file_names_list))
    print(rw_ratio_list)

    plt.bar(range_, rw_ratio_list, log=True)
    plt.xticks(range_, file_names_list, rotation=90)
    plt.show()


def generate_iatime_histogram(filename):
    logging.info("Starting processing of file {}".format(filename))
    input_fd = open(filename, "r")
    dataset = csv.DictReader(input_fd)
    list_of_iat = []
    opfilename, actual_name = return_iat_graph_path(filename=filename)
    i = 1
    for row in dataset:
        if i % 100000 == 0:
            logging.info("Finished adding {} records into list".format(i))
        list_of_iat.append(float(row["INTERARRIVAL"]))
        i = i + 1

    label_text = "Interarrival time of {} [LOG SCALE]".format(actual_name)
    plt.figure(figsize=(15, 10))
    plt.title(label_text)
    plt.yscale("log")
    plt.hist(list_of_iat)
    plt.savefig(opfilename, format="eps", dpi=10000)
    plt.show()


def generate_unique_block_count_histogram():
    logging.info("Starting unique block count of file")
    json_fd = open(workload_unique_block_path(), "r")
    dataset = json.load(json_fd)
    print(dataset)


def run_generate_graphs():
    # generate_read_write_graph()
    generate_unique_block_count_histogram()
    # all_files_list = ret_all_csv_trace_files()
    # for file_ in all_files_list:
    #     st_time = time.time()
    #     generate_iatime_histogram(filename=file_)
    #     end_time = time.time()
    #     time_ = end_time - st_time
    #     logging.info("Time taken for completing {} is {}".format(time, time_))