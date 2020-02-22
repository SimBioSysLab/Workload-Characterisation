import matplotlib

matplotlib.use('TkAgg')
import json
import numpy as np
import logging
import time
import csv
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
    font = {'family': 'normal',
            'size': 20}
    plt.rc('font', **font)
    plt.rc('xtick', labelsize=20)
    plt.rc('ytick', labelsize=20)
    plt.title(label_text)
    plt.yscale("log")
    plt.hist(list_of_iat)
    plt.savefig(opfilename, format="png")
    plt.show()


def generate_unique_block_count_histogram():
    logging.info("Starting unique block count of file")
    json_fd = open(workload_unique_block_path(), "r")
    dataset = json.load(json_fd)

    sorted_list = sorted(dataset, key=lambda k: k["block_count"], reverse=True)
    block_count_list = []
    files_names = []
    for values in sorted_list:
        block_count_list.append(values["block_count"] / values["total_block_count"])
        files_names.append(values['filename'])

    block_count_list = sorted(block_count_list)
    bars = np.arange(len(files_names))

    label_text = "Block count LOG SCALE"
    plt.figure(figsize=(15, 10))
    plt.title(label_text)
    plt.xticks(bars, files_names, rotation=90)
    plt.yscale("log")
    plt.bar(bars, block_count_list)
    plt.show()


def run_generate_graphs():
    # generate_read_write_graph()
    # generate_unique_block_count_histogram()
    all_files_list = ret_all_csv_trace_files()
    for file_ in all_files_list:
        st_time = time.time()
        generate_iatime_histogram(filename=file_)
        end_time = time.time()
        time_ = end_time - st_time
        logging.info("Time taken for completing {} is {}".format(time, time_))