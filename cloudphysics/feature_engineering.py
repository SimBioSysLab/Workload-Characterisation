import os
import csv
import json
import logging
import time
import pandas as pd

from loadconfig import config
from cloudphysics.utils import ret_all_csv_trace_files, ret_file_name_modified_file, ret_read_write_json_path, \
    return_rw_ia_json, return_block_rw_ia_json, ret_workload_metadata_path, ret_workload_rw_metadata_path, \
    ret_block_len_stats, ret_unique_block_count_path, ret_rw_unique_block_count, read_all_cp_trace_files, \
    ret_file_name_csv, get_hit_ratio_filename

from PyMimircache import Cachecow

temp_anything_list = []


def convert_to_opcodes(filename):
    logging.info("Starting opcode conversion of file {}".format(filename))
    i = 0
    dataset_file = open(filename, "r")
    file_details = csv.DictReader(dataset_file)

    file_keys = file_details.fieldnames

    if "BIN_OPCODE" not in file_keys:
        file_keys = file_keys + ["BIN_OPCODE"]
    else:
        logging.info("File {} already exists. No need to convert things!".format(filename))
        return

    new_file_name = ret_file_name_modified_file(filename=filename)
    writing_file_d = open(new_file_name, "w")
    file_writer = csv.DictWriter(writing_file_d, fieldnames=file_keys)
    file_writer.writeheader()

    read_opcodes = config.config_["READ_OPCODES"]
    i = 1

    for row in file_details:

        if i % 10000 == 0:
            logging.info("Finished {} lines of {}".format(i, filename))

        try:
            value = int(row["OP_CODE"])
            if value in read_opcodes:
                row["BIN_OPCODE"] = 0
            else:
                row["BIN_OPCODE"] = 1

        except ValueError as e:
            logging.info("The exception is {}, file is {} and value is {}".format(e, filename, row["OP_CODE"]))
            row["BIN_CODE"] = -1

        i += 1

        file_writer.writerow(row)

    if "generated" in filename:
        os.remove(filename)
        os.rename(new_file_name, filename)


def generate_inter_arrival_times(filename):
    logging.info("Starting inter arrival-request conversion of file {}".format(filename))
    dataset_file = open(filename, "r")
    file_details = csv.DictReader(dataset_file)

    file_keys = file_details.fieldnames

    if "INTERARRIVAL" not in file_keys:
        file_keys = file_keys + ["INTERARRIVAL"]
    else:
        logging.info("File {} already exists. No need to calculate the timings!".format(filename))
        return

    last_time_stamp = -1

    new_file_name = ret_file_name_modified_file(filename=filename)
    writing_file_d = open(new_file_name, "w")
    file_writer = csv.DictWriter(writing_file_d, fieldnames=file_keys)
    file_writer.writeheader()

    i = 1
    for row in file_details:
        if i % 10000 == 0:
            logging.info("Finished {} lines of {}".format(i, filename))

        if last_time_stamp == -1:
            row["INTERARRIVAL"] = -1
            last_time_stamp = float(row["TIME_STAMP"])
        else:
            row["INTERARRIVAL"] = float(row["TIME_STAMP"]) - last_time_stamp
            last_time_stamp = float(row["TIME_STAMP"])

        file_writer.writerow(row)
        i = i + 1

    if "generated" in filename:
        os.remove(filename)
        os.rename(new_file_name, filename)


def generate_rw_ia_times(filename):

    logging.info("Starting RW IA calculation for file {}".format(filename))

    logging.info("Starting count of read and write {}".format(filename))
    i = 1
    dataset_file = open(filename, "r")
    file_details = csv.DictReader(dataset_file)

    read_ia_list = []
    write_ia_list = []

    last_read_time = -1
    last_write_time = -1
    for row_ in file_details:
        if i % 10000 == 0:
            logging.info("Finished processing {} lines of file {}".format(i, filename))

        if int(row_["BIN_OPCODE"]) == 0:
            if last_read_time == -1:
                read_ia_list.append(row_["TIME_STAMP"])
                last_read_time = float(row_["TIME_STAMP"])
            else:
                read_ia_list.append(float(row_["TIME_STAMP"]) - last_read_time)
                last_read_time = float(row_["TIME_STAMP"])
        else:
            if last_write_time == -1:
                write_ia_list.append(row_["TIME_STAMP"])
                last_write_time = float(row_["TIME_STAMP"])
            else:
                write_ia_list.append(float(row_["TIME_STAMP"]) - last_write_time)
                last_write_time = float(row_["TIME_STAMP"])
        i = i + 1

    opfile_details, actual_name = return_rw_ia_json(filename)
    op_dict = {
        "file_name": actual_name,
        "read_list": read_ia_list,
        "write_list": write_ia_list
    }
    logging.info("Writing file {} to {}".format(actual_name, opfile_details))
    file_fd = open(opfile_details, "w")
    json.dump(op_dict, file_fd)


def number_of_read_and_writes(filename):

    logging.info("Starting count of read and write {}".format(filename))
    i = 0
    dataset_file = open(filename, "r")
    file_details = csv.DictReader(dataset_file)

    if "BIN_OPCODE" not in file_details.fieldnames:
        logging.info("File {} doesn't have Bin Opcode".format(filename))

    read_count = 0
    write_count = 0
    i = 1

    for row in file_details:
        if i % 10000 == 0:
            logging.info("Process {} lines of file {}".format(i, filename))

        if int(row["BIN_OPCODE"]) == 0:
            read_count = read_count + 1
        else:
            write_count = write_count + 1

        i = i + 1

    return read_count, write_count


def read_write_list(filename):

    write_to_file, json_value = ret_read_write_json_path(filename)
    json_fp = open(file=write_to_file, mode="w")
    read, write = number_of_read_and_writes(filename=filename)
    temp_dict = {
        "read_count": read,
        "write_count": write,
        "filename": json_value
    }

    temp_anything_list.append(temp_dict)
    json.dump(obj=temp_anything_list, fp=json_fp)
    return 1


def read_write_block_count(filename):

    logging.info("Start read write for each block {}".format(filename))

    i = 1
    dataset_file = open(filename, "r")
    file_details = csv.DictReader(dataset_file)

    output_dict = dict()
    for row in file_details:
        if i % 100000 == 0:
            logging.info("Finished processing {} lines of file {}".format(i, filename))

        if row["BLOCK_NUMBER"] not in output_dict.keys():
            if int(row["BIN_OPCODE"]) == 0:
                output_dict[row["BLOCK_NUMBER"]] = {
                    "read_list": [float(row["TIME_STAMP"])],
                    "write_list": []
                }
            else:
                output_dict[row["BLOCK_NUMBER"]] = {
                    "read_list": [],
                    "write_list": [float(row["TIME_STAMP"])]
                }
        else:
            if int(row["BIN_OPCODE"]) == 0:
                if output_dict[row["BLOCK_NUMBER"]]["read_list"]:
                    output_dict[row["BLOCK_NUMBER"]]["read_list"].append(float(row["TIME_STAMP"]))
                else:
                    output_dict[row["BLOCK_NUMBER"]]["read_list"].append(float(row["TIME_STAMP"]))
            else:
                if output_dict[row["BLOCK_NUMBER"]]["write_list"]:
                    output_dict[row["BLOCK_NUMBER"]]["write_list"].append(float(row["TIME_STAMP"]))
                else:
                    output_dict[row["BLOCK_NUMBER"]]["write_list"].append(float(row["TIME_STAMP"]))
        i = i + 1

    output_file_name, actual_name = return_block_rw_ia_json(filename=filename)
    op_file_fd = open(output_file_name, "w")
    json.dump(obj=output_dict, fp=op_file_fd)
    return 1


def workload_metadata(filename):
    logging.info("Starting workload calculations of file {}".format(filename))
    i = 1
    dataset = pd.read_csv(filename)
    new_df = pd.DataFrame(dataset["INTERARRIVAL"], columns=["INTERARRIVAL"])
    new_df = new_df[new_df.INTERARRIVAL != -1]
    print(new_df)
    del dataset
    range = new_df.max() - new_df.min()
    average = new_df.mean()
    median = new_df.median()

    return range[0], average[0], median[0]


def workload_metadata_list(filename):

    write_to_file, json_value = ret_workload_metadata_path(filename)
    json_fp = open(file=write_to_file, mode="w")
    range, average, median = workload_metadata(filename=filename)
    print(range, average, median)
    temp_dict = {
        "range": range,
        "average": average,
        "median": median,
        "filename": json_value
    }

    temp_anything_list.append(temp_dict)
    logging.info("Writing info {} to json file".format(json_value))
    json.dump(obj=temp_anything_list, fp=json_fp)
    return 1


def workload_metadata_read_write(filename):
    logging.info("Starting metadata calc of file {}".format(filename))
    dataset = pd.read_csv(filename)
    read_mask = (dataset['BIN_OPCODE'] == 0) & (dataset["INTERARRIVAL"] != -1)
    write_mask = (dataset['BIN_OPCODE'] == 1) & (dataset["INTERARRIVAL"] != -1)
    read_df = dataset[read_mask]["INTERARRIVAL"]
    write_df = dataset[write_mask]["INTERARRIVAL"]
    read_range = read_df.max() - read_df.min()
    read_avg = read_df.mean()
    read_median = read_df.median()
    read_tuple = (read_range, read_avg, read_median)

    write_range = write_df.max() - write_df.min()
    write_avg = write_df.mean()
    write_median = write_df.median()
    write_tuple = (write_range, write_avg, write_median)

    return read_tuple, write_tuple


def workload_metadata_rw_list(filename):

    write_to_file, json_value = ret_workload_rw_metadata_path(filename)
    json_fp = open(file=write_to_file, mode="w")
    read_tuple, write_tuple = workload_metadata_read_write(filename=filename)

    temp_dict = {
        "read_tuple": read_tuple,
        "write_tuple": write_tuple,
        "filename": json_value
    }

    temp_anything_list.append(temp_dict)
    logging.info("Writing info {} to json file".format(json_value))
    json.dump(obj=temp_anything_list, fp=json_fp)
    return 1


def block_length_stats(filename):
    logging.info("Starting block length of file {}".format(filename))
    dataset = pd.read_csv(filename)
    new_df = pd.DataFrame(dataset["LEN"], columns=["LEN"])
    new_df = new_df["LEN"] / 512
    del dataset
    range_ = new_df.max() - new_df.min()
    average = new_df.mean()
    median = new_df.median()

    return range_, average, median


def block_length_list(filename):

    write_to_file, json_value = ret_block_len_stats(filename)
    json_fp = open(file=write_to_file, mode="w")
    range_, average, median = block_length_stats(filename=filename)

    temp_dict = {
        "range": range_,
        "average": average,
        "median": median,
        "filename": json_value
    }

    temp_anything_list.append(temp_dict)
    logging.info("Writing info {} to json file".format(json_value))
    json.dump(obj=temp_anything_list, fp=json_fp)
    return 1


def get_unique_block_count(filename):
    logging.info("Starting unique block count of file {}".format(filename))
    dataset = pd.read_csv(filename)
    new_df = pd.DataFrame(dataset["BLOCK_NUMBER"], columns=["BLOCK_NUMBER"])
    del dataset
    unique_block_count = new_df.BLOCK_NUMBER.nunique()
    length = len(new_df.index)
    del new_df
    return unique_block_count, length


def unique_block_length_list(filename):
    write_to_file, json_value = ret_unique_block_count_path(filename=filename)
    json_fp = open(file=write_to_file, mode='w')
    block_count, length = get_unique_block_count(filename=filename)
    temp_dict = {
        "block_count": block_count,
        "filename": json_value,
        "total_block_count": length
    }
    temp_anything_list.append(temp_dict)
    logging.info("Writing info {} to json file".format(filename))
    json.dump(obj=temp_anything_list, fp=json_fp)
    return 1


def unique_read_write_block_count(filename):
    logging.info("Starting read write unique block count of {}".format(filename))
    dataset = pd.read_csv(filename)
    read_mask = (dataset['BIN_OPCODE'] == 0)
    write_mask = (dataset['BIN_OPCODE'] == 1)
    read_df = dataset[read_mask]['BLOCK_NUMBER']
    write_df = dataset[write_mask]["BLOCK_NUMBER"]
    del dataset

    read_count = read_df.nunique()
    read_total = len(read_df.index)
    write_count = write_df.nunique()
    write_total = len(write_df.index)

    del read_df
    del write_df

    return read_count, read_total, write_count, write_total


def unique_read_write_block_list(filename):
    write_to_file, json_value = ret_rw_unique_block_count(filename=filename)
    json_fp = open(file=write_to_file, mode='w')
    read_block_count, read_total_block, write_block_count, write_total_block = \
        unique_read_write_block_count(filename=filename)
    temp_dict = {
        "read_unique_count": read_block_count,
        "read_total_count": read_total_block,
        "write_unqiue_count": write_block_count,
        "write_total_count": write_total_block
    }
    temp_anything_list.append(temp_dict)
    json.dump(obj=temp_anything_list, fp=json_fp)
    return 1


def get_hrc_for_file(file_name, algorithm_name):

    logging.info("Working on file: {} and algorithm: {}".format(file_name, algorithm_name))
    actual_name = file_name.split("/")[-1]
    actual_name = actual_name.split(".")[0]
    cache_size = 500000
    c = Cachecow()
    if "vscsi1" in file_name:
        trace_type = 1
    else:
        trace_type = 2

    c.vscsi(file_name, vscsi_type=trace_type)
    dict_value = c.get_hit_ratio_dict(algorithm_name, cache_size=cache_size, cache_params=None, bin_size=-1)
    json_file_name = get_hit_ratio_filename(actual_name, algorithm_name)
    json_fp = open(json_file_name, "w")
    json.dump(obj=dict_value, fp=json_fp)


def get_all_cp_trace_files():

    c = Cachecow()
    all_files_list = read_all_cp_trace_files()
    algorithm_list = ["LRU", "LFU", "FIFO", "MRU"]
    for file_ in all_files_list:
        for algo in algorithm_list:
            start_time = time.time()
            get_hrc_for_file(file_name=file_, algorithm_name=algo)
            end_time = time.time()
            k = end_time - start_time
            logging.info("Finished File: {} and Algo: {} in {}".format(file_, algo, k))

def run_feature_engineering():
    # all_files_list = ret_all_csv_trace_files()
    get_all_cp_trace_files()
    # for file_ in all_files_list:
    #     start_time = time.time()
    #     # convert_to_opcodes(filename=file_)
    #     # read_write_list(filename=file_)
    #     # generate_inter_arrival_times(filename=file_)
    #     # generate_rw_ia_times(filename=file_)
    #     # read_write_block_count(filename=file_)
    #     # workload_metadata_list(filename=file_)
    #     # workload_metadata_rw_list(filename=file_)
    #     # block_length_list(filename=file_)
    #     # unique_block_length_list(filename=file_)
    #     unique_read_write_block_list(filename=file_)
    #     end_time = time.time()
    #     k = end_time - start_time
    #     logging.info("The time taken to execute {} is {}".format(file_, k))
