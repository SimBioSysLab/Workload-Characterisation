import os
import csv
import json
import logging
import time
import pandas as pd
import arrow
import threading

from loadconfig import config
from cp_block_level.utils import read_all_cpb_traces, extract_file_name, create_extraction_folders, \
    get_extraction_folder


def read_write_count(file_path):
    logging.info("Processing file: {}".format(file_path))
    dataset = pd.read_csv(file_path, names=config.config_["HEADERS"])
    print(dataset.head(10))
    read_dataset = dataset[dataset.READ_WRITE == "r"]
    write_dataset = dataset[dataset.READ_WRITE == "w"]
    read_count = len(read_dataset.index)
    write_count = len(write_dataset.index)

    read_unique_count = read_dataset.BLOCK_NUMBER.nunique()
    write_unique_count = write_dataset.BLOCK_NUMBER.nunique()
    total_unique_count = dataset.BLOCK_NUMBER.nunique()

    del read_dataset
    del write_dataset
    del dataset

    return read_count, write_count, read_unique_count, write_unique_count, total_unique_count


def read_write_meta(all_files_list):
    all_read_write_dict = []
    for file_ in all_files_list:
        st_time = time.time()
        read_count, write_count, read_unique, write_unique, total_count = read_write_count(file_path=file_)
        file_name = extract_file_name(file_)
        temp_dict = {
            "file_name": file_name,
            "read_count": read_count,
            "write_count": write_count,
            "read_unique": read_unique,
            "write_unique": write_unique,
            "total_unique": total_count
        }
        all_read_write_dict.append(temp_dict)
        end_time = time.time()
        time_ = end_time - st_time
        logging.info("Finished processing file: {} in {} seconds".format(file_, time_))
    with open(config.config_["OP_PATH"], 'w') as outfile:
        json.dump(all_read_write_dict, outfile)
    logging.info("Finished writing to json file")


def split_files_into_days(dataset_file):
    data_f = open(dataset_file)
    dataset = csv.DictReader(data_f, fieldnames=config.config_["HEADERS"])

    all_days_list = list()
    curr_time_final = None
    temp_list_for_days = list()
    i = 1
    for row in dataset:
        if not curr_time_final:
            curr_time_final = int(row['ACCESS_TIME'])

        if int(row["ACCESS_TIME"]) - curr_time_final <= config.config_["DAY_MS"]:
            temp_list_for_days.append(row)
        else:
            logging.info("Finished extracting day {} from {}".format(i, dataset_file))
            i += 1
            all_days_list.append(temp_list_for_days)
            temp_list_for_days = list()
            temp_list_for_days.append(row)
            curr_time_final = int(row['ACCESS_TIME'])

    all_days_list.append(temp_list_for_days)
    return all_days_list


def thread_level_for_writing(obj, file_, day):
    file_name = get_extraction_folder(file_, day)
    write_to_files = open(file_name, "w")
    writer = csv.DictWriter(write_to_files, fieldnames=config.config_["HEADERS"])
    writer.writeheader()
    writer.writerows(obj)
    logging.info("Finished writing to file: {}".format(file_name))


def split_feature_files(all_files_list):
    create_extraction_folders()
    for file_ in all_files_list:
        st_time = time.time()
        split_lists = split_files_into_days(dataset_file=file_)
        for idx, day in enumerate(split_lists):
            threads = threading.Thread(target=thread_level_for_writing, args=(day, file_, idx+1))
            threads.start()
        end_time = time.time()
        time_ = end_time - st_time
        logging.info("Finished splitting file: {} in {} seconds".format(file_, time_))


def run_feature_engineering():
    all_trace_files = read_all_cpb_traces()
    # read_write_meta(all_files_list=all_trace_files)
    split_feature_files(all_files_list=all_trace_files)
