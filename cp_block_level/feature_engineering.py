import csv
import json
import logging
import time
import pandas as pd
import math
import threading
from multiprocessing import Pool

from loadconfig import config
from cp_block_level.utils import read_all_cpb_traces, extract_file_name, create_extraction_folders, \
    get_extraction_folder, create_multi_day_extraction, get_multi_extraction, get_all_extraction_files


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


def thread_split_writing_per_file(dataset_file, day):
    print("Starting file: {} for day: {}".format(dataset_file, day))
    data_f = open(dataset_file, "r")
    dataset = csv.DictReader(data_f, fieldnames=config.config_["HEADERS"])
    curr_time_final = None
    second_file = None
    second_writer = None
    st_time = time.time()
    if day == 1 or day == 7:
        first_file = get_extraction_folder(file_name=dataset_file, day_num=day)
        second_file = None
    else:
        first_file = get_extraction_folder(file_name=dataset_file, day_num=day)
        base = 1
        for i in range(2, day+1):
            base = "{}{}".format(base, i)
            second_file = get_multi_extraction(file_name=dataset_file, day_nums=base)

    first_file = open(first_file, "w")
    first_writer = csv.DictWriter(first_file, fieldnames=config.config_["HEADERS"])
    first_writer.writeheader()

    if second_file:
        second_file = open(second_file, "w")
        second_writer = csv.DictWriter(second_file, fieldnames=config.config_["HEADERS"])
        second_writer.writeheader()

    for row in dataset:
        if not curr_time_final:
            curr_time_final = int(row["ACCESS_TIME"])

        time_diff = int(row["ACCESS_TIME"]) - curr_time_final
        if day == 1:
            if time_diff <= config.config_["DAY_MS"]:
                first_writer.writerow(row)

        if day == 2:
            if config.config_["DAY_MS"] < time_diff <= 2 * config.config_["DAY_MS"]:
                first_writer.writerow(row)
            if time_diff <= 2 * config.config_["DAY_MS"]:
                second_writer.writerow(row)

        if day == 3:
            if 2 * config.config_["DAY_MS"] < time_diff <= 3 * config.config_["DAY_MS"]:
                first_writer.writerow(row)
            if time_diff <= 3 * config.config_["DAY_MS"]:
                second_writer.writerow(row)

        if day == 4:
            if 3 * config.config_["DAY_MS"] < time_diff <= 4 * config.config_["DAY_MS"]:
                first_writer.writerow(row)
            if time_diff <= 4 * config.config_["DAY_MS"]:
                second_writer.writerow(row)

        if day == 5:
            if 4 * config.config_["DAY_MS"] < time_diff <= 5 * config.config_["DAY_MS"]:
                first_writer.writerow(row)
            if time_diff <= 5 * config.config_["DAY_MS"]:
                second_writer.writerow(row)

        if day == 6:
            if 5 * config.config_["DAY_MS"] < time_diff <= 6 * config.config_["DAY_MS"]:
                first_writer.writerow(row)
            if time_diff <= 6 * config.config_["DAY_MS"]:
                second_writer.writerow(row)

        if day == 7:
            if 6 * config.config_["DAY_MS"] < time_diff <= 7 * config.config_["DAY_MS"]:
                first_writer.writerow(row)

    end_time = time.time()
    time_ = end_time - st_time
    print("Finished running file {} for day {} in time {} seconds".format(dataset_file, day, time_))


def process_split_per_file(dataset_file):

    extraction_files_list, multi_extraction_file_list = get_all_extraction_files(file_name=dataset_file)
    file_writer = dict()
    for idx, file_ in enumerate(extraction_files_list):
        open_file = open(file_, "w")
        csv_writer = csv.DictWriter(open_file, fieldnames=config.config_["HEADERS"])
        csv_writer.writeheader()
        base = "base_{}".format(idx+1)
        file_writer[base] = csv_writer

    base = "base_1"
    for idx, file_ in enumerate(multi_extraction_file_list):
        open_file = open(file_, "w")
        csv_writer = csv.DictWriter(open_file, fieldnames=config.config_["HEADERS"])
        csv_writer.writeheader()
        base = "{}{}".format(base, idx+2)
        file_writer[base] = csv_writer

    data_file = open(dataset_file, "r")
    dataset = csv.DictReader(data_file, fieldnames=config.config_["HEADERS"])
    current_time = None
    count = -1
    st_time = time.time()
    for row in dataset:
        count += 1
        if count % 100000 == 0:
            logging.info("Processing line {} of file {}".format(count, dataset_file))

        if not current_time:
            current_time = int(row["ACCESS_TIME"])

        time_diff = int(row["ACCESS_TIME"]) - current_time
        k = math.ceil(time_diff / config.config_["DAY_MS"])

        if k == 1 or k == 0:
            file_writer["base_1"].writerow(row)
            base = "base_1"
            for i in range(2, 7):
                base = "{}{}".format(base, i)
                file_writer[base].writerow(row)

        if k == 2:
            file_writer["base_2"].writerow(row)
            base = "base_1"
            for i in range(2, 7):
                base = "{}{}".format(base, i)
                file_writer[base].writerow(row)

        if k == 3:
            file_writer["base_3"].writerow(row)
            base = "base_12"
            for i in range(3, 7):
                base = "{}{}".format(base, i)
                file_writer[base].writerow(row)

        if k == 4:
            file_writer["base_4"].writerow(row)
            base = "base_123"
            for i in range(4, 7):
                base = "{}{}".format(base, i)
                file_writer[base].writerow(row)

        if k == 5:
            file_writer["base_5"].writerow(row)
            base = "base_1234"
            for i in range(2, 7):
                base = "{}{}".format(base, i)
                file_writer[base].writerow(row)

        if k == 6:
            file_writer["base_6"].writerow(row)
            base = "base_12345"
            for i in range(6, 7):
                base = "{}{}".format(base, i)
                file_writer[base].writerow(row)

        if k == 7:
            file_writer["base_7"].writerow(row)

    end_time = time.time()
    time_ = end_time - st_time
    logging.info("Time taken to process {} is {}".format(dataset_file, time_))


def split_feature_files(all_files_list):
    logging.info("Starting the splitting of files! Extracting files!")
    create_extraction_folders()
    create_multi_day_extraction()

    with Pool(4) as p:
        p.map(process_split_per_file, all_files_list)


def run_feature_engineering():
    logging.info("Input Path is: {}".format(config.config_["IP_PATH"]))
    logging.info("Output Path is: {}".format(config.config_["OP_PATH"]))
    logging.info("Aggregate Path is: {}".format(config.config_["OP_AGGR"]))
    all_trace_files = read_all_cpb_traces()
    if config.config_["SPLIT_PATH"]:
        split_feature_files(all_files_list=all_trace_files)
