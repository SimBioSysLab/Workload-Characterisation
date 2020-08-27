import csv
import pytz, datetime
import json
import logging
import time
import pandas as pd
import math
from multiprocessing import Pool

from loadconfig import config
from cp_block_level.utils import read_all_cpb_traces, extract_file_name, create_extraction_folders, \
    get_extraction_folder, create_multi_day_extraction, get_multi_extraction, get_all_extraction_files, \
    get_logging_string, get_all_day_split


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


def process_split_per_file(dataset_file):

    extraction_files_list, multi_extraction_file_list = get_all_extraction_files(file_name=dataset_file)
    file_writer = dict()
    actual_name = dataset_file.split("/")[-1]
    actual_name = actual_name.split(".")[0]
    time_zone = pytz.timezone('US/Eastern')
    curr_date = datetime.datetime.now(tz=time_zone)
    date_as_string = curr_date.strftime('%Y-%m-%d_%H-%M-%S')
    logging.basicConfig(filename="./{}/split/{}_{}.log".format(config.config_["LOGGING_FOLDER"], actual_name,
                                                               date_as_string), level=logging.INFO)
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
        if count % 1000000 == 0:
            e_t = time.time()
            e_t = e_t - st_time
            logging.info("Processing line {} of file {} in {} seconds".format(count, dataset_file, e_t))
            # print("Processing line {} of file {} in {} seconds".format(count, dataset_file, e_t))

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
            for i in range(5, 7):
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
    # print("Time taken to process {} is {}".format(dataset_file, time_))


def split_feature_files(all_files_list):
    logging.info("Starting the splitting of files! Extracting files!")
    create_extraction_folders()
    create_multi_day_extraction()
    file_list = ['01', '02', '04', '05', '08', '09', '10', '11', '15', '16', '17', '21', '22', '24', '26', '29', '31',
                 '36', '39', '41', '46', '47', '49', '52', '53', '55', '58', '73', '75', '78', '89', '102']
    appened_files_list = ["{}w{}.csv".format(config.config_["IP_PATH"], t) for t in file_list]
    print(appened_files_list)
    with Pool(2) as p:
        p.map(process_split_per_file, all_files_list)
    # for file_ in all_files_list:
    #     process_split_per_file(dataset_file=file_)


def extract_stats(file_list):
    for file_ in file_list:
        pass


def count_statistics_for_individual():
    create_extraction_folders()
    create_multi_day_extraction()
    for day in range(7):
        all_files_list = get_all_day_split(day=day+1)
        print(all_files_list)


def run_feature_engineering():
    logging.info("Input Path is: {}".format(config.config_["IP_PATH"]))
    logging.info("Output Path is: {}".format(config.config_["OP_PATH"]))
    logging.info("Aggregate Path is: {}".format(config.config_["OP_AGGR"]))
    all_trace_files = read_all_cpb_traces()
    if config.config_["SPLIT_PATH"]:
        split_feature_files(all_files_list=all_trace_files)
        # count_statistics_for_individual()
    if config.config_["COMPUTE_STAT"]:
        count_statistics_for_individual()
