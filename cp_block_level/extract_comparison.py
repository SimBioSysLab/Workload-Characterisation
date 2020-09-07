import csv
import pytz, datetime
import json
import logging
import time
import numpy as np
import math
import random
from multiprocessing import Process, Manager
import glob
from loadconfig import config
from cp_block_level.utils import read_all_cpb_traces, extract_file_name, create_extraction_folders, \
    get_extraction_folder, create_multi_day_extraction, get_multi_extraction, get_all_extraction_files, \
    get_logging_string, get_all_day_split, verify_file_size
from scipy.spatial.distance import jaccard, cosine


def read_files(day, file_name, return_dict):
    curr_file = get_extraction_folder(file_name=file_name, day_num=day)
    print(curr_file)
    return_file = "day_{}".format(day)
    return_dict[return_file] = return_file
    folder_day = "folders_{}".format(day)
    return_dict[folder_day] = curr_file
    csv_file = open(curr_file, "r")
    csv_reader = csv.reader(csv_file)
    next(csv_reader)
    np_array = set()
    for row in csv_reader:
        # print(row[0])
        np_array.add(int(row[0]))
        # print("Values are: {}".format(np_array))

    np_day = "array_day_{}".format(day)
    np_array = tuple(np_array)
    # print("The day is {} and the array length is {} and the 10th element is {}".format(day, len(np_array),
    # np_array[9]))
    # np_array = np.array(np_array)
    # print("The array for day {} is {}".format(day, np_array))
    return_dict[np_day] = np_array


def compare_data(file_):
    logging.info("Processing file {} for comparing across days".format(file_))
    st_time = time.time()
    manager = Manager()
    return_dict = manager.dict()
    job_list = []
    for i in range(7):
        p = Process(target=read_files, args=(i+1, file_, return_dict))
        job_list.append(p)
        p.start()

    for proc in job_list:
        proc.join()

    jaccard_dict = dict()
    for i in range(7):
        outer_dict = "array_day_{}".format(i+1)
        for j in range(i+1, 7):
            inner_dict = "array_day_{}".format(j+1)
            jac_string = "day_{}_day_{}".format(i+1, j+1)
            # print("Finding jaccard of {} and {}".format(outer_dict, inner_dict))
            # print("Length of outer values is: {}".format(len(return_dict[outer_dict])))
            # print("Length of inner values is: {}".format(len(return_dict[inner_dict]), return_dict[outer_dict][9]))
            common_elements = (set(return_dict[inner_dict]) & set(return_dict[outer_dict]))
            union_elements = (set(return_dict[inner_dict] + return_dict[outer_dict]))
            # print("The length of common elements is {}".format(len(common_elements)))
            # print("The length of union elements is {}".format(len(union_elements)))
            # val = random.sample(common_elements, 1)[0]
            # if val in return_dict[outer_dict] and val in return_dict[inner_dict]:
            #     print("Val: {} present in both".format(val))

            if len(return_dict[outer_dict]) > 0 and len(return_dict[inner_dict]) > 0:
                jaccard_dict[jac_string] = len(common_elements) / len(union_elements)
            else:
                jaccard_dict[jac_string] = 0
            ti_el = time.time() - st_time
            logging.info("Finished processing file {}, days {}, value {}, time {}".format(file_, jac_string,
                                                                                          jaccard_dict[jac_string],
                                                                                          ti_el))
            del common_elements
            del union_elements

    del return_dict
    act_name = extract_file_name(file_path=file_)
    json_file = "{}/{}.json".format(config.config_["OP_AGGR"], act_name)
    with open(json_file, 'w') as outfile:
        json.dump(jaccard_dict, outfile)


def extract_comparison_meta():
    files = read_all_cpb_traces()
    create_extraction_folders()
    for file_ in files:
        compare_data(file_=file_)


def run_comparison():
    if config.config_["COMPARE_DAYS"]:
        extract_comparison_meta()
