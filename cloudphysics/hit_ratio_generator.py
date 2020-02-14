import logging
import json
import pandas as pd
from cloudphysics.utils import workload_unique_block_path, get_hit_ratio
from cloudphysics.read_vscsi import get_all_file_names
import bisect

algorithm_list = ["LRU", "FIFO", "LFU", "ARC", "MRU", "Optimal"]
count_list = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]


def get_unique_block():
    logging.info("Getting unique block counts")
    json_dataset = pd.read_json(workload_unique_block_path())
    json_dataset.set_index("filename", inplace=True)
    return json_dataset


def calculate_file_individual():
    files_names_list = get_all_file_names()
    unique_block_json = get_unique_block()
    file_algo_list = []
    files_names_list = ['w99_vscsi1']
    for file_ in files_names_list:
        for algo in algorithm_list:
            file_name = get_hit_ratio(file_, algo)
            if not file_name:
                continue
            unique_row = unique_block_json.loc[[file_]]
            unique_count = unique_row["block_count"][0]
            file_algo_list.append((file_name, unique_count))

    for algo_file_name, count in file_algo_list:
        unique_count_list = [x * count for x in count_list]
        curr_fd = open(algo_file_name)
        dataset = json.load(curr_fd)
        key_list = [int(x) for x in dataset.keys()]
        key_list = sorted(key_list)
        print(key_list)
        # print(algo_file_name, key_list)
        idx_value = 0
        count_value = unique_count_list[idx_value]
        # print(dataset)
        for row, keys in enumerate(key_list):
            # print(row, keys)
            if count_value > keys:
                continue

            if count_value < keys:
                print(idx_value, count_value, keys, dataset[str(keys)])
                idx_value = idx_value + 1
                if idx_value == 9:
                    break
                count_value = unique_count_list[idx_value]


def run_hr_gen():
    calculate_file_individual()
