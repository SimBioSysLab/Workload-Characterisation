import os
import glob
from loadconfig import config


def read_all_cpb_traces():
    print(config.config_["IP_PATH"])
    all_files_list = glob.glob("{}/*.csv".format(config.config_["IP_PATH"]))
    return all_files_list


def extract_file_name(file_path):
    actual_name = file_path.split("/")[-1]
    actual_name = actual_name.split(".")[0]

    return actual_name
