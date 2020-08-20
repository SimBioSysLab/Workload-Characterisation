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


def create_extraction_folders():
    base_1 = "{}1_day".format(config.config_["OP_PATH"])
    base_2 = "{}2_day".format(config.config_["OP_PATH"])
    base_3 = "{}3_day".format(config.config_["OP_PATH"])
    base_4 = "{}4_day".format(config.config_["OP_PATH"])
    base_5 = "{}5_day".format(config.config_["OP_PATH"])
    base_6 = "{}6_day".format(config.config_["OP_PATH"])
    base_7 = "{}7_day".format(config.config_["OP_PATH"])

    if not os.path.exists(base_1):
        os.mkdir(base_1)

    if not os.path.exists(base_2):
        os.mkdir(base_2)

    if not os.path.exists(base_3):
        os.mkdir(base_3)

    if not os.path.exists(base_4):
        os.mkdir(base_4)

    if not os.path.exists(base_5):
        os.mkdir(base_5)

    if not os.path.exists(base_6):
        os.mkdir(base_6)

    if not os.path.exists(base_7):
        os.mkdir(base_7)
