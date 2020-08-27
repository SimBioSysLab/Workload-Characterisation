import os
import glob
import pytz
import datetime
from loadconfig import config


def read_all_cpb_traces():
    all_files_list = glob.glob("{}/*.csv".format(config.config_["IP_PATH"]))
    return all_files_list


def extract_file_name(file_path):
    actual_name = file_path.split("/")[-1]
    actual_name = actual_name.split(".")[0]

    return actual_name


def create_extraction_folders():
    config.config_["base_1"] = "{}1_day".format(config.config_["OP_PATH"])
    config.config_["base_2"] = "{}2_day".format(config.config_["OP_PATH"])
    config.config_["base_3"] = "{}3_day".format(config.config_["OP_PATH"])
    config.config_["base_4"] = "{}4_day".format(config.config_["OP_PATH"])
    config.config_["base_5"] = "{}5_day".format(config.config_["OP_PATH"])
    config.config_["base_6"] = "{}6_day".format(config.config_["OP_PATH"])
    config.config_["base_7"] = "{}7_day".format(config.config_["OP_PATH"])

    if not os.path.exists(config.config_["base_1"]):
        os.mkdir(config.config_["base_1"])

    if not os.path.exists(config.config_["base_2"]):
        os.mkdir(config.config_["base_2"])

    if not os.path.exists(config.config_["base_3"]):
        os.mkdir(config.config_["base_3"])

    if not os.path.exists(config.config_["base_4"]):
        os.mkdir(config.config_["base_4"])

    if not os.path.exists(config.config_["base_5"]):
        os.mkdir(config.config_["base_5"])

    if not os.path.exists(config.config_["base_6"]):
        os.mkdir(config.config_["base_6"])

    if not os.path.exists(config.config_["base_7"]):
        os.mkdir(config.config_["base_7"])


def create_multi_day_extraction():
    config.config_["base_12"] = "{}days_12".format(config.config_["OP_AGGR"])
    config.config_["base_123"] = "{}days_123".format(config.config_["OP_AGGR"])
    config.config_["base_1234"] = "{}days_1234".format(config.config_["OP_AGGR"])
    config.config_["base_12345"] = "{}days_12345".format(config.config_["OP_AGGR"])
    config.config_["base_123456"] = "{}days_123456".format(config.config_["OP_AGGR"])

    if not os.path.exists(config.config_["base_12"]):
        os.mkdir(config.config_["base_12"])

    if not os.path.exists(config.config_["base_123"]):
        os.mkdir(config.config_["base_123"])

    if not os.path.exists(config.config_["base_1234"]):
        os.mkdir(config.config_["base_1234"])

    if not os.path.exists(config.config_["base_12345"]):
        os.mkdir(config.config_["base_12345"])

    if not os.path.exists(config.config_["base_123456"]):
        os.mkdir(config.config_["base_123456"])


def get_all_extraction_files(file_name):
    file_names_list = []
    multi_file_names = []

    for i in range(7):
        file_names_list.append(get_extraction_folder(file_name=file_name, day_num=i+1))

    base = "1"
    for i in range(2, 7):
        base = "{}{}".format(base, i)
        multi_file_names.append(get_multi_extraction(file_name=file_name, day_nums=base))

    return file_names_list, multi_file_names


def get_extraction_folder(file_name, day_num):
    actual_name = file_name.split("/")[-1]
    actual_name = actual_name.split(".")[0]
    base = "base_{}".format(day_num)
    base_n = "{}/{}.csv".format(config.config_[base], actual_name)
    return base_n


def get_multi_extraction(file_name, day_nums):
    actual_name = file_name.split("/")[-1]
    actual_name = actual_name.split(".")[0]
    base = "base_{}".format(day_nums)
    base_n = "{}/{}.csv".format(config.config_[base], actual_name)
    return base_n


def get_logging_string():
    time_zone = pytz.timezone('US/Eastern')
    curr_date = datetime.datetime.now(tz=time_zone)
    date_as_string = curr_date.strftime('%Y-%m-%d_%H-%M-%S')
    filename = "./{}/{}.log".format(config.config_["LOGGING_FOLDER"], date_as_string)
    return filename


def get_all_day_split(day):
    base_n = "base_{}".format(day)
    all_day_files = glob.glob("{}/*.csv".format(config.config_[base_n]))
    return all_day_files


def get_split_files():
    pass