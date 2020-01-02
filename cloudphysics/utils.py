import glob
from loadconfig import config


def read_all_cp_trace_files():
    all_files_list = glob.glob("./{}/cloudphysics/*.vscsitrace".format(config.config_["DATASET_FOLDER"]))
    return all_files_list


def ret_file_name_csv(filename):
    actual_name = filename.split("/")[-1]
    actual_name = actual_name.split(".")[0]
    actual_name = "{}.csv".format(actual_name)

    file_location = "./{}/cloudphysics/csv/{}".format(config.config_["DATASET_FOLDER"], actual_name)
    return file_location
