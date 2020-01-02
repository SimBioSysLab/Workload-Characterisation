import glob
from loadconfig import config


def read_all_cp_trace_files():
    all_files_list = glob.glob("./{}/cloudphysics/*.vscsitrace".format(config.config_["DATASET_FOLDER"]))
    return all_files_list
