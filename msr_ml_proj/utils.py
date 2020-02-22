import glob
import datetime
from loadconfig import config


def get_cleaned_files_path(file_name):
    df_file_name = file_name.split('/')[3]
    df_file_name = "df{}".format(df_file_name)
    constructed_filename = "./{}/msr_traces/cleaned_dataset/{}".format(config.config_["DATASET_FOLDER"], df_file_name)
    return constructed_filename


def get_all_cleaned_files():
    all_files_list = glob.glob("./{}/msr_traces/cleaned_dataset/*.csv".format(config.config_["DATASET_FOLDER"]))
    return all_files_list


def get_writing_file_name():
    file_name = "./{}/all_result.csv".format(config.config_["DATASET_FOLDER"])
    return file_name


def get_all_uncleaned_files():

    all_files_list = glob.glob("./{}/msr_traces/*.csv".format(config.config_["DATASET_FOLDER"]))
    return all_files_list
