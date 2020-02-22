# LIST OF IN BUILT LIBRARIES
import csv
import logging
import time
from datetime import datetime

# LIST OF INSTALLED LIBRARIES
import pandas as pd

# PROJECT LIBRARIES
from msr_ml_proj.utils import get_all_uncleaned_files, get_cleaned_files_path
from loadconfig import config


def convert_filetime_to_dt(filetime):
    if type(filetime) == str:
        filetime = float(filetime)
    return datetime.utcfromtimestamp((filetime - config.config_["EPOCH_AS_FILETIME"]) /
                                     config.config_["HUNDREDS_OF_SECONDS"])


def convert_to_df(filename):
    file_descriptor = open(filename, "r")
    file_reader = csv.reader(file_descriptor, delimiter=",")
    all_values = list(file_reader)

    for value in all_values:
        value[0] = convert_filetime_to_dt(float(value[0]))

    file_path = get_cleaned_files_path(file_name=filename)
    print(file_path)
    dataset = pd.DataFrame(all_values, columns=config.config_["HEADER_LIST"])
    dataset.to_csv(file_path, index=False)


def run():
    all_files = get_all_uncleaned_files()
    start_time = time.time()
    for file_ in all_files:
        logging.info("Processing File: {}".format(file_))
        convert_to_df(filename=file_)
        logging.info("Running time is {}".format(time.time() - start_time))


if __name__ == '__main__':
    run()
