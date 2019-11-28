# LIST OF IN BUILT LIBRARIES
import csv
import time
from datetime import datetime
import os

# LIST OF INSTALLED LIBRARIES
import pandas as pd

# PROJECT LIBRARIES
from utils import get_all_uncleaned_files, get_cleaned_files_path
from loadconfig import config


def convert_filetime_to_dt(filetime):
    if type(filetime) == str:
        filetime = float(filetime)
    return datetime.utcfromtimestamp((filetime - config["EPOCH_AS_FILETIME"]) / config["HUNDREDS_OF_SECONDS"])


def convert_to_df(filename):
    print("Parent Process: {}".format(os.getppid()))
    print("Current Process: {}".format(os.getpid()))
    file_descriptor = open(filename, "r")
    file_reader = csv.reader(file_descriptor, delimiter=",")
    all_values = list(file_reader)

    for value in all_values:
        value[0] = convert_filetime_to_dt(float(value[0]))

    file_path = get_cleaned_files_path(file_name=filename)
    dataset = pd.DataFrame(all_values, columns=config["HEADER_LIST"])
    dataset.to_csv(file_path, index=False)


def run():
    all_files = get_all_uncleaned_files()
    start_time = time.time()
    for file_ in all_files:
        print("Processing File: {}".format(file_))
        convert_to_df(filename=file_)
        print("Running time is {}".format(time.time() - start_time))


if __name__ == '__main__':
    run()
