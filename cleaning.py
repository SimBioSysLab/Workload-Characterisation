import csv
import glob
from datetime import datetime, timedelta, tzinfo
from calendar import timegm
import pandas as pd

EPOCH_AS_FILETIME = 116444736000000000  # January 1, 1970 as MS file time
HUNDREDS_OF_NANOSECONDS = 10000000
HEADER_LIST = ["Timestamp", "Hostname", "DiskNumber", "Type", "Offset", "Size", "ResponseTime"]


def convert_filetime_to_dt(filetime):
    return datetime.utcfromtimestamp((filetime - EPOCH_AS_FILETIME) / HUNDREDS_OF_NANOSECONDS)


def convert_to_df(list_of_values):

    for value in list_of_values:
        value[0] = convert_filetime_to_dt(int(value[0]))
        print(value)
        exit()


def read_file(filename):
    file_descriptor = open(filename, "r")
    file_reader = csv.reader(file_descriptor, delimiter=",")
    all_values = list(file_reader)
    convert_to_df(all_values)


def get_file_names():
    filenames = glob.glob("./dataset/*.csv")
    read_file(filenames[0])


def run():
    get_file_names()


if __name__ == '__main__':
    run()