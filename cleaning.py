import csv
import glob
import pandas as pd

HEADER_LIST = ["Timestamp", "Hostname", "DiskNumber", "Type", "Offset", "Size", "ResponseTime"]


def convert_to_df():
    pass


def read_file(filename):
    file_descriptor = open(filename, "r")
    file_reader = csv.reader(file_descriptor, delimiter=",")
    print(list(file_reader))
    # for row in file_reader:
    #     print(row)


def get_file_names():
    filenames = glob.glob("./dataset/*.csv")
    read_file(filenames[0])


def run():
    get_file_names()


if __name__ == '__main__':
    run()