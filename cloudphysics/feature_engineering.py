import os
import csv
import logging
import time

from loadconfig import config
from cloudphysics.utils import ret_all_csv_trace_files, ret_file_name_modified_file


def convert_to_opcodes(filename):
    logging.info("Starting opcode conversion of file {}".format(filename))
    i = 0
    dataset_file = open(filename, "r")
    file_details = csv.DictReader(dataset_file)

    file_keys = file_details.fieldnames

    if "BIN_OPCODE" not in file_keys:
        file_keys = file_keys + ["BIN_OPCODE"]
    else:
        logging.info("File {} already exists. No need to convert things!".format(filename))
        return

    new_file_name = ret_file_name_modified_file(filename=filename)
    writing_file_d = open(new_file_name, "w")
    file_writer = csv.DictWriter(writing_file_d, fieldnames=file_keys)
    file_writer.writeheader()

    read_opcodes = config.config_["READ_OPCODES"]
    i = 1
    dict_list = []

    value = -2
    for row in file_details:

        if i % 10000 == 0:
            logging.info("Finished {} lines of {}".format(i, filename))

        try:
            value = int(row["OP_CODE"])
            if value in read_opcodes:
                row["BIN_OPCODE"] = 0
            else:
                row["BIN_OPCODE"] = 1

        except ValueError as e:
            logging.info("The exception is {}, file is {} and value is {}".format(e, filename, row["OP_CODE"]))
            row["BIN_CODE"] = -1

        i += 1

        file_writer.writerow(row)

    if "generated" in filename:
        os.remove(filename)
        os.rename(new_file_name, filename)


def run_feature_engineering():
    all_files_list = ret_all_csv_trace_files()
    for file_ in all_files_list:
        start_time = time.time()
        convert_to_opcodes(filename=file_)
        end_time = time.time()
        k = end_time - start_time
        logging.info("The time taken to execute {} is {}".format(file_, k))
