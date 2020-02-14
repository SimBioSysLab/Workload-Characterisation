import time

import csv
from PyMimircache import Cachecow

from cloudphysics.utils import read_all_cp_trace_files, ret_file_name_csv, get_split_file_name, ret_all_csv_trace_files
from loadconfig import config
import logging


def convert_to_csv(file_name):
    logging.info("Processing File: {}".format(file_name))

    type_1_dict = config.config_["TYPE_1_INDICES"]
    type_2_dict = config.config_["TYPE_2_INDICES"]

    i = 1

    if "vscsi1" in file_name:
        trace_type = 1
    else:
        trace_type = 2

    act_name = ret_file_name_csv(filename=file_name)

    c = Cachecow()

    try:
        reader = c.vscsi(file_path=file_name, vscsi_type=trace_type)

    except AssertionError as e:
        logging.info("The exception is {}".format(e))
        logging.info("Excepted file name is {}".format(file_name))
        return

    if trace_type == 1:

        data = reader.read_complete_req()

        csv_file = open(act_name, mode='w')
        csv_writer = csv.DictWriter(csv_file, fieldnames=type_1_dict.keys())
        csv_writer.writeheader()

        while data:

            if i % 10000 == 0:
                logging.info("Finished {} records of type 1 and file {}".format(i, file_name))

            temp_dict = {
                "LEN": data[type_1_dict["LEN"]],
                "OP_CODE": data[type_1_dict["OP_CODE"]],
                "BLOCK_NUMBER": data[type_1_dict["BLOCK_NUMBER"]],
                "TIME_STAMP": data[type_1_dict["TIME_STAMP"]],
            }

            csv_writer.writerow(temp_dict)
            # print(data)
            data = reader.read_complete_req()
            i = i + 1

    else:

        csv_file = open(act_name, mode='w')
        csv_writer = csv.DictWriter(csv_file, fieldnames=type_2_dict.keys())
        data = reader.read_complete_req()
        csv_writer.writeheader()
        while data:
            if i % 10000 == 0:
                logging.info("Finished {} records of type 1 and file {}".format(i, file_name))

            temp_dict = {
                "LEN": data[type_2_dict["LEN"]],
                "OP_CODE": data[type_2_dict["OP_CODE"]],
                "BLOCK_NUMBER": data[type_2_dict["BLOCK_NUMBER"]],
                "TIME_STAMP": data[type_2_dict["TIME_STAMP"]],
                "RESPONSE_TIME": data[type_2_dict["RESPONSE_TIME"]],
            }

            csv_writer.writerow(temp_dict)
            # print(temp_dict)
            data = reader.read_complete_req()
            i = i + 1

    return 1


def get_all_file_names():
    logging.info("Getting all filenames")
    files_list = ret_all_csv_trace_files()
    file_prefix_list = []
    for file_ in files_list:
        temp_fname = get_split_file_name(file_)
        file_prefix_list.append(temp_fname)
    logging.info("Returning file names")
    return file_prefix_list


def run_reading():

    all_files_list = read_all_cp_trace_files()

    for file_ in all_files_list:
        st_time = time.time()
        convert_to_csv(file_name=file_)
        end_time = time.time()
        time_ = end_time - st_time
        logging.info("Processing of file {} took {} time".format(file_, time_))


if __name__ == '__main__':
    pass
