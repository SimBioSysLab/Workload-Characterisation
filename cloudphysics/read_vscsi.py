import time

import pandas as pd
from PyMimircache import Cachecow

from cloudphysics.utils import read_all_cp_trace_files, ret_file_name_csv
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
    reader = c.vscsi(file_path=file_name, vscsi_type=trace_type)

    if trace_type == 1:

        changed_dataset = pd.DataFrame(columns=config.config_["TYPE_1_INDICES"])
        data = reader.read_complete_req()
        while data:
            if i % 1000 == 0:
                logging.info("Finished {} records of type 1 and file {}".format(i, file_name))
                
            temp_dict = {
                "LEN": data[type_1_dict["LEN"]],
                "OP_CODE": data[type_1_dict["OP_CODE"]],
                "BLOCK_NUMBER": data[type_1_dict["BLOCK_NUMBER"]],
                "TIME_STAMP": data[type_1_dict["TIME_STAMP"]],
            }

            changed_dataset = changed_dataset.append(temp_dict,ignore_index=True)

            # print(data)
            data = reader.read_complete_req()
            i = i + 1

    else:

        changed_dataset = pd.DataFrame(columns=config.config_["TYPE_2_INDICES"])
        data = reader.read_complete_req()
        while data:
            if i % 1000 == 0:
                logging.info("Finished {} records of type 1 and file {}".format(i, file_name))

            temp_dict = {
                "LEN": data[type_2_dict["LEN"]],
                "OP_CODE": data[type_2_dict["OP_CODE"]],
                "BLOCK_NUMBER": data[type_2_dict["BLOCK_NUMBER"]],
                "TIME_STAMP": data[type_2_dict["TIME_STAMP"]],
                "RESPONSE_TIME": data[type_2_dict["RESPONSE_TIME"]],
            }

            changed_dataset = changed_dataset.append(temp_dict, ignore_index=True)
            # print(temp_dict)
            data = reader.read_complete_req()
            i = i + 1
    logging.info("Writing to file {}".format(act_name))
    changed_dataset.to_csv(act_name, index=False)

    return 1


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
