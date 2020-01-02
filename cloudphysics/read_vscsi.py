import pandas as pd
from PyMimircache import Cachecow

from cloudphysics.utils import read_all_cp_trace_files
from loadconfig import config


def convert_to_csv(file_name):
    print("Filename: {}".format(file_name))
    print("Config values: {}".format(config.config_["WRITE_OPCODES"]))

    if "vscsi1" in file_name:
        trace_type = 1
    else:
        trace_type = 2

    c = Cachecow()
    reader = c.vscsi(file_path=file_name, vscsi_type=trace_type)

    changed_dataset = 1
    # print("The trace type is {}".format(trace_type))
    data = reader.read_complete_req()

    for i in range(5):
        print(data)
        data = reader.read_complete_req()


def run_reading():

    all_files_list = read_all_cp_trace_files()
    for file_ in all_files_list:
        convert_to_csv(file_name=file_)


if __name__ == '__main__':
    pass
