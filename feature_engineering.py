import datetime
import logging
import arrow

import pandas as pd

from loadconfig import config
from utils import get_all_cleaned_files, unix_time_millis


def derive_interarrival_times(file_name):
    logging.info("Inter Arrival File: {}".format(file_name))
    logging.info("Starting calculation of Interarrival Times")
    df_dataset = pd.read_csv(file_name, parse_dates=["Timestamp"])
    df_dup_list = df_dataset["Timestamp"].tolist()
    difference_list = list()
    prev_value = -1
    for row in df_dup_list:
        if prev_value == -1:
            time_stamp = arrow.get(row)
            prev_value = time_stamp.float_timestamp
            difference_list.append(None)
        else:
            time_stamp = arrow.get(row)
            curr_value = time_stamp.float_timestamp
            difference = curr_value - prev_value
            difference_list.append(difference)
            prev_value = curr_value
    df_dataset["InterarrivalTime"] = difference_list
    print(df_dataset.head())
    df_dataset.to_csv(path_or_buf=file_name, index=False)
    return df_dataset


def convert_hostname_to_number(file_name):

    df_dataset = pd.read_csv(file_name)
    x = df_dataset["Hostname"][0]
    df_dataset["Hostname"] = config.config_["FILE_CONSTANTS"][x]
    del x
    df_dataset["Type"] = df_dataset["Type"].map(config.config_["FUNCTIONALITIES"])
    df_dataset.to_csv(file_name)


def run():
    logging.info(msg="Logging Feature Engineering.")
    cleaned_files_path = get_all_cleaned_files()
    for file_ in cleaned_files_path:
        logging.info("Starting Conversion for {}".format(file_))
        convert_hostname_to_number(file_name=file_)
        derive_interarrival_times(file_name=file_)


if __name__ == "__main__":
    run()
