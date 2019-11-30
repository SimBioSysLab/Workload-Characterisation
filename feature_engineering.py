import datetime
import logging

import pandas as pd

from loadconfig import config
from utils import get_all_cleaned_files


def derive_interarrival_times(file_name):
    df_dataset = pd.read_csv(file_name, parse_dates=["Timestamp"])
    df_dup = df_dataset["Timestamp"].copy(deep=True)
    

    exit()


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
    # for file_ in cleaned_files_path:
    #     convert_hostname_to_number(file_name=file_)
    for file_ in cleaned_files_path:
        derive_interarrival_times(file_name=file_)


if __name__ == "__main__":
    run()
