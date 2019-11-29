import datetime
import logging

import pandas as pd

from loadconfig import config
from utils import get_all_cleaned_files


def derive_interarrival_times(file_name):
    df_dataset = pd.read_csv(file_name)
    print(df_dataset.head())
    exit()


def convert_hostname_to_number(file_name):
    pass

def run():
    logging.info(msg="Logging Feature Engineering.")
    cleaned_files_path = get_all_cleaned_files()
    for file_ in cleaned_files_path:
        derive_interarrival_times(file_name=file_)


if __name__ == "__main__":
    run()
