import logging
import arrow

import pandas as pd


from loadconfig import config
from msr_ml_proj.utils import get_all_cleaned_files, get_writing_file_name


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
            difference = (curr_value - prev_value)
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


def extract_project_dataset():
    logging.info("Starting Extraction on Dataset")

    all_files_list = get_all_cleaned_files()
    column_list = config.config_["HEADER_LIST"]
    column_list.append("InterarrivalTime")
    all_df_list = pd.DataFrame(columns=column_list)

    for file_ in all_files_list:
        logging.info("Starting on file: {}".format(file_))
        temp_dataset = pd.read_csv(file_)
        print(temp_dataset)
        del temp_dataset["Unnamed: 0"]
        temp_dataset_ = temp_dataset.head(2000)
        all_df_list = all_df_list.append(temp_dataset_, sort=False, ignore_index=True)

    all_df_list.to_csv(path_or_buf=get_writing_file_name(), index=False)
    return 1


def run():
    logging.info(msg="Logging Feature Engineering.")
    cleaned_files_path = get_all_cleaned_files()
    extract_project_dataset()
    # for file_ in cleaned_files_path:
    #     logging.info("Starting Conversion for {}".format(file_))
    #     convert_hostname_to_number(file_name=file_)
    #     derive_interarrival_times(file_name=file_)


if __name__ == "__main__":
    run()
