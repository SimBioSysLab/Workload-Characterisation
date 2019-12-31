import glob
import datetime


def get_all_cp_files():
    pass


def get_cleaned_files_path(file_name):
    df_file_name = file_name.split('/')[2]
    df_file_name = "df{}".format(df_file_name)
    constructed_filename = "./{}/cleaned_dataset/{}".format(config.config_["DATASET_FOLDER"], df_file_name)
    return constructed_filename


def get_all_cleaned_files():
    all_files_list = glob.glob("./{}/cleaned_dataset/*.csv".format(config.config_["DATASET_FOLDER"]))
    return all_files_list


def get_writing_file_name():
    file_name = "./{}/all_result.csv".format(config.config_["DATASET_FOLDER"])
    return file_name


def get_project_dataset():
    file_name = "./{}/combined_dataset.csv".format(config.config_["DATASET_FOLDER"])
    return file_name


def unix_time_millis(dt):
    dt_obj = datetime.datetime.strptime(date_string=dt)
    print(dt_obj.second)
