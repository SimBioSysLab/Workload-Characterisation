import os
import glob
from loadconfig import config


def read_all_cp_trace_files():
    all_files_list = glob.glob("./{}/cloudphysics/*.vscsitrace".format(config.config_["DATASET_FOLDER"]))
    return all_files_list


def ret_file_name_csv(filename):
    actual_name = filename.split("/")[-1]
    actual_name = actual_name.split(".")[0]
    actual_name = "{}.csv".format(actual_name)

    file_location = "./{}/cloudphysics/csv/{}".format(config.config_["DATASET_FOLDER"], actual_name)
    return file_location


def get_split_file_name(filename):
    actual_name = filename.split("/")[-1]
    actual_name = actual_name.split(".")[0]

    return actual_name


def ret_all_csv_trace_files():

    init_path = "./{}/cloudphysics/csv/generated/".format(config.config_["DATASET_FOLDER"])
    if len(os.listdir(init_path)) == 0:
        all_files_list = glob.glob("./{}/cloudphysics/csv/*.csv".format(config.config_["DATASET_FOLDER"]))
    else:
        all_files_list = glob.glob("./{}/cloudphysics/csv/generated/*.csv".format(config.config_["DATASET_FOLDER"]))

    return all_files_list


def ret_file_name_modified_file(filename):
    actual_name = filename.split("/")[-1]
    actual_name = actual_name.split(".")[0]

    if "generated" in filename:
        actual_name = "{}_1.csv".format(actual_name)
        file_location = "./{}/cloudphysics/csv/generated/{}".format(config.config_["DATASET_FOLDER"], actual_name)
    else:
        actual_name = "{}.csv".format(actual_name)
        file_location = "./{}/cloudphysics/csv/generated/{}".format(config.config_["DATASET_FOLDER"], actual_name)

    return file_location


def ret_read_write_json_path(filename):

    actual_name = filename.split("/")[-1]
    actual_name = actual_name.split(".")[0]

    read_write_json_file_path = "./{}/result.json".format(config.config_["RESULTS_FOLDER"])
    return read_write_json_file_path, actual_name


def ret_workload_metadata_path(filename):

    actual_name = filename.split("/")[-1]
    actual_name = actual_name.split(".")[0]

    read_write_json_file_path = "./{}/workload_result.json".format(config.config_["RESULTS_FOLDER"])
    return read_write_json_file_path, actual_name


def ret_workload_rw_metadata_path(filename):

    actual_name = filename.split("/")[-1]
    actual_name = actual_name.split(".")[0]

    read_write_json_file_path = "./{}/workload_rw_result.json".format(config.config_["RESULTS_FOLDER"])
    return read_write_json_file_path, actual_name


def ret_block_len_stats(filename):

    actual_name = filename.split("/")[-1]
    actual_name = actual_name.split(".")[0]

    read_write_json_file_path = "./{}/workload_block_result.json".format(config.config_["RESULTS_FOLDER"])
    return read_write_json_file_path, actual_name


def ret_unique_block_count_path(filename):

    actual_name = filename.split("/")[-1]
    actual_name = actual_name.split(".")[0]

    read_write_json_file_path = "./{}/unique_workload_count_1.json".format(config.config_["RESULTS_FOLDER"])
    return read_write_json_file_path, actual_name


def ret_rw_unique_block_count(filename):

    actual_name = filename.split("/")[-1]
    actual_name = actual_name.split(".")[0]

    read_write_json_file_path = "./{}/read_write_unique_workload.json".format(config.config_["RESULTS_FOLDER"])
    return read_write_json_file_path, actual_name


def ret_server_result_json():

    actual_path = "./{}/server_data/server_result.json".format(config.config_["RESULTS_FOLDER"])

    return actual_path


def return_rw_graph_path():

    actual_path = "./{}/rwgraph.eps".format(config.config_["GRAPH_FOLDER"])

    return actual_path


def return_iat_graph_path(filename):
    actual_name = filename.split("/")[-1]
    actual_name = actual_name.split(".")[0]

    actual_path = "./{}/iat/{}.eps".format(config.config_["GRAPH_FOLDER"], actual_name)
    return actual_path, actual_name


def return_rw_ia_json(filename):
    actual_name = filename.split("/")[-1]
    actual_name = actual_name.split(".")[0]

    actual_path = "./{}/rwia/{}.json".format(config.config_["RESULTS_FOLDER"], actual_name)
    return actual_path, actual_name


def return_block_rw_ia_json(filename):
    actual_name = filename.split("/")[-1]
    actual_name = actual_name.split(".")[0]

    actual_path = "./{}/block_rwia/{}.json".format(config.config_["RESULTS_FOLDER"], actual_name)
    return actual_path, actual_name


def block_result_path():
    read_write_json_file_path = "./{}/server_data/workload_block_result.json".format(config.config_["RESULTS_FOLDER"])
    return read_write_json_file_path


def workload_rw_path():
    read_write_json_file_path = "./{}/server_data/workload_rw_result.json".format(config.config_["RESULTS_FOLDER"])
    return read_write_json_file_path


def workload_iat_path():
    read_write_json_file_path = "./{}/server_data/workload_result.json".format(config.config_["RESULTS_FOLDER"])
    return read_write_json_file_path


def workload_unique_block_path():

    read_write_json_file_path = "./{}/server_data/unique_workload_count_1.json".format(config.config_["RESULTS_FOLDER"])
    return read_write_json_file_path


def workload_unique_block_non_server():

    read_write_json_file_path = "./{}/unique_workload_count_1.json".format(config.config_["RESULTS_FOLDER"])
    return read_write_json_file_path


def df_dataset_path():
    read_write_json_file_path = "./{}/combined_workload.csv".format(config.config_["RESULTS_FOLDER"])
    return read_write_json_file_path


def get_hit_ratio_filename(file_, algorithm_name):
    json_file_name = "./{}/hit_ratio/{}_{}_.json".format(config.config_["RESULTS_FOLDER"], file_, algorithm_name)
    result = os.path.exists(json_file_name)
    if result:
        return False
    return json_file_name


def get_hit_ratio(file_, algorithm_name):
    json_file_name = "./{}/hit_ratio/{}_{}_.json".format(config.config_["RESULTS_FOLDER"], file_, algorithm_name)
    result = os.path.exists(json_file_name)
    if result:
        return json_file_name
    return False


def bucket_json_path():
    read_write_json_file_path = "./{}/bucketed_hit_rate.json".format(config.config_["RESULTS_FOLDER"])
    return read_write_json_file_path


def server_bucketed_json_path():
    server_bucketed = "./{}/server_data/bucketed_hit_rate.json".format(config.config_["RESULTS_FOLDER"])
    return server_bucketed
