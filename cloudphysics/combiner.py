import json
import pandas as pd
from cloudphysics.utils import ret_server_result_json, block_result_path, workload_rw_path, workload_iat_path, \
    df_dataset_path


def get_server_result():
    data_fd = open(ret_server_result_json())
    dataset = json.load(data_fd)
    return dataset


def get_rw_result():
    data_fd = open(workload_rw_path())
    dataset = json.load(data_fd)
    return dataset


def get_iat_result():
    data_fd = open(workload_iat_path())
    dataset = json.load(data_fd)
    return dataset


def get_block_result():
    data_fd = open(block_result_path())
    dataset = json.load(data_fd)
    return dataset


def combine_data():
    server_data = get_server_result()
    rw_result = get_rw_result()
    iat_result = get_iat_result()
    block_result = get_block_result()

    temp_op_list = list()
    for serve, rw, iat, block in zip(server_data, rw_result, iat_result, block_result):
        temp_dict = {
            "filename": serve["filename"],
            "rw_ratio": float(serve["read_count"]) / float(serve["write_count"]),
            "iat_range": iat["range"],
            "iat_avg": iat["average"],
            "iat_median": iat["median"],
            "read_range": rw["read_tuple"][0],
            "read_avg": rw["read_tuple"][1],
            "read_median": rw["read_tuple"][2],
            "write_range": rw["write_tuple"][0],
            "write_avg": rw["write_tuple"][1],
            "write_median": rw["write_tuple"][2],
            "block_range": block["range"],
            "block_average": block["average"],
            "block_median": block["median"]
        }
        temp_op_list.append(temp_dict)

    dataframe = pd.DataFrame(temp_op_list)
    op_file_path = df_dataset_path()
    dataframe.to_csv(op_file_path, index=False)


def run_combiner():
    combine_data()


if __name__ == '__main__':
    run_combiner()