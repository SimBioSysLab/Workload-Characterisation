import matplotlib

matplotlib.use('TkAgg')
import json
import plotly.express as px
import plotly.graph_objects as go
import numpy

import matplotlib.pyplot as plt
from cloudphysics.utils import ret_server_result_json, return_rw_graph_path


def generate_read_write_graph():
    file_path = ret_server_result_json()
    json_fd = open(file_path, 'r')
    dataset = json.load(json_fd)
    file_names_list = [x["filename"] for x in dataset]
    rw_ratio_list = [x["read_count"] / x["write_count"] for x in dataset]
    rw_ratio_list = sorted(rw_ratio_list)
    # print(file_names_list)
    # print(rw_ratio_list)
    range_ = numpy.arange(len(file_names_list))
    print(rw_ratio_list)

    plt.bar(range_, rw_ratio_list, log=True)
    plt.xticks(range_, file_names_list, rotation=90)
    plt.show()


def run_generate_graphs():
    generate_read_write_graph()
