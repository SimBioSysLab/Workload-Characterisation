import logging
import time

from loadconfig import config
from cloudphysics.read_vscsi import run_reading
from cloudphysics.feature_engineering import run_feature_engineering
from cloudphysics.graph_generator import run_generate_graphs


def load_configuration(dataset):

    assert dataset in ["cp", "msr"], "Wrong Dataset Name"

    config.dataset = dataset
    if config.dataset == "msr":
        config.load_msr_yaml()
    if config.dataset == "cp":
        config.load_cp_yaml()

    config.load_logging_config()


def run_cp_traces():
    st_time = time.time()
    # run_reading()
    # run_feature_engineering()
    run_generate_graphs()
    end_time = time.time()
    time_ = end_time - st_time
    logging.info("Total running time is : {}".format(time_))


if __name__ == '__main__':
    import argparse
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("dataset", help="Either cloudphysics or msr")
    args = argument_parser.parse_args()
    dataset_ = args.dataset
    load_configuration(dataset=dataset_)

    if dataset_ == "cp":
        run_cp_traces()
    else:
        pass
