import logging
import time
import os

from loadconfig import config
from cloudphysics.read_vscsi import run_reading
from cloudphysics.feature_engineering import run_feature_engineering
from cloudphysics.graph_generator import run_generate_graphs
from cloudphysics.combiner import run_combiner
from cloudphysics.model import run_model
from cloudphysics.hit_ratio_generator import run_hr_gen

from msr_ml_proj.cleaning import run as cleaning_run
from msr_ml_proj.plots import run as plot_run
from msr_ml_proj.feature_engineering import run as feature_run

from cp_block_level.feature_engineering import run_feature_engineering as cpb_feature_run
from cp_block_level.extract_comparison import run_comparison


def load_configuration(dataset, ip_path=None, op_path=None, op_aggr=None, split_file=False, compute_stat=False,
                       compare_days=False):

    assert dataset in ["cp", "msr", "cpb"], "Wrong Dataset Name"

    config.dataset = dataset
    if config.dataset == "msr":
        config.load_msr_yaml()
    if config.dataset == "cp":
        config.load_cp_yaml()
    if config.dataset == "cpb":
        if split_file:
            print(ip_path)
            print(op_path)
            print(op_aggr)
            print(split_file)
            print(compute_stat)
            if not ip_path or not op_path or not os.path.isdir(op_path) or not os.path.exists(op_path) or not \
                    os.path.exists(op_aggr) or not os.path.isdir(op_aggr):
                print("Please enter the correct input and output path")
                exit(0)

            config.load_cpb_yaml(ip_path=ip_path, op_path=op_path, op_aggr=op_aggr, split_file=split_file)
        if compute_stat:
            print(ip_path)
            print(op_path)
            print(op_aggr)
            print(split_file)
            print(compute_stat)
            # if not op_path or not os.path.isdir(op_path) or not os.path.exists(op_path) or not \
            #         os.path.exists(op_aggr) or not os.path.isdir(op_aggr):
            #     print("Please enter the correct input and output path")
            #     exit(0)
            config.load_cpb_yaml(ip_path=ip_path, op_path=op_path, op_aggr=op_aggr, split_file=split_file,
                                 compute_stat=True, compare_days=True)
        if compare_days:
            config.load_cpb_yaml(ip_path=ip_path, op_path=op_path, op_aggr=None, split_file=split_file,
                                compute_stat=False, compare_days=True)
        else:
            pass
    config.load_logging_config()


def run_cp_traces():
    st_time = time.time()
    # run_reading()
    # run_feature_engineering()
    run_generate_graphs()
    # run_combiner()
    # run_model()
    # run_hr_gen()
    end_time = time.time()
    time_ = end_time - st_time
    logging.info("Total running time is : {}".format(time_))


def run_msr_traces():
    st_time = time.time()
    # cleaning_run()
    # feature_run()
    plot_run()
    end_time = time.time()
    time_ = end_time - st_time
    logging.info("Total running time is : {}".format(time_))


def run_cpb_traces():
    st_time = time.time()
    # cpb_feature_run()
    run_comparison()
    end_time = time.time()
    time_ = end_time - st_time
    logging.info("Total running time is: {}".format(time_))


if __name__ == '__main__':
    import argparse
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument("dataset", help="Either cloudphysics or msr")
    argument_parser.add_argument("--ip_file", help="Input file path, only for cpb")
    argument_parser.add_argument("--op_file", help="Output file path, only for cpb")
    argument_parser.add_argument("--op_aggr", help="Output file path for multiple days, cpb, if splitting")
    argument_parser.add_argument("--split_script", help="1 or 0 to split the files into particular slices")
    argument_parser.add_argument("--compute_stat", help="1 or 0 to collect stats for day 1-7")
    argument_parser.add_argument("--compare_days", help="1 or 0 based on comparing stuff")
    args = argument_parser.parse_args()
    dataset_ = args.dataset
    ip_path = args.ip_file
    op_path = args.op_file
    op_aggr = args.op_aggr
    split_file = int(args.split_script)
    compute_stat = int(args.compute_stat)
    compare_days = int(args.compare_days)
    if dataset_ == "cpb":
        if split_file == 1:
            load_configuration(dataset=dataset_, ip_path=ip_path, op_path=op_path, op_aggr=op_aggr, split_file=True)

        if compute_stat == 1:
            load_configuration(dataset=dataset_, ip_path=ip_path, op_path=op_path, op_aggr=op_aggr, split_file=False,
                               compute_stat=True)

        if compare_days == 1:
            load_configuration(dataset=dataset_, ip_path=ip_path, op_path=op_path, op_aggr=None, split_file=False,
                               compute_stat=False, compare_days=True)
        else:
            load_configuration(dataset=dataset_, op_path=op_path, op_aggr=op_aggr, split_file=False, compute_stat=False)
    else:
        load_configuration(dataset=dataset_)

    if dataset_ == "cp":
        run_cp_traces()
    if dataset_ == "msr":
        run_msr_traces()
    if dataset_ == "cpb":
        run_cpb_traces()
    else:
        pass
