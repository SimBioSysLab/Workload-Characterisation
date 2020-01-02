from loadconfig import config
from cloudphysics.read_vscsi import run_reading


def load_configuration(dataset):

    assert dataset in ["cp", "msr"], "Wrong Dataset Name"

    config.dataset = dataset
    if config.dataset == "msr":
        config.load_msr_yaml()
    if config.dataset == "cp":
        config.load_cp_yaml()

    config.load_logging_config()


def run_cp_traces():
    run_reading()


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
