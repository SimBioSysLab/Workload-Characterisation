from msr_ml_proj.loadconfig import Configuration
from msr_ml_proj.plots import run as plot_run


def load_configurations():
    config = Configuration()
    config.load_yaml()


def run_program():
    load_configurations()
    # cleaning_run()
    feature_engineering_run()
    # plot_run()


if __name__ == '__main__':
    run_program()
