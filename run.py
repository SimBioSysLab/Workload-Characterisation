from loadconfig import Configuration
from cleaning import run as cleaning_run
from feature_engineering import run as feature_engineering_run
from plots import run as plot_run


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
