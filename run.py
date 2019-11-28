from cleaning import run as cleaning_run

from loadconfig import Configuration


def load_configurations():
    config = Configuration()
    config.load_yaml()
    return config


def run_program():
    config = load_configurations()


if __name__ == '__main__':
    run_program()