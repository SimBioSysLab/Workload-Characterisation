from cleaning import run as cleaning_run

from loadconfig import Configuration


def load_configurations():
    config = Configuration()
    config.load_yaml()


def run_program():
    load_configurations()
    cleaning_run()


if __name__ == '__main__':
    run_program()
