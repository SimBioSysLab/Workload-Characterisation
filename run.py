from cleaning import run as cleaning_run
from loadconfig import Config

def load_configurations():
    config = Config()
    config.load_yaml()


def run_program():
    load_configurations()


if __name__ == '__main__':
    run_program()