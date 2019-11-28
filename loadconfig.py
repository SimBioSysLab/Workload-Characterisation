import yaml
import os
import logging


class Configuration(object):
    """

    """
    def __init__(self):
        self.config_ = dict()

    def load_yaml(self):
        file_name = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.yml')
        config_file = open(file=file_name)
        all_config_dict = yaml.load(config_file, Loader=yaml.FullLoader)
        self.config_["HUNDREDS_OF_SECONDS"] = all_config_dict["CONSTANTS"]["HUNDREDS_OF_SECONDS"]
        self.config_["EPOCH_AS_FILETIME"] = all_config_dict["CONSTANTS"]["EPOCH_AS_FILETIME"]
        self.config_["HEADER_LIST"] = all_config_dict["CONSTANTS"]["HEADER_LIST"]
        self.config_["DATASET_FOLDER"] = all_config_dict["CONSTANTS"]["DATASET_FOLDER"]

    def load_logging_config(self):
        logging.basicConfig(filename="example.log", level=logging.INFO)


config = Configuration()
config.load_yaml()
config.load_logging_config()
