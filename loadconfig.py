import yaml
import os
import logging
import datetime
import pytz


class Configuration(object):
    """

    """
    def __init__(self, curr_dataset):
        self.config_ = dict()
        self.dataset = curr_dataset

    def load_msr_yaml(self):
        file_name = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'msr_config.yml')
        config_file = open(file=file_name)
        all_config_dict = yaml.load(config_file, Loader=yaml.FullLoader)
        self.config_["HUNDREDS_OF_SECONDS"] = all_config_dict["CONSTANTS"]["HUNDREDS_OF_SECONDS"]
        self.config_["EPOCH_AS_FILETIME"] = all_config_dict["CONSTANTS"]["EPOCH_AS_FILETIME"]
        self.config_["HEADER_LIST"] = all_config_dict["CONSTANTS"]["HEADER_LIST"]
        self.config_["DATASET_FOLDER"] = all_config_dict["CONSTANTS"]["DATASET_FOLDER"]
        self.config_["LOGGING_FOLDER"] = all_config_dict["CONSTANTS"]["LOGGING_FOLDER"]
        self.config_["GRAPH_FOLDER"] = all_config_dict["CONSTANTS"]["GRAPH_FOLDER"]

        self.config_["FILE_CONSTANTS"] = dict()
        kv_pairs = all_config_dict["FILE_CONSTANTS"]
        for key in kv_pairs.keys():
            self.config_["FILE_CONSTANTS"][key] = kv_pairs[key]

        del kv_pairs
        self.config_["FUNCTIONALITIES"] = dict()
        kv_pairs = all_config_dict["FUNCTIONALITIES"]
        for key in kv_pairs:
            self.config_["FUNCTIONALITIES"][key] = kv_pairs[key]

    def load_cp_yaml(self):
        file_name = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cp.yml')
        config_file = open(file=file_name)
        all_config_dict = yaml.load(config_file, Loader=yaml.FullLoader)
        self.config_["DATASET_FOLDER"] = all_config_dict["CONSTANTS"]["DATASET_FOLDER"]
        self.config_["LOGGING_FOLDER"] = all_config_dict["CONSTANTS"]["LOGGING_FOLDER"]
        self.config_["GRAPH_FOLDER"] = all_config_dict["CONSTANTS"]["GRAPH_FOLDER"]

        self.config_["READ_OPCODES"] = all_config_dict["READ_OPCODES"]
        self.config_["WRITE_OPCODES"] = all_config_dict["WRITE_OPCODES"]
        self.config_["TYPE_1_INDICES"] = all_config_dict["TYPE_1_INDICES"]
        self.config_["TYPE_2_INDICES"] = all_config_dict["TYPE_2_INDICES"]

    def load_logging_config(self):
        time_zone = pytz.timezone('US/Eastern')
        curr_date = datetime.datetime.now(tz=time_zone)
        date_as_string = curr_date.strftime('%Y-%m-%d_%H-%M-%S')
        logging.basicConfig(filename="./{}/{}.log".format(self.config_["LOGGING_FOLDER"], date_as_string),
                            level=logging.INFO)


config = Configuration(curr_dataset="def")
