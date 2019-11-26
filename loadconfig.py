import yaml
import os


class Configuration(object):
    """

    """
    def __init__(self):
        config_ = dict()

    def load_yaml(self):
        file_name = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.yml')
        config_file = open(file=file_name)
        all_config_dict = yaml.load(config_file, Loader=yaml.FullLoader)
        print(all_config_dict)
