import pandas as pd
import datetime
import glob


def get_all_files():
    all_files_list = glob.glob('./dataset/cleaned_dataset/*.csv')
    print(all_files_list) 


def get_millisecond_value():
    pass


def run():
    get_all_files()


if __name__ == "__main__":
    run()