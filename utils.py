import glob


def get_all_uncleaned_files():
    all_files_list = glob.glob("./dataset/*.csv")
    return all_files_list


def get_cleaned_files_path(file_name):
    df_file_name = file_name.split('/')[2]
    df_file_name = "df{}".format(df_file_name)
    constructed_filename = "./dataset/cleaned_dataset/{}".format(df_file_name)
    return constructed_filename


def get_all_cleaned_files():
    pass
