import os

def create_dir_if_not_exist(dir_path: str):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


def file_exist(filepath: str):
    return os.path.exists(filepath)