import os

# 查看是否存在该文件路径，不存在则创建
def path_exists_or_create(file_path):
    if not os.path.exists(file_path):
        os.makedirs(file_path)