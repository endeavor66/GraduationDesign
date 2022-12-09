from utils.file_utils import create_dir_if_not_exist

# 文件类型
FILE_TYPES = ["fork_merge", "fork_close", "unfork_merge", "unfork_close"]

# 敏感文件相关信息
SENSITIVE_FILE_SUFFIX = ["xml", "json", "jar", "ini", "dat", "cnf", "yml", "toml", "gradle", "bin", "config", "exe",
                         "properties", "cmd", "build", "cfg"]


# 文件路径
FEATURE_DIR = "feature"
LOG_ALL_SCENE_DIR = "../ProcessMining/process_data/log_all_scene"
OUTPUT_DATA_DIR = "output_data"

# 创建不存在的目录
create_dir_if_not_exist(FEATURE_DIR)
create_dir_if_not_exist(OUTPUT_DATA_DIR)


