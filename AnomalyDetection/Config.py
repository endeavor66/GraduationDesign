from utils.file_utils import create_dir_if_not_exist

# 文件类型
FILE_TYPES = ["fork_merge", "fork_close", "unfork_merge", "unfork_close"]

# 敏感文件相关信息
SENSITIVE_FILE_SUFFIX = ["xml", "json", "jar", "ini", "dat", "cnf", "yml", "toml", "gradle", "bin", "config", "exe",
                         "properties", "cmd", "build", "cfg"]

# 异常数据比例
ANOMALY_PERCENT = 5

# 文件路径
FEATURE_DIR = "feature"
LOG_ALL_SCENE_DIR = "../ProcessMining/process_data/log_all_scene"
OUTPUT_DATA_DIR = "output_data"
BOX_PLOT_DIR = f"{OUTPUT_DATA_DIR}/box_plot"
ISOLATION_FOREST_DIR = f"{OUTPUT_DATA_DIR}/isolation_forest"
ONE_CLASS_SVM_DIR = f"{OUTPUT_DATA_DIR}/one_class_svm"
LOF_DIR = f"{OUTPUT_DATA_DIR}/lof"
MULTI_MODEL_VOTE_DIR = f"{OUTPUT_DATA_DIR}/multi_model_vote"
RANDOM_FOREST_DIR = f"{OUTPUT_DATA_DIR}/random_forest"


# 创建不存在的目录
create_dir_if_not_exist(FEATURE_DIR)
create_dir_if_not_exist(OUTPUT_DATA_DIR)
create_dir_if_not_exist(BOX_PLOT_DIR)
create_dir_if_not_exist(ISOLATION_FOREST_DIR)
create_dir_if_not_exist(ONE_CLASS_SVM_DIR)
create_dir_if_not_exist(LOF_DIR)
create_dir_if_not_exist(MULTI_MODEL_VOTE_DIR)
create_dir_if_not_exist(RANDOM_FOREST_DIR)


