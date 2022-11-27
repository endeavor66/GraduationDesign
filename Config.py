from utils.file_utils import create_dir_if_not_exist

# 文件类型
FILE_TYPES = ["fork_merge", "fork_close", "unfork_merge", "unfork_close"]

# 文件路径
EVENT_LOG_DIR = "DataAcquire/event_log_data"
TEMP_DATA_DIR = f"{EVENT_LOG_DIR}/temp_data"
PROCESS_DATA_DIR = 'ProcessMining/process_data'
INPUT_DATA_DIR = "ProcessMining/input_data"
OUTPUT_DATA_DIR = "ProcessMining/output_data"
FEATURE_DIR = "AnomalyDetection/feature"

# 创建不存在的目录
create_dir_if_not_exist(TEMP_DATA_DIR)
create_dir_if_not_exist(PROCESS_DATA_DIR)
create_dir_if_not_exist(OUTPUT_DATA_DIR)
create_dir_if_not_exist(FEATURE_DIR)


