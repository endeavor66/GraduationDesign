from utils.file_utils import create_dir_if_not_exist

# 文件类型
FILE_TYPES = ["fork_merge", "fork_close", "unfork_merge", "unfork_close"]

# 原始事件日志
EVENT_LOG_DIR = "../DataAcquire/event_log_data"

# 待检验的事件日志
INPUT_DATA_DIR = "input_data"

# 事件日志预处理结果(更换列名/细分SubmitCommit)、角色信息、角色变更
PROCESS_DATA_DIR = "process_data"
LOG_ALL_SCENE_DIR = f"{PROCESS_DATA_DIR}/log_all_scene"
LOG_SINGLE_SCENE_DIR = f"{PROCESS_DATA_DIR}/log_single_scene"
ROLE_CHANGE_DIR = f"{PROCESS_DATA_DIR}/role_change"

# 过程模型、轨迹对齐
OUTPUT_DATA_DIR = "output_data"
PROCESS_MODEL_DIR = f"{OUTPUT_DATA_DIR}/process_model"
HEURISTICS_NET_DIR = f"{PROCESS_MODEL_DIR}/heuristics_net"
PETRI_NET_DIR = f"{PROCESS_MODEL_DIR}/petri_net"
ALIGNMENT_DIR = f"{OUTPUT_DATA_DIR}/alignment"

# 创建不存在的目录
create_dir_if_not_exist(PROCESS_DATA_DIR)
create_dir_if_not_exist(LOG_ALL_SCENE_DIR)
create_dir_if_not_exist(LOG_SINGLE_SCENE_DIR)
create_dir_if_not_exist(ROLE_CHANGE_DIR)
create_dir_if_not_exist(OUTPUT_DATA_DIR)
create_dir_if_not_exist(PROCESS_DATA_DIR)
create_dir_if_not_exist(HEURISTICS_NET_DIR)
create_dir_if_not_exist(PETRI_NET_DIR)
create_dir_if_not_exist(ALIGNMENT_DIR)


