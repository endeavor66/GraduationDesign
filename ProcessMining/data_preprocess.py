import pandas as pd
import pm4py
from Config import *


def load_data(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    df['StartTimestamp'] = pd.to_datetime(df['StartTimestamp'])
    event_log = pm4py.format_dataframe(df,
                                       case_id='CaseID',
                                       activity_key='Activity',
                                       timestamp_key='StartTimestamp',
                                       timest_format='%Y-%m-%d %H:%M:%S')
    return event_log


def data_preprocess(repo: str):
    for t in FILE_TYPES:
        # 初始化参数
        filename = f"{repo}_{t}.csv"
        input_path = f"{EVENT_LOG_DIR}/{filename}"
        output_path = f"{PROCESS_DATA_DIR}/{filename}"

        # 加载事件日志
        log = load_data(input_path)

        # 剔除无效案例
        log = pm4py.filter_start_activities(log, ["ForkRepository", "CreateBranch", "SubmitCommit", "OpenPR"])
        log = pm4py.filter_end_activities(log, ["DeleteBranch", "ClosePR", "MergePR"])

        # 保存文件
        log.to_csv(output_path, header=True, index=False)


if __name__ == '__main__':
    repo = "tensorflow"
    data_preprocess(repo)
