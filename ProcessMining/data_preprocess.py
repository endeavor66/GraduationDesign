import pm4py
import os
import pandas as pd


EVENT_LOG_DIR = "../DataAcquire/event_log_data"
PROCESS_DATA_DIR = 'process_data'
if not os.path.exists(PROCESS_DATA_DIR):
    os.makedirs(PROCESS_DATA_DIR)


def load_data(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    log = pm4py.format_dataframe(df,
                                 case_id='CaseID',
                                 activity_key='Activity',
                                 timestamp_key='StartTimestamp',
                                 timest_format='%Y-%m-%d %H:%M:%S')
    # 保存读取结果
    filename = filepath[filepath.rindex('/') + 1:]
    out_path = f"{PROCESS_DATA_DIR}/{filename}"
    log.to_csv(out_path, header=True, index=False)
    return log


if __name__ == '__main__':
    df = load_data(f"{EVENT_LOG_DIR}/tensorflow_fork_close.csv")
    print(df.shape)