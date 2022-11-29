import pandas as pd
import pm4py
from ProcessMining.Config import *


def load_data(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    df['StartTimestamp'] = pd.to_datetime(df['StartTimestamp'])
    event_log = pm4py.format_dataframe(df,
                                       case_id='CaseID',
                                       activity_key='Activity',
                                       timestamp_key='StartTimestamp',
                                       timest_format='%Y-%m-%d %H:%M:%S')
    return event_log


def log_info(log: pd.DataFrame):
    case_array = log['case:concept:name'].unique()
    print("rest case: %d" % len(case_array))
    # print("case: %s \n" % case_array)


def data_preprocess(repo: str):
    for t in FILE_TYPES:
        # 初始化参数
        filename = f"{repo}_{t}.csv"
        input_path = f"{EVENT_LOG_DIR}/{filename}"
        output_path = f"{PROCESS_DATA_DIR}/{filename}"

        # 加载事件日志
        log = load_data(input_path)
        temp_path = f"{PROCESS_TEMP_DATA_DIR}/temp-{filename}"
        log.to_csv(temp_path, header=True, index=False)
        print("total case: %d" % len(log['case:concept:name'].unique()))

        # 剔除无效案例
        log = pm4py.filter_start_activities(log, ["ForkRepository", "CreateBranch", "SubmitCommit", "OpenPR"])
        print("filter_start_activities")
        log_info(log)

        log = pm4py.filter_end_activities(log, ["DeleteBranch", "ClosePR", "MergePR"])
        print("filter_end_activities")
        log_info(log)

        log = pm4py.filter_event_attribute_values(log, 'concept:name', ['OpenPR'], level="case", retain=True)
        print("filter_event_attribute_values (OpenPR)")
        log_info(log)

        log = pm4py.filter_event_attribute_values(log, 'concept:name', ['ClosePR', 'MergePR'], level="case", retain=True)
        print("filter_event_attribute_values (ClosePR, MergePR)")
        log_info(log)

        # 保存文件
        log.to_csv(output_path, header=True, index=False)
        print(f"{filename} process done\n")


def test_data_preprocess(filename):
    # 初始化参数
    input_path = f"{INPUT_DATA_DIR}/{filename}"
    output_path = f"{PROCESS_DATA_DIR}/{filename}"

    # 加载事件日志
    log = load_data(input_path)
    temp_path = f"{PROCESS_TEMP_DATA_DIR}/temp-{filename}"
    # log.to_csv(temp_path, header=True, index=False)
    print("origin")
    log_info(log)

    # 剔除无效案例
    log = pm4py.filter_start_activities(log, ["ForkRepository", "CreateBranch", "SubmitCommit", "OpenPR"])
    print("filter_start_activities")
    log_info(log)

    log = pm4py.filter_end_activities(log, ["DeleteBranch", "ClosePR", "MergePR"])
    print("filter_end_activities")
    log_info(log)

    log = pm4py.filter_event_attribute_values(log, 'concept:name', ['OpenPR'], level="case", retain=True)
    print("filter_event_attribute_values (OpenPR)")
    log_info(log)

    log = pm4py.filter_event_attribute_values(log, 'concept:name', ['ClosePR', 'MergePR'], level="case",
                                              retain=True)
    print("filter_event_attribute_values (ClosePR, MergePR)")
    log_info(log)

    # 保存文件
    # log.to_csv(output_path, header=True, index=False)
    # print(f"{filename} process done\n")


if __name__ == '__main__':
    repo = "tensorflow"
    data_preprocess(repo)