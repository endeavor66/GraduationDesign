import pandas as pd
from datetime import datetime, timedelta
from typing import List
from DataAcquire.Config import *
from DataAcquire.data_process import get_all_pr_number_between

'''
功能：修订2021年1月到3月中CreateBranch的时间
'''
def cal_new_time(event_type: str, branch_create_time: datetime, pr_open_time: datetime) -> datetime:
    new_time = branch_create_time
    if event_type == 'CreateBranch':
        # 把时间加回去
        new_time = branch_create_time + timedelta(minutes=3)
        if (not pd.isna(pr_open_time)) and new_time > pr_open_time:
            # 修订时间
            new_time = pr_open_time - timedelta(seconds=10)
    return new_time


def modify_create_event_time(input_path: str, output_path: str, pr_number_list: List):
    df = pd.read_csv(input_path, parse_dates=['StartTimestamp'], infer_datetime_format=True)
    df_excel = pd.DataFrame()
    for name, group in df.groupby('CaseID'):
        if name not in pr_number_list:
            group['new_time'] = group['StartTimestamp']
            df_excel = pd.concat([df_excel, group])
            continue
        open_pr_df = group.loc[group['Activity'] == 'OpenPR']
        pr_open_time = None
        if open_pr_df.shape[0] > 0:
            pr_open_time = open_pr_df.iloc[0, 1]
        group['new_time'] = group.apply(lambda x: cal_new_time(x['Activity'], x['StartTimestamp'], pr_open_time), axis=1)
        group = group.sort_values(by='new_time')
        df_excel = pd.concat([df_excel, group])
        print(f"{name} process done")

    df_excel.drop(columns=['StartTimestamp'], inplace=True)
    df_excel.rename(columns={'new_time': 'StartTimestamp'}, inplace=True)
    df_excel.to_csv(output_path, index=False, header=True)


if __name__ == '__main__':
    repo = 'tensorflow'
    # 1-3月份未处理
    start = datetime(2021, 1, 1)
    end = datetime(2021, 4, 1)
    pr_list = get_all_pr_number_between("tensorflow/tensorflow", start, end)
    pr_number_list = [x['pr_number'] for x in pr_list]
    for t in FILE_TYPES:
        # 初始化参数
        input_path = f"{EVENT_LOG_DIR}/{repo}_{t}.csv"
        output_path = f"{EVENT_LOG_DIR}/{repo}_{t}_out.csv"
        modify_create_event_time(input_path, output_path, pr_number_list)
        print(f"{input_path} process done \n")