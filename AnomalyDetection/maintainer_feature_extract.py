import pandas as pd
from typing import List
from AnomalyDetection.Config import *
from utils.time_utils import cal_time_delta_minutes


'''
功能：根据传入的活动，判断是否为合入PR
'''
def is_merge(activity: str):
    if activity == 'MergePR':
        return 1
    elif activity == 'ClosePR':
        return 0
    raise Exception("invalid param, activity should be Union['MergePR', 'ClosePR']")


def cal_average_response_time(response_time_list: List):
    avg_response_time_list = []
    df = pd.DataFrame(data=response_time_list, columns=['people', 'pr_number', 'response_time'])
    for person, group in df.groupby('people'):
        avg_response_time = group['response_time'].mean()
        avg_response_time_list.append([person, avg_response_time])
    return avg_response_time_list


'''
功能：计算项目维护者相关特征
'''
def cal_maintainer_feature(repo: str, output_path: str):
    decision_events = ['MergePR', 'ClosePR']
    input_path = f"{LOG_ALL_SCENE_DIR}/{repo}.csv"
    df = pd.read_csv(input_path, parse_dates=['time:timestamp'])

    # 计算每个maintainer的PR合入批准率
    merge_rate_list = []
    df_maintain = df.loc[df['concept:name'].isin(decision_events)]
    for person, group in df_maintain.groupby('People'):
        group['IsMerge'] = group['concept:name'].apply(lambda x: is_merge(x))
        # PR合入批准率
        merge_rate = group['IsMerge'].sum() / group.shape[0]
        merge_rate_list.append([person, merge_rate])

    # 计算每个maintainer对PR的响应时间(创建PR到关闭PR的时间)
    response_time_list = []
    for pr_number, group in df.groupby('case:concept:name'):
        pr_open = group.loc[group['concept:name'] == 'OpenPR']
        pr_close = group.loc[group['concept:name'].isin(decision_events)]
        pr_open_time = pr_open['time:timestamp'].iloc[0]
        pr_close_time = pr_close['time:timestamp'].iloc[0]
        maintainer = pr_close['People'].iloc[0]
        # PR响应时间
        response_time = cal_time_delta_minutes(pr_open_time, pr_close_time)
        response_time_list.append([maintainer, pr_number, response_time])
    avg_response_time_list = cal_average_response_time(response_time_list)

    # 借助pandas合并merge_rate_list和response_time_list
    df_merge_rate = pd.DataFrame(data=merge_rate_list, columns=['people', 'merge_rate'])
    df_response_time = pd.DataFrame(data=avg_response_time_list, columns=['people', 'response_time'])

    df_feature = pd.merge(df_merge_rate, df_response_time, on=['people'], how='outer')
    df_feature.to_csv(output_path, index=False, header=True)


if __name__ == '__main__':
    repo = 'tensorflow'
    output_path = f"{FEATURE_DIR}/{repo}_maintainer_feature.csv"
    cal_maintainer_feature(repo, output_path)

