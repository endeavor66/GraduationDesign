import pandas as pd
from typing import List
from ProcessMining.Config import *


'''
功能：寻找所有权限变更发生的时刻
'''
def permission_change(input_path: str, unfork_pr_list: List):
    df = pd.read_csv(input_path, parse_dates=['time:timestamp'])
    role_change = []
    for person, group in df.groupby('People'):
        group.sort_values(by='time:timestamp', inplace=True)

        # 判断是否获得过reviewer权限
        df_reviewer = group.loc[group['concept:name'].isin(['PRReviewApprove', 'PRReviewReject'])]
        if df_reviewer.shape[0] > 0:
            row = df_reviewer.iloc[0]
            role_change.append([row['People'], row['case:concept:name'], row['time:timestamp'], 'Reviewer'])

        # 判断是否获得过maintainer权限
        df_maintainer = group.loc[group['concept:name'].isin(['MergePR', 'ClosePR'])]
        if df_maintainer.shape[0] > 0:
            row = df_maintainer.iloc[0]
            role_change.append([row['People'], row['case:concept:name'], row['time:timestamp'], 'Maintainer'])

    # 筛选出所有"协作者模式"的PR，判断committer权限变更
    df = df.loc[df['case:concept:name'].isin(unfork_pr_list)]
    for person, group in df.groupby('People'):
        df_committer = group.loc[group['concept:name'].isin(['SubmitCommit', 'Revise'])]
        if df_committer.shape[0] > 0:
            row = df_committer.iloc[0]
            role_change.append([row['People'], row['case:concept:name'], row['time:timestamp'], 'Committer'])

    return role_change


'''
功能：找出所有的采用协作者模式开发的PR
'''
def cal_unfork_pr_list(repo: str):
    unfork_pr_list = []
    for scene in ['unfork_merge', 'unfork_close']:
        input_path = f"{LOG_SINGLE_SCENE_DIR}/{repo}_{scene}.csv"
        df = pd.read_csv(input_path)
        pr_list = df['case:concept:name'].unique()
        unfork_pr_list.extend(pr_list)
        del df
    return unfork_pr_list


'''
功能：权限变更识别流程自动化
'''
def auto_analysis(repo: str):
    output_path = f"{ROLE_CHANGE_DIR}/{repo}_role_change.csv"
    role_change = []

    # 获取unfork_pr_list
    unfork_pr_list = cal_unfork_pr_list(repo)

    # 读取log文件，识别权限变更信息
    input_path = f"{LOG_ALL_SCENE_DIR}/{repo}.csv"
    result = permission_change(input_path, unfork_pr_list)
    role_change.extend(result)

    # 保存为文件
    df_file = pd.DataFrame(data=role_change, columns=['people', 'change_pr_number', 'change_role_time', 'change_role'])
    df_file.to_csv(output_path, index=False, header=True)


if __name__ == '__main__':
    repo = 'dubbo'
    auto_analysis(repo)