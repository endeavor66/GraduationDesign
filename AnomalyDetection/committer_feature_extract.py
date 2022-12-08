import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime
from AnomalyDetection.Config import *
from utils.mysql_utils import select_all, select_one
from utils.time_utils import cal_time_delta_minutes


'''
功能：获取特定时间段内的所有pr_number
'''
def get_all_pr_number_between(repo: str, start: datetime, end: datetime) -> List:
    start_time = start.strftime('%Y-%m-%d %H:%M:%S')
    end_time = end.strftime('%Y-%m-%d %H:%M:%S')
    sql = f"select pr_number from `{repo}_self` where created_at >= '{start_time}' and created_at < '{end_time}'"
    pr_list = select_all(sql)
    pr_number_list = [x['pr_number'] for x in pr_list]
    return pr_number_list


'''
功能：获取pr_number_list包含的所有commit
'''
def get_commit_of_pr_number(repo: str, pr_number_list: List):
    pr_number_list.sort()
    start_pr = pr_number_list[0]
    end_pr = pr_number_list[-1]
    sql = f"select * from {repo}_commit where pr_number >= {start_pr} and pr_number <= {end_pr}"
    data = select_all(sql)
    return data


'''
功能：获取PR的所有属性
'''
def get_pr_attributes(repo: str, pr_number: int) -> Dict:
    sql = f"select * from `{repo}_self` where repo_name='{repo}' and pr_number={pr_number}"
    data = select_one(sql)
    return data


'''
功能：计算平均时间间隔
'''
def cal_commit_interval(commit_date: List):
    commit_date.sort()
    interval = []
    for i in range(1, len(commit_date)):
        inv = cal_time_delta_minutes(commit_date[i-1], commit_date[i])
        interval.append(inv)
    return np.nanmean(interval)


'''
功能：计算committer的所有相关特征
'''
def cal_committer_feature(repo: str, start: datetime, end: datetime, output_path: str):
    pr_number_list = get_all_pr_number_between(repo, start, end)

    # 从repo_commit表中提取所有在pr_number_list的commit数据
    commit_list = get_commit_of_pr_number(repo, pr_number_list)

    # 从commit_list中提取committer相关指标
    df = pd.DataFrame(commit_list)
    committer_feature = []
    for person, group in df.groupby('people'):
        # 计算一段时间内，committer提交的commit数量, 添加的代码行总数, 删除的代码行总数, 变动的文件数量
        commit_num = group.shape[0]
        line_addition = group['line_addition'].sum()
        line_deletion = group['line_deletion'].sum()
        file_edit_num = group['file_edit_num'].sum()

        # 计算一段时间内，committer所参与的PR被合入的几率
        pr_state = []
        for pr_number in group['pr_number'].unique():
            pr_attribute = get_pr_attributes(repo, int(pr_number.__str__()))
            pr_state.append(pr_attribute['merged'])
        merge_rate = np.sum(pr_state) / len(pr_state)

        # 计算一段时间内，committer提交commit的频率(平均时间间隔)
        commit_date = group['committer_date'].tolist()
        avg_commit_interval = cal_commit_interval(commit_date)

        # 保存所有特征
        data = [person, commit_num, line_addition, line_deletion, file_edit_num, merge_rate, avg_commit_interval]
        committer_feature.append(data)

    # 保存为文件
    df_file = pd.DataFrame(data=committer_feature)
    df_file.to_csv(output_path, index=False, header=True)


if __name__ == '__main__':
    repo = 'tensorflow'
    start = datetime(2021, 1, 1)
    end = datetime(2021, 2, 1)
    output_path = f"{FEATURE_DIR}/{repo}_committer_feature.csv"
    cal_committer_feature(repo, start, end, output_path)
