import json
from datetime import datetime
from typing import List

import numpy as np
import pandas as pd

from AnomalyDetection.Config import *
from utils.mysql_utils import select_all
from utils.pr_self_utils import get_pr_attributes, get_all_pr_number_between
from utils.time_utils import cal_time_interval

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
功能：计算文件的类型
'''
def cal_file_type(filename: str):
    filetype = filename
    file_split = filename.split('.')
    if len(file_split) >= 2:
        filetype = file_split[-1]
    else:
        filetype = file_split[0]
    return filetype


'''
功能：提取文件相关的特征: 文件添加/删除行数，文件变更种类数，敏感文件变更数量
'''
def cal_file_feature(file_content: str):
    file_list = json.loads(file_content)
    change_file_type = set()

    sensitive_file_addition = 0
    sensitive_file_deletion = 0
    sensitive_file_edit_num = 0
    for file in file_list.values():
        filename = file['filename']
        filetype = cal_file_type(filename)
        change_file_type.add(filetype)
        is_sensitive_file = filetype in SENSITIVE_FILE_SUFFIX
        status = file['status']
        addition = file['additions']
        deletion = file['deletions']

        # 敏感文件特征
        if is_sensitive_file:
            sensitive_file_addition += addition
            sensitive_file_deletion += deletion
            sensitive_file_edit_num += 1

    total_file_edit_type = len(change_file_type)
    return [total_file_edit_type, sensitive_file_addition, sensitive_file_deletion, sensitive_file_edit_num]


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
    for person, group in df.groupby('author'):
        # 计算一段时间内，committer提交的commit数量, 添加的代码行总数, 删除的代码行总数
        commit_num = group.shape[0]
        total_line_addition = group['line_addition'].sum()
        total_line_deletion = group['line_deletion'].sum()
        total_file_edit_num = group['file_edit_num'].sum()

        # 计算一段时间内，committer变动的敏感文件数量、敏感文件的变动行数(添加、删除)
        file_feature = []
        for index, row in group.iterrows():
            file_content = row['file_content']
            file_feature.append(cal_file_feature(file_content))
        columns = ['total_file_edit_type', 'sensitive_file_addition', 'sensitive_file_deletion', 'sensitive_file_edit_num']
        df_file_feature = pd.DataFrame(data=file_feature, columns=columns)
        total_file_edit_type = df_file_feature['total_file_edit_type'].sum()
        sensitive_line_addition = df_file_feature['sensitive_file_addition'].sum()
        sensitive_line_deletion = df_file_feature['sensitive_file_deletion'].sum()
        sensitive_file_edit_num = df_file_feature['sensitive_file_edit_num'].sum()

        # 计算一段时间内，committer所参与的PR被合入的几率
        pr_state = []
        for pr_number in group['pr_number'].unique():
            pr_attribute = get_pr_attributes(repo, int(pr_number.__str__()))
            pr_state.append(pr_attribute['merged'])
        merge_rate = np.sum(pr_state) / len(pr_state)

        # 计算一段时间内，committer提交commit的频率(平均时间间隔)
        commit_date = group['committer_date'].tolist()
        commit_interval = cal_time_interval(commit_date)

        # 保存所有特征
        data = [person, commit_num,
                total_line_addition, total_line_deletion, total_file_edit_num, total_file_edit_type,
                sensitive_line_addition, sensitive_line_deletion, sensitive_file_edit_num,
                merge_rate, commit_interval]
        committer_feature.append(data)

    # 保存为文件
    columns = ['people', 'commit_num',
               'total_line_addition', 'total_line_deletion', 'total_file_edit_num', 'total_file_edit_type',
               'sensitive_line_addition', 'sensitive_line_deletion', 'sensitive_file_edit_num',
               'merge_rate', 'commit_interval']
    df_file = pd.DataFrame(data=committer_feature, columns=columns)
    df_file.to_csv(output_path, index=False, header=True)


if __name__ == '__main__':
    repos = ['zipkin', 'netbeans', 'opencv', 'dubbo', 'phoenix']
    start = datetime(2021, 1, 1)
    end = datetime(2022, 1, 1)
    for repo in repos:
        output_path = f"{FEATURE_DIR}/{repo}_committer_feature.csv"
        cal_committer_feature(repo, start, end, output_path)
        print(f"{repo} process done")
