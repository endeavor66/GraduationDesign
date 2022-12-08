import pandas as pd
import numpy as np
import json
from typing import List
from datetime import datetime
from AnomalyDetection.Config import *
from utils.mysql_utils import select_all
from utils.time_utils import time_reverse, cal_time_delta_minutes
from utils.pr_self_utils import get_all_pr_number_between


'================= 第一部分 对repo_self表中提取评审信息=================='
'''
功能：从repo_self表中提取所有评审相关的信息
'''
def cal_review_comment_list(repo: str, start: datetime, end: datetime):
    # 从repo_self表中查询特定时间段的PR信息
    table = f"{repo}_self"
    sql = f"select * from `{table}` where created_at >= '{start}' and created_at < '{end}'"
    data = select_all(sql)

    # 借助pandas对PR集合进行分析
    review_comment_list = []
    df = pd.DataFrame(data)
    for index, row in df.iterrows():
        pr_number = row['pr_number']
        pr_created_time = row['created_at']
        review_comments_content = row['review_comments_content']
        # 提取所有评审意见
        for review in json.loads(review_comments_content):
            reviewer = review['user']['login']
            review_comment = review['body']
            review_comment_created_time = time_reverse(review['created_at'])
            # 保存结果
            review_comment_list.append([reviewer, pr_number, review_comment, review_comment_created_time, pr_created_time])

    return review_comment_list


'''
功能：计算字符串长度
'''
def cal_str_length(s: str):
    if not pd.isna(s):
        return len(s)
    return 0


'''
功能：计算评审意见长度、首次评审响应时间
'''
def process_review_comment_list(review_comment_list: List):
    columns = ['reviewer', 'pr_number', 'review_comment', 'review_comment_created_time', 'pr_created_time']

    # 借助pandas进行分析，添加两列: 评审意见长度、首次评审响应时间
    df = pd.DataFrame(data=review_comment_list, columns=columns)
    df['review_comment_length'] = df['review_comment'].apply(lambda x: cal_str_length(x))
    df['review_response_time'] = df.apply(lambda x: cal_time_delta_minutes(x['pr_created_time'], x['review_comment_created_time']), axis=1)

    # 依次计算每个reviewer的特征
    reviewer_feature = []
    for person, group in df.groupby('reviewer'):
        # 发表评审的总次数
        review_num = group.shape[0]

        # 参与的PR数量
        pr_num = len(group['pr_number'].unique())

        # 计算评审意见长度的平均值
        avg_review_comment_length = group['review_comment_length'].mean()

        # 计算首次评审响应时间的平均值，在每个PR中的平均评审次数
        review_num_list = []
        first_response_time_list = []
        for pr_number, group_pr in group.groupby('pr_number'):
            group_pr.sort_values(by='review_response_time', inplace=True)
            resp_time = group_pr['review_response_time'].iloc[0]
            first_response_time_list.append(resp_time)
            review_num_list.append(group_pr.shape[0])
        avg_review_response_time, avg_review_num = 0, 0
        if len(review_num_list) > 0:
            avg_review_response_time = np.nanmean(first_response_time_list)
            avg_review_num = np.nanmean(review_num_list)

        # 保存结果
        reviewer_feature.append([person, pr_num, review_num, avg_review_num, avg_review_comment_length, avg_review_response_time])

    return reviewer_feature


'================= 第二部分 从log日志提取评审信息 ======================'
'''
功能：根据传入的评审活动，判断是否同意合入
'''
def is_approve(activity: str):
    if activity == 'PRReviewApprove':
        return 1
    elif activity == 'PRReviewReject' or activity == 'PRReviewComment':
        return 0
    raise Exception("invalid param, activity should be Union['PRReviewApprove', 'PRReviewReject', 'PRReviewComment']")


'''
功能：计算每个reviewer的评审通过率
'''
def cal_review_approve_rate(repo: str, pr_number_list: List):
    review_events = ['PRReviewApprove', 'PRReviewReject', 'PRReviewComment']
    input_path = f"{LOG_ALL_SCENE_DIR}/{repo}.csv"
    df = pd.read_csv(input_path)
    df = df.loc[df['case:concept:name'].isin(pr_number_list)]

    # 计算评审通过率
    approve_rate_list = []
    df_review = df.loc[df['concept:name'].isin(review_events)]
    for person, group in df_review.groupby('People'):
        group['IsApprove'] = group['concept:name'].apply(lambda x: is_approve(x))
        # 评审通过率
        approve_rate = group['IsApprove'].sum() / group.shape[0]
        approve_rate_list.append([person, approve_rate])

    return approve_rate_list


'=============== 第三部分 综合前两个部分提取的数据，计算评审者的特征 ===================='
def cal_reviewer_feature(repo: str, start: datetime, end: datetime, output_path: str):
    # 1.从repo_self表中提取所有评审相关的信息
    review_comment_list = cal_review_comment_list(repo, start, end)

    # 2.计算每个评审者的评审意见长度，评审响应时间
    review_comment_feature = process_review_comment_list(review_comment_list)

    # 3.从repo_self表中提取指定时间段的所有pr_number
    pr_number_list = get_all_pr_number_between(repo, start, end)

    # 4.计算每个评审者的评审通过率
    approve_rate_list = cal_review_approve_rate(repo, pr_number_list)

    # 5.合并上述所有特征，关联值为reviewer
    df1 = pd.DataFrame(data=review_comment_feature, columns=['reviewer', 'pr_num', 'review_num', 'avg_review_num', 'avg_review_comment_length', 'avg_review_response_time'])
    df2 = pd.DataFrame(data=approve_rate_list, columns=['reviewer', 'approve_rate'])
    df3 = pd.merge(df1, df2, how='outer', on='reviewer')

    # 保存为结果
    df3.to_csv(output_path, index=False, header=True)


if __name__ == '__main__':
    repo = 'tensorflow'
    start = datetime(2021, 1, 1)
    end = datetime(2021, 2, 1)
    output_path = f"{FEATURE_DIR}/{repo}_reviewer_feature.csv"
    cal_reviewer_feature(repo, start, end, output_path)
