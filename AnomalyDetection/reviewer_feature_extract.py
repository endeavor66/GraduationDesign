import pandas as pd
import json
from typing import List
from datetime import datetime
from AnomalyDetection.Config import *
from utils.mysql_utils import select_all
from utils.time_utils import time_reverse, cal_time_delta_minutes


'''
功能：从PR中提取评审意见特征(评审意见,评审响应时间)
'''
def extract_review_info(pr_number: int, review_comments_content: str, pr_created_time: datetime):
    review_info = []
    if pd.isna(review_comments_content):
        return review_info
    review_list = json.loads(review_comments_content)
    if len(review_list) == 0:
        return review_info
    for review in review_list:
        reviewer = review['user']['login']
        review_comment = review['body']
        review_comment_created_time = time_reverse(review['created_at'])

        review_info.append([reviewer, pr_number, review_comment, review_comment_created_time, pr_created_time])
    return review_info


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
        review_info = extract_review_info(pr_number, review_comments_content, pr_created_time)
        if len(review_info) > 0:
            review_comment_list.extend(review_info)

    return review_comment_list


'''
功能：计算字符串长度
'''
def cal_str_length(s: str):
    if not pd.isna(s):
        return len(s)
    return 0


'''
功能：计算每个评审者的平均评审意见长度
'''
def cal_average_review_comment_length(review_comment_list: List):
    avg_review_comment_length_list = []
    df = pd.DataFrame(data=review_comment_list, columns=['people', 'pr_number', 'review_comment', 'review_comment_created_time', 'pr_created_time'])
    for person, group in df.groupby('people'):
        group['review_comment_length'] = group['review_comment'].apply(lambda x: cal_str_length(x))
        avg_review_comment_length = group['review_comment_length'].mean()
        avg_review_comment_length_list.append([person, avg_review_comment_length])
    return avg_review_comment_length_list


'''
功能：计算每个评审者在所有PR中的首次评审的平均响应时间
'''
def cal_average_review_response_time_helper(first_response_time_list: List):
    # 计算平均首次评审响应时间
    avg_response_time_list = []
    df = pd.DataFrame(data=first_response_time_list, columns=['people', 'pr_number', 'first_response_time'])
    for person, group in df.groupby('people'):
        avg_response_time = group['first_response_time'].mean()
        avg_response_time_list.append([person, avg_response_time])
    return avg_response_time_list


'''
功能：计算每个评审者在每个PR中的首次评审响应时间
'''
def cal_average_review_response_time(review_comment_list: List):
    df = pd.DataFrame(data=review_comment_list, columns=['people', 'pr_number', 'review_comment', 'review_comment_created_time', 'pr_created_time'])
    df['review_comment_created_time'] = pd.to_datetime(df['review_comment_created_time'])
    df['pr_created_time'] = pd.to_datetime(df['pr_created_time'])

    first_response_time_list = []
    for person, group_person in df.groupby('people'):
        for pr_number, group_pr in group_person.groupby('pr_number'):
            group_pr.sort_values(by='review_comment_created_time', inplace=True)
            pr_open_time = group_pr['pr_created_time'].iloc[0]
            review_comment_created_time = group_pr['review_comment_created_time'].iloc[0]
            first_response_time = cal_time_delta_minutes(pr_open_time, review_comment_created_time)
            first_response_time_list.append([person, pr_number, first_response_time])

    # 计算平均首次评审响应时间
    avg_response_time_list = cal_average_review_response_time_helper(first_response_time_list)
    return avg_response_time_list


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
功能：根据传入的评审活动，判断是否同意合入
'''
def is_approve(activity: str):
    if activity == 'PRReviewApprove':
        return 1
    elif activity == 'PRReviewReject' or activity == 'PRReviewComment':
        return 0
    raise Exception("invalid param, activity should be Union['PRReviewApprove', 'PRReviewReject', 'PRReviewComment']")


# 计算每个reviewer的评审通过率
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


def cal_reviewer_feature(repo: str, start: datetime, end: datetime, output_path: str):
    # 1.从repo_self表中提取所有评审相关的信息
    review_comment_list = cal_review_comment_list(repo, start, end)

    # 2.计算每个评审者的平均评审意见长度
    avg_review_comment_length_list = cal_average_review_comment_length(review_comment_list)

    # 3.计算每个评审者的平均首次评审响应时间
    avg_response_time_list = cal_average_review_response_time(review_comment_list)

    # 4.从repo_self表中提取指定时间段的所有pr_number
    pr_number_list = get_all_pr_number_between(repo, start, end)

    # 4.计算每个评审者的评审通过率
    approve_rate_list = cal_review_approve_rate(repo, pr_number_list)

    # 5.合并上述所有特征，关联值为reviewer
    df1 = pd.DataFrame(data=avg_review_comment_length_list, columns=['people', 'avg_review_comment_length'])
    df2 = pd.DataFrame(data=avg_response_time_list, columns=['people', 'avg_review_response_time'])
    df3 = pd.DataFrame(data=approve_rate_list, columns=['people', 'approve_rate'])

    df4 = pd.merge(df1, df2, how='outer', on='people')
    df5 = pd.merge(df3, df4, how='outer', on='people')

    df5.to_csv(output_path, index=False, header=True)


if __name__ == '__main__':
    repo = 'tensorflow'
    start = datetime(2021, 1, 1)
    end = datetime(2021, 2, 1)
    output_path = f"{FEATURE_DIR}/{repo}_reviewer_feature.csv"
    cal_reviewer_feature(repo, start, end, output_path)
