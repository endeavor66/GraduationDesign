import pandas as pd
import json
from datetime import datetime
from utils.mysql_utils import select_all
from utils.time_utils import time_reverse
from AnomalyDetection.Config import *


'''
功能：计算x的长度，x可能是str或list
'''
def cal_length(x) -> int:
    if pd.isna(x):
        return 0
    return len(x)


'''
功能：计算标签的数量
'''
def cal_labels(labels: str) -> int:
    if pd.isna(labels):
        return 0
    return len(json.loads(labels))


'''
功能：计算时间差，单位hours
'''
def cal_time_delta(start: datetime, end: datetime):
    if pd.isna(start) or pd.isna(end):
        return None
    delta = (end - start).components
    delta_hours = delta.days * 24 + delta.hours
    return delta_hours


'''
计算评审响应时间，单位hours
'''
def cal_review_response_time(created_at: datetime, closed_at: datetime, review_comments_content: str):
    if pd.isna(review_comments_content):
        return None
    review_list = json.loads(review_comments_content)
    if len(review_list) == 0:
        return None
    first_review_time = time_reverse(review_list[0]['created_at'])
    review_response_time = cal_time_delta(created_at, first_review_time)
    return review_response_time


'''
功能：计算repo中特定PR改动的文件类型数量
'''
def cal_changed_file_type(repo: str, pr_number: int):
    table = f"{repo.replace('-', '_')}_file"
    sql = f"select changed_file_name from `{table}` where pr_number={pr_number}"
    file_list = select_all(sql)
    file_type = set()
    for file in file_list:
        file_name = file['changed_file_name']
        file_split = file_name.split('.')
        if len(file_split) >= 2:
            ftype = file_split[-1]
        else:
            ftype = file_split[0]
        file_type.add(ftype)
    return len(file_type)


'''
功能：从repo_self表中提取PR特征
'''
def feature_extract(repo: str):
    # 从repo_self表中查询特定时间段的PR信息
    table = f"{repo.replace('-', '_')}_self"
    sql = f"select * from `{table}` where created_at >= '2021-01-01 00:00:00' and created_at < '2021-02-01 00:00:00'"
    data = select_all(sql)

    # 借助pandas对PR集合进行分析
    df = pd.DataFrame(data)
    feature = {}
    for pr_user_id, group in df.groupby(['pr_user_id']):
        # self相关特征
        pr_user_id = str(pr_user_id)
        feature[pr_user_id] = dict()

        # PR标题长度
        feature[pr_user_id]['title_len_average'] = group['title'].apply(lambda x: cal_length(x)).mean()

        # PR内容长度
        feature[pr_user_id]['body_len_average'] = group['body'].apply(lambda x: cal_length(x)).mean()

        # PR标签数量
        feature[pr_user_id]['labels_average'] = group['labels'].apply(lambda x: cal_labels(x)).mean()

        # PR包含的commit数量
        feature[pr_user_id]['commit_number_average'] = group['commit_number'].mean()

        # PR改动的文件类型数量
        group['changed_file_type'] = group['pr_number'].apply(lambda x: cal_changed_file_type(repo, x))
        feature[pr_user_id]['changed_file_type_average'] = group['changed_file_type'].mean()

        # PR改动的文件数量
        feature[pr_user_id]['changed_file_num_average'] = group['changed_file_num'].mean()

        # PR总增加代码行数
        feature[pr_user_id]['total_add_line_average'] = group['total_add_line'].mean()

        # PR总删除代码行数
        feature[pr_user_id]['total_delete_line_average'] = group['total_delete_line'].mean()

        # PR评审者响应时间：PR创建到评审者第一次做出review的时间
        group['review_response_time'] = group.apply(lambda x: cal_review_response_time(x['created_at'], x['closed_at'], x['review_comments_content']), axis=1)
        feature[pr_user_id]['review_response_time_average'] = group['review_response_time'].mean()

        # PR评论数量
        feature[pr_user_id]['comments_number_average'] = group['comments_number'].mean()

        # PR review评论数量
        feature[pr_user_id]['review_comments_number_average'] = group['review_comments_number'].mean()

        # PR提交者参与效率：项目中某用户提交PR平均创建到关闭时间间隔
        group['user_efficiency'] = group.apply(lambda x: cal_time_delta(x['created_at'], x['closed_at']), axis=1)
        feature[pr_user_id]['user_efficiency_average'] = group['user_efficiency'].mean()

        # PR提交者参与质量：项目中某用户提交的PR被合入的比例
        feature[pr_user_id]['merged_average'] = group['merged'].mean()

        print(f"pr_user_id#{pr_user_id} process done")

    # 将字典转为dataframe
    data = pd.DataFrame.from_dict(feature)
    df_feature = pd.DataFrame(data.values.T, columns=data.index, index=data.columns)
    df_feature['pr_user_id'] = data.columns
    output_path = f"{FEATURE_DIR}/{repo}_feature.csv"
    df_feature.to_csv(output_path, header=True, index=False)


if __name__ == '__main__':
    repo = "tensorflow"
    feature_extract(repo)