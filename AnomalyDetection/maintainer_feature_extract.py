import pandas as pd
import json
from typing import List
from datetime import datetime
from AnomalyDetection.Config import *
from utils.time_utils import cal_time_delta_minutes, cal_time_interval
from utils.pr_self_utils import get_pr_attributes, get_all_pr_number_between

'''
功能：根据传入的活动，判断是否为合入PR
'''
def cal_pr_state(activity: str):
    if activity == 'MergePR':
        return 1
    elif activity == 'ClosePR':
        return 0
    raise Exception("invalid param, activity should be Union['MergePR', 'ClosePR']")


'''
功能：提取maintainer相关数据
'''
def cal_maintainer_pr_data(repo: str, start: datetime, end: datetime, input_path: str):
    decision_events = ['MergePR', 'ClosePR']

    # 从log日志中筛选出位于特定时间段的所有pr
    pr_number_list = get_all_pr_number_between(repo, start, end)
    df = pd.read_csv(input_path, parse_dates=['time:timestamp'])
    df = df.loc[df['case:concept:name'].isin(pr_number_list)]

    # 计算每个maintainer对PR的响应时间(创建PR到关闭PR的时间)
    maintainer_pr_data = []
    for pr_number, group in df.groupby('case:concept:name'):
        # 获取maintainer
        maintainer_activity = group.loc[group['concept:name'].isin(decision_events)]
        maintainer = maintainer_activity['People'].iloc[0]

        # 计算PR的审核结果
        activity = maintainer_activity['concept:name'].iloc[0]
        pr_state = cal_pr_state(activity)

        # 计算PR的响应时间(操作PR的具体时间)
        operate_time = maintainer_activity['time:timestamp'].iloc[0]

        # 获取PR属性
        pr_attribute = get_pr_attributes(repo, pr_number)
        pr_open_time = pr_attribute['created_at']
        pr_close_time = pr_attribute['closed_at']

        # 获取maintainer分配的评审人数量
        requested_reviewer_list = json.loads(pr_attribute['requested_reviewers_content'])
        pr_reviewer_num = len(requested_reviewer_list)

        # 计算PR响应时间(从创建到关闭PR的时间)
        response_time = cal_time_delta_minutes(pr_open_time, pr_close_time)

        # 保存结果
        maintainer_pr_data.append([maintainer, pr_number, pr_state, pr_reviewer_num, response_time, operate_time])

    return maintainer_pr_data


'''
功能：计算maintainer的相关指标
'''
def cal_maintainer_feature(maintainer_pr_data: List, output_path: str):
    maintainer_feature = []
    df = pd.DataFrame(data=maintainer_pr_data, columns=['people', 'pr_number', 'pr_state', 'pr_reviewer_num',
                                                        'response_time', 'operate_time'])
    for person, group in df.groupby('people'):
        # 参与的PR数量
        pr_num = group.shape[0]

        # 计算一段时间内各项指标的平均值
        merge_rate = group['pr_state'].sum() / pr_num
        avg_assign_reviewer_num = group['pr_reviewer_num'].sum() / pr_num
        avg_response_time = group['response_time'].sum() / pr_num
        avg_response_interval = cal_time_interval(group['operate_time'].tolist())

        # 保存结果
        maintainer_feature.append([person, pr_num, merge_rate,
                                   avg_assign_reviewer_num, avg_response_time, avg_response_interval])

    # 保存为文件
    df_file = pd.DataFrame(data=maintainer_feature,
                           columns=['people', 'pr_num', 'merge_rate',
                                    'avg_assign_reviewer_num', 'avg_response_time', 'avg_response_interval'])
    df_file.to_csv(output_path, index=False, header=True)


if __name__ == '__main__':
    projects = ['openzipkin/zipkin', 'apache/netbeans', 'opencv/opencv', 'apache/dubbo', 'phoenixframework/phoenix',
                'ARM-software/arm-trusted-firmware', 'apache/zookeeper',
                'spring-projects/spring-framework', 'spring-cloud/spring-cloud-function',
                'vim/vim', 'gpac/gpac', 'ImageMagick/ImageMagick', 'apache/hadoop',
                'libexpat/libexpat', 'apache/httpd', 'madler/zlib', 'redis/redis', 'stefanberger/swtpm']
    start = datetime(2021, 1, 1)
    end = datetime(2022, 7, 1)
    for pro in projects:
        repo = pro.split('/')[1]
        input_path = f"{LOG_ALL_SCENE_DIR}/{repo}.csv"
        output_path = f"{FEATURE_DIR}/{repo}_maintainer_feature.csv"
        maintainer_pr_data = cal_maintainer_pr_data(repo, start, end, input_path)
        cal_maintainer_feature(maintainer_pr_data, output_path)
        print(f"repo#{repo} process done")

