import pandas as pd
import pm4py
from ProcessMining.Config import *
from datetime import datetime
import os


'''
功能：根据活动推导出人员角色
'''
def cal_role(event_type: str):
    # TODO 问题1:Committer和Maintainer都有资格操作ClosePR和ReopenPR，ClosePR和ReopenPR分别该划分为哪种角色？
    # TODO 问题2:一个人可能从事多种类型的活动，如:代码提交、评审、合入。此时他的role如何确定？
    role = None
    if event_type in ["ForkRepository", "CreateBranch", "DeleteBranch", "SubmitCommit", "Revise", "OpenPR"]:
        role = "Committer"
    elif event_type in ["PRReviewApprove", "PRReviewReject", "PRReviewComment"]:
        role = "Reviewer"
    elif event_type in ["PRReviewDismiss", "ClosePR", "ReopenPR", "MergePR"]:
        role = "Maintainer"
    return role


'''
功能：对SubmitCommit进行细分: SubmitCommit(OpenPR之前), Revise(OpenPR之后)
'''
def divide_submit_commit(df: pd.DataFrame) -> pd.DataFrame:
    def divide(event_type: str, event_time: datetime, pr_open_time: datetime):
        if event_type == 'SubmitCommit' and (not pd.isna(pr_open_time)) and event_time > pr_open_time:
            return "Revise"
        return event_type
    df_new = pd.DataFrame()
    for name, group in df.groupby('CaseID'):
        open_pr_df = group.loc[group['Activity'] == 'OpenPR']
        pr_open_time = None
        if open_pr_df.shape[0] > 0:
            pr_open_time = open_pr_df['StartTimestamp'].iloc[0]
        group['Activity'] = group.apply(lambda x: divide(x['Activity'], x['StartTimestamp'], pr_open_time), axis=1)
        df_new = pd.concat([df_new, group])
    return df_new


'''
功能：读取csv文件，转换为事件日志标准格式，添加角色列，删除重复列，重新组织列的次序
'''
def load_data(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    # 删除df中的重复行(同样的时间，同样的人，干同样的事)
    df2 = df.drop_duplicates().copy()
    df2.reset_index(drop=True, inplace=True)
    print("drop line: %d, rest line: %d" % (df.shape[0] - df2.shape[0], df2.shape[0]))
    # 对SubmitCommit进行细分: SubmitCommit(OpenPR之前), Revise(OpenPR之后)
    df2 = divide_submit_commit(df2)
    # 转化为事件日志的标准格式，本质上是添加了几个新列(CaseID->case:concept:name, Activity->concept:name, StartTimestamp->time:timestamp)
    event_log = pm4py.format_dataframe(df2,
                                       case_id='CaseID',
                                       activity_key='Activity',
                                       timestamp_key='StartTimestamp')
    # 添加角色列
    # event_log['Role'] = event_log['Activity'].apply(lambda x: cal_role(x))
    # 删除重复列
    event_log.drop(columns=['CaseID', 'Activity', 'StartTimestamp'], inplace=True)
    # 重新组织列的次序
    event_log = event_log[['case:concept:name', 'concept:name', 'time:timestamp', 'People']]
    return event_log


'''
功能：打印事件日志中案例的总数量
'''
def log_info(log: pd.DataFrame):
    case_array = log['case:concept:name'].unique()
    print("rest case: %d" % len(case_array))


'''
功能：事件日志数据预处理：加载事件日志、剔除无效案例，结果保存在 process_data 目录中
'''
def data_preprocess(repo: str):
    all_scene_output_path = f"{LOG_ALL_SCENE_DIR}/{repo}.csv"
    df_all_scene = pd.DataFrame()
    for t in FILE_TYPES:
        # 初始化参数
        filename = f"{repo}_{t}.csv"
        input_path = f"{EVENT_LOG_DIR}/{filename}"
        output_path = f"{LOG_SINGLE_SCENE_DIR}/{filename}"

        # 加载事件日志
        if not os.path.exists(input_path):
            print(f"{input_path} don't exist")
            continue
        log = load_data(input_path)
        print("total case: %d" % len(log['case:concept:name'].unique()))

        # 强制性限制
        # case的起始活动限制
        log = pm4py.filter_start_activities(log, ["ForkRepository", "CreateBranch", "SubmitCommit", "OpenPR"])
        print("filter_start_activities")
        log_info(log)

        # case的结束活动限制
        log = pm4py.filter_end_activities(log, ["DeleteBranch", "ClosePR", "MergePR"])
        print("filter_end_activities")
        log_info(log)

        # case中必须包含 OpenPR
        log = pm4py.filter_event_attribute_values(log, 'concept:name', ['OpenPR'], level="case", retain=True)
        print("filter_event_attribute_values (OpenPR)")
        log_info(log)

        # case中必须包含 ClosePR/MergePR
        log = pm4py.filter_event_attribute_values(log, 'concept:name', ['ClosePR', 'MergePR'], level="case", retain=True)
        print("filter_event_attribute_values (ClosePR, MergePR)")
        log_info(log)

        # 非强制性限制
        # case中必须包含 SubmitCommit,Revise
        # log = pm4py.filter_event_attribute_values(log, 'concept:name', ['SubmitCommit', 'Revise'], level="case", retain=True)
        # print("filter_event_attribute_values (SubmitCommit,Revise)")
        # log_info(log)

        # case的长度必须大于2，目的是过滤掉 OpenPR->ClosePR/MergePR的情况
        # log = pm4py.filter_case_size(log, 3, 100)
        # print("filter_case_size, must >= 3")
        # log_info(log)

        # 过滤包含低频行为(ReopenPR, PRReviewDismiss)的案例
        log = pm4py.filter_event_attribute_values(log, 'concept:name', ['ReopenPR', 'PRReviewDismiss'], level="case", retain=False)
        print("filter_event_attribute_values (ReopenPR, PRReviewDismiss)")
        log_info(log)

        # 保存文件
        log.to_csv(output_path, header=True, index=False)
        print(f"{filename} process done\n")

        # 添加到全场景
        df_all_scene = pd.concat([df_all_scene, log], ignore_index=True)

    df_all_scene.to_csv(all_scene_output_path, index=False, header=True)


'''
功能：验证爬取处理后的event log是否有问题（CreateBranch是否会在OpenPR之后发生）
'''
def valid(repo: str):
    t = FILE_TYPES[0]
    filename = f"{repo}_{t}.csv"
    input_path = f"{EVENT_LOG_DIR}/{filename}"
    df = pd.read_csv(input_path, parse_dates=['StartTimestamp'])
    for name, group in df.groupby('CaseID'):
        create_event = group.loc[group['Activity'] == 'CreateBranch']
        openPR_event = group.loc[group['Activity'] == 'OpenPR']
        if create_event.shape[0] > 0 and openPR_event.shape[0] > 0 and create_event['StartTimestamp'].iloc[0] > openPR_event['StartTimestamp'].iloc[0]:
            print(f"pr_number#{name} 异常, CreateBranch: {create_event.iloc[0, 3]}, openPR: {openPR_event.iloc[0, 3]}")


if __name__ == '__main__':
    repos = ['zipkin', 'netbeans', 'opencv', 'dubbo', 'phoenix']
    for repo in repos:
        data_preprocess(repo)
        print(f"{repo} process done")