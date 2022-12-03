import pandas as pd
import pm4py
from ProcessMining.Config import *
from datetime import datetime


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
    event_log = event_log[['case:concept:name', 'concept:name', 'time:timestamp', 'People', 'Role']]
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
    for t in FILE_TYPES:
        # 初始化参数
        filename = f"{repo}_{t}.csv"
        input_path = f"{INPUT_DATA_DIR}/{filename}"
        output_path = f"{PROCESS_DATA_DIR}/{filename}"

        # 加载事件日志
        log = load_data(input_path)
        temp_path = f"{PROCESS_TEMP_DATA_DIR}/temp-{filename}"
        log.to_csv(temp_path, header=True, index=False)
        print("total case: %d" % len(log['case:concept:name'].unique()))

        # 剔除无效案例
        log = pm4py.filter_start_activities(log, ["ForkRepository", "CreateBranch", "SubmitCommit", "OpenPR"])
        print("filter_start_activities")
        log_info(log)

        log = pm4py.filter_end_activities(log, ["DeleteBranch", "ClosePR", "MergePR"])
        print("filter_end_activities")
        log_info(log)

        log = pm4py.filter_event_attribute_values(log, 'concept:name', ['OpenPR'], level="case", retain=True)
        print("filter_event_attribute_values (OpenPR)")
        log_info(log)

        log = pm4py.filter_event_attribute_values(log, 'concept:name', ['ClosePR', 'MergePR'], level="case", retain=True)
        print("filter_event_attribute_values (ClosePR, MergePR)")
        log_info(log)

        # 保存文件
        log.to_csv(output_path, header=True, index=False)
        print(f"{filename} process done\n")


def valid(repo: str):
    t = FILE_TYPES[0]
    filename = f"{repo}_{t}.csv"
    input_path = f"{INPUT_DATA_DIR}/{filename}"
    df = pd.read_csv(input_path, parse_dates=['StartTimestamp'], infer_datetime_format=True)
    for name, group in df.groupby('CaseID'):
        create_event = group.loc[group['Activity'] == 'CreateBranch']
        openPR_event = group.loc[group['Activity'] == 'OpenPR']
        if create_event.shape[0] > 0 and openPR_event.shape[0] > 0 and create_event['StartTimestamp'].iloc[0] > openPR_event['StartTimestamp'].iloc[0]:
            print(f"pr_number#{name} 异常, CreateBranch: {create_event.iloc[0, 3]}, openPR: {openPR_event.iloc[0, 3]}")


if __name__ == '__main__':
    repo = "tensorflow"
    data_preprocess(repo)
    # valid(repo)