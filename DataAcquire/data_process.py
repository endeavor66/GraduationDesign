import pandas as pd
import os
import json
from typing import List, Dict
from datetime import datetime, timedelta
from utils.mysql_utils import select_all, select_one
from utils.time_utils import time_reverse
from DataAcquire.Config import *

'''
功能：获取特定PR(pr_number)的创建和关闭时间(created_at, closed_at)
'''
def get_pr_attributes(repo: str, pr_number: int) -> (datetime, datetime):
    sql = f"select * from `{repo}_self` where repo_name='{repo}' and pr_number={pr_number}"
    data = select_one(sql)
    return data


'''
功能：在events表中找到payload.ref(新建分支名)==branch的CreateEvent(最接近pr_open_time的一条记录)
输入：
    repo_full_name: str -> 当前仓库
    branch: str -> head关联的分支名
    pr_open_time: datetime -> pr创建的时间
输出：
    搜索到的事件，如果没找到则返回None
'''
def get_create_branch_event(repo: str, repo_full_name: str, branch: str, pr_open_time: datetime) -> Dict:
    # 发现很多异常数据: create_event 仅在 pr_open_time 之后不到一分钟内发生，因此将 create_event 的创建时间修改提前三分钟，人工消除误差
    search_time = pr_open_time + timedelta(minutes=3)
    sql = f"select * from `{repo}_events` where repo_name='{repo_full_name}' and type='CreateEvent' and payload_ref='{branch}' and created_at < '{search_time}' ORDER BY created_at DESC LIMIT 1"
    data = select_one(sql)
    if (not pd.isna(data)) and data['created_at'] > pr_open_time:
        data['created_at'] = pr_open_time - timedelta(seconds=10)
    return data


'''
功能：在events表中找到payload.ref(新建分支名)==branch的DeleteEvent(最接近pr_close_time的一条记录)
输入：
    repo: str -> 当前仓库
    branch: str -> head关联的分支名
    pr_close_time: datetime -> pr关闭的时间
输出：
    搜索到的事件，如果没找到则返回None
'''
def get_delete_branch_event(repo: str, repo_full_name: str, branch: str, pr_close_time: datetime) -> Dict:
    sql = f"select * from `{repo}_events` where repo_name='{repo_full_name}' and type='DeleteEvent' and payload_ref='{branch}' and created_at > '{pr_close_time}' ORDER BY created_at ASC LIMIT 1"
    data = select_one(sql)
    return data


'''
功能：搜索当前PR关联的head仓中特定branch的创建&删除事件
'''
def get_branch_event(repo: str, pr_attributes: Dict) -> List:
    head_repo_full_name = pr_attributes['head_repo_full_name']
    head_ref = pr_attributes['head_ref']
    pr_open_time = pr_attributes['created_at']
    pr_close_time = pr_attributes['closed_at']
    branch_events = []
    # 搜索CreateEvent, DeleteEvent
    if (not pd.isna(head_repo_full_name)) and (not pd.isna(head_ref)):
        create_event = get_create_branch_event(repo, head_repo_full_name, head_ref, pr_open_time)
        delete_event = get_delete_branch_event(repo, head_repo_full_name, head_ref, pr_close_time)
        if not pd.isna(create_event):
            branch_events.append(create_event)
        if not pd.isna(delete_event):
            branch_events.append(delete_event)
    return branch_events


'''
功能：搜索当前PR关联的fork仓的fork事件（前提是当前PR来自fork仓）
'''
def get_fork_event(owner: str, repo: str, pr_attributes: Dict) -> Dict:
    head_repo_fork = pr_attributes['head_repo_fork']
    head_repo_full_name = pr_attributes['head_repo_full_name']
    pr_open_time = pr_attributes['created_at']
    # 如果是fork仓，则搜索ForkEvent
    fork_event = None
    if (not pd.isna(head_repo_fork)) and head_repo_fork and (not pd.isna(head_repo_full_name)):
        sql = f"select * from `{repo}_events` where repo_name='{owner}/{repo}' and type='ForkEvent' and payload_forkee_full_name='{head_repo_full_name}' and created_at < '{pr_open_time}' ORDER BY created_at DESC LIMIT 1"
        fork_event = select_one(sql)
    return fork_event


'''
功能：获取PR中最早的commit提交时间
'''
def get_first_commit_time(commit_content: str) -> datetime:
    commit_dic_list = json.loads(commit_content)
    first_commit_time = datetime.now()
    for commit in commit_dic_list:
        # 初始化一个event
        try:
            commit_time = time_reverse(commit['commit']['committer']['date'])
        except:
            commit_time = None
        if (not pd.isna(commit_time)) and commit_time < first_commit_time:
            first_commit_time = commit_time
    return first_commit_time


'''
功能：从events表中提取特定时间段的PushEvent(PR中包含的第一个commit的提交时间 <= PushEvent <= PR关闭的时间)
'''
def get_push_event(repo: str, pr_attributes: dict) -> List:
    # 条件一:PushEvent的时间段
    start_time = get_first_commit_time(pr_attributes['commit_content'])
    end_time = pr_attributes['closed_at']
    if pd.isna(start_time) or pd.isna(end_time):
        print(f"无法确定PushEvent的搜索起止时间段, start_time:{start_time}, end_time:{end_time}")
        return []
    # 条件二:PushEvent的关联分支
    head_repo_full_name = pr_attributes['head_repo_full_name']
    head_ref = pr_attributes['head_ref']
    if pd.isna(head_repo_full_name) or pd.isna(head_ref):
        return []
    # 根据上述条件查询PushEvent
    sql = f"""select * from `{repo}_events` 
                where repo_name='{head_repo_full_name}' 
                and type='PushEvent' 
                and (payload_ref='{head_ref}' or payload_ref='refs/heads/{head_ref}'
                and created_at >= '{start_time}' and created_at <= '{end_time}')
           """
    push_events = select_all(sql)
    return push_events


'''
功能：从events表中提取特定PR(pr_number)的相关事件
'''
def get_pr_events(owner: str, repo: str, pr_number: int) -> List:
    sql = f"select * from `{repo}_events` where repo_name='{owner}/{repo}' and payload_pr_number={pr_number} and (type='PullRequestEvent' OR type='PullRequestReviewEvent' OR type='PullRequestReviewCommentEvent')"
    data = select_all(sql)
    return data


'''
功能：从events中提取特定仓库的特定PR的关联事件
'''
def search_pr_events(owner: str, repo: str, pr_number: int, pr_attributes: dict) -> List:
    # 存储一次完整PR涉及的事件集合
    pr_events = []
    # 1.从events表中提取ForkEvent
    fork_event = get_fork_event(owner, repo, pr_attributes)
    if not pd.isna(fork_event):
        pr_events.append(fork_event)
    # 2.从events表中提取BranchEvent(CreateEvent,DeleteEvent)
    branch_events = get_branch_event(repo, pr_attributes)
    pr_events.extend(branch_events)
    # 3.从events表中提取PushEvent
    push_events = get_push_event(repo, pr_attributes)
    pr_events.extend(push_events)
    # 4.从events表中提取PR相关事件: PullRequestEvent、PullRequestReviewEvent、PullRequestReviewCommentEvent、IssueCommentEvent
    events = get_pr_events(owner, repo, pr_number)
    pr_events.extend(events)
    # 5.保存为中间文件temp_data
    df = pd.DataFrame(data=pr_events)
    df['pr_number'] = pr_number
    filepath = f"{TEMP_DATA_DIR}/{repo}.csv"
    header = not os.path.exists(filepath)
    df.to_csv(filepath, header=header, index=False, mode='a')
    return pr_events


'''
功能：对PR关联事件进行加工，转换为事件日志标准格式，并保存在event_log_data目录中
'''
def process_pr_events(pr_events: List, pr_state: bool, pr_number: int, filepath: str) -> None:
    # 1.细化事件类型
    for event in pr_events:
        if event['type'] == 'ForkEvent':
            event['type'] = 'ForkRepository'
            event['payload_pr_number'] = pr_number
        elif event['type'] == 'CreateEvent':
            event['type'] = 'CreateBranch'
            event['payload_pr_number'] = pr_number
        elif event['type'] == 'PushEvent':
            event['type'] = 'SubmitCommit'
            event['payload_pr_number'] = pr_number
        elif event['type'] == 'DeleteEvent':
            event['type'] = 'DeleteBranch'
            event['payload_pr_number'] = pr_number
        elif event['type'] == 'PullRequestEvent':
            if event['payload_action'] == 'opened':
                event['type'] = 'OpenPR'
            elif event['payload_action'] == 'reopened':
                event['type'] = 'ReopenPR'
            elif event['payload_action'] == 'closed' and pr_state == 1:
                event['type'] = 'MergePR'
            elif event['payload_action'] == 'closed' and pr_state == 0:
                event['type'] = 'ClosePR'
        elif event['type'] == 'PullRequestReviewEvent':
            if event['payload_review_state'] == 'approved':
                event['type'] = 'PRReviewApprove'
            elif event['payload_review_state'] == 'changes_requested':
                event['type'] = 'PRReviewReject'
            elif event['payload_review_state'] == 'commented':
                event['type'] = 'PRReviewComment'
            elif event['payload_review_state'] == 'dismissed':
                event['type'] = 'PRReviewDismiss'
        # PullRequestReviewCommentEvent 和 PullRequestReviewEvent.review_state='commented'视作相同类型的活动
        elif event['type'] == 'PullRequestReviewCommentEvent':
            event['type'] = 'PRReviewComment'
    # 2.提取事件日志需要的列
    df = pd.DataFrame(data=pr_events)
    df.rename(columns={"payload_pr_number": "CaseID", "created_at": "StartTimestamp", "type": "Activity",
                       "actor_login": "People"}, inplace=True)
    df = df[["CaseID", "StartTimestamp", "Activity", "People"]]
    # 3.保存为文件
    header = not os.path.exists(filepath)
    df.to_csv(filepath, header=header, index=False, mode='a')


'''
功能：根据PR的类型，选择不同的文件路径保存
'''
def get_filepath(repo: str, pr_state: bool, is_fork: bool) -> str:
    filepath = f"{EVENT_LOG_DIR}/{repo}"
    if is_fork and pr_state:
        filepath = filepath + "_fork_merge.csv"
    elif is_fork and (not pr_state):
        filepath = filepath + "_fork_close.csv"
    elif (not is_fork) and pr_state:
        filepath = filepath + "_unfork_merge.csv"
    elif (not is_fork) and (not pr_state):
        filepath = filepath + "_unfork_close.csv"
    return filepath


'''
功能：全流程自动化
'''
def auto_process(owner: str, repo: str, pr_number: int):
    # 1.获取PR所有属性
    pr_attributes = get_pr_attributes(repo, pr_number)
    if pr_attributes is None:
        print(f"PR#{pr_number},数据库中没有查询到PR信息")
        return
    # 2.搜索PR所有关联事件
    pr_events = search_pr_events(owner, repo, pr_number, pr_attributes)
    pr_state = pr_attributes['merged']
    is_fork = pr_attributes['head_repo_fork']
    # 3.剔除无效PR(没有明确的合入状态或fork信息)
    if pd.isna(pr_state) or pd.isna(is_fork):
        print(f"PR#{pr_number},merged或head_repo_fork字段为None")
        return
    if len(pr_events) == 0:
        print(f"PR#{pr_number},没有找到相关事件")
        return
    # 4.加工PR，从中提取关键属性
    filepath = get_filepath(repo, pr_state, is_fork)
    process_pr_events(pr_events, pr_state, pr_number, filepath)
    print(f"PR#{pr_number} process done")


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


if __name__ == '__main__':
    owner = "apache"
    repo = "dubbo"
    start = datetime(2021, 1, 1)
    end = datetime(2021, 7, 1)
    pr_number_list = get_all_pr_number_between(repo, start, end)
    total = len(pr_number_list)
    index = 1
    for pr_number in pr_number_list:
        auto_process(owner, repo, pr_number)
        print(f"{index}/{total}")
        index += 1
    # auto_process(repo, 50323)
