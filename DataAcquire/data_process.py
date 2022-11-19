"""
数据加工步骤
1、通过Pulls API获取PR信息（PR的创建/关闭/合入时间，PR合入者信息、PR评审人列表、PR的commits列表）
2、根据PR的创建时间(create_at)和源仓信息(PR关联的仓库名head.repo.full_name、PR关联的分支head.ref)：
	2.1)如果是同仓库PR(没有Fork)，那么从数据库表events向前回溯，找到createEvent(分支创建信息)
	2.2)如果是其他仓库PR(有Fork)，那么从数据库表events向前回溯，找到forkEvent(Fork仓库信息)
3、根据PR包含的commits_url，调用Git API获取PR包含的所有commit信息（committer，创建时间）
4、根据PR包含的时间点信息（create_at/close_at/merge_at），确定时间段，从events中提取 PullRequestEvent、PullRequestReviewEvent、PullRequestReviewCommentEvent、IssueCommentEvent
5、根据PR包含的 merged_by 获得合入信息(合入者, 合入时间)。合入结果可以通过merged_at是否为NULL 或 merged 字段来判断
"""

import pandas as pd
import os
from typing import List
from datetime import datetime
from utils.mysql_utils import select_all, select_one

EVENT_LOG_DATA_DIR = "event_log_data"
TEMP_DATA_DIR = f"{EVENT_LOG_DATA_DIR}/temp_data"

if not os.path.exists(TEMP_DATA_DIR):
    os.makedirs(TEMP_DATA_DIR)


'''
功能：获取特定PR(pr_number)的创建和关闭时间(created_at, closed_at)
'''
def get_pr_attributes(repo: str, pr_number: int) -> (datetime, datetime):
    shortname = repo[repo.index('/')+1:]
    table = f"{shortname}_self"
    sql = f"select * from {table} where repo_name='{shortname}' and pr_number={pr_number}"
    data = select_one(sql)
    return data


'''
功能：在events表中找到payload.forkee.full_name(克隆仓名)==repo的ForkEvent
输入：
    repo: str -> 当前仓库
    created_at: datetime -> PR被创建的时间
    head_repo_full_name: str -> head关联的仓库名
输出：
    搜索到的事件，如果没找到则返回None
'''
def get_fork_event(repo: str, head_repo_full_name: str) -> List:
    sql = f"select * from events where repo_name='{repo}' and type='ForkEvent' and payload_forkee_full_name='{head_repo_full_name}'"
    data = select_all(sql)
    return data


'''
功能：在events表中找到payload.ref(新建分支名)==branch的CreateEvent和DeleteEvent
输入：
    repo: str -> 当前仓库
    branch: str -> head关联的分支名
输出：
    搜索到的事件，如果没找到则返回None
'''
def get_branch_event(repo: str, branch: str) -> List:
    sql = f"select * from events where repo_name='{repo}' and (type='CreateEvent' or type='DeleteEvent') and payload_ref='{branch}'"
    data = select_all(sql)
    return data


'''
功能：从PR创建时间开始向前回溯，寻找:
    1) 如果PR关联了Fork仓，则搜索ForkEvent(fork仓库事件)
    2) 否则，搜索CreateEvent(创建分支事件)
输入:
    head_repo_fork: bool -> head关联仓库是否为Fork仓
    head_repo_full_name: str -> head关联仓库名
    head_ref:str -> head关联分支
输出：
    1) 如果没有fork信息，返回None
    2) 否则，返回搜索到的事件
'''
def get_fork_or_branch_event(repo: str, head_repo_fork: bool, head_repo_full_name: str, head_ref: str) -> List:
    # 如果不是fork仓，则搜索createEvent
    if (head_repo_fork is None) or (head_repo_fork == False):
        event = get_branch_event(repo, head_ref)
    # 如果是fork仓，则搜索forkEvent
    elif head_repo_fork:
        event = get_fork_event(repo, head_repo_full_name)
    return event


'''
功能：从events表中提取特定PR(pr_number)的相关事件
'''
def get_pr_events(repo: str, pr_number: int) -> List:
    sql = f"select * from events where repo_name='{repo}' and payload_pr_number={pr_number} and (type='PullRequestEvent' OR type='PullRequestReviewEvent' OR type='PullRequestReviewCommentEvent')"
    data = select_all(sql)
    return data


'''
功能：从events中提取特定仓库的特定PR的关联事件
'''
def search_pr_events(repo: str, pr_number: int, pr_attributes: dict) -> List:
    # 存储一次完整PR涉及的事件集合
    pr_events = []
    # 1.从events表中提取forkEvent或createEvent
    event = get_fork_or_branch_event(repo, pr_attributes['head_repo_fork'], pr_attributes['head_repo_full_name'], pr_attributes['head_ref'])
    if event is not None:
        pr_events.extend(event)
    # 2.从events表中提取PR相关事件: PullRequestEvent、PullRequestReviewEvent、PullRequestReviewCommentEvent、IssueCommentEvent
    events = get_pr_events(repo, pr_number)
    pr_events.extend(events)
    # 3.保存为中间文件temp_data
    df = pd.DataFrame(data=pr_events)
    shortname = repo[repo.index('/') + 1:]
    filepath = f"{TEMP_DATA_DIR}/{shortname}.csv"
    header = not os.path.exists(filepath)
    df.to_csv(filepath, header=header, index=False, mode='a')
    return pr_events


'''
功能：对PR关联事件进行加工，转换为事件日志标准格式，并保存在event_log_data目录中
'''
def process_pr_events(pr_events: List, pr_state: bool, pr_number: int):
    # 1.细化事件类型
    for event in pr_events:
        if event['type'] == 'ForkEvent':
            event['type'] = 'ForkRepository'
            event['payload_pr_number'] = pr_number
        elif event['type'] == 'CreateEvent':
            event['type'] = 'CreateBranch'
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
        # PullRequestReviewCommentEvent 和 PullRequestReviewEvent.review_state='commented'视作相同类型的活动
        elif event['type'] == 'PullRequestReviewCommentEvent':
            event['type'] = 'PRReviewComment'
    # 2.提取事件日志需要的列
    df = pd.DataFrame(data=pr_events)
    df.rename(columns={"payload_pr_number": "CaseID", "created_at": "StartTimestamp", "type": "Activity", "actor_login": "People"}, inplace=True)
    df = df[["CaseID", "StartTimestamp", "Activity", "People"]]
    # 3.保存为文件
    shortname = repo[repo.index('/')+1:]
    filepath = f"{EVENT_LOG_DATA_DIR}/{shortname}.csv"
    header = not os.path.exists(filepath)
    df.to_csv(filepath, header=header, index=False, mode='a')


'''
功能：全流程自动化
'''
def auto_process(repo: str, pr_number: int):
    # 1.获取特定PR的状态，是否合入
    pr_attributes = get_pr_attributes(repo, pr_number)
    if pr_attributes is None:
        print(f"PR#{pr_number},数据库中没有查询到PR信息")
        return
    pr_events = search_pr_events(repo, pr_number, pr_attributes)
    pr_state = pr_attributes['merged']
    process_pr_events(pr_events, pr_state, pr_number)


if __name__ == '__main__':
    repo = "tensorflow/tensorflow"
    for pr_number in range(53259, 53596):
        auto_process(repo, pr_number)

