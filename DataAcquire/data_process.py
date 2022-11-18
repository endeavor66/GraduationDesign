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
from typing import List
from datetime import datetime
from utils.mysql_utils import select_all, select_one

'''
功能：获取特定PR(pr_number)的创建和关闭时间(created_at, closed_at)
'''
def get_pr_attributes(repo: str, pr_number: int) -> (datetime, datetime):
    table = f"{repo}_self"
    sql = f"select * from {table} where pr_number={pr_number}"
    data = select_one(sql)
    print(data)
    return data


'''
功能：在events表中找到payload.forkee.full_name(克隆仓名)==repo的ForkEvent
输入：
    repo: str -> head关联的仓库名
    created_at: datetime -> PR被创建的时间
输出：
    搜索到的事件，如果没找到则返回None
'''
def get_fork_event(repo: str) -> List:
    sql = f"select * from events where type='ForkEvent' and payload_forkee_full_name='{repo}'"
    data = select_all(sql)
    return data


'''
功能：在events表中找到payload.ref(新建分支名)==branch的CreateEvent和DeleteEvent
输入：
    branch: str -> head关联的分支名
    created_at: datetime -> PR被创建的时间
输出：
    搜索到的事件，如果没找到则返回None
'''
def get_branch_event(branch: str) -> List:
    sql = f"select * from events where (type='CreateEvent' or type='DeleteEvent') and payload_ref='{branch}'"
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
def get_fork_or_branch_event(head_repo_fork: bool, head_repo_full_name: str, head_ref: str) -> List:
    # 如果没有fork信息，直接返回None
    if head_repo_fork is None:
        return None
    # 如果是fork仓，则搜索forkEvent
    if head_repo_fork:
        event = get_fork_event(head_repo_full_name)
    # 如果不是fork仓，则搜索createEvent
    else:
        event = get_branch_event(head_ref)
    return event


'''
功能：从events表中提取特定PR(pr_number)的相关事件
'''
def get_pr_events(pr_number: int) -> List:
    sql = f"select * from events where payload_pr_number={pr_number} and (type='PullRequestEvent' OR type='PullRequestReviewEvent' OR type='PullRequestReviewCommentEvent')"
    data = select_all(sql)
    return data


def search_pr_events(repo: str, pr_number: int) -> List:
    # 1.获取特定PR的相关信息
    pr_attributes = get_pr_attributes(repo, pr_number)
    if pr_attributes is None:
        print(f"PR#{pr_number},数据库中没有查询到PR信息")
        return
    # 存储一次完整PR涉及的事件集合
    pr_events = []
    # 2.从events表中提取forkEvent或createEvent
    event = get_fork_or_branch_event(pr_attributes['head_repo_fork'], pr_attributes['head_repo_full_name'], pr_attributes['head_ref'])
    if event is not None:
        pr_events.extend(event)
    # 3.从events表中提取PR相关事件: PullRequestEvent、PullRequestReviewEvent、PullRequestReviewCommentEvent、IssueCommentEvent
    events = get_pr_events(pr_number)
    pr_events.extend(events)
    return pr_events


repo = "tensorflow"
pr_events = search_pr_events(repo, 53496)
print(pr_events)


df = pd.DataFrame(data=pr_events)
filepath = f"event_log_data/{repo}.csv"
#从df中筛选出需要的字段
df.to_csv(filepath, header=True, index=False, mode='a')
