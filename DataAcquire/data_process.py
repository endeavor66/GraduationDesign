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

from typing import List
from utils.mysql_utils import execute_query

def search_pr_events(repo: str, pr_number: int, case_events: List) -> None:
    # 1.确定PR的(开始, 结束)时间
    table = f"{repo}_self"
    sql = f"select * from {table} where pr_number={pr_number}"
    data = execute_query(sql)
    print(data)
    if len(data) == 0:
        print("没有查询到任何数据")
        return
    pr_info = data[0]
    created_at = pr_info['created_at']
    closed_at = pr_info['closed_at']
    if created_at is None or closed_at is None:
        print(f"PR#{pr_number} 开始/结束时间不完整")
        return
    # 2.从events表中提取PR相关事件: PullRequestEvent、PullRequestReviewEvent、PullRequestReviewCommentEvent、IssueCommentEvent
    sql = f"select * from events where created_at >= '{created_at}' and created_at <= '{closed_at}'"
    data = execute_query(sql)
    # 3.从事件中提取关键属性，添加到case_events
    print("="*20)
    print(data)


search_pr_events("tensorflow", 53262, [])