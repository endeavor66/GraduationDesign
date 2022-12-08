import pandas as pd
import json
from utils.mysql_utils import batch_insert_into_events


'''
功能：从commits中提取所有commit的SHA，拼接为字符串后返回(#连接)，格式：SHA1#SHA2#SHA3
'''
def join_commits_sha(commits: str) -> str:
    if commits is None:
        return None
    commits_dic_list = json.loads(commits)
    commit_sha = []
    for commit in commits_dic_list:
        commit_sha.append(commit['sha'])
    return "#".join(commit_sha)


'''
功能：如果v是(Nan, None, NaN, NaT), 统一转换为None; 否则返回原值
'''
def check(v):
    if pd.isna(v):
        return None
    # 如果v是字符串，且首尾带引号，去掉引号; 否则直接返回
    if isinstance(v, str) and v[0] == "\"" and v[-1] == "\"":
        v = v[1:-1]
    return v



'''
功能：从csv文件(文件路径为filepath)中提取所有事件，保存到数据库
'''
def extract_data_from_bigquery_csv(filepath: str, repo: str):
    # 1.从csv文件中筛选出repo及其fork仓的所有相关事件(形式: repo_name like '%/repo')
    df = pd.read_csv(filepath)
    df = df.loc[df.repo_name.str.endswith(repo)]
    df2 = df.drop_duplicates()
    print(f"{filepath}去除重复数据:%d, 剩余数据:%d" % ((df.shape[0] - df2.shape[0]), df2.shape[0]))
    # 2.提取关键活动
    datas = []
    for index, event in df2.iterrows():
        t = (check(event['id']),
             check(event['type']),
             check(event['public']),
             check(event['created_at']),
             check(event['actor_id']),
             check(event['actor_login']),
             check(event['repo_id']),
             check(event['repo_name']),
             check(event['payload_ref']),
             check(event['payload_ref_type']),
             check(event['payload_pusher_type']),
             check(event['payload_push_id']),
             check(event['payload_size']),
             check(event['payload_distinct_size']),
             join_commits_sha(check(event['payload_commits'])),
             check(event['payload_action']),
             check(event['payload_pr_number']),
             check(event['payload_forkee_full_name']),
             check(event['payload_changes']),
             check(event['payload_review_state']),
             check(event['payload_review_author_association']),
             check(event['payload_member_id']),
             check(event['payload_member_login']),
             check(event['payload_member_type']),
             check(event['payload_member_site_admin'])
             )
        datas.append(t)
    # 3.保存到数据库
    batch_insert_into_events(repo, datas)


if __name__ == '__main__':
    repo = "zipkin"
    from datetime import datetime, timedelta
    import os
    start = datetime(2021, 1, 1)  # 年，月，日，时，分，秒 其中年，月，日是必须的
    end = datetime(2021, 7, 1)
    index = 0
    while start < end:
        cur = start.strftime("%Y%m%d")
        filepath = f"bigquery_data/{cur}.csv"
        if not os.path.exists(filepath):
            print(f"{filepath} does not exist")
            start = start + timedelta(days=1)
            continue
        extract_data_from_bigquery_csv(filepath, repo)
        print(f"{filepath} process done")
        start = start + timedelta(days=1)
        index += 1
    print(f"共处理{index}份文件")