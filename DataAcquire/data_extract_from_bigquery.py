import pandas as pd
import json
from utils.mysql_utils import insert_batch

repo = "tensorflow/tensorflow"
filepath = "bigquery_data/20211223.csv"
df = pd.read_csv(filepath)
df = df.loc[df['repo_name'] == repo]

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
    return v


datas = []
for index, event in df.iterrows():
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
         check(event['payload_member_id']),
         check(event['payload_member_login']),
         check(event['payload_member_type']),
         check(event['payload_member_site_admin'])
         )
    datas.append(t)

insert_batch("events", datas, repo[repo.index('/') + 1:])