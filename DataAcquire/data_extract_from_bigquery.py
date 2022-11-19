import pandas as pd
import json
from utils.mysql_utils import insert_batch


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
功能：从csv文件(文件路径为filepath)中提取repo所有相关的事件，保存到数据库
'''
def extract_data_from_bigquery_csv(repo: str, filepath: str):
    # 1.从csv文件中筛选出repo所有相关事件
    df = pd.read_csv(filepath)
    df = df.loc[df['repo_name'] == repo]
    # 2.提取关键活动
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
             check(event['payload_review_state']),
             check(event['payload_review_author_association']),
             check(event['payload_member_id']),
             check(event['payload_member_login']),
             check(event['payload_member_type']),
             check(event['payload_member_site_admin'])
             )
        datas.append(t)
    # 3.保存到数据库
    insert_batch("events", datas, repo[repo.index('/') + 1:])


if __name__ == '__main__':
    repo = "tensorflow/tensorflow"
    filepath = "bigquery_data/20211210.csv"
    extract_data_from_bigquery_csv(repo, filepath)