import requests
import pandas as pd
import json
from datetime import datetime
from typing import List
from utils.access_key import get_token
from utils.mysql_utils import select_all, insert_into_commit
from utils.time_utils import time_reverse


# 初始化变量
access_token = get_token()
headers = {'Authorization': 'token ' + access_token}


'''
功能：将对象数组转为json字符串保存
'''
def list_to_json(temp_list: List):
    dic = {}
    for i in range(0, len(temp_list)):
        dic[i] = temp_list[i]
    return json.dumps(dic)


'''
功能：借助GitHub Commit Api爬取commit数据，并保存到数据库
'''
def crawl_commit(owner: str, repo: str, pr_number: int, commit_sha_list: List):
    for sha in commit_sha_list:
        url = f"https://api.github.com/repos/{owner}/{repo}/commits/{sha}"
        print(url)
        response = requests.get(url, headers=headers)
        # 如果返回的状态码以2开头，则说明正常此时去写入到数据库中即可
        if response.status_code >= 200 and response.status_code < 300:
            commit = response.json()
            author, author_email, author_date, committer, committer_email, committer_date, message = None, None, None, None, None, None, None
            # 从中提取需要的属性，写入到数据库
            if not pd.isna(commit['author']) and len(commit['author']) > 0:
                author = commit['author']['login']
            if not pd.isna(commit['committer']) and len(commit['committer']) > 0:
                committer = commit['committer']['login']
            if not pd.isna(commit['commit']) and len(commit['commit']) > 0:
                message = commit['commit']['message']
                author_date = time_reverse(commit['commit']['author']['date'])
                author_email = commit['commit']['author']['email'] if not pd.isna(commit['commit']['author']) else None
                committer_date = commit['commit']['committer']['date']
                committer_email = time_reverse(commit['commit']['committer']['email']) if not pd.isna(commit['commit']['committer']) else None
            line_addition = commit['stats']['additions']
            line_deletion = commit['stats']['deletions']
            file_edit_num = len(commit['files'])
            # 处理json数据
            file_content = list_to_json(commit['files'])

            # 保存到数据库
            commit_data = [
                pr_number,
                sha,
                author,
                author_email,
                author_date,
                committer,
                committer_email,
                committer_date,
                message,
                line_addition,
                line_deletion,
                file_edit_num,
                file_content
            ]
            result = insert_into_commit(repo, commit_data)
            print(f"commit_sha: %s, insert %s" % (sha, result > 0))
        else:
            print(f"commit_sha: %s, request url failed, status code: %s" % (sha, response.status_code))


'''
功能：从json字符串中提取所有commit的sha，列表形式返回
'''
def extract_commit_sha(commit_content: str):
    commit_sha_list = []
    commit_list = json.loads(commit_content)
    if len(commit_list) == 0:
        return commit_sha_list
    for commit in commit_list:
        commit_sha = commit['sha']
        commit_sha_list.append(commit_sha)
    return commit_sha_list


'''
功能：从repo_self表中提取所有commit相关的信息
'''
def crawl_commit_between(owner: str, repo: str, start: datetime, end: datetime):
    # 从repo_self表中查询特定时间段的PR信息
    table = f"{repo.replace('-', '_')}_self"
    sql = f"select pr_number, created_at, closed_at, commit_number, commit_content from `{table}` where created_at >= '{start}' and created_at < '{end}'"
    data = select_all(sql)

    # 借助pandas对PR集合进行分析
    df = pd.DataFrame(data)
    for index, row in df.iterrows():
        pr_number = row['pr_number']
        # if pr_number <= 693:
        #     print(f"pr#{pr_number} process done")
        #     continue
        commit_content = row['commit_content']
        # 提取当前PR包含的commit集合
        commit_sha_list = extract_commit_sha(commit_content)
        # 根据sha查询commit信息，插入到数据库
        crawl_commit(owner, repo, pr_number, commit_sha_list)
        print(f"pr#{pr_number} process done")


if __name__ == '__main__':
    projects = ['openzipkin/zipkin', 'apache/netbeans', 'opencv/opencv', 'apache/dubbo', 'phoenixframework/phoenix',
                'ARM-software/arm-trusted-firmware', 'apache/zookeeper',
                'spring-projects/spring-framework', 'spring-cloud/spring-cloud-function',
                'vim/vim', 'gpac/gpac', 'ImageMagick/ImageMagick', 'apache/hadoop',
                'libexpat/libexpat', 'apache/httpd', 'madler/zlib', 'redis/redis', 'stefanberger/swtpm']

    for pro in ['apache/dubbo']:
        owner = pro.split('/')[0]
        repo = pro.split('/')[1]
        start = datetime(2022, 7, 1)
        end = datetime(2022, 7, 2)
        print(f"project#{repo}/{owner} begin")
        crawl_commit_between(owner, repo, start, end)
        print(f"project#{repo}/{owner} process done")