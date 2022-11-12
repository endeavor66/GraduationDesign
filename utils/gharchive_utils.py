from gharchive import GHArchive
from gharchive.models import Commit
from typing import Optional, Union, Tuple, Sequence, List


"""
功能：获取gharchive客户端
"""
def get_client():
    return GHArchive()


"""
功能：传入commits对象数组，提取每个commit的sha，拼接为字符串返回。格式：SHA1#SHA2#SHA3
"""
def join_commits_sha(commits: List[Commit]) -> str:
    if commits is None:
        return ""
    sha = []
    for commit in commits:
        sha.append(commit.sha)
    return "#".join(sha)