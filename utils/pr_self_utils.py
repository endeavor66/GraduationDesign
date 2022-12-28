from utils.mysql_utils import select_one, select_all
from typing import Dict, List
from datetime import datetime

'''
功能：获取特定pr
'''
def get_pr_attributes(repo: str, pr_number: int) -> Dict:
    table = f"{repo.replace('-', '_')}_self"
    sql = f"select * from `{table}` where repo_name='{repo.replace('-', '_')}' and pr_number={pr_number}"
    data = select_one(sql)
    return data


'''
功能：获取特定时间段内的所有pr_number
'''
def get_all_pr_number_between(repo: str, start: datetime, end: datetime) -> List:
    start_time = start.strftime('%Y-%m-%d %H:%M:%S')
    end_time = end.strftime('%Y-%m-%d %H:%M:%S')
    table = f"{repo.replace('-', '_')}_self"
    sql = f"select pr_number from `{table}` where created_at >= '{start_time}' and created_at < '{end_time}'"
    pr_list = select_all(sql)
    pr_number_list = [x['pr_number'] for x in pr_list]
    return pr_number_list
