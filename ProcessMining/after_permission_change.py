import pandas as pd
from ProcessMining.Config import *
from datetime import datetime
from typing import List


'''
功能：确定一个特定的person在权限发生变更之后一段时间内所参与的所有PR，并在这些PR中使用了新的权限
'''
def pr_after_permission_change_for_single_person(repo: str, person: str, change_role: str, change_pr_open_time: datetime) -> List:
    role_info_path = f"{ROLE_INFO_DIR}/{repo}_role_info.csv"
    # TODO 此处有BUG，pr_open_time和pr_close_time无法成功转换为datetime类型，并且读取后的时间精度没有到秒
    df = pd.read_csv(role_info_path, parse_dates=['pr_open_time', 'pr_close_time'], infer_datetime_format=True)
    pr_df = df.loc[(df['people'] == person) & (df['role'] == change_role) & (df['pr_open_time'] >= change_pr_open_time)]
    pr_number_list = pr_df['pr_number'].tolist()
    return pr_number_list


'''
功能：确定一个仓库中所有发生权限变更的person，在权限变更后一段时间内所参与的所有PR，并在这些PR中使用了新的权限
'''
def pr_after_permission_change_for_all_people(repo: str):
    role_change_path = f"{ROLE_CHANGE_DIR}/{repo}_role_change.csv"
    df_role_change = pd.read_csv(role_change_path, parse_dates=['initial_pr_open_time', 'change_pr_open_time'], infer_datetime_format=True)
    for index, row in df_role_change.iterrows():
        person = row['people']
        change_role = row['change_role']
        change_pr_open_time = row['change_pr_open_time']

        pr_number_list = pr_after_permission_change_for_single_person(repo, person, change_role, change_pr_open_time)
        print(f"person info: %s" % row.to_dict())
        print(f"\t pr number list after permission change:%s" % pr_number_list)


if __name__ == '__main__':
    repo = "tensorflow"
    pr_after_permission_change_for_all_people(repo)