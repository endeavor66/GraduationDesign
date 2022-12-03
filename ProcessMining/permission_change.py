import pandas as pd
import os
from ProcessMining.Config import *
from utils.mysql_utils import select_one
from datetime import datetime


'''
功能：获取特定PR(pr_number)的创建和关闭时间(created_at, closed_at)
'''
def get_pr_attributes(repo: str, pr_number: int) -> (datetime, datetime):
    sql = f"select * from `{repo}_self` where repo_name='{repo}' and pr_number={pr_number}"
    data = select_one(sql)
    return data


'''
功能：为不同用户添加角色, 用户在PR中只有一个特定的角色(maintainer, reviewer, committer)
规则：
1、maintainer：MergePR、PRReviewDismiss、OpenPR和ClosePR不是同一个人(因为committer也可以ClosePR)
2、reviewer：明确给出了评审意见(PRReviewApprove, PRReviewReject)，或者只做过PRReviewComment(因为committer也可以PRReviewComment)
3、committer：{"ForkRepository", "CreateBranch", "DeleteBranch", "SubmitCommit", "Revise", "OpenPR"}
'''
def record_role_info_single_type(repo: str, input_path: str, output_path: str):
    df = pd.read_csv(input_path)
    committer_activities = {"ForkRepository", "CreateBranch", "DeleteBranch", "SubmitCommit", "Revise", "OpenPR"}
    # reviewer_activities = {"PRReviewApprove", "PRReviewReject", "PRReviewComment"}
    # maintainer_activities = {"PRReviewDismiss", "ClosePR", "ReopenPR", "MergePR"}
    role_info = []
    for pr_number, group_case in df.groupby('case:concept:name'):
        # 查询pr的创建/关闭时间
        pr_attribute = get_pr_attributes(repo, pr_number)
        pr_open_time = pr_attribute['created_at']
        pr_close_time = pr_attribute['closed_at']
        for person, group_person in group_case.groupby('People'):
            activities = set(group_person['concept:name'].to_list())
            role = None
            if len(activities & {"PRReviewDismiss", "MergePR"}) > 0:
                role = "maintainer"
            elif len(activities & {"ClosePR", "ReopenPR"}) > 0 and ("OpenPR" not in activities):
                role = "maintainer"
            elif len(activities & {"PRReviewApprove", "PRReviewReject"}) > 0 or activities == {"PRReviewComment"}:
                role = "reviewer"
            elif len(activities & committer_activities) > 0:
                role = "committer"
            else:
                print(f"pr_number: {pr_number}, person: {person}, can't determine role")
            role_info.append([pr_number, person, role, pr_open_time, pr_close_time])
        # print(f"pr_number#{pr_number} process done")
    df_file = pd.DataFrame(data=role_info, columns=['pr_number', 'people', 'role', 'pr_open_time', 'pr_close_time'])
    df_file.to_csv(output_path, index=False, header=True, mode='a')


'''
功能：记录四种类型文件中所有人员的角色信息，保存到一个文件中
'''
def record_role_info(repo: str):
    output_path = f"{ROLE_INFO_DIR}/{repo}_role_info.csv"
    if os.path.exists(output_path):
        os.remove(output_path)
    for t in FILE_TYPES:
        filename = f"{repo}_{t}.csv"
        input_path = f"{PROCESS_DATA_DIR}/{filename}"

        record_role_info_single_type(repo, input_path, output_path)
        print(f"{filename} process done")


'''
功能：寻找所有权限变更发生的时刻
'''
def permission_change(repo: str):
    input_path = f"{ROLE_INFO_DIR}/{repo}_role_info.csv"
    output_path = f"{ROLE_CHANGE_DIR}/{repo}_role_change.csv"
    df = pd.read_csv(input_path, parse_dates=['pr_open_time', 'pr_close_time'], infer_datetime_format=True)
    role_change = []
    for name, group in df.groupby('people'):
        group.sort_values(by='pr_open_time', inplace=True)
        initial_role = group.iloc[0]
        change_role = group.loc[group['role'] != initial_role['role']]
        if change_role.shape[0] > 0:
            # 记录第一次权限变更的信息
            first_change_role = change_role.iloc[0]
            role_change.append([
                initial_role['people'],
                initial_role['pr_number'],
                initial_role['role'],
                initial_role['pr_open_time'],
                first_change_role['pr_number'],
                first_change_role['role'],
                first_change_role['pr_open_time'],
            ])
            print("permission change")
            print("\t initial role info: %s" % initial_role.to_dict())
            print("\t change role info: %s" % first_change_role.to_dict())

    df_file = pd.DataFrame(data=role_change, columns=[
        'people',
        'initial_pr_number', 'initial_role', 'initial_pr_open_time',
        'change_pr_number', 'change_role', 'change_pr_open_time'
    ])
    df_file.to_csv(output_path, index=False, header=True)



if __name__ == '__main__':
    repo = 'tensorflow'
    # record_role_info(repo)
    permission_change(repo)