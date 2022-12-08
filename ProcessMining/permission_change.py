import pandas as pd
from ProcessMining.Config import *


'''
功能：寻找所有权限变更发生的时刻
'''
def permission_change(input_path: str, is_fork: bool):
    df = pd.read_csv(input_path, parse_dates=['time:timestamp'])
    role_change = []
    for person, group in df.groupby('People'):
        group.sort_values(by='time:timestamp', inplace=True)

        # 若为协作者模式，判断是否获得过committer权限
        if not is_fork:
            df_committer = group.loc[group['concept:name'].isin(['SubmitCommit', 'Revise'])]
            if df_committer.shape[0] > 0:
                row = df_committer.iloc[0]
                role_change.append([row['People'], row['case:concept:name'], row['time:timestamp'], 'Committer'])

        # 判断是否获得过reviewer权限
        df_reviewer = group.loc[group['concept:name'].isin(['PRReviewApprove', 'PRReviewReject'])]
        if df_reviewer.shape[0] > 0:
            row = df_reviewer.iloc[0]
            role_change.append([row['People'], row['case:concept:name'], row['time:timestamp'], 'Reviewer'])

        # 判断是否获得过maintainer权限
        df_maintainer = group.loc[group['concept:name'].isin(['MergePR', 'ClosePR'])]
        if df_maintainer.shape[0] > 0:
            row = df_maintainer.iloc[0]
            role_change.append([row['People'], row['case:concept:name'], row['time:timestamp'], 'Maintainer'])

    return role_change


if __name__ == '__main__':
    repo = 'tensorflow'
    output_path = f"{ROLE_CHANGE_DIR}/{repo}_role_change.csv"
    role_change = []
    for t in FILE_TYPES:
        input_path = f"{PROCESS_DATA_DIR}/{repo}_{t}.csv"
        is_fork = t.split('_')[0] == 'fork'
        result = permission_change(input_path, is_fork)
        role_change.extend(result)

    df_file = pd.DataFrame(data=role_change, columns=['people', 'change_pr_number', 'change_role', 'change_role_time'])
    df_file.to_csv(output_path, index=False, header=True)