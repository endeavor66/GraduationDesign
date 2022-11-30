import pandas as pd
from ProcessMining.Config import *


'''
功能：寻找所有权限变更发生的时刻
'''
def permission_change(input_path: str):
    df = pd.read_csv(input_path)
    for name, group in df.groupby('People'):
        prev_role = None
        for index, row in group.iterrows():
            cur_role = row['Role']
            if (not pd.isna(prev_role)) and cur_role != prev_role:
                print(f"{name} permission change, prev_role: {prev_role}, cur_role: {cur_role}")
            prev_role = cur_role


if __name__ == '__main__':
    repo = 'tensorflow'
    t = FILE_TYPES[0]
    input_path = f"{PROCESS_DATA_DIR}/{repo}_{t}.csv"
    permission_change(input_path)
    print(f"{input_path} process done")