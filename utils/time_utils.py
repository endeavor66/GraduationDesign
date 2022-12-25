from datetime import datetime, timedelta
import pandas as pd
from utils.math_utils import cal_mean
from typing import List

# 2021-10-08T01:11:34Z时间转换2021-10-08 01:11:34
def time_reverse(time_str):
    try:
        time_temp = time_str.replace('T', ' ')
        time_temp = time_temp.replace('Z', '')
        return datetime.strptime(str(time_temp), '%Y-%m-%d %H:%M:%S')
    except:
        return None


'''
功能：计算时间差，单位hours
'''
def cal_time_delta_hours(start: datetime, end: datetime):
    if pd.isna(start) or pd.isna(end):
        return None
    delta_seconds = (end - start).total_seconds()
    delta_hours = delta_seconds / 3600
    return delta_hours


'''
功能：计算时间差，单位minutes
'''
def cal_time_delta_minutes(start: datetime, end: datetime):
    if pd.isna(start) or pd.isna(end):
        return None
    delta_seconds = (end - start).total_seconds()
    delta_minutes = delta_seconds / 60
    return delta_minutes


'''
功能：计算平均时间间隔
'''
def cal_time_interval(date_list: List):
    date_list.sort()
    interval = []
    for i in range(1, len(date_list)):
        inv = cal_time_delta_minutes(date_list[i - 1], date_list[i])
        interval.append(inv)
    return cal_mean(interval)


if __name__ == '__main__':
    start = datetime(2021, 1, 1, 1, 10, 0)
    end = datetime(2021, 1, 2, 2, 20, 10)
    delta_minutes = cal_time_delta_minutes(start, end)
    print(delta_minutes)
