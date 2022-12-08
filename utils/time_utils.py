import datetime
import pandas as pd

# 2021-10-08T01:11:34Z时间转换2021-10-08 01:11:34
def time_reverse(time_str):
    try:
        time_temp = time_str.replace('T', ' ')
        time_temp = time_temp.replace('Z', '')
        return datetime.datetime.strptime(str(time_temp), '%Y-%m-%d %H:%M:%S')
    except:
        return None


'''
功能：计算时间差，单位hours
'''
def cal_time_delta_hours(start: datetime, end: datetime):
    if pd.isna(start) or pd.isna(end):
        return None
    delta = (end - start).components
    delta_hours = delta.days * 24 + delta.hours
    return delta_hours


'''
功能：计算时间差，单位minutes
'''
def cal_time_delta_minutes(start: datetime, end: datetime):
    if pd.isna(start) or pd.isna(end):
        return None
    delta = (end - start).components
    delta_minutes = delta.days * 24 * 60 + delta.hours * 60 + delta.minutes
    return delta_minutes
