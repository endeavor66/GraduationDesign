import datetime

# 2021-10-08T01:11:34Z时间转换2021-10-08 01:11:34
def time_reverse(time_str):
    try:
        time_temp = time_str.replace('T', ' ')
        time_temp = time_temp.replace('Z', '')
        return datetime.datetime.strptime(str(time_temp), '%Y-%m-%d %H:%M:%S')
    except:
        return None
