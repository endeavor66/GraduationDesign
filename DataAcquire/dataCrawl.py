import requests
import gzip
import json
import pandas as pd

url_prefix = 'https://data.gharchive.org/'

'''
功能: 从GH Archive网址上爬取指定时间段(start_time, end_time)的事件日志数据
输入:
    start_time: 开始时间
    end_time: 结束时间
输出:
    爬取到的数据源文件保存在OriginData目录中，命名格式类似'2015-01-01-15.json.gz'
'''
def crawDataBetweenTime(start_time, end_time):
    pass


'''
功能: 从路径为source的json文件中提取项目名为project的事件日志
输入:
    project: 待提取的项目名
    source: 数据源文件, 事件日志数据来源，存放在OriginData目录中
    target: 数据目标文件，用来保存提取好的数据文件，存放在ProjectData目录中。注意以追加的形式保存在该文件中
输出:
    提示信息，如：共提取到的事件日志数量，异常信息等
'''
def crawlDataForProject(project, source, target):
    pass



from gharchive import GHArchive
gh = GHArchive()
#
data = gh.get('2020-05-06 11:00:00', '2020-05-06 11:00:00', filters=[
    ('repo.name', 'bitcoin/bitcoin')
])
#
print(data)

# if __name__ == '__main__':
#     pass
    # project = ''
    # start_time = ''
    # end_time = ''
    #
    # crawDataBetweenTime()
    # crawlDataForProject()
    # r = requests.get('https://data.gharchive.org/2015-01-01-15.json.gz', headers={'Connection':'close'}, stream=True, verify=False, timeout=(5,5))
    # g = gzip.decompress(r.content)
    # data_str = g.decode("utf8")
    # data_strs = [s for s in data_str.split("\n") if s]
    # json_str = "[" + ", ".join(data_strs) + "]"
    # data = json.loads(json_str)

    # print(data)




# def _date_to_str(date: pd.Timestamp):
#     mdy = date.strftime("%Y-%m-%d")
#     if date.hour == 0:
#         h = '0'
#     else:
#         h = date.strftime("%H").lstrip("0")
#     return f'{mdy}-{h}'
#
# dates = pd.date_range(start='2020-05-06 11:00:00', end='2020-05-06 13:00:00', freq='h')
# date_strs = [_date_to_str(date) for date in dates]
# print(date_strs)

import pymysql
conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', password='123456',
                       db='poison', charset='utf8')
cur = conn.cursor()
# 第三步：执行sql语句
sql = """insert into user(name, age) values(%s, %s)"""

# 返回的是查询的数据条数
res = cur.execute(sql, ('zhaoliu', 24))
conn.commit()
print(res)

cur.close()
conn.close()

