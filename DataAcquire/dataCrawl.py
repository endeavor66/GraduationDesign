import requests

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

if __name__ == '__main__':
    project = ''
    start_time = ''
    end_time = ''

    crawDataBetweenTime()
    crawlDataForProject()