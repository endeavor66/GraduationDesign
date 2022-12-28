import pandas as pd
from ProcessMining.Config import *
from datetime import datetime, timedelta
from utils.pr_self_utils import get_all_pr_number_between


'''
功能：确定一个特定的person在权限发生变更之后一段时间内所参与的所有PR，(并在这些PR中使用了新的权限)
'''
def cal_involved_pr(repo: str, person: str, change_role_time: datetime):
    # 获取权限变更后一个月内的PR
    end_time = change_role_time + timedelta(days=30)
    pr_number_list = get_all_pr_number_between(repo, change_role_time, end_time)

    # 从log中筛选出权限变更人在权限变更后一个月内参与的所有PR
    role_info_path = f"{LOG_ALL_SCENE_DIR}/{repo}.csv"
    df = pd.read_csv(role_info_path, parse_dates=['time:timestamp'])
    pr_df = df.loc[(df['People'] == person) & df['case:concept:name'].isin(pr_number_list)]
    involved_pr_list = pr_df['case:concept:name'].unique()
    return "#".join(str(x) for x in involved_pr_list)


'''
功能：确定一个仓库中所有发生权限变更的person，在权限变更后一段时间内所参与的所有PR，并在这些PR中使用了新的权限
'''
def cal_involved_pr_after_permission_change(repo: str):
    role_change_path = f"{ROLE_CHANGE_DIR}/{repo}_role_change.csv"
    df_role_change = pd.read_csv(role_change_path, parse_dates=['change_role_time'])
    df_role_change['involved_pr_after_permission_change'] = df_role_change.apply(lambda x: cal_involved_pr(repo, x['people'], x['change_role_time']), axis=1)
    df_role_change.to_csv(role_change_path, index=False, header=True)


if __name__ == '__main__':
    projects = ['openzipkin/zipkin', 'apache/netbeans', 'opencv/opencv', 'apache/dubbo', 'phoenixframework/phoenix',
                'ARM-software/arm-trusted-firmware', 'apache/zookeeper',
                'spring-projects/spring-framework', 'spring-cloud/spring-cloud-function',
                'vim/vim', 'gpac/gpac', 'ImageMagick/ImageMagick', 'apache/hadoop',
                'libexpat/libexpat', 'apache/httpd', 'madler/zlib', 'redis/redis', 'stefanberger/swtpm']
    for pro in projects:
        repo = pro.split('/')[1]
        cal_involved_pr_after_permission_change(repo)
        print(f"repo#{repo} process done")