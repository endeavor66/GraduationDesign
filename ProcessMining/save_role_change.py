from ProcessMining.Config import *
from utils.mysql_utils import batch_insert_into_permission_change
import os
import pandas as pd

def save(repo: str):
    filepath = f"{ROLE_CHANGE_DIR}/{repo}_role_change.csv"
    if not os.path.exists(filepath):
        print(f"{filepath} don't exist")
        return

    df = pd.read_csv(filepath)
    datas = []
    for index, row in df.iterrows():
        t = (
            repo,
            row['people'],
            row['change_pr_number'],
            row['change_role_time'],
            row['change_role']
        )
        datas.append(t)
    batch_insert_into_permission_change(repo, datas)


if __name__ == '__main__':
    projects = [
                # 'openzipkin/zipkin',
                # 'apache/netbeans', 'opencv/opencv', 'apache/dubbo', 'phoenixframework/phoenix',
                # 'ARM-software/arm-trusted-firmware', 'apache/zookeeper',
                # 'spring-projects/spring-framework', 'spring-cloud/spring-cloud-function',
                # 'vim/vim', 'gpac/gpac', 'ImageMagick/ImageMagick', 'apache/hadoop',
                # 'libexpat/libexpat', 'apache/httpd', 'madler/zlib', 'redis/redis', 'stefanberger/swtpm'
                ]
    for pro in ['apache/dubbo', 'gpac/gpac']:
        repo = pro.split('/')[1]
        save(repo)
        print(f"{repo} process done")
