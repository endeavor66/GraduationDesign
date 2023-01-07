from ProcessMining.Config import *
from utils.mysql_utils import batch_insert_into_process_events
import os
import pandas as pd

def save(repo: str, scene: str):
    filepath = f"{LOG_SINGLE_SCENE_DIR}/{repo}_{scene}.csv"
    if not os.path.exists(filepath):
        print(f"{filepath} don't exist")
        return

    df = pd.read_csv(filepath)
    df['time:timestamp'] = pd.to_datetime(df['time:timestamp'])
    datas = []
    for index, row in df.iterrows():
        t = (
            repo,
            row['case:concept:name'],
            row['concept:name'],
            row['time:timestamp'],
            row['People'],
            scene
        )
        datas.append(t)
    batch_insert_into_process_events(repo, datas)


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
        for scene in FILE_TYPES:
            save(repo, scene)
            print(f"{repo} {scene} process done")
