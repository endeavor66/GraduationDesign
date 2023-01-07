import pandas as pd
import numpy as np
from AnomalyDetection.Config import *
import os

def anomaly_analysis(repo: str, role: str):
    input_path1 = f"{MULTI_MODEL_VOTE_DIR}/{repo}_{role}_multi_model_vote.csv"
    input_path2 = f"{BOX_PLOT_DIR}/{repo}_{role}_box_plot.csv"
    output_path = f"{ANOMALY_ANALYSIS_DIR}/{repo}_{role}_anomaly_analysis.csv"

    if not os.path.exists(input_path1):
        print(f"{input_path1} don't exist")
        return

    df1 = pd.read_csv(input_path1)
    df2 = pd.read_csv(input_path2)

    # 筛选出可疑人员
    df3 = df1.loc[df1['vote'] == -1]
    if df3.shape[0] == 0:
        print(f"{repo} {role} don't detect any anomaly people")
        return

    # df2修改列名，以便能够正确和df3合并
    df2.rename(columns={'scene': 'people'}, inplace=True)

    # 合并df3和df2
    df_merge = pd.concat([df3, df2], ignore_index=True)
    df_merge.to_csv(output_path, index=False, header=True)


if __name__ == '__main__':
    roles = ["reviewer", "maintainer", "committer"]
    projects = ['openzipkin/zipkin', 'apache/netbeans', 'opencv/opencv', 'apache/dubbo', 'phoenixframework/phoenix',
                'ARM-software/arm-trusted-firmware', 'apache/zookeeper',
                'spring-projects/spring-framework', 'spring-cloud/spring-cloud-function',
                'vim/vim', 'gpac/gpac', 'ImageMagick/ImageMagick', 'apache/hadoop',
                'libexpat/libexpat', 'apache/httpd', 'madler/zlib', 'redis/redis', 'stefanberger/swtpm']
    for pro in projects:
        repo = pro.split('/')[1]
        for role in roles:
            anomaly_analysis(repo, role)
            print(f"{repo} {role} process done")