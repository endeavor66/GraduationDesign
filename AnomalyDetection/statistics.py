import pandas as pd
from AnomalyDetection.Config import *
import os


'''
功能：统计每个项目场景中可疑人员数量和比例
'''
def cal_anomaly_people_percent(repo: str, role: str):
    input_path = f"{MULTI_MODEL_VOTE_DIR}/{repo}_{role}_multi_model_vote.csv"

    if not os.path.exists(input_path):
        print(f"{input_path} don't exist")
        return []

    df = pd.read_csv(input_path)
    df_anomaly = df.loc[df['vote'] == -1]

    anomaly_num = df_anomaly.shape[0]
    total_num = df.shape[0]
    anomaly_percent = anomaly_num / total_num

    del df, df_anomaly

    return [repo, role, anomaly_num, total_num, anomaly_percent]


if __name__ == '__main__':
    roles = ["reviewer", "maintainer", "committer"]
    projects = ['openzipkin/zipkin', 'apache/netbeans', 'opencv/opencv', 'apache/dubbo', 'phoenixframework/phoenix',
                'ARM-software/arm-trusted-firmware', 'apache/zookeeper',
                'spring-projects/spring-framework', 'spring-cloud/spring-cloud-function',
                'vim/vim', 'gpac/gpac', 'ImageMagick/ImageMagick', 'apache/hadoop',
                'libexpat/libexpat', 'apache/httpd', 'madler/zlib', 'redis/redis', 'stefanberger/swtpm']
    result = []
    for pro in projects:
        repo = pro.split('/')[1]
        for role in roles:
            r = cal_anomaly_people_percent(repo, role)
            if len(r) == 0:
                continue
            result.append(r)
            print(f"{repo} {role} process done")

    output_path = f"{OUTPUT_DATA_DIR}/anomaly_people_statistics.csv"
    df_file = pd.DataFrame(data=result, columns=['repo', 'role', 'anomaly_num', 'total_num', 'anomaly_percent'])
    df_file.to_csv(output_path, index=False, header=True)