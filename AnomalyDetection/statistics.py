import pandas as pd
from AnomalyDetection.Config import *


'''
功能：统计每个项目场景中可疑人员数量和比例
'''
def cal_anomaly_people_percent(repo: str, role: str):
    input_path = f"{MULTI_MODEL_VOTE_DIR}/{repo}_{role}_multi_model_vote.csv"

    df = pd.read_csv(input_path)
    df_anomaly = df.loc[df['vote'] == -1]

    anomaly_num = df_anomaly.shape[0]
    total_num = df.shape[0]
    anomaly_percent = anomaly_num / total_num

    del df, df_anomaly

    return [repo, role, anomaly_num, total_num, anomaly_percent]


if __name__ == '__main__':
    repos = ['zipkin', 'netbeans', 'opencv', 'dubbo', 'phoenix']
    roles = ["reviewer", "maintainer", "committer"]
    result = []
    for repo in repos:
        for role in roles:
            r = cal_anomaly_people_percent(repo, role)
            result.append(r)
            print(f"{repo} {role} process done")

    output_path = f"{OUTPUT_DATA_DIR}/anomaly_people_statistics.csv"
    df_file = pd.DataFrame(data=result, columns=['repo', 'role', 'anomaly_num', 'total_num', 'anomaly_percent'])
    df_file.to_csv(output_path, index=False, header=True)