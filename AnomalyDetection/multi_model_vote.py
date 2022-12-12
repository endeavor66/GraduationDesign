import pandas as pd
from AnomalyDetection.Config import *


'''
功能：根据少数服从多数原则，计算投票结果
'''
def cal_vote(label1: int, label2: int, label3: int):
    r = sum([label1, label2, label3])
    if r > 0:
        return 1
    else:
        return -1


'''
功能：根据多个模型的识别结果，采取投票法来打标签
'''
def multi_model_vote(repo: str, role: str):
    # 读取文件
    input_path1 = f"{ISOLATION_FOREST_DIR}/{repo}_{role}_isolation_forest.csv"
    input_path2 = f"{ONE_CLASS_SVM_DIR}/{repo}_{role}_one_class_svm.csv"
    input_path3 = f"{LOF_DIR}/{repo}_{role}_lof.csv"
    output_path = f"{MULTI_MODEL_VOTE_DIR}/{repo}_{role}_multi_model_vote.csv"

    df1 = pd.read_csv(input_path1)
    df2 = pd.read_csv(input_path2)
    df3 = pd.read_csv(input_path3)

    # 只保留需要的列
    df2 = df2[['people', 'score', 'anomaly']]
    df3 = df3[['people', 'score', 'anomaly']]

    # 修改列名
    df1.rename(columns={'score': 'score_if', 'anomaly': 'anomaly_if'}, inplace=True)
    df2.rename(columns={'score': 'score_ocsvm', 'anomaly': 'anomaly_ocsvm'}, inplace=True)
    df3.rename(columns={'score': 'score_lof', 'anomaly': 'anomaly_lof'}, inplace=True)

    # 合并
    df4 = pd.merge(df1, df2, how='inner', on='people')
    df5 = pd.merge(df4, df3, how='inner', on='people')

    # 计算投票结果(少数服从多数)
    df5['vote'] = df5.apply(lambda x: cal_vote(x['anomaly_if'], x['anomaly_ocsvm'], x['anomaly_lof']), axis=1)

    df5.to_csv(output_path, index=False, header=True)


if __name__ == '__main__':
    repos = ['zipkin', 'netbeans', 'opencv', 'dubbo', 'phoenix']
    roles = ["reviewer", "maintainer", "committer"]
    for repo in repos:
        for role in roles:
            multi_model_vote(repo, role)
            print(f"{repo} {role} process done")