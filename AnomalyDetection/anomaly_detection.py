import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn import svm
from sklearn.preprocessing import MinMaxScaler
from AnomalyDetection.Config import *
from AnomalyDetection.dagmm2.cal_score import score


'''
功能：基于孤立森林算法来检测x是否为异常点
'''
def isolation_forest(input_path: str, output_path: str):
    # 准备数据
    df = pd.read_csv(input_path)
    data = df.iloc[:, 1:]

    # 训练模型
    model = IsolationForest(random_state=0)
    model.fit(data)

    # 异常程度: 值越小，越异常(默认以0作为判断异常点的阈值)
    score = model.decision_function(data)
    # +1 for inliers, -1 for outliers
    anomaly = model.predict(data)

    # 模型预测
    df['score'] = score

    # 选择5%作为异常数据，-1 for outlier, 1 for inlier
    threshold = np.percentile(df['score'], (ANOMALY_PERCENT), interpolation='nearest')
    df['anomaly'] = df['score'].apply(lambda x: -1 if x < threshold else 1)

    # 保存文件
    return df.to_csv(output_path, index=False, header=True)


'''
功能：归一化
'''
def min_max_scale(data: pd.DataFrame):
    scaler = MinMaxScaler()
    new_data = scaler.fit_transform(data)
    return new_data


'''
功能：One Class SVM 异常检测算法
'''
def one_class_svm(input_path: str, output_path: str):
    # 准备数据
    df = pd.read_csv(input_path)
    data = df.iloc[:, 1:]

    # 数据预处理: 归一化
    data = min_max_scale(data)

    # 训练模型
    model = svm.OneClassSVM()
    model.fit(data)

    # 值越小，越异常
    df['score'] = model.decision_function(data)

    # 选择5%作为异常数据，-1 for outlier, 1 for inlier
    threshold = np.percentile(df['score'], (ANOMALY_PERCENT), interpolation='nearest')
    df['anomaly'] = df['score'].apply(lambda x: -1 if x < threshold else 1)

    df.to_csv(output_path, index=False, header=True)


'''
功能：LOF 异常检测算法
'''
def lof(input_path: str, output_path: str):
    # 准备数据
    df = pd.read_csv(input_path)
    data = df.iloc[:, 1:]

    # 数据预处理: 归一化
    data = min_max_scale(data)

    # 训练模型
    clf = LocalOutlierFactor(novelty=False, contamination=0.05)
    clf.fit(data)

    # 值越小，越异常
    df['score'] = clf.negative_outlier_factor_
    # 选择5%作为异常数据，-1 for outlier, 1 for inlier
    threshold = np.percentile(df['score'], (ANOMALY_PERCENT), interpolation='nearest')
    df['anomaly'] = df['score'].apply(lambda x: -1 if x < threshold else 1)

    # 保存结果
    df.to_csv(output_path, index=False, header=True)


'''
功能：采用高维度算法检测异常点，低维度算法分析异常点的各个属性是否异常(分析异常原因)
'''
def anomaly_detection(repo: str, role: str):
    input_path = f"{FEATURE_DIR}/{repo}_{role}_feature.csv"

    # 高维度检测异常值：孤立森林
    output_path = f"{ISOLATION_FOREST_DIR}/{repo}_{role}_isolation_forest.csv"
    isolation_forest(input_path, output_path)

    # 高维度检测异常值：One Class SVM
    output_path = f"{ONE_CLASS_SVM_DIR}/{repo}_{role}_one_class_svm.csv"
    one_class_svm(input_path, output_path)

    # 高维度检测异常值：LOF
    output_path = f"{LOF_DIR}/{repo}_{role}_lof.csv"
    lof(input_path, output_path)


if __name__ == '__main__':
    repos = ['zipkin', 'netbeans', 'opencv', 'dubbo', 'phoenix']
    roles = ["reviewer", "maintainer", "committer"]
    for repo in repos:
        for role in roles:
            anomaly_detection(repo, role)
            print(f"{repo} {role} process done")
