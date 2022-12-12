import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn import svm
from sklearn.preprocessing import MinMaxScaler
from AnomalyDetection.Config import *

'''
功能：基于箱线图来检测x是否为异常点
'''
def boxplot(repo: str, role: str):
    input_path = f"{ISOLATION_FOREST_DIR}/{repo}_{role}_isolation_forest.csv"
    output_path = f"{BOX_PLOT_DIR}/{repo}_{role}_box_plot.csv"

    df = pd.read_csv(input_path)
    columns = df.columns.values.tolist()

    # 每一列计算上下四分位
    statistics = []
    for index, column in df.iteritems():
        if index in ['person', 'score', 'anomaly']:
            continue
        res = np.percentile(column, (25, 50, 75), interpolation='midpoint')
        mean = column.mean()
        statistics.append([res[0], res[1], res[2], mean])
        print(f"{index} statistics: {res}")

    # 保存各个属性的上下四分位数、中位数、平均值
    statistics_T = np.array(statistics).transpose()
    df_file = pd.DataFrame(data=statistics_T, index=['25%', '50%', '75%', 'mean'], columns=columns[1:-2])
    df_file.to_csv(output_path, index=True, header=True)


'''
功能：基于孤立森林算法来检测x是否为异常点
'''
def isolation_forest(input_path: str, output_path: str):
    # 准备数据
    df = pd.read_csv(input_path)
    data = df.iloc[:, 1:]
    data.fillna(0, inplace=True)

    # 训练模型
    model = IsolationForest(random_state=0)
    model.fit(data)

    # 异常程度: 值越小，越异常(默认以0作为判断异常点的阈值)
    score = model.decision_function(data)
    # +1 for inliers, -1 for outliers
    anomaly = model.predict(data)

    # 模型预测
    df['score'] = score
    df['anomaly'] = anomaly

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

    # 数据预处理: 填充零值，归一化
    data.fillna(0, inplace=True)
    data = min_max_scale(data)

    # 训练模型
    model = svm.OneClassSVM()
    model.fit(data)

    # 计算样本到超平面的距离，含正负号，+表示在超平面内，-表示在超平面外
    df['score'] = model.decision_function(data)
    # 判断数据是在超平面内还是超平面外，返回+1或-1，正号是超平面内，负号是在超平面外
    df['anomaly'] = model.predict(data)

    df.to_csv(output_path, index=False, header=True)


'''
功能：LOF 异常检测算法
'''
def lof(input_path: str, output_path: str):
    # 准备数据
    df = pd.read_csv(input_path)
    data = df.iloc[:, 1:]

    # 数据预处理: 填充零值，归一化
    data.fillna(0, inplace=True)
    data = min_max_scale(data)

    # 训练模型
    clf = LocalOutlierFactor(novelty=True)
    clf = clf.fit(data)

    # 值越小，越异常。负号表示 outlier, 正号表示 inlier
    df['score'] = clf.decision_function(data)
    # 1 inlier, -1 outlier
    df['anomaly'] = clf.predict(data)

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

    # TODO 低维度分析原因：箱线图


if __name__ == '__main__':
    repos = ['zipkin', 'netbeans', 'opencv', 'dubbo', 'phoenix']
    roles = ["reviewer", "maintainer", "committer"]
    for repo in repos:
        for role in roles:
            anomaly_detection(repo, role)
            print(f"{repo} {role} process done")
    # boxplot("dubbo", "committer")