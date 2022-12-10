import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import List, Union
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from AnomalyDetection.Config import *

'''
功能：基于箱线图来检测x是否为异常点
'''
def boxplot(data: List[Union[int, float]], x: Union[int, float]) -> bool:
    res = np.percentile(data, (25, 50, 75), interpolation='midpoint')
    print(res)
    # 绘制箱线图
    plt.boxplot(data)
    plt.show()
    # 判断是否为异常点
    is_outlier = x < res[0] or x > res[2]
    return is_outlier


'''
功能：基于孤立森林算法来检测x是否为异常点
'''
def isolation_forest(input_path: str, output_path: str):
    df = pd.read_csv(input_path)
    data = df.iloc[:, 1:]
    data.fillna(0, inplace=True)
    model = IsolationForest(random_state=0)
    model.fit(data)
    score = model.decision_function(data)
    anomaly = model.predict(data)
    df['score'] = score
    df['anomaly'] = anomaly

    return df.to_csv(output_path, index=False, header=True)


'''
功能：基于LOF算法来检测x是否为异常点
'''
def LOF(data: List[Union[int, float]], x: Union[int, float]) -> bool:
    clf = LocalOutlierFactor(novelty=True).fit(data)
    y = clf.predict(x)
    # Returns -1 for anomalies/outliers and +1 for inliers.
    return y == -1


'''
功能：采用高维度算法检测异常点，低维度算法分析异常点的各个属性是否异常(分析异常原因)
'''
def anomaly_detection(repo: str, role: str):
    input_path = f"{FEATURE_DIR}/{repo}_{role}_feature.csv"

    # 高维度检测异常值：孤立森林
    output_path = f"{OUTPUT_DATA_DIR}/{repo}_{role}_isolation_forest.csv"
    isolation_forest(input_path, output_path)

    # TODO 低维度分析原因：箱线图


if __name__ == '__main__':
    repos = ['zipkin', 'netbeans', 'opencv', 'dubbo', 'phoenix']
    roles = ["reviewer", "maintainer", "committer"]
    for repo in repos:
        for role in roles:
            anomaly_detection(repo, role)
            print(f"{repo} {role} process done" )