import pm4py
import pandas as pd
import numpy as np
from typing import List, Union
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor

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
def isolation_forest(data: List[Union[int, float]], x: Union[int, float]) -> bool:
    clf = IsolationForest(random_state=0).fit(data)
    y = clf.predict(x)
    # Returns -1 for anomalies/outliers and +1 for inliers.
    return y


'''
功能：基于LOF算法来检测x是否为异常点
'''
def LOF(data: List[Union[int, float]], x: Union[int, float]) -> bool:
    clf = LocalOutlierFactor(novelty=True).fit(data)
    y = clf.predict(x)
    # Returns -1 for anomalies/outliers and +1 for inliers.
    return y == -1


repo = "tensorflow"
log_path = f"../ProcessMining/process_data/{repo}.csv"
log = pd.read_csv(log_path, parse_dates=['StartTimestamp', 'time:timestamp'], infer_datetime_format=True)
all_case_durations = pm4py.get_all_case_durations(log)
boxplot(all_case_durations, 100)