import pandas as pd
import numpy as np
from AnomalyDetection.Config import *


'''
功能：基于箱线图来检测x是否为异常点
'''
def boxplot(repo: str, role: str):
    input_path = f"{FEATURE_DIR}/{repo}_{role}_feature.csv"
    output_path = f"{BOX_PLOT_DIR}/{repo}_{role}_box_plot.csv"

    df = pd.read_csv(input_path)
    columns = df.columns.values.tolist()

    # 每一列计算上下四分位
    statistics = []
    for index, column in df.iteritems():
        if index == 'people':
            continue
        res = np.percentile(column, (25, 50, 75), interpolation='midpoint')
        mean = column.mean()
        statistics.append([res[0], res[1], res[2], mean])

    # 保存各个属性的上下四分位数、中位数、平均值
    statistics_T = np.array(statistics).transpose()
    df_file = pd.DataFrame(data=statistics_T, index=['25%', '50%', '75%', 'mean'], columns=columns[1:])
    df_file.to_csv(output_path, index=True, header=True)


if __name__ == '__main__':
    repos = ['zipkin', 'netbeans', 'opencv', 'dubbo', 'phoenix']
    roles = ["reviewer", "maintainer", "committer"]
    for repo in repos:
        for role in roles:
            boxplot(repo, role)
            print(f"{repo} {role} process done")
