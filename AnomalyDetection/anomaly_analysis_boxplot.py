import pandas as pd
import numpy as np
from AnomalyDetection.Config import *
import os


'''
功能：基于箱线图来检测x是否为异常点
'''
def boxplot(repo: str, role: str):
    input_path = f"{FEATURE_DIR}/{repo}_{role}_feature.csv"
    output_path = f"{BOX_PLOT_DIR}/{repo}_{role}_box_plot.csv"

    if not os.path.exists(input_path):
        print(f"{input_path} don't exist")
        return

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
    df_file = pd.DataFrame(data=statistics_T, columns=columns[1:])
    df_file.insert(0, 'scene', ['25%', '50%', '75%', 'mean'])
    df_file.to_csv(output_path, index=False, header=True)


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
            boxplot(repo, role)
            print(f"{repo} {role} process done")
