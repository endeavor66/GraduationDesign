# -*- coding:utf-8 -*-
#异常值检测
import pandas as pd
import matplotlib.pyplot as plt

from utils.path_exist import path_exists_or_create


def plot_box(repo_name, sheet_name, index):
    data_path = '../calculate/features/' + repo_name + '_feature.xlsx'
    df = pd.read_excel(data_path, sheet_name=sheet_name)
    data = df[index]
    #绘制箱线图
    plt.boxplot(x = data,
                whis = 1.5,#1.5倍的四分位差
                widths = 0.7,
                patch_artist = True,#填充箱体颜色
                showmeans= True,# 显示均值
                boxprops = {'facecolor':"steelblue"},#箱体填充色
                flierprops = {'markerfacecolor':'red','markeredgecolor':'red','markersize':4},#异常点填充色、边框、大小
                meanprops = {'marker':'D','markerfacecolor':'black','markersize':4},#均值点标记符号、填充色、大小
                medianprops = {'linestyle':'--','color':'orange'},#中位数处的标记符号、填充色、大小
                labels = [index])
    #plt.show()
    img_path = "../calculate/img/"
    path_exists_or_create(img_path)
    plt.savefig(img_path + repo_name + '_' + index + '_box.png')
    plt.close()
    # 计算上下四分位数
    Q1 = data.quantile(q = 0.25)
    Q3 = data.quantile(q = 0.75)

    #异常值判断标准， 1.5倍的四分位差 计算上下须对应的值
    low_quantile = Q1 - 1.5*(Q3-Q1)
    high_quantile = Q3 + 1.5*(Q3-Q1)

    # 输出异常值
    value = df[(df[index] > high_quantile) | (df[index] < low_quantile)]
    print(value[['id', index]])
    print(value[['id']].count())

plot_box("phoenix", "activity", "title_len")