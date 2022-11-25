import os

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest

from utils.path_exist import path_exists_or_create


def forest_semantic(repo_name):
    data_path = '../calculate/features/' + repo_name + '_feature.xlsx'
    df = pd.read_excel(data_path, sheet_name="activity")
    model = IsolationForest(n_estimators=100,
                            max_samples='auto',
                            contamination=float(0.1),
                            max_features=1.0)
    data = df[['title_len', 'body_len', 'labels']]
    model.fit(data.values)
    df['scores'] = model.decision_function(data.values)
    df['anomaly'] = model.predict(data.values)
    print(df)
    save_excel(repo_name, df[['id','title_len', 'body_len', 'labels', 'scores', 'anomaly']], "forest_semantic")

def forest_modify(repo_name):
    data_path = '../calculate/features/' + repo_name + '_feature.xlsx'
    df = pd.read_excel(data_path, sheet_name="activity")
    model = IsolationForest(n_estimators=100,
                            max_samples='auto',
                            contamination=float(0.1),
                            max_features=1.0)
    data = df[['commit_number', 'changed_file_type_count', 'changed_file_num', 'total_add_line', 'total_delete_line']]
    model.fit(data.values)
    df['scores'] = model.decision_function(data.values)
    df['anomaly'] = model.predict(data.values)
    print(df)
    save_excel(repo_name, df[['id', 'commit_number', 'changed_file_type_count', 'changed_file_num', 'total_add_line', 'total_delete_line', 'scores', 'anomaly']], "modify")

def forest_delivery(repo_name):
    data_path = '../calculate/features/' + repo_name + '_feature.xlsx'
    df = pd.read_excel(data_path, sheet_name="activity")
    model = IsolationForest(n_estimators=100,
                            max_samples='auto',
                            contamination=float(0.1),
                            max_features=1.0)
    data = df[['last_time_interval']]
    model.fit(data.values)
    df['scores'] = model.decision_function(data.values)
    df['anomaly'] = model.predict(data.values)
    print(df)
    save_excel(repo_name, df[['id', 'last_time_interval', 'scores', 'anomaly']], "delivery")

def forest_flow(repo_name):
    data_path = '../calculate/features/' + repo_name + '_feature.xlsx'
    df = pd.read_excel(data_path, sheet_name="flow_activity")
    model = IsolationForest(n_estimators=100,
                            max_samples='auto',
                            contamination=float(0.1),
                            max_features=1.0)
    model.fit(df[['run_time']].values)
    df['scores'] = model.decision_function(df[['run_time']].values)
    df['anomaly'] = model.predict(df[['run_time']].values)
    print(df)
    save_excel(repo_name, df, "flow")

def forest_comment(repo_name):
    data_path = '../calculate/features/' + repo_name + '_feature.xlsx'
    df = pd.read_excel(data_path, sheet_name="activity")
    model = IsolationForest(n_estimators=100,
                            max_samples='auto',
                            contamination=float(0.1),
                            max_features=1.0)
    data = df[['review_response_time', 'review_comments_number', 'comments_number']]
    model.fit(data.values)
    df['scores'] = model.decision_function(data.values)
    df['anomaly'] = model.predict(data.values)
    print(df)
    save_excel(repo_name, df[['id', 'review_response_time', 'review_comments_number', 'comments_number', 'scores', 'anomaly']], "comment")

def forest_close(repo_name):
    data_path = '../calculate/features/' + repo_name + '_feature.xlsx'
    df = pd.read_excel(data_path, sheet_name="activity")
    model = IsolationForest(n_estimators=100,
                            max_samples='auto',
                            contamination=float(0.1),
                            max_features=1.0)
    model.fit(df[['create_close']].values)
    df['scores'] = model.decision_function(df[['create_close']].values)
    df['anomaly'] = model.predict(df[['create_close']].values)
    print(df)
    save_excel(repo_name, df[['id', 'create_close', 'scores', 'anomaly']], "close")

def save_excel(repo_name, data, sheet_name):
    filePath = '../data/'
    path_exists_or_create(filePath)
    if os.path.exists(filePath + repo_name + '_activity_score.xlsx'):
        with pd.ExcelWriter(filePath + repo_name + '_activity_score.xlsx', mode='a', engine='openpyxl',
                            if_sheet_exists='replace') as writer:
            data.to_excel(writer, sheet_name=sheet_name, index=False)
    else:
        data.to_excel(filePath + repo_name + '_activity_score.xlsx', sheet_name=sheet_name, index=False)

forest_semantic("phoenix")
forest_modify("phoenix")
forest_delivery("phoenix")
forest_flow("phoenix")
forest_comment("phoenix")
forest_close("phoenix")