import os.path

from java_project.project_spider.database_operation import get_database_connection
import json
import pandas as pd
from datetime import datetime
from utils.time_utils import time_reverse
from utils.path_exist import path_exists_or_create
from dagmm2.cal_score import score
from FigurePrinter import printImage
user_index = 0
pr_index = 1
author_association_index = 2
title_index = 3
body_index = 4
labels_index = 5
created_index = 6
closed_index = 7
merged_index = 8
assignees_index = 9
reviewer_index = 10
comments_number_index = 11
comments_content_index = 12
review_number_index = 13
review_content_index = 14
commit_number_index = 15
file_num_index = 16
add_line_index = 17
delete_line_index = 18
issue_index = 19
def get_project(repo_name):
    class DateEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, datetime):
                return obj.strftime("%Y-%m-%d %H:%M:%S")
            else:
                return json.JSONEncoder.default(self, obj)
    select_pr_self_sql = "SELECT DISTINCT pr_user_id, pr_number, pr_author_association, title, body, labels, created_at, closed_at, merged, assignees_content, " \
                         "requested_reviewers_content, comments_number, comments_content, review_comments_number, review_comments_content, " \
                         "commit_number, changed_file_num, total_add_line, total_delete_line, issue_url from " + repo_name + "_self"
    database = get_database_connection()
    # database = db.connect(host='127.0.0.1', port=3306, user='root', password='root', db='pr_second', charset='utf8')
    # 创建游标对象
    cursor = database.cursor()
    database.ping(reconnect=True)
    # 利用游标对象进行操作
    cursor.execute(select_pr_self_sql)
    data = cursor.fetchall()
    data_len = data.__len__()
    map = {}
    map_excel = {}
    for index in range(data_len):
        # 取出查询的数据
        pr_user_id = data[index][user_index]
        pr_number = data[index][pr_index]
        if map.__contains__(pr_user_id):
            print(pr_user_id.__str__() + "已存在")
        else:
            map[pr_user_id] = {}
            map_excel[pr_user_id] = {}
            map_excel[pr_user_id]["file_set"] = set()




        map[pr_user_id][pr_number] = {}
        map[pr_user_id][pr_number]["author_association_with_repo"] = data[index][author_association_index]
        map[pr_user_id][pr_number]["title"] = data[index][title_index]
        map[pr_user_id][pr_number]["body"] = data[index][body_index]
        map[pr_user_id][pr_number]["labels"] = data[index][labels_index]
        map[pr_user_id][pr_number]["created_at"] = data[index][created_index]
        map[pr_user_id][pr_number]["closed_at"] = data[index][closed_index]
        map[pr_user_id][pr_number]["merged"] = data[index][merged_index]
        map[pr_user_id][pr_number]["assignees_content"] = data[index][assignees_index]
        map[pr_user_id][pr_number]["requested_reviewers_content"] = data[index][reviewer_index]
        map[pr_user_id][pr_number]["comments_number"] = data[index][comments_number_index]

        comments_json = json.loads(data[index][comments_content_index])

        comments_content_map_list = []
        for i in range(comments_json.__len__()):
            comments_content_map = {}
            comments_content_map["id"] = comments_json[i]["id"]
            comments_content_map["user_id"] = comments_json[i]["user"]["id"]
            comments_content_map["created_at"] = comments_json[i]["created_at"]
            comments_content_map["author_association"] = comments_json[i]["author_association"]
            comments_content_map["body"] = comments_json[i]["body"]
            comments_content_map_list.append(comments_content_map)
        map[pr_user_id][pr_number]["comments_content"] = comments_content_map_list

        map[pr_user_id][pr_number]["review_comments_number"] = data[index][review_number_index]

        review_comments_json = json.loads(data[index][review_content_index])
        review_comments_content_map_list = []
        for i in range(review_comments_json.__len__()):
            review_comments_content_map = {}
            review_comments_content_map["id"] = review_comments_json[i]["id"]
            if review_comments_json[i]["user"] is None:
                review_comments_content_map["user_id"] = -1
            else:
                review_comments_content_map["user_id"] = review_comments_json[i]["user"]["id"]
            review_comments_content_map["created_at"] = review_comments_json[i]["created_at"]
            review_comments_content_map["author_association"] = review_comments_json[i]["author_association"]
            review_comments_content_map["body"] = review_comments_json[i]["body"]
            review_comments_content_map_list.append(review_comments_content_map)
        map[pr_user_id][pr_number]["review_comments_content"] = review_comments_content_map_list

        map[pr_user_id][pr_number]["commit_number"] = data[index][commit_number_index]
        map[pr_user_id][pr_number]["changed_file_num"] = data[index][file_num_index]
        select_pr_file_sql = "SELECT changed_file_name, changed_file_status, lines_added_num, lines_deleted_num from " + repo_name + "_file where pr_number = " + pr_number.__str__()
        cursor.execute(select_pr_file_sql)
        file_data = cursor.fetchall()
        tmp_map = {}
        for tmp_index in range(file_data.__len__()):
            tmp_map["changed_file_name"] = file_data[tmp_index][0]
            tmp_map["changed_file_status"] = file_data[tmp_index][1]
            tmp_map["lines_added_num"] = file_data[tmp_index][2]
            tmp_map["lines_deleted_num"] = file_data[tmp_index][3]
        map[pr_user_id][pr_number]["files"] = tmp_map
        map[pr_user_id][pr_number]["total_add_line"] = data[index][add_line_index]
        map[pr_user_id][pr_number]["total_delete_line"] = data[index][delete_line_index]
        map[pr_user_id][pr_number]["issue_url"] = data[index][issue_index]

        if data[index][title_index] is not None:
            map_excel[pr_user_id]["title_len"] = map_excel[pr_user_id].setdefault("title_len", 0) + len(data[index][title_index])
        else:
            map_excel[pr_user_id]["title_len"] = map_excel[pr_user_id].setdefault("title_len", 0)
        if data[index][body_index] is not None:
            map_excel[pr_user_id]["body_len"] = map_excel[pr_user_id].setdefault("body_len", 0) + len(data[index][body_index])
        else:
            map_excel[pr_user_id]["body_len"] = map_excel[pr_user_id].setdefault("body_len", 0)

        map_excel[pr_user_id]["labels"] = map_excel[pr_user_id].setdefault("labels", 0) + json.loads(data[index][labels_index]).__len__()

        map_excel[pr_user_id]["pr_nums"] = map_excel[pr_user_id].setdefault("pr_nums", 0) + 1
        map_excel[pr_user_id]["commit_number"] = map_excel[pr_user_id].setdefault("commit_number", 0) + data[index][
            commit_number_index]

        select_file_sql = "SELECT changed_file_name from " + repo_name + "_file where pr_number=" + pr_number.__str__()
        cursor.execute(select_file_sql)
        data_file = cursor.fetchall()
        data_file_len = data_file.__len__()
        if len(map_excel[pr_user_id]["file_set"]) == 0:
            map_excel[pr_user_id]["changed_file_type_count"] = map_excel[pr_user_id].setdefault("changed_file_type_count", 0)
            for index_file in range(data_file_len):
                file_names = data_file[index_file][0].split('.')
                if len(file_names) >= 2:
                    file_suffix = file_names[len(file_names) - 1]
                else:
                    file_suffix = file_names[0]
                map_excel[pr_user_id]["file_set"].add(file_suffix)
        else:
            for index_file in range(data_file_len):
                file_names = data_file[index_file][0].split('.')
                if len(file_names) >= 2:
                    file_suffix = file_names[len(file_names) - 1]
                else:
                    file_suffix = file_names[0]
                if file_suffix not in map_excel[pr_user_id]["file_set"]:
                    print(map_excel[pr_user_id]["file_set"])
                    print(file_suffix)
                    map_excel[pr_user_id]["changed_file_type_count"] = map_excel[pr_user_id]["changed_file_type_count"] + 1
                    map_excel[pr_user_id]["file_set"].add(file_suffix)

        map_excel[pr_user_id]["changed_file_num"] = map_excel[pr_user_id].setdefault("changed_file_num", 0) + data[index][
            file_num_index]
        map_excel[pr_user_id]["total_add_line"] = map_excel[pr_user_id].setdefault("total_add_line", 0) + data[index][
            add_line_index]
        map_excel[pr_user_id]["total_delete_line"] = map_excel[pr_user_id].setdefault("total_delete_line", 0) + data[index][
            delete_line_index]
        if review_comments_json.__len__() > 0:
            map_excel[pr_user_id]["review_response_time"] = map_excel[pr_user_id].setdefault("review_response_time", 0) + \
                                                            (time_reverse(review_comments_json[0]["created_at"]) - (data[index][created_index])).total_seconds()
        else:
            if data[index][closed_index] is None:
                map_excel[pr_user_id]["review_response_time"] = map_excel[pr_user_id].setdefault("review_response_time",0) + \
                                                                (datetime.now() - (data[index][created_index])).total_seconds()
            else:
                #print(data[index][closed_index], data[index][created_index])
                map_excel[pr_user_id]["review_response_time"] = map_excel[pr_user_id].setdefault("review_response_time",0) + \
                                                            ((data[index][closed_index]) - (data[index][created_index])).total_seconds()
        map_excel[pr_user_id]["comments_number"] = map_excel[pr_user_id].setdefault("comments_number", 0) + data[index][
            comments_number_index]
        map_excel[pr_user_id]["review_comments_number"] = map_excel[pr_user_id].setdefault("review_comments_number", 0) + data[index][
            review_number_index]
        if data[index][closed_index] is None:
            map_excel[pr_user_id]["user_efficiency_sum"] = map_excel[pr_user_id].setdefault("user_efficiency_sum",0) + (datetime.now() - (data[index][created_index])).total_seconds()
        else:
            map_excel[pr_user_id]["user_efficiency_sum"] = map_excel[pr_user_id].setdefault("user_efficiency_sum",0) + \
                                                           (data[index][closed_index] - data[index][created_index]).total_seconds()
        map_excel[pr_user_id]["merged_sum"] = map_excel[pr_user_id].setdefault("merged_sum", 0) + data[index][merged_index]

    select_workflow_sql = "SELECT status, conclusion, user_id from " + repo_name + "_flow"
    cursor.execute(select_workflow_sql)
    data = cursor.fetchall()
    data_len = data.__len__()
    for index in range(data_len):
        user_id = data[index][2]
        if map_excel.__contains__(user_id):
            map_excel[user_id]["flow_nums"] = map_excel[user_id].setdefault("flow_nums", 0) + 1
            if data[index][1] is not None:
                if data[index][1] == "success":
                    map_excel[user_id]["flow_success_sum"] = map_excel[user_id].setdefault("flow_success_sum", 0) + 1



    map_excel_final = {}
    for key in map_excel.keys():
        map_excel_final[key] = {}
        map_excel_final[key]["title_len_average"] = map_excel[key]["title_len"] / map_excel[key]["pr_nums"]
        map_excel_final[key]["body_len_average"] = map_excel[key]["body_len"] / map_excel[key]["pr_nums"]
        map_excel_final[key]["labels_average"] = map_excel[key]["labels"] / map_excel[key]["pr_nums"]
        map_excel_final[key]["commit_number_average"] = map_excel[key]["commit_number"] / map_excel[key]["pr_nums"]
        map_excel_final[key]["changed_file_type_average"] = map_excel[key]["changed_file_type_count"] / map_excel[key]["pr_nums"]
        map_excel_final[key]["changed_file_num_average"] = map_excel[key]["changed_file_num"] / map_excel[key]["pr_nums"]
        map_excel_final[key]["total_add_line_average"] = map_excel[key]["total_add_line"] / map_excel[key]["pr_nums"]
        map_excel_final[key]["total_delete_line_average"] = map_excel[key]["total_delete_line"] / map_excel[key]["pr_nums"]
        map_excel_final[key]["review_response_time_average"] = map_excel[key]["review_response_time"] / map_excel[key]["pr_nums"]
        map_excel_final[key]["comments_number_average"] = map_excel[key]["comments_number"] / map_excel[key]["pr_nums"]
        map_excel_final[key]["review_comments_number_average"] = map_excel[key]["review_comments_number"] / map_excel[key]["pr_nums"]
        map_excel_final[key]["user_efficiency_average"] = map_excel[key]["user_efficiency_sum"] / map_excel[key]["pr_nums"]
        map_excel_final[key]["merged_average"] = map_excel[key]["merged_sum"] / map_excel[key]["pr_nums"]
        map_excel_final[key]["flow_success_average"] = map_excel[key].setdefault("flow_success_sum", 0) / map_excel[key].setdefault("flow_nums", 1)

    data = pd.DataFrame.from_dict(map_excel_final)
    print(data)
    df = pd.DataFrame(data.values.T, columns=data.index, index=data.columns)
    print(df)
    project_normalization(df, repo_name, "project")

def get_activity(repo_name, important_user_id):
    class DateEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, datetime):
                return obj.strftime("%Y-%m-%d %H:%M:%S")
            else:
                return json.JSONEncoder.default(self, obj)
    select_pr_self_sql = "SELECT DISTINCT pr_user_id, pr_number, pr_author_association, title, body, labels, created_at, closed_at, merged, assignees_content, " \
                         "requested_reviewers_content, comments_number, comments_content, review_comments_number, review_comments_content, " \
                         "commit_number, changed_file_num, total_add_line, total_delete_line, issue_url from " + repo_name + "_self"
    database = get_database_connection()
    # database = db.connect(host='127.0.0.1', port=3306, user='root', password='root', db='pr_second', charset='utf8')
    # 创建游标对象
    cursor = database.cursor()
    database.ping(reconnect=True)
    # 利用游标对象进行操作
    cursor.execute(select_pr_self_sql)
    data = cursor.fetchall()
    data_len = data.__len__()
    map = {}
    map_user = {}
    for index in range(data_len):
        # 取出查询的数据
        pr_user_id = data[index][user_index]
        pr_number = data[index][pr_index]
        if pr_user_id not in map_user:
            map_user[pr_user_id] = {}
            map_user[pr_user_id]["file_set"] = set()
            map_user[pr_user_id]["pr_created_list"] = list()


        map[pr_number] = {}
        if data[index][title_index] is not None:
            map[pr_number]["title_len"] = len(data[index][title_index])
        else:
            map[pr_number]["title_len"] = 0
        if data[index][body_index] is not None:
            map[pr_number]["body_len"] = len(data[index][body_index])
        else:
            map[pr_number]["body_len"] = 0
        map[pr_number]["labels"] = json.loads(data[index][labels_index]).__len__()

        map[pr_number]["commit_number"] = data[index][commit_number_index]
        select_file_sql = "SELECT changed_file_name from " + repo_name + "_file where pr_number=" + pr_number.__str__()
        cursor.execute(select_file_sql)
        data_file = cursor.fetchall()
        data_file_len = data_file.__len__()
        if len(map_user[pr_user_id]["file_set"]) == 0:
            map[pr_number]["changed_file_type_count"] = 0
            for index_file in range(data_file_len):
                file_names = data_file[index_file][0].split('.')
                if len(file_names) >= 2:
                    file_suffix = file_names[len(file_names) - 1]
                else:
                    file_suffix = file_names[0]
                map_user[pr_user_id]["file_set"].add(file_suffix)
        else:
            if data_file_len == 0:
                map[pr_number]["changed_file_type_count"] = 0
            else:
                for index_file in range(data_file_len):
                    file_names = data_file[index_file][0].split('.')
                    if len(file_names) >= 2:
                        file_suffix = file_names[len(file_names) - 1]
                    else:
                        file_suffix = file_names[0]
                    if file_suffix not in map_user[pr_user_id]["file_set"]:
                        #print(map_user[pr_user_id]["file_set"])
                        #print(file_suffix)
                        map[pr_number]["changed_file_type_count"] = map[pr_number].setdefault("changed_file_type_count", 0)+1
                        map_user[pr_user_id]["file_set"].add(file_suffix)
                map[pr_number].setdefault("changed_file_type_count", 0)
        map[pr_number]["changed_file_num"] = data[index][file_num_index]
        map[pr_number]["total_add_line"] = data[index][add_line_index]
        map[pr_number]["total_delete_line"] = data[index][delete_line_index]

        if len(map_user[pr_user_id]["pr_created_list"]) == 0:
            map[pr_number]["last_time_interval"] = 0
            map_user[pr_user_id]["pr_created_list"].append(data[index][created_index])
        else:
            map[pr_number]["last_time_interval"] = (data[index][created_index] - map_user[pr_user_id]["pr_created_list"][len(map_user[pr_user_id]["pr_created_list"])-1]).total_seconds()
            map_user[pr_user_id]["pr_created_list"].append(data[index][created_index])
        review_comments_json = json.loads(data[index][review_content_index])
        if review_comments_json.__len__() > 0:
            map[pr_number]["review_response_time"] = (time_reverse(review_comments_json[0]["created_at"]) - (data[index][created_index])).total_seconds()
        else:
            if data[index][closed_index] is None:
                map[pr_number]["review_response_time"] = (datetime.now() - (data[index][created_index])).total_seconds()
            else:
                map[pr_number]["review_response_time"] = ((data[index][closed_index]) - (data[index][created_index])).total_seconds()
        map[pr_number]["review_comments_number"] = data[index][review_number_index]
        map[pr_number]["comments_number"] = data[index][comments_number_index]

        if data[index][closed_index] is None:
            map[pr_number]["create_close"] = (datetime.now() - (data[index][created_index])).total_seconds()
        else:
            map[pr_number]["create_close"] = (data[index][closed_index] - data[index][created_index]).total_seconds()

    data = pd.DataFrame.from_dict(map)
    print(data)
    df = pd.DataFrame(data.values.T, columns=data.index, index=data.columns)
    print(df)
    project_normalization(df, repo_name, "activity")

    select_workflow_sql = "SELECT run_id, conclusion, user_id, created_at, updated_at from " + repo_name + "_flow"
    cursor.execute(select_workflow_sql)
    data = cursor.fetchall()
    data_len = data.__len__()
    map_flow = {}
    for index in range(data_len):
        user_id = data[index][2]
        run_id = data[index][0]
        if data[index][1] is not None:
            map_flow[run_id] = {}
            map_flow[run_id]["user_id"] = user_id
            map_flow[run_id]["run_time"] = (data[index][4] - data[index][3]).total_seconds()

    data_flow = pd.DataFrame.from_dict(map_flow)
    print(data_flow)
    df = pd.DataFrame(data_flow.values.T, columns=data_flow.index, index=data_flow.columns)
    print(df)
    for col in df.columns:
        print(df[col].max() - df[col].min())
        df[col] = (df[col] - df[col].min()) / (df[col].max() - df[col].min())
    df.index.name = 'id'
    filePath = './features/'
    path_exists_or_create(filePath)
    if os.path.exists(filePath + repo_name + '_feature.xlsx'):
        with pd.ExcelWriter(filePath + repo_name + '_feature.xlsx', mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name="flow_activity", index=True)
    else:
        df.to_excel(filePath + repo_name + '_feature.xlsx', sheet_name="flow_activity", index=True)
def project_normalization(data, repo_name, sheet_name):
    for col in data.columns:
        print(data[col].max() - data[col].min())
        data[col] = (data[col] - data[col].min()) / (data[col].max() - data[col].min())

    print(data)
    data.index.name = 'id'
    filePath = './features/'
    path_exists_or_create(filePath)
    if os.path.exists(filePath + repo_name + '_feature.xlsx'):
        with pd.ExcelWriter(filePath + repo_name + '_feature.xlsx', mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            data.to_excel(writer, sheet_name=sheet_name, index=True)
    else:
        data.to_excel(filePath + repo_name + '_feature.xlsx', sheet_name=sheet_name, index=True)

if __name__ == "__main__" :
    repo_list = ["phoenix", "Katello"]#"phoenix","Katello"
    for repo_name in repo_list:
        get_project(repo_name)
        score(repo_name, "project")
        printImage(repo_name, "project")
        #get_activity(repo_name, 123)