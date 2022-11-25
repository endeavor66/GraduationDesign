import pymysql
import requests
from utils.access_key import get_mysql_root_psw, get_token
from utils.exception_handdle import write_file

# 获取数据库连接对象
username, password = get_mysql_root_psw()
conn = pymysql.connect(host='127.0.0.1', port=3306, user=username, password=password, db='poison', charset='utf8', cursorclass=pymysql.cursors.DictCursor)

owner = 'tensorflow'
repo = 'tensorflow'
table = f"{repo}_self_back"

# 查询所有pr_number
select_sql = f"select pr_number from {table} where pr_number >= 27645"
cursor = conn.cursor()
conn.ping(reconnect=True)
print("执行SQL:" + select_sql)
cursor.execute(select_sql)
result = cursor.fetchall()

# 初始化变量
access_token = get_token()
headers = {'Authorization': 'token ' + access_token}
exception_filename = repo + '_exception.csv'

for pr in result:
    pr_number = pr['pr_number']
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}"
    pr_r = requests.get(url, headers=headers)
    print("pr_url: " + url + "  Status Code:", pr_r.status_code)
    # 如果返回的状态码以2开头，则说明正常此时去写入到数据库中即可
    if 200 <= pr_r.status_code < 300:
        pr_json_str = pr_r.json()
        # 补充字段
        head_ref = pr_json_str["head"]["ref"]
        head_repo_full_name = pr_json_str["head"]["repo"]["full_name"] if pr_json_str["head"]["repo"] is not None else None
        head_repo_fork = pr_json_str["head"]["repo"]["fork"] if pr_json_str["head"]["repo"] is not None else None
        base_ref = pr_json_str["base"]["ref"]
        base_repo_full_name = pr_json_str["base"]["repo"]["full_name"] if pr_json_str["base"]["repo"] is not None else None
        base_repo_fork = pr_json_str["base"]["repo"]["fork"] if pr_json_str["base"]["repo"] is not None else None
        # 执行更新操作
        try:
            update_sql = """update """ + table + """ set 
                    head_ref=%s, 
                    head_repo_full_name=%s,
                    head_repo_fork=%s,
                    base_ref=%s,
                    base_repo_full_name=%s,
                    base_repo_fork=%s 
                where pr_number=%s"""
            update_data = (head_ref,
                           head_repo_full_name,
                           head_repo_fork,
                           base_ref,
                           base_repo_full_name,
                           base_repo_fork,
                           pr_number)
            conn.ping(reconnect=True)
            cursor.execute(update_sql, update_data)
            conn.commit()
            print(f"pr_number:{pr_number}更新数据库成功")
        except Exception as e:
            conn.ping(reconnect=True)
            conn.rollback()
            print(e)
            print(f"pr_number:{pr_number}更新数据库失败")
            write_file(f"pr_number:{pr_number}更新数据库失败", exception_filename)
    else:
        print(f"pr_number:{pr_number} 请求状态码响应错误-{pr_r.status_code}")
        write_file(f"pr_number:{pr_number} 请求状态码响应错误-{pr_r.status_code}", exception_filename)

