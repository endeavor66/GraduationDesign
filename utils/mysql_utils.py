import pymysql
from utils.access_key import get_mysql_root_psw
from utils.exception_handdle import write_file

EVENT_TABLE_FIELDS = [
        "id",
        "type",
        "public",
        "created_at",
        "actor_id",
        "actor_login",
        "repo_id",
        "repo_name",
        "payload_ref",
        "payload_ref_type",
        "payload_pusher_type",
        "payload_push_id",
        "payload_size",
        "payload_distinct_size",
        "payload_commits",
        "payload_action",
        "payload_pr_number",
        "payload_forkee_full_name",
        "payload_changes",
        "payload_review_state",
        "payload_review_author_association",
        "payload_member_id",
        "payload_member_login",
        "payload_member_type",
        "payload_member_site_admin"]


# 获取数据库连接对象
username, password = get_mysql_root_psw()
conn = pymysql.connect(host='127.0.0.1', port=3306, user=username, password=password, db='poison', charset='utf8', cursorclass=pymysql.cursors.DictCursor)


"""
功能：批量插入数据
输入
    table: 操作的表名，必须为TABLE_FIELDS的键
    datas: 待插入的数据，格式: List中数据类型必须为Tuple元组, eg.[(0, 3132), (1, 1298)]
    project: 操作的项目，操作异常信息将记录在对于"exception_data/project_exception.csv文件中"
输出
    控制台打印过程信息，如：执行的SQL，SQL执行后的影响行数，SQL执行异常
"""
def insert_batch(repo, datas):
    table = f"{repo}_events"
    fields = ",".join(EVENT_TABLE_FIELDS)
    fields_param = ("%s," * len(EVENT_TABLE_FIELDS))[0:-1]
    sql = "insert into " + table + "(" + fields + ")" + " values " + "(" + fields_param + ")"
    # print("执行SQL: " + sql)

    cursor = conn.cursor()
    try:
        conn.ping(reconnect=True)
        result = cursor.executemany(sql, datas)
        conn.commit()
        cursor.close()
        # 如果在一次程序运行过程中多次调用该函数，可能会出问题，conn.close()关闭后后续可能无法获取连接
        # conn.close()
        print("操作成功，插入%d条数据" % result)
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        print("数据库执行出错:" + str(e))
        write_file(exception=str(e), filename=repo + "_exception.csv")


"""
对table执行sql查询
"""
def select_all(sql):
    # print("执行SQL: " + sql)
    conn.ping(reconnect=True)
    cursor = conn.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    return data


def select_one(sql):
    # print("执行SQL: " + sql)
    conn.ping(reconnect=True)
    cursor = conn.cursor()
    cursor.execute(sql)
    data = cursor.fetchone()
    cursor.close()
    conn.close()
    return data



# dts = [("11111113", "MemberEvent", 1, "2022-11-12 10:00:59", 1111, "endeavor66", 11111, "endeavor66/DMS",
#                      "head/master", "branch", "pusher", 1111, 10, 10, "SHA1#SHA2#SHA3", "created", 111111,
#                      "admin->reviewer", 11111111, "lisi", "admin", 0)]
# insert_batch("events", dts, "test")
