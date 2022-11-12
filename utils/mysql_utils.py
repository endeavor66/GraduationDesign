import pymysql
from access_key import get_mysql_root_psw
from exception_handdle import write_file

TABLE_FIELDS = {
    "events": [
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
        "payload_number",
        "payload_changes",
        "payload_member_id",
        "payload_member_login",
        "payload_member_type",
        "payload_member_site_admin"]
}

# 获取数据库连接对象
username, password = get_mysql_root_psw()
conn = pymysql.connect(host='127.0.0.1', port=3306, user=username, password=password, db='poison', charset='utf8')
cursor = conn.cursor()


"""
功能：插入一条数据
输入
    table: 操作的表名，必须为TABLE_FIELDS的键
    data: 待插入的数据，格式: Tuple元组, eg.(0, 3132)
    project: 操作的项目，操作异常信息将记录在对于"exception_data/project_exception.csv文件中"
输出
    控制台打印过程信息，如：执行的SQL，SQL执行后的影响行数，SQL执行异常
"""
def insert_one(table, data, project):
    if table not in TABLE_FIELDS:
        print("不存在%s的字段信息" % table)
        return
    fields = ",".join(TABLE_FIELDS[table])
    fields_param = ("%s," * len(TABLE_FIELDS[table]))[0:-1]
    sql = "insert into " + table + "(" + fields + ")" + " values " + "(" + fields_param + ")"
    print("执行SQL: " + sql)

    try:
        conn.ping(reconnect=True)
        result = cursor.execute(sql, data)
        conn.commit()
        print("操作成功，插入%d条数据" % result)
    except Exception as e:
        conn.rollback()
        conn.close()
        print("数据库执行出错:" + str(e))
        write_file(exception=str(e), filename=project + "_exception.csv")


"""
功能：批量插入数据
输入
    table: 操作的表名，必须为TABLE_FIELDS的键
    datas: 待插入的数据，格式: List中数据类型必须为Tuple元组, eg.[(0, 3132), (1, 1298)]
    project: 操作的项目，操作异常信息将记录在对于"exception_data/project_exception.csv文件中"
输出
    控制台打印过程信息，如：执行的SQL，SQL执行后的影响行数，SQL执行异常
"""
def insert_batch(table, datas, project):
    if table not in TABLE_FIELDS:
        print("不存在%s的字段信息" % table)
        return
    fields = ",".join(TABLE_FIELDS[table])
    fields_param = ("%s," * len(TABLE_FIELDS[table]))[0:-1]
    sql = "insert into " + table + "(" + fields + ")" + " values " + "(" + fields_param + ")"
    print("执行SQL: " + sql)

    try:
        conn.ping(reconnect=True)
        result = cursor.executemany(sql, datas)
        conn.commit()
        print("操作成功，插入%d条数据" % result)
    except Exception as e:
        conn.rollback()
        conn.close()
        print("数据库执行出错:" + str(e))
        write_file(exception=str(e), filename=project + "_exception.csv")


def select_one():
    pass


def select_batch():
    pass

dts = [("11111113", "MemberEvent", 1, "2022-11-12 10:00:59", 1111, "endeavor66", 11111, "endeavor66/DMS",
                     "head/master", "branch", "pusher", 1111, 10, 10, "SHA1#SHA2#SHA3", "created", 111111,
                     "admin->reviewer", 11111111, "lisi", "admin", 0)]
insert_batch("events", dts, "test")
