import pymysql
from typing import List
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
COMMIT_TABLE_FIELDS = [
    "pr_number",
    "sha",
    "author",
    "author_email",
    "author_date",
    "committer",
    "committer_email",
    "committer_date",
    "message",
    "line_addition",
    "line_deletion",
    "file_edit_num",
    "file_content"]
PROCESS_EVENT_TABLE_FIELDS = [
    "repo",
    "pr_number",
    "activity",
    "created_at",
    "people",
    "scene"
]
PERMISSION_CHANGE_TABLE_FIELDS = [
    "repo",
    "people",
    "pr_number",
    "change_time",
    "permission"
]

# 获取数据库连接对象
username, password = get_mysql_root_psw()
conn = pymysql.connect(host='127.0.0.1', port=3306, user=username, password=password, db='poison', charset='utf8', cursorclass=pymysql.cursors.DictCursor)


"""
功能：批量插入到repo_events表中
"""
def batch_insert_into_events(repo: str, data: List):
    if len(data) == 0:
        print("数据为空，不需要插入到数据库")
        return
    table = f"{repo.replace('-', '_')}_events"
    fields = ",".join(EVENT_TABLE_FIELDS)
    fields_param = ("%s," * len(EVENT_TABLE_FIELDS))[0:-1]
    sql = f"insert into `{table}` ({fields}) values({fields_param})"
    # print("执行SQL: " + sql)

    cursor = conn.cursor()
    try:
        conn.ping(reconnect=True)
        result = cursor.executemany(sql, data)
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
功能：批量插入到process_events表中
"""
def batch_insert_into_process_events(repo: str, data: List):
    if len(data) == 0:
        print("数据为空，不需要插入到数据库")
        return
    fields = ",".join(PROCESS_EVENT_TABLE_FIELDS)
    fields_param = ("%s," * len(PROCESS_EVENT_TABLE_FIELDS))[0:-1]
    sql = f"insert into process_events ({fields}) values({fields_param})"
    # print("执行SQL: " + sql)

    cursor = conn.cursor()
    try:
        conn.ping(reconnect=True)
        result = cursor.executemany(sql, data)
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
功能：批量插入到permission_change表中
"""
def batch_insert_into_permission_change(repo: str, data: List):
    if len(data) == 0:
        print("数据为空，不需要插入到数据库")
        return
    fields = ",".join(PERMISSION_CHANGE_TABLE_FIELDS)
    fields_param = ("%s," * len(PERMISSION_CHANGE_TABLE_FIELDS))[0:-1]
    sql = f"insert into permission_change ({fields}) values({fields_param})"
    # print("执行SQL: " + sql)

    cursor = conn.cursor()
    try:
        conn.ping(reconnect=True)
        result = cursor.executemany(sql, data)
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


'''
功能：插入一条记录到repo_commit表中
'''
def insert_into_commit(repo: str, data: List):
    table = f"{repo.replace('-', '_')}_commit"
    fields = ",".join(COMMIT_TABLE_FIELDS)
    fields_param = ("%s," * len(COMMIT_TABLE_FIELDS))[0:-1]
    sql = f"insert into `{table}` ({fields}) values({fields_param})"
    # print("执行SQL: " + sql)

    cursor = conn.cursor()
    result = 0
    try:
        conn.ping(reconnect=True)
        result = cursor.execute(sql, data)
        conn.commit()
        cursor.close()
        # 如果在一次程序运行过程中多次调用该函数，可能会出问题，conn.close()关闭后后续可能无法获取连接
        # conn.close()
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        write_file(exception=str(e), filename=repo + "_exception.csv")
    return result


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


def create_table(repo: str):
    sql = f"""
        CREATE TABLE `{repo.replace('-', '_')}_events` (
          `id` varchar(255) NOT NULL DEFAULT '' COMMENT '事件的唯一标识',
          `type` varchar(255) DEFAULT NULL COMMENT '事件的类型',
          `public` tinyint(4) DEFAULT NULL COMMENT '事件是否对所有用户可见',
          `created_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '事件触发时间',
          `actor_id` varchar(255) DEFAULT NULL COMMENT 'actor的唯一标识',
          `actor_login` varchar(255) DEFAULT NULL COMMENT 'actor的用户名',
          `repo_id` varchar(255) DEFAULT NULL COMMENT '仓库的唯一标识',
          `repo_name` varchar(255) DEFAULT NULL COMMENT '仓库名',
          `payload_ref` varchar(255) DEFAULT NULL COMMENT 'git ref信息，例如，refs/heads/main',
          `payload_ref_type` varchar(255) DEFAULT NULL COMMENT 'either branch or tag',
          `payload_pusher_type` varchar(255) DEFAULT NULL,
          `payload_push_id` varchar(255) DEFAULT NULL COMMENT 'push的唯一标识',
          `payload_size` int(11) DEFAULT NULL COMMENT 'push包含的commit数量',
          `payload_distinct_size` int(11) DEFAULT NULL COMMENT 'push包含的distinct commit数量',
          `payload_commits` text COMMENT 'push或pull request包含的所有commit.sha，格式："SHA1#SHA2#SHA3"，#作为分隔符',
          `payload_action` varchar(255) DEFAULT NULL COMMENT '事件的子动作，如：added, edited, opened',
          `payload_pr_number` int(11) DEFAULT NULL COMMENT 'pull request的number，可以理解为标识符',
          `payload_forkee_full_name` varchar(255) DEFAULT NULL,
          `payload_changes` varchar(255) DEFAULT NULL COMMENT '事件的子动作导致的变更，如：permission变更信息',
          `payload_review_state` varchar(255) DEFAULT NULL,
          `payload_review_author_association` varchar(255) DEFAULT NULL,
          `payload_member_id` varchar(255) DEFAULT NULL COMMENT 'MemberEvent相关，被添加到组织的member.id',
          `payload_member_login` varchar(255) DEFAULT NULL COMMENT 'MemberEvent相关，被添加到组织的member.username',
          `payload_member_type` varchar(255) DEFAULT NULL COMMENT 'MemberEvent相关，被添加到组织的member.type',
          `payload_member_site_admin` tinyint(4) DEFAULT NULL COMMENT 'MemberEvent相关，被添加到组织的member.site_admin',
          PRIMARY KEY (`id`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;
    """
    conn.ping(reconnect=True)
    cursor = conn.cursor()
    cursor.execute(sql)
    cursor.close()
    conn.close()

if __name__ == '__main__':
    repos = ['hadoop', 'zookeeper', 'spring-framework', 'spring-cloud-function', 'vim',
             'gpac', 'ImageMagick', 'arm-trusted-firmware', 'libexpat', 'httpd', 'zlib',
             'redis', 'swtpm']
    for repo in repos:
        create_table(repo)
        print(f"{repo}_event table create done")
