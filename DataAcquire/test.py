from google.cloud import bigquery
import openpyxl
import datetime

BQ_TIMEOUT = 240
#max is 100 per project
BQ_THREAD_DRY_RUN_LIMIT = 20
BQ_QUERY_SLEEP_SECONDS = 1

# 认证文件，在BigQuery生成认证文件
AUTH_JSON_FILE_PATH = '123456.json'

# Client 认证
def bq_InitConnection():
    return bigquery.Client.from_service_account_json(AUTH_JSON_FILE_PATH)

# 向BigQuery请求数据
def bq_query(SQL):
    client = bq_InitConnection()

    # print(bqSql)
    bqJob = client.query(SQL)

    bqList = list(bqJob.result(timeout=BQ_TIMEOUT))  # Waits for job to complete.
    return bqList

# 处理BigQuery的数据，返回处理好的数据
def query_Collect(SQL):
    # print("yesterday is:", getYesterdayFbStr())
    retList = []
    bqListRet = bq_query(SQL)
    i = 0
    for listItem in bqListRet:
        item = list(listItem)
        retList.append(item)
        i += 1
    return retList

# 传入SQL查询命令，返回Sheet  (ts,uid)，此函数要根据查询得到的数据格式进行修改
def query_SaveSheet_TsUid(SQL, SheetTitle, FilePath):
    # 创建Excel的Sheet
    wb = openpyxl.Workbook()
    ws = wb.active
    # 向BigQuery查询
    bqListRet = bq_query(SQL)
    # 向Sheet第一行添加
    ws.append(["ts", "uid"])
    # 查询到的数据存入Excel
    i = 0
    for listItem in bqListRet:
        item = list(listItem)
        # 格式进行转换
        item[0] = datetime.datetime.strptime(str(item[0]), '%Y-%m-%d %H:%M:%S')
        item[1] = int(item[1])
        ws.append(item)
        i += 1
    # Sheet 保存
    ws.title = SheetTitle
    wb.save(FilePath)

    print(FilePath + "  has been downloaded")

if __name__ == '__main__':
    SQL = """
		select
          id,
          type,
          public,
          created_at,
          actor.id as actor_id,
          actor.login as actor_login,
          repo.id as repo_id,
          repo.name as repo_name,
          JSON_EXTRACT (payload, '$.ref') as payload_ref,
          JSON_EXTRACT (payload, '$.ref_type')  as payload_ref_type,
          JSON_EXTRACT (payload, '$.pusher_type')  as payload_pusher_type,
          JSON_EXTRACT (payload, '$.push_id')  as payload_push_id,
          JSON_EXTRACT (payload, '$.size')  as payload_size,
          JSON_EXTRACT (payload, '$.distinct_size')  as payload_distinct_size,
          JSON_EXTRACT (payload, '$.commits')  as payload_commits,
          JSON_EXTRACT (payload, '$.action')  as payload_action,
          JSON_EXTRACT (payload, '$.pull_request.number')  as payload_pr_number,
          JSON_EXTRACT (payload, '$.forkee.full_name')  as payload_forkee_full_name,
          JSON_EXTRACT (payload, '$.changes')  as payload_changes,
          JSON_EXTRACT (payload, '$.review.state')  as payload_review_state,
          JSON_EXTRACT (payload, '$.review.author_association')  as payload_review_author_association,
          JSON_EXTRACT (payload, '$.member.id')  as payload_member_id,
          JSON_EXTRACT (payload, '$.member.login')  as payload_member_login,
          JSON_EXTRACT (payload, '$.member.type')  as payload_member_type,
          JSON_EXTRACT (payload, '$.member.site_admin')  as payload_member_site_admin  
        FROM  `githubarchive.day.20150102` 
        where repo.name like '%/tensorflow' or repo.name like '%/opencv' or repo.name like '%/cocos2d-x' 
         or repo.name like '%/dubbo' or repo.name like '%/zipkin' or repo.name like '%/incubator-heron' 
         or repo.name like '%/netbeans' or repo.name like '%/moby' or repo.name like '%/terraform' 
         or repo.name like '%/kuma' or repo.name like '%/scikit-learn' or repo.name like '%/ipython' 
         or repo.name like '%/helix' or repo.name like '%/react' or repo.name like '%/yii2'  
         or repo.name like '%/katello' or repo.name like '%/phoenix' 
         or repo.name like '%/hadoop' or repo.name like '%/zookeeper' or repo.name like '%/spring-framework' 
         or repo.name like '%/spring-cloud-function' or repo.name like '%/vim' or repo.name like '%/gpac' 
         or repo.name like '%/ImageMagick' or repo.name like '%/arm-trusted-firmware' or repo.name like '%/libexpat' 
         or repo.name like '%/httpd' or repo.name like '%/zlib' or repo.name like '%/redis' 
         or repo.name like '%/swtpm' 
		"""
    # 直接查询
    res = query_Collect(SQL)
    print(res)
    print(res[0][0])
    # 将查询保存为Excel
    # query_SaveSheet_TsUid(SQL, SheetTitle="Online", FilePath=".\Online.xlsx")
