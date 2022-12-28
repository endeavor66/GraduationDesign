from google.cloud import bigquery
from datetime import datetime, timedelta
import pandas as pd
import time
import os

def crawl_data(cur: str):
    if os.path.exists(f"data/{cur}.csv"):
        return

    # Perform a query.
    query = f"""
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
        FROM  `githubarchive.day.{cur}` 
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

    index = 1
    while True:
        try:
            results = client.query(query)

            datas = []
            for row in results:
                record = list(row)
                record[3] = record[3].strftime('%Y-%m-%d %H:%M:%S')
                datas.append(record)

            columns = ['id', 'type', 'public', 'create_at', 'actor_id', 'actor_login', 'repo_id', 'repo_name', 'payload_ref',
                       'payload_ref_type', 'payload_pusher_type', 'payload_push_id', 'payload_size', 'payload_distinct_size',
                       'payload_commits', 'payload_action', 'payload_pr_number', 'payload_forkee_full_name', 'payload_changes',
                       'payload_review_state', 'payload_review_author_association', 'payload_member_id', 'payload_member_login',
                       'payload_member_type', 'payload_member_site_admin']
            df_file = pd.DataFrame(data=datas, columns=columns)
            df_file.to_csv(f"data/{cur}.csv", index=False, header=True, encoding="utf_8_sig")
            return
        except Exception as e:
            if str(e).__contains__('403 Quota exceeded'):
                print(f"403 Quota exceeded, it will retry after 10 min, this is {index} retry")
                time.sleep(600)
                index += 1



if __name__ == '__main__':
    client = bigquery.Client()
    start = datetime(2021, 1, 1)  # 年，月，日，时，分，秒 其中年，月，日是必须的
    end = datetime(2021, 2, 1)
    while start < end:
        cur = start.strftime("%Y%m%d")
        crawl_data(cur)
        print(f"{cur} process done")
        start = start + timedelta(days=1)

