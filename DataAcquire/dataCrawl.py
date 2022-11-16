from utils.gharchive_utils import get_client, join_commits_sha
from utils.mysql_utils import insert_batch
from datetime import datetime

repo = 'tensorflow/tensorflow'

# 查询数据
start = datetime.now()

gh = get_client()
archive = gh.get('2021-12-01 01:00:00', '2021-12-01 05:00:00', filters=[
    ('repo.name', repo)
])

end = datetime.now()
print("一共查询到 %d 条数据, 耗时 %d s" % (len(archive.data), (end-start).seconds))

# 保存到数据库
datas = []
for archive_element in archive:
    t = (archive_element.id,
         archive_element.type,
         archive_element.public,
         archive_element.created_at,
         archive_element.actor.id,
         archive_element.actor.login,
         archive_element.repo.id,
         archive_element.repo.name,
         archive_element.payload.ref,
         archive_element.payload.ref_type,
         archive_element.payload.pusher_type,
         archive_element.payload.push_id,
         archive_element.payload.size,
         archive_element.payload.distinct_size,
         join_commits_sha(archive_element.payload.commits),
         archive_element.payload.action,
         archive_element.payload.pr_number,
         archive_element.payload.changes,
         archive_element.member.id if archive_element.member is not None else None,
         archive_element.member.login if archive_element.member is not None else None,
         archive_element.member.type if archive_element.member is not None else None,
         archive_element.member.site_admin if archive_element.member is not None else None
    )
    datas.append(t)

start = datetime.now()
insert_batch("events", datas, repo[repo.index('/')+1:])
end = datetime.now()
print("插入数据库耗时 %d s" % (end-start).seconds)






