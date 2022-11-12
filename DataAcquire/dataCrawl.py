from utils.gharchive_utils import get_client, join_commits_sha
from utils.mysql_utils import insert_batch

repo = 'tensorflow/tensorflow'

# 查询数据
gh = get_client()
data = gh.get('2021-12-01 08:00:00', '2022-12-01 12:00:00', filters=[
    ('repo.name', repo)
])

# 保存到数据库
datas = []
for archive in data:
    t = (archive.id,
         archive.type,
         archive.public,
         archive.created_at,
         archive.actor.id,
         archive.actor.login,
         archive.repo.id,
         archive.repo.name,
         archive.payload.ref,
         archive.payload.ref_type,
         archive.payload.pusher_type,
         archive.payload.push_id,
         archive.payload.size,
         archive.payload.distinct_size,
         join_commits_sha(archive.payload.commits),
         archive.payload.action,
         archive.payload.number,
         archive.payload.changes,
         archive.member.id if archive.member is not None else None,
         archive.member.login if archive.member is not None else None,
         archive.member.type if archive.member is not None else None,
         archive.member.site_admin if archive.member is not None else None
    )
    datas.append(t)

insert_batch("events", datas, repo[repo.index('/')+1:])






