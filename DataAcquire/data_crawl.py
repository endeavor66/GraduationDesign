from utils.gharchive_utils import get_client, join_commits_sha
from utils.mysql_utils import insert_batch
from datetime import datetime, timedelta


'''
功能：通过GHArchive client查询特定仓库repo在特定时间段(start_time, end_time)的数据，结果保存到数据库。以一天作为一个批次执行插入
'''
def crawl_data(repo: str, start_time: datetime, end_time: datetime):
    gh = get_client()
    start = start_time
    end = min(end_time, start + timedelta(hours=23))
    while start <= end_time:
        print(f"start: {start}, end: {end}")
        # 1.通过GHArchive client查询数据
        archive = gh.get(start, end, filters=[('repo.name', repo)])
        print("一共查询到 %d 条数据" % len(archive.data))
        # 2.提取关键活动，封装为元组list
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
                 archive_element.payload.pull_request.number if archive_element.payload.pull_request is not None else None,
                 archive_element.payload.forkee.full_name if archive_element.payload.forkee is not None else None,
                 archive_element.payload.changes,
                 # PullRequestReviewEvent相关
                 archive_element.payload.review.state if archive_element.payload.review is not None else None,
                 archive_element.payload.review.author_association if archive_element.payload.review is not None else None,
                 # MemberEvent相关
                 archive_element.payload.member.id if archive_element.payload.member is not None else None,
                 archive_element.payload.member.login if archive_element.payload.member is not None else None,
                 archive_element.payload.member.type if archive_element.payload.member is not None else None,
                 archive_element.payload.member.site_admin if archive_element.payload.member is not None else None
                 )
            datas.append(t)
        # 3.保存到数据库
        insert_batch("events", datas, repo[repo.index('/') + 1:])
        del archive
        del datas
        # 4.更新新一轮的时间段
        start = start + timedelta(days=1)
        end = min(end_time, start + timedelta(hours=23))


if __name__ == '__main__':
    repo = 'tensorflow/tensorflow'
    start_time = datetime(2021, 12, 11, 1, 0, 0)  # 年，月，日，时，分，秒 其中年，月，日是必须的
    end_time = datetime(2021, 12, 13, 5, 0, 0)
    crawl_data(repo, start_time, end_time)
