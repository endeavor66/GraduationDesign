import requests
from tqdm import tqdm

def download(url: str, fname: str):
    # 用流stream的方式获取url的数据
    resp = requests.get(url, stream=True)
    # 拿到文件的长度，并把total初始化为0
    total = int(resp.headers.get('content-length', 0))
    # 打开当前目录的fname文件(名字你来传入)
    # 初始化tqdm，传入总数，文件名等数据，接着就是写入，更新等操作了
    with open(fname, 'wb') as file, tqdm(
        desc=fname,
        total=total,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in resp.iter_content(chunk_size=1024):
            size = file.write(data)
            bar.update(size)


if __name__ == "__main__":
    # 下载文件，并传入文件名
    filename = "2021-12-01-16.json.gz"
    url = "https://data.gharchive.org/" + filename
    download(url, filename)

# import requests
# from contextlib import closing
#
# class ProgressBar(object):
#
#     def __init__(self, title,
#                  count=0.0,
#                  run_status=None,
#                  fin_status=None,
#                  total=100.0,
#                  unit='', sep='/',
#                  chunk_size=1.0):
#         super(ProgressBar, self).__init__()
#         self.info = "【%s】%s %.2f %s %s %.2f %s"
#         self.title = title
#         self.total = total
#         self.count = count
#         self.chunk_size = chunk_size
#         self.status = run_status or ""
#         self.fin_status = fin_status or " " * len(self.status)
#         self.unit = unit
#         self.seq = sep
#
#     def __get_info(self):
#         # 【名称】状态 进度 单位 分割线 总数 单位
#         _info = self.info % (self.title, self.status,
#                              self.count/self.chunk_size, self.unit, self.seq, self.total/self.chunk_size, self.unit)
#         return _info
#
#     def refresh(self, count=1, status=None):
#         self.count += count
#         # if status is not None:
#         self.status = status or self.status
#         end_str = "\r"
#         if self.count >= self.total:
#             end_str = '\n'
#             self.status = status or self.fin_status
#         print(self.__get_info(), end=end_str)
#
# filename = "2021-12-01-16.json.gz"
# url = "https://data.gharchive.org/" + filename
#
# with closing(requests.get(url, stream=True)) as response:
#     chunk_size = 1024 # 单次请求最大值
#     content_size = int(response.headers['content-length']) # 内容体总大小
#     progress = ProgressBar(filename, total=content_size,
#                                      unit="KB", chunk_size=chunk_size, run_status="正在下载", fin_status="下载完成")
#     with open(filename, "wb") as file:
#        for data in response.iter_content(chunk_size=chunk_size):
#            file.write(data)
#            progress.refresh(count=len(data))
#
