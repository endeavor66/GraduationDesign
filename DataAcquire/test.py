import requests
from tqdm import tqdm
import gzip
import json

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
    filename = "2015-01-01-15.json.gz"
    url = "http://data.gharchive.org/" + filename
    download(url, filename)
    with gzip.open(filename, 'r') as reader:
        data_str = reader.read().decode("utf8")
        data_strs = [s for s in data_str.split("\n") if s]
        del data_str
        json_str = "[" + ", ".join(data_strs) + "]"
        del data_strs
        data = json.loads(json_str)
        print(data)

