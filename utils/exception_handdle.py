# 处理无法获得的PR，然后记录到文件中，记录格式如下，时间|PRNumber，时间|PR编号
import os
import time

# 将异常写到文件中
from utils.path_exist import path_exists_or_create


def write_file(exception, filename):
    current_path = os.getcwd() + '\\exception_data\\'  # 获取当前路径
    path_exists_or_create(current_path)
    # print(current_path)
    filepath = current_path + filename  # 在当前路径创建名为test的文本文件
    now_time = time.strftime('%Y-%m-%d %H:%M:%S ', time.localtime(time.time()))  # 获取当前时间
    context = now_time + ', ' + exception + "\n"
    print(context)

    with open(filepath, 'a+') as writer:
        writer.write(context)

# 写入数据库时，去操作一下
write_file("connect failed", "android.csv")
