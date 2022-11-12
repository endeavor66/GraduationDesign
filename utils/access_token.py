# 获取本地文件中的access_tocken相关值
def get_token(num=0):
    with open("token.txt", 'r') as reader:
        token = reader.readline()
    return token

print(get_token())
