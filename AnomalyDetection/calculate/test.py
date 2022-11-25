import difflib
# 优点：python自带模块，效率比较高
def similar_diff_ratio(str1, str2):
    return difflib.SequenceMatcher(None, str1, str2).ratio()

# quick_ratio()比ratio()计算效率更高，计算结果一致
def similar_diff_qk_ratio(str1, str2):
    return difflib.SequenceMatcher(None, str1, str2).quick_ratio()

# None参数是一个函数，用来去掉不需要比较的字符。比如，列表lst_str表示计算相似度时不需要比较的字符
def similar_diff_ratio_filter(lst_str, str1, str2):
    return difflib.SequenceMatcher(lambda x: x in lst_str, str1, str2).ratio()

print(similar_diff_ratio("临安区中小企业创业基地", "临安区电子商务科技园"))
print(similar_diff_qk_ratio("临安区中小企业创业基地", "临安区电子商务科技园"))
# 有一点疑问，将不需要比较的字符加入后，相似度计算结果没变化，欢迎大佬留言解惑，谢谢！
lst_str = ['临安区', '创业']
print(similar_diff_ratio_filter(lst_str, "临安区中小企业创业基地", "临安区电子商务科技园"))