import pandas as pd
from typing import List

def cal_mean(l: List):
    sum = 0
    len = 0
    for v in l:
        if pd.isna(v):
            continue
        sum += v
        len += 1
    if len == 0:
        return 0
    return sum * 1.0 / len