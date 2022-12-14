from sklearn.metrics import confusion_matrix, precision_recall_fscore_support


'''
功能：计算F值
'''
def cal_f_measure(precision, recall):
    if precision == 0 or recall == 0:
        f1, f2 = 0, 0
    else:
        f1 = 2 * precision * recall / (precision + recall)
        f2 = 5 * precision * recall / (4 * precision + recall)
        # f05 = 1.25 * precision * recall / (0.25 * precision + recall)
    return f1, f2


'''
功能：模型评价
'''
def model_evaluation(y_true, y_pred, positive_label):
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
    precision, recall, f_score, support = precision_recall_fscore_support(y_true, y_pred, pos_label=positive_label, average="binary")
    f1, f2 = cal_f_measure(precision, recall)
    return [tn, fp, fn, tp, precision, recall, f1, f2]


'''
功能：模型评价(精简版，只返回precision, recall, f1)
'''
def model_evaluation_precision_recall_fscore(y_true, y_pred, positive_label):
    precision, recall, f_score, support = precision_recall_fscore_support(y_true, y_pred, pos_label=positive_label, average="binary")
    return [precision, recall, f_score]