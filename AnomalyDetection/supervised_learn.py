import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix, precision_recall_fscore_support
from AnomalyDetection.Config import *


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
功能：随机森林
'''
def random_forest_classifier(repo: str, role: str):
    input_path = f"{MULTI_MODEL_VOTE_DIR}/{repo}_{role}_multi_model_vote.csv"

    df = pd.read_csv(input_path)
    df.fillna(0, inplace=True)
    df_x = df.iloc[:, 1:-7]
    df_y = df.iloc[:, -1]

    # 数据集划分
    x_train, x_test, y_train, y_test = train_test_split(df_x, df_y, test_size=0.3, random_state=42)

    # 训练模型
    clf = RandomForestClassifier()
    clf.fit(x_train, y_train)

    # 模型预测
    y_pred = clf.predict(x_test)

    # 模型评价
    score = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)

    # 模型评价
    evaluation = model_evaluation(y_test, y_pred, -1)

    print(f"{repo}_{role} model evaluation")
    print(f"accuracy:%f" % score)
    print(report)

    return evaluation


'''
功能：汇总随机森林在所有项目中的性能表现
'''
def evaluation_model():
    repos = ['dubbo', 'opencv', 'netbeans']
    roles = ["reviewer", "maintainer", "committer"]
    output_path = f"{RANDOM_FOREST_DIR}/random_forest_evaluation.csv"
    data = []
    for repo in repos:
        for role in roles:
            evaluation = random_forest_classifier(repo, role)
            evaluation.extend([repo, role])
            data.append(evaluation)
            print(f"{repo} {role} process done")
    df_file = pd.DataFrame(data=data,
                           columns=['tn', 'fp', 'fn', 'tp', 'precision', 'recall', 'f1', 'f2', 'repo', 'role'])
    df_file.to_csv(output_path, index=False, header=True)


if __name__ == '__main__':
    evaluation_model()