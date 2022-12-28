import pandas as pd
from AnomalyDetection.Config import *
from utils.machine_learn_utils import model_evaluation_precision_recall_fscore
import os


'''
功能：依据投票结果，对每种无监督模型进行评价
'''
def evaluation_unsupervised_model():
    roles = ["reviewer", "maintainer", "committer"]
    evaluation_all = []
    projects = ['openzipkin/zipkin', 'apache/netbeans', 'opencv/opencv', 'apache/dubbo', 'phoenixframework/phoenix',
                'ARM-software/arm-trusted-firmware', 'apache/zookeeper',
                'spring-projects/spring-framework', 'spring-cloud/spring-cloud-function',
                'vim/vim', 'gpac/gpac', 'ImageMagick/ImageMagick', 'apache/hadoop',
                'libexpat/libexpat', 'apache/httpd', 'madler/zlib', 'redis/redis', 'stefanberger/swtpm']
    for pro in projects:
        repo = pro.split('/')[1]
        for role in roles:
            input_path = f"{MULTI_MODEL_VOTE_DIR}/{repo}_{role}_multi_model_vote.csv"

            if not os.path.exists(input_path):
                print(f"{input_path} don't exist!")
                continue

            df = pd.read_csv(input_path)

            evaluation_if = model_evaluation_precision_recall_fscore(df['vote'], df['anomaly_if'], -1)
            evaluation_if.extend([repo, role, 'if'])
            evaluation_all.append(evaluation_if)

            evaluation_ocsvm = model_evaluation_precision_recall_fscore(df['vote'], df['anomaly_ocsvm'], -1)
            evaluation_ocsvm.extend([repo, role, 'ocsvm'])
            evaluation_all.append(evaluation_ocsvm)

            evaluation_lof = model_evaluation_precision_recall_fscore(df['vote'], df['anomaly_lof'], -1)
            evaluation_lof.extend([repo, role, 'lof'])
            evaluation_all.append(evaluation_lof)

            print(f"{repo} {role} process done")

    output_path = f"{OUTPUT_DATA_DIR}/unsupervised_model_evaluation.csv"
    df_file = pd.DataFrame(data=evaluation_all, columns=['precision', 'recall', 'f_score', 'repo', 'role', 'model'])
    df_file.to_csv(output_path, index=False, header=True)


if __name__ == '__main__':
    evaluation_unsupervised_model()