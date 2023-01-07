import pandas as pd
from ProcessMining.Config import *
import os

def search_anomaly_pr(repo: str, scene: str):
    input_path = f"{LOG_SINGLE_SCENE_DIR}/{repo}_{scene}.csv"

    if not os.path.exists(input_path):
        print(f"{input_path} don't exist")
        return []

    data = []
    df = pd.read_csv(input_path, parse_dates=['time:timestamp'])
    for pr_number, group in df.groupby('case:concept:name'):
        review_approve = group.loc[group['concept:name']=='PRReviewApprove']
        review_reject = group.loc[group['concept:name']=='PRReviewReject']

        review_approve_num = review_approve.shape[0]
        review_reject_num = review_reject.shape[0]

        pr_state = scene.split('_')[1]

        if pr_state == 'merge':
            # 寻找评审 reject
            if review_reject_num > 0 and review_approve_num == 0:
                revise = group.loc[group['concept:name'] == 'Revise']
                last_reject = review_reject.iloc[-1]
                revise_after_last_reject = revise.loc[revise['time:timestamp'] > last_reject['time:timestamp']]
                if revise_after_last_reject.shape[0] == 0:
                    print(f"{repo} {scene}, pr_number#{pr_number}, review reject but mergePR")
                    data.append([repo, scene, pr_number, 'review reject but mergePR'])
        elif pr_state == 'close':
            # 寻找评审 approve
            if review_approve_num > 0 and review_reject_num == 0:
                print(f"{repo} {scene}, pr_number#{pr_number}, review approve but closePR")
                data.append([repo, scene, pr_number, 'review approve but closePR'])

    return data


if __name__ == '__main__':
    projects = ['openzipkin/zipkin', 'apache/netbeans', 'opencv/opencv', 'apache/dubbo', 'phoenixframework/phoenix',
                'ARM-software/arm-trusted-firmware', 'apache/zookeeper',
                'spring-projects/spring-framework', 'spring-cloud/spring-cloud-function',
                'vim/vim', 'gpac/gpac', 'ImageMagick/ImageMagick', 'apache/hadoop',
                'libexpat/libexpat', 'apache/httpd', 'madler/zlib', 'redis/redis', 'stefanberger/swtpm']
    anomaly_prs = []
    for pro in projects:
        repo = pro.split('/')[1]
        for scene in FILE_TYPES:
            data = search_anomaly_pr(repo, scene)
            anomaly_prs.extend(data)
            print(f"{repo} {scene} process done")

    output_path = f"{OUTPUT_DATA_DIR}/anomaly_prs.csv"
    df_file = pd.DataFrame(data=anomaly_prs, columns=['repo', 'scene', 'pr_number', 'anomaly_reason'])
    df_file.to_csv(output_path, index=False, header=True)