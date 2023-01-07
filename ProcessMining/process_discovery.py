import pm4py
import os
import pandas as pd
from typing import Union, List
from pm4py.objects.log.obj import EventLog
from pm4py.algo.evaluation.generalization import algorithm as generalization_evaluator
from pm4py.algo.evaluation.simplicity import algorithm as simplicity_evaluator
from ProcessMining.Config import *

def inductive_mining(log: Union[EventLog, pd.DataFrame], petri_net_filename: str):
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(log)
    pm4py.write_pnml(petri_net, initial_marking, final_marking, f"{petri_net_filename}.pnml")
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, f"{petri_net_filename}.png")
    return petri_net, initial_marking, final_marking

def alpha_mining(log: Union[EventLog, pd.DataFrame], petri_net_filename: str):
    petri_net, initial_marking, final_marking = pm4py.discover_petri_net_alpha(log)
    pm4py.write_pnml(petri_net, initial_marking, final_marking, f"{petri_net_filename}.pnml")
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, f"{petri_net_filename}.png")
    return petri_net, initial_marking, final_marking


'''
功能：启发式算法
'''
def heuristics_mining(log: Union[EventLog, pd.DataFrame], heuristics_net_filepath: str, petri_net_filename: str,
                      bpmn_filename: str):
    # 恢复heuristics_net
    heuristics_net = pm4py.discover_heuristics_net(log,
                                                   dependency_threshold=0.5,
                                                   and_threshold=0.65,
                                                   loop_two_threshold=0.5)
    pm4py.save_vis_heuristics_net(heuristics_net, heuristics_net_filepath)

    # 转化为Petri-Net
    petri_net, initial_marking, final_marking = pm4py.convert_to_petri_net(heuristics_net)
    pm4py.write_pnml(petri_net, initial_marking, final_marking, f"{petri_net_filename}.pnml")
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, f"{petri_net_filename}.png")

    # # 转化为BPMN
    bpmn = pm4py.convert_to_bpmn(petri_net, initial_marking, final_marking)
    pm4py.save_vis_bpmn(bpmn, f"{bpmn_filename}.png")

    return petri_net, initial_marking, final_marking


'''
功能：模型评估，从四个方面评估模型: 拟合度 fitness, 简单度 simplicity, 泛化度 generalization, 精确度 precision
'''
def model_evaluation(log: Union[EventLog, pd.DataFrame], petri_net, initial_marking, final_marking):
    fitness = pm4py.fitness_token_based_replay(log, petri_net, initial_marking, final_marking)
    precision = pm4py.precision_token_based_replay(log, petri_net, initial_marking, final_marking)
    generalization = generalization_evaluator.apply(log, petri_net, initial_marking, final_marking)
    simplicity = simplicity_evaluator.apply(petri_net)

    print('''
    ====================
    = model evaluation =
    ====================
    ''')
    print("fitness")
    print("\taverage_trace_fitness:%.3f" % fitness['average_trace_fitness'])
    print("\tpercentage_of_fitting_traces:%.3f" % fitness['percentage_of_fitting_traces'])

    print("precision:%.3f" % precision)

    print("generalization:%.3f" % generalization)

    print("simplicity:%.3f" % simplicity)


'''
功能：过程发现
'''
def process_discovery_for_single_repo(repo: str, scene: str):
    filename = f"{repo}_{scene}"
    log_path = f"{LOG_SINGLE_SCENE_DIR}/{filename}.csv"
    heuristics_net_filepath = f"{HEURISTICS_NET_DIR}/{filename}_heuristics_net.png"
    petri_net_filename = f"{PETRI_NET_DIR}/{filename}_petri_net"

    # 加载事件日志
    log = pd.read_csv(log_path, parse_dates=['time:timestamp'])

    # 过程发现
    petri_net, im, fm = heuristics_mining(log, heuristics_net_filepath, petri_net_filename)

    # 模型评估
    model_evaluation(log, petri_net, im, fm)


def process_discovery_for_single_scene(repos: List, scene: str):
    log = pd.DataFrame()
    for repo in repos:
        input_path = f"{LOG_SINGLE_SCENE_DIR}/{repo}_{scene}.csv"
        if not os.path.exists(input_path):
            print(f"{input_path} don't exist")
            continue
        df = pd.read_csv(input_path, parse_dates=['time:timestamp'])
        log = pd.concat([log, df], ignore_index=True)

    print("log case: %s" % len(log['case:concept:name'].unique()))

    # 过滤包含低频行为(ReopenPR, PRReviewDismiss)的案例
    log = pm4py.filter_event_attribute_values(log, 'concept:name', ['ReopenPR', 'PRReviewDismiss'], level="case", retain=False)
    print("filter_event_attribute_values (ReopenPR, PRReviewDismiss)")
    print("rest case: %d" % len(log['case:concept:name'].unique()))

    # 过程发现
    heuristics_net_filepath = f"{HEURISTICS_NET_DIR}/{scene}_heuristics_net.png"
    petri_net_filename = f"{PETRI_NET_DIR}/{scene}_petri_net"
    bpmn_filename = f"{BPMN_DIR}/{scene}_bpmn"

    petri_net, im, fm = heuristics_mining(log,
                                          heuristics_net_filepath,
                                          petri_net_filename,
                                          bpmn_filename)

    # petri_net, im, fm = alpha_mining(log, petri_net_filename)

    # petri_net, im, fm = inductive_mining(log, petri_net_filename)

    # 模型评估
    model_evaluation(log, petri_net, im, fm)


'''
功能：验证爬取处理后的event log是否有问题（CreateBranch是否会在OpenPR之后发生）
'''
def valid(repos: List):
    # for scene in FILE_TYPES:
    scene = FILE_TYPES[2]
    index = 0
    for repo in repos:
        input_path = f"{LOG_SINGLE_SCENE_DIR}/{repo}_{scene}.csv"
        if not os.path.exists(input_path):
            print(f"{input_path} don't exist")
            continue
        df = pd.read_csv(input_path, parse_dates=['time:timestamp'])

        for name, group in df.groupby('case:concept:name'):
            create_event = group.loc[group['concept:name'] == 'CreateBranch']
            delete_event = group.loc[group['concept:name'] == 'DeleteBranch']
            openPR_event = group.loc[group['concept:name'] == 'OpenPR']

            create_number = create_event.shape[0]
            delete_number = delete_event.shape[0]
            openPR_number = openPR_event.shape[0]

            create_time = create_event['time:timestamp'].iloc[0] if create_number > 0 else None
            delete_time = delete_event['time:timestamp'].iloc[0] if delete_number > 0 else None
            openPR_time = openPR_event['time:timestamp'].iloc[0] if openPR_number > 0 else None

            if create_number > 1:
                print(f"pr_number#{name} 异常, CreateBranch number: {create_number}")
            if delete_number > 1:
                print(f"pr_number#{name} 异常, DeleteBranch number: {delete_number}")

            if (not pd.isna(create_time)) and (not pd.isna(openPR_time)) and create_time > openPR_time:
                print(f"pr_number#{name} 异常, CreateBranchTime: {create_time}, openPR: {openPR_time}")
            if (not pd.isna(create_time)) and (not pd.isna(delete_time)) and create_time > delete_time:
                print(f"pr_number#{name} 异常, CreateBranchTime: {create_time}, DeleteBranchTime: {delete_time}")

            if delete_number > 0:
                index += 1
                submit_event = group.loc[group['concept:name'] == 'SubmitCommit']
                unvalid_submit = submit_event.loc[submit_event['time:timestamp'] > delete_time]
                if unvalid_submit.shape[0] > 0:
                    print(f"pr_number#{name} 异常, UnvalidSubmitNumber: {unvalid_submit.shape[0]}")

        print(f"{repo} {scene} process done")

    print(index)


if __name__ == "__main__":
    projects = ['openzipkin/zipkin', 'apache/netbeans'] # 'opencv/opencv', 'apache/dubbo', 'phoenixframework/phoenix']
    #             'ARM-software/arm-trusted-firmware', 'apache/zookeeper',
    #             'spring-projects/spring-framework', 'spring-cloud/spring-cloud-function',
    #             'vim/vim', 'gpac/gpac', 'ImageMagick/ImageMagick', 'apache/hadoop',
    #             'libexpat/libexpat', 'apache/httpd', 'madler/zlib', 'redis/redis', 'stefanberger/swtpm']
    # projects = [
                # 'ARM-software/arm-trusted-firmware', 'apache/zookeeper',
                # 'spring-projects/spring-framework', 'spring-cloud/spring-cloud-function',
                # 'vim/vim', 'gpac/gpac', 'ImageMagick/ImageMagick', 'apache/hadoop',
                # 'libexpat/libexpat',
                # 'apache/httpd',
                # 'madler/zlib',
                # 'redis/redis',
                # 'stefanberger/swtpm'
                # ]
    repos = []
    for pro in projects:
        repo = pro.split('/')[1]
        repos.append(repo)

    # valid(repos)

    scene = FILE_TYPES[2]
    process_discovery_for_single_scene(repos, scene)
    print(f"{scene} process done")

