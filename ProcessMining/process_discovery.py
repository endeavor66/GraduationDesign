import pm4py
import os
import pandas as pd
from typing import Union
from pm4py.objects.log.obj import EventLog
from pm4py.algo.evaluation.generalization import algorithm as generalization_evaluator
from pm4py.algo.evaluation.simplicity import algorithm as simplicity_evaluator
from ProcessMining.Config import *


'''
功能：启发式算法
'''
def heuristics_mining(log: Union[EventLog, pd.DataFrame], heuristics_net_filepath: str, petri_net_filename):
    # 恢复heuristics_net
    heuristics_net = pm4py.discover_heuristics_net(log, dependency_threshold=0.5)
    pm4py.save_vis_heuristics_net(heuristics_net, heuristics_net_filepath)

    # 转化为Petri-Net
    petri_net, initial_marking, final_marking = pm4py.convert_to_petri_net(heuristics_net)
    pm4py.write_pnml(petri_net, initial_marking, final_marking, f"{petri_net_filename}.pnml")
    pm4py.save_vis_petri_net(petri_net, initial_marking, final_marking, f"{petri_net_filename}.png")

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


def process_discovery_for_single_scene(scene: str):
    repos = ['zipkin', 'netbeans', 'opencv', 'dubbo', 'phoenix']

    log = pd.DataFrame()
    for repo in repos:
        input_path = f"{LOG_SINGLE_SCENE_DIR}/{repo}_{scene}.csv"
        if not os.path.exists(input_path):
            print(f"{input_path} don't exist")
            continue
        df = pd.read_csv(input_path, parse_dates=['time:timestamp'])
        log = pd.concat([log, df], ignore_index=True)

    # 过程发现
    heuristics_net_filepath = f"{HEURISTICS_NET_DIR}/{scene}_heuristics_net.png"
    petri_net_filename = f"{PETRI_NET_DIR}/{scene}_petri_net"
    petri_net, im, fm = heuristics_mining(log, heuristics_net_filepath, petri_net_filename)

    # 模型评估
    model_evaluation(log, petri_net, im, fm)


if __name__ == "__main__":
    repo = "tensorflow"
    scene = FILE_TYPES[2]
    process_discovery_for_single_scene(scene)
