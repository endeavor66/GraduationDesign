import pm4py
import pandas as pd
from typing import Union
from pm4py.objects.log.obj import EventLog
from pm4py.algo.evaluation.generalization import algorithm as generalization_evaluator
from pm4py.algo.evaluation.simplicity import algorithm as simplicity_evaluator
from ProcessMining.Config import *


def process_discovery(log: Union[EventLog, pd.DataFrame]) -> None:
    # 启发式算法
    net = pm4py.discover_heuristics_net(log, dependency_threshold=0.9)
    pm4py.view_heuristics_net(net)

    petri_net, initial_marking, final_marking = pm4py.convert_to_petri_net(net)
    # net, initial_marking, final_marking = pm4py.discover_petri_net_heuristics(log)

    # alpha算法
    # net, initial_marking, final_marking = pm4py.discover_petri_net_alpha(log)

    # inductive算法
    # net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(log)
    pm4py.view_petri_net(petri_net, initial_marking, final_marking)

    # 模型评估
    model_evaluation(log, petri_net, initial_marking, final_marking)


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


if __name__ == "__main__":
    repo = "tensorflow"
    for t in FILE_TYPES:
        filename = f"{repo}_{t}.csv"
        filepath = f"{PROCESS_DATA_DIR}/{filename}"

        # 加载事件日志
        log = pd.read_csv(filepath)

        # # 过程发现
        process_discovery(log)

