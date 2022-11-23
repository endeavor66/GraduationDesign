import pm4py
import pandas as pd
from typing import Union
from pm4py.objects.log.obj import EventLog


def process_discovery(log: Union[EventLog, pd.DataFrame]) -> None:
    # 启发式算法
    # net = pm4py.discover_heuristics_net(log)
    # pm4py.view_heuristics_net(net)

    # alpha算法
    # net, initial_marking, final_marking = pm4py.discover_petri_net_alpha(log)

    # inductive算法
    net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(log)

    pm4py.view_petri_net(net, initial_marking, final_marking)


repo = "tensorflow"
log_path = f"process_data/{repo}.csv"
log = pd.read_csv(log_path)
# 对日志进行分组
log_merge_PR = pm4py.filter_end_activities(log, ['MergePR'], retain=True)
log_close_PR = pm4py.filter_end_activities(log, ['ClosePR'], retain=True)
# 每组分别进行过程发现
# process_discovery(log_merge_PR)
process_discovery(log_close_PR)

