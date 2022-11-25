import pm4py
import pandas as pd
from typing import Union
from pm4py.objects.log.obj import EventLog
from ProcessMining.data_preprocess import load_data, EVENT_LOG_DIR


def process_discovery(log: Union[EventLog, pd.DataFrame]) -> None:
    # 启发式算法
    # net = pm4py.discover_heuristics_net(log)
    # pm4py.view_heuristics_net(net)
    net, initial_marking, final_marking = pm4py.discover_petri_net_heuristics(log)

    # alpha算法
    # net, initial_marking, final_marking = pm4py.discover_petri_net_alpha(log)

    # inductive算法
    net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(log)
    pm4py.view_petri_net(net, initial_marking, final_marking)


if __name__ == "__main__":
    repo = "tensorflow"
    types = ["fork_merge", "fork_close", "unfork_merge", "unfork_close"]
    for t in ["fork_close"]:
        filename = f"{repo}_{t}.csv"
        filepath = f"{EVENT_LOG_DIR}/{filename}"

        # 加载事件日志
        log = load_data(filepath)
        # 剔除无效案例
        log = pm4py.filter_start_activities(log, ["ForkRepository", "CreateBranch", "SubmitCommit", "OpenPR"])
        log = pm4py.filter_end_activities(log, ["DeleteBranch", "ClosePR", "MergePR"])

        # 过程发现
        process_discovery(log)

