import pm4py
import pandas as pd

repo = "tensorflow"
log_path = f"process_data/{repo}.csv"
log = pd.read_csv(log_path)

# 启发式算法
# net, initial_marking, final_marking = pm4py.discover_petri_net_heuristics(log, dependency_threshold=0.5)

# alpha算法
# net, initial_marking, final_marking = pm4py.discover_petri_net_alpha(log)


# inductive算法
net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(log)

pm4py.view_petri_net(net, initial_marking, final_marking)
