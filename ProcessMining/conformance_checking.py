import pm4py
import pandas as pd
from pm4py.algo.discovery.footprints import algorithm as footprints_discovery

repo = "tensorflow"
log_path = f"process_data/{repo}.csv"
log = pd.read_csv(log_path)

# Token-based replay算法
net, initial_marking, final_marking = pm4py.discover_petri_net_alpha(log)
replayed_traces = pm4py.conformance_diagnostics_token_based_replay(log, net, initial_marking, final_marking)

# Alignments算法
aligned_traces = pm4py.conformance_diagnostics_alignments(log, net, initial_marking, final_marking)


# 输出结果
for trace in aligned_traces:
    print(trace)

