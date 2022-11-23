import pm4py
import pandas as pd
from typing import Union, List, Dict, Any
from pm4py.objects.log.obj import EventLog


def conformance_checking(log: Union[EventLog, pd.DataFrame]) -> List[Dict[str, Any]]:
    # Token-based replay算法
    net, initial_marking, final_marking = pm4py.discover_petri_net_heuristics(log)
    # traces = pm4py.conformance_diagnostics_token_based_replay(log, net, initial_marking, final_marking)

    # Alignments算法
    traces = pm4py.conformance_diagnostics_alignments(log, net, initial_marking, final_marking)
    return traces


repo = "tensorflow"
log_path = f"process_data/{repo}.csv"
log = pd.read_csv(log_path)
traces = conformance_checking(log)
# 输出结果
output_path = f"output_data/{repo}.csv"
df = pd.DataFrame(traces)
df.to_csv(output_path, header=True, index=False)

