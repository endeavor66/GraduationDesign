import pm4py
import pandas as pd
from typing import Union, List, Dict, Any
from pm4py.objects.log.obj import EventLog
from ProcessMining.Config import *


'''
功能：Token-based replay 算法
'''
def token_based_replay(log: Union[EventLog, pd.DataFrame], petri_net_path: str) -> List[Dict[str, Any]]:
    net, initial_marking, final_marking = pm4py.read_pnml(petri_net_path)
    traces = pm4py.conformance_diagnostics_token_based_replay(log, net, initial_marking, final_marking)
    return traces


'''
功能：Alignments 算法
'''
def alignments(log: Union[EventLog, pd.DataFrame], petri_net_path: str) -> List[Dict[str, Any]]:
    net, initial_marking, final_marking = pm4py.read_pnml(petri_net_path)
    # pm4py.view_petri_net(net, initial_marking, final_marking)
    traces = pm4py.conformance_diagnostics_alignments(log, net, initial_marking, final_marking)
    return traces


if __name__ == "__main__":
    repo = "tensorflow"
    # for t in FILE_TYPES:
    # 初始化参数
    t = FILE_TYPES[1]
    filename = f"{repo}_{t}"
    log_path = f"{PROCESS_DATA_DIR}/{filename}.csv"
    petri_net_path = f"{PETRI_NET_DIR}/{filename}_petri_net.pnml"
    alignment_path = f"{ALIGNMENT_DIR}/{filename}_alignment.csv"

    log = pd.read_csv(log_path)
    try:
        # 合规性检查
        traces = alignments(log, petri_net_path)

        # 保存结果
        df = pd.DataFrame(traces)
        df['case:concept:name'] = log['case:concept:name'].unique()
        df.to_csv(alignment_path, header=True, index=False)
        print(f"{filename} process done")
    except Exception as e:
        print(e)
        print(f"{filename} process failed")
