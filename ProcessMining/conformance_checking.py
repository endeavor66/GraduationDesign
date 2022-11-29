import pm4py
import pandas as pd
from typing import Union, List, Dict, Any
from pm4py.objects.log.obj import EventLog
from ProcessMining.Config import *


def conformance_checking(log: Union[EventLog, pd.DataFrame]) -> List[Dict[str, Any]]:
    # Token-based replay算法
    net, initial_marking, final_marking = pm4py.discover_petri_net_heuristics(log, dependency_threshold=0.9)
    # traces = pm4py.conformance_diagnostics_token_based_replay(log, net, initial_marking, final_marking)

    # Alignments算法
    traces = pm4py.conformance_diagnostics_alignments(log, net, initial_marking, final_marking)
    return traces


if __name__ == "__main__":
    repo = "tensorflow"
    for t in FILE_TYPES:
        # 初始化参数
        filename = f"{repo}_{t}.csv"
        input_path = f"{INPUT_DATA_DIR}/{filename}"
        output_path = f"{OUTPUT_DATA_DIR}/{filename}"

        # 合规性检查
        log = pd.read_csv(input_path)
        traces = conformance_checking(log)

        # 保存结果
        df = pd.DataFrame(traces)
        df.to_csv(output_path, header=True, index=False)
