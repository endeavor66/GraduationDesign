import pm4py
import os
import pandas as pd

PROCESS_DATA_DIR = 'process_data'
if not os.path.exists(PROCESS_DATA_DIR):
    os.makedirs(PROCESS_DATA_DIR)

if __name__ == "__main__":
    repo = "tensorflow"
    file_path = f"../DataAcquire/event_log_data/{repo}.csv"
    df = pd.read_csv(file_path, sep=',')
    log = pm4py.format_dataframe(df, case_id='CaseID',
                                       activity_key='Activity', timestamp_key='StartTimestamp')
    # log_filter = pm4py.filter_start_activities(log, ['CreateBranch', 'ForkRepository', 'CreatePR', 'SubmitCommit'], retain=True)
    # log_filter2 = pm4py.filter_end_activities(log_filter, ['ClosePR', 'MergePR', 'DeleteBranch', 'SubmitCommit'], retain=True)

    out_path = f"{PROCESS_DATA_DIR}/{repo}.csv"
    # log_filter2.to_csv(out_path)
    log.to_csv(out_path)