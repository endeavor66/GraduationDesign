import pm4py
import pandas as pd


if __name__ == "__main__":
    repo = "tensorflow"
    file_path = f"../DataAcquire/event_log_data/{repo}.csv"
    df = pd.read_csv(file_path, sep=',')
    log = pm4py.format_dataframe(df, case_id='CaseID',
                                       activity_key='Activity', timestamp_key='StartTimestamp')
    log_filter = pm4py.filter_start_activities(log, ['CreateBranch', 'ForkRepository', 'CreatePR'], retain=True)
    log_filter2 = pm4py.filter_end_activities(log_filter, ['ClosePR', 'MergePR', 'DeleteBranch'], retain=True)

    out_path = f"process_data/{repo}.csv"
    log_filter2.to_csv(out_path)