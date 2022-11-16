import pm4py
import pandas as pd

def import_csv(file_path):
    event_log = pd.read_csv(file_path, sep=';')
    event_log = pm4py.format_dataframe(event_log, case_id='case_id', activity_key='activity', timestamp_key='timestamp')
    start_activities = pm4py.get_start_activities(event_log)
    end_activities = pm4py.get_end_activities(event_log)
    print("Start activities: {}\nEnd activities: {}".format(start_activities, end_activities))


if __name__ == "__main__":
    file_path = "input_data/running-example.csv"
    df = pd.read_csv(file_path, sep=';')
    event_log = pm4py.format_dataframe(df, case_id='case_id',
                                       activity_key='activity', timestamp_key='timestamp')
    net, initial_marking, final_marking = pm4py.discover_petri_net_alpha(event_log)

    aligned_traces = pm4py.conformance_diagnostics_alignments(event_log, net, initial_marking, final_marking)
    print(aligned_traces)