import pm4py
import pandas as pd
from typing import Union, List, Dict, Any
from pm4py.objects.log.obj import EventLog
from ProcessMining.Config import *
from utils.pr_self_utils import get_pr_attributes


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


'''
功能：对特定项目中的特定场景下的所有案例，进行一致性检查
'''
def conformance_checking_for_single_repo(repo: str, scene: str):
    filename = f"{repo}_{scene}"
    log_path = f"{LOG_SINGLE_SCENE_DIR}/{filename}.csv"
    petri_net_path = f"{PETRI_NET_DIR}/{scene}_petri_net.pnml"
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


'''
功能：判断特定项目中的特定PR所属的场景(fork_merge, fork_close, unfork_merge, unfork_close)
'''
def cal_scene_for_pr(repo: str, pr_number: int):
    pr_attributes = get_pr_attributes(repo, pr_number)
    pr_state = pr_attributes['merged']
    is_fork = pr_attributes['head_repo_fork']
    scene = None
    if is_fork and pr_state:
        scene = "fork_merge"
    elif is_fork and (not pr_state):
        scene = "fork_close"
    elif (not is_fork) and pr_state:
        scene = "unfork_merge"
    elif (not is_fork) and (not pr_state):
        scene = "unfork_close"
    return scene


'''
功能：查询特定项目中，所有权限变更人在权限变更后，所参与的所有PR
'''
def cal_involved_pr_scene(repo: str):
    input_path = f"{ROLE_CHANGE_DIR}/{repo}_role_change.csv"
    df = pd.read_csv(input_path, parse_dates=['change_role_time'])
    pr_scene = []
    for index, row in df.iterrows():
        person = row['people']
        involved_pr = row['involved_pr_after_permission_change']
        if pd.isna(involved_pr):
            continue
        involved_pr_list = involved_pr.split("#")
        for pr_number in involved_pr_list:
            pr_number = int(pr_number)

            # 查询pr对应的子场景
            scene = cal_scene_for_pr(repo, pr_number)

            pr_scene.append([person, pr_number, scene])

    return pr_scene


'''
功能：根据传入的log，执行一致性检查
'''
def conformance_checking(log: Union[pd.DataFrame], repo: str, scene: str):
    filename = f"{repo}_{scene}"
    petri_net_path = f"{PETRI_NET_DIR}/{scene}_petri_net.pnml"

    traces = []
    try:
        traces = alignments(log, petri_net_path)
    except Exception as e:
        print(e)
        print(f"{filename} process failed")
    return traces


'''
功能：自动对权限变更人，在权限变更后的一段时间内参与的所有PR，执行一致性检验
'''
def auto_analyse(repo: str):
    output_path = f"{ALIGNMENT_DIR}/{repo}_alignment.csv"

    # 项目的事件日志
    log_path = f"{LOG_ALL_SCENE_DIR}/{repo}.csv"
    df_log = pd.read_csv(log_path)

    # 获取权限变更人在权限变更后一段时间内参与的所有PR
    pr_scene = cal_involved_pr_scene(repo)

    # 按场景进行一致性检验
    df_alignment_result = pd.DataFrame()
    df_scene = pd.DataFrame(data=pr_scene, columns=['people', 'pr_number', 'scene'])
    for person, group_people in df_scene.groupby('people'):
        for scene, group in group_people.groupby('scene'):
            # 筛选特定场景下的log日志
            pr_number_list = group['pr_number'].tolist()
            scene_log = df_log.loc[df_log['case:concept:name'].isin(pr_number_list)]

            # 执行一致性检查
            traces = conformance_checking(scene_log, repo, str(scene))
            df_traces = pd.DataFrame(data=traces)
            df_traces['people'] = [person] * len(traces)
            df_traces['pr_number'] = scene_log['case:concept:name'].unique()
            df_traces['scene'] = [str(scene)] * len(traces)

            # 保存结果
            df_alignment_result = pd.concat([df_alignment_result, df_traces], ignore_index=True)

    df_alignment_result.to_csv(output_path, index=False, header=True)


if __name__ == "__main__":
    repo = 'dubbo'
    auto_analyse(repo)
