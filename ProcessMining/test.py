import pm4py
from pm4py.objects.bpmn.obj import Marking
from pm4py.objects.petri_net.obj import PetriNet
from pm4py.objects.petri_net.utils import petri_utils, reachability_graph
from pm4py.visualization.transition_system import visualizer as ts_visualizer
from ProcessMining.Config import *

def construct_petri_net():
    net = PetriNet("new_petri_net")
    # creating source, p_1 and sink place
    source = PetriNet.Place("source")
    sink = PetriNet.Place("sink")
    p_1 = PetriNet.Place("p_1")
    # add the places to the Petri Net
    net.places.add(source)
    net.places.add(sink)
    net.places.add(p_1)
    # Create transitions
    t_1 = PetriNet.Transition("name_1", "label_1")
    t_2 = PetriNet.Transition("name_2", "label_2")
    # Add the transitions to the Petri Net
    net.transitions.add(t_1)
    net.transitions.add(t_2)

    petri_utils.add_arc_from_to(source, t_1, net)
    petri_utils.add_arc_from_to(t_1, p_1, net)
    petri_utils.add_arc_from_to(p_1, t_2, net)
    petri_utils.add_arc_from_to(t_2, sink, net)

    initial_marking = Marking()
    initial_marking[source] = 1
    final_marking = Marking()
    final_marking[sink] = 1

    pm4py.view_petri_net(net, initial_marking, final_marking)

def reachability_graph():
    scene = "unfork_merge"
    petri_net_path = f"{PETRI_NET_DIR}/{scene}_petri_net.pnml"
    net, initial_marking, final_marking = pm4py.read_pnml(petri_net_path)

    ts = reachability_graph.construct_reachability_graph(net, initial_marking)

    gviz = ts_visualizer.apply(ts, parameters={ts_visualizer.Variants.VIEW_BASED.value.Parameters.FORMAT: "svg"})
    ts_visualizer.view(gviz)


'''
功能：统计SEECODER考试分数
'''
def score_statistics_seecoder():
    import pandas as pd

    DIR_PATH = "E:/南京大学/实验室/助教-计算机组织结构/成绩统计/SEECODER"
    df_all = pd.DataFrame()
    for i in range(1, 10):
        input_path = f"{DIR_PATH}/Score{i}.csv"
        df = pd.read_csv(input_path)
        df.rename(columns={"用户名": "user", "成绩": f"exam{i}"}, inplace=True)

        df_unvalid = df[~df[['user']].apply(lambda x: str(x[0]).isdigit(), axis=1)]
        print(f"Exam{i}, unvalid user:")
        print(df_unvalid['user'].tolist())

        # 筛选出所有有效的数据
        df_valid = df[df[['user']].apply(lambda x: str(x[0]).isdigit(), axis=1)]
        df_valid['user'] = df_valid['user'].astype('int64')

        if df_all.shape[0] == 0:
            df_all = df_valid
        else:
            df_all = pd.merge(df_all, df_valid, how="outer", on="user")

    output_path = f"{DIR_PATH}/total.csv"
    df_all.to_csv(output_path, index=False, header=True)


'''
功能：统计书面考试分数
'''
def score_statistics_book():
    import pandas as pd

    DIR_PATH = "E:/南京大学/实验室/助教-计算机组织结构/成绩统计/书面作业"
    df_all = pd.DataFrame()
    for i in range(2, 9):
        input_path = f"{DIR_PATH}/书面作业0{i}成绩统计.xls"
        df = pd.read_excel(input_path, sheet_name=0)
        df.rename(columns={"用户名": "user", "学号": "id", "成绩": f"exam{i}"}, inplace=True)

        df_unvalid = df[~df[['id']].apply(lambda x: str(x[0]).isdigit(), axis=1)]
        print(f"Exam{i}, unvalid user:")
        print(df_unvalid['id'].tolist())

        # 筛选出所有有效的数据
        df_valid = df[df[['id']].apply(lambda x: str(x[0]).isdigit(), axis=1)]
        df_valid['id'] = df_valid['id'].astype('int64')

        if df_all.shape[0] == 0:
            df_all = df_valid
        else:
            df_valid = df_valid[['id', f'exam{i}']]
            df_all = pd.merge(df_all, df_valid, how="outer", on="id")

    output_path = f"{DIR_PATH}/total_book.csv"
    df_all.to_csv(output_path, index=False, header=True, encoding="gbk")


if __name__ == '__main__':
    score_statistics_book()