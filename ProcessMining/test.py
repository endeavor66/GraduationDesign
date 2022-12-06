import pm4py
from pm4py.objects.petri_net.obj import PetriNet, Marking

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

from pm4py.objects.petri_net.utils import petri_utils
petri_utils.add_arc_from_to(source, t_1, net)
petri_utils.add_arc_from_to(t_1, p_1, net)
petri_utils.add_arc_from_to(p_1, t_2, net)
petri_utils.add_arc_from_to(t_2, sink, net)

# add token
initial_marking = Marking()
initial_marking[source] = 1
final_marking = Marking()
final_marking[sink] = 1

# view petri net
pm4py.view_petri_net(net, initial_marking, final_marking)