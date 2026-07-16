import pickle
from ticktalkpython.SmartMapper import SmartMapper
from ticktalkpython.UnifiedGraph import UnifiedPlacementGraph
from ticktalkpython.DeploymentLoader import load_deployment
with open("./output_deadline/app_deadline.pickle","rb") as f2:
    g=pickle.load(f2)
ensembles=load_deployment("debug_deployment.yaml")
print("Building...")
unified=UnifiedPlacementGraph({"deadline_app":{"graph":g}},ensembles)
print("Running QPF...")
mapper=SmartMapper(unified)
result=mapper.optimize(objective="makespan",trials=1000,deadline_constrained=True)
print("QPF result:",result)
