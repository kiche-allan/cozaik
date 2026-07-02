import pickle
from ticktalkpython.Combiner import combine

with open('./output_branch/app_multi_branch.pickle', 'rb') as f:
    g_branch = pickle.load(f)
with open('./output_app1/app_one.pickle', 'rb') as f:
    g_one = pickle.load(f)

combined = combine(g_branch, g_one, app_ids=['branch_app', 'app_one'])
dag = combined.get_dag()

print('NODES:')
for n in dag.nodes():
    print(' ', n)

print()
print('EDGES:')
for src, dst in dag.edges():
    print(f'  {src}  ->  {dst}')
