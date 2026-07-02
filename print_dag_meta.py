import pickle
from ticktalkpython.Combiner import combine

with open('./output_app1/app_one.pickle', 'rb') as f:
    g1 = pickle.load(f)
with open('./output_app2/app_two.pickle', 'rb') as f:
    g2 = pickle.load(f)

combined = combine(g1, g2, app_ids=['app_one', 'app_two'])
dag = combined.get_dag()

edge_meta = {}
for edge in combined.sspg_edges:
    edge_meta[(edge['source'], edge['target'])] = edge

print('EDGES WITH METADATA:')
for src, dst in dag.edges():
    meta = edge_meta.get((src, dst))
    if meta:
        print(f'  {src}  ->  {dst}')
        print(f'    type={meta["type"]}  symbol={meta["symbol"]}  app_id={meta["app_id"]}')
    else:
        print(f'  {src}  ->  {dst}  (no sspg metadata)')
