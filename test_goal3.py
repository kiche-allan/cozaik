import pickle

with open('./output_app1/app_one.pickle', 'rb') as f:
    g = pickle.load(f)

for sq in g.sqs:
    print('SQ:', sq.sq_name)
    print('  criticality:', sq.criticality)
    print('  execution_time_estimates:', sq.execution_time_estimates)
    print('  energy_cost_estimates:', sq.energy_cost_estimates)
