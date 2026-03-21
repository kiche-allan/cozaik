# eval_taxi.py — Taxi Fleet Analytics Pipeline
#
# RIoTBench TAXI application: composite ETL -> parallel (STATS + PRED + TRAIN)
# Uses TAXI dataset (trip records with fares, distances, locations).
#
# Topology: Fork-join composite
#   taxi_source -> parse -> range -> bloom -> interp -> join -> annotate -> csv_to_senml (ETL 7)
#     +--> STATS: avg -> kalman -> linreg -> moment -> distinct -> sliding_reg -> stats_join -> stats_pub (8)
#     +--> PRED: dt_classify -> block_avg -> error_est -> kalman -> linreg -> pred_join -> pred_dt -> pred_pub (8)
#     +--> TRAIN: table_range -> lr_train -> dt_train -> blob_upload -> train_join -> train_pub (6)
#     All three -> taxi_fusion -> alert -> dashboard -> mqtt -> sink (5)
#
# App SQs: 1 source + 7 ETL + 8 STATS + 8 PRED + 6 TRAIN + 5 tail = 35
# Dataset: TAXI (TAXI_sample_data_senml.csv)
#
from ticktalkpython.SQ import STREAMify, SQify, GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *


# ===== SOURCE =====
@STREAMify
def taxi_source(trigger, workload_type):
    import sys, os, time
    global sq_state
    if sq_state.get('generator', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_senml_generator, parse_senml_message
        sq_state['generator'] = get_senml_generator(workload_type)
        sq_state['parse_fn'] = parse_senml_message
        sq_state['msg_id'] = 0
    sq_state['msg_id'] += 1
    message = sq_state['generator'].generate_message()
    parsed = sq_state['parse_fn'](message, workload_type)
    parsed['msgid'] = sq_state['msg_id']
    return [parsed, time.time()]


# ===== ETL CHAIN (7 SQs) =====
@SQify
def taxi_parse(raw_data):
    import sys, os, time
    global sq_state
    data = raw_data[0]
    arrival_time = raw_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('senml_parse')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]

@SQify
def taxi_range(parsed_data):
    import sys, os, time
    global sq_state
    data = parsed_data[0]
    arrival_time = parsed_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('range_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]

@SQify
def taxi_bloom(range_data):
    import sys, os, time
    global sq_state
    data = range_data[0]
    arrival_time = range_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('bloom_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]

@SQify
def taxi_interp(bloom_data):
    import sys, os, time
    global sq_state
    data = bloom_data[0]
    arrival_time = bloom_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('interpolation')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]

@SQify
def taxi_join_etl(interp_data):
    import sys, os, time
    global sq_state
    data = interp_data[0]
    arrival_time = interp_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('join')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]

@SQify
def taxi_annotate(joined_data):
    import sys, os, time
    global sq_state
    data = joined_data[0]
    arrival_time = joined_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('annotation')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['annotation'] = 'taxi_fleet_v1'
    return [data, arrival_time, time.time()]

@SQify
def taxi_csv_senml(ann_data):
    import sys, os, time
    global sq_state
    data = ann_data[0]
    arrival_time = ann_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('csv_to_senml')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]


# ===== STATS BRANCH (8 SQs) =====
@SQify
def s_avg(etl_data):
    import sys, os, time
    global sq_state
    data = etl_data[0]
    arrival_time = etl_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('average')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['avg_fare'] = data.get('fare_amount', 10.0)
    return [data, arrival_time, time.time()]

@SQify
def s_kalman(avg_data):
    import sys, os, time
    global sq_state
    data = avg_data[0]
    arrival_time = avg_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['distance_filtered'] = data.get('trip_distance', 5.0) * 0.95
    return [data, arrival_time, time.time()]

@SQify
def s_linreg(kalman_data):
    import sys, os, time
    global sq_state
    data = kalman_data[0]
    arrival_time = kalman_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('simple_linear_regression')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['fare_predicted'] = data.get('distance_filtered', 5) * 2.5 + 3.0
    return [data, arrival_time, time.time()]

@SQify
def s_moment(linreg_data):
    import sys, os, time
    global sq_state
    data = linreg_data[0]
    arrival_time = linreg_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('second_order_moment')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['fare_variance'] = abs(data.get('fare_amount', 10) - data.get('fare_predicted', 10)) ** 2
    return [data, arrival_time, time.time()]

@SQify
def s_distinct(moment_data):
    import sys, os, time
    global sq_state
    data = moment_data[0]
    arrival_time = moment_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('distinct_approx_count')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]

@SQify
def s_sliding(distinct_data):
    import sys, os, time
    global sq_state
    data = distinct_data[0]
    arrival_time = distinct_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('sliding_linear_reg')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['fare_trend'] = data.get('fare_predicted', 10) * 1.02
    return [data, arrival_time, time.time()]

@SQify
def s_join(sliding_data):
    import sys, os, time
    global sq_state
    data = sliding_data[0]
    arrival_time = sliding_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('join')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]

@SQify
def s_publish(join_data):
    import sys, os, time
    global sq_state
    data = join_data[0]
    arrival_time = join_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('mqtt_publish')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]


# ===== PRED BRANCH (8 SQs) =====
@SQify
def p_dt(etl_data):
    import sys, os, time
    global sq_state
    data = etl_data[0]
    arrival_time = etl_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('decision_tree_classify')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    trip_time = data.get('trip_time_in_secs', 600)
    data['trip_class'] = 'short' if trip_time < 600 else 'medium' if trip_time < 1800 else 'long'
    return [data, arrival_time, time.time()]

@SQify
def p_block_avg(dt_data):
    import sys, os, time
    global sq_state
    data = dt_data[0]
    arrival_time = dt_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('average')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['block_avg_fare'] = data.get('fare_amount', 10.0)
    return [data, arrival_time, time.time()]

@SQify
def p_error(block_data):
    import sys, os, time
    global sq_state
    data = block_data[0]
    arrival_time = block_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('error_estimation')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['prediction_error'] = abs(data.get('block_avg_fare', 10) - data.get('total_amount', 10))
    return [data, arrival_time, time.time()]

@SQify
def p_kalman(error_data):
    import sys, os, time
    global sq_state
    data = error_data[0]
    arrival_time = error_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['error_filtered'] = data.get('prediction_error', 0) * 0.9
    return [data, arrival_time, time.time()]

@SQify
def p_linreg(kalman_data):
    import sys, os, time
    global sq_state
    data = kalman_data[0]
    arrival_time = kalman_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('linear_regression')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['error_trend'] = data.get('error_filtered', 0) * 1.05
    return [data, arrival_time, time.time()]

@SQify
def p_join(linreg_data):
    import sys, os, time
    global sq_state
    data = linreg_data[0]
    arrival_time = linreg_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('join')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]

@SQify
def p_dt_final(join_data):
    import sys, os, time
    global sq_state
    data = join_data[0]
    arrival_time = join_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('decision_tree_classify')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['pred_quality'] = 'good' if data.get('error_filtered', 0) < 3 else 'poor'
    return [data, arrival_time, time.time()]

@SQify
def p_publish(dt_data):
    import sys, os, time
    global sq_state
    data = dt_data[0]
    arrival_time = dt_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('mqtt_publish')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]


# ===== TRAIN BRANCH (6 SQs) =====
@SQify
def tr_table_range(etl_data):
    import sys, os, time
    global sq_state
    data = etl_data[0]
    arrival_time = etl_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('azure_table_range')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]

@SQify
def tr_lr_train(table_data):
    import sys, os, time
    global sq_state
    data = table_data[0]
    arrival_time = table_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('linear_regression_train')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['lr_model_trained'] = True
    return [data, arrival_time, time.time()]

@SQify
def tr_dt_train(lr_data):
    import sys, os, time
    global sq_state
    data = lr_data[0]
    arrival_time = lr_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('decision_tree_train')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['dt_model_trained'] = True
    return [data, arrival_time, time.time()]

@SQify
def tr_blob_upload(dt_data):
    import sys, os, time
    global sq_state
    data = dt_data[0]
    arrival_time = dt_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('azure_blob_upload')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]

@SQify
def tr_join(blob_data):
    import sys, os, time
    global sq_state
    data = blob_data[0]
    arrival_time = blob_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('join')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]

@SQify
def tr_publish(join_data):
    import sys, os, time
    global sq_state
    data = join_data[0]
    arrival_time = join_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('mqtt_publish')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]


# ===== FUSION + TAIL (5 SQs) =====
@SQify
def taxi_fusion(stats_data, pred_data, train_data):
    import sys, os, time
    global sq_state
    s = stats_data[0]
    p = pred_data[0]
    t = train_data[0]
    arrival_time = stats_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('join')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    fused = {
        'avg_fare': s.get('avg_fare', 0), 'fare_trend': s.get('fare_trend', 0),
        'trip_class': p.get('trip_class', ''), 'pred_quality': p.get('pred_quality', ''),
        'models_trained': t.get('lr_model_trained', False),
    }
    return [fused, arrival_time, time.time()]

@SQify
def taxi_alert(fused_data):
    import sys, os, time
    global sq_state
    data = fused_data[0]
    arrival_time = fused_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('mqtt_publish')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]

@SQify
def taxi_dashboard(alert_data):
    import sys, os, time
    global sq_state
    data = alert_data[0]
    arrival_time = alert_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('azure_blob_upload')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]

@SQify
def taxi_mqtt(dash_data):
    import sys, os, time
    global sq_state
    data = dash_data[0]
    arrival_time = dash_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('mqtt_publish')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]

@SQify
def taxi_sink(mqtt_data):
    import os, time, json
    global sq_state
    data = mqtt_data[0]
    arrival_time = mqtt_data[1]
    completion_time = time.time()
    latency_ms = (completion_time - arrival_time) * 1000
    run_label = os.environ.get('TTPYTHON_RUN_LABEL', '')
    predicted_ms = float(os.environ.get('TTPYTHON_PREDICTED_MS', '0'))
    log_entry = {
        'timestamp': completion_time, 'arrival_time': arrival_time,
        'latency_ms': latency_ms, 'workload': 'eval_taxi',
        'run_label': run_label, 'predicted_ms': predicted_ms,
    }
    log_dir = os.environ.get('TTPYTHON_LOG_DIR', '.')
    log_file = os.path.join(log_dir, 'runtime_log_' + (run_label or 'eval_taxi') + '.jsonl')
    with open(log_file, 'a') as f:
        f.write(json.dumps(log_entry) + '\n')
    print('TAXI done, latency=' + str(latency_ms) + 'ms')
    return 1


# ===== GRAPH WIRING =====
@GRAPHify
def eval_taxi(trigger):
    A_1 = 1
    with TTClock.root() as root_clock:
        start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
        N = 100
        stop_time = start_time + (2000000 * N)
        sampling_time = VALUES_TO_TTTIME(start_time, stop_time)
        sample_window = COPY_TTTIME(A_1, sampling_time)

        # ===== Source =====
        with TTConstraint(components=["sensor_interface"]):
            wl = 'TAXI'
            raw = taxi_source(sample_window, wl, TTClock=root_clock, TTPeriod=2000000, TTPhase=0, TTDataIntervalWidth=1000000)

        # ===== ETL chain =====
        with TTConstraint(components=["compute"]):
            parsed = taxi_parse(raw)
            ranged = taxi_range(parsed)
            bloomed = taxi_bloom(ranged)
            interped = taxi_interp(bloomed)
            joined = taxi_join_etl(interped)
            annotated = taxi_annotate(joined)
            csvd = taxi_csv_senml(annotated)

        # ===== STATS branch =====
        with TTConstraint(components=["compute"]):
            sa = s_avg(csvd)
            sk = s_kalman(sa)
            slr = s_linreg(sk)
            sm = s_moment(slr)
            sd = s_distinct(sm)
            ssl = s_sliding(sd)
            sj = s_join(ssl)

        with TTConstraint(components=["mqtt_broker"]):
            sp = s_publish(sj)

        # ===== PRED branch =====
        with TTConstraint(components=["compute"]):
            pdt = p_dt(csvd)
            pba = p_block_avg(pdt)
            pe = p_error(pba)
            pk2 = p_kalman(pe)
            plr = p_linreg(pk2)
            pj = p_join(plr)
            pdf = p_dt_final(pj)

        with TTConstraint(components=["mqtt_broker"]):
            pp = p_publish(pdf)

        # ===== TRAIN branch =====
        with TTConstraint(components=["compute", "storage"]):
            ttr = tr_table_range(csvd)
            tlr = tr_lr_train(ttr)
            tdt = tr_dt_train(tlr)
            tbu = tr_blob_upload(tdt)
            ttj = tr_join(tbu)

        with TTConstraint(components=["mqtt_broker"]):
            ttp = tr_publish(ttj)

        # ===== Fusion + tail =====
        with TTConstraint(components=["compute"]):
            fused = taxi_fusion(sp, pp, ttp)

        with TTConstraint(components=["mqtt_broker", "storage"]):
            alerted = taxi_alert(fused)
            dashed = taxi_dashboard(alerted)
            published = taxi_mqtt(dashed)
            result = taxi_sink(published)
