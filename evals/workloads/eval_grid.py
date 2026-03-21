# eval_grid.py — Power Grid Monitoring & Stability Analysis
#
# RIoTBench-inspired power grid application. Reinterprets SYS dataset fields
# as grid telemetry: temperature->transformer_temp, humidity->cooling_efficiency,
# light->solar_irradiance, dust->particulate_fouling, airquality->grid_load.
#
# Topology: Four-branch parallel with fusion
#   grid_source -> parse -> bloom -+-> Load branch (7): kalman -> linreg -> forecast -> moment -> sliding -> load_join -> load_pub
#                                  +-> Anomaly branch (7): threshold -> moment -> classify -> interp -> error -> anom_join -> anom_pub
#                                  +-> Stability branch (7): interp -> avg -> distinct -> kalman -> score -> stab_join -> stab_pub
#                                  +-> Demand branch (8): avg -> kalman -> linreg -> dt -> demand_interp -> demand_error -> demand_join -> demand_pub
#                                  All four -> grid_fusion -> response -> alert -> dashboard -> mqtt -> sink
#
# App SQs: 1 + 2 + 7 + 7 + 7 + 8 + 6 = 38
# Dataset: SYS (SYS_sample_data_senml.csv, reinterpreted as grid telemetry)
#
from ticktalkpython.SQ import STREAMify, SQify, GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *


# ===== SOURCE =====
@STREAMify
def grid_source(trigger, workload_type):
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
    parsed['transformer_temp'] = parsed.get('temperature', 25.0)
    parsed['cooling_eff'] = parsed.get('humidity', 50.0)
    parsed['solar_irradiance'] = parsed.get('light', 500)
    parsed['particulate'] = parsed.get('dust', 200)
    parsed['grid_load'] = parsed.get('airquality_raw', 50)
    return [parsed, time.time()]


# ===== COMMON: parse -> bloom =====
@SQify
def grid_parse(raw_data):
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
def grid_bloom(parsed_data):
    import sys, os, time
    global sq_state
    data = parsed_data[0]
    arrival_time = parsed_data[1]
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


# ===== BRANCH 1: Load Prediction (7 SQs) =====
@SQify
def load_kalman(bloom_data):
    import sys, os, time
    global sq_state
    data = bloom_data[0]
    arrival_time = bloom_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['load_filtered'] = data.get('grid_load', 50) * 0.92
    return [data, arrival_time, time.time()]

@SQify
def load_linreg(kalman_data):
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
    data['load_trend'] = data.get('load_filtered', 50) * 1.05
    return [data, arrival_time, time.time()]

@SQify
def load_forecast(linreg_data):
    import sys, os, time
    global sq_state
    data = linreg_data[0]
    arrival_time = linreg_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('linear_regression')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    solar_factor = min(1.0, data.get('solar_irradiance', 500) / 10000)
    data['load_forecast'] = data.get('load_trend', 50) * (1 - solar_factor * 0.2)
    return [data, arrival_time, time.time()]

@SQify
def load_moment(forecast_data):
    import sys, os, time
    global sq_state
    data = forecast_data[0]
    arrival_time = forecast_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('second_order_moment')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['load_variance'] = abs(data.get('load_forecast', 50) - data.get('grid_load', 50)) ** 2
    return [data, arrival_time, time.time()]

@SQify
def load_sliding(moment_data):
    import sys, os, time
    global sq_state
    data = moment_data[0]
    arrival_time = moment_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('sliding_linear_reg')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['load_sliding_avg'] = data.get('load_forecast', 50) * 0.98
    return [data, arrival_time, time.time()]

@SQify
def load_join(sliding_data):
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
def load_publish(join_data):
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


# ===== BRANCH 2: Anomaly Detection (7 SQs) =====
@SQify
def anom_threshold(bloom_data):
    import sys, os, time
    global sq_state
    data = bloom_data[0]
    arrival_time = bloom_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('range_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    temp = data.get('transformer_temp', 25)
    data['temp_anomaly'] = temp > 80 or temp < -20
    data['load_anomaly'] = data.get('grid_load', 50) > 300
    return [data, arrival_time, time.time()]

@SQify
def anom_moment(threshold_data):
    import sys, os, time
    global sq_state
    data = threshold_data[0]
    arrival_time = threshold_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('second_order_moment')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['anom_variance'] = data.get('grid_load', 50) * 0.15
    return [data, arrival_time, time.time()]

@SQify
def anom_classify(moment_data):
    import sys, os, time
    global sq_state
    data = moment_data[0]
    arrival_time = moment_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('decision_tree_classify')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    if data.get('temp_anomaly') or data.get('load_anomaly'):
        data['anomaly_class'] = 'critical'
    elif data.get('anom_variance', 0) > 10:
        data['anomaly_class'] = 'warning'
    else:
        data['anomaly_class'] = 'normal'
    return [data, arrival_time, time.time()]

@SQify
def anom_interp(classify_data):
    import sys, os, time
    global sq_state
    data = classify_data[0]
    arrival_time = classify_data[1]
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
def anom_error(interp_data):
    import sys, os, time
    global sq_state
    data = interp_data[0]
    arrival_time = interp_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('error_estimation')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['detection_confidence'] = 0.95 if data.get('anomaly_class') == 'critical' else 0.7
    return [data, arrival_time, time.time()]

@SQify
def anom_join(error_data):
    import sys, os, time
    global sq_state
    data = error_data[0]
    arrival_time = error_data[1]
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
def anom_publish(join_data):
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


# ===== BRANCH 3: Stability Analysis (7 SQs) =====
@SQify
def stab_interp(bloom_data):
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
def stab_avg(interp_data):
    import sys, os, time
    global sq_state
    data = interp_data[0]
    arrival_time = interp_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('average')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['avg_load'] = data.get('grid_load', 50)
    data['avg_temp'] = data.get('transformer_temp', 25)
    return [data, arrival_time, time.time()]

@SQify
def stab_distinct(avg_data):
    import sys, os, time
    global sq_state
    data = avg_data[0]
    arrival_time = avg_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('distinct_approx_count')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['distinct_sources'] = int(data.get('grid_load', 50)) % 20
    return [data, arrival_time, time.time()]

@SQify
def stab_kalman(distinct_data):
    import sys, os, time
    global sq_state
    data = distinct_data[0]
    arrival_time = distinct_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['stability_filtered'] = data.get('avg_load', 50) * 0.93
    return [data, arrival_time, time.time()]

@SQify
def stab_score(kalman_data):
    import sys, os, time
    global sq_state
    data = kalman_data[0]
    arrival_time = kalman_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('interpolation')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    load = data.get('stability_filtered', 50)
    temp = data.get('avg_temp', 25)
    data['stability_index'] = max(0, 100 - abs(load - 150) * 0.3 - abs(temp - 40) * 0.5)
    return [data, arrival_time, time.time()]

@SQify
def stab_join(score_data):
    import sys, os, time
    global sq_state
    data = score_data[0]
    arrival_time = score_data[1]
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
def stab_publish(join_data):
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


# ===== BRANCH 4: Demand Forecast (8 SQs) =====
@SQify
def demand_avg(bloom_data):
    import sys, os, time
    global sq_state
    data = bloom_data[0]
    arrival_time = bloom_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('average')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['demand_base'] = data.get('grid_load', 50) * 1.1
    return [data, arrival_time, time.time()]

@SQify
def demand_kalman(avg_data):
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
    data['demand_filtered'] = data.get('demand_base', 55) * 0.94
    return [data, arrival_time, time.time()]

@SQify
def demand_linreg(kalman_data):
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
    data['demand_trend'] = data.get('demand_filtered', 55) * 1.03
    return [data, arrival_time, time.time()]

@SQify
def demand_dt(linreg_data):
    import sys, os, time
    global sq_state
    data = linreg_data[0]
    arrival_time = linreg_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('decision_tree_classify')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    demand = data.get('demand_trend', 55)
    data['demand_class'] = 'peak' if demand > 200 else 'normal' if demand > 80 else 'low'
    return [data, arrival_time, time.time()]

@SQify
def demand_interp(dt_data):
    import sys, os, time
    global sq_state
    data = dt_data[0]
    arrival_time = dt_data[1]
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
def demand_error(interp_data):
    import sys, os, time
    global sq_state
    data = interp_data[0]
    arrival_time = interp_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('error_estimation')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['demand_confidence'] = 0.85
    return [data, arrival_time, time.time()]

@SQify
def demand_join(error_data):
    import sys, os, time
    global sq_state
    data = error_data[0]
    arrival_time = error_data[1]
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
def demand_publish(join_data):
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


# ===== FUSION + TAIL (6 SQs) =====
@SQify
def grid_fusion(load_data, anom_data, stab_data, demand_data):
    import sys, os, time
    global sq_state
    l = load_data[0]
    a = anom_data[0]
    s = stab_data[0]
    d = demand_data[0]
    arrival_time = load_data[1]
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
        'load_forecast': l.get('load_forecast', 0),
        'anomaly_class': a.get('anomaly_class', 'normal'),
        'stability_index': s.get('stability_index', 0),
        'demand_class': d.get('demand_class', 'normal'),
        'transformer_temp': l.get('transformer_temp', 0),
    }
    return [fused, arrival_time, time.time()]

@SQify
def grid_response(fused_data):
    import sys, os, time
    global sq_state
    data = fused_data[0]
    arrival_time = fused_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('decision_tree_classify')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    if data.get('anomaly_class') == 'critical':
        data['response'] = 'emergency_shutdown'
    elif data.get('stability_index', 100) < 30:
        data['response'] = 'load_shedding'
    elif data.get('demand_class') == 'peak':
        data['response'] = 'activate_reserves'
    else:
        data['response'] = 'normal_operation'
    return [data, arrival_time, time.time()]

@SQify
def grid_alert(response_data):
    import sys, os, time
    global sq_state
    data = response_data[0]
    arrival_time = response_data[1]
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
def grid_dashboard(alert_data):
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
def grid_mqtt(dash_data):
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
def grid_sink(mqtt_data):
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
        'latency_ms': latency_ms, 'workload': 'eval_grid',
        'run_label': run_label, 'predicted_ms': predicted_ms,
    }
    log_dir = os.environ.get('TTPYTHON_LOG_DIR', '.')
    log_file = os.path.join(log_dir, 'runtime_log_' + (run_label or 'eval_grid') + '.jsonl')
    with open(log_file, 'a') as f:
        f.write(json.dumps(log_entry) + '\n')
    print('GRID done, latency=' + str(latency_ms) + 'ms')
    return 1


# ===== GRAPH WIRING =====
@GRAPHify
def eval_grid(trigger):
    A_1 = 1
    with TTClock.root() as root_clock:
        start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
        N = 100
        stop_time = start_time + (2000000 * N)
        sampling_time = VALUES_TO_TTTIME(start_time, stop_time)
        sample_window = COPY_TTTIME(A_1, sampling_time)

        with TTConstraint(components=["sensor_interface"]):
            wl = 'SYS'
            raw = grid_source(sample_window, wl, TTClock=root_clock, TTPeriod=2000000, TTPhase=0, TTDataIntervalWidth=1000000)

        with TTConstraint(components=["compute"]):
            parsed = grid_parse(raw)
            bloomed = grid_bloom(parsed)

        # Branch 1: Load Prediction
        with TTConstraint(components=["compute"]):
            lk = load_kalman(bloomed)
            llr = load_linreg(lk)
            lf = load_forecast(llr)
            lm = load_moment(lf)
            ls = load_sliding(lm)
            lj = load_join(ls)
        with TTConstraint(components=["mqtt_broker"]):
            lp = load_publish(lj)

        # Branch 2: Anomaly Detection
        with TTConstraint(components=["compute"]):
            at = anom_threshold(bloomed)
            am = anom_moment(at)
            ac = anom_classify(am)
            ai = anom_interp(ac)
            ae = anom_error(ai)
            aj = anom_join(ae)
        with TTConstraint(components=["mqtt_broker"]):
            ap = anom_publish(aj)

        # Branch 3: Stability Analysis
        with TTConstraint(components=["compute"]):
            si = stab_interp(bloomed)
            sa = stab_avg(si)
            sd = stab_distinct(sa)
            sk = stab_kalman(sd)
            ss = stab_score(sk)
            sj = stab_join(ss)
        with TTConstraint(components=["mqtt_broker"]):
            sp = stab_publish(sj)

        # Branch 4: Demand Forecast
        with TTConstraint(components=["compute"]):
            da = demand_avg(bloomed)
            dk = demand_kalman(da)
            dlr = demand_linreg(dk)
            ddt = demand_dt(dlr)
            di = demand_interp(ddt)
            de = demand_error(di)
            dj = demand_join(de)
        with TTConstraint(components=["mqtt_broker"]):
            dp = demand_publish(dj)

        # Fusion + tail
        with TTConstraint(components=["compute"]):
            fused = grid_fusion(lp, ap, sp, dp)
            responded = grid_response(fused)
        with TTConstraint(components=["mqtt_broker", "storage"]):
            alerted = grid_alert(responded)
            dashed = grid_dashboard(alerted)
            published = grid_mqtt(dashed)
            result = grid_sink(published)
