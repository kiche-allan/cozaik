# eval_city.py — Smart City Environmental + Traffic Monitoring
#
# Consolidation of eval_env_monitoring + eval_urban_sensing (per Joannah).
# Dual-source composite: environmental sensor station + traffic flow sensors.
#
# Topology:
#   Source A (env) -> parse -> bloom -> 8 parallel branches (filter->kalman->check each) -> fusion -> AQI
#   Source B (traffic) -> parse -> range -> bloom -> interp -> speed -> congestion -> route -> join -> annotate
#   Both -> city_fusion -> visualize -> alert -> dashboard -> mqtt -> sink
#
# App SQs: 2 sources + 2 common + 24 branches + 2 fusion + 9 traffic + 6 tail = 45
# Dataset: SYS (SYS_sample_data_senml.csv)
#
from ticktalkpython.SQ import STREAMify, SQify, GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *


# ===== SOURCE A: Environmental sensor station =====
@STREAMify
def env_source(trigger, workload_type):
    import sys, os, time, random
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
    parsed['temp_c'] = parsed.get('temperature', random.uniform(-10, 45))
    parsed['humid_pct'] = parsed.get('humidity', random.uniform(10, 95))
    parsed['pm25'] = parsed.get('dust', random.uniform(50, 500))
    parsed['co_ppm'] = random.uniform(0.1, 15)
    parsed['no2_ppb'] = random.uniform(5, 200)
    parsed['o3_ppb'] = random.uniform(10, 150)
    parsed['noise_db'] = random.uniform(30, 95)
    parsed['wind_ms'] = random.uniform(0, 30)
    return [parsed, time.time()]


# ===== SOURCE B: Traffic flow sensors =====
@STREAMify
def traffic_source(trigger, workload_type):
    import sys, os, time, random
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
    parsed['flow_rate'] = random.uniform(5, 60)
    parsed['occupancy'] = random.uniform(0.05, 0.95)
    parsed['avg_speed'] = random.uniform(10, 120)
    return [parsed, time.time()]


# ===== ENV COMMON: parse -> bloom =====
@SQify
def env_parse(raw_data):
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
def env_bloom(parsed_data):
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


# ===== BRANCH 1: Temperature =====
@SQify
def temp_filter(bloom_data):
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
    data['temp_valid'] = -50 < data.get('temp_c', 20) < 60
    return [data, arrival_time, time.time()]

@SQify
def temp_kalman(filtered_data):
    import sys, os, time
    global sq_state
    data = filtered_data[0]
    arrival_time = filtered_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['temp_smoothed'] = data.get('temp_c', 20) * 0.95
    return [data, arrival_time, time.time()]

@SQify
def temp_check(kalman_data):
    import sys, os, time
    global sq_state
    data = kalman_data[0]
    arrival_time = kalman_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('average')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['temp_anomaly'] = data.get('temp_smoothed', 20) > 42 or data.get('temp_smoothed', 20) < -15
    return [data, arrival_time, time.time()]


# ===== BRANCH 2: Humidity =====
@SQify
def humid_filter(bloom_data):
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
    return [data, arrival_time, time.time()]

@SQify
def humid_kalman(filtered_data):
    import sys, os, time
    global sq_state
    data = filtered_data[0]
    arrival_time = filtered_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['humid_smoothed'] = data.get('humid_pct', 50) * 0.96
    return [data, arrival_time, time.time()]

@SQify
def humid_check(kalman_data):
    import sys, os, time
    global sq_state
    data = kalman_data[0]
    arrival_time = kalman_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('average')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    return [data, arrival_time, time.time()]


# ===== BRANCH 3: PM2.5 =====
@SQify
def pm25_filter(bloom_data):
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
    return [data, arrival_time, time.time()]

@SQify
def pm25_kalman(filtered_data):
    import sys, os, time
    global sq_state
    data = filtered_data[0]
    arrival_time = filtered_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['pm25_smoothed'] = data.get('pm25', 100) * 0.92
    return [data, arrival_time, time.time()]

@SQify
def pm25_check(kalman_data):
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
    data['pm25_anomaly'] = data.get('pm25_smoothed', 100) > 300
    return [data, arrival_time, time.time()]


# ===== BRANCH 4: CO =====
@SQify
def co_filter(bloom_data):
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
    return [data, arrival_time, time.time()]

@SQify
def co_kalman(filtered_data):
    import sys, os, time
    global sq_state
    data = filtered_data[0]
    arrival_time = filtered_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['co_smoothed'] = data.get('co_ppm', 1) * 0.93
    return [data, arrival_time, time.time()]

@SQify
def co_check(kalman_data):
    import sys, os, time
    global sq_state
    data = kalman_data[0]
    arrival_time = kalman_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('second_order_moment')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['co_anomaly'] = data.get('co_smoothed', 1) > 10
    return [data, arrival_time, time.time()]


# ===== BRANCH 5: NO2 =====
@SQify
def no2_filter(bloom_data):
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
    return [data, arrival_time, time.time()]

@SQify
def no2_kalman(filtered_data):
    import sys, os, time
    global sq_state
    data = filtered_data[0]
    arrival_time = filtered_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['no2_smoothed'] = data.get('no2_ppb', 40) * 0.94
    return [data, arrival_time, time.time()]

@SQify
def no2_check(kalman_data):
    import sys, os, time
    global sq_state
    data = kalman_data[0]
    arrival_time = kalman_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('distinct_approx_count')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['no2_anomaly'] = data.get('no2_smoothed', 40) > 150
    return [data, arrival_time, time.time()]


# ===== BRANCH 6: Ozone =====
@SQify
def o3_filter(bloom_data):
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
    return [data, arrival_time, time.time()]

@SQify
def o3_kalman(filtered_data):
    import sys, os, time
    global sq_state
    data = filtered_data[0]
    arrival_time = filtered_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['o3_smoothed'] = data.get('o3_ppb', 50) * 0.91
    return [data, arrival_time, time.time()]

@SQify
def o3_check(kalman_data):
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
    data['o3_anomaly'] = data.get('o3_smoothed', 50) > 120
    return [data, arrival_time, time.time()]


# ===== BRANCH 7: Noise =====
@SQify
def noise_filter(bloom_data):
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
    return [data, arrival_time, time.time()]

@SQify
def noise_kalman(filtered_data):
    import sys, os, time
    global sq_state
    data = filtered_data[0]
    arrival_time = filtered_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['noise_smoothed'] = data.get('noise_db', 50) * 0.97
    return [data, arrival_time, time.time()]

@SQify
def noise_check(kalman_data):
    import sys, os, time
    global sq_state
    data = kalman_data[0]
    arrival_time = kalman_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('average')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['noise_anomaly'] = data.get('noise_smoothed', 50) > 85
    return [data, arrival_time, time.time()]


# ===== BRANCH 8: Wind =====
@SQify
def wind_filter(bloom_data):
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
    return [data, arrival_time, time.time()]

@SQify
def wind_kalman(filtered_data):
    import sys, os, time
    global sq_state
    data = filtered_data[0]
    arrival_time = filtered_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['wind_smoothed'] = data.get('wind_ms', 5) * 0.94
    return [data, arrival_time, time.time()]

@SQify
def wind_check(kalman_data):
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
    data['wind_anomaly'] = data.get('wind_smoothed', 5) > 25
    return [data, arrival_time, time.time()]


# ===== ENV FUSION + AQI =====
@SQify
def env_fusion(t_data, h_data, pm_data, co_data, no2_data, o3_data, noise_data, wind_data):
    import sys, os, time
    global sq_state
    data = t_data[0].copy()
    arrival_time = t_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('join')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['humid_smoothed'] = h_data[0].get('humid_smoothed', 0)
    data['pm25_smoothed'] = pm_data[0].get('pm25_smoothed', 0)
    data['co_smoothed'] = co_data[0].get('co_smoothed', 0)
    data['no2_smoothed'] = no2_data[0].get('no2_smoothed', 0)
    data['o3_smoothed'] = o3_data[0].get('o3_smoothed', 0)
    data['noise_smoothed'] = noise_data[0].get('noise_smoothed', 0)
    data['wind_smoothed'] = wind_data[0].get('wind_smoothed', 0)
    return [data, arrival_time, time.time()]

@SQify
def aqi_compute(fused_data):
    import sys, os, time
    global sq_state
    data = fused_data[0]
    arrival_time = fused_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('interpolation')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    pm = data.get('pm25_smoothed', 50)
    no2 = data.get('no2_smoothed', 40)
    o3 = data.get('o3_smoothed', 50)
    data['aqi'] = min(500, int(pm * 0.5 + no2 * 0.3 + o3 * 0.2))
    data['aqi_category'] = 'Good' if data['aqi'] < 50 else 'Moderate' if data['aqi'] < 100 else 'Unhealthy'
    return [data, arrival_time, time.time()]


# ===== TRAFFIC CHAIN (9 SQs) =====
@SQify
def t_parse(raw_data):
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
def t_range(parsed_data):
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
def t_bloom(range_data):
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
def t_interp(bloom_data):
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
def t_speed(interp_data):
    import sys, os, time
    global sq_state
    data = interp_data[0]
    arrival_time = interp_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['speed_filtered'] = data.get('avg_speed', 50) * 0.95
    return [data, arrival_time, time.time()]

@SQify
def t_congestion(speed_data):
    import sys, os, time
    global sq_state
    data = speed_data[0]
    arrival_time = speed_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('decision_tree_classify')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    flow = data.get('flow_rate', 20)
    occ = data.get('occupancy', 0.3)
    if flow > 40 and occ > 0.7:
        data['congestion'] = 'severe'
    elif flow > 25 or occ > 0.5:
        data['congestion'] = 'moderate'
    else:
        data['congestion'] = 'free_flow'
    return [data, arrival_time, time.time()]

@SQify
def t_route(cong_data):
    import sys, os, time
    global sq_state
    data = cong_data[0]
    arrival_time = cong_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('interpolation')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['route'] = 'alternate' if data.get('congestion') == 'severe' else 'normal'
    return [data, arrival_time, time.time()]

@SQify
def t_join(route_data):
    import sys, os, time
    global sq_state
    data = route_data[0]
    arrival_time = route_data[1]
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
def t_annotate(joined_data):
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
    data['annotation'] = 'city_traffic_v1'
    return [data, arrival_time, time.time()]


# ===== CITY FUSION + DASHBOARD TAIL (6 SQs) =====
@SQify
def city_fusion(env_data, traffic_data):
    import sys, os, time
    global sq_state
    e = env_data[0]
    t = traffic_data[0]
    arrival_time = env_data[1]
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
        'aqi': e.get('aqi', 0), 'aqi_category': e.get('aqi_category', ''),
        'congestion': t.get('congestion', ''), 'route': t.get('route', ''),
        'temp': e.get('temp_smoothed', 0), 'pm25': e.get('pm25_smoothed', 0),
        'speed': t.get('speed_filtered', 0),
    }
    return [fused, arrival_time, time.time()]

@SQify
def city_visualize(fused_data):
    import sys, os, time
    global sq_state
    data = fused_data[0]
    arrival_time = fused_data[1]
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
def city_alert(viz_data):
    import sys, os, time
    global sq_state
    data = viz_data[0]
    arrival_time = viz_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('mqtt_publish')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    alerts = []
    if data.get('aqi', 0) > 100:
        alerts.append('air_quality_warning')
    if data.get('congestion') == 'severe':
        alerts.append('traffic_congestion')
    data['alerts'] = alerts
    return [data, arrival_time, time.time()]

@SQify
def city_dashboard(alert_data):
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
def city_mqtt(dash_data):
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
def city_sink(mqtt_data):
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
        'latency_ms': latency_ms, 'workload': 'eval_city',
        'run_label': run_label, 'predicted_ms': predicted_ms,
    }
    log_dir = os.environ.get('TTPYTHON_LOG_DIR', '.')
    log_file = os.path.join(log_dir, 'runtime_log_' + (run_label or 'eval_city') + '.jsonl')
    with open(log_file, 'a') as f:
        f.write(json.dumps(log_entry) + '\n')
    print('CITY done, latency=' + str(latency_ms) + 'ms')
    return 1


# ===== GRAPH WIRING =====
@GRAPHify
def eval_city(trigger):
    A_1 = 1
    with TTClock.root() as root_clock:
        start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
        N = 100
        stop_time = start_time + (2500000 * N)
        sampling_time = VALUES_TO_TTTIME(start_time, stop_time)
        sample_window = COPY_TTTIME(A_1, sampling_time)

        with TTConstraint(components=["sensor_interface"]):
            wl_env = 'SYS'
            env_raw = env_source(sample_window, wl_env, TTClock=root_clock, TTPeriod=2500000, TTPhase=0, TTDataIntervalWidth=1250000)

        with TTConstraint(components=["sensor_interface"]):
            wl_traffic = 'SYS'
            traffic_raw = traffic_source(sample_window, wl_traffic, TTClock=root_clock, TTPeriod=2500000, TTPhase=500000, TTDataIntervalWidth=1250000)

        with TTConstraint(components=["compute"]):
            ep = env_parse(env_raw)
            eb = env_bloom(ep)

        with TTConstraint(components=["compute"]):
            tf = temp_filter(eb)
            tk = temp_kalman(tf)
            tc = temp_check(tk)

        with TTConstraint(components=["compute"]):
            hf = humid_filter(eb)
            hk = humid_kalman(hf)
            hc = humid_check(hk)

        with TTConstraint(components=["compute"]):
            pf = pm25_filter(eb)
            pk = pm25_kalman(pf)
            pc = pm25_check(pk)

        with TTConstraint(components=["compute"]):
            cf = co_filter(eb)
            ck = co_kalman(cf)
            cc = co_check(ck)

        with TTConstraint(components=["compute"]):
            nf = no2_filter(eb)
            nk = no2_kalman(nf)
            nc = no2_check(nk)

        with TTConstraint(components=["compute"]):
            of = o3_filter(eb)
            ok2 = o3_kalman(of)
            oc = o3_check(ok2)

        with TTConstraint(components=["compute"]):
            nsf = noise_filter(eb)
            nsk = noise_kalman(nsf)
            nsc = noise_check(nsk)

        with TTConstraint(components=["compute"]):
            wf = wind_filter(eb)
            wk = wind_kalman(wf)
            wc = wind_check(wk)

        with TTConstraint(components=["compute"]):
            fused_env = env_fusion(tc, hc, pc, cc, nc, oc, nsc, wc)
            aqi = aqi_compute(fused_env)

        with TTConstraint(components=["compute"]):
            tp = t_parse(traffic_raw)
            tr = t_range(tp)
            tb = t_bloom(tr)
            ti = t_interp(tb)
            ts = t_speed(ti)
            tcon = t_congestion(ts)
            trt = t_route(tcon)
            tj = t_join(trt)
            ta = t_annotate(tj)

        with TTConstraint(components=["compute"]):
            cfused = city_fusion(aqi, ta)
            cv = city_visualize(cfused)

        with TTConstraint(components=["mqtt_broker", "storage"]):
            ca = city_alert(cv)
            cd = city_dashboard(ca)
            cm = city_mqtt(cd)
            result = city_sink(cm)
