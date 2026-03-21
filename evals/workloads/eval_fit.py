# eval_fit.py — Fitness & Health Monitoring Pipeline
#
# RIoTBench-inspired mHealth application using FIT dataset (wearable sensors:
# chest/ankle/arm accelerometers, gyroscopes, magnetometers, ECG leads).
#
# Topology: Five-branch parallel with health fusion
#   fit_source -> parse -> bloom -+-> Activity branch (7): avg -> kalman -> linreg -> dt -> moment -> act_join -> act_pub
#                                 +-> Vitals branch (7): kalman -> linreg -> threshold -> error -> moment -> vit_join -> vit_pub
#                                 +-> Gait branch (7): interp -> moment -> kalman -> distinct -> score -> gait_join -> gait_pub
#                                 +-> Posture branch (7): avg -> range -> dt -> interp -> kalman -> post_join -> post_pub
#                                 +-> Rest branch (5): avg -> kalman -> dt -> rest_join -> rest_pub
#                                 All five -> health_fusion -> health_score -> alert -> dashboard -> mqtt -> sink
#
# App SQs: 1 + 2 + 7 + 7 + 7 + 7 + 5 + 6 = 42
# Dataset: FIT (FIT_sample_data_senml.csv, mHealth wearable sensor data)
#
from ticktalkpython.SQ import STREAMify, SQify, GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *


# ===== SOURCE =====
@STREAMify
def fit_source(trigger, workload_type):
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


# ===== COMMON: parse -> bloom =====
@SQify
def fit_parse(raw_data):
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
def fit_bloom(parsed_data):
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


# ===== BRANCH 1: Activity Classification — chest accelerometer (7 SQs) =====
@SQify
def act_avg(bloom_data):
    import sys, os, time, math
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
    ax = data.get('acc_chest_x', 0)
    ay = data.get('acc_chest_y', 0)
    az = data.get('acc_chest_z', 0)
    data['accel_magnitude'] = math.sqrt(ax**2 + ay**2 + az**2)
    return [data, arrival_time, time.time()]

@SQify
def act_kalman(avg_data):
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
    data['accel_filtered'] = data.get('accel_magnitude', 9.8) * 0.95
    return [data, arrival_time, time.time()]

@SQify
def act_linreg(kalman_data):
    import sys, os, time
    global sq_state
    data = kalman_data[0]
    arrival_time = kalman_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('sliding_linear_reg')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['accel_trend'] = data.get('accel_filtered', 9.8) * 1.01
    return [data, arrival_time, time.time()]

@SQify
def act_classify(linreg_data):
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
    mag = data.get('accel_filtered', 9.8)
    if mag < 9.5:
        data['activity'] = 'lying'
    elif mag < 10.5:
        data['activity'] = 'standing'
    elif mag < 12:
        data['activity'] = 'walking'
    else:
        data['activity'] = 'running'
    return [data, arrival_time, time.time()]

@SQify
def act_moment(classify_data):
    import sys, os, time
    global sq_state
    data = classify_data[0]
    arrival_time = classify_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('second_order_moment')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['accel_variance'] = (data.get('accel_filtered', 9.8) - 9.8) ** 2
    return [data, arrival_time, time.time()]

@SQify
def act_join(moment_data):
    import sys, os, time
    global sq_state
    data = moment_data[0]
    arrival_time = moment_data[1]
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
def act_publish(join_data):
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


# ===== BRANCH 2: Vitals Monitor — ECG (7 SQs) =====
@SQify
def vit_kalman(bloom_data):
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
    ecg1 = data.get('ecg_lead_1', 0.0)
    ecg2 = data.get('ecg_lead_2', 0.0)
    data['ecg_filtered'] = (ecg1 + ecg2) / 2
    data['heart_rate_est'] = 60 + abs(ecg1) * 400
    return [data, arrival_time, time.time()]

@SQify
def vit_linreg(kalman_data):
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
    data['hr_trend'] = data.get('heart_rate_est', 72) * 0.98
    return [data, arrival_time, time.time()]

@SQify
def vit_threshold(linreg_data):
    import sys, os, time
    global sq_state
    data = linreg_data[0]
    arrival_time = linreg_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('range_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    hr = data.get('heart_rate_est', 72)
    data['hr_alert'] = hr > 180 or hr < 40
    return [data, arrival_time, time.time()]

@SQify
def vit_error(threshold_data):
    import sys, os, time
    global sq_state
    data = threshold_data[0]
    arrival_time = threshold_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('error_estimation')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['ecg_quality'] = 1.0 - abs(data.get('ecg_filtered', 0)) * 2
    return [data, arrival_time, time.time()]

@SQify
def vit_moment(error_data):
    import sys, os, time
    global sq_state
    data = error_data[0]
    arrival_time = error_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('second_order_moment')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['hr_variance'] = (data.get('heart_rate_est', 72) - 72) ** 2
    return [data, arrival_time, time.time()]

@SQify
def vit_join(moment_data):
    import sys, os, time
    global sq_state
    data = moment_data[0]
    arrival_time = moment_data[1]
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
def vit_publish(join_data):
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


# ===== BRANCH 3: Gait Analysis — ankle sensors (7 SQs) =====
@SQify
def gait_interp(bloom_data):
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
def gait_moment(interp_data):
    import sys, os, time, math
    global sq_state
    data = interp_data[0]
    arrival_time = interp_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('second_order_moment')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    gx = data.get('gyro_ankle_x', 0)
    gy = data.get('gyro_ankle_y', 0)
    gz = data.get('gyro_ankle_z', 0)
    data['ankle_gyro_mag'] = math.sqrt(gx**2 + gy**2 + gz**2)
    return [data, arrival_time, time.time()]

@SQify
def gait_kalman(moment_data):
    import sys, os, time
    global sq_state
    data = moment_data[0]
    arrival_time = moment_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    data['gait_filtered'] = data.get('ankle_gyro_mag', 0) * 0.9
    return [data, arrival_time, time.time()]

@SQify
def gait_distinct(kalman_data):
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
    return [data, arrival_time, time.time()]

@SQify
def gait_score(distinct_data):
    import sys, os, time
    global sq_state
    data = distinct_data[0]
    arrival_time = distinct_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('interpolation')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    gait = data.get('gait_filtered', 0)
    data['gait_symmetry'] = max(0, 100 - abs(gait - 1.0) * 50)
    return [data, arrival_time, time.time()]

@SQify
def gait_join(score_data):
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
def gait_publish(join_data):
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


# ===== BRANCH 4: Posture Detection — arm sensors (7 SQs) =====
@SQify
def post_avg(bloom_data):
    import sys, os, time, math
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
    ax = data.get('acc_arm_x', 0)
    ay = data.get('acc_arm_y', 0)
    az = data.get('acc_arm_z', 0)
    data['arm_magnitude'] = math.sqrt(ax**2 + ay**2 + az**2)
    return [data, arrival_time, time.time()]

@SQify
def post_range(avg_data):
    import sys, os, time
    global sq_state
    data = avg_data[0]
    arrival_time = avg_data[1]
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
def post_classify(range_data):
    import sys, os, time
    global sq_state
    data = range_data[0]
    arrival_time = range_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('decision_tree_classify')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    mag = data.get('arm_magnitude', 9.8)
    if mag < 9.0:
        data['posture'] = 'arms_raised'
    elif mag < 10.5:
        data['posture'] = 'arms_down'
    else:
        data['posture'] = 'arms_active'
    return [data, arrival_time, time.time()]

@SQify
def post_interp(classify_data):
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
def post_kalman(interp_data):
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
    data['posture_smoothed'] = data.get('arm_magnitude', 9.8) * 0.96
    return [data, arrival_time, time.time()]

@SQify
def post_join(kalman_data):
    import sys, os, time
    global sq_state
    data = kalman_data[0]
    arrival_time = kalman_data[1]
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
def post_publish(join_data):
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


# ===== BRANCH 5: Rest/Recovery Detection — magnetometer (5 SQs) =====
@SQify
def rest_avg(bloom_data):
    import sys, os, time, math
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
    mx = data.get('magnetometer_arm_x', 0)
    my = data.get('magnetometer_arm_y', 0)
    mz = data.get('magnetometer_arm_z', 0)
    data['mag_magnitude'] = math.sqrt(mx**2 + my**2 + mz**2)
    return [data, arrival_time, time.time()]

@SQify
def rest_kalman(avg_data):
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
    data['mag_filtered'] = data.get('mag_magnitude', 1.0) * 0.93
    return [data, arrival_time, time.time()]

@SQify
def rest_classify(kalman_data):
    import sys, os, time
    global sq_state
    data = kalman_data[0]
    arrival_time = kalman_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('decision_tree_classify')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    mag = data.get('mag_filtered', 1.0)
    data['rest_state'] = 'resting' if mag < 0.5 else 'active' if mag < 1.5 else 'vigorous'
    return [data, arrival_time, time.time()]

@SQify
def rest_join(classify_data):
    import sys, os, time
    global sq_state
    data = classify_data[0]
    arrival_time = classify_data[1]
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
def rest_publish(join_data):
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


# ===== FUSION + HEALTH SCORE + TAIL (6 SQs) =====
@SQify
def health_fusion(act_data, vit_data, gait_data, post_data, rest_data):
    import sys, os, time
    global sq_state
    a = act_data[0]
    v = vit_data[0]
    g = gait_data[0]
    p = post_data[0]
    r = rest_data[0]
    arrival_time = act_data[1]
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
        'activity': a.get('activity', 'unknown'),
        'heart_rate': v.get('heart_rate_est', 72),
        'hr_alert': v.get('hr_alert', False),
        'gait_symmetry': g.get('gait_symmetry', 0),
        'posture': p.get('posture', 'unknown'),
        'rest_state': r.get('rest_state', 'unknown'),
    }
    return [fused, arrival_time, time.time()]

@SQify
def health_score(fused_data):
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
    hr = data.get('heart_rate', 72)
    gait = data.get('gait_symmetry', 50)
    hr_score = max(0, 100 - abs(hr - 72) * 1.5)
    data['health_score'] = (hr_score + gait) / 2
    data['health_status'] = 'good' if data['health_score'] > 70 else 'monitor' if data['health_score'] > 40 else 'alert'
    return [data, arrival_time, time.time()]

@SQify
def fit_alert(score_data):
    import sys, os, time
    global sq_state
    data = score_data[0]
    arrival_time = score_data[1]
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
    if data.get('hr_alert'):
        alerts.append('abnormal_heart_rate')
    if data.get('health_status') == 'alert':
        alerts.append('low_health_score')
    data['alerts'] = alerts
    return [data, arrival_time, time.time()]

@SQify
def fit_dashboard(alert_data):
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
def fit_mqtt(dash_data):
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
def fit_sink(mqtt_data):
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
        'latency_ms': latency_ms, 'workload': 'eval_fit',
        'run_label': run_label, 'predicted_ms': predicted_ms,
    }
    log_dir = os.environ.get('TTPYTHON_LOG_DIR', '.')
    log_file = os.path.join(log_dir, 'runtime_log_' + (run_label or 'eval_fit') + '.jsonl')
    with open(log_file, 'a') as f:
        f.write(json.dumps(log_entry) + '\n')
    print('FIT done, latency=' + str(latency_ms) + 'ms')
    return 1


# ===== GRAPH WIRING =====
@GRAPHify
def eval_fit(trigger):
    A_1 = 1
    with TTClock.root() as root_clock:
        start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
        N = 100
        stop_time = start_time + (2000000 * N)
        sampling_time = VALUES_TO_TTTIME(start_time, stop_time)
        sample_window = COPY_TTTIME(A_1, sampling_time)

        with TTConstraint(components=["sensor_interface"]):
            wl = 'FIT'
            raw = fit_source(sample_window, wl, TTClock=root_clock, TTPeriod=2000000, TTPhase=0, TTDataIntervalWidth=1000000)

        with TTConstraint(components=["compute"]):
            parsed = fit_parse(raw)
            bloomed = fit_bloom(parsed)

        # Branch 1: Activity Classification
        with TTConstraint(components=["compute"]):
            aa = act_avg(bloomed)
            ak = act_kalman(aa)
            alr = act_linreg(ak)
            acl = act_classify(alr)
            am = act_moment(acl)
            aj = act_join(am)
        with TTConstraint(components=["mqtt_broker"]):
            ap = act_publish(aj)

        # Branch 2: Vitals Monitor
        with TTConstraint(components=["compute"]):
            vk = vit_kalman(bloomed)
            vlr = vit_linreg(vk)
            vt = vit_threshold(vlr)
            ve = vit_error(vt)
            vm = vit_moment(ve)
            vj = vit_join(vm)
        with TTConstraint(components=["mqtt_broker"]):
            vp = vit_publish(vj)

        # Branch 3: Gait Analysis
        with TTConstraint(components=["compute"]):
            gi = gait_interp(bloomed)
            gm = gait_moment(gi)
            gk = gait_kalman(gm)
            gd = gait_distinct(gk)
            gs = gait_score(gd)
            gj = gait_join(gs)
        with TTConstraint(components=["mqtt_broker"]):
            gp = gait_publish(gj)

        # Branch 4: Posture Detection
        with TTConstraint(components=["compute"]):
            pa = post_avg(bloomed)
            pr = post_range(pa)
            pcl = post_classify(pr)
            pi = post_interp(pcl)
            pk = post_kalman(pi)
            pj = post_join(pk)
        with TTConstraint(components=["mqtt_broker"]):
            pp = post_publish(pj)

        # Branch 5: Rest/Recovery Detection
        with TTConstraint(components=["compute"]):
            ra = rest_avg(bloomed)
            rk = rest_kalman(ra)
            rcl = rest_classify(rk)
            rj = rest_join(rcl)
        with TTConstraint(components=["mqtt_broker"]):
            rp = rest_publish(rj)

        # Fusion + tail
        with TTConstraint(components=["compute"]):
            fused = health_fusion(ap, vp, gp, pp, rp)
            scored = health_score(fused)
        with TTConstraint(components=["mqtt_broker", "storage"]):
            alerted = fit_alert(scored)
            dashed = fit_dashboard(alerted)
            published = fit_mqtt(dashed)
            result = fit_sink(published)
