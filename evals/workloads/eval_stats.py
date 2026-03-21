from ticktalkpython.SQ import STREAMify, SQify, GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *

@STREAMify
def stats_spout(trigger, workload_type):
    import sys, os, time
    global sq_state
    if sq_state.get('generator', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_senml_generator
        sq_state['generator'] = get_senml_generator(workload_type)
    message = sq_state['generator'].generate_message()
    return [message, time.time()]

@SQify
def parse_project(raw_data):
    import sys, os, time
    global sq_state
    message = raw_data[0]
    arrival_time = raw_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('parse_project')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    from riotbench_provider import parse_senml_message
    p = parse_senml_message(message, 'SYS')
    parsed = {
        'sensor_id': p['sensor_id'],
        'obs_type': p['obs_type'],
        'timestamp': p['timestamp'],
        'values': p['values'],
        'raw_readings': p['readings'],
    }
    return [parsed, arrival_time, time.time()]

@SQify
def bloom_filter_check(parsed_data):
    import sys, os, time, hashlib
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
        sq_state['bitmap_size'] = 10000
        sq_state['bitmap'] = [False] * sq_state['bitmap_size']
        from riotbench_provider import get_bloom_ids
        for real_id in get_bloom_ids('SYS'):
            h = int(hashlib.md5(real_id.encode()).hexdigest(), 16) % sq_state['bitmap_size']
            sq_state['bitmap'][h] = True
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    sensor_id = data.get('sensor_id', '')
    h = int(hashlib.md5(sensor_id.encode()).hexdigest(), 16) % sq_state['bitmap_size']
    data['bloom_valid'] = sq_state['bitmap'][h]
    return [data, arrival_time, time.time()]

@SQify
def kalman_filter(bloom_data):
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
        sq_state['process_variance'] = 0.125
        sq_state['measurement_variance'] = 0.32
        sq_state['estimates'] = {}
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    sensor_id = data.get('sensor_id', 'unknown')
    values = data.get('values', [])
    if sensor_id not in sq_state['estimates']:
        sq_state['estimates'][sensor_id] = {'x': 0.0, 'p': 1.0}
    state = sq_state['estimates'][sensor_id]
    q = sq_state['process_variance']
    r = sq_state['measurement_variance']
    smoothed_values = []
    kalman_gain = 0
    for measurement in values:
        x_pred = state['x']
        p_pred = state['p'] + q
        kalman_gain = p_pred / (p_pred + r)
        state['x'] = x_pred + kalman_gain * (measurement - x_pred)
        state['p'] = (1 - kalman_gain) * p_pred
        smoothed_values.append(state['x'])
    result = {
        'sensor_id': sensor_id,
        'obs_type': data.get('obs_type'),
        'timestamp': data.get('timestamp', 0),
        'smoothed_values': smoothed_values,
        'kalman_gain': kalman_gain,
        'original_values': values,
    }
    return [result, arrival_time, time.time()]

@SQify
def simple_linear_regression(kalman_data):
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
        sq_state['windows'] = {}
        sq_state['window_size'] = 20
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    sensor_id = data.get('sensor_id', 'unknown')
    smoothed_values = data.get('smoothed_values', [])
    if sensor_id not in sq_state['windows']:
        sq_state['windows'][sensor_id] = []
    window = sq_state['windows'][sensor_id]
    window.extend(smoothed_values)
    if len(window) > sq_state['window_size']:
        window = window[-sq_state['window_size']:]
        sq_state['windows'][sensor_id] = window
    n = len(window)
    slope = 0
    intercept = 0
    predicted = 0
    if n >= 2:
        x_vals = list(range(n))
        y_vals = window
        x_mean = sum(x_vals) / n
        y_mean = sum(y_vals) / n
        numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_vals, y_vals))
        denominator = sum((x - x_mean) ** 2 for x in x_vals)
        slope = numerator / denominator if denominator != 0 else 0
        intercept = y_mean - slope * x_mean
        predicted = slope * n + intercept
    elif n == 1:
        predicted = window[0]
        intercept = window[0]
    result = {
        'sensor_id': sensor_id,
        'obs_type': data.get('obs_type'),
        'timestamp': data.get('timestamp', 0),
        'slope': slope,
        'intercept': intercept,
        'predicted_next': predicted,
        'window_size': n,
        'stat_type': 'linear_regression',
    }
    return [result, arrival_time, time.time()]

@SQify
def second_order_moment(bloom_data):
    import sys, os, time, math
    global sq_state
    data = bloom_data[0]
    arrival_time = bloom_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('second_order_moment')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['windows'] = {}
        sq_state['window_size'] = 50
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    sensor_id = data.get('sensor_id', 'unknown')
    values = data.get('values', [])
    if sensor_id not in sq_state['windows']:
        sq_state['windows'][sensor_id] = []
    window = sq_state['windows'][sensor_id]
    window.extend(values)
    if len(window) > sq_state['window_size']:
        window = window[-sq_state['window_size']:]
        sq_state['windows'][sensor_id] = window
    n = len(window)
    if n > 0:
        mean = sum(window) / n
        variance = sum((x - mean) ** 2 for x in window) / n if n > 1 else 0
        std_dev = math.sqrt(variance)
    else:
        mean = 0
        variance = 0
        std_dev = 0
    result = {
        'sensor_id': sensor_id,
        'obs_type': data.get('obs_type'),
        'timestamp': data.get('timestamp', 0),
        'mean': mean,
        'variance': variance,
        'std_dev': std_dev,
        'sample_count': n,
        'stat_type': 'second_order_moment',
    }
    return [result, arrival_time, time.time()]

@SQify
def distinct_approx_count(bloom_data):
    import sys, os, time, hashlib
    global sq_state
    data = bloom_data[0]
    arrival_time = bloom_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('distinct_approx_count')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['registers'] = {}
        sq_state['num_registers'] = 64
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    obs_type = data.get('obs_type', 'unknown')
    sensor_id = data.get('sensor_id', 'unknown')
    values = data.get('values', [])
    if obs_type not in sq_state['registers']:
        sq_state['registers'][obs_type] = [0] * sq_state['num_registers']
    registers = sq_state['registers'][obs_type]
    for value in values:
        hash_input = sensor_id + '_' + str(value)
        h = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
        register_idx = h % sq_state['num_registers']
        leading_zeros = 1
        temp = h >> 6
        while temp > 0 and (temp & 1) == 0:
            leading_zeros += 1
            temp >>= 1
        registers[register_idx] = max(registers[register_idx], leading_zeros)
    alpha = 0.7213 / (1 + 1.079 / sq_state['num_registers'])
    harmonic_mean = sq_state['num_registers'] / sum(2 ** (-r) for r in registers)
    estimate = alpha * sq_state['num_registers'] * harmonic_mean
    result = {
        'obs_type': obs_type,
        'timestamp': data.get('timestamp', 0),
        'distinct_count_estimate': int(estimate),
        'stat_type': 'distinct_approx_count',
    }
    return [result, arrival_time, time.time()]

@SQify
def mqtt_publish_stats(lr_result, som_result, dac_result):
    import sys, os, time
    global sq_state
    lr_data = lr_result[0]
    som_data = som_result[0]
    dac_data = dac_result[0]
    arrival_time = lr_result[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('mqtt_publish')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['publish_count'] = 0
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    sq_state['publish_count'] += 1
    combined = {
        'mqtt_msg_id': sq_state['publish_count'],
        'sensor_id': lr_data.get('sensor_id', som_data.get('sensor_id', 'unknown')),
        'timestamp': lr_data.get('timestamp', 0),
        'statistics': {
            'linear_regression': {
                'slope': lr_data.get('slope', 0),
                'predicted_next': lr_data.get('predicted_next', 0),
            },
            'second_order_moment': {
                'mean': som_data.get('mean', 0),
                'variance': som_data.get('variance', 0),
                'std_dev': som_data.get('std_dev', 0),
            },
            'distinct_count': {
                'estimate': dac_data.get('distinct_count_estimate', 0),
            },
        },
    }
    return [combined, arrival_time, time.time()]

@SQify
def stats_sink(mqtt_data):
    import os, time, json
    data = mqtt_data[0]
    arrival_time = mqtt_data[1]
    completion_time = time.time()
    latency_ms = (completion_time - arrival_time) * 1000
    run_label = os.environ.get('TTPYTHON_RUN_LABEL', '')
    predicted_ms = float(os.environ.get('TTPYTHON_PREDICTED_MS', '0'))
    log_entry = {
        'timestamp': completion_time,
        'arrival_time': arrival_time,
        'latency_ms': latency_ms,
        'workload': 'eval_stats',
        'run_label': run_label,
        'predicted_ms': predicted_ms,
    }
    log_dir = os.environ.get('TTPYTHON_LOG_DIR', '.')
    log_file = os.path.join(log_dir, 'runtime_log_' + (run_label or 'eval_stats') + '.jsonl')
    with open(log_file, 'a') as f:
        f.write(json.dumps(log_entry) + '\n')
    print('STATS done, latency=' + str(latency_ms) + 'ms')
    return 1


@GRAPHify
def eval_stats(trigger):
    A_1 = 1
    with TTClock.root() as root_clock:
        start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
        N = 100
        stop_time = start_time + (500000 * N)
        sampling_time = VALUES_TO_TTTIME(start_time, stop_time)
        sample_window = COPY_TTTIME(A_1, sampling_time)

        # Sensor data ingestion: needs access to sensor hardware
        with TTConstraint(components=["sensor_interface"]):
            workload = 'SYS'
            raw_stream = stats_spout(sample_window, workload, TTClock=root_clock, TTPeriod=500000, TTPhase=0, TTDataIntervalWidth=250000)
            parsed = parse_project(raw_stream)
            bloom_checked = bloom_filter_check(parsed)

        # Statistical branch 1 (sequential): Kalman → Linear Regression
        with TTConstraint(components=["compute"]):
            kalman_output = kalman_filter(bloom_checked)
            lr_output = simple_linear_regression(kalman_output)

        # Statistical branch 2 (parallel): Second-order moment
        with TTConstraint(components=["compute"]):
            som_output = second_order_moment(bloom_checked)

        # Statistical branch 3 (parallel): Distinct approximate count
        with TTConstraint(components=["compute"]):
            dac_output = distinct_approx_count(bloom_checked)

        # Aggregation and publishing: needs MQTT broker access
        with TTConstraint(components=["mqtt_broker"]):
            published = mqtt_publish_stats(lr_output, som_output, dac_output)
            result = stats_sink(published)
