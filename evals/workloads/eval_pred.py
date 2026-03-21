from ticktalkpython.SQ import STREAMify, SQify, GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *

@STREAMify
def pred_data_spout(trigger, workload_type):
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

@STREAMify
def mqtt_subscribe_spout(trigger, analytic_type):
    import sys, os, time
    global sq_state
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('mqtt_subscribe')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['update_count'] = 0
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    sq_state['update_count'] += 1
    notification = {
        'analytic_type': analytic_type,
        'version': sq_state['update_count'],
        'blob_url': 'https://storage.example.com/models/' + analytic_type + '_v' + str(sq_state['update_count']) + '.pkl',
        'timestamp': time.time(),
    }
    return [notification, time.time()]

@SQify
def blob_download(mqtt_notification):
    import sys, os, time, random
    global sq_state
    notification = mqtt_notification[0]
    arrival_time = mqtt_notification[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('azure_blob_download')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    analytic_type = notification.get('analytic_type', 'unknown')
    version = notification.get('version', 0)
    if sq_state.get('model_provider') is None:
        from riotbench_provider import get_model_provider
        sq_state['model_provider'] = get_model_provider('TAXI')
    if analytic_type == 'decision_tree':
        model_data = sq_state['model_provider'].get_dt_model()
        model_data['version'] = version
        model_data['analytic_type'] = 'DT'
    else:
        model_data = sq_state['model_provider'].get_lr_model()
        model_data['version'] = version
        model_data['analytic_type'] = 'MLR'
    return [model_data, arrival_time, time.time()]

@SQify
def senml_parse_pred(raw_data):
    import sys, os, time
    global sq_state
    message = raw_data[0]
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
    from riotbench_provider import parse_senml_message, get_dataset_config
    p = parse_senml_message(message, 'TAXI')
    dt_feature_names = get_dataset_config('TAXI')['dt_features']
    reading_map = {}
    for r in p['readings']:
        if 'v' in r:
            reading_map[r['n']] = r['v']
    features = [reading_map.get(f, 0.0) for f in dt_feature_names]
    parsed = {
        'sensor_id': p['sensor_id'],
        'timestamp': p['timestamp'],
        'features': features,
        'raw_readings': p['readings'],
    }
    return [parsed, arrival_time, time.time()]

@SQify
def decision_tree_classify(parsed_data, model_data):
    import sys, os, time
    global sq_state
    parsed = parsed_data[0]
    model = model_data[0]
    arrival_time = parsed_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('decision_tree_classify')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        from riotbench_provider import get_dataset_config
        cfg = get_dataset_config('TAXI')
        sq_state['current_model'] = {
            'thresholds': [10.0, 18.5],
            'classes': cfg['dt_classes'],
            'feature_index': 2,
        }
    if model.get('type') == 'decision_tree':
        sq_state['current_model'] = model
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    features = parsed.get('features', [])
    thresholds = sq_state['current_model'].get('thresholds', [10.0, 18.5])
    classes = sq_state['current_model'].get('classes', ['Bad', 'Good', 'VeryGood'])
    feat_idx = sq_state['current_model'].get('feature_index', 2)
    classify_val = features[feat_idx] if feat_idx < len(features) else 0
    predicted_class = classes[-1]
    confidence = 0.8
    for ti, thr in enumerate(thresholds):
        if classify_val < thr:
            predicted_class = classes[ti]
            confidence = 0.9 - (ti * 0.1)
            break
    result = {
        'sensor_id': parsed.get('sensor_id', 'unknown'),
        'timestamp': parsed.get('timestamp', 0),
        'classification': predicted_class,
        'confidence': confidence,
        'avg_feature': sum(features) / len(features) if features else 0,
        'analytic_type': 'DT',
    }
    return [result, arrival_time, time.time()]

@SQify
def linear_regression_predict(parsed_data, model_data):
    import sys, os, time
    global sq_state
    parsed = parsed_data[0]
    model = model_data[0]
    arrival_time = parsed_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('linear_regression')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['current_model'] = {
            'coefficients': [0.5, 0.3, 0.2],
            'intercept': 10.0,
        }
    if model.get('type') == 'linear_regression':
        sq_state['current_model'] = model
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    features = parsed.get('features', [])
    coefficients = sq_state['current_model'].get('coefficients', [0.5, 0.3, 0.2])
    intercept = sq_state['current_model'].get('intercept', 10.0)
    prediction = intercept
    for i, feature in enumerate(features):
        if i < len(coefficients):
            prediction += coefficients[i] * feature
    result = {
        'sensor_id': parsed.get('sensor_id', 'unknown'),
        'timestamp': parsed.get('timestamp', 0),
        'predicted_value': prediction,
        'analytic_type': 'MLR',
    }
    return [result, arrival_time, time.time()]

@SQify
def block_window_average(parsed_data):
    import sys, os, time
    global sq_state
    parsed = parsed_data[0]
    arrival_time = parsed_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('block_window_average')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['windows'] = {}
        sq_state['window_size'] = 10
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    sensor_id = parsed.get('sensor_id', 'unknown')
    features = parsed.get('features', [])
    if sensor_id not in sq_state['windows']:
        sq_state['windows'][sensor_id] = []
    window = sq_state['windows'][sensor_id]
    if features:
        window.append(sum(features) / len(features))
    if len(window) > sq_state['window_size']:
        window = window[-sq_state['window_size']:]
        sq_state['windows'][sensor_id] = window
    block_avg = sum(window) / len(window) if window else 0
    result = {
        'sensor_id': sensor_id,
        'timestamp': parsed.get('timestamp', 0),
        'block_average': block_avg,
        'window_size': len(window),
    }
    return [result, arrival_time, time.time()]

@SQify
def error_estimation(lr_result, bwa_result):
    import sys, os, time
    global sq_state
    lr_data = lr_result[0]
    bwa_data = bwa_result[0]
    arrival_time = lr_result[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('error_estimation')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['error_history'] = []
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    predicted = lr_data.get('predicted_value', 0)
    block_avg = bwa_data.get('block_average', 0)
    residual = abs(predicted - block_avg)
    sq_state['error_history'].append(residual)
    if len(sq_state['error_history']) > 100:
        sq_state['error_history'] = sq_state['error_history'][-100:]
    avg_error = sum(sq_state['error_history']) / len(sq_state['error_history'])
    result = {
        'sensor_id': lr_data.get('sensor_id', 'unknown'),
        'timestamp': lr_data.get('timestamp', 0),
        'predicted_value': predicted,
        'block_average': block_avg,
        'residual_error': residual,
        'average_error': avg_error,
        'analytic_type': 'ERR',
    }
    return [result, arrival_time, time.time()]

@SQify
def mqtt_publish_pred(error_result, dt_result):
    import sys, os, time
    global sq_state
    error_data = error_result[0]
    dt_data = dt_result[0]
    arrival_time = error_result[1]
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
    result = {
        'mqtt_msg_id': sq_state['publish_count'],
        'sensor_id': error_data.get('sensor_id', dt_data.get('sensor_id', 'unknown')),
        'timestamp': error_data.get('timestamp', 0),
        'classification': dt_data.get('classification'),
        'confidence': dt_data.get('confidence'),
        'predicted_value': error_data.get('predicted_value'),
        'block_average': error_data.get('block_average'),
        'residual_error': error_data.get('residual_error'),
    }
    return [result, arrival_time, time.time()]

@SQify
def pred_sink(mqtt_data):
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
        'workload': 'eval_pred',
        'run_label': run_label,
        'predicted_ms': predicted_ms,
    }
    log_dir = os.environ.get('TTPYTHON_LOG_DIR', '.')
    log_file = os.path.join(log_dir, 'runtime_log_' + (run_label or 'eval_pred') + '.jsonl')
    with open(log_file, 'a') as f:
        f.write(json.dumps(log_entry) + '\n')
    print('PRED done, latency=' + str(latency_ms) + 'ms')
    return 1


@GRAPHify
def eval_pred(trigger):
    A_1 = 1
    A_2 = 2
    with TTClock.root() as root_clock:
        start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
        N = 100
        stop_time = start_time + (200000 * N)
        sampling_time = VALUES_TO_TTTIME(start_time, stop_time)
        sample_window = COPY_TTTIME(A_1, sampling_time)
        model_stop_time = start_time + (10000000 * 10)
        model_sampling_time = VALUES_TO_TTTIME(start_time, model_stop_time)
        model_window = COPY_TTTIME(A_2, model_sampling_time)

        # Model retrieval from cloud: needs blob storage and MQTT broker
        with TTConstraint(components=["storage", "mqtt_broker"]):
            dt_analytic = 'decision_tree'
            dt_mqtt_notification = mqtt_subscribe_spout(model_window, dt_analytic, TTClock=root_clock, TTPeriod=10000000, TTPhase=0, TTDataIntervalWidth=5000000)
            dt_model = blob_download(dt_mqtt_notification)
            lr_analytic = 'linear_regression'
            lr_mqtt_notification = mqtt_subscribe_spout(model_window, lr_analytic, TTClock=root_clock, TTPeriod=10000000, TTPhase=2500000, TTDataIntervalWidth=5000000)
            lr_model = blob_download(lr_mqtt_notification)

        # Sensor data ingestion: needs sensor hardware
        with TTConstraint(components=["sensor_interface"]):
            workload = 'TAXI'
            data_stream = pred_data_spout(sample_window, workload, TTClock=root_clock, TTPeriod=200000, TTPhase=0, TTDataIntervalWidth=100000)
            parsed = senml_parse_pred(data_stream)

        # Prediction and analysis: needs compute
        with TTConstraint(components=["compute"]):
            dt_result = decision_tree_classify(parsed, dt_model)
            lr_result = linear_regression_predict(parsed, lr_model)
            bwa_result = block_window_average(parsed)
            error_result = error_estimation(lr_result, bwa_result)

        # Publishing: needs MQTT broker access
        with TTConstraint(components=["mqtt_broker"]):
            published = mqtt_publish_pred(error_result, dt_result)
            result = pred_sink(published)
