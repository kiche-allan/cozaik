from ticktalkpython.SQ import STREAMify, SQify, GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *

@STREAMify
def train_timer_spout(trigger, batch_size):
    import sys, os, time
    global sq_state
    if sq_state.get('batch_count', None) is None:
        sq_state['batch_count'] = 0
    sq_state['batch_count'] += 1
    trigger_msg = {
        'batch_id': sq_state['batch_count'],
        'batch_size': batch_size,
        'timestamp': time.time(),
    }
    return [trigger_msg, time.time()]

@SQify
def azure_table_range_query(trigger_msg):
    import sys, os, time, random
    global sq_state
    trigger = trigger_msg[0]
    arrival_time = trigger_msg[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('azure_table_range')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'server_x86')
        sq_state['total_rows_scanned'] = 0
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    batch_size = trigger.get('batch_size', 1000)
    batch_id = trigger.get('batch_id', 0)
    if sq_state.get('model_provider') is None:
        from riotbench_provider import get_model_provider
        sq_state['model_provider'] = get_model_provider('TAXI')
    batch_samples = sq_state['model_provider'].get_training_batch(batch_size)
    training_data = []
    for i, sample in enumerate(batch_samples):
        training_data.append({
            'features': sample['features'],
            'regression_target': sample['regression_target'],
            'row_key': 'row_' + str(batch_id) + '_' + str(i),
        })
    sq_state['total_rows_scanned'] += batch_size
    batch_data = {
        'batch_id': batch_id,
        'batch_size': batch_size,
        'training_samples': training_data,
        'timestamp': trigger.get('timestamp', 0),
    }
    return [batch_data, arrival_time, time.time()]

@SQify
def linear_regression_train(batch_data):
    import sys, os, time
    global sq_state
    data = batch_data[0]
    arrival_time = batch_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('linear_regression_train')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'server_x86')
        sq_state['training_iterations'] = 0
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    samples = data.get('training_samples', [])
    batch_id = data.get('batch_id', 0)
    n_features = 4
    coefficients = [0.0] * n_features
    intercept = 0.0
    r_squared = 0.0
    if samples:
        n = len(samples)
        feature_means = [0.0] * n_features
        target_mean = 0.0
        for sample in samples:
            features = sample.get('features', [0] * n_features)
            target = sample.get('regression_target', 0)
            for i in range(min(len(features), n_features)):
                feature_means[i] += features[i]
            target_mean += target
        feature_means = [m / n for m in feature_means]
        target_mean /= n
        for i in range(n_features):
            numerator = 0
            denominator = 0
            for sample in samples:
                features = sample.get('features', [0] * n_features)
                target = sample.get('regression_target', 0)
                if i < len(features):
                    fi = features[i] - feature_means[i]
                    numerator += fi * (target - target_mean)
                    denominator += fi * fi
            coefficients[i] = numerator / denominator if denominator > 0 else 0
        intercept = target_mean - sum(c * m for c, m in zip(coefficients, feature_means))
        ss_res = 0
        ss_tot = 0
        for sample in samples:
            features = sample.get('features', [0] * n_features)
            target = sample.get('regression_target', 0)
            predicted = intercept + sum(c * f for c, f in zip(coefficients, features[:n_features]))
            ss_res += (target - predicted) ** 2
            ss_tot += (target - target_mean) ** 2
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
    sq_state['training_iterations'] += 1
    model = {
        'type': 'linear_regression',
        'batch_id': batch_id,
        'version': sq_state['training_iterations'],
        'coefficients': coefficients,
        'intercept': intercept,
        'r_squared': r_squared,
        'samples_used': len(samples),
    }
    return [model, arrival_time, time.time()]

@SQify
def annotate_dt_class(batch_data):
    import sys, os, time
    global sq_state
    data = batch_data[0]
    arrival_time = batch_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('annotate_dt_class')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'server_x86')
        sq_state['thresholds'] = [10.0, 18.5]
        sq_state['classes'] = ['Bad', 'Good', 'VeryGood']
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    samples = data.get('training_samples', [])
    thresholds = sq_state['thresholds']
    classes = sq_state['classes']
    annotated_samples = []
    for sample in samples:
        features = sample.get('features', [])
        classify_val = features[2] if len(features) > 2 else (features[0] if features else 0)
        if classify_val < thresholds[0]:
            class_label = classes[0]
        elif classify_val < thresholds[1]:
            class_label = classes[1]
        else:
            class_label = classes[2]
        annotated_sample = sample.copy()
        annotated_sample['classification_label'] = class_label
        annotated_samples.append(annotated_sample)
    annotated_data = {
        'batch_id': data.get('batch_id', 0),
        'batch_size': data.get('batch_size', 0),
        'training_samples': annotated_samples,
        'timestamp': data.get('timestamp', 0),
        'annotation_thresholds': thresholds,
    }
    return [annotated_data, arrival_time, time.time()]

@SQify
def decision_tree_train(annotated_data):
    import sys, os, time
    global sq_state
    data = annotated_data[0]
    arrival_time = annotated_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('decision_tree_train')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'server_x86')
        sq_state['training_iterations'] = 0
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    samples = data.get('training_samples', [])
    batch_id = data.get('batch_id', 0)
    class_samples = {'good': [], 'average': [], 'poor': []}
    for sample in samples:
        label = sample.get('classification_label', 'average')
        features = sample.get('features', [])
        mean_feature = sum(features) / len(features) if features else 0
        if label in class_samples:
            class_samples[label].append(mean_feature)
    thresholds = []
    for cls in ['good', 'average']:
        cls_vals = class_samples.get(cls, [])
        if cls_vals:
            threshold = max(cls_vals)
            thresholds.append(threshold)
    while len(thresholds) < 2:
        thresholds.append(33 + len(thresholds) * 33)
    sq_state['training_iterations'] += 1
    correct = 0
    for sample in samples:
        features = sample.get('features', [])
        mean_val = sum(features) / len(features) if features else 0
        predicted = 'good' if mean_val < thresholds[0] else ('average' if mean_val < thresholds[1] else 'poor')
        if predicted == sample.get('classification_label'):
            correct += 1
    accuracy = correct / len(samples) if samples else 0
    model = {
        'type': 'decision_tree',
        'batch_id': batch_id,
        'version': sq_state['training_iterations'],
        'thresholds': thresholds,
        'classes': ['good', 'average', 'poor'],
        'training_accuracy': accuracy,
        'samples_used': len(samples),
    }
    return [model, arrival_time, time.time()]

@SQify
def azure_blob_upload(lr_model, dt_model):
    import sys, os, time
    global sq_state
    lr_data = lr_model[0]
    dt_data = dt_model[0]
    arrival_time = lr_model[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('azure_blob_upload')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'server_x86')
        sq_state['upload_count'] = 0
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    sq_state['upload_count'] += 1
    batch_id = dt_data.get('batch_id', lr_data.get('batch_id', 0))
    blob_urls = {
        'decision_tree': 'https://storage.example.com/models/dt_batch' + str(batch_id) + '_v' + str(sq_state['upload_count']) + '.pkl',
        'linear_regression': 'https://storage.example.com/models/lr_batch' + str(batch_id) + '_v' + str(sq_state['upload_count']) + '.pkl',
    }
    result = {
        'batch_id': batch_id,
        'models': {
            'decision_tree': dt_data,
            'linear_regression': lr_data,
        },
        'blob_urls': blob_urls,
        'upload_timestamp': time.time(),
        'total_samples': dt_data.get('samples_used', 0),
    }
    return [result, arrival_time, time.time()]

@SQify
def mqtt_publish_train(upload_data):
    import sys, os, time
    global sq_state
    data = upload_data[0]
    arrival_time = upload_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('mqtt_publish')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'server_x86')
        sq_state['publish_count'] = 0
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    sq_state['publish_count'] += 1
    notification = {
        'event': 'model_update',
        'batch_id': data.get('batch_id', 0),
        'blob_urls': data.get('blob_urls', {}),
        'metrics': {
            'dt_accuracy': data['models']['decision_tree'].get('training_accuracy', 0),
            'lr_r_squared': data['models']['linear_regression'].get('r_squared', 0),
        },
        'timestamp': time.time(),
        'msg_id': sq_state['publish_count'],
    }
    data['notification'] = notification
    data['mqtt_msg_id'] = sq_state['publish_count']
    return [data, arrival_time, time.time()]

@SQify
def train_sink(published_data):
    import os, time, json
    data = published_data[0]
    arrival_time = published_data[1]
    completion_time = time.time()
    latency_ms = (completion_time - arrival_time) * 1000
    run_label = os.environ.get('TTPYTHON_RUN_LABEL', '')
    predicted_ms = float(os.environ.get('TTPYTHON_PREDICTED_MS', '0'))
    log_entry = {
        'timestamp': completion_time,
        'arrival_time': arrival_time,
        'latency_ms': latency_ms,
        'workload': 'eval_train',
        'run_label': run_label,
        'predicted_ms': predicted_ms,
    }
    log_dir = os.environ.get('TTPYTHON_LOG_DIR', '.')
    log_file = os.path.join(log_dir, 'runtime_log_' + (run_label or 'eval_train') + '.jsonl')
    with open(log_file, 'a') as f:
        f.write(json.dumps(log_entry) + '\n')
    print('TRAIN done, latency=' + str(latency_ms) + 'ms')
    return 1

@GRAPHify
def eval_train(trigger):
    A_1 = 1
    with TTClock.root() as root_clock:
        start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
        N = 10
        stop_time = start_time + (30000000 * N)
        sampling_time = VALUES_TO_TTTIME(start_time, stop_time)
        sample_window = COPY_TTTIME(A_1, sampling_time)

        # Data ingestion: needs access to table storage
        with TTConstraint(components=["storage"]):
            batch_size = 1000
            train_trigger = train_timer_spout(sample_window, batch_size,
                                              TTClock=root_clock,
                                              TTPeriod=30000000,
                                              TTPhase=0,
                                              TTDataIntervalWidth=15000000)
            batch_data = azure_table_range_query(train_trigger)

        # Linear regression branch: needs compute
        with TTConstraint(components=["compute"]):
            lr_model = linear_regression_train(batch_data)

        # Decision tree branch: needs compute
        with TTConstraint(components=["compute"]):
            annotated_data = annotate_dt_class(batch_data)
            dt_model = decision_tree_train(annotated_data)

        # Upload + publish: needs blob storage and MQTT broker
        with TTConstraint(components=["storage", "mqtt_broker"]):
            uploaded = azure_blob_upload(lr_model, dt_model)
            published = mqtt_publish_train(uploaded)
            result = train_sink(published)
