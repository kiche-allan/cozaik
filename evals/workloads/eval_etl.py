from ticktalkpython.SQ import STREAMify, SQify, GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *

@STREAMify
def senml_spout(trigger, workload_type):
    import sys, os, time
    global sq_state
    if sq_state.get('generator', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_senml_generator
        sq_state['generator'] = get_senml_generator(workload_type)
        sq_state['msg_id'] = 0
    sq_state['msg_id'] += 1
    message = sq_state['generator'].generate_message()
    message['msgid'] = sq_state['msg_id']
    return [message, time.time()]

@SQify
def senml_parse(raw_data):
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
    from riotbench_provider import parse_senml_message
    p = parse_senml_message(message, 'TAXI')
    parsed = {
        'msgid': message.get('msgid', 0),
        'sensor_id': p['sensor_id'],
        'base_time': p['timestamp'],
        'readings': p['readings'],
        'obs_type': p['obs_type'],
    }
    return [parsed, arrival_time, time.time()]

@SQify
def range_filter(parsed_data):
    import sys, os, time
    global sq_state
    parsed = parsed_data[0]
    arrival_time = parsed_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('range_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        from riotbench_provider import get_dataset_config
        sq_state['ranges'] = get_dataset_config('TAXI')['range_filter']
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    filtered_readings = []
    for reading in parsed.get('readings', []):
        field_name = reading.get('n', '')
        value = reading.get('v')
        if value is None:
            filtered_readings.append(reading)
            continue
        if field_name in sq_state['ranges']:
            rng = sq_state['ranges'][field_name]
            if rng[0] <= value <= rng[1]:
                filtered_readings.append(reading)
        else:
            filtered_readings.append(reading)
    filtered = parsed.copy()
    filtered['readings'] = filtered_readings
    filtered['filtered_count'] = len(parsed.get('readings', [])) - len(filtered_readings)
    return [filtered, arrival_time, time.time()]

@SQify
def bloom_filter(filtered_data):
    import sys, os, time, hashlib
    global sq_state
    data = filtered_data[0]
    arrival_time = filtered_data[1]
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
        for real_id in get_bloom_ids('TAXI'):
            h = int(hashlib.md5(real_id.encode()).hexdigest(), 16) % sq_state['bitmap_size']
            sq_state['bitmap'][h] = True
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    sensor_id = data.get('sensor_id', '')
    h = int(hashlib.md5(sensor_id.encode()).hexdigest(), 16) % sq_state['bitmap_size']
    is_valid = sq_state['bitmap'][h]
    data['bloom_valid'] = is_valid
    return [data, arrival_time, time.time()]

@SQify
def interpolation(bloom_data):
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
        sq_state['last_values'] = {}
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    sensor_id = data.get('sensor_id', 'unknown')
    readings = data.get('readings', [])
    for reading in readings:
        field_name = reading.get('n', 'value')
        if reading.get('v') is None:
            key = sensor_id + '_' + field_name
            reading['v'] = sq_state['last_values'].get(key, 0.0)
            reading['interpolated'] = True
        else:
            key = sensor_id + '_' + field_name
            sq_state['last_values'][key] = reading['v']
    data['readings'] = readings
    return [data, arrival_time, time.time()]

@SQify
def join_bolt(interp_data):
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
        sq_state['join_buffer'] = {}
        sq_state['join_count'] = 0
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    msg_id = data.get('msgid', 0)
    sq_state['join_count'] += 1
    joined = {
        'msgid': msg_id,
        'sensor_id': data.get('sensor_id'),
        'readings': data.get('readings', []),
        'join_timestamp': time.time(),
        'join_sequence': sq_state['join_count'],
    }
    return [joined, arrival_time, time.time()]

@SQify
def annotation(joined_data):
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
        from riotbench_provider import load_annotation_metadata
        sq_state['annotation_lookup'] = load_annotation_metadata('TAXI')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    sid = data.get('sensor_id', '')
    ann = sq_state['annotation_lookup'].get(sid, {})
    data['annotations'] = {
        'processed_at': time.time(),
        'taxi_company': ann.get('taxi_company', 'unknown'),
        'drivername': ann.get('drivername', 'unknown'),
        'taxi_city': ann.get('taxi_city', 'unknown'),
        'pipeline': 'etl_v1',
    }
    return [data, arrival_time, time.time()]

@SQify
def csv_to_senml(annotated_data):
    import sys, os, time, json
    global sq_state
    data = annotated_data[0]
    arrival_time = annotated_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('csv_to_senml')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    senml_output = {
        'bn': data.get('sensor_id', 'unknown'),
        'bt': data.get('base_time', 0),
        'e': data.get('readings', []),
        'meta': data.get('annotations', {}),
    }
    data['senml_output'] = senml_output
    data['senml_json'] = json.dumps(senml_output)
    return [data, arrival_time, time.time()]

@SQify
def mqtt_publish(senml_data):
    import sys, os, time
    global sq_state
    data = senml_data[0]
    arrival_time = senml_data[1]
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
    data['mqtt_msg_id'] = sq_state['publish_count']
    data['mqtt_published'] = True
    return [data, arrival_time, time.time()]

@SQify
def etl_sink(mqtt_data):
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
        'workload': 'eval_etl',
        'run_label': run_label,
        'predicted_ms': predicted_ms,
    }
    log_dir = os.environ.get('TTPYTHON_LOG_DIR', '.')
    log_file = os.path.join(log_dir, 'runtime_log_' + (run_label or 'eval_etl') + '.jsonl')
    with open(log_file, 'a') as f:
        f.write(json.dumps(log_entry) + '\n')
    print('ETL done, latency=' + str(latency_ms) + 'ms')
    return 1


@GRAPHify
def eval_etl(trigger):
    A_1 = 1
    with TTClock.root() as root_clock:
        start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
        N = 100
        stop_time = start_time + (1000000 * N)
        sampling_time = VALUES_TO_TTTIME(start_time, stop_time)
        sample_window = COPY_TTTIME(A_1, sampling_time)

        # Sensor data ingestion: needs access to sensor hardware
        with TTConstraint(components=["sensor_interface"]):
            workload = 'TAXI'
            raw = senml_spout(sample_window, workload, TTClock=root_clock, TTPeriod=1000000, TTPhase=0, TTDataIntervalWidth=500000)
            parsed = senml_parse(raw)

        # Data filtering and preprocessing: needs compute
        with TTConstraint(components=["compute"]):
            filtered = range_filter(parsed)
            bloomed = bloom_filter(filtered)
            interped = interpolation(bloomed)

        # Data transformation: needs compute
        with TTConstraint(components=["compute"]):
            joined = join_bolt(interped)
            annotated = annotation(joined)
            converted = csv_to_senml(annotated)

        # Publishing: needs MQTT broker access
        with TTConstraint(components=["mqtt_broker"]):
            published = mqtt_publish(converted)
            result = etl_sink(published)
