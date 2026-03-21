# eval_etl_mini.py — Minimal ETL Preprocessing Pipeline (6 app SQs)
#
# Downsized version of eval_etl representing a lightweight data ingestion
# pipeline. Based on the ETL topology from RIoTBench (Shukla et al., 2017).
#
# Topology: Pure sequential chain
#   senml_spout → senml_parse → bloom_filter → interpolation → mqtt_publish → etl_mini_sink
#
# App SQs: 6 (the smallest workload in our evaluation suite)
# Shape: Linear chain — no parallelism, tests QPF on minimal problem size
#
from ticktalkpython.SQ import STREAMify, SQify, GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *

@STREAMify
def mini_spout(trigger, workload_type):
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
def mini_parse(raw_data):
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
    parsed = {
        'msgid': message.get('msgid', 0),
        'sensor_id': message.get('bn', 'unknown'),
        'base_time': message.get('bt', 0),
        'readings': message.get('e', []),
    }
    return [parsed, arrival_time, time.time()]

@SQify
def mini_bloom(parsed_data):
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
        for i in range(1000):
            h = int(hashlib.md5(('sensor_' + str(i)).encode()).hexdigest(), 16) % sq_state['bitmap_size']
            sq_state['bitmap'][h] = True
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    sensor_id = data.get('sensor_id', '')
    h = int(hashlib.md5(sensor_id.encode()).hexdigest(), 16) % sq_state['bitmap_size']
    data['bloom_valid'] = sq_state['bitmap'][h]
    return [data, arrival_time, time.time()]

@SQify
def mini_interpolation(bloom_data):
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
    for reading in data.get('readings', []):
        field_name = reading.get('n', 'value')
        key = sensor_id + '_' + field_name
        if reading.get('v') is None:
            reading['v'] = sq_state['last_values'].get(key, 0.0)
            reading['interpolated'] = True
        else:
            sq_state['last_values'][key] = reading['v']
    return [data, arrival_time, time.time()]

@SQify
def mini_publish(interp_data):
    import sys, os, time
    global sq_state
    data = interp_data[0]
    arrival_time = interp_data[1]
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
    return [data, arrival_time, time.time()]

@SQify
def mini_sink(pub_data):
    import os, time
    data = pub_data[0]
    arrival_time = pub_data[1]
    completion_time = time.time()
    latency_ms = (completion_time - arrival_time) * 1000
    outfile = os.environ.get('TTPYTHON_OUTPUT_FILE', '/tmp/eval_etl_mini_output.txt')
    with open(outfile, 'a') as f:
        f.write(str(data.get('msgid', 'N/A')) + '\n')
    print('ETL-MINI done, latency=' + str(latency_ms) + 'ms')
    return 1


@GRAPHify
def eval_etl_mini(trigger):
    A_1 = 1
    with TTClock.root() as root_clock:
        start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
        N = 100
        stop_time = start_time + (1000000 * N)
        sampling_time = VALUES_TO_TTTIME(start_time, stop_time)
        sample_window = COPY_TTTIME(A_1, sampling_time)

        with TTConstraint(components=["sensor_interface"]):
            workload = 'TAXI'
            raw = mini_spout(sample_window, workload, TTClock=root_clock,
                             TTPeriod=1000000, TTPhase=0, TTDataIntervalWidth=500000)

        with TTConstraint(components=["compute"]):
            parsed = mini_parse(raw)
            bloomed = mini_bloom(parsed)
            interped = mini_interpolation(bloomed)

        with TTConstraint(components=["mqtt_broker"]):
            published = mini_publish(interped)
            result = mini_sink(published)
