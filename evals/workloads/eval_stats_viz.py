# eval_stats_viz.py
# TTPython equivalent of StatsWithVisualizationTopology.java (RIoTBench)
#
# Java topology:
#   SampleSenMLSpout → SenMLParseBolt → BlockWindowAverageBolt ──────────────────→ MultiLinePlotBolt → AzureBlobUploadTaskBolt → Sink
#                                      → KalmanFilterBolt → SimpleLinRegPredBolt →↑
#                                      → DistinctApproxCountBolt ────────────────→↑
#
# Key differences from eval_stats (IoTStatsTopology):
#   1. SenMLParseBolt (not ParseProjectSYSBolt) — parses SenML JSON directly
#   2. NO BloomFilterCheckBolt — parse feeds the 3 branches directly
#   3. BlockWindowAverageBolt replaces SecondOrderMomentBolt
#   4. MultiLinePlotBolt (Visualization) replaces MQTTPublishTaskBolt as the 3-input join
#   5. AzureBlobUploadTaskBolt added between Visualization and Sink

from ticktalkpython.SQ import STREAMify, SQify, GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *


 
# SPOUT — SampleSenMLSpout equivalent
 

@STREAMify
def stats_viz_spout(trigger, workload_type):
    # Java equivalent: SampleSenMLSpout. Emits SenML-format sensor data messages.

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


 
# BOLT 1 — SenMLParseBolt equivalent
 

@SQify
def senml_parse(raw_data):
    # Java equivalent: SenMLParseBolt — parses SenML JSON payload
    # Unlike ParseProjectSYSBolt (eval_stats), handles standard SenML schema
    
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

    # SenML parsing: extract base name, base time, and sensor entries
    base_name = message.get('bn', 'unknown')
    base_time = message.get('bt', 0)
    entries = message.get('e', [])

    # Extract sensor ID from base name (format: urn:dev:type:id)
    parts = base_name.split(':')
    sensor_id = parts[-1] if len(parts) > 1 else base_name

    # Extract observation type and values from entries
    obs_type = entries[0].get('n', 'unknown') if entries else 'unknown'
    values = [e.get('v', 0) for e in entries if e.get('v') is not None]

    parsed = {
        'sensor_id': sensor_id,
        'obs_type': obs_type,
        'timestamp': base_time,
        'values': values,
        'raw_entries': entries,
    }
    return [parsed, arrival_time, time.time()]


 
# BOLT 2a — BlockWindowAverageBolt equivalent
# Receives from: SenMLParseBolt (fieldsGrouping on SENSORID, OBSTYPE)
 

@SQify
def block_window_average(parsed_data):
    # Java equivalent: BlockWindowAverageBolt — block-windowed average
    # Replaces SecondOrderMomentBolt from IoTStatsTopology

    import sys, os, time
    global sq_state
    data = parsed_data[0]
    arrival_time = parsed_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('average')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['windows'] = {}
        sq_state['window_size'] = 50  # Block window size
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)

    sensor_id = data.get('sensor_id', 'unknown')
    values = data.get('values', [])

    # Maintain per-sensor sliding window
    if sensor_id not in sq_state['windows']:
        sq_state['windows'][sensor_id] = []
    window = sq_state['windows'][sensor_id]
    window.extend(values)
    if len(window) > sq_state['window_size']:
        window = window[-sq_state['window_size']:]
        sq_state['windows'][sensor_id] = window

    # Compute block window average
    n = len(window)
    if n > 0:
        mean = sum(window) / n
        min_val = min(window)
        max_val = max(window)
    else:
        mean = 0.0
        min_val = 0.0
        max_val = 0.0

    result = {
        'sensor_id': sensor_id,
        'obs_type': data.get('obs_type'),
        'timestamp': data.get('timestamp', 0),
        'average': mean,
        'min': min_val,
        'max': max_val,
        'window_count': n,
        'stat_type': 'block_window_average',
    }
    return [result, arrival_time, time.time()]


 
# BOLT 2b — KalmanFilterBolt equivalent
# Receives from: SenMLParseBolt (fieldsGrouping on SENSORID, OBSTYPE)
 

@SQify
def kalman_filter_viz(parsed_data):
    # Java equivalent: KalmanFilterBolt — 1D Kalman smoothing
    # Same as eval_stats kalman_filter but no bloom filter before it

    import sys, os, time
    global sq_state
    data = parsed_data[0]
    arrival_time = parsed_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['process_variance'] = 1e-5
        sq_state['measurement_variance'] = 0.1
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


 
# BOLT 3 — SimpleLinearRegressionPredictorBolt equivalent
# Receives from: KalmanFilterBolt (fieldsGrouping on SENSORID, OBSTYPE)
 

@SQify
def linear_regression_predict(kalman_data):
    # Java equivalent: SimpleLinearRegressionPredictorBolt
    # Sliding window linear regression on Kalman-smoothed values

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


 
# BOLT 2c — DistinctApproxCountBolt equivalent
# Receives from: SenMLParseBolt (fieldsGrouping on OBSTYPE)
 

@SQify
def distinct_approx_count_viz(parsed_data):
    # Java equivalent: DistinctApproxCountBolt — HyperLogLog distinct count
    # Same as eval_stats but receives from SenML parse directly (no bloom)

    import sys, os, time, hashlib
    global sq_state
    data = parsed_data[0]
    arrival_time = parsed_data[1]
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


 
# BOLT 4 — MultiLinePlotBolt (Visualization) equivalent
# Receives from: BlockWindowAverageBolt, SimpleLinRegPredBolt,
#                DistinctApproxCountBolt (fieldsGrouping from all three)
# THIS IS THE 3-INPUT JOIN NODE — structurally distinct from eval_stats
 

@SQify
def visualize(bwa_result, lr_result, dac_result):
    # Java equivalent: MultiLinePlotBolt (Visualization)
    # 3-input join — replaces MQTTPublishTaskBolt from eval_stats
    # Prepares multi-line plot data with display range computation

    import sys, os, time
    global sq_state
    bwa_data = bwa_result[0]
    lr_data = lr_result[0]
    dac_data = dac_result[0]
    arrival_time = bwa_result[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('plot')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['plot_count'] = 0
        sq_state['plot_history'] = []
        sq_state['max_history'] = 100
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)

    sq_state['plot_count'] += 1

    # Build multi-line plot data point from all 3 branches
    plot_point = {
        'timestamp': bwa_data.get('timestamp', lr_data.get('timestamp', 0)),
        'sensor_id': bwa_data.get('sensor_id', lr_data.get('sensor_id', 'unknown')),
        'lines': {
            'average': bwa_data.get('average', 0),
            'avg_min': bwa_data.get('min', 0),
            'avg_max': bwa_data.get('max', 0),
            'predicted': lr_data.get('predicted_next', 0),
            'slope': lr_data.get('slope', 0),
            'distinct_count': dac_data.get('distinct_count_estimate', 0),
        },
    }

    # Maintain plot history for rendering context
    sq_state['plot_history'].append(plot_point)
    if len(sq_state['plot_history']) > sq_state['max_history']:
        sq_state['plot_history'] = sq_state['plot_history'][-sq_state['max_history']:]

    # Compute display ranges across history for axis scaling
    all_avgs = [p['lines']['average'] for p in sq_state['plot_history']]
    all_preds = [p['lines']['predicted'] for p in sq_state['plot_history']]

    viz_output = {
        'plot_id': sq_state['plot_count'],
        'current_point': plot_point,
        'history_length': len(sq_state['plot_history']),
        'display_range': {
            'avg_min': min(all_avgs) if all_avgs else 0,
            'avg_max': max(all_avgs) if all_avgs else 0,
            'pred_min': min(all_preds) if all_preds else 0,
            'pred_max': max(all_preds) if all_preds else 0,
        },
    }
    return [viz_output, arrival_time, time.time()]


 
# BOLT 5 — AzureBlobUploadTaskBolt equivalent
# Receives from: MultiLinePlotBolt (shuffleGrouping)
# THIS BOLT DOES NOT EXIST IN eval_stats — it's an additional I/O stage
 

@SQify
def blob_upload(viz_data):
    # Java equivalent: AzureBlobUploadTaskBolt
    # Serializes viz data and simulates blob upload overhead

    import sys, os, time, json, hashlib
    global sq_state
    data = viz_data[0]
    arrival_time = viz_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('azure_blob_upload')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['upload_count'] = 0
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)

    sq_state['upload_count'] += 1

    # Simulate serialization for upload
    payload = json.dumps(data).encode('utf-8')
    checksum = hashlib.sha256(payload).hexdigest()

    upload_result = {
        'upload_id': sq_state['upload_count'],
        'plot_id': data.get('plot_id', 0),
        'blob_size_bytes': len(payload),
        'checksum': checksum,
        'timestamp': data.get('current_point', {}).get('timestamp', 0),
    }
    return [upload_result, arrival_time, time.time()]


 
# SINK — Sink equivalent
 

@SQify
def stats_viz_sink(upload_data):
    # Java equivalent: Sink — logs completion and measures latency

    import os, time
    data = upload_data[0]
    arrival_time = upload_data[1]
    completion_time = time.time()
    outfile = os.environ.get('TTPYTHON_OUTPUT_FILE', '/tmp/eval_stats_viz_output.txt')
    latency_ms = (completion_time - arrival_time) * 1000
    with open(outfile, 'a') as f:
        f.write(str(data.get('plot_id', 'N/A')) + '\n')
    print('STATS_VIZ done, latency=' + str(latency_ms) + 'ms')
    return 1


 
# GRAPH WIRING — Exact replication of StatsWithVisualizationTopology.java
 

@GRAPHify
def eval_stats_viz(trigger):
    # TTPython graph equivalent of StatsWithVisualizationTopology.java
    # No bloom filter, block_window_average replaces second_order_moment,
    # visualize (3-input join) replaces mqtt_publish, blob_upload added

    A_1 = 1
    with TTClock.root() as root_clock:
        start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
        N = 100
        stop_time = start_time + (500000 * N)
        sampling_time = VALUES_TO_TTTIME(start_time, stop_time)
        sample_window = COPY_TTTIME(A_1, sampling_time)

        # Sensor data ingestion: SenML spout + parse
        with TTConstraint(components=["sensor_interface"]):
            workload = 'CITY'
            raw_stream = stats_viz_spout(sample_window, workload, TTClock=root_clock, TTPeriod=500000, TTPhase=0, TTDataIntervalWidth=250000)
            parsed = senml_parse(raw_stream)

        # Branch 1 (parallel): Block window average
        # Java: BlockWindowAverageBolt ← fieldsGrouping(ParseSenML, SENSORID, OBSTYPE)
        with TTConstraint(components=["compute"]):
            bwa_output = block_window_average(parsed)

        # Branch 2 (sequential): Kalman filter → Linear regression predictor
        # Java: KalmanFilterBolt ← fieldsGrouping(ParseSenML, SENSORID, OBSTYPE)
        #        → SimpleLinearRegressionPredictorBolt ← fieldsGrouping(KalmanFilterBolt, SENSORID, OBSTYPE)
        with TTConstraint(components=["compute"]):
            kalman_output = kalman_filter_viz(parsed)
            lr_output = linear_regression_predict(kalman_output)

        # Branch 3 (parallel): Distinct approximate count
        # Java: DistinctApproxCountBolt ← fieldsGrouping(ParseSenML, OBSTYPE)
        with TTConstraint(components=["compute"]):
            dac_output = distinct_approx_count_viz(parsed)

        # Visualization aggregation (3-input join) + blob upload + sink
        # Java: MultiLinePlotBolt ← fieldsGrouping(BWA, LinReg, DistinctApprox)
        #        → AzureBlobUploadTaskBolt ← shuffleGrouping(Visualization)
        #        → Sink ← shuffleGrouping(AzureBlobUpload)
        with TTConstraint(components=["storage"]):
            viz_output = visualize(bwa_output, lr_output, dac_output)
            uploaded = blob_upload(viz_output)
            result = stats_viz_sink(uploaded)