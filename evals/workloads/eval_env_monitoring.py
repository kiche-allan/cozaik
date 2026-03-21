# eval_env_monitoring.py — Multi-Sensor Environmental Monitoring (30 app SQs)
#
# Inspired by: López et al., "IoT- and AI-informed urban air quality models
# for vehicle pollution monitoring," arXiv, 2025; Kang et al., "An Indoor
# Multi-Environment Sensor System Based on Intelligent Edge Computing,"
# Electronics, 2023; Saha et al., "Real-time IoT-powered AI system for
# monitoring and forecasting of air pollution," Ecotoxicology, 2024.
#
# Environmental monitoring systems deploy multiple heterogeneous sensor
# types (temperature, humidity, PM2.5, CO, NO2, O3, noise, wind) each
# requiring independent parse → filter → anomaly detection before fusion
# into a unified Air Quality Index (AQI). This creates naturally wide
# parallelism that stresses placement algorithms.
#
# Topology: Wide parallel (8 independent branches + join + tail)
#   env_source ─┬→ sensor_parse → sensor_filter → sensor_anomaly ─┐ (×8 branches)
#               └→ ... (8 branches total)                          ├→ fusion → aqi_compute → alert_publish → dashboard_upload → env_sink
#                                                                  ┘
#
# App SQs: 1 source + (8×3 branch) + 1 fusion + 1 aqi + 1 alert + 1 upload + 1 sink = 30
# Shape: Width 8 — widest parallelism in the suite
#
# Parameterizable: For scaling experiments, use eval_env_monitoring_4 (4 branches = 18 SQs)
# and eval_env_monitoring_6 (6 branches = 24 SQs) by adjusting wiring in GRAPHify.
#
from ticktalkpython.SQ import STREAMify, SQify, GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *

#   Source  
@STREAMify
def env_source(trigger, workload_type):
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
    # Multiplex into 8 sensor readings (one per sensor type)
    import random
    env_reading = {
        'msgid': sq_state['msg_id'],
        'station_id': message.get('bn', 'station_0'),
        'timestamp': message.get('bt', 0),
        'temp_c': random.uniform(-10, 45),
        'humidity_pct': random.uniform(10, 95),
        'pm25_ugm3': random.uniform(0, 300),
        'co_ppm': random.uniform(0, 50),
        'no2_ppb': random.uniform(0, 200),
        'o3_ppb': random.uniform(0, 150),
        'noise_db': random.uniform(30, 100),
        'wind_ms': random.uniform(0, 30),
    }
    return [env_reading, time.time()]

#   Branch Stage 1: Sensor Parse  
# Each call extracts one sensor type's reading from the multiplexed message.
# Uses senml_parse provider.
@SQify
def sensor_parse(raw_data, sensor_type):
    import sys, os, time
    global sq_state
    env = raw_data[0]
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
    # Sensor type field mapping
    field_map = {
        'temp': ('temp_c', 'celsius', (-40, 60)),
        'humid': ('humidity_pct', 'percent', (0, 100)),
        'pm25': ('pm25_ugm3', 'ug/m3', (0, 500)),
        'co': ('co_ppm', 'ppm', (0, 100)),
        'no2': ('no2_ppb', 'ppb', (0, 400)),
        'o3': ('o3_ppb', 'ppb', (0, 300)),
        'noise': ('noise_db', 'dB', (20, 130)),
        'wind': ('wind_ms', 'm/s', (0, 50)),
    }
    field, unit, valid_range = field_map.get(sensor_type, ('temp_c', '?', (0, 100)))
    parsed = {
        'msgid': env.get('msgid', 0),
        'station_id': env.get('station_id', 'unknown'),
        'timestamp': env.get('timestamp', 0),
        'sensor_type': sensor_type,
        'value': env.get(field, 0),
        'unit': unit,
        'valid_range': valid_range,
    }
    return [parsed, arrival_time, time.time()]

#   Branch Stage 2: Sensor Filter  
# Range validation and deduplication per sensor stream.
# Uses bloom_filter provider.
@SQify
def sensor_filter(parsed_data, sensor_type):
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
        sq_state['bitmap_size'] = 5000
        sq_state['bitmap'] = [False] * sq_state['bitmap_size']
        sq_state['prev_value'] = None
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    value = data.get('value', 0)
    vmin, vmax = data.get('valid_range', (0, 100))
    data['in_range'] = vmin <= value <= vmax
    # Bloom filter for deduplication
    key = str(data.get('station_id', '')) + '_' + str(data.get('msgid', 0))
    h = int(hashlib.md5(key.encode()).hexdigest(), 16) % sq_state['bitmap_size']
    data['is_duplicate'] = sq_state['bitmap'][h]
    sq_state['bitmap'][h] = True
    # Simple delta encoding check
    data['delta'] = abs(value - sq_state['prev_value']) if sq_state['prev_value'] is not None else value
    sq_state['prev_value'] = value
    return [data, arrival_time, time.time()]

#   Branch Stage 3: Sensor Anomaly Detection  
# Kalman-based anomaly detection per sensor stream.
# Uses kalman_filter provider.
@SQify
def sensor_anomaly(filtered_data, sensor_type):
    import sys, os, time, math
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
        sq_state['est'] = 0.0
        sq_state['err_est'] = 1.0
        sq_state['process_var'] = 1e-4
        sq_state['meas_var'] = 0.5
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    measurement = data.get('value', 0)
    # Kalman update
    gain = sq_state['err_est'] / (sq_state['err_est'] + sq_state['meas_var'])
    predicted = sq_state['est']
    sq_state['est'] = sq_state['est'] + gain * (measurement - sq_state['est'])
    sq_state['err_est'] = (1 - gain) * sq_state['err_est'] + sq_state['process_var']
    # Anomaly = large deviation from Kalman prediction
    residual = abs(measurement - predicted)
    threshold = 3 * math.sqrt(sq_state['err_est'] + sq_state['meas_var'])
    data['kalman_estimate'] = sq_state['est']
    data['residual'] = residual
    data['anomaly_threshold'] = threshold
    data['is_anomaly'] = residual > threshold
    return [data, arrival_time, time.time()]

#   Fusion: 8-input Join  
# Merges all 8 sensor anomaly outputs into a single environment snapshot.
# Uses join provider.
@SQify
def env_fusion(temp_data, humid_data, pm25_data, co_data,
               no2_data, o3_data, noise_data, wind_data):
    import sys, os, time
    global sq_state
    arrival_time = temp_data[1]  # Use first branch's arrival time
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('join')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['fusion_count'] = 0
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    sq_state['fusion_count'] += 1
    all_branches = [temp_data[0], humid_data[0], pm25_data[0], co_data[0],
                    no2_data[0], o3_data[0], noise_data[0], wind_data[0]]
    snapshot = {
        'msgid': temp_data[0].get('msgid', 0),
        'station_id': temp_data[0].get('station_id', 'unknown'),
        'fusion_sequence': sq_state['fusion_count'],
        'sensors': {},
        'anomaly_count': 0,
    }
    for branch in all_branches:
        stype = branch.get('sensor_type', 'unknown')
        snapshot['sensors'][stype] = {
            'value': branch.get('value', 0),
            'kalman_estimate': branch.get('kalman_estimate', 0),
            'is_anomaly': branch.get('is_anomaly', False),
        }
        if branch.get('is_anomaly', False):
            snapshot['anomaly_count'] += 1
    return [snapshot, arrival_time, time.time()]

#   AQI Compute  
# Compute composite Air Quality Index from fused sensor data.
# Uses second_order_moment provider (statistical aggregation).
@SQify
def aqi_compute(fused_data):
    import sys, os, time, math
    global sq_state
    snapshot = fused_data[0]
    arrival_time = fused_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('second_order_moment')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['aqi_history'] = []
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    # Simplified AQI: weighted combination of pollutant readings
    sensors = snapshot.get('sensors', {})
    pm25_val = sensors.get('pm25', {}).get('value', 0)
    co_val = sensors.get('co', {}).get('value', 0)
    no2_val = sensors.get('no2', {}).get('value', 0)
    o3_val = sensors.get('o3', {}).get('value', 0)
    # EPA-style breakpoint interpolation (simplified)
    aqi_pm25 = min(pm25_val / 300 * 500, 500)
    aqi_co = min(co_val / 50 * 500, 500)
    aqi_no2 = min(no2_val / 200 * 500, 500)
    aqi_o3 = min(o3_val / 150 * 500, 500)
    composite_aqi = max(aqi_pm25, aqi_co, aqi_no2, aqi_o3)
    sq_state['aqi_history'].append(composite_aqi)
    if len(sq_state['aqi_history']) > 100:
        sq_state['aqi_history'] = sq_state['aqi_history'][-100:]
    # Compute running stats
    mean_aqi = sum(sq_state['aqi_history']) / len(sq_state['aqi_history'])
    variance = sum((x - mean_aqi) ** 2 for x in sq_state['aqi_history']) / len(sq_state['aqi_history'])
    snapshot['composite_aqi'] = composite_aqi
    snapshot['aqi_mean'] = mean_aqi
    snapshot['aqi_std'] = math.sqrt(variance) if variance > 0 else 0
    if composite_aqi <= 50:
        snapshot['aqi_category'] = 'good'
    elif composite_aqi <= 100:
        snapshot['aqi_category'] = 'moderate'
    elif composite_aqi <= 150:
        snapshot['aqi_category'] = 'unhealthy_sensitive'
    elif composite_aqi <= 200:
        snapshot['aqi_category'] = 'unhealthy'
    else:
        snapshot['aqi_category'] = 'hazardous'
    return [snapshot, arrival_time, time.time()]

#   Alert Publish  
# Generate and publish threshold-based alerts.
# Uses mqtt_publish provider.
@SQify
def alert_publish(aqi_data):
    import sys, os, time
    global sq_state
    snapshot = aqi_data[0]
    arrival_time = aqi_data[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('mqtt_publish')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['alert_count'] = 0
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    aqi = snapshot.get('composite_aqi', 0)
    n_anomalies = snapshot.get('anomaly_count', 0)
    if aqi > 150 or n_anomalies >= 3:
        sq_state['alert_count'] += 1
        snapshot['alert'] = {
            'alert_id': sq_state['alert_count'],
            'severity': 'high' if aqi > 200 else 'medium',
            'aqi': aqi,
            'anomaly_count': n_anomalies,
        }
    else:
        snapshot['alert'] = None
    return [snapshot, arrival_time, time.time()]

#   Dashboard Upload  
# Serialize snapshot for cloud dashboard.
# Uses azure_blob_upload provider.
@SQify
def dashboard_upload(alert_data):
    import sys, os, time, json, hashlib
    global sq_state
    snapshot = alert_data[0]
    arrival_time = alert_data[1]
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
    payload = json.dumps({
        'station_id': snapshot.get('station_id'),
        'aqi': snapshot.get('composite_aqi'),
        'category': snapshot.get('aqi_category'),
        'anomaly_count': snapshot.get('anomaly_count'),
    })
    snapshot['upload_checksum'] = hashlib.md5(payload.encode()).hexdigest()
    snapshot['upload_sequence'] = sq_state['upload_count']
    return [snapshot, arrival_time, time.time()]

#   Sink  
@SQify
def env_sink(upload_data):
    import os, time
    snapshot = upload_data[0]
    arrival_time = upload_data[1]
    completion_time = time.time()
    latency_ms = (completion_time - arrival_time) * 1000
    outfile = os.environ.get('TTPYTHON_OUTPUT_FILE', '/tmp/eval_env_monitoring_output.txt')
    with open(outfile, 'a') as f:
        f.write('station=' + str(snapshot.get('station_id', 'N/A')) +
                ' aqi=' + str(round(snapshot.get('composite_aqi', 0), 1)) +
                ' cat=' + str(snapshot.get('aqi_category', '?')) +
                ' anomalies=' + str(snapshot.get('anomaly_count', 0)) +
                ' latency=' + str(round(latency_ms, 2)) + 'ms\n')
    print('ENV done, aqi=' + str(round(snapshot.get('composite_aqi', 0), 1)) +
          ' cat=' + str(snapshot.get('aqi_category', '?')) +
          ' latency=' + str(round(latency_ms, 2)) + 'ms')
    return 1


# ====================================================================
# Graph Wiring — 8 parallel sensor branches
# ====================================================================
@GRAPHify
def eval_env_monitoring(trigger):
    A_1 = 1
    with TTClock.root() as root_clock:
        start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
        N = 100
        stop_time = start_time + (2000000 * N)
        sampling_time = VALUES_TO_TTTIME(start_time, stop_time)
        sample_window = COPY_TTTIME(A_1, sampling_time)

        # Environmental sensor station: needs sensor interface
        with TTConstraint(components=["sensor_interface"]):
            workload = 'SYS'
            raw = env_source(sample_window, workload,
                             TTClock=root_clock, TTPeriod=2000000,
                             TTPhase=0, TTDataIntervalWidth=1000000)

        # ===== 8 parallel sensor branches =====
        # Each branch: parse → filter → anomaly (3 SQs per branch)
        # All branches are compute-intensive and can run in parallel

        with TTConstraint(components=["compute"]):
            # Branch 1: Temperature
            temp_parsed = sensor_parse(raw, 'temp')
            temp_filtered = sensor_filter(temp_parsed, 'temp')
            temp_anomaly = sensor_anomaly(temp_filtered, 'temp')

        with TTConstraint(components=["compute"]):
            # Branch 2: Humidity
            humid_parsed = sensor_parse(raw, 'humid')
            humid_filtered = sensor_filter(humid_parsed, 'humid')
            humid_anomaly = sensor_anomaly(humid_filtered, 'humid')

        with TTConstraint(components=["compute"]):
            # Branch 3: PM2.5
            pm25_parsed = sensor_parse(raw, 'pm25')
            pm25_filtered = sensor_filter(pm25_parsed, 'pm25')
            pm25_anomaly = sensor_anomaly(pm25_filtered, 'pm25')

        with TTConstraint(components=["compute"]):
            # Branch 4: Carbon Monoxide
            co_parsed = sensor_parse(raw, 'co')
            co_filtered = sensor_filter(co_parsed, 'co')
            co_anomaly = sensor_anomaly(co_filtered, 'co')

        with TTConstraint(components=["compute"]):
            # Branch 5: Nitrogen Dioxide
            no2_parsed = sensor_parse(raw, 'no2')
            no2_filtered = sensor_filter(no2_parsed, 'no2')
            no2_anomaly = sensor_anomaly(no2_filtered, 'no2')

        with TTConstraint(components=["compute"]):
            # Branch 6: Ozone
            o3_parsed = sensor_parse(raw, 'o3')
            o3_filtered = sensor_filter(o3_parsed, 'o3')
            o3_anomaly = sensor_anomaly(o3_filtered, 'o3')

        with TTConstraint(components=["compute"]):
            # Branch 7: Noise
            noise_parsed = sensor_parse(raw, 'noise')
            noise_filtered = sensor_filter(noise_parsed, 'noise')
            noise_anomaly = sensor_anomaly(noise_filtered, 'noise')

        with TTConstraint(components=["compute"]):
            # Branch 8: Wind
            wind_parsed = sensor_parse(raw, 'wind')
            wind_filtered = sensor_filter(wind_parsed, 'wind')
            wind_anomaly = sensor_anomaly(wind_filtered, 'wind')

        # ===== Fusion and post-processing =====
        with TTConstraint(components=["compute"]):
            fused = env_fusion(temp_anomaly, humid_anomaly, pm25_anomaly, co_anomaly,
                               no2_anomaly, o3_anomaly, noise_anomaly, wind_anomaly)
            aqi = aqi_compute(fused)

        with TTConstraint(components=["mqtt_broker", "storage"]):
            alerted = alert_publish(aqi)
            uploaded = dashboard_upload(alerted)
            result = env_sink(uploaded)
