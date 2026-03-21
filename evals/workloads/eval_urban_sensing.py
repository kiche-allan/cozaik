# eval_urban_sensing.py — Urban Sensing Composite Platform (42 app SQs)
#
# Inspired by: Fu et al., "AgileDart: An Agile and Scalable Edge Stream
# Processing Engine," 2024 (evaluates 3 concurrent IoT applications on
# shared edge infrastructure); Sajjad et al. (Beaver), "High-throughput
# Real-time Edge Stream Processing," NSF, 2024 (concurrent RIoTBench
# topologies); Shukla et al., "RIoTBench," Concurrency & Computation, 2017.
#
# A city-scale edge deployment composes three sub-pipelines — traffic ETL,
# environmental statistics, and predictive analytics — feeding a unified
# dashboard. Each sub-pipeline follows a real RIoTBench topology pattern.
#
# Topology: Composite (3 STREAMify sources, mixed chain/fork-join/diamond)
#   Sub-A: Traffic ETL (12 SQs) — sequential chain
#   Sub-B: Environment Stats (13 SQs) — fork-join with 5 branches
#   Sub-C: Predictive Analytics (12 SQs) — fork-join + sequential tail
#   Dashboard Merge (5 SQs) — 3-input join + visualization tail
#
# App SQs: 12 + 13 + 12 + 5 = 42
# Shape: Largest workload in suite — maximum placement complexity

 
from ticktalkpython.SQ import STREAMify, SQify, GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *


# Helper: common provider initialization pattern
def _init_provider(sq_state, task_name):
    """Initialize riotbench provider on first call. Returns (provider, device_type)."""
    import sys, os
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider(task_name)
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    return sq_state['provider'], sq_state['device_type']

def _exec_sleep(provider, device_type):
    """Sleep for calibrated execution time."""
    import time
    exec_time_ms = provider.get_execution_time(device_type)
    time.sleep(exec_time_ms / 1000)


 
# SUB-PIPELINE A: Traffic ETL (12 app SQs)
# Based on eval_etl pattern (RIoTBench ETL topology)
#
# traffic_src → t_parse → t_range → t_bloom → t_interp → t_speed →
# t_congestion → t_route → t_join → t_annotate → t_db_insert → t_mqtt_a
 

@STREAMify
def traffic_src(trigger, workload_type):
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
    import random
    message['msgid'] = sq_state['msg_id']
    message['gps_lat'] = random.uniform(-1.0, 1.0)
    message['gps_lon'] = random.uniform(-1.0, 1.0)
    message['speed_kmh'] = random.uniform(0, 120)
    message['vehicle_id'] = 'veh_' + str(random.randint(1, 500))
    return [message, time.time()]

@SQify
def t_parse(raw_data):
    import time
    global sq_state
    msg = raw_data[0]; arrival = raw_data[1]
    p, d = _init_provider(sq_state, 'senml_parse'); _exec_sleep(p, d)
    parsed = {
        'msgid': msg.get('msgid', 0), 'vehicle_id': msg.get('vehicle_id', ''),
        'lat': msg.get('gps_lat', 0), 'lon': msg.get('gps_lon', 0),
        'speed': msg.get('speed_kmh', 0), 'timestamp': msg.get('bt', 0),
        'readings': msg.get('e', []),
    }
    return [parsed, arrival, time.time()]

@SQify
def t_range(parsed_data):
    import time
    global sq_state
    data = parsed_data[0]; arrival = parsed_data[1]
    p, d = _init_provider(sq_state, 'range_filter'); _exec_sleep(p, d)
    data['speed_valid'] = 0 <= data.get('speed', 0) <= 200
    data['gps_valid'] = -90 <= data.get('lat', 0) <= 90
    return [data, arrival, time.time()]

@SQify
def t_bloom(range_data):
    import time, hashlib
    global sq_state
    data = range_data[0]; arrival = range_data[1]
    p, d = _init_provider(sq_state, 'bloom_filter')
    if 'bitmap' not in sq_state:
        sq_state['bitmap_size'] = 10000
        sq_state['bitmap'] = [False] * sq_state['bitmap_size']
    _exec_sleep(p, d)
    key = str(data.get('vehicle_id', '')) + '_' + str(data.get('msgid', 0))
    h = int(hashlib.md5(key.encode()).hexdigest(), 16) % sq_state['bitmap_size']
    data['is_duplicate'] = sq_state['bitmap'][h]
    sq_state['bitmap'][h] = True
    return [data, arrival, time.time()]

@SQify
def t_interp(bloom_data):
    import time
    global sq_state
    data = bloom_data[0]; arrival = bloom_data[1]
    p, d = _init_provider(sq_state, 'interpolation')
    if 'last_pos' not in sq_state:
        sq_state['last_pos'] = {}
    _exec_sleep(p, d)
    vid = data.get('vehicle_id', '')
    if data.get('lat', 0) == 0 and vid in sq_state['last_pos']:
        data['lat'] = sq_state['last_pos'][vid][0]
        data['lon'] = sq_state['last_pos'][vid][1]
        data['interpolated'] = True
    else:
        sq_state['last_pos'][vid] = (data.get('lat', 0), data.get('lon', 0))
    return [data, arrival, time.time()]

@SQify
def t_speed(interp_data):
    import time
    global sq_state
    data = interp_data[0]; arrival = interp_data[1]
    p, d = _init_provider(sq_state, 'average')
    if 'speed_window' not in sq_state:
        sq_state['speed_window'] = []
    _exec_sleep(p, d)
    sq_state['speed_window'].append(data.get('speed', 0))
    if len(sq_state['speed_window']) > 50:
        sq_state['speed_window'] = sq_state['speed_window'][-50:]
    data['avg_speed'] = sum(sq_state['speed_window']) / len(sq_state['speed_window'])
    return [data, arrival, time.time()]

@SQify
def t_congestion(speed_data):
    import time
    global sq_state
    data = speed_data[0]; arrival = speed_data[1]
    p, d = _init_provider(sq_state, 'decision_tree_classify'); _exec_sleep(p, d)
    avg = data.get('avg_speed', 50)
    if avg < 15:
        data['congestion_level'] = 'severe'
    elif avg < 30:
        data['congestion_level'] = 'moderate'
    elif avg < 50:
        data['congestion_level'] = 'light'
    else:
        data['congestion_level'] = 'free_flow'
    return [data, arrival, time.time()]

@SQify
def t_route(cong_data):
    import time
    global sq_state
    data = cong_data[0]; arrival = cong_data[1]
    p, d = _init_provider(sq_state, 'decision_tree_classify'); _exec_sleep(p, d)
    lat, lon = data.get('lat', 0), data.get('lon', 0)
    if abs(lat) < 0.3 and abs(lon) < 0.3:
        data['route_zone'] = 'city_center'
    elif abs(lat) < 0.6:
        data['route_zone'] = 'suburban'
    else:
        data['route_zone'] = 'highway'
    return [data, arrival, time.time()]

@SQify
def t_join(route_data):
    import time
    global sq_state
    data = route_data[0]; arrival = route_data[1]
    p, d = _init_provider(sq_state, 'join')
    if 'join_count' not in sq_state:
        sq_state['join_count'] = 0
    _exec_sleep(p, d)
    sq_state['join_count'] += 1
    data['join_sequence'] = sq_state['join_count']
    return [data, arrival, time.time()]

@SQify
def t_annotate(joined_data):
    import time
    global sq_state
    data = joined_data[0]; arrival = joined_data[1]
    p, d = _init_provider(sq_state, 'annotate'); _exec_sleep(p, d)
    data['annotations'] = {
        'pipeline': 'traffic_etl', 'processed_at': time.time(),
        'region': 'urban_core',
    }
    return [data, arrival, time.time()]

@SQify
def t_db_insert(ann_data):
    import time
    global sq_state
    data = ann_data[0]; arrival = ann_data[1]
    p, d = _init_provider(sq_state, 'azure_table_insert'); _exec_sleep(p, d)
    data['db_inserted'] = True
    return [data, arrival, time.time()]

@SQify
def t_mqtt_a(db_data):
    import time
    global sq_state
    data = db_data[0]; arrival = db_data[1]
    p, d = _init_provider(sq_state, 'mqtt_publish'); _exec_sleep(p, d)
    data['pipeline_output'] = 'traffic_etl'
    return [data, arrival, time.time()]


 
# SUB-PIPELINE B: Environment Statistics (13 app SQs)
# Based on eval_stats pattern (RIoTBench Stats topology)
#
# env_src → e_parse → e_bloom → ┬→ e_temp_avg ──────────────┐
#                                ├→ e_humid_kalman ───────────┤
#                                ├→ e_pm25_kalman → e_pm_lr ──┤→ e_agg → e_aqi → e_alert_b → e_mqtt_b
#                                ├→ e_co_moment ──────────────┤
#                                └→ e_no2_distinct ───────────┘
 

@STREAMify
def env_src(trigger, workload_type):
    import sys, os, time, random
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
    message['temp_c'] = random.uniform(-5, 40)
    message['humidity_pct'] = random.uniform(20, 90)
    message['pm25'] = random.uniform(0, 200)
    message['co_ppm'] = random.uniform(0, 30)
    message['no2_ppb'] = random.uniform(0, 150)
    return [message, time.time()]

@SQify
def e_parse(raw_data):
    import time
    global sq_state
    msg = raw_data[0]; arrival = raw_data[1]
    p, d = _init_provider(sq_state, 'senml_parse'); _exec_sleep(p, d)
    parsed = {
        'msgid': msg.get('msgid', 0),
        'sensor_id': msg.get('bn', 'env_station_0'),
        'timestamp': msg.get('bt', 0),
        'temp': msg.get('temp_c', 0), 'humidity': msg.get('humidity_pct', 0),
        'pm25': msg.get('pm25', 0), 'co': msg.get('co_ppm', 0),
        'no2': msg.get('no2_ppb', 0),
        'values': [msg.get('temp_c', 0), msg.get('humidity_pct', 0), msg.get('pm25', 0)],
    }
    return [parsed, arrival, time.time()]

@SQify
def e_bloom(parsed_data):
    import time, hashlib
    global sq_state
    data = parsed_data[0]; arrival = parsed_data[1]
    p, d = _init_provider(sq_state, 'bloom_filter')
    if 'bitmap' not in sq_state:
        sq_state['bitmap_size'] = 10000
        sq_state['bitmap'] = [False] * sq_state['bitmap_size']
    _exec_sleep(p, d)
    key = str(data.get('sensor_id', '')) + '_' + str(data.get('msgid', 0))
    h = int(hashlib.md5(key.encode()).hexdigest(), 16) % sq_state['bitmap_size']
    data['bloom_valid'] = not sq_state['bitmap'][h]
    sq_state['bitmap'][h] = True
    return [data, arrival, time.time()]

# --- Branch 1: Temperature average ---
@SQify
def e_temp_avg(bloom_data):
    import time
    global sq_state
    data = bloom_data[0]; arrival = bloom_data[1]
    p, d = _init_provider(sq_state, 'average')
    if 'window' not in sq_state:
        sq_state['window'] = []
    _exec_sleep(p, d)
    sq_state['window'].append(data.get('temp', 0))
    if len(sq_state['window']) > 50:
        sq_state['window'] = sq_state['window'][-50:]
    data['temp_avg'] = sum(sq_state['window']) / len(sq_state['window'])
    data['branch_output'] = 'temp_avg'
    return [data, arrival, time.time()]

# --- Branch 2: Humidity Kalman ---
@SQify
def e_humid_kalman(bloom_data):
    import time
    global sq_state
    data = bloom_data[0]; arrival = bloom_data[1]
    p, d = _init_provider(sq_state, 'kalman_filter')
    if 'est' not in sq_state:
        sq_state['est'] = 50.0; sq_state['err'] = 1.0
    _exec_sleep(p, d)
    meas = data.get('humidity', 50)
    gain = sq_state['err'] / (sq_state['err'] + 0.5)
    sq_state['est'] += gain * (meas - sq_state['est'])
    sq_state['err'] = (1 - gain) * sq_state['err'] + 1e-4
    data['humidity_smoothed'] = sq_state['est']
    data['branch_output'] = 'humid_kalman'
    return [data, arrival, time.time()]

# --- Branch 3a: PM2.5 Kalman ---
@SQify
def e_pm25_kalman(bloom_data):
    import time
    global sq_state
    data = bloom_data[0]; arrival = bloom_data[1]
    p, d = _init_provider(sq_state, 'kalman_filter')
    if 'est' not in sq_state:
        sq_state['est'] = 0.0; sq_state['err'] = 1.0
    _exec_sleep(p, d)
    meas = data.get('pm25', 0)
    gain = sq_state['err'] / (sq_state['err'] + 0.5)
    sq_state['est'] += gain * (meas - sq_state['est'])
    sq_state['err'] = (1 - gain) * sq_state['err'] + 1e-4
    data['pm25_smoothed'] = sq_state['est']
    return [data, arrival, time.time()]

# --- Branch 3b: PM2.5 Linear Regression (sequential after Kalman) ---
@SQify
def e_pm_lr(kalman_data):
    import time
    global sq_state
    data = kalman_data[0]; arrival = kalman_data[1]
    p, d = _init_provider(sq_state, 'linear_regression')
    if 'x_window' not in sq_state:
        sq_state['x_window'] = []; sq_state['y_window'] = []; sq_state['n'] = 0
    _exec_sleep(p, d)
    sq_state['n'] += 1
    sq_state['x_window'].append(sq_state['n'])
    sq_state['y_window'].append(data.get('pm25_smoothed', 0))
    if len(sq_state['x_window']) > 20:
        sq_state['x_window'] = sq_state['x_window'][-20:]
        sq_state['y_window'] = sq_state['y_window'][-20:]
    n = len(sq_state['x_window'])
    if n >= 2:
        sx = sum(sq_state['x_window']); sy = sum(sq_state['y_window'])
        sxy = sum(x * y for x, y in zip(sq_state['x_window'], sq_state['y_window']))
        sxx = sum(x * x for x in sq_state['x_window'])
        denom = n * sxx - sx * sx
        slope = (n * sxy - sx * sy) / denom if denom != 0 else 0
        data['pm25_trend'] = slope
    else:
        data['pm25_trend'] = 0
    data['branch_output'] = 'pm25_lr'
    return [data, arrival, time.time()]

# --- Branch 4: CO Second-order Moment ---
@SQify
def e_co_moment(bloom_data):
    import time, math
    global sq_state
    data = bloom_data[0]; arrival = bloom_data[1]
    p, d = _init_provider(sq_state, 'second_order_moment')
    if 'n' not in sq_state:
        sq_state['n'] = 0; sq_state['mean'] = 0; sq_state['m2'] = 0
    _exec_sleep(p, d)
    sq_state['n'] += 1
    val = data.get('co', 0)
    delta = val - sq_state['mean']
    sq_state['mean'] += delta / sq_state['n']
    delta2 = val - sq_state['mean']
    sq_state['m2'] += delta * delta2
    var = sq_state['m2'] / sq_state['n'] if sq_state['n'] > 1 else 0
    data['co_mean'] = sq_state['mean']
    data['co_std'] = math.sqrt(var) if var > 0 else 0
    data['branch_output'] = 'co_moment'
    return [data, arrival, time.time()]

# --- Branch 5: NO2 Distinct Count ---
@SQify
def e_no2_distinct(bloom_data):
    import time, hashlib
    global sq_state
    data = bloom_data[0]; arrival = bloom_data[1]
    p, d = _init_provider(sq_state, 'distinct_count')
    if 'registers' not in sq_state:
        sq_state['registers'] = [0] * 64
    _exec_sleep(p, d)
    val = str(round(data.get('no2', 0), 1))
    h = int(hashlib.md5(val.encode()).hexdigest(), 16)
    idx = h % 64
    rho = len(bin(h >> 6)) - len(bin(h >> 6).rstrip('0'))
    sq_state['registers'][idx] = max(sq_state['registers'][idx], rho)
    alpha = 0.7213 / (1 + 1.079 / 64)
    z = 1.0 / sum(2.0 ** (-r) for r in sq_state['registers'])
    data['no2_distinct_estimate'] = int(alpha * 64 * 64 * z)
    data['branch_output'] = 'no2_distinct'
    return [data, arrival, time.time()]

# --- Aggregation: 5-way join ---
@SQify
def e_agg(temp_data, humid_data, pm_data, co_data, no2_data):
    import time
    global sq_state
    arrival = temp_data[1]
    p, d = _init_provider(sq_state, 'join'); _exec_sleep(p, d)
    combined = {
        'msgid': temp_data[0].get('msgid', 0),
        'sensor_id': temp_data[0].get('sensor_id', 'unknown'),
        'temp_avg': temp_data[0].get('temp_avg', 0),
        'humidity_smoothed': humid_data[0].get('humidity_smoothed', 0),
        'pm25_trend': pm_data[0].get('pm25_trend', 0),
        'co_mean': co_data[0].get('co_mean', 0),
        'co_std': co_data[0].get('co_std', 0),
        'no2_distinct': no2_data[0].get('no2_distinct_estimate', 0),
    }
    return [combined, arrival, time.time()]

@SQify
def e_aqi(agg_data):
    import time
    global sq_state
    data = agg_data[0]; arrival = agg_data[1]
    p, d = _init_provider(sq_state, 'second_order_moment'); _exec_sleep(p, d)
    # Simple AQI from PM2.5 trend and CO levels
    pm_score = min(abs(data.get('pm25_trend', 0)) * 100, 200)
    co_score = min(data.get('co_mean', 0) / 30 * 200, 200)
    data['aqi'] = max(pm_score, co_score)
    data['aqi_category'] = 'good' if data['aqi'] < 50 else ('moderate' if data['aqi'] < 100 else 'unhealthy')
    return [data, arrival, time.time()]

@SQify
def e_alert_b(aqi_data):
    import time
    global sq_state
    data = aqi_data[0]; arrival = aqi_data[1]
    p, d = _init_provider(sq_state, 'mqtt_publish')
    if 'alert_count' not in sq_state:
        sq_state['alert_count'] = 0
    _exec_sleep(p, d)
    if data.get('aqi', 0) > 100:
        sq_state['alert_count'] += 1
        data['env_alert'] = sq_state['alert_count']
    data['pipeline_output'] = 'env_stats'
    return [data, arrival, time.time()]

@SQify
def e_mqtt_b(alert_data):
    import time
    global sq_state
    data = alert_data[0]; arrival = alert_data[1]
    p, d = _init_provider(sq_state, 'mqtt_publish'); _exec_sleep(p, d)
    data['env_published'] = True
    return [data, arrival, time.time()]


 
# SUB-PIPELINE C: Predictive Analytics (12 app SQs)
# Based on eval_pred pattern (RIoTBench Prediction topology)
#
# pred_src → p_parse → p_bloom → ┬→ p_avg ────────────────────┐
#                                 ├→ p_kalman → ┬→ p_linreg ───┤→ p_join → p_dt → p_pred_out → p_mqtt_c
#                                 │             └→ p_moment ────┤
#                                 └→ p_distinct ────────────────┘
 

@STREAMify
def pred_src(trigger, workload_type):
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
def p_parse(raw_data):
    import time
    global sq_state
    msg = raw_data[0]; arrival = raw_data[1]
    p, d = _init_provider(sq_state, 'senml_parse'); _exec_sleep(p, d)
    readings = msg.get('e', [])
    values = [r.get('v', 0) for r in readings if r.get('v') is not None]
    parsed = {
        'msgid': msg.get('msgid', 0), 'sensor_id': msg.get('bn', 'pred_sensor'),
        'timestamp': msg.get('bt', 0), 'values': values if values else [0],
    }
    return [parsed, arrival, time.time()]

@SQify
def p_bloom(parsed_data):
    import time, hashlib
    global sq_state
    data = parsed_data[0]; arrival = parsed_data[1]
    p, d = _init_provider(sq_state, 'bloom_filter')
    if 'bitmap' not in sq_state:
        sq_state['bitmap_size'] = 10000
        sq_state['bitmap'] = [False] * sq_state['bitmap_size']
    _exec_sleep(p, d)
    key = str(data.get('sensor_id', '')) + '_' + str(data.get('msgid', 0))
    h = int(hashlib.md5(key.encode()).hexdigest(), 16) % sq_state['bitmap_size']
    data['bloom_valid'] = not sq_state['bitmap'][h]
    sq_state['bitmap'][h] = True
    return [data, arrival, time.time()]

# --- Branch 1: Block window average ---
@SQify
def p_avg(bloom_data):
    import time
    global sq_state
    data = bloom_data[0]; arrival = bloom_data[1]
    p, d = _init_provider(sq_state, 'average')
    if 'window' not in sq_state:
        sq_state['window'] = []
    _exec_sleep(p, d)
    val = data.get('values', [0])[0]
    sq_state['window'].append(val)
    if len(sq_state['window']) > 50:
        sq_state['window'] = sq_state['window'][-50:]
    data['pred_avg'] = sum(sq_state['window']) / len(sq_state['window'])
    data['pred_min'] = min(sq_state['window'])
    data['pred_max'] = max(sq_state['window'])
    return [data, arrival, time.time()]

# --- Branch 2a: Kalman filter ---
@SQify
def p_kalman(bloom_data):
    import time
    global sq_state
    data = bloom_data[0]; arrival = bloom_data[1]
    p, d = _init_provider(sq_state, 'kalman_filter')
    if 'est' not in sq_state:
        sq_state['est'] = 0.0; sq_state['err'] = 1.0
    _exec_sleep(p, d)
    meas = data.get('values', [0])[0]
    gain = sq_state['err'] / (sq_state['err'] + 0.1)
    sq_state['est'] += gain * (meas - sq_state['est'])
    sq_state['err'] = (1 - gain) * sq_state['err'] + 1e-5
    data['kalman_est'] = sq_state['est']
    return [data, arrival, time.time()]

# --- Branch 2b: Linear regression (from Kalman) ---
@SQify
def p_linreg(kalman_data):
    import time
    global sq_state
    data = kalman_data[0]; arrival = kalman_data[1]
    p, d = _init_provider(sq_state, 'linear_regression')
    if 'xs' not in sq_state:
        sq_state['xs'] = []; sq_state['ys'] = []; sq_state['seq'] = 0
    _exec_sleep(p, d)
    sq_state['seq'] += 1
    sq_state['xs'].append(sq_state['seq'])
    sq_state['ys'].append(data.get('kalman_est', 0))
    if len(sq_state['xs']) > 20:
        sq_state['xs'] = sq_state['xs'][-20:]
        sq_state['ys'] = sq_state['ys'][-20:]
    n = len(sq_state['xs'])
    if n >= 2:
        sx = sum(sq_state['xs']); sy = sum(sq_state['ys'])
        sxy = sum(x * y for x, y in zip(sq_state['xs'], sq_state['ys']))
        sxx = sum(x * x for x in sq_state['xs'])
        denom = n * sxx - sx * sx
        data['slope'] = (n * sxy - sx * sy) / denom if denom != 0 else 0
        data['predicted_next'] = data['slope'] * (sq_state['seq'] + 1) + (sy - data['slope'] * sx) / n
    else:
        data['slope'] = 0; data['predicted_next'] = 0
    return [data, arrival, time.time()]

# --- Branch 2c: Second-order moment (from Kalman) ---
@SQify
def p_moment(kalman_data):
    import time, math
    global sq_state
    data = kalman_data[0]; arrival = kalman_data[1]
    p, d = _init_provider(sq_state, 'second_order_moment')
    if 'n' not in sq_state:
        sq_state['n'] = 0; sq_state['mean'] = 0; sq_state['m2'] = 0
    _exec_sleep(p, d)
    sq_state['n'] += 1
    val = data.get('kalman_est', 0)
    delta = val - sq_state['mean']
    sq_state['mean'] += delta / sq_state['n']
    delta2 = val - sq_state['mean']
    sq_state['m2'] += delta * delta2
    var = sq_state['m2'] / sq_state['n'] if sq_state['n'] > 1 else 0
    data['moment_mean'] = sq_state['mean']
    data['moment_std'] = math.sqrt(var) if var > 0 else 0
    return [data, arrival, time.time()]

# --- Branch 3: Distinct count ---
@SQify
def p_distinct(bloom_data):
    import time, hashlib
    global sq_state
    data = bloom_data[0]; arrival = bloom_data[1]
    p, d = _init_provider(sq_state, 'distinct_count')
    if 'registers' not in sq_state:
        sq_state['registers'] = [0] * 64
    _exec_sleep(p, d)
    val = str(round(data.get('values', [0])[0], 1))
    h = int(hashlib.md5(val.encode()).hexdigest(), 16)
    idx = h % 64
    rho = len(bin(h >> 6)) - len(bin(h >> 6).rstrip('0'))
    sq_state['registers'][idx] = max(sq_state['registers'][idx], rho)
    alpha = 0.7213 / (1 + 1.079 / 64)
    z = 1.0 / sum(2.0 ** (-r) for r in sq_state['registers'])
    data['distinct_estimate'] = int(alpha * 64 * 64 * z)
    return [data, arrival, time.time()]

# --- Join: 4-way ---
@SQify
def p_join(avg_data, lr_data, moment_data, distinct_data):
    import time
    global sq_state
    arrival = avg_data[1]
    p, d = _init_provider(sq_state, 'join'); _exec_sleep(p, d)
    combined = {
        'msgid': avg_data[0].get('msgid', 0),
        'pred_avg': avg_data[0].get('pred_avg', 0),
        'slope': lr_data[0].get('slope', 0),
        'predicted_next': lr_data[0].get('predicted_next', 0),
        'moment_mean': moment_data[0].get('moment_mean', 0),
        'moment_std': moment_data[0].get('moment_std', 0),
        'distinct_count': distinct_data[0].get('distinct_estimate', 0),
    }
    return [combined, arrival, time.time()]

# --- Decision tree classification ---
@SQify
def p_dt(joined_data):
    import time
    global sq_state
    data = joined_data[0]; arrival = joined_data[1]
    p, d = _init_provider(sq_state, 'decision_tree_classify'); _exec_sleep(p, d)
    slope = data.get('slope', 0)
    std = data.get('moment_std', 0)
    if slope > 0.5 and std < 1.0:
        data['prediction_class'] = 'rising_stable'
    elif slope > 0.5:
        data['prediction_class'] = 'rising_volatile'
    elif slope < -0.5:
        data['prediction_class'] = 'falling'
    else:
        data['prediction_class'] = 'stable'
    return [data, arrival, time.time()]

@SQify
def p_pred_out(dt_data):
    import time
    global sq_state
    data = dt_data[0]; arrival = dt_data[1]
    p, d = _init_provider(sq_state, 'sliding_linear_reg'); _exec_sleep(p, d)
    data['final_prediction'] = data.get('predicted_next', 0)
    data['confidence'] = max(0, 1.0 - data.get('moment_std', 0))
    return [data, arrival, time.time()]

@SQify
def p_mqtt_c(pred_data):
    import time
    global sq_state
    data = pred_data[0]; arrival = pred_data[1]
    p, d = _init_provider(sq_state, 'mqtt_publish'); _exec_sleep(p, d)
    data['pipeline_output'] = 'predictive_analytics'
    return [data, arrival, time.time()]


 
# DASHBOARD MERGE (5 app SQs)
# Fuses outputs from all 3 sub-pipelines into unified platform view
 

@SQify
def dash_fuse(traffic_data, env_data, pred_data):
    import time
    global sq_state
    arrival = traffic_data[1]
    p, d = _init_provider(sq_state, 'join')
    if 'fuse_count' not in sq_state:
        sq_state['fuse_count'] = 0
    _exec_sleep(p, d)
    sq_state['fuse_count'] += 1
    dashboard = {
        'dashboard_seq': sq_state['fuse_count'],
        'traffic': {
            'congestion': traffic_data[0].get('congestion_level', 'unknown'),
            'avg_speed': traffic_data[0].get('avg_speed', 0),
            'zone': traffic_data[0].get('route_zone', 'unknown'),
        },
        'environment': {
            'aqi': env_data[0].get('aqi', 0),
            'category': env_data[0].get('aqi_category', 'unknown'),
            'alert': env_data[0].get('env_alert', None),
        },
        'prediction': {
            'class': pred_data[0].get('prediction_class', 'unknown'),
            'next_value': pred_data[0].get('final_prediction', 0),
            'confidence': pred_data[0].get('confidence', 0),
        },
    }
    return [dashboard, arrival, time.time()]

@SQify
def dash_visualize(fused_data):
    import time
    global sq_state
    data = fused_data[0]; arrival = fused_data[1]
    p, d = _init_provider(sq_state, 'plot')
    if 'viz_history' not in sq_state:
        sq_state['viz_history'] = []
    _exec_sleep(p, d)
    sq_state['viz_history'].append({
        'seq': data.get('dashboard_seq', 0),
        'aqi': data.get('environment', {}).get('aqi', 0),
        'speed': data.get('traffic', {}).get('avg_speed', 0),
    })
    if len(sq_state['viz_history']) > 100:
        sq_state['viz_history'] = sq_state['viz_history'][-100:]
    data['viz_points'] = len(sq_state['viz_history'])
    return [data, arrival, time.time()]

@SQify
def dash_alert(viz_data):
    import time
    global sq_state
    data = viz_data[0]; arrival = viz_data[1]
    p, d = _init_provider(sq_state, 'mqtt_publish')
    if 'platform_alerts' not in sq_state:
        sq_state['platform_alerts'] = 0
    _exec_sleep(p, d)
    env_alert = data.get('environment', {}).get('alert')
    congestion = data.get('traffic', {}).get('congestion', '')
    if env_alert or congestion in ('severe', 'moderate'):
        sq_state['platform_alerts'] += 1
        data['platform_alert_id'] = sq_state['platform_alerts']
    return [data, arrival, time.time()]

@SQify
def dash_upload(alert_data):
    import time, json, hashlib
    global sq_state
    data = alert_data[0]; arrival = alert_data[1]
    p, d = _init_provider(sq_state, 'azure_blob_upload')
    if 'upload_count' not in sq_state:
        sq_state['upload_count'] = 0
    _exec_sleep(p, d)
    sq_state['upload_count'] += 1
    payload = json.dumps(data, default=str)
    data['upload_checksum'] = hashlib.md5(payload.encode()).hexdigest()
    return [data, arrival, time.time()]

@SQify
def platform_sink(upload_data):
    import os, time
    data = upload_data[0]
    arrival = upload_data[1]
    completion_time = time.time()
    latency_ms = (completion_time - arrival) * 1000
    outfile = os.environ.get('TTPYTHON_OUTPUT_FILE', '/tmp/eval_urban_sensing_output.txt')
    with open(outfile, 'a') as f:
        f.write('seq=' + str(data.get('dashboard_seq', 'N/A')) +
                ' traffic=' + str(data.get('traffic', {}).get('congestion', '?')) +
                ' aqi=' + str(round(data.get('environment', {}).get('aqi', 0), 1)) +
                ' pred=' + str(data.get('prediction', {}).get('class', '?')) +
                ' latency=' + str(round(latency_ms, 2)) + 'ms\n')
    print('URBAN done, seq=' + str(data.get('dashboard_seq', 0)) +
          ' latency=' + str(round(latency_ms, 2)) + 'ms')
    return 1


# ====================================================================
# Graph Wiring — 3 sub-pipelines feeding unified dashboard
# ====================================================================
@GRAPHify
def eval_urban_sensing(trigger):
    A_1 = 1
    with TTClock.root() as root_clock:
        start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
        N = 100
        stop_time = start_time + (3000000 * N)
        sampling_time = VALUES_TO_TTTIME(start_time, stop_time)
        sample_window = COPY_TTTIME(A_1, sampling_time)

        # =============================================================
        # SUB-PIPELINE A: Traffic ETL (12 SQs)
        # =============================================================
        with TTConstraint(components=["sensor_interface"]):
            workload_t = 'TAXI'
            traffic_raw = traffic_src(sample_window, workload_t,
                                      TTClock=root_clock, TTPeriod=3000000,
                                      TTPhase=0, TTDataIntervalWidth=1500000)

        with TTConstraint(components=["compute"]):
            tp = t_parse(traffic_raw)
            tr = t_range(tp)
            tb = t_bloom(tr)
            ti = t_interp(tb)
            ts = t_speed(ti)
            tc = t_congestion(ts)
            trt = t_route(tc)
            tj = t_join(trt)
            ta = t_annotate(tj)

        with TTConstraint(components=["storage"]):
            tdb = t_db_insert(ta)

        with TTConstraint(components=["mqtt_broker"]):
            traffic_out = t_mqtt_a(tdb)

        # =============================================================
        # SUB-PIPELINE B: Environment Statistics (13 SQs)
        # =============================================================
        with TTConstraint(components=["sensor_interface"]):
            workload_e = 'SYS'
            env_raw = env_src(sample_window, workload_e,
                              TTClock=root_clock, TTPeriod=3000000,
                              TTPhase=500000, TTDataIntervalWidth=1500000)

        with TTConstraint(components=["compute"]):
            ep = e_parse(env_raw)
            eb = e_bloom(ep)

        # 5 parallel branches from bloom
        with TTConstraint(components=["compute"]):
            b1_temp = e_temp_avg(eb)

        with TTConstraint(components=["compute"]):
            b2_humid = e_humid_kalman(eb)

        with TTConstraint(components=["compute"]):
            b3_pm_k = e_pm25_kalman(eb)
            b3_pm_lr = e_pm_lr(b3_pm_k)

        with TTConstraint(components=["compute"]):
            b4_co = e_co_moment(eb)

        with TTConstraint(components=["compute"]):
            b5_no2 = e_no2_distinct(eb)

        # Aggregation
        with TTConstraint(components=["compute"]):
            e_aggregated = e_agg(b1_temp, b2_humid, b3_pm_lr, b4_co, b5_no2)
            e_aq = e_aqi(e_aggregated)

        with TTConstraint(components=["mqtt_broker"]):
            e_alerted = e_alert_b(e_aq)
            env_out = e_mqtt_b(e_alerted)

        # =============================================================
        # SUB-PIPELINE C: Predictive Analytics (12 SQs)
        # =============================================================
        with TTConstraint(components=["sensor_interface"]):
            workload_p = 'FIT'
            pred_raw = pred_src(sample_window, workload_p,
                                TTClock=root_clock, TTPeriod=3000000,
                                TTPhase=1000000, TTDataIntervalWidth=1500000)

        with TTConstraint(components=["compute"]):
            pp = p_parse(pred_raw)
            pb = p_bloom(pp)

        # 3 branches + sub-branches from bloom
        with TTConstraint(components=["compute"]):
            c1_avg = p_avg(pb)

        with TTConstraint(components=["compute"]):
            c2_kalman = p_kalman(pb)
            c2b_lr = p_linreg(c2_kalman)
            c2c_mom = p_moment(c2_kalman)

        with TTConstraint(components=["compute"]):
            c3_dist = p_distinct(pb)

        # Join + sequential tail
        with TTConstraint(components=["compute"]):
            p_joined = p_join(c1_avg, c2b_lr, c2c_mom, c3_dist)
            p_classified = p_dt(p_joined)
            p_output = p_pred_out(p_classified)

        with TTConstraint(components=["mqtt_broker"]):
            pred_out = p_mqtt_c(p_output)

        # =============================================================
        # DASHBOARD MERGE (5 SQs)
        # =============================================================
        with TTConstraint(components=["compute"]):
            fused = dash_fuse(traffic_out, env_out, pred_out)
            visualized = dash_visualize(fused)

        with TTConstraint(components=["mqtt_broker", "storage"]):
            alerted = dash_alert(visualized)
            uploaded = dash_upload(alerted)
            result = platform_sink(uploaded)
