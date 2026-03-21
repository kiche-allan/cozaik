# eval_video_analytics.py — Video Analytics Pipeline (13 app SQs)
#
# Inspired by: Elgamal et al., "Internet-of-Things Edge Computing Systems
# for Streaming Video Analytics," IoT, 2023; Liu et al., "Edge Computing
# for Autonomous Driving," Proceedings of the IEEE, 2019.
#
# Video analytics pipelines are explicitly described in the literature as
# a DAG of sequential stages including: video decompression, frame
# pre-processing, object detection, classification, tracking, pose
# estimation, and action classification. We implement 13 stages using
# RIoTBench task primitives mapped to each pipeline stage.
#
# Topology: Pure deep sequential chain (depth = 13)
#   vap_source → frame_decode → noise_filter → resize_normalize →
#   background_subtract → object_detect → object_classify →
#   object_track → trajectory_predict → anomaly_detect →
#   event_classify → log_upload → vap_sink
#
# App SQs: 13
# Shape: Deep chain — no parallelism, longest critical path in the suite
#
from ticktalkpython.SQ import STREAMify, SQify, GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *

# SQ 1: Camera Frame Source 
@STREAMify
def vap_source(trigger, workload_type):
    import sys, os, time
    global sq_state
    if sq_state.get('generator', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_senml_generator
        sq_state['generator'] = get_senml_generator(workload_type)
        sq_state['frame_id'] = 0
    sq_state['frame_id'] += 1
    # Generate synthetic frame metadata (not actual pixels)
    message = sq_state['generator'].generate_message()
    frame = {
        'frame_id': sq_state['frame_id'],
        'camera_id': message.get('bn', 'cam_0'),
        'timestamp': message.get('bt', 0),
        'resolution': [1920, 1080],
        'pixel_data_hash': hash(str(message.get('e', []))),
        'raw_metadata': message.get('e', []),
    }
    return [frame, time.time()]

# SQ 2: Frame Decode 
# Analogous to video decompression / H.265 decode
# Uses xml_parse provider (CPU-intensive parsing, heaviest single-stage cost)
@SQify
def frame_decode(raw_frame):
    import sys, os, time
    global sq_state
    frame = raw_frame[0]
    arrival_time = raw_frame[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('xml_parse')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    frame['decoded'] = True
    frame['color_space'] = 'BGR'
    frame['channels'] = 3
    return [frame, arrival_time, time.time()]

# SQ 3: Noise Filter 
# Temporal noise smoothing across consecutive frames
# Uses kalman_filter provider
@SQify
def noise_filter(decoded_frame):
    import sys, os, time
    global sq_state
    frame = decoded_frame[0]
    arrival_time = decoded_frame[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['process_var'] = 1e-5
        sq_state['meas_var'] = 0.01
        sq_state['est'] = 0.0
        sq_state['err_est'] = 1.0
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    # Kalman-based temporal smoothing on frame intensity
    measurement = frame.get('pixel_data_hash', 0) % 256
    gain = sq_state['err_est'] / (sq_state['err_est'] + sq_state['meas_var'])
    sq_state['est'] = sq_state['est'] + gain * (measurement - sq_state['est'])
    sq_state['err_est'] = (1 - gain) * sq_state['err_est'] + sq_state['process_var']
    frame['noise_filtered'] = True
    frame['smoothed_intensity'] = sq_state['est']
    return [frame, arrival_time, time.time()]

# SQ 4: Resize & Normalize 
# Spatial interpolation for model input dimensions
# Uses interpolation provider
@SQify
def resize_normalize(filtered_frame):
    import sys, os, time
    global sq_state
    frame = filtered_frame[0]
    arrival_time = filtered_frame[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('interpolation')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['target_size'] = [416, 416]
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    orig = frame.get('resolution', [1920, 1080])
    frame['scale_x'] = sq_state['target_size'][0] / orig[0]
    frame['scale_y'] = sq_state['target_size'][1] / orig[1]
    frame['resolution'] = sq_state['target_size']
    frame['normalized'] = True
    return [frame, arrival_time, time.time()]

# SQ 5: Background Subtraction 
# Filter unchanged regions using frame-to-frame comparison
# Uses bloom_filter provider (fast lookup / membership test)
@SQify
def background_subtract(norm_frame):
    import sys, os, time, hashlib
    global sq_state
    frame = norm_frame[0]
    arrival_time = norm_frame[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('bloom_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['bg_model_size'] = 5000
        sq_state['bg_model'] = [False] * sq_state['bg_model_size']
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    # Simulate background model lookup
    key = str(frame.get('frame_id', 0))
    h = int(hashlib.md5(key.encode()).hexdigest(), 16) % sq_state['bg_model_size']
    frame['foreground_detected'] = not sq_state['bg_model'][h]
    sq_state['bg_model'][h] = True
    return [frame, arrival_time, time.time()]

# SQ 6: Object Detection 
# Primary detection pass (e.g., YOLO-like inference)
# Uses decision_tree_classify provider (heaviest compute per-frame)
@SQify
def object_detect(bg_frame):
    import sys, os, time, random
    global sq_state
    frame = bg_frame[0]
    arrival_time = bg_frame[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('decision_tree_classify')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['det_id'] = 0
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    # Generate synthetic detections
    n_detections = random.randint(0, 5)
    detections = []
    for i in range(n_detections):
        sq_state['det_id'] += 1
        detections.append({
            'det_id': sq_state['det_id'],
            'bbox': [random.randint(0, 300), random.randint(0, 300),
                     random.randint(50, 150), random.randint(50, 150)],
            'confidence': random.uniform(0.3, 0.99),
            'class_id': -1,  # unclassified
        })
    frame['detections'] = detections
    frame['detection_count'] = n_detections
    return [frame, arrival_time, time.time()]

# SQ 7: Object Classification 
# Fine-grained classification of detected objects
# Uses decision_tree_classify provider
@SQify
def object_classify(det_frame):
    import sys, os, time, random
    global sq_state
    frame = det_frame[0]
    arrival_time = det_frame[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('decision_tree_classify')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['class_labels'] = ['car', 'truck', 'pedestrian', 'cyclist', 'bus']
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    for det in frame.get('detections', []):
        det['class_id'] = random.randint(0, len(sq_state['class_labels']) - 1)
        det['class_label'] = sq_state['class_labels'][det['class_id']]
    return [frame, arrival_time, time.time()]

# SQ 8: Object Tracking 
# Multi-object tracking using Kalman state estimation
# Uses kalman_filter provider
@SQify
def object_track(cls_frame):
    import sys, os, time
    global sq_state
    frame = cls_frame[0]
    arrival_time = cls_frame[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('kalman_filter')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['tracks'] = {}
        sq_state['next_track_id'] = 0
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    for det in frame.get('detections', []):
        # Simple nearest-neighbor track association
        sq_state['next_track_id'] += 1
        det['track_id'] = sq_state['next_track_id']
        cx = det['bbox'][0] + det['bbox'][2] / 2
        cy = det['bbox'][1] + det['bbox'][3] / 2
        det['track_state'] = {'cx': cx, 'cy': cy, 'vx': 0, 'vy': 0}
    frame['tracked'] = True
    return [frame, arrival_time, time.time()]

# SQ 9: Trajectory Prediction 
# Predict future positions using linear regression on track history
# Uses sliding_linear_reg provider
@SQify
def trajectory_predict(tracked_frame):
    import sys, os, time
    global sq_state
    frame = tracked_frame[0]
    arrival_time = tracked_frame[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('sliding_linear_reg')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['history'] = {}
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    for det in frame.get('detections', []):
        state = det.get('track_state', {})
        # Simple linear extrapolation
        det['predicted_pos'] = {
            'cx': state.get('cx', 0) + state.get('vx', 0),
            'cy': state.get('cy', 0) + state.get('vy', 0),
        }
    frame['trajectory_predicted'] = True
    return [frame, arrival_time, time.time()]

# SQ 10: Anomaly Detection 
# Statistical anomaly scoring on trajectories
# Uses second_order_moment provider
@SQify
def anomaly_detect(pred_frame):
    import sys, os, time, math
    global sq_state
    frame = pred_frame[0]
    arrival_time = pred_frame[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('second_order_moment')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
        sq_state['speed_history'] = []
        sq_state['mean'] = 0.0
        sq_state['m2'] = 0.0
        sq_state['n'] = 0
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    # Welford's online algorithm on detection count as anomaly signal
    n_det = frame.get('detection_count', 0)
    sq_state['n'] += 1
    delta = n_det - sq_state['mean']
    sq_state['mean'] += delta / sq_state['n']
    delta2 = n_det - sq_state['mean']
    sq_state['m2'] += delta * delta2
    variance = sq_state['m2'] / sq_state['n'] if sq_state['n'] > 1 else 0
    std_dev = math.sqrt(variance) if variance > 0 else 0.001
    z_score = abs(n_det - sq_state['mean']) / std_dev if std_dev > 0 else 0
    frame['anomaly_score'] = z_score
    frame['is_anomaly'] = z_score > 2.0
    return [frame, arrival_time, time.time()]

# SQ 11: Event Classification 
# Classify the frame-level event (normal traffic, incident, congestion)
# Uses decision_tree_classify provider
@SQify
def event_classify(anomaly_frame):
    import sys, os, time
    global sq_state
    frame = anomaly_frame[0]
    arrival_time = anomaly_frame[1]
    if sq_state.get('provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from riotbench_provider import get_task_provider
        sq_state['provider'] = get_task_provider('decision_tree_classify')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'raspberry_pi_3')
    exec_time_ms = sq_state['provider'].get_execution_time(sq_state['device_type'])
    time.sleep(exec_time_ms / 1000)
    score = frame.get('anomaly_score', 0)
    n_det = frame.get('detection_count', 0)
    if frame.get('is_anomaly', False) and n_det > 3:
        frame['event_type'] = 'congestion'
    elif frame.get('is_anomaly', False):
        frame['event_type'] = 'incident'
    else:
        frame['event_type'] = 'normal'
    frame['event_confidence'] = min(score / 3.0, 1.0)
    return [frame, arrival_time, time.time()]

# SQ 12: Log Upload 
# Serialize event results and upload to cloud storage
# Uses azure_blob_upload provider
@SQify
def log_upload(event_frame):
    import sys, os, time, json
    global sq_state
    frame = event_frame[0]
    arrival_time = event_frame[1]
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
        'frame_id': frame.get('frame_id'),
        'event_type': frame.get('event_type'),
        'anomaly_score': frame.get('anomaly_score'),
        'detection_count': frame.get('detection_count'),
    })
    import hashlib
    frame['upload_checksum'] = hashlib.md5(payload.encode()).hexdigest()
    frame['upload_sequence'] = sq_state['upload_count']
    return [frame, arrival_time, time.time()]

# SQ 13: Sink 
@SQify
def vap_sink(upload_data):
    import os, time
    frame = upload_data[0]
    arrival_time = upload_data[1]
    completion_time = time.time()
    latency_ms = (completion_time - arrival_time) * 1000
    outfile = os.environ.get('TTPYTHON_OUTPUT_FILE', '/tmp/eval_vap_output.txt')
    with open(outfile, 'a') as f:
        f.write('frame=' + str(frame.get('frame_id', 'N/A')) +
                ' event=' + str(frame.get('event_type', '?')) +
                ' latency=' + str(round(latency_ms, 2)) + 'ms\n')
    print('VAP done, frame=' + str(frame.get('frame_id')) +
          ' event=' + str(frame.get('event_type')) +
          ' latency=' + str(round(latency_ms, 2)) + 'ms')
    return 1


@GRAPHify
def eval_video_analytics(trigger):
    A_1 = 1
    with TTClock.root() as root_clock:
        start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
        N = 100
        stop_time = start_time + (2000000 * N)
        sampling_time = VALUES_TO_TTTIME(start_time, stop_time)
        sample_window = COPY_TTTIME(A_1, sampling_time)

        # Camera source: needs sensor interface
        with TTConstraint(components=["sensor_interface"]):
            workload = 'SYS'
            raw_frames = vap_source(sample_window, workload,
                                    TTClock=root_clock, TTPeriod=2000000,
                                    TTPhase=0, TTDataIntervalWidth=1000000)

        # Frame preprocessing: compute-intensive
        with TTConstraint(components=["compute"]):
            decoded = frame_decode(raw_frames)
            denoised = noise_filter(decoded)
            resized = resize_normalize(denoised)
            fg_frames = background_subtract(resized)

        # Detection and classification: heavy compute
        with TTConstraint(components=["compute"]):
            detected = object_detect(fg_frames)
            classified = object_classify(detected)
            tracked = object_track(classified)
            predicted = trajectory_predict(tracked)

        # Anomaly analysis and event classification
        with TTConstraint(components=["compute"]):
            anomalies = anomaly_detect(predicted)
            events = event_classify(anomalies)

        # Upload and sink: needs storage/network
        with TTConstraint(components=["storage"]):
            uploaded = log_upload(events)
            result = vap_sink(uploaded)
