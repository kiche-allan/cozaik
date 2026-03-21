# Copyright 2021 The Authors
# Copyright 2025 TTPython Extensions - Evaluation Workload
#
# Evaluation Workload: 6-CAV Sensor Fusion
#
# SQ Count: ~52 SQs
# Depth: 4

from ticktalkpython.SQ import STREAMify, SQify, GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *


@STREAMify
def camera_sampler(trigger, cav_num):
    import sys, os, time
    global sq_state
    if sq_state.get('camera_provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from data_provider import get_camera_provider
        sq_state['camera_provider'] = get_camera_provider(cav_num, profile='riot_edge_ai')
    frame_read, camera_timestamp = sq_state['camera_provider'].get_frame()
    return [frame_read, camera_timestamp, time.time()]


@STREAMify
def lidar_sampler(trigger, cav_num):
    import sys, os, time
    global sq_state
    if sq_state.get('lidar_provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from data_provider import get_lidar_provider
        sq_state['lidar_provider'] = get_lidar_provider(cav_num, profile='riot_edge_ai')
    localization, lidar_frame, lidar_timestamp = sq_state['lidar_provider'].get_frame()
    return [localization, lidar_frame, lidar_timestamp, time.time()]


@SQify
def process_camera(cam_sample):
    import sys, os, time, random
    global sq_state
    camera_frame = cam_sample[0]
    camera_timestamp = cam_sample[1]
    arrival_time = cam_sample[2]
    if sq_state.get('camera_provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from data_provider import get_camera_provider
        sq_state['camera_provider'] = get_camera_provider(0, profile='riot_edge_ai')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'jetson_nano')
    device_type = sq_state['device_type']
    exec_time_ms = sq_state['camera_provider'].get_execution_time(device_type)
    time.sleep(exec_time_ms / 1000)
    num_detections = random.randint(2, 8)
    coordinates = [[random.uniform(0.1, 0.9), random.uniform(0.1, 0.9), random.uniform(0.05, 0.3),
                    random.uniform(0.05, 0.3), random.uniform(0.5, 0.99),
                    random.choice(['vehicle', 'pedestrian', 'cyclist'])] for _ in range(num_detections)]
    return [coordinates, time.time(), arrival_time, time.time()]


@SQify
def process_lidar(lidar_package):
    import sys, os, time
    global sq_state
    localization = lidar_package[0]
    lidar_frame = lidar_package[1]
    lidar_timestamp = lidar_package[2]
    arrival_time = lidar_package[3]
    if sq_state.get('lidar_provider', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from data_provider import get_lidar_provider
        sq_state['lidar_provider'] = get_lidar_provider(0, profile='riot_edge_ai')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'jetson_nano')
    device_type = sq_state['device_type']
    exec_time_ms = sq_state['lidar_provider'].get_execution_time(device_type)
    time.sleep(exec_time_ms / 1000)
    lidarcoordinates = [[point.get('x', 0), point.get('y', 0), 0.85] for point in lidar_frame[:15]]
    return [localization, lidarcoordinates, lidar_timestamp, arrival_time, time.time()]


@SQify
def local_fusion(processed_camera, processed_lidar, cav_num):
    import sys, os, time
    global sq_state
    if sq_state.get('fusion_init', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from data_provider import get_timing_profile
        sq_state['timing'] = get_timing_profile('riot_edge_ai')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'jetson_nano')
        sq_state['fusion_init'] = True
    device_type = sq_state['device_type']
    exec_time_ms = sq_state['timing']['fusion_ms'].get(device_type, 30)
    time.sleep(exec_time_ms / 1000)
    cam_detections = processed_camera[0]
    camera_timestamp = processed_camera[1]
    localization = processed_lidar[0]
    lidar_detections = processed_lidar[1]
    localization_offsets = [[-.75, 0.0, 0.], [-1.5, 0.0, 0.], [-2.25, 0.0, 0.], [-3.0, 0.0, 0.], [-3.75, 0.0, 0.], [-4.5, 0.0, 0.]]
    loc_offset = localization_offsets[cav_num] if cav_num < len(localization_offsets) else [0, 0, 0]
    adjusted_localization = [localization[0] + loc_offset[0], localization[1] + loc_offset[1],
                             localization[2] + loc_offset[2] if len(localization) > 2 else 0]
    fusion_result = []
    for det in cam_detections:
        fusion_result.append((det[0] + adjusted_localization[0], det[1] + adjusted_localization[1],
                              det[4], det[5], 'camera', cav_num))
    for det in lidar_detections:
        fusion_result.append((det[0] + adjusted_localization[0], det[1] + adjusted_localization[1],
                              det[2], 'object', 'lidar', cav_num))
    time_watch = [processed_camera[2], processed_lidar[3], processed_camera[3], processed_lidar[4], time.time()]
    return [fusion_result, camera_timestamp, adjusted_localization, time_watch]


@SQify
def global_fusion(fusion_cav0, fusion_cav1, fusion_cav2, fusion_cav3, fusion_cav4, fusion_cav5):
    import sys, os, time
    global sq_state
    if sq_state.get('global_init', None) is None:
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from data_provider import get_timing_profile
        sq_state['timing'] = get_timing_profile('riot_edge_ai')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'server_x86')
        sq_state['global_init'] = True
    device_type = sq_state['device_type']
    exec_time_ms = sq_state['timing']['global_fusion_ms'].get(device_type, 20)
    time.sleep(exec_time_ms / 1000)
    all_detections = []
    all_detections.extend([(d[0], d[1], d[2], 'cav0') for d in fusion_cav0[0]])
    all_detections.extend([(d[0], d[1], d[2], 'cav1') for d in fusion_cav1[0]])
    all_detections.extend([(d[0], d[1], d[2], 'cav2') for d in fusion_cav2[0]])
    all_detections.extend([(d[0], d[1], d[2], 'cav3') for d in fusion_cav3[0]])
    all_detections.extend([(d[0], d[1], d[2], 'cav4') for d in fusion_cav4[0]])
    all_detections.extend([(d[0], d[1], d[2], 'cav5') for d in fusion_cav5[0]])
    results = [[det[0], det[1], det[2]] for det in all_detections]
    vehicle_positions = [[fusion_cav0[2][0], fusion_cav0[2][1], 'cav0'],
                         [fusion_cav1[2][0], fusion_cav1[2][1], 'cav1'],
                         [fusion_cav2[2][0], fusion_cav2[2][1], 'cav2'],
                         [fusion_cav3[2][0], fusion_cav3[2][1], 'cav3'],
                         [fusion_cav4[2][0], fusion_cav4[2][1], 'cav4'],
                         [fusion_cav5[2][0], fusion_cav5[2][1], 'cav5']]
    return [results, vehicle_positions, time.time()]


@SQify
def write_to_file(fusion_result):
    import os, time
    outfile = os.environ.get('TTPYTHON_OUTPUT_FILE', '/tmp/eval_6cav_output.txt')
    results = fusion_result[0]
    vehicle_positions = fusion_result[1]
    with open(outfile, 'a') as f:
        f.write(f"=== Global Fusion @ {time.time()} ===\n")
        f.write(f"Vehicle positions: {vehicle_positions}\n")
        f.write(f"Detected objects: {len(results)}\n")
    print(f"[write_to_file] Processed global fusion @ {time.time():.3f}, {len(results)} objects")
    return 1


@GRAPHify
def eval_6cav_fusion(trigger):
    A_1 = 1
    with TTClock.root() as root_clock:
        start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
        N = 100
        stop_time = start_time + (2500000 * N)
        sampling_time = VALUES_TO_TTTIME(start_time, stop_time)
        sample_window = COPY_TTTIME(A_1, sampling_time)

        with TTConstraint(name="cav0"):
            cav_0 = 0
            cam_sample = camera_sampler(cav_0, sample_window, TTClock=root_clock, TTPeriod=2500000, TTPhase=0, TTDataIntervalWidth=1250000)
            lidar_sample = lidar_sampler(cav_0, sample_window, TTClock=root_clock, TTPeriod=2500000, TTPhase=0, TTDataIntervalWidth=1250000)
            cam_output = process_camera(cam_sample)
            lidar_output = process_lidar(lidar_sample)
            fusion_result = local_fusion(cam_output, lidar_output, cav_0)

        with TTConstraint(name="cav1"):
            cav_1 = 1
            cam_sample2 = camera_sampler(cav_1, sample_window, TTClock=root_clock, TTPeriod=2500000, TTPhase=0, TTDataIntervalWidth=1250000)
            lidar_sample2 = lidar_sampler(cav_1, sample_window, TTClock=root_clock, TTPeriod=2500000, TTPhase=0, TTDataIntervalWidth=1250000)
            cam_output2 = process_camera(cam_sample2)
            lidar_output2 = process_lidar(lidar_sample2)
            fusion_result2 = local_fusion(cam_output2, lidar_output2, cav_1)

        with TTConstraint(name="cav2"):
            cav_2 = 2
            cam_sample3 = camera_sampler(cav_2, sample_window, TTClock=root_clock, TTPeriod=2500000, TTPhase=0, TTDataIntervalWidth=1250000)
            lidar_sample3 = lidar_sampler(cav_2, sample_window, TTClock=root_clock, TTPeriod=2500000, TTPhase=0, TTDataIntervalWidth=1250000)
            cam_output3 = process_camera(cam_sample3)
            lidar_output3 = process_lidar(lidar_sample3)
            fusion_result3 = local_fusion(cam_output3, lidar_output3, cav_2)

        with TTConstraint(name="cav3"):
            cav_3 = 3
            cam_sample4 = camera_sampler(cav_3, sample_window, TTClock=root_clock, TTPeriod=2500000, TTPhase=0, TTDataIntervalWidth=1250000)
            lidar_sample4 = lidar_sampler(cav_3, sample_window, TTClock=root_clock, TTPeriod=2500000, TTPhase=0, TTDataIntervalWidth=1250000)
            cam_output4 = process_camera(cam_sample4)
            lidar_output4 = process_lidar(lidar_sample4)
            fusion_result4 = local_fusion(cam_output4, lidar_output4, cav_3)

        with TTConstraint(name="cav4"):
            cav_4 = 4
            cam_sample5 = camera_sampler(cav_4, sample_window, TTClock=root_clock, TTPeriod=2500000, TTPhase=0, TTDataIntervalWidth=1250000)
            lidar_sample5 = lidar_sampler(cav_4, sample_window, TTClock=root_clock, TTPeriod=2500000, TTPhase=0, TTDataIntervalWidth=1250000)
            cam_output5 = process_camera(cam_sample5)
            lidar_output5 = process_lidar(lidar_sample5)
            fusion_result5 = local_fusion(cam_output5, lidar_output5, cav_4)

        with TTConstraint(name="cav5"):
            cav_5 = 5
            cam_sample6 = camera_sampler(cav_5, sample_window, TTClock=root_clock, TTPeriod=2500000, TTPhase=0, TTDataIntervalWidth=1250000)
            lidar_sample6 = lidar_sampler(cav_5, sample_window, TTClock=root_clock, TTPeriod=2500000, TTPhase=0, TTDataIntervalWidth=1250000)
            cam_output6 = process_camera(cam_sample6)
            lidar_output6 = process_lidar(lidar_sample6)
            fusion_result6 = local_fusion(cam_output6, lidar_output6, cav_5)

        with TTConstraint(name="rsu"):
            global_fusion_result = global_fusion(fusion_result, fusion_result2, fusion_result3, fusion_result4, fusion_result5, fusion_result6)
            result = write_to_file(global_fusion_result)