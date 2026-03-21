# Copyright 2021 The Authors
# Copyright 2025 TTPython Extensions - Evaluation Workload
#
# Evaluation Workload: 3-CAV Sensor Fusion (Real Execution Version)
# =================================================================
#
# Natural extension of example_3_two_cavs_fusion_stream.py
# Modified to use data_provider.py for timing-accurate synthetic data.
#
# SQ Count: ~17 SQs
# Depth: 4

from ticktalkpython.SQ import STREAMify, SQify, GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *


# =============================================================================
# DATA SAMPLING SQs
# =============================================================================

@STREAMify
def camera_sampler(trigger, cav_num):
    # Sample camera frames using data_provider.
    import sys, os, time
    global sq_state

    if sq_state.get('camera_provider', None) is None:
        # Ensure evals/ is in path for data_provider import
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from data_provider import get_camera_provider
        sq_state['camera_provider'] = get_camera_provider(cav_num, profile='riot_edge_ai')
        sq_state['sample_count'] = 0
    
    frame_read, camera_timestamp = sq_state['camera_provider'].get_frame()
    sq_state['sample_count'] += 1
    
    return [frame_read, camera_timestamp, time.time()]


@STREAMify
def lidar_sampler(trigger, cav_num):
    # Sample LiDAR frames using data_provider.
    import sys, os, time
    global sq_state

    if sq_state.get('lidar_provider', None) is None:
        # Ensure evals/ is in path for data_provider import
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from data_provider import get_lidar_provider
        sq_state['lidar_provider'] = get_lidar_provider(cav_num, profile='riot_edge_ai')
        sq_state['sample_count'] = 0
    
    localization, lidar_frame, lidar_timestamp = sq_state['lidar_provider'].get_frame()
    sq_state['sample_count'] += 1
    
    return [localization, lidar_frame, lidar_timestamp, time.time()]


# =============================================================================
# PROCESSING SQs
# =============================================================================

@SQify
def process_camera(cam_sample):
    # Process camera frame - simulates YOLO-style object detection.
    #Timing is based on RIoT edge AI profile for realistic execution.
    import sys, os, time, random
    global sq_state

    camera_frame = cam_sample[0]
    camera_timestamp = cam_sample[1]
    arrival_time = cam_sample[2]
    
    # Initialize provider for timing information
    if sq_state.get('camera_provider', None) is None:
        # Ensure evals/ is in path for data_provider import
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from data_provider import get_camera_provider
        sq_state['camera_provider'] = get_camera_provider(0, profile='riot_edge_ai')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'jetson_nano')
    
    # Simulate processing time based on device type
    device_type = sq_state['device_type']
    exec_time_ms = sq_state['camera_provider'].get_execution_time(device_type)
    time.sleep(exec_time_ms / 1000)
    
    # Generate synthetic detection output
    # Format: [x, y, width, height, confidence, class_name]
    num_detections = random.randint(2, 8)
    coordinates = []
    for _ in range(num_detections):
        detection = [
            random.uniform(0.1, 0.9),  # x
            random.uniform(0.1, 0.9),  # y
            random.uniform(0.05, 0.3), # width
            random.uniform(0.05, 0.3), # height
            random.uniform(0.5, 0.99), # confidence
            random.choice(['vehicle', 'pedestrian', 'cyclist'])  # class
        ]
        coordinates.append(detection)
    
    processed_timestamp = time.time()
    
    return [coordinates, processed_timestamp, arrival_time, time.time()]


@SQify
def process_lidar(lidar_package):
    # Process LiDAR point cloud - simulates point cloud segmentation.
    # Timing is based on RIoT edge AI profile for realistic execution.
    import sys, os, time
    global sq_state

    localization = lidar_package[0]
    lidar_frame = lidar_package[1]
    lidar_timestamp = lidar_package[2]
    arrival_time = lidar_package[3]
    
    # Initialize provider for timing information
    if sq_state.get('lidar_provider', None) is None:
        # Ensure evals/ is in path for data_provider import
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from data_provider import get_lidar_provider
        sq_state['lidar_provider'] = get_lidar_provider(0, profile='riot_edge_ai')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'jetson_nano')
    
    # Simulate processing time based on device type
    device_type = sq_state['device_type']
    exec_time_ms = sq_state['lidar_provider'].get_execution_time(device_type)
    time.sleep(exec_time_ms / 1000)
    
    # Generate synthetic processed output
    # Convert raw points to detected objects with confidence
    lidarcoordinates = []
    for point in lidar_frame[:15]:  # Process up to 15 detected objects
        obj = [
            point.get('x', 0),
            point.get('y', 0),
            0.85  # confidence
        ]
        lidarcoordinates.append(obj)
    
    return [localization, lidarcoordinates, lidar_timestamp, arrival_time, time.time()]


# =============================================================================
# FUSION SQs
# =============================================================================

@SQify
def local_fusion(processed_camera, processed_lidar, cav_num):
    # Fuse camera and LiDAR detections from a single CAV.
    # Combines detections in vehicle-local coordinate frame.
    import sys, os, time
    global sq_state
    
    # Initialize timing
    if sq_state.get('fusion_init', None) is None:
        # Ensure evals/ is in path for data_provider import
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from data_provider import get_timing_profile
        sq_state['timing'] = get_timing_profile('riot_edge_ai')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'jetson_nano')
        sq_state['fusion_init'] = True
    
    # Simulate fusion processing time
    device_type = sq_state['device_type']
    exec_time_ms = sq_state['timing']['fusion_ms'].get(device_type, 30)
    time.sleep(exec_time_ms / 1000)
    
    # Extract inputs
    cam_detections = processed_camera[0]
    camera_timestamp = processed_camera[1]
    localization = processed_lidar[0]
    lidar_detections = processed_lidar[1]
    lidar_timestamp = processed_lidar[2]
    
    # Positional offsets for vehicles (CAV spacing)
    localization_offsets = [[-.75, 0.0, 0.], [-1.5, 0.0, 0.], [-2.25, 0.0, 0.]]
    loc_offset = localization_offsets[cav_num] if cav_num < len(localization_offsets) else [0, 0, 0]
    
    adjusted_localization = [
        localization[0] + loc_offset[0],
        localization[1] + loc_offset[1],
        localization[2] + loc_offset[2] if len(localization) > 2 else 0
    ]
    
    # Fuse detections (simplified fusion logic)
    fusion_result = []
    
    # Add camera detections (converted to world frame)
    for det in cam_detections:
        world_x = det[0] + adjusted_localization[0]
        world_y = det[1] + adjusted_localization[1]
        fusion_result.append((world_x, world_y, det[4], det[5], 'camera', cav_num))
    
    # Add lidar detections (already in local frame, convert to world)
    for det in lidar_detections:
        world_x = det[0] + adjusted_localization[0]
        world_y = det[1] + adjusted_localization[1]
        fusion_result.append((world_x, world_y, det[2], 'object', 'lidar', cav_num))
    
    time_watch = [
        processed_camera[2], processed_lidar[3],  # arrival times
        processed_camera[3], processed_lidar[4],  # processing end times
        time.time()  # fusion end time
    ]
    
    return [fusion_result, camera_timestamp, adjusted_localization, time_watch]


@SQify
def global_fusion(fusion_result_cav0, fusion_result_cav1, fusion_result_cav2):
    # Global fusion for 3 CAVs at RSU.
    # Combines local fusion results from all vehicles into unified scene.
    import sys, os, time
    global sq_state
    
    # Initialize timing
    if sq_state.get('global_init', None) is None:
        # Ensure evals/ is in path for data_provider import
        evals_path = os.path.join(os.getcwd(), 'evals')
        if evals_path not in sys.path:
            sys.path.insert(0, evals_path)
        from data_provider import get_timing_profile
        sq_state['timing'] = get_timing_profile('riot_edge_ai')
        sq_state['device_type'] = os.environ.get('TTPYTHON_DEVICE_TYPE', 'server_x86')
        sq_state['global_init'] = True
    
    # Simulate global fusion processing time
    device_type = sq_state['device_type']
    exec_time_ms = sq_state['timing']['global_fusion_ms'].get(device_type, 20)
    time.sleep(exec_time_ms / 1000)
    
    # Extract inputs from all CAVs
    cav0_detections = fusion_result_cav0[0]
    cav0_timestamp = fusion_result_cav0[1]
    cav0_loc = fusion_result_cav0[2]
    
    cav1_detections = fusion_result_cav1[0]
    cav1_timestamp = fusion_result_cav1[1]
    cav1_loc = fusion_result_cav1[2]
    
    cav2_detections = fusion_result_cav2[0]
    cav2_timestamp = fusion_result_cav2[1]
    cav2_loc = fusion_result_cav2[2]
    
    # Combine all detections
    all_detections = []
    all_detections.extend([(d[0], d[1], d[2], 'cav0') for d in cav0_detections])
    all_detections.extend([(d[0], d[1], d[2], 'cav1') for d in cav1_detections])
    all_detections.extend([(d[0], d[1], d[2], 'cav2') for d in cav2_detections])
    
    # Simple clustering/deduplication (in real system, would use more sophisticated fusion)
    # For now, just collect unique detections
    results = []
    for det in all_detections:
        results.append([det[0], det[1], det[2]])  # x, y, confidence
    
    # Add vehicle positions
    vehicle_positions = [
        [cav0_loc[0], cav0_loc[1], 'cav0'],
        [cav1_loc[0], cav1_loc[1], 'cav1'],
        [cav2_loc[0], cav2_loc[1], 'cav2']
    ]
    
    return [
        results,
        cav0_timestamp, cav1_timestamp, cav2_timestamp,
        fusion_result_cav0[3], fusion_result_cav1[3], fusion_result_cav2[3],  # time watches
        vehicle_positions,
        time.time()
    ]


# =============================================================================
# OUTPUT SQ
# =============================================================================

@SQify
def write_to_file(fusion_result):
    # Write global fusion results to output file.
    import time
    
    outfile = os.environ.get('TTPYTHON_OUTPUT_FILE', '/tmp/eval_3cav_output.txt')
    
    results = fusion_result[0]
    timestamps = (fusion_result[1], fusion_result[2], fusion_result[3])
    vehicle_positions = fusion_result[7]
    
    with open(outfile, 'a') as f:
        f.write(f"=== Global Fusion @ {time.time()} ===\n")
        f.write(f"CAV timestamps: {timestamps}\n")
        f.write(f"Vehicle positions: {vehicle_positions}\n")
        f.write(f"Detected objects: {len(results)}\n")
        for obj in results[:5]:  # Write first 5 detections
            f.write(f"  {obj}\n")
        f.write("\n")
    
    print(f"[write_to_file] Processed global fusion @ {time.time():.3f}, {len(results)} objects")
    
    return 1


# =============================================================================
# GRAPH DEFINITION
# =============================================================================

@GRAPHify
def eval_3cav_fusion(trigger):
    # 3-CAV sensor fusion evaluation workload.
    # Natural extension of example_3 with one additional vehicle.
    # Uses data_provider for timing-accurate synthetic data.
    A_1 = 1
    with TTClock.root() as root_clock:
        # Collect a timestamp from a clock for STREAMify's periodic firing rule
        start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
        N = 100  # Number of iterations
        stop_time = start_time + (2500000 * N)

        # Create a sampling interval
        sampling_time = VALUES_TO_TTTIME(start_time, stop_time)
        sample_window = COPY_TTTIME(A_1, sampling_time)

        # ===== CAV 0 =====
        with TTConstraint(name="cav0"):
            cav_0 = 0
            cam_sample = camera_sampler(cav_0,
                                        sample_window,
                                        TTClock=root_clock,
                                        TTPeriod=2500000,
                                        TTPhase=0,
                                        TTDataIntervalWidth=1250000)
            lidar_sample = lidar_sampler(cav_0,
                                         sample_window,
                                         TTClock=root_clock,
                                         TTPeriod=2500000,
                                         TTPhase=0,
                                         TTDataIntervalWidth=1250000)
            cam_output = process_camera(cam_sample)
            lidar_output = process_lidar(lidar_sample)
            fusion_result = local_fusion(cam_output, lidar_output, cav_0)

        # ===== CAV 1 =====
        with TTConstraint(name="cav1"):
            cav_1 = 1
            cam_sample2 = camera_sampler(cav_1,
                                         sample_window,
                                         TTClock=root_clock,
                                         TTPeriod=2500000,
                                         TTPhase=0,
                                         TTDataIntervalWidth=1250000)
            lidar_sample2 = lidar_sampler(cav_1,
                                          sample_window,
                                          TTClock=root_clock,
                                          TTPeriod=2500000,
                                          TTPhase=0,
                                          TTDataIntervalWidth=1250000)
            cam_output2 = process_camera(cam_sample2)
            lidar_output2 = process_lidar(lidar_sample2)
            fusion_result2 = local_fusion(cam_output2, lidar_output2, cav_1)

        # ===== CAV 2 =====
        with TTConstraint(name="cav2"):
            cav_2 = 2
            cam_sample3 = camera_sampler(cav_2,
                                         sample_window,
                                         TTClock=root_clock,
                                         TTPeriod=2500000,
                                         TTPhase=0,
                                         TTDataIntervalWidth=1250000)
            lidar_sample3 = lidar_sampler(cav_2,
                                          sample_window,
                                          TTClock=root_clock,
                                          TTPeriod=2500000,
                                          TTPhase=0,
                                          TTDataIntervalWidth=1250000)
            cam_output3 = process_camera(cam_sample3)
            lidar_output3 = process_lidar(lidar_sample3)
            fusion_result3 = local_fusion(cam_output3, lidar_output3, cav_2)

        # ===== Global Fusion at RSU =====
        with TTConstraint(name="rsu"):
            global_fusion_result = global_fusion(fusion_result, fusion_result2, fusion_result3)
            result = write_to_file(global_fusion_result)