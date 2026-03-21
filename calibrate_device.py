#!/usr/bin/env python3
"""
TTPython Device Calibration Script
===================================

Measures actual execution times for RIoTBench task primitives on the current
device. Produces an empirically calibrated device profile for use in QPF
evaluation experiments.

Usage:
    python calibrate_device.py --name my_pc --type server_x86 --iterations 2000
    python calibrate_device.py --name edge_vm --type raspberry_pi_4 --iterations 2000
    python calibrate_device.py --name mid_vm --type intel_nuc_i5 --iterations 2000

Output:
    calibration_results_{name}.json   — raw measurement data
    profile_{name}.yaml               — calibrated device profile for TTPython

The script performs actual computational work matching each RIoTBench task
category (parsing, filtering, statistics, prediction, I/O simulation) so that
measured times reflect genuine device performance differences.
"""

import argparse
import json
import yaml
import time
import math
import random
import hashlib
import struct
import statistics
import platform
import os
import sys
from collections import defaultdict
from datetime import datetime


# =============================================================================
# COMPUTATIONAL KERNELS
# =============================================================================
# Each kernel performs work representative of its RIoTBench task category.
# The goal is NOT to replicate exact RIoTBench implementations, but to create
# workloads whose execution time scales realistically with device performance.
#
# Kernels are calibrated so that on a Raspberry Pi 4 (or equivalent slow device),
# execution times roughly match the RIoTBench paper's reported throughputs.
# Faster devices will naturally execute these faster, giving us real speed ratios.
# =============================================================================


def _generate_senml_payload(n_fields=20):
    """Generate a realistic SenML-like JSON payload."""
    return json.dumps({
        "bn": f"urn:dev:mac:0024befffe80{random.randint(1000,9999)}",
        "bt": time.time(),
        "e": [
            {
                "n": f"sensor_{i}",
                "u": random.choice(["Cel", "%RH", "Pa", "lx", "dB"]),
                "v": random.uniform(-40, 120),
                "t": random.uniform(-10, 0)
            }
            for i in range(n_fields)
        ]
    })


def _generate_csv_row(n_fields=15):
    """Generate a CSV row similar to IoT sensor data."""
    fields = [str(time.time())]
    fields.append(f"sensor_{random.randint(1, 1000)}")
    fields.extend([f"{random.uniform(-100, 100):.6f}" for _ in range(n_fields)])
    return ",".join(fields)


def _generate_xml_payload(n_elements=30):
    """Generate an XML-like string payload (CPU-intensive to parse)."""
    elements = []
    for i in range(n_elements):
        elements.append(
            f'<observation id="{i}" type="sensor_{i % 5}">'
            f'<value>{random.uniform(0, 100):.8f}</value>'
            f'<timestamp>{time.time()}</timestamp>'
            f'<quality>{random.randint(0, 100)}</quality>'
            f'<metadata key="unit" value="{random.choice(["C", "F", "Pa", "lx"])}"/>'
            f'</observation>'
        )
    return f'<sensorData xmlns="iot:senml">{" ".join(elements)}</sensorData>'


# --- Parse Tasks ---

def kernel_senml_parse(payload):
    """Parse SenML JSON and extract sensor values."""
    data = json.loads(payload)
    results = []
    for entry in data.get("e", []):
        results.append({
            "name": entry.get("n"),
            "value": entry.get("v", 0) * 1.0,
            "unit": entry.get("u"),
            "time": data.get("bt", 0) + entry.get("t", 0)
        })
    return results


def kernel_xml_parse(payload):
    """Parse XML-like payload by string scanning (CPU-intensive)."""
    results = []
    pos = 0
    while True:
        start = payload.find('<observation', pos)
        if start == -1:
            break
        end = payload.find('</observation>', start)
        if end == -1:
            break
        fragment = payload[start:end]

        # Extract value
        v_start = fragment.find('<value>') + 7
        v_end = fragment.find('</value>')
        value = float(fragment[v_start:v_end]) if v_start > 6 and v_end > 0 else 0.0

        # Extract timestamp
        t_start = fragment.find('<timestamp>') + 11
        t_end = fragment.find('</timestamp>')
        ts = float(fragment[t_start:t_end]) if t_start > 10 and t_end > 0 else 0.0

        # Extract quality
        q_start = fragment.find('<quality>') + 9
        q_end = fragment.find('</quality>')
        quality = int(fragment[q_start:q_end]) if q_start > 8 and q_end > 0 else 0

        results.append({"value": value, "timestamp": ts, "quality": quality})
        pos = end + 14
    return results


def kernel_csv_to_senml(csv_row):
    """Convert CSV row to SenML-like dict."""
    fields = csv_row.split(",")
    timestamp = float(fields[0])
    sensor_id = fields[1]
    values = [float(f) for f in fields[2:]]
    return {
        "bn": f"urn:dev:{sensor_id}",
        "bt": timestamp,
        "e": [{"n": f"field_{i}", "v": v} for i, v in enumerate(values)]
    }


# --- Filter Tasks ---

def kernel_bloom_filter(value, n_hashes=8, filter_size=1024):
    """Simulate Bloom filter membership check with multiple hash functions."""
    bit_array = [0] * filter_size
    data = str(value).encode()
    for i in range(n_hashes):
        h = hashlib.md5(data + struct.pack('I', i)).digest()
        idx = int.from_bytes(h[:4], 'little') % filter_size
        bit_array[idx] = 1
    return all(bit_array[int.from_bytes(
        hashlib.md5(data + struct.pack('I', i)).digest()[:4], 'little'
    ) % filter_size] for i in range(n_hashes))


def kernel_range_filter(value, low=10.0, high=90.0):
    """Range filter with type conversion and boundary checks."""
    v = float(value)
    # Simulate field-based filtering across multiple attributes
    checks = []
    for offset in range(8):
        adjusted = v + offset * 0.1
        checks.append(low <= adjusted <= high)
    return any(checks)


# --- Statistical Tasks ---

def kernel_average(window_data):
    """Compute windowed average over sensor stream."""
    n = len(window_data)
    if n == 0:
        return 0.0
    total = sum(window_data)
    mean = total / n
    # Also compute variance for quality metric
    variance = sum((x - mean) ** 2 for x in window_data) / n
    return {"mean": mean, "variance": variance, "std": math.sqrt(variance), "count": n}


def kernel_accumulator(values):
    """Accumulate and aggregate message batches."""
    accumulated = defaultdict(list)
    for v in values:
        key = int(v * 10) % 20
        accumulated[key].append(v)
    return {k: sum(vs) / len(vs) for k, vs in accumulated.items()}


def kernel_kalman_filter(measurements, process_var=0.01, measurement_var=0.1):
    """1D Kalman filter over measurement series."""
    estimate = measurements[0]
    error_est = 1.0
    estimates = []
    for z in measurements:
        # Predict
        error_est += process_var
        # Update
        kalman_gain = error_est / (error_est + measurement_var)
        estimate = estimate + kalman_gain * (z - estimate)
        error_est = (1 - kalman_gain) * error_est
        estimates.append(estimate)
    return estimates


def kernel_distinct_count(values, n_hashes=16):
    """HyperLogLog-style approximate distinct count."""
    max_zeros = [0] * n_hashes
    for v in values:
        data = str(v).encode()
        h = int(hashlib.sha256(data).hexdigest(), 16)
        bucket = h % n_hashes
        bits = h >> 4
        zeros = 0
        while bits and (bits & 1) == 0:
            zeros += 1
            bits >>= 1
        max_zeros[bucket] = max(max_zeros[bucket], zeros)
    harmonic_mean = n_hashes / sum(2 ** (-m) for m in max_zeros)
    return 0.7213 / (1 + 1.079 / n_hashes) * n_hashes * harmonic_mean


def kernel_second_order_moment(values):
    """Compute second-order statistical moment (AMS sketch style)."""
    n = len(values)
    mean = sum(values) / n
    m2 = sum((x - mean) ** 2 for x in values) / n
    m3 = sum((x - mean) ** 3 for x in values) / n
    m4 = sum((x - mean) ** 4 for x in values) / n
    skewness = m3 / (m2 ** 1.5) if m2 > 0 else 0
    kurtosis = m4 / (m2 ** 2) if m2 > 0 else 0
    return {"moment2": m2, "skewness": skewness, "kurtosis": kurtosis}


# --- Predictive Tasks ---

def kernel_decision_tree_classify(features, depth=8):
    """Simulate decision tree traversal for classification."""
    node = 0
    random.seed(int(features[0] * 1000) if features else 42)
    for level in range(depth):
        feature_idx = node % len(features)
        threshold = (node * 0.31415 + level * 0.27183) % 1.0
        if features[feature_idx] < threshold:
            node = 2 * node + 1
        else:
            node = 2 * node + 2
    # Leaf computation — weighted vote
    class_scores = [0.0] * 5
    for i, f in enumerate(features):
        class_scores[i % 5] += f * (node % (i + 1) + 1)
    return class_scores.index(max(class_scores))


def kernel_decision_tree_train(data_matrix, labels, n_trees=5, max_depth=6):
    """Simulate ensemble decision tree training (simplified random forest)."""
    n_samples = len(data_matrix)
    n_features = len(data_matrix[0]) if data_matrix else 0
    trees = []
    for t in range(n_trees):
        # Bootstrap sample
        indices = [random.randint(0, n_samples - 1) for _ in range(n_samples)]
        tree = []
        for depth in range(max_depth):
            feat = random.randint(0, n_features - 1)
            vals = [data_matrix[i][feat] for i in indices]
            threshold = sum(vals) / len(vals)
            split = {"feature": feat, "threshold": threshold, "depth": depth}
            # Compute Gini impurity
            left = [labels[i] for i in indices if data_matrix[i][feat] < threshold]
            right = [labels[i] for i in indices if data_matrix[i][feat] >= threshold]
            for group in [left, right]:
                if group:
                    counts = defaultdict(int)
                    for label in group:
                        counts[label] += 1
                    total = len(group)
                    gini = 1.0 - sum((c / total) ** 2 for c in counts.values())
                    split["gini"] = gini
            tree.append(split)
        trees.append(tree)
    return trees


def kernel_interpolation(values, target_size=50):
    """Linear interpolation to fill missing values."""
    n = len(values)
    result = []
    for i in range(target_size):
        pos = i * (n - 1) / (target_size - 1)
        low = int(pos)
        high = min(low + 1, n - 1)
        frac = pos - low
        interpolated = values[low] * (1 - frac) + values[high] * frac
        result.append(interpolated)
    return result


def kernel_linear_regression(features_matrix, targets):
    """Multi-variate linear regression (normal equation approximation)."""
    n = len(features_matrix)
    p = len(features_matrix[0])
    # Compute X^T X (p x p matrix)
    xtx = [[0.0] * p for _ in range(p)]
    xty = [0.0] * p
    for i in range(n):
        for j in range(p):
            xty[j] += features_matrix[i][j] * targets[i]
            for k in range(p):
                xtx[j][k] += features_matrix[i][j] * features_matrix[i][k]
    # Simple diagonal solve (approximation — avoids full inverse)
    coefficients = []
    for j in range(p):
        if xtx[j][j] != 0:
            coefficients.append(xty[j] / xtx[j][j])
        else:
            coefficients.append(0.0)
    # Compute predictions and error
    predictions = []
    for i in range(n):
        pred = sum(features_matrix[i][j] * coefficients[j] for j in range(p))
        predictions.append(pred)
    mse = sum((targets[i] - predictions[i]) ** 2 for i in range(n)) / n
    return {"coefficients": coefficients, "mse": mse}


def kernel_linear_regression_train(data_matrix, targets, epochs=10):
    """Gradient descent training for linear regression."""
    n = len(data_matrix)
    p = len(data_matrix[0])
    weights = [0.0] * p
    lr = 0.001
    for epoch in range(epochs):
        gradients = [0.0] * p
        total_loss = 0.0
        for i in range(n):
            pred = sum(data_matrix[i][j] * weights[j] for j in range(p))
            error = pred - targets[i]
            total_loss += error ** 2
            for j in range(p):
                gradients[j] += error * data_matrix[i][j]
        for j in range(p):
            weights[j] -= lr * gradients[j] / n
    return {"weights": weights, "final_loss": total_loss / n}


def kernel_sliding_linear_reg(values, window_size=20):
    """Sliding window linear regression."""
    results = []
    for i in range(len(values) - window_size + 1):
        window = values[i:i + window_size]
        n = len(window)
        x_mean = (n - 1) / 2.0
        y_mean = sum(window) / n
        numerator = sum((j - x_mean) * (window[j] - y_mean) for j in range(n))
        denominator = sum((j - x_mean) ** 2 for j in range(n))
        slope = numerator / denominator if denominator != 0 else 0
        intercept = y_mean - slope * x_mean
        results.append({"slope": slope, "intercept": intercept})
    return results


# --- I/O Simulation Tasks ---
# These simulate the computational overhead of I/O operations (serialization,
# checksumming, protocol framing) without actual network/cloud calls.

def kernel_io_light(data, iterations=50):
    """Simulate light I/O: MQTT publish, subscribe (serialize + checksum)."""
    payload = json.dumps(data)
    encoded = payload.encode('utf-8')
    for _ in range(iterations):
        checksum = hashlib.md5(encoded).hexdigest()
    return checksum


def kernel_io_medium(data, iterations=200):
    """Simulate medium I/O: table insert, blob download (larger serialization)."""
    payload = json.dumps(data)
    encoded = payload.encode('utf-8')
    # Simulate batched writes
    results = []
    for i in range(iterations):
        chunk = encoded[i % len(encoded):]
        checksum = hashlib.sha256(chunk).hexdigest()
        results.append(checksum[:8])
    return results


def kernel_io_heavy(data, iterations=1000):
    """Simulate heavy I/O: table range scan, blob upload (large batch processing)."""
    payload = json.dumps(data)
    encoded = payload.encode('utf-8')
    accumulated = b""
    for i in range(iterations):
        accumulated += hashlib.sha256(encoded + struct.pack('I', i)).digest()
    final_hash = hashlib.sha256(accumulated).hexdigest()
    return final_hash


# --- Other Tasks ---

def kernel_annotate(data):
    """Annotate data with metadata."""
    annotated = dict(data) if isinstance(data, dict) else {"value": data}
    annotated["annotation_timestamp"] = time.time()
    annotated["source"] = f"device_{random.randint(1, 100)}"
    annotated["quality_score"] = random.uniform(0.8, 1.0)
    annotated["processing_stage"] = "annotated"
    # Simulate metadata lookup
    for i in range(20):
        key = f"meta_{i}"
        annotated[key] = hashlib.md5(key.encode()).hexdigest()[:8]
    return annotated


def kernel_error_estimate(predictions, actuals):
    """Compute error metrics between predictions and actuals."""
    n = len(predictions)
    mae = sum(abs(predictions[i] - actuals[i]) for i in range(n)) / n
    mse = sum((predictions[i] - actuals[i]) ** 2 for i in range(n)) / n
    rmse = math.sqrt(mse)
    ss_res = sum((actuals[i] - predictions[i]) ** 2 for i in range(n))
    mean_actual = sum(actuals) / n
    ss_tot = sum((actuals[i] - mean_actual) ** 2 for i in range(n))
    r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
    return {"mae": mae, "mse": mse, "rmse": rmse, "r_squared": r_squared}


def kernel_join(left_batch, right_batch):
    """Stream join on key field."""
    # Build hash table on smaller side
    index = {}
    for item in left_batch:
        key = item["key"]
        if key not in index:
            index[key] = []
        index[key].append(item)
    # Probe with larger side
    results = []
    for item in right_batch:
        key = item["key"]
        if key in index:
            for match in index[key]:
                results.append({**match, **item, "joined": True})
    return results


def kernel_plot(data_series):
    """Simulate visualization data preparation (formatting, scaling, binning)."""
    n = len(data_series)
    # Normalize
    min_v = min(data_series)
    max_v = max(data_series)
    range_v = max_v - min_v if max_v != min_v else 1.0
    normalized = [(v - min_v) / range_v for v in data_series]
    # Bin into histogram
    n_bins = 50
    bins = [0] * n_bins
    for v in normalized:
        idx = min(int(v * n_bins), n_bins - 1)
        bins[idx] += 1
    # Compute running stats for each "pixel row"
    rows = []
    window = 10
    for i in range(0, n - window, window):
        chunk = data_series[i:i + window]
        rows.append({
            "mean": sum(chunk) / len(chunk),
            "min": min(chunk),
            "max": max(chunk),
            "range": max(chunk) - min(chunk)
        })
    return {"histogram": bins, "rows": rows}


# =============================================================================
# TASK REGISTRY — maps task names to (kernel_function, input_generator)
# =============================================================================

def _make_senml_input():
    return _generate_senml_payload(20)

def _make_xml_input():
    return _generate_xml_payload(30)

def _make_csv_input():
    return _generate_csv_row(15)

def _make_float_value():
    return random.uniform(0, 100)

def _make_window_data():
    return [random.uniform(0, 100) for _ in range(100)]

def _make_measurements():
    return [random.gauss(50, 10) for _ in range(50)]

def _make_feature_vector():
    return [random.uniform(0, 1) for _ in range(12)]

def _make_training_data():
    n, p = 100, 8
    matrix = [[random.gauss(0, 1) for _ in range(p)] for _ in range(n)]
    labels = [random.randint(0, 4) for _ in range(n)]
    targets = [sum(row) + random.gauss(0, 0.1) for row in matrix]
    return matrix, labels, targets

def _make_io_data():
    return {"sensor_id": f"s{random.randint(1,1000)}", "readings": [random.uniform(0, 100) for _ in range(50)]}

def _make_join_batches():
    keys = list(range(50))
    left = [{"key": random.choice(keys), "value_l": random.uniform(0, 100)} for _ in range(80)]
    right = [{"key": random.choice(keys), "value_r": random.uniform(0, 100)} for _ in range(80)]
    return left, right

def _make_predictions_actuals():
    actuals = [random.gauss(50, 10) for _ in range(100)]
    predictions = [a + random.gauss(0, 2) for a in actuals]
    return predictions, actuals

def _make_data_series():
    return [math.sin(i * 0.1) * 50 + random.gauss(50, 5) for i in range(500)]


TASK_REGISTRY = {
    # Parse tasks
    'senml_parse': {
        'kernel': lambda inp: kernel_senml_parse(inp),
        'gen_input': _make_senml_input,
        'category': 'parse',
    },
    'xml_parse': {
        'kernel': lambda inp: kernel_xml_parse(inp),
        'gen_input': _make_xml_input,
        'category': 'parse',
    },
    'csv_to_senml': {
        'kernel': lambda inp: kernel_csv_to_senml(inp),
        'gen_input': _make_csv_input,
        'category': 'parse',
    },

    # Filter tasks
    'bloom_filter': {
        'kernel': lambda inp: kernel_bloom_filter(inp),
        'gen_input': _make_float_value,
        'category': 'filter',
    },
    'range_filter': {
        'kernel': lambda inp: kernel_range_filter(inp),
        'gen_input': _make_float_value,
        'category': 'filter',
    },

    # Statistical tasks
    'average': {
        'kernel': lambda inp: kernel_average(inp),
        'gen_input': _make_window_data,
        'category': 'statistics',
    },
    'accumulator': {
        'kernel': lambda inp: kernel_accumulator(inp),
        'gen_input': _make_window_data,
        'category': 'statistics',
    },
    'kalman_filter': {
        'kernel': lambda inp: kernel_kalman_filter(inp),
        'gen_input': _make_measurements,
        'category': 'statistics',
    },
    'distinct_count': {
        'kernel': lambda inp: kernel_distinct_count(inp),
        'gen_input': _make_window_data,
        'category': 'statistics',
    },
    'second_order_moment': {
        'kernel': lambda inp: kernel_second_order_moment(inp),
        'gen_input': _make_window_data,
        'category': 'statistics',
    },

    # Predictive tasks
    'decision_tree_classify': {
        'kernel': lambda inp: kernel_decision_tree_classify(inp),
        'gen_input': _make_feature_vector,
        'category': 'predictive',
    },
    'decision_tree_train': {
        'kernel': lambda inp: kernel_decision_tree_train(inp[0], inp[1]),
        'gen_input': lambda: _make_training_data()[:2],  # (matrix, labels)
        'category': 'predictive_heavy',
    },
    'interpolation': {
        'kernel': lambda inp: kernel_interpolation(inp),
        'gen_input': _make_measurements,
        'category': 'predictive',
    },
    'linear_regression': {
        'kernel': lambda inp: kernel_linear_regression(inp[0], inp[1]),
        'gen_input': lambda: (_make_training_data()[0], _make_training_data()[2]),
        'category': 'predictive',
    },
    'linear_regression_train': {
        'kernel': lambda inp: kernel_linear_regression_train(inp[0], inp[1], epochs=10),
        'gen_input': lambda: (_make_training_data()[0], _make_training_data()[2]),
        'category': 'predictive_heavy',
    },
    'sliding_linear_reg': {
        'kernel': lambda inp: kernel_sliding_linear_reg(inp, window_size=20),
        'gen_input': _make_measurements,
        'category': 'predictive',
    },

    # I/O tasks (simulated — computational overhead only)
    'mqtt_publish': {
        'kernel': lambda inp: kernel_io_light(inp, iterations=50),
        'gen_input': _make_io_data,
        'category': 'io_light',
    },
    'mqtt_subscribe': {
        'kernel': lambda inp: kernel_io_light(inp, iterations=30),
        'gen_input': _make_io_data,
        'category': 'io_light',
    },
    'azure_table_insert': {
        'kernel': lambda inp: kernel_io_medium(inp, iterations=200),
        'gen_input': _make_io_data,
        'category': 'io_medium',
    },
    'azure_blob_download': {
        'kernel': lambda inp: kernel_io_medium(inp, iterations=300),
        'gen_input': _make_io_data,
        'category': 'io_medium',
    },
    'azure_blob_upload': {
        'kernel': lambda inp: kernel_io_medium(inp, iterations=400),
        'gen_input': _make_io_data,
        'category': 'io_medium',
    },
    'azure_table_range': {
        'kernel': lambda inp: kernel_io_heavy(inp, iterations=1000),
        'gen_input': _make_io_data,
        'category': 'io_heavy',
    },

    # Visualization
    'plot': {
        'kernel': lambda inp: kernel_plot(inp),
        'gen_input': _make_data_series,
        'category': 'visualization',
    },

    # Annotation
    'annotate': {
        'kernel': lambda inp: kernel_annotate(inp),
        'gen_input': _make_io_data,
        'category': 'annotation',
    },

    # Error/aggregation
    'error_estimate': {
        'kernel': lambda inp: kernel_error_estimate(inp[0], inp[1]),
        'gen_input': _make_predictions_actuals,
        'category': 'statistics',
    },
    'join': {
        'kernel': lambda inp: kernel_join(inp[0], inp[1]),
        'gen_input': _make_join_batches,
        'category': 'aggregation',
    },
}


# =============================================================================
# BENCHMARKING ENGINE
# =============================================================================

def benchmark_task(task_name, task_info, iterations, warmup=50):
    """
    Benchmark a single task type.

    Returns dict with timing statistics in milliseconds.
    """
    kernel = task_info['kernel']
    gen_input = task_info['gen_input']

    # Warmup — let JIT/caches stabilize
    for _ in range(warmup):
        inp = gen_input()
        kernel(inp)

    # Actual measurement
    times_ns = []
    for i in range(iterations):
        inp = gen_input()

        start = time.perf_counter_ns()
        result = kernel(inp)
        end = time.perf_counter_ns()

        elapsed_ns = end - start
        times_ns.append(elapsed_ns)

        # Prevent dead code elimination — use result
        if result is None:
            pass

    # Convert to milliseconds
    times_ms = [t / 1_000_000 for t in times_ns]

    # Statistics
    times_sorted = sorted(times_ms)
    n = len(times_sorted)

    return {
        'task': task_name,
        'category': task_info['category'],
        'iterations': iterations,
        'mean_ms': statistics.mean(times_ms),
        'median_ms': statistics.median(times_ms),
        'std_ms': statistics.stdev(times_ms) if n > 1 else 0.0,
        'min_ms': times_sorted[0],
        'max_ms': times_sorted[-1],
        'p5_ms': times_sorted[int(n * 0.05)],
        'p25_ms': times_sorted[int(n * 0.25)],
        'p75_ms': times_sorted[int(n * 0.75)],
        'p95_ms': times_sorted[int(n * 0.95)],
        'p99_ms': times_sorted[int(n * 0.99)],
        'raw_times_ms': times_ms,  # For detailed analysis
    }


def get_system_info():
    """Collect system information for the calibration report."""
    info = {
        'platform': platform.platform(),
        'processor': platform.processor(),
        'machine': platform.machine(),
        'python_version': platform.python_version(),
        'cpu_count_logical': os.cpu_count(),
    }

    # Try to get more detailed CPU info on Linux
    try:
        with open('/proc/cpuinfo', 'r') as f:
            for line in f:
                if 'model name' in line:
                    info['cpu_model'] = line.split(':')[1].strip()
                    break
    except FileNotFoundError:
        pass

    # Memory info
    try:
        with open('/proc/meminfo', 'r') as f:
            for line in f:
                if 'MemTotal' in line:
                    mem_kb = int(line.split()[1])
                    info['memory_total_gb'] = round(mem_kb / 1024 / 1024, 1)
                    break
    except FileNotFoundError:
        pass

    return info


# =============================================================================
# PROFILE GENERATION
# =============================================================================

def generate_profile_yaml(device_name, device_type, results, system_info):
    """
    Generate a calibrated device profile YAML from benchmark results.

    The profile uses actual measured execution times instead of theoretical
    estimates, making QPF decisions empirically grounded.
    """
    # Compute cpu_speed relative to RPi4 baseline estimates
    # We compare our measured times to the RPi4 reference times from riotbench_provider
    RPI4_REFERENCE = {
        'senml_parse': 0.15, 'xml_parse': 3.2, 'csv_to_senml': 0.2,
        'bloom_filter': 0.015, 'range_filter': 0.015,
        'average': 0.02, 'accumulator': 0.015, 'kalman_filter': 0.02,
        'distinct_count': 0.015, 'second_order_moment': 0.1,
        'decision_tree_classify': 0.25, 'interpolation': 0.15,
        'linear_regression': 0.3, 'sliding_linear_reg': 0.5,
        'annotate': 0.02, 'error_estimate': 0.1, 'join': 0.05,
    }

    # Compute speed ratios for compute-bound tasks only (skip I/O)
    speed_ratios = []
    for task, rpi4_time in RPI4_REFERENCE.items():
        if task in results:
            measured = results[task]['median_ms']
            if measured > 0:
                ratio = rpi4_time / measured  # >1 means faster than RPi4
                speed_ratios.append(ratio)

    avg_cpu_speed = statistics.median(speed_ratios) if speed_ratios else 1.0

    # Build per-task execution time profile
    task_times = {}
    for task_name, result in results.items():
        task_times[task_name] = {
            'mean_ms': round(result['mean_ms'], 6),
            'median_ms': round(result['median_ms'], 6),
            'p95_ms': round(result['p95_ms'], 6),
        }

    # Get power profile from device_types.yaml reference (or use defaults)
    POWER_DEFAULTS = {
        'raspberry_pi_4': {'idle': 2.5, 'active': 5.0, 'tx': 3.0, 'rx': 2.5},
        'intel_nuc_i5': {'idle': 8.0, 'active': 15.0, 'tx': 3.0, 'rx': 3.0},
        'server_x86': {'idle': 50.0, 'active': 100.0, 'tx': 5.0, 'rx': 5.0},
        'cloud_vm': {'idle': 0.0, 'active': 20.0, 'tx': 2.0, 'rx': 2.0},
    }
    power = POWER_DEFAULTS.get(device_type, {'idle': 5.0, 'active': 15.0, 'tx': 3.0, 'rx': 3.0})

    profile = {
        'device_profile': {
            'name': device_name,
            'type': device_type,
            'description': f"Calibrated profile for {device_name} ({system_info.get('cpu_model', 'unknown CPU')})",
            'calibration_date': datetime.now().isoformat(),
            'system_info': system_info,
            'cpu_speed': round(avg_cpu_speed, 2),
            'memory_size': int(system_info.get('memory_total_gb', 4) * 1024 * 1024 * 1024),
            'power_idle': power['idle'],
            'power_active': power['active'],
            'power_transmit': power['tx'],
            'power_receive': power['rx'],
            'supports_concurrent': system_info.get('cpu_count_logical', 1) >= 4,
            'task_execution_times_ms': task_times,
        }
    }

    return profile


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="TTPython Device Calibration — Benchmark RIoTBench tasks on this device",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python calibrate_device.py --name my_pc --type server_x86
    python calibrate_device.py --name edge_vm --type raspberry_pi_4 --iterations 1000
    python calibrate_device.py --name mid_vm --type intel_nuc_i5 --iterations 2000
        """
    )
    parser.add_argument('--name', required=True, help='Device name (e.g., my_pc, edge_vm, mid_vm)')
    parser.add_argument('--type', required=True,
                       choices=['raspberry_pi_4', 'raspberry_pi_3', 'jetson_nano',
                                'intel_nuc_i5', 'server_x86', 'cloud_vm', 'cloud_vm_large'],
                       help='Device type for power profile reference')
    parser.add_argument('--iterations', type=int, default=2000,
                       help='Number of iterations per task (default: 2000)')
    parser.add_argument('--tasks', nargs='*', default=None,
                       help='Specific tasks to benchmark (default: all)')
    parser.add_argument('--output-dir', default='.', help='Output directory for results')

    args = parser.parse_args()

    print("=" * 70)
    print(f"  TTPython Device Calibration")
    print(f"  Device: {args.name} (type: {args.type})")
    print(f"  Iterations per task: {args.iterations}")
    print("=" * 70)

    # Collect system info
    system_info = get_system_info()
    print(f"\n  CPU: {system_info.get('cpu_model', system_info.get('processor', 'unknown'))}")
    print(f"  Cores: {system_info.get('cpu_count_logical', '?')}")
    print(f"  RAM: {system_info.get('memory_total_gb', '?')} GB")
    print(f"  Python: {system_info['python_version']}")
    print()

    # Select tasks
    tasks_to_run = args.tasks if args.tasks else list(TASK_REGISTRY.keys())

    # Run benchmarks
    results = {}
    total = len(tasks_to_run)

    for i, task_name in enumerate(tasks_to_run):
        if task_name not in TASK_REGISTRY:
            print(f"  [{i+1}/{total}] SKIP {task_name} — not in registry")
            continue

        task_info = TASK_REGISTRY[task_name]
        print(f"  [{i+1}/{total}] Benchmarking {task_name} ({task_info['category']})...", end='', flush=True)

        result = benchmark_task(task_name, task_info, args.iterations)
        results[task_name] = result

        print(f"  mean={result['mean_ms']:.4f}ms  median={result['median_ms']:.4f}ms  "
              f"p95={result['p95_ms']:.4f}ms  std={result['std_ms']:.4f}ms")

    # Generate outputs
    os.makedirs(args.output_dir, exist_ok=True)

    # 1. Raw results JSON (without raw_times to keep file size reasonable)
    results_export = {}
    for task_name, result in results.items():
        r = dict(result)
        del r['raw_times_ms']  # Exclude raw data from JSON (too large)
        results_export[task_name] = r

    results_path = os.path.join(args.output_dir, f"calibration_results_{args.name}.json")
    with open(results_path, 'w') as f:
        json.dump({
            'device_name': args.name,
            'device_type': args.type,
            'system_info': system_info,
            'calibration_date': datetime.now().isoformat(),
            'iterations': args.iterations,
            'results': results_export,
        }, f, indent=2)
    print(f"\n  Raw results → {results_path}")

    # 2. Calibrated profile YAML
    profile = generate_profile_yaml(args.name, args.type, results, system_info)
    profile_path = os.path.join(args.output_dir, f"profile_{args.name}.yaml")
    with open(profile_path, 'w') as f:
        yaml.dump(profile, f, default_flow_style=False, sort_keys=False)
    print(f"  Device profile → {profile_path}")

    # 3. Summary table
    print(f"\n{'=' * 70}")
    print(f"  CALIBRATION SUMMARY — {args.name}")
    print(f"{'=' * 70}")
    print(f"  {'Task':<30} {'Category':<16} {'Mean (ms)':>10} {'Median':>10} {'P95':>10}")
    print(f"  {'-' * 76}")

    for task_name in sorted(results.keys(), key=lambda t: results[t]['median_ms']):
        r = results[task_name]
        print(f"  {task_name:<30} {r['category']:<16} {r['mean_ms']:>10.4f} "
              f"{r['median_ms']:>10.4f} {r['p95_ms']:>10.4f}")

    cpu_speed = profile['device_profile']['cpu_speed']
    print(f"\n  Computed cpu_speed (relative to RPi4): {cpu_speed:.2f}x")
    print(f"  (>1.0 = faster than RPi4, <1.0 = slower)")
    print(f"{'=' * 70}")


if __name__ == '__main__':
    main()
