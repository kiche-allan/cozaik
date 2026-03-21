# Copyright 2025 TTPython Extensions - RIoTBench Data Provider
#
# Execution time profiles and data replay for RIoTBench micro-benchmarks.
# Based on "RIoTBench: A Real-time IoT Benchmark for Distributed Stream
# Processing" — Shukla, Chaturvedi, Simmhan (2017)
#
# This provider does two things:
#   1. Replays real SenML CSV data from the RIoTBench sample datasets
#   2. Provides calibrated execution time estimates per task per device
#
# Dataset CSV files are expected in evals/resources/ relative to cwd.

import random
import json
import os


# ============================================================================
# DATASET CONFIGURATION — extracted from RIoTBench tasks_*.properties
# ============================================================================

DATASET_CONFIGS = {
    'TAXI': {
        'csv_file': 'TAXI_sample_data_senml.csv',
        'id_field': 'taxi_identifier',
        'id_type': 'sv',
        'range_filter': {
            'trip_time_in_secs': (140, 3155),
            'trip_distance': (1.37, 29.86),
            'fare_amount': (6.00, 201.00),
            'tip_amount': (0.65, 38.55),
            'tolls_amount': (2.50, 18.00),
        },
        'numeric_fields': [
            'trip_time_in_secs', 'trip_distance', 'fare_amount',
            'surcharge', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount',
        ],
        'interpolation_fields': ['trip_time_in_secs', 'trip_distance'],
        'meta_fields': [
            'pickup_datetime', 'timestamp', 'pickup_longitude', 'pickup_latitude',
            'dropoff_longitude', 'dropoff_latitude', 'payment_type',
        ],
        'dt_features': ['trip_time_in_secs', 'trip_distance', 'fare_amount'],
        'dt_classes': ['Bad', 'Good', 'VeryGood'],
        'dt_result_index': 3,
        'lr_target': 'total_amount',
        'annotation_file': 'taxi-metadata-fulldataset.txt',
        'annotation_format': 'id:company,driver,city',
        'kalman_process_noise': 0.125,
        'kalman_sensor_noise': 0.32,
        'kalman_estimated_error': 30,
        'block_avg_fields': [
            'trip_time_in_secs', 'trip_distance', 'fare_amount',
            'surcharge', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount',
        ],
    },
    'SYS': {
        'csv_file': 'SYS_sample_data_senml.csv',
        'id_field': 'source',
        'id_type': 'sv',
        'range_filter': {
            'temperature': (-12.5, 43.1),
            'humidity': (10.7, 95.2),
            'light': (1345, 26282),
            'dust': (186.61, 5188.21),
            'airquality_raw': (17, 363),
        },
        'numeric_fields': ['temperature', 'humidity', 'light', 'dust', 'airquality_raw'],
        'interpolation_fields': ['temperature', 'humidity', 'light', 'dust', 'airquality_raw'],
        'meta_fields': ['timestamp', 'source', 'longitude', 'latitude'],
        'dt_features': ['temperature', 'humidity', 'light', 'dust', 'airquality_raw'],
        'dt_classes': ['Bad', 'Average', 'Good', 'VeryGood', 'Excellent'],
        'dt_result_index': 6,
        'lr_target': 'airquality_raw',
        'annotation_file': 'city-metadata.txt',
        'annotation_format': 'id:location,type',
        'kalman_process_noise': 0.125,
        'kalman_sensor_noise': 0.32,
        'kalman_estimated_error': 30,
        'block_avg_fields': ['temperature', 'humidity', 'light', 'dust', 'airquality_raw'],
    },
    'FIT': {
        'csv_file': 'FIT_sample_data_senml.csv',
        'id_field': 'subjectId',
        'id_type': 'sv',
        'range_filter': {
            'acc_chest_x': (-13.931, 4.123),
            'acc_chest_y': (-4.6376, 5.2361),
            'acc_chest_z': (-8.1881, 7.8786),
            'ecg_lead_1': (-4.9314, 6.1371),
            'ecg_lead_2': (-6.786, 6.6604),
            'acc_ankle_x': (-5.0006, 8.1472),
            'acc_ankle_y': (-14.303, 1.5909),
            'acc_ankle_z': (-8.6234, 8.6958),
            'acc_arm_x': (-9.824, 5.5778),
            'acc_arm_y': (-10.059, 8.506),
            'acc_arm_z': (-6.6739, 9.5725),
        },
        'numeric_fields': [
            'acc_chest_x', 'acc_chest_y', 'acc_chest_z',
            'ecg_lead_1', 'ecg_lead_2',
            'acc_ankle_x', 'acc_ankle_y', 'acc_ankle_z',
            'gyro_ankle_x', 'gyro_ankle_y', 'gyro_ankle_z',
            'magnetometer_ankle_x', 'magnetometer_ankle_y', 'magnetometer_ankle_z',
            'acc_arm_x', 'acc_arm_y', 'acc_arm_z',
            'gyro_arm_x', 'gyro_arm_y', 'gyro_arm_z',
            'magnetometer_arm_x', 'magnetometer_arm_y', 'magnetometer_arm_z',
            'label',
        ],
        'interpolation_fields': [
            'acc_chest_x', 'acc_chest_y', 'acc_chest_z',
            'ecg_lead_1', 'ecg_lead_2',
            'acc_ankle_x', 'acc_ankle_y', 'acc_ankle_z',
            'acc_arm_x', 'acc_arm_y', 'acc_arm_z',
        ],
        'meta_fields': [
            'subjectId', 'timestamp',
            'gyro_ankle_x', 'gyro_ankle_y', 'gyro_ankle_z',
            'magnetometer_ankle_x', 'magnetometer_ankle_y', 'magnetometer_ankle_z',
            'gyro_arm_x', 'gyro_arm_y', 'gyro_arm_z',
            'magnetometer_arm_x', 'magnetometer_arm_y', 'magnetometer_arm_z',
            'label',
        ],
        'dt_features': ['acc_chest_x', 'acc_chest_y', 'acc_chest_z',
                        'ecg_lead_1', 'ecg_lead_2',
                        'acc_ankle_x', 'acc_ankle_y', 'acc_ankle_z',
                        'acc_arm_x', 'acc_arm_y', 'acc_arm_z'],
        'dt_classes': ['Standing', 'Sitting', 'Walking', 'Running'],
        'dt_result_index': 3,
        'lr_target': 'acc_chest_x',
        'annotation_file': 'mhealth_annotation_mapping.csv',
        'annotation_format': 'id:age,gender',
        'kalman_process_noise': 0.125,
        'kalman_sensor_noise': 0.32,
        'kalman_estimated_error': 30,
        'block_avg_fields': [
            'acc_chest_x', 'acc_chest_y', 'acc_chest_z',
            'ecg_lead_1', 'ecg_lead_2',
            'acc_ankle_x', 'acc_ankle_y', 'acc_ankle_z',
            'acc_arm_x', 'acc_arm_y', 'acc_arm_z',
        ],
    },
    'GRID': {
        'csv_file': None,
        'id_field': 'meterid',
        'id_type': 'sv',
        'range_filter': {
            'energyconsumed': (1.828, 36.658),
        },
        'numeric_fields': ['energyconsumed'],
        'interpolation_fields': ['energyconsumed'],
        'meta_fields': ['timestamp', 'meterid'],
        'dt_features': ['energyconsumed'],
        'dt_classes': ['Low', 'Medium', 'High'],
        'dt_result_index': 1,
        'lr_target': 'energyconsumed',
        'annotation_file': None,
        'annotation_format': None,
        'kalman_process_noise': 0.125,
        'kalman_sensor_noise': 0.32,
        'kalman_estimated_error': 30,
        'block_avg_fields': ['energyconsumed'],
    },
}


def get_dataset_config(workload_type):
    return DATASET_CONFIGS.get(workload_type, DATASET_CONFIGS.get('SYS'))


# ============================================================================
# CSV FILE REPLAY GENERATOR — reads real RIoTBench SenML data
# ============================================================================

def _find_resource_path(filename):
    candidates = [
        os.path.join(os.getcwd(), 'evals', 'resources', filename),
        os.path.join(os.getcwd(), 'evals', filename),
        os.path.join(os.path.dirname(__file__), 'resources', filename),
        os.path.join(os.path.dirname(__file__), filename),
    ]
    for path in candidates:
        if os.path.exists(path):
            return path
    return None


class SenMLFileReplayGenerator:
    def __init__(self, workload_type):
        self.workload_type = workload_type
        self.config = DATASET_CONFIGS.get(workload_type)
        self.messages = []
        self.index = 0
        self.msg_count = 0
        csv_file = self.config.get('csv_file') if self.config else None
        if csv_file:
            path = _find_resource_path(csv_file)
            if path:
                self._load_csv(path)
        if not self.messages:
            self._fallback_synthetic()

    def _load_csv(self, path):
        with open(path, 'r', encoding='utf-8-sig') as f:
            for line in f:
                line = line.strip().rstrip('\r')
                if not line:
                    continue
                comma_idx = line.index(',')
                timestamp_str = line[:comma_idx]
                json_str = line[comma_idx + 1:]
                try:
                    msg = json.loads(json_str)
                    msg['_csv_timestamp'] = timestamp_str
                    self.messages.append(msg)
                except json.JSONDecodeError:
                    continue

    def _fallback_synthetic(self):
        config = self.config or {}
        id_field = config.get('id_field', 'sensor_id')
        numeric_fields = config.get('numeric_fields', ['value'])
        for i in range(100):
            entries = [{'n': id_field, 'sv': id_field + '_' + str(random.randint(1, 1000)), 'u': 'string'}]
            for field in numeric_fields:
                entries.append({'n': field, 'v': str(round(random.uniform(0, 100), 4)), 'u': 'unit'})
            self.messages.append({'e': entries, 'bt': 1000000 * (i + 1)})

    def generate_message(self):
        self.msg_count += 1
        if not self.messages:
            return {'e': [], 'bt': 0}
        msg = self.messages[self.index]
        self.index = (self.index + 1) % len(self.messages)
        return msg


# ============================================================================
# SYNTHETIC GENERATOR — fallback for GRID or missing CSV
# ============================================================================

class SenMLSyntheticGenerator:
    def __init__(self, workload_type='GRID'):
        self.workload_type = workload_type
        self.msg_count = 0
        self.config = DATASET_CONFIGS.get(workload_type, {})

    def generate_message(self):
        self.msg_count += 1
        id_field = self.config.get('id_field', 'sensor_id')
        numeric_fields = self.config.get('numeric_fields', ['value'])
        entries = [{'n': id_field, 'sv': id_field + '_' + str(random.randint(1, 10000)), 'u': 'string'}]
        for field in numeric_fields:
            rng = self.config.get('range_filter', {}).get(field, (0, 100))
            val = random.uniform(rng[0], rng[1])
            entries.append({'n': field, 'v': str(round(val, 4)), 'u': 'unit'})
        return {'e': entries, 'bt': 1000000 * self.msg_count}


# ============================================================================
# MODEL PROVIDER — trains sklearn-equivalent models from sample CSV data
# ============================================================================

class RIoTBenchModelProvider:
    def __init__(self, workload_type):
        self.workload_type = workload_type
        self.config = DATASET_CONFIGS.get(workload_type, {})
        self._dt_model = None
        self._lr_model = None
        self._training_data = None
        self._train_models()

    def _extract_features_from_csv(self):
        csv_file = self.config.get('csv_file')
        if not csv_file:
            return [], []
        path = _find_resource_path(csv_file)
        if not path:
            return [], []
        dt_feature_names = self.config.get('dt_features', [])
        lr_target_name = self.config.get('lr_target', '')
        rows = []
        targets = []
        with open(path, 'r', encoding='utf-8-sig') as f:
            for line in f:
                line = line.strip().rstrip('\r')
                if not line:
                    continue
                comma_idx = line.index(',')
                json_str = line[comma_idx + 1:]
                try:
                    msg = json.loads(json_str)
                except json.JSONDecodeError:
                    continue
                field_vals = {}
                for entry in msg.get('e', []):
                    name = entry.get('n', '')
                    raw_v = entry.get('v')
                    if raw_v is not None:
                        try:
                            field_vals[name] = float(raw_v)
                        except (ValueError, TypeError):
                            pass
                features = []
                valid = True
                for fname in dt_feature_names:
                    if fname in field_vals:
                        features.append(field_vals[fname])
                    else:
                        valid = False
                        break
                if valid and features:
                    rows.append(features)
                    target_val = field_vals.get(lr_target_name, 0.0)
                    targets.append(target_val)
        return rows, targets

    def _train_models(self):
        rows, targets = self._extract_features_from_csv()
        if not rows:
            self._dt_model = self._default_dt_model()
            self._lr_model = self._default_lr_model()
            self._training_data = {'features': [], 'targets': []}
            return
        self._training_data = {'features': rows, 'targets': targets}
        self._dt_model = self._train_dt(rows, targets)
        self._lr_model = self._train_lr(rows, targets)

    def _train_dt(self, rows, targets):
        dt_classes = self.config.get('dt_classes', ['Low', 'Medium', 'High'])
        n_classes = len(dt_classes)
        target_sorted = sorted(targets)
        n = len(target_sorted)
        thresholds = []
        for i in range(1, n_classes):
            idx = int(n * i / n_classes)
            if idx < n:
                thresholds.append(target_sorted[idx])
            else:
                thresholds.append(target_sorted[-1])
        feature_idx = len(rows[0]) - 1
        return {
            'type': 'decision_tree',
            'feature_index': feature_idx,
            'thresholds': thresholds,
            'classes': dt_classes,
            'n_features': len(rows[0]),
        }

    def _train_lr(self, rows, targets):
        n = len(rows)
        if n == 0:
            return self._default_lr_model()
        n_features = len(rows[0])
        # OLS: compute coefficients via normal equations (simplified)
        # For numerical stability, use per-feature univariate regression
        # and average, since we don't have numpy
        target_mean = sum(targets) / n
        feature_means = [0.0] * n_features
        for row in rows:
            for i in range(n_features):
                feature_means[i] += row[i]
        feature_means = [m / n for m in feature_means]
        coefficients = [0.0] * n_features
        for i in range(n_features):
            num = 0.0
            den = 0.0
            for j in range(n):
                fi = rows[j][i] - feature_means[i]
                ti = targets[j] - target_mean
                num += fi * ti
                den += fi * fi
            coefficients[i] = num / den if den > 0 else 0.0
        intercept = target_mean - sum(c * m for c, m in zip(coefficients, feature_means))
        # Compute R²
        ss_res = 0.0
        ss_tot = 0.0
        for j in range(n):
            pred = intercept + sum(coefficients[i] * rows[j][i] for i in range(n_features))
            ss_res += (targets[j] - pred) ** 2
            ss_tot += (targets[j] - target_mean) ** 2
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
        return {
            'type': 'linear_regression',
            'coefficients': coefficients,
            'intercept': intercept,
            'r_squared': r_squared,
            'n_features': n_features,
            'feature_means': feature_means,
        }

    def _default_dt_model(self):
        dt_classes = self.config.get('dt_classes', ['Low', 'Medium', 'High'])
        return {
            'type': 'decision_tree',
            'feature_index': 0,
            'thresholds': [33.0, 66.0],
            'classes': dt_classes,
            'n_features': 1,
        }

    def _default_lr_model(self):
        return {
            'type': 'linear_regression',
            'coefficients': [1.0],
            'intercept': 0.0,
            'r_squared': 0.0,
            'n_features': 1,
            'feature_means': [0.0],
        }

    def get_dt_model(self):
        return self._dt_model

    def get_lr_model(self):
        return self._lr_model

    def get_training_batch(self, batch_size=100):
        if not self._training_data or not self._training_data['features']:
            return self._synthetic_batch(batch_size)
        features_pool = self._training_data['features']
        targets_pool = self._training_data['targets']
        dt_classes = self.config.get('dt_classes', ['Low', 'Medium', 'High'])
        thresholds = self._dt_model.get('thresholds', [33, 66])
        batch = []
        for _ in range(batch_size):
            idx = random.randint(0, len(features_pool) - 1)
            features = features_pool[idx]
            target = targets_pool[idx]
            # Classify for DT annotation
            classify_val = features[self._dt_model.get('feature_index', 0)] if features else 0
            cls_label = dt_classes[-1]
            for ti, thr in enumerate(thresholds):
                if classify_val < thr:
                    cls_label = dt_classes[ti]
                    break
            batch.append({
                'features': features,
                'regression_target': target,
                'classification_label': cls_label,
            })
        return batch

    def _synthetic_batch(self, batch_size):
        dt_classes = self.config.get('dt_classes', ['Low', 'Medium', 'High'])
        batch = []
        for _ in range(batch_size):
            features = [random.uniform(0, 100) for _ in range(3)]
            target = sum(features) / len(features) + random.gauss(0, 5)
            batch.append({
                'features': features,
                'regression_target': target,
                'classification_label': random.choice(dt_classes),
            })
        return batch


# ============================================================================
# BLOOM FILTER SEED — extracts real IDs from sample CSV
# ============================================================================

def get_bloom_ids(workload_type):
    config = DATASET_CONFIGS.get(workload_type, {})
    csv_file = config.get('csv_file')
    id_field = config.get('id_field', 'sensor_id')
    if not csv_file:
        return set()
    path = _find_resource_path(csv_file)
    if not path:
        return set()
    ids = set()
    with open(path, 'r', encoding='utf-8-sig') as f:
        for line in f:
            line = line.strip().rstrip('\r')
            if not line:
                continue
            comma_idx = line.index(',')
            json_str = line[comma_idx + 1:]
            try:
                msg = json.loads(json_str)
            except json.JSONDecodeError:
                continue
            for entry in msg.get('e', []):
                if entry.get('n') == id_field:
                    val = entry.get('sv', entry.get('v', ''))
                    if val:
                        ids.add(str(val))
                    break
    return ids


# ============================================================================
# ANNOTATION METADATA LOADER
# ============================================================================

def load_annotation_metadata(workload_type):
    config = DATASET_CONFIGS.get(workload_type, {})
    ann_file = config.get('annotation_file')
    if not ann_file:
        return {}
    path = _find_resource_path(ann_file)
    if not path:
        return {}
    metadata = {}
    with open(path, 'r', encoding='utf-8-sig') as f:
        for line in f:
            line = line.strip().rstrip('\r')
            if not line or ':' not in line:
                continue
            colon_idx = line.index(':')
            key = line[:colon_idx]
            values = line[colon_idx + 1:].split(',')
            fmt = config.get('annotation_format', '')
            if 'company,driver,city' in fmt:
                metadata[key] = {
                    'taxi_company': values[0] if len(values) > 0 else '',
                    'drivername': values[1] if len(values) > 1 else '',
                    'taxi_city': values[2] if len(values) > 2 else '',
                }
            elif 'location,type' in fmt:
                metadata[key] = {
                    'location': values[0] if len(values) > 0 else '',
                    'sensor_type': values[1] if len(values) > 1 else '',
                }
            elif 'age,gender' in fmt:
                metadata[key] = {
                    'age': values[0] if len(values) > 0 else '',
                    'gender': values[1] if len(values) > 1 else '',
                }
            else:
                metadata[key] = {'raw': ','.join(values)}
    return metadata


# ============================================================================
# SENML MESSAGE PARSING UTILITY
# ============================================================================

def parse_senml_message(message, workload_type):
    config = DATASET_CONFIGS.get(workload_type, {})
    id_field = config.get('id_field', 'sensor_id')
    numeric_fields_set = set(config.get('numeric_fields', []))
    entries = message.get('e', [])
    bt = message.get('bt', 0)
    # Handle bt that might be a string
    if isinstance(bt, str):
        try:
            bt = int(bt)
        except ValueError:
            bt = 0
    sensor_id = 'unknown'
    obs_type = 'unknown'
    values = []
    readings = []
    first_numeric = True
    for entry in entries:
        name = entry.get('n', '')
        raw_v = entry.get('v')
        raw_sv = entry.get('sv')
        unit = entry.get('u', '')
        if name == id_field:
            sensor_id = raw_sv if raw_sv else (str(raw_v) if raw_v else 'unknown')
            continue
        # Try to get numeric value
        numeric_val = None
        if raw_v is not None:
            try:
                numeric_val = float(raw_v)
            except (ValueError, TypeError):
                pass
        if numeric_val is None and raw_sv is not None:
            try:
                numeric_val = float(raw_sv)
            except (ValueError, TypeError):
                pass
        reading = {'n': name, 'u': unit}
        if numeric_val is not None:
            reading['v'] = numeric_val
            if name in numeric_fields_set:
                values.append(numeric_val)
            if first_numeric:
                obs_type = name
                first_numeric = False
        if raw_sv is not None:
            reading['sv'] = raw_sv
        readings.append(reading)
    return {
        'sensor_id': sensor_id,
        'obs_type': obs_type,
        'timestamp': bt,
        'values': values,
        'readings': readings,
    }


# ============================================================================
# EXECUTION TIME PROVIDER — unchanged from original (timing simulation)
# ============================================================================

DEVICE_SCALING = {
    'raspberry_pi': 1.0,
    'raspberry_pi_3': 1.0,
    'raspberry_pi_4': 0.8,
    'jetson_nano': 0.6,
    'edge_server': 0.3,
    'cloud_vm': 0.15,
    'server_x86': 0.1,
}

TASK_EXEC_TIMES_MS = {
    'senml_parse': 0.15,
    'xml_parse': 3.2,
    'csv_to_senml': 0.2,
    'bloom_filter': 0.015,
    'range_filter': 0.015,
    'average': 0.02,
    'accumulator': 0.015,
    'kalman_filter': 0.02,
    'distinct_count': 0.015,
    'distinct_approx_count': 0.015,
    'second_order_moment': 0.1,
    'decision_tree_classify': 0.25,
    'decision_tree_train': 15.0,
    'interpolation': 0.15,
    'linear_regression': 0.3,
    'linear_regression_train': 20.0,
    'simple_linear_regression': 0.5,
    'sliding_linear_reg': 0.5,
    'azure_table_insert': 5.0,
    'azure_table_range': 50.0,
    'azure_blob_upload': 10.0,
    'azure_blob_download': 8.0,
    'mqtt_publish': 0.5,
    'mqtt_subscribe': 0.3,
    'plot': 2.0,
    'annotate': 0.02,
    'annotation': 0.02,
    'error_estimate': 0.1,
    'error_estimation': 0.1,
    'join': 0.05,
    'block_window_average': 0.02,
    'parse_project': 0.15,
}


class RIoTBenchProvider:
    def __init__(self, task_type, device_type='raspberry_pi', variance=0.1):
        self.task_type = task_type
        self.device_type = device_type
        self.variance = variance
        self.base_exec_time = TASK_EXEC_TIMES_MS.get(task_type, 0.1)
        self.scale_factor = DEVICE_SCALING.get(device_type, 1.0)

    def get_execution_time(self, device_type=None):
        if device_type is None:
            device_type = self.device_type
        scale = DEVICE_SCALING.get(device_type, 1.0)
        base_time = self.base_exec_time * scale
        if self.variance > 0:
            variance_range = base_time * self.variance
            base_time += random.uniform(-variance_range, variance_range)
        return max(0.001, base_time)


# ============================================================================
# FACTORY FUNCTIONS
# ============================================================================

def get_task_provider(task_type, device_type='raspberry_pi', variance=0.1):
    return RIoTBenchProvider(task_type, device_type, variance)


def get_senml_generator(workload_type='SYS'):
    config = DATASET_CONFIGS.get(workload_type)
    if config and config.get('csv_file'):
        gen = SenMLFileReplayGenerator(workload_type)
        if gen.messages:
            return gen
    return SenMLSyntheticGenerator(workload_type)


def get_model_provider(workload_type='TAXI'):
    return RIoTBenchModelProvider(workload_type)


def get_timing_profile(profile_name='riotbench_edge'):
    profiles = {
        'riotbench_edge': {
            'base_device': 'raspberry_pi',
            'senml_parse_ms': {'raspberry_pi': 0.15, 'jetson_nano': 0.09, 'edge_server': 0.045},
            'xml_parse_ms': {'raspberry_pi': 3.2, 'jetson_nano': 1.9, 'edge_server': 0.96},
            'bloom_filter_ms': {'raspberry_pi': 0.015, 'jetson_nano': 0.009, 'edge_server': 0.0045},
            'range_filter_ms': {'raspberry_pi': 0.015, 'jetson_nano': 0.009, 'edge_server': 0.0045},
            'average_ms': {'raspberry_pi': 0.02, 'jetson_nano': 0.012, 'edge_server': 0.006},
            'kalman_filter_ms': {'raspberry_pi': 0.02, 'jetson_nano': 0.012, 'edge_server': 0.006},
            'sliding_lin_reg_ms': {'raspberry_pi': 0.5, 'jetson_nano': 0.3, 'edge_server': 0.15},
            'interpolation_ms': {'raspberry_pi': 0.15, 'jetson_nano': 0.09, 'edge_server': 0.045},
            'decision_tree_ms': {'raspberry_pi': 0.25, 'jetson_nano': 0.15, 'edge_server': 0.075},
            'linear_reg_ms': {'raspberry_pi': 0.3, 'jetson_nano': 0.18, 'edge_server': 0.09},
            'dt_train_ms': {'raspberry_pi': 15.0, 'jetson_nano': 9.0, 'edge_server': 4.5},
            'lr_train_ms': {'raspberry_pi': 20.0, 'jetson_nano': 12.0, 'edge_server': 6.0},
            'table_insert_ms': {'raspberry_pi': 5.0, 'jetson_nano': 5.0, 'edge_server': 3.0},
            'table_range_ms': {'raspberry_pi': 50.0, 'jetson_nano': 50.0, 'edge_server': 30.0},
            'blob_upload_ms': {'raspberry_pi': 10.0, 'jetson_nano': 10.0, 'edge_server': 6.0},
            'mqtt_publish_ms': {'raspberry_pi': 0.5, 'jetson_nano': 0.5, 'edge_server': 0.3},
            'plot_ms': {'raspberry_pi': 2.0, 'jetson_nano': 1.2, 'edge_server': 0.6},
            'annotate_ms': {'raspberry_pi': 0.02, 'jetson_nano': 0.012, 'edge_server': 0.006},
            'error_estimate_ms': {'raspberry_pi': 0.1, 'jetson_nano': 0.06, 'edge_server': 0.03},
        },
        'riotbench_cloud': {
            'base_device': 'cloud_vm',
            'senml_parse_ms': {'cloud_vm': 0.02, 'server_x86': 0.015},
            'xml_parse_ms': {'cloud_vm': 0.48, 'server_x86': 0.32},
            'bloom_filter_ms': {'cloud_vm': 0.002, 'server_x86': 0.0015},
            'range_filter_ms': {'cloud_vm': 0.002, 'server_x86': 0.0015},
            'average_ms': {'cloud_vm': 0.003, 'server_x86': 0.002},
            'kalman_filter_ms': {'cloud_vm': 0.003, 'server_x86': 0.002},
            'sliding_lin_reg_ms': {'cloud_vm': 0.075, 'server_x86': 0.05},
            'interpolation_ms': {'cloud_vm': 0.02, 'server_x86': 0.015},
            'decision_tree_ms': {'cloud_vm': 0.04, 'server_x86': 0.025},
            'linear_reg_ms': {'cloud_vm': 0.045, 'server_x86': 0.03},
            'dt_train_ms': {'cloud_vm': 2.25, 'server_x86': 1.5},
            'lr_train_ms': {'cloud_vm': 3.0, 'server_x86': 2.0},
            'table_insert_ms': {'cloud_vm': 2.0, 'server_x86': 1.5},
            'table_range_ms': {'cloud_vm': 20.0, 'server_x86': 15.0},
            'blob_upload_ms': {'cloud_vm': 4.0, 'server_x86': 3.0},
            'mqtt_publish_ms': {'cloud_vm': 0.2, 'server_x86': 0.15},
            'plot_ms': {'cloud_vm': 0.3, 'server_x86': 0.2},
            'annotate_ms': {'cloud_vm': 0.003, 'server_x86': 0.002},
            'error_estimate_ms': {'cloud_vm': 0.015, 'server_x86': 0.01},
        },
    }
    return profiles.get(profile_name, profiles['riotbench_edge'])
