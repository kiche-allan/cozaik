
from ticktalkpython.SQ import GRAPHify, SQify

@SQify
def read_sensor(device_id):
    # Simulates reading a temperature value
    return 42.5

@SQify
def apply_threshold(value):
    # Checks if value exceeds threshold
    threshold = 75.0
    return value > threshold

@SQify
def compute_alert(raw_value, is_high):
    # Computes alert level
    # Paper equivalent: @proc
    if is_high:
        return raw_value * 2.0
    return raw_value * 0.5

@GRAPHify
def sensor_pipeline(device_id):
    # Top-level application function
    # Paper equivalent: @app(priority=1)
    raw = read_sensor(device_id)
    high = apply_threshold(raw)
    alert = compute_alert(raw, high)
    return alert
