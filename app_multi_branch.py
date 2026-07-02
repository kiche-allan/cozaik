from ticktalkpython.SQ import GRAPHify, SQify

@SQify
def source_a(value):
    return value * 2

@SQify
def source_b(value):
    return value + 5

@SQify
def sink_a(value):
    return value - 1

@SQify
def sink_b(value):
    return value * 10

@GRAPHify
def branching_app(x, y):
    a = source_a(x)
    b = source_b(y)
    out_a = sink_a(a)
    out_b = sink_b(b)
    return out_a
