from ticktalkpython.SQ import GRAPHify, SQify

@SQify
def process(value):
    return value * 2

@GRAPHify
def app_one_main(x):
    result = process(x)
    return result
