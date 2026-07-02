from ticktalkpython.SQ import GRAPHify, SQify

@SQify
def process(value):
    return value + 100

@GRAPHify
def app_two_main(y):
    result = process(y)
    return result
