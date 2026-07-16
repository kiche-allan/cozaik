from ticktalkpython.SQ import GRAPHify, SQify
from ticktalkpython.Instructions import *

@SQify
def process(value):
    return value * 2

@GRAPHify
def constrained_app(x):
    with TTConstraint(name="cav99"):
        result = process(x)
    return result
