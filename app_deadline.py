from ticktalkpython.SQ import GRAPHify, SQify
from ticktalkpython.Instructions import *

@SQify
def read_data(x):
    return x * 2

@SQify
def plan_b():
    return -1

@GRAPHify
def deadline_app(x):
    with TTConstraint(name="edge0"):
        raw = read_data(x)
        result = TTFinishByOtherwise(
            raw,
            TTTimeDeadline=READ_TTCLOCK(x) + 1,
            TTPlanB=plan_b(),
            TTWillContinue=True)
    return result
