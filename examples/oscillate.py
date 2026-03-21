# Copyright 2024 The Authors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from logging import root
from ticktalkpython.SQ import STREAMify, GRAPHify
from ticktalkpython.Clock import TTClock
from ticktalkpython.Instructions import *


# streamify is meant for generating sampled data streams
@STREAMify
def oscillate(trigger):
    global sq_state
    if sq_state.get('count', None) is None:
        sq_state['count'] = 1

    if sq_state['count'] == 0:
        sq_state['count'] = 1
    else:
        sq_state['count'] = 0

    return sq_state['count']


@GRAPHify
def streamify_test(trigger):
    A_1 = 1

    with TTClock.root() as root_clock:
        # collect a timestamp from a clock; needs a trigger whose arrival will
        # make the timestamp be taken. This is for setting the start-tick of
        # the STREAMify's periodic firing rule
        start_time = READ_TTCLOCK(trigger, TTClock=root_clock)
        N = 4
        # Setup the stop-tick of the STREAMify's firing rule
        # sample for N seconds
        stop_time = start_time + (1000000 * N)

        # create a sampling interval by copying the start and stop tick from
        # token values to the token time interval
        sampling_time = VALUES_TO_TTTIME(start_time, stop_time)

        # copy the sampling interval to the input values to the STREAMify
        # node; these input values will be treated as sticky tokens, and
        # define the duration over which STREAMify'd nodes must run
        A1_sample = COPY_TTTIME(A_1, sampling_time)

        inc = oscillate(A1_sample,
                         TTClock=root_clock,
                         TTPeriod=500000,
                         TTPhase=0,
                         TTDataIntervalWidth=100000,
                         TTFirstInstanceDelay=2_000_000)

        return inc * inc
