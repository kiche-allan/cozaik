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

import concurrent.futures
import math
import multiprocess as mp
import pickle
import reprlib
import os
import time

from abc import ABC
from abc import abstractmethod
from typing import List  # 3.6

from . import Clock
from . import SQExecute
from . import DebugLogger
from .ExecuteProcessInterface import TTExecutionContext
from . import SQ
from . import Tag
from .Time import TTTime
from .Time import TTTimeSpec
from .TTToken import TTToken

# TODO: remove these top level debuggers
logger = DebugLogger.get_logger('engine')


def _log_sq_timing(sq_name, start, end, mode, core_slot):
    import json as _json
    run_label = os.environ.get('TTPYTHON_RUN_LABEL', '')
    if not run_label:
        return
    entry = {
        'sq_name': sq_name,
        'start': start,
        'end': end,
        'execution_ms': (end - start) * 1000,
        'mode': mode,
        'core_slot': core_slot,
        'pid': os.getpid(),
        'timestamp': time.time(),
    }
    log_file = f'sq_timing_{run_label}.jsonl'
    try:
        with open(log_file, 'a') as f:
            f.write(_json.dumps(entry) + '\n')
    except:
        pass


class EngineOutput:

    # describe the payloads/tokens to be sent to the network or back to input
    # token handler
    def __init__(self, source_sq_name, ntwk_payloads, itp_tokens):
        self.source_sq_name = source_sq_name
        self.ntwk_payloads = ntwk_payloads
        self.itp_tokens = itp_tokens


class Engine(ABC):

    def __init__(self, root_clock):
        self.root_clock = root_clock
        # {sq_name : sq_execute}
        self.sq_executes = {}
        # Learned data size measurement: accumulate per-port samples
        # and store averaged sizes after N invocations.
        self._data_size_samples = {}   # {(sq_name, opp_name): [size_bytes, ...]}
        self._data_size_stored = set() # {(sq_name, opp_name)} already persisted
        self._DATA_SIZE_SAMPLE_COUNT = 5

    def add_sq(self, sq_ex: SQExecute.TTSQExecute):
        self.sq_executes[sq_ex.sq_name] = sq_ex

    # returns list of prepped tokens ready to be sent over the network as
    # payloads
    def prep_tokens(self, raw_tokens, sq_ex: SQExecute.TTSQExecute,
                    ex_ctx: TTExecutionContext, execute_time, completion_time):
        # let's estimate that the value we generated was create
        # approximately halfway between when we started this
        # function and when it returned.
        est_sampling_timestamp = (execute_time + completion_time) // 2
        payloads = []

        for idx, raw_token in enumerate(raw_tokens):
            logger.debug(f'SQ {sq_ex.sq_name} for opp {idx} produced '
                         f'token: {raw_token}')

            # create the basis for a tag, starting from the application
            # context. The rest will be filled in when forwarding to all arc
            # destinations
            raw_token.tag = Tag.TTTag(ex_ctx.inputs[0].tag.u)

            # If this is a STREAMify or timed self-retriggering node, modify
            # the TTTime based on the data_validity_interval (retrived from
            # a keyword in the TTPython program)
            if (sq_ex.pattern == SQ.TTSQPattern.TriggerInNOut
                    and sq_ex.data_validity_interval):

                # if this is a sampling node and it has a
                # data-validity-interval, then recalculate the time based on
                # an approximate sampling time
                #
                # if the interval is odd, then this will actually be a bit
                # shorter; maybe do a ceiling and floor
                raw_token.time = TTTime(
                    self.root_clock, est_sampling_timestamp -
                    math.ceil(sq_ex.data_validity_interval / 2),
                    est_sampling_timestamp +
                    math.ceil(sq_ex.data_validity_interval / 2))

            # replace with a TTTimeSpec before sending it to the next
            # process
            raw_token.time = TTTimeSpec.from_time(raw_token.time)
            payload = {
                'token': raw_token,
                'source_sq': sq_ex.sq_name
            }
            payloads.append(payload)

            # Measure actual data size for learned estimation.
            # After _DATA_SIZE_SAMPLE_COUNT samples per (sq_name, opp_name),
            # average and persist to learned_data_sizes.pkl for use by the
            # compiler on subsequent compilations.
            if hasattr(sq_ex, 'opp_names') and idx < len(sq_ex.opp_names):
                opp_name = sq_ex.opp_names[idx]
                key = (sq_ex.sq_name, opp_name)
                if key not in self._data_size_stored:
                    try:
                        measured_size = len(pickle.dumps(raw_token.value))
                        samples = self._data_size_samples.setdefault(key, [])
                        samples.append(measured_size)
                        if len(samples) >= self._DATA_SIZE_SAMPLE_COUNT:
                            avg_size = sum(samples) // len(samples)
                            self._persist_learned_data_size(
                                sq_ex.sq_name, opp_name, avg_size)
                            self._data_size_stored.add(key)
                            del self._data_size_samples[key]
                    except Exception:
                        pass  # Measurement failure must not disrupt execution

        return EngineOutput(sq_ex.sq_name, payloads,
                            self.generate_periodic_ctrl_token(sq_ex, ex_ctx))

    def generate_periodic_ctrl_token(self, sq_ex, ex_ctx) -> List[TTToken]:
        feedback_sequence_token = None

        if sq_ex.is_sequential:
            time_overlap = ex_ctx.input_time_overlap
            # the next invocation must be on tokens that are strictly
            # older than than the ones used for this invocation. We want
            # to process in chronological order
            # TODO: find a way to make
            # this less susceptible to skipping iterations. May require
            # deadlines or assumption (could be learned) about how far
            # apart stream values are generated (assuming periodic) and
            # how long they take to arrive(??). May require extra
            # specification in TTPython program with kwargs
            time = TTTimeSpec(Clock.TTClockSpec.from_clock(time_overlap.clock),
                              time_overlap.start_tick, TTTime.MAX_TIMESTAMP)

            # create a tag for the feedback token; let's assume the
            # control port is at a port starting at the number of inputs
            # for execution
            # TODO: brittle: assumes that ctrl_port is after all num_inputs
            # last one. Constructs this in the EP which breaks ntwk
            # construction. Should Tags be made in the Engine?
            tag = Tag.TTTag(context=ex_ctx.inputs[0].tag.u,
                            sq=sq_ex.sq_name,
                            port=sq_ex.num_inputs)
            feedback_sequence_token = TTToken(None, time, tag=tag)
            logger.debug('Retriggering: %s', feedback_sequence_token)

        return [feedback_sequence_token]

    def set_root_clock(self, clk):
        self.root_clock = clk

    def _persist_learned_data_size(self, sq_name, output_name, avg_size):
        """
        Store averaged data size measurement to learned_data_sizes.pkl.

        This file is read by the compiler's _get_learned_data_size on
        subsequent compilations, replacing structural estimates with
        empirical measurements.

        Key format: {sq_name: {output_name: size_in_bytes}}
        Must match the format expected by Compiler._get_learned_data_size.
        """
        learned_sizes_file = 'learned_data_sizes.pkl'
        try:
            learned_data = {}
            if os.path.exists(learned_sizes_file):
                with open(learned_sizes_file, 'rb') as f:
                    learned_data = pickle.load(f)

            if sq_name not in learned_data:
                learned_data[sq_name] = {}
            learned_data[sq_name][output_name] = avg_size

            with open(learned_sizes_file, 'wb') as f:
                pickle.dump(learned_data, f)

            logger.info(f"Learned data size: {sq_name}.{output_name} = "
                        f"{avg_size} bytes (avg of {self._DATA_SIZE_SAMPLE_COUNT})")
        except Exception as e:
            logger.warning(f"Could not persist learned data size: {e}")

    @abstractmethod
    def submit_job(self, ex_ctx: TTExecutionContext):
        ...

    @abstractmethod
    def cleanup(self):
        ...


class TTSQMetadata:

    def __init__(self, sq_execute, execute_context, execute_time):
        self.job_id = id(self)
        self.sq_execute = sq_execute
        self.execute_context = execute_context
        self.execute_time = execute_time

    def get_id(self):
        return self.job_id


class TTSQClosure:

    def __init__(self, job_id, sq_name, func_name, namespace, args, kwargs,
                 core_slot=None):
        self.job_id = job_id
        self.sq_name = sq_name
        self.func_name = func_name
        self.namespace = namespace
        self.args = args
        self.kwargs = kwargs
        self.core_slot = core_slot


class TTSQOutput:

    def __init__(self, job_id, name, sq_state, output):
        self.job_id = job_id
        self.name = name
        self.sq_state = sq_state
        self.output = output

    def __str__(self):
        return (f"TTSQOutput: ({self.name}, output:{self.output}, "
                f"sq_state:{reprlib.repr(self.sq_state)}")


# Needs to support event driven for clients to actually use this
# as an ApplyAsync equivalent
class PoolJob:

    def __init__(self, job_id=None):
        self.job_id = job_id if job_id is not None else id(self)

    def __eq__(self, other):
        if isinstance(other, PoolJob):
            return self.job_id == other.job_id
        return NotImplemented

    def __hash__(self):
        return hash(self.job_id)

    def __repr__(self):
        return f"PoolJob: {self.job_id}"


def assign_func(func_name, func, state, in_queue, out_queue, core_slot=None, mode=None):
    # Pin this process to a specific core if multitenancy requires it
    if core_slot is not None and hasattr(os, 'sched_setaffinity'):
        try:
            os.sched_setaffinity(0, {core_slot})
            logger.info(f'Persistent SQ {func_name} (pid {os.getpid()}) '
                        f'pinned to core {core_slot}')
        except OSError as e:
            logger.warning(f'Failed to pin {func_name} to core {core_slot}: {e}')

    namespace = {
        "SQify": SQ.SQify,
        "STREAMify": SQ.STREAMify,
        'sq_state': state
    }
    exec(func, namespace)
    func = namespace[func_name]
    logger.debug(f'pid {os.getpid()} is assigned to {func_name}')
    _mode = mode or ('concurrent' if core_slot is not None else 'persistent')
    while 1:
        closure: TTSQClosure = in_queue.get()
        _t0 = time.perf_counter()
        output = func(*closure.args, **closure.kwargs)
        _t1 = time.perf_counter()
        _log_sq_timing(func_name, _t0, _t1, _mode, core_slot)
        out_queue.put(
            TTSQOutput(closure.job_id, closure.func_name,
                       namespace['sq_state'], output))


class PersistentPool:

    def __init__(self):
        self._finished_jobs = mp.Queue()
        self.finished_jobs = {}
        # workers are named {func_name: WorkerInfo}
        self.workers = {}

    class WorkerInfo():

        def __init__(self, queue, process):
            self.queue = queue
            self.process = process

    def reserve_process(self, func_name, uniq_id, func, core_slot=None, mode=None):
        # TODO: when saving state across device shutdown, enable env to be non
        # empty
        env = {}
        j = PoolJob()
        p_queue = mp.Queue()
        p = mp.Process(target=assign_func,
                       args=(func_name, func, env, p_queue,
                             self._finished_jobs, core_slot, mode))
        p.start()
        self.workers[uniq_id] = self.WorkerInfo(p_queue, p)
        return j

    def apply_async(self, uniq_id, func_name, args=(), kwds=None):
        j = PoolJob()
        kwargs = kwds if kwds is not None else {}
        c = TTSQClosure(j.job_id, None, func_name, {}, args, kwargs)
        self.workers[uniq_id].queue.put(c)
        return j

    def get_any_finished_job(self) -> TTSQOutput:
        if not self._finished_jobs.empty():
            return self._finished_jobs.get()
        return None

    # check the status
    def job_is_ready(self, j: PoolJob):
        return j.job_id in self.finished_jobs

    def cleanup(self):
        for w_info in self.workers.values():
            w_info.process.terminate()
            # would like to call close(), but 3.6 doesn't have it


class SharedProcessEngine:

    def __init__(self, sq_code):
        self.sq_code = sq_code

    def execute_job(self, closure: TTSQClosure):
        code = self.sq_code[closure.sq_name]
        exec(code, closure.namespace)
        func = closure.namespace[closure.func_name]
        _t0 = time.perf_counter()
        output = func(*closure.args, **closure.kwargs)
        _t1 = time.perf_counter()
        _log_sq_timing(closure.func_name, _t0, _t1, 'shared_process', None)
        return TTSQOutput(closure.job_id, closure.func_name,
                          closure.namespace['sq_state'], output)

    def apply_async(self, sq_name, func_name, namespace, args, kwargs, pool):
        j = PoolJob()
        result = pool.apply_async(
            self.execute_job,
            (TTSQClosure(j, sq_name, func_name, namespace, args, kwargs), ))
        return result


class SharedThreadEngine:

    def __init__(self):
        self.t_pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    def apply_async(self, sq_name, func_name, namespace, args, kwargs,
                    core_slot=None):
        j = PoolJob()
        closure = TTSQClosure(j, sq_name, func_name, namespace, args, kwargs,
                              core_slot=core_slot)
        return self.t_pool.submit(self.execute_job, closure)

    def execute_job(self, closure: TTSQClosure):
        # Pin this worker thread to a specific core if multitenancy requires it
        if closure.core_slot is not None and hasattr(os, 'sched_setaffinity'):
            try:
                os.sched_setaffinity(0, {closure.core_slot})
            except OSError:
                pass  # Best-effort; thread may not support affinity on all OSes

        func = closure.namespace[closure.func_name]
        _t0 = time.perf_counter()
        output = func(*closure.args, **closure.kwargs)
        _t1 = time.perf_counter()
        _log_sq_timing(closure.func_name, _t0, _t1, 'concurrent', closure.core_slot)
        return TTSQOutput(closure.job_id, closure.func_name,
                          closure.namespace['sq_state'], output)

    def cleanup(self):
        self.t_pool.shutdown()

    # TODO: save contexts with you


class TTPhyEngine(Engine):

    def __init__(self, root_clock, sq_executes):
        super().__init__(root_clock)


class TTExecutingEngine(Engine):

    def __init__(self, root_clock):
        super().__init__(root_clock)
        # {sq_name : sq_executes}
        self.sq_executes = {}
        self.persistent_pool = PersistentPool()
        self.waiting_jobs = {}
        self.shared_pool = SharedThreadEngine()
        self.waiting_p_jobs = {}
        # Multitenancy: {sq_name: allocation_metadata_dict}
        self.sq_allocations = {}

    def register_allocation(self, sq_name, allocation_metadata):
        """Store multitenancy allocation metadata for an SQ."""
        if allocation_metadata is not None:
            self.sq_allocations[sq_name] = allocation_metadata

    def _get_core_slot(self, sq_name):
        """Return the core_slot for an SQ, or None if not in concurrent mode."""
        alloc = self.sq_allocations.get(sq_name)
        if alloc and alloc.get('mode') == 'concurrent':
            return alloc.get('core_slot')
        return None

    def _is_timesliced(self, sq_name):
        """Return True if SQ is in timesliced mode (needs process isolation)."""
        alloc = self.sq_allocations.get(sq_name)
        return alloc is not None and alloc.get('mode') == 'timesliced'

    def add_sq(self, sq_ex: SQExecute.TTSQExecute):
        self.sq_executes[sq_ex.sq_name] = sq_ex

        # Route to PersistentPool if:
        # 1. SQ is persistent (original behavior), OR
        # 2. SQ is timesliced (needs dedicated OS process for CFS interleaving)
        use_dedicated_process = sq_ex.is_persistent or self._is_timesliced(sq_ex.sq_name)

        if use_dedicated_process:
            core_slot = self._get_core_slot(sq_ex.sq_name)
            if self._is_timesliced(sq_ex.sq_name):
                _sq_mode = 'timesliced'
            elif core_slot is not None:
                _sq_mode = 'concurrent'
            else:
                _sq_mode = 'persistent'
            self.persistent_pool.reserve_process(sq_ex.function_name,
                                                 sq_ex.sq_name, sq_ex.code,
                                                 core_slot=core_slot, mode=_sq_mode)
            if self._is_timesliced(sq_ex.sq_name):
                logger.info(f'SQ {sq_ex.sq_name} spawned as dedicated process '
                            f'(timesliced mode — OS CFS handles interleaving)')

    def submit_job(self, ex_ctx: TTExecutionContext):
        execute_time = self.root_clock.now()

        try:
            sq_ex = self.sq_executes[ex_ctx.sq_name]

        except KeyError:
            logger.error('Failed to find SQ named %s', ex_ctx.sq_name)
            return

        logger.profile('Execute for SQ %s', sq_ex.sq_name)

        if sq_ex.interpreter is SQ.TTInterpreter.Python3:

            # The sq should have already been instantiated (or at least
            # 'prepared')

            if len(ex_ctx.inputs) != sq_ex.num_inputs:
                # raise error instead? Likely that a runtime error will be
                # thrown. If we provided some default or null (None) input,
                # those should still be here in the proper index
                logger.profile("Execute SQ %s on %d inputs -- %d expected",
                               sq_ex.sq_name, len(ex_ctx.inputs),
                               sq_ex.num_inputs)

            metadata = TTSQMetadata(sq_ex, ex_ctx, execute_time)
            core_slot = self._get_core_slot(sq_ex.sq_name)

            # Route to PersistentPool if persistent OR timesliced
            use_persistent = sq_ex.is_persistent or self._is_timesliced(sq_ex.sq_name)

            if use_persistent:
                logger.debug('Dispatching %s via PersistentPool (persistent=%s, timesliced=%s)',
                             sq_ex.sq_name, sq_ex.is_persistent,
                             self._is_timesliced(sq_ex.sq_name))
                job = self.persistent_pool.apply_async(sq_ex.sq_name,
                                                       sq_ex.function_name,
                                                       ex_ctx.inputs,
                                                       sq_ex.kwargs)
                self.waiting_p_jobs[job] = metadata
            else:
                result = self.shared_pool.apply_async(sq_ex.sq_name,
                                                      sq_ex.function_name,
                                                      sq_ex.namespace,
                                                      ex_ctx.inputs,
                                                      sq_ex.kwargs,
                                                      core_slot=core_slot)
                self.waiting_jobs[result] = metadata

        else:
            raise ValueError('Interpreter not supported')

        return self

    def get_finished_jobs(self) -> List[EngineOutput]:
        finished_results = []
        for result, metadata in list(self.waiting_jobs.items()):
            if result.done():
                finished_results.append((result.result(), metadata))
                del self.waiting_jobs[result]

        result = self.persistent_pool.get_any_finished_job()
        while result is not None:
            job = PoolJob(result.job_id)
            finished_results.append((result, self.waiting_p_jobs[job]))
            del self.waiting_p_jobs[job]
            result = self.persistent_pool.get_any_finished_job()

        prepped_tokens = [
            self.prep_tokens(*job_result) for job_result in finished_results
        ]

        return prepped_tokens

    def prep_tokens(self, job_result,
                    sq_metadata: TTSQMetadata) -> EngineOutput:
        sq_execute = sq_metadata.sq_execute
        execute_context = sq_metadata.execute_context
        execute_time = sq_metadata.execute_time

        logger.debug(f'SQ {sq_execute.sq_name} returned')

        completion_time = self.root_clock.now()
        # TODO: if sequential, but one execution is very late, we may not want
        # to update the state
        # a persistent
        if not sq_execute.is_persistent:
            sq_execute.state.update(job_result.sq_state)

        return super().prep_tokens(job_result.output, sq_execute,
                                   execute_context, execute_time,
                                   completion_time)

    def cleanup(self):
        self.persistent_pool.cleanup()
        self.shared_pool.cleanup()
        # would like to call close(), but 3.6 doesn't have it

    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup()
