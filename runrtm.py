# Copyright 2022 The Authors
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
import argparse
import os
import sys
import pickle
import time
import timedinput
import logging

from typing import Dict, Any, Optional, TYPE_CHECKING

from ticktalkpython import DebugLogger
from ticktalkpython import RuntimeManager
from ticktalkpython import Graph
from ticktalkpython.IPC import *
from ticktalkpython.Constants import get_readable_time
from ticktalkpython.RuntimeAdapter import RuntimeAdapter

from output_functions import get_applied_output_func


def unpack_graph(filename, runtime_adapter=None):
    with open(filename, 'rb') as inpickle:
        graph = pickle.load(inpickle)
    # Set up runtime adapter if enabled
    if runtime_adapter == "pending":
        adapter_instance = RuntimeAdapter()
        runtime_adapter = adapter_instance
    # Accept TTGraph (single app) or CombinedGraph (multi-app)
    is_combined = hasattr(graph, 'is_combined_graph') and graph.is_combined_graph()
    if not isinstance(graph, Graph.TTGraph) and not is_combined:
        raise ValueError(
            f'Pickle must contain a TTGraph or CombinedGraph, got {type(graph)}')
    return graph, runtime_adapter 


# TODO: input values should be modifiable by the user.
# What format should this be in? perhaps JSON? {input: val}
def send_input_tokens(graph,
                      logger,
                      runtime_manager: RuntimeManager.TTRuntimeManager,
                      inputs: Optional[Dict[str, Any]] = None):
    if inputs is not None:
        graph_inputs = inputs
    else:
        graph_inputs = {}
        for input_var in graph.source_var_names():
            graph_inputs[input_var] = 0xdeadbeef

    execute_graph_message = Message(RuntimeMsg.ExecuteGraphOnInputs,
                                    (graph, graph_inputs),
                                    Recipient.ProcessRuntimeManager)

    logger.info('Sending token inputs\n\n\n')

    runtime_manager.send_to_runtime(execute_graph_message)


def run_application_rtm(name,
                        pickled_graph_file_paths,  # Changed: now accepts list
                        ip,
                        port,
                        output_func,
                        log_file_name,
                        logger,
                        timeout,
                        subscription_time,
                        in_jupyter=False,
                        is_multi_app=False,        # New parameter
                        priorities=None,           # New parameter (internal use only)
                        app_ids=None,              # New parameter
                        strategy='qpf',            # Placement strategy
                        objective='makespan',      # QPF optimization objective
                        forced_mapping=None):      # Pre-computed mapping from --mapping
    # Check for runtime adaptation flag
    runtime_adapter = None
    if '--enable-adaptation' in sys.argv:
        print("Runtime adaptation enabled")
        runtime_adapter = "pending"

    try:
        with open(log_file_name, 'a') as logfile:
            logfile.write(f'\nstart execution of phy ({name}) at '
                          f'{get_readable_time(time.time())}\n')

        rtm = RuntimeManager.TTRuntimeManagerPhysical(ip, port, port + 1,
                                                      log_file_name,
                                                      output_func)

        time.sleep(0.5)
        # timedinput doesn't play nice with jupyter notebooks, hack around it
        if in_jupyter:
            time.sleep(subscription_time)
        else:
            if is_multi_app:
                timedinput.timedinput(
                    f'wait for {subscription_time} secs for devices to connect...\n'
                    f'Deploying {len(pickled_graph_file_paths)} applications: {app_ids}\n'
                    f'hit enter\n\n', subscription_time, ' ')
            else:
                timedinput.timedinput(
                    f'wait for {subscription_time} secs for devices '
                    'to connect... hit enter\n\n', subscription_time, ' ')

        if is_multi_app:
            # Multi-app deployment (separate pickle files)
            graphs = []
            for file_path in pickled_graph_file_paths:
                graph, runtime_adapter = unpack_graph(file_path, runtime_adapter)
                graphs.append(graph)
            
            print(f"Deploying {len(graphs)} applications: {app_ids}")
            instantiate_graphs_msg = Message(
                RuntimeMsg.InstantiateAndMapMultipleGraphs,
                (graphs, priorities, app_ids),
                Recipient.ProcessRuntimeManager)
            rtm.send_to_runtime(instantiate_graphs_msg)
            
            time.sleep(2)
            for graph in graphs:
                send_input_tokens(graph, logger, rtm)
        else:
            # Single file — could be TTGraph or CombinedGraph
            graph, runtime_adapter = unpack_graph(pickled_graph_file_paths[0], runtime_adapter)
            
            is_combined = hasattr(graph, 'is_combined_graph') and graph.is_combined_graph()
            if is_combined:
                print(f"Deploying CombinedGraph: {graph.graph_name} "
                      f"({len(graph.app_ids)} apps)")
                rtm.instantiate_combined_graph(
                    graph,
                    deployment_config={
                        'strategy': strategy,
                        'objective': objective,
                        'trials': 1000
                    })
            else:
                deploy_metadata = {'strategy': strategy, 'objective': objective}
                instantiate_graph_msg = Message(RuntimeMsg.InstantiateAndMapGraph,
                                                (graph, forced_mapping, deploy_metadata),
                                                Recipient.ProcessRuntimeManager)
                rtm.send_to_runtime(instantiate_graph_msg)

            time.sleep(2)
            send_input_tokens(graph, logger, rtm)

        if timeout <= 0:
            rtm.manager_ensemble.enter_steady_state()
        else:
            rtm.manager_ensemble.enter_steady_state(timeout)

    except KeyboardInterrupt:
        print('KB interrupt; exit physical test')


def main():

    parser = argparse.ArgumentParser(
        description='instantiate the runtime manager for a TTPython program')

    parser.add_argument('files',
                        metavar='F',
                        type=str,
                        nargs='+',
                        help='the pickled dataflow graph(s) to execute (one or more .pickle files)')
    parser.add_argument(
        '--ip',
        default='127.0.0.1',
        help='the ip of the runtime manager (default: localhost:127.0.0.1)')
    parser.add_argument('port', help='the port of the runtime manager')
    parser.add_argument(
        '--timeout',
        default=60,
        type=float,
        help='runtime manager timeout (default: 60 (sec), 0 for infty)')
    parser.add_argument(
        '-o',
        '--output_func',
        type=str,
        default=['log_msg_to_file', './output.log'],
        nargs='*',
        help='the str name of the function found in module '
        '"output_functions" used to send tokens with no correponding '
        'downstream SQ (default: -o log_msg_to_file ./output.log). You '
        'mustprovide the argument list to execute said function. '
        'The function assumes the last argument is a class type Msg '
        '(in IPC.py) to write, which should not be included in the provided '
        'arg list.')
    parser.add_argument(
        '--log',
        default='./output.log',
        help='specify the log file used to capture runtime behavior')
    parser.add_argument(
        '-d',
        '--debug',
        nargs='?',
        const=logging.DEBUG,
        default=logging.INFO,
        help='set level for Python logger. '
        'Add an additional argument to only get function call profiling')
    parser.add_argument(
        '-s',
        '--sub_time',
        default=3600,
        type=int,
        help='how many seconds to wait for devices to connect (default 1 hr)')
    parser.add_argument(
        '-j',
        '--jupyter_compat',
        action='store_true',
        help='set this argument if you use this in a Jupyter Notebook.')
    parser.add_argument(
        '--app-ids',
        type=str,
        nargs='*',
        default=None,
        help='application IDs for multiple apps. '
             'Must match number of files if provided. Defaults to [app_0, app_1, ...]')
    parser.add_argument(
        '--strategy',
        type=str,
        choices=['qpf', 'static', 'random', 'trivial'],
        default='qpf',
        help='placement strategy (default: qpf)')
    parser.add_argument(
        '--objective',
        type=str,
        choices=['makespan', 'energy'],
        default='makespan',
        help='optimization objective for QPF strategy (default: makespan). '
             'Ignored for non-QPF strategies.')
    parser.add_argument(
        '--mapping',
        type=str,
        default=None,
        help='Path to pre-computed mapping JSON file from runtime_validation.py. '
             'Deploys this exact placement instead of computing one at runtime.')
    parser.add_argument(
        '--run-label',
        type=str,
        default=None,
        help='Label for this run (e.g. eval_etl_qpf_makespan). '
             'Exported as TTPYTHON_RUN_LABEL env var for structured sink logging.')

    args = parser.parse_args()
    file_paths = args.files
    ip = args.ip
    port = int(args.port)
    timeout = args.timeout
    output_list = args.output_func
    log_file_name = args.log
    debug_level = args.debug
    subscription_time = args.sub_time
    in_jupyter = args.jupyter_compat
    app_ids = args.app_ids
    strategy = args.strategy
    objective = args.objective

    # Load pre-computed mapping if provided
    forced_mapping = None
    if args.mapping:
        import json as _json
        with open(args.mapping) as _f:
            mapping_data = _json.load(_f)
        forced_mapping = mapping_data.get('mapping', {})
        strategy = mapping_data.get('strategy', strategy)
        print(f"Using pre-computed mapping from: {args.mapping}")
        print(f"  Strategy: {strategy}, SQs: {len(forced_mapping)}")
        for sq, dev in sorted(forced_mapping.items()):
            print(f"    {sq} -> {dev}")

    # Export env vars for structured sink logging
    if args.run_label:
        os.environ['TTPYTHON_RUN_LABEL'] = args.run_label
    if args.mapping:
        os.environ['TTPYTHON_PREDICTED_MS'] = str(
            mapping_data.get('predicted_makespan_ms', 0))

    name = 'runtime_manager'

    DebugLogger.set_base_logger_info()
    if debug_level is logging.DEBUG:
        DebugLogger.set_base_logger_debug()
    elif debug_level is not logging.INFO:
        DebugLogger.set_base_logger_profiling()

    logger = DebugLogger.get_logger(name)

    # get partially applied output function
    output_func_args = output_list[1:]
    output_func_name = output_list[0]
    applied_func = get_applied_output_func(output_func_name, output_func_args)

    # Determine multi-app mode: multiple files = separate apps
    is_multi_app = len(file_paths) > 1
    priorities = None
    if is_multi_app:
        if app_ids is None:
            app_ids = [f'app_{i}' for i in range(len(file_paths))]
        elif len(app_ids) != len(file_paths):
            parser.error(f"--app-ids count ({len(app_ids)}) must match file count ({len(file_paths)})")

    run_application_rtm(name, file_paths, ip, port, applied_func, log_file_name,
                        logger, timeout, subscription_time, in_jupyter,
                        is_multi_app, priorities, app_ids, strategy, objective,
                        forced_mapping)

    print("runtime shutdown. program output using "
          f"output function '{output_func_name}'")


if __name__ == "__main__":
    main()
