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

import pickle, simpy, time
import argparse

from ticktalkpython import DebugLogger
from ticktalkpython import RuntimeManager
from ticktalkpython import Ensemble
from ticktalkpython import Graph
from ticktalkpython.IPC import *

from output_functions import get_applied_output_func


def unpack_graph(filename):
    """
    Load a compiled TTPython graph from a pickle file.

    Accepts both TTGraph (single app) and CombinedGraph (multi-app SSPG).
    The pickle type determines the deployment path through RuntimeManager.

    :param filename: Path to the pickled graph file
    :return: TTGraph or CombinedGraph object
    """
    with open(filename, 'rb') as inpickle:
        graph = pickle.load(inpickle)

    # Accept TTGraph (single app) or CombinedGraph (multi-app)
    is_combined = hasattr(graph, 'is_combined_graph') and graph.is_combined_graph()

    if not isinstance(graph, Graph.TTGraph) and not is_combined:
        raise TypeError(
            f'Pickle must contain a TTGraph or CombinedGraph, got {type(graph)}')

    if is_combined:
        print(f"Loaded CombinedGraph: {graph.graph_name} "
              f"({len(graph.app_ids)} apps: {graph.app_ids})")
    else:
        print(f"Loaded TTGraph: {graph.graph_name}")

    return graph


def create_device_ensembles(graph, sim, rtm, log_file_name, output_func,
                            delay, logger):
    """
    Create simulated device ensembles from the deployment spec embedded
    in the compiled graph.

    The compiler embeds deployment_spec in the graph at compile time
    (via compile.py --deployment). Each entry describes a device that
    the application expects to run on. This function creates one
    simulated TTEnsemble per device and connects it to the RuntimeManager,
    using the same APIs that physical devices use.

    When an ensemble connects, RuntimeManager._handle_predeclared_device_joining()
    adds it to connected_ensembles and calls _try_execute_pending_graphs(),
    which triggers deployment once all required devices are present.

    :param graph: TTGraph or CombinedGraph with deployment_spec
    :param sim: simpy.Environment
    :param rtm: TTRuntimeManagerSim instance
    :param log_file_name: Path to log file
    :param output_func: Output function for token logging
    :param delay: Network delay for simulated processes
    :param logger: Logger instance
    :return: Dict of {device_name: TTEnsemble}
    """
    deployment_spec = getattr(graph, 'deployment_spec', {})

    if not deployment_spec:
        logger.warning('Graph has no deployment_spec — '
                       'running without device ensembles (legacy mode)')
        return {}

    logger.info(f'Creating {len(deployment_spec)} device ensembles '
                f'from deployment spec')

    ensembles = {}

    for device_id, spec in deployment_spec.items():
        device_type = spec.get('type', 'unknown')

        logger.info(f'  Creating ensemble: {device_id} ({device_type})')

        # Create ensemble — same constructor as runens.py
        ens = Ensemble.TTEnsemble(log_file_name, device_id, output_func,
                                  is_runtime_mgr=False)

        # Set device identity so RTM's _verify_device_matches_spec sees
        # the correct type when this device joins
        ens.set_device_info(device_type=device_type)

        # Setup simulated queues and processes
        ens.setup_queues(is_sim=True)
        ens.setup_simulation_processes(sim=sim, delay=delay)

        # Connect to RTM — sends JoinTickTalkSystem message, which triggers
        # _handle_predeclared_device_joining → adds to connected_ensembles
        # → _try_execute_pending_graphs when all devices are present
        ens.connect_to_TickTalk_network(
            runtime_manager_address=rtm.manager_ensemble)

        ensembles[device_id] = ens

    logger.info(f'All {len(ensembles)} device ensembles created and connected')
    return ensembles


def send_input_tokens(graph, runtime_manager, logger, inputs=None):
    trigger_inputs = {'trigger': 0xdeadbeef} if inputs is None else inputs
    execute_graph_message = Message(RuntimeMsg.ExecuteGraphOnInputs,
                                    (graph, trigger_inputs),
                                    Recipient.ProcessRuntimeManager)

    logger.info('Sending token inputs\n\n\n\n\n\n')

    runtime_manager.send_to_runtime(execute_graph_message)


def send_graph_sim(runtime_manager, sim, graph, logger):
    """
    SimPy process that sends the graph to RuntimeManager for deployment.

    Detects whether the loaded graph is a TTGraph (single app) or
    CombinedGraph (multi-app SSPG) and sends the appropriate message:

    - TTGraph → InstantiateAndMapGraph
      RTM maps via SmartMapper (QPF random search if deployment_spec present,
      static mapping otherwise).

    - CombinedGraph → InstantiateAndMapCombinedGraph
      RTM maps via SmartMapper.optimize_combined_graph() with global QPF
      optimization, contention detection, and multitenancy scheduling.

    :param runtime_manager: TTRuntimeManagerSim instance
    :param sim: simpy.Environment
    :param graph: TTGraph or CombinedGraph (already loaded)
    :param logger: Logger instance
    """
    # Yield once to let device join messages propagate through RTM first
    yield sim.timeout(0)

    is_combined = hasattr(graph, 'is_combined_graph') and graph.is_combined_graph()

    if is_combined:
        logger.info(f'Deploying CombinedGraph: {graph.graph_name} '
                    f'({len(graph.app_ids)} apps)')
        # Uses RTM.instantiate_combined_graph() → InstantiateAndMapCombinedGraph
        # RTM handler: SmartMapper global QPF → contention detection →
        # concurrent upgrade → allocation_metadata per SQ → deployment
        runtime_manager.instantiate_combined_graph(graph)
    else:
        logger.info(f'Deploying TTGraph: {graph.graph_name}')
        # Uses RTM.instantiate_and_map_graph() → InstantiateAndMapGraph
        # RTM handler: deployment_spec → SmartMapper QPF → deployment
        runtime_manager.instantiate_and_map_graph(graph)

    yield sim.timeout(0)
    send_input_tokens(graph, runtime_manager, logger)


def main():
    parser = argparse.ArgumentParser(
        description=
        'simulate an execution of a compiled TTPython dataflow graph. '
        'Accepts both TTGraph (single app) and CombinedGraph (multi-app). '
        'Device ensembles are created automatically from the deployment '
        'spec embedded in the graph at compile time. '
        'logical ticks are equivalent to seconds')

    parser.add_argument('file',
                        metavar='F',
                        type=str,
                        help='the pickled dataflow graph to simulate '
                        '(TTGraph or CombinedGraph)')
    parser.add_argument(
        '--timeout',
        type=int,
        default=1_000_000_000,
        help='simulation timeout (default: 1_000_000_000 (logical ticks))')
    parser.add_argument('-n',
                        '--ntwk_delay',
                        type=float,
                        default=0,
                        help='specify network delay. '
                        'simulates time for token to travel between SQs.'
                        '(default: 0 (logical ticks))')
    parser.add_argument(
        '-o',
        '--output_func',
        type=str,
        default=['log_msg_to_file', './output.log'],
        nargs='*',
        help='the str name of the function found in module '
        '"output_functions" used to send tokens with no correponding '
        'downstream SQ (default: log_to_file). You must provide the '
        'argument list to execute said function. The function assumes '
        'the last argument is a class type Msg (in IPC.py) to write, which '
        'should not be included in the provided arg list.')
    parser.add_argument(
        '--log',
        default='./output.log',
        help='specify the log file used to capture runtime behavior')
    parser.add_argument('-d',
                        '--debug',
                        action='store_true',
                        help='flag whether to show debug information')

    args = parser.parse_args()
    file_path = args.file
    timeout = args.timeout
    delay = args.ntwk_delay
    output_list = args.output_func
    log_file_name = args.log
    is_debug = args.debug

    name = file_path.split('/')[-1][:-7]

    # get partially applied output function
    output_func_args = output_list[1:]
    output_func_name = output_list[0]
    applied_func = get_applied_output_func(output_func_name, output_func_args)

    logger = DebugLogger.get_logger(name)
    DebugLogger.set_base_logger_info()
    if is_debug:
        DebugLogger.set_base_logger_debug()

    with open(log_file_name, 'a') as f:
        f.write(
            f"\nstart execution of simulation ({name}) at {time.time()}\r\n")

    # Step 1: Load the compiled graph (TTGraph or CombinedGraph)
    # We load it early so we can read deployment_spec before creating RTM
    logger.info('loading compiled graph')
    graph = unpack_graph(file_path)

    # Step 2: Setup simpy environment
    logger.info('setup sim')
    sim = simpy.Environment(initial_time=0)

    # Step 3: Create RuntimeManager
    logger.info('setup runtime manager')
    rtm = RuntimeManager.TTRuntimeManagerSim(log_file_name, [],
                                             sim,
                                             applied_func,
                                             delay=delay)

    # Load deployment spec into RTM so it recognizes pre-declared devices
    rtm.manager_ensemble.runtime_mgr_proc._load_deployment_spec_from_graph(graph)
    
    # Step 4: Create device ensembles from the deployment spec embedded
    # in the graph. Each device connects to RTM via JoinTickTalkSystem.
    # If no deployment_spec is present, this is a no-op and the graph
    # runs in legacy mode (RTM-only, no device distribution).
    logger.info('setup device ensembles')
    ensembles = create_device_ensembles(graph, sim, rtm, log_file_name,
                                        applied_func, delay, logger)

    # Step 5: Schedule graph deployment and token injection.
    # The graph object is passed directly — no second pickle load needed.
    # send_graph_sim detects TTGraph vs CombinedGraph and sends the
    # appropriate message to RTM.
    logger.info('schedule graph deployment')
    sim.process(send_graph_sim(rtm, sim, graph, logger))

    # Step 6: Enter steady state for RTM and all device ensembles.
    # RTM steady state processes heartbeats and pending graph checks.
    # Device steady states keep their simpy processes alive.
    rtm.manager_ensemble.enter_steady_state(timeout=timeout)
    for ens in ensembles.values():
        ens.enter_steady_state(timeout=timeout)

    # Step 7: Run the simulation
    sim.run(until=timeout)

    print("simulation finished. program output using "
          f"output function '{output_func_name}'")


if __name__ == "__main__":
    main()