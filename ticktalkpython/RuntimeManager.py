# Copyright 2021 The Authors
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

'''
A runtime manager is a higher level entity in the TickTalk system that serves to
coordinate the setup and teardown of the TickTalk system and runtime, including
notifying ``TTEnsembles`` of each other, generating a mapping (with
``TTMapper``) of the graph, distributing the SQs to ensembles, injecting initial
tokens into the system to kickstart graph interpretation and logging final
output tokens for future analysis. In other words, the runtime manager handles
the management plane of the system.

The ``TTRuntimeManager`` is effectively another ``TTEnsemble``, but differs in
that it implements an extra process, ``TTRuntimeManagerProcess``. In essence,
the TTRuntimeManager is really a wrapper for this process, and is best suited as
the user-facing device so the user can see other ensembles in the system connect
and personally trigger a graph to be instantiated and interpretation started
once the system is setup per their needs.
'''

import math
from abc import ABC
import queue
import os
import time
from typing import Dict, Optional

from .Graph import TTGraph
from . import DebugLogger
from . import Mapper
from .SQ import TTSQ
from .SQExecute import TTSQExecute
from .TTToken import TTToken
from . import Clock
from . import Tag
from . import Time
from .IPC import Message
from .IPC import NetMsg
from .IPC import Recipient
from .IPC import RuntimeMsg
from .IPC import SyncMsg
from .IPC import ExecuteMsg
from .IPC import FinishedException
from .Constants import RUNTIME_MANAGER_ENSEMBLE_NAME
from .Query import TTEnsembleInfo
from . import SmartMapper


logger = DebugLogger.get_logger('RuntimeManager')

class TTRuntimeManager(ABC):
    '''
    An entity to manage the environment at runtime. The program starts from
    here, getting mapped to ensembles either dynamically or according to
    extant information within the SQs in the graph. This is technically an
    ensemble in that it has a network interface. Simulated and physical
    variants exist for this as child classes, similar to the network
    interfaces.

    The log file is used to record the tokens on the graph's output arcs.
    '''

    def __init__(self,
                 log_file_name,
                 output_func,
                 name=RUNTIME_MANAGER_ENSEMBLE_NAME,
                 runtime_adapter=None): # New Parameter
        # the ensemble will go through ordinary setup procedures, which are
        # somewhat specific to the runtime environment (physical vs.
        # simulation)
        # TODO: Refactor this to not need a local import to avoid circular
        # dependency
        from . import Ensemble
        self.log_file_name = log_file_name
        self.output_func = output_func
        self.manager_ensemble = Ensemble.TTEnsemble(log_file_name,
                                                    name,
                                                    output_func,
                                                    is_runtime_mgr=True)

        self.logger = DebugLogger.get_logger(f'TTRuntimeManager({name})')
        self.connected_ensembles = []
        # this is effectively a copy of the routing table, but may also
        # contain additional metadata about ensemble capabilities

    def send_to_runtime(self, msg):
        '''
        Only the runtime manager process will actually interact with the rest of
        the system; this simply serves as a proxy from the user-level
        environment (the main process on the machine hsoting the runtime
        manager)

        :param msg: The message to pass to the actual runtime manager process

        :type msg: Message
        '''
        if (self.manager_ensemble and 
            hasattr(self.manager_ensemble, 'runtime_mgr_proc') and 
            self.manager_ensemble.runtime_mgr_proc is not None):
            self.manager_ensemble.runtime_mgr_proc.input_msg(msg)
        else:
            self.logger.warning("Runtime manager process not available - message dropped")

    def instantiate_and_map_graph(self, graph: TTGraph):
        '''
        Signal the runtime manager process to instanatiate the graph for
        execution by generating a mapping to of SQs to ensembles and
        distributing those SQs accordingly

        :param graph: The graph representing a TTPython program to execute

        :type graph: TTGraph
        '''
        # TODO; allow a statically-produced mapping to be provided here as well.
        # In that case, the graph and mapping should be set as the payload in a
        # tuple (graph, mapping).
        graph_msg = Message(
            RuntimeMsg.InstantiateAndMapGraph,
            graph,
            Recipient.ProcessRuntimeManager)
        self.send_to_runtime(graph_msg)

    def instantiate_and_map_graphs(self, graphs, app_ids=None):
        '''
        Signal the runtime manager process to instantiate multiple graphs for
        execution. This enables multi-application deployment where multiple
        TTGraphs share device resources via multitenancy.

        :param graphs: List of TTGraph objects representing multiple TTPython programs

        :param app_ids: Optional list of application identifiers (strings).
                       If None, will auto-generate as "app_0", "app_1", etc.
                       Must have same length as graphs if provided.

        :type graphs: List[TTGraph]
        :type app_ids: Optional[List[str]]
        '''
        if app_ids is not None and len(app_ids) != len(graphs):
            raise ValueError(f"Number of app_ids ({len(app_ids)}) must match "
                           f"number of graphs ({len(graphs)})")
        
        if app_ids is None:
            app_ids = [f"app_{i}" for i in range(len(graphs))]
        
        multi_graph_msg = Message(
            RuntimeMsg.InstantiateAndMapMultipleGraphs,
            (graphs, app_ids),
            Recipient.ProcessRuntimeManager)
        self.send_to_runtime(multi_graph_msg)

    def instantiate_and_map_app(self, graph, app_config):
        '''
        Deploy a single application incrementally.
        
        This is the primary entry point for dynamic app deployment.
        The RuntimeManager will place this app on available resources,
        or queue it if resources are insufficient.
        
        :param graph: The TTGraph for this application
        :type graph: TTGraph
        
        :param app_config: Application configuration
                      {'app_id': str}
        :type app_config: dict
        '''
        deploy_msg = Message(
            RuntimeMsg.DeployApp,
            (graph, app_config),
            Recipient.ProcessRuntimeManager)
        self.send_to_runtime(deploy_msg)
    
    def terminate_app(self, app_id):
        '''
        Terminate an application and release its resources.
        
        :param app_id: The application identifier to terminate
        :type app_id: str
        '''
        terminate_msg = Message(
            RuntimeMsg.TerminateApp,
            app_id,
            Recipient.ProcessRuntimeManager)
        self.send_to_runtime(terminate_msg)

    def instantiate_combined_graph(self, combined_graph, deployment_config=None):
        """
        Deploy a CombinedGraph with proper multitenancy handling.
        
        This is the primary entry point for deploying SSPG-combined applications.
        
        :param combined_graph: CombinedGraph from Combiner.combine()
        :type combined_graph: CombinedGraph
        
        :param deployment_config: Optional configuration dict
                                  {'strategy': 'qpf'|'static'|...,
                                   'objective': 'makespan'|'energy',
                                   'trials': int}
        :type deployment_config: dict
        """
        if deployment_config is None:
            deployment_config = {'strategy': 'qpf', 'objective': 'makespan', 'trials': 1000}
        
        deploy_msg = Message(
            RuntimeMsg.InstantiateAndMapCombinedGraph,
            (combined_graph, deployment_config),
            Recipient.ProcessRuntimeManager)
        self.send_to_runtime(deploy_msg)

class TTRuntimeManagerSim(TTRuntimeManager):
    '''
    A simulated runtime manager. Can directly access any reference to another
    ensemble, clock, SQ, etc.; uses a simulated network interface. This is is
    mainly used to configure the ensemble acting as the Runtime Manager

    :param ensembles: A list of the ensembles that compose the system. This may
        be empty, in the case where the other ensembles are created *after*
        the runtime manager starts (such that they join the TickTalk system as
        any physical ensemble would).

    :type ensembles: [TTEnsemble]
    '''
    def __init__(self,
                 log_file_name,
                 ensembles,
                 sim,
                 output_func,
                 delay=0,
                 name=RUNTIME_MANAGER_ENSEMBLE_NAME,
                 runtime_adapter=None):
        super().__init__(log_file_name, output_func, name=name, runtime_adapter=runtime_adapter)
        self.ensembles = ensembles
        self.sim = sim

        self.manager_ensemble.setup_queues(is_sim=True)
        self.manager_ensemble.setup_simulation_processes(sim=self.sim,
                                                         delay=delay)
        # this will block the rest
        # of execution until an uncaught exception or KB interrupt occurs
        # self.manager_ensemble.enter_steady_state()

        ens_description = TTEnsembleInfo(RUNTIME_MANAGER_ENSEMBLE_NAME,
                                         self.manager_ensemble,
                                         self.manager_ensemble.components)

        add_self_to_routing_msg = Message(
                RuntimeMsg.JoinTickTalkSystem,
                ens_description,
                Recipient.ProcessRuntimeManager)
        self.send_to_runtime(add_self_to_routing_msg)


class TTRuntimeManagerPhysical(TTRuntimeManager):
    '''
    A runtime manager on a physical device; one ensemble will take on this
    coordination role.

    :param ip: The IPv4 address of the runtime manager. Must be accessible by
        all other ensembles that wish to join the system.

    :type ip: string

    :param rx_port: The port the runtime manager ensemble expects to receive
        input messages from

    :type rx_port: int

    :param tx_port: The port the runtime manager plans to use for sending
        outputs to other ensembles in the system

    :type tx_port: int
    '''
    def __init__(self,
                 ip,
                 rx_port,
                 tx_port,
                 log_file_name,
                 output_func,
                 name=RUNTIME_MANAGER_ENSEMBLE_NAME):
        super().__init__(log_file_name, output_func, name=name)
        self.manager_ensemble.setup_queues(is_sim=False)
        self.manager_ensemble.setup_physical_processes(
            network_ip=ip,
            rx_network_port=rx_port,
            tx_network_port=tx_port)
        # this will block the rest
        # of execution until an uncaught exception or KB interrupt occurs
        # self.manager_ensemble.enter_steady_state()

        ens_description = TTEnsembleInfo(RUNTIME_MANAGER_ENSEMBLE_NAME,
                                         f'{ip}:{rx_port}',
                                         self.manager_ensemble.components)

        # add self to the routing table
        add_self_to_routing_msg = Message(RuntimeMsg.JoinTickTalkSystem,
                                          ens_description,
                                          Recipient.ProcessRuntimeManager)
        self.send_to_runtime(add_self_to_routing_msg)


class TTRuntimeManagerProcess():
    '''
    A priveleged process included only on the runtime manager ensemble that can
    receive from and send into the ``TTNetworkManager`` local to itself.
    It is responsible for forwarding routing-table additions to all connected
    ensembles, mapping SQs from the graph (and sending the corresponding
    messages), sending initial input tokens to trigger graph execution, and
    logging output tokens.

    All TT*Process classes follows the same design patterns. They implement a
    singular input queue from which they read new ``Messages``, which
    self-identify their function.  After processes are created, they exchange
    interfacing information, primarily in the form of callback functions. After
    configuring interfaces, the processes start. Each of these processes spends
    its idle time waiting for new inputs within a 'run loop', responding to
    messages as they arrive; the responses will modify internal process state
    and produce new messages for other processes implemented on the Ensemble,
    which 'owns' the processes.

    :param input_queue: An input queue to serve new data (as ``Messages``) to
        this process

    :type input_queue: queue.Queue | multiprocess.Queue

    :param ensemble_name: The name of this ensemble

    :type ensemble_name: string
    '''

    # ========== RUNTIME ADAPTATION CONFIGURATION ==========
    # Default Timing Constants (used as fallbacks)
    DEFAULT_HEARTBEAT_INTERVAL = 5.0
    DEFAULT_GRACE_PERIOD_MULTIPLIER = 6
    DEFAULT_STABILITY_WINDOW = 300.0
    DEFAULT_RELIABILITY_WINDOW = 3600.0
    
    # Reliability and Performance Thresholds
    RELIABILITY_FAILURE_THRESHOLD = 3     # 3 failures in time window = "problematic" 
    EXPONENTIAL_BACKOFF_MAX = 8           # max timeout multiplier for unreliable devices
    PERFORMANCE_THRESHOLD_MIN = 10.0      # minimum % improvement for migration
    PERFORMANCE_THRESHOLD_IMMEDIATE = 30.0 # % improvement for immediate integration
    SYSTEM_LOAD_THRESHOLD = 0.8           # Don't integrate new devices if load > 80%

    def __init__(self, log_file_name, output_func, input_queue, input_network_func, ensemble_name=None, runtime_adapter=None):
        # ========== CORE RUNTIME MANAGER STATE ==========
        self.log_file_name = log_file_name
        self.output_func = output_func
        self.input_queue = input_queue
        self.input_network_func = input_network_func
        self.ensemble_name = ensemble_name
        self.runtime_adapter = runtime_adapter
        
        # Core operational data
        self.connected_ensembles = {}        # Runtime: Devices that have joined {device_id: TTEnsembleInfo}
        self.deployment_spec = {}            # Design-time: Expected devices from YAML {device_id: DeviceSpec dict}
        self.pending_graphs = {}             # Graphs waiting for devices {graph_name: (graph, mapping, metadata)}
        self.instantiated_graphs = {}
        self.pending_inputs = {}             # Input tokens waiting for graph deployment {graph_name: (graph, input_dict)}
        self.deployed_port_wiring = {}       # {graph_name: {sq_name: [port_info]}} - frozen at deployment
        self.inactive_sqs = {}               # {graph_name: set(sq_name)} - SQs in graceful degradation
        
        # Deployment strategy metadata (for strategy-consistent runtime adaptation)
        self.deployment_metadata = {}   # {graph_name: {'strategy': str, 'placement_alternatives': dict, 'objective': str}}
        self.sim = None
        self.sim_process = None
        
        # ========== DEVICE STATE AND HEALTH MANAGEMENT ==========
        # Primary device tracking
        self.device_states = {}              # {device_id: 'ACTIVE'|'SUSPECTED'|'FAILED'|'REJOINING'|'AVAILABLE'}
        self.device_heartbeats = {}          # {device_id: last_heartbeat_timestamp}
        self.device_grace_periods = {}       # {device_id: grace_period_expiry_timestamp}
        
        # ========== CONTEXT-DRIVEN TIMING CONFIGURATION ==========
        # Derive all timing constants from application characteristics
        timing_config = self._derive_timing_constants_from_app()
        
        # Core timing parameters (derived from application context)
        self.heartbeat_timeout = timing_config['heartbeat_interval']
        self.grace_period_base = timing_config['grace_period_base']
        self.stability_window = timing_config['stability_window']
        self.reliability_time_window = timing_config['reliability_window']
        
        # Derived timing intervals (based on core parameters)
        self.adaptation_interval = self.heartbeat_timeout + 1.0     # Check after heartbeat expected
        self.stability_check_interval = self.stability_window / 10  # 10 checks during stability window
        self.coordination_interval = max(10.0, self.heartbeat_timeout * 2)  # At least 10s, or 2x heartbeat
        
        # Reliability and failure tracking
        self.device_reliability = {}         # {device_id: consecutive_failures_count}
        self.device_reliability_history = {} # {device_id: {'total_failures': int, 'last_failure': timestamp, 'total_downtime': seconds, 'unique_id': str}}
        self.pending_health_queries = {}     # {device_id: health_query_sent_timestamp}
        self.max_consecutive_failures = self.RELIABILITY_FAILURE_THRESHOLD
        
        # Rejoining device stability tracking
        self.device_stability_tracking = {}  # {device_id: {'start_time': timestamp, 'required_duration': seconds}}
        self.rejoining_devices = {}          # {device_id: rejoin_start_timestamp}
        
        # Timing state for periodic checks
        self.last_availability_check = time.time()
        self.last_stability_check = time.time()
        self.last_coordination_check = time.time()

        # ========== PERFORMANCE TRACKING AND METRICS ==========
        self.adaptation_metrics = {
            'total_adaptations': 0,
            'average_adaptation_time': 0.0,
            'device_failure_count': 0,
            'false_positive_prevented': 0,
            'successful_reconnections': 0,
            'new_devices_added': 0
        }

        # ========== DEADLINE METRICS ==========
        self.deadline_metrics = {
            'total_deadline_sqs': 0,
            'total_checks': 0,
            'total_misses': 0,
            'miss_rate': 0.0,
            'planb_executions': 0,
            'latency_samples': [],      # [{sq_name, actual_us, budget_us, met}]
            'latency_p50_us': 0.0,
            'latency_p95_us': 0.0,
            'latency_p99_us': 0.0,
            'worst_case_latency_us': 0.0,
            'best_case_latency_us': float('inf'),
            'per_sq_stats': {},         # {sq_name: {checks, misses, latencies}}
        }

        # ========== MULTI-APPLICATION COORDINATION ==========
        self.applications = {}               # {app_id: {'graph': TTGraph, 'mapped_sqs': dict}}
        self.device_allocations = {}         # {device_id: {'apps': {app_id: [sq_names]}}}
        self.pending_apps = []               # Apps waiting for resources
        self.app_configs = {}                # {app_id: {'graph': TTGraph}}
        self.app_states = {}                 # {app_id: 'ACTIVE' | 'TERMINATED'}
        
        # ========== RESOURCE LEDGER ==========
        # Tracks device capacity and current usage for parallel placement
        self.resource_ledger = {}            # {device_id: {'capacity': {...}, 'used': {...}, 'allocations': [...]}}

        # Multitenancy: Device concurrent capacity and pre-allocated scenarios
        self.device_runtime_state = {}       # {device_name: {concurrent_capacity, supports_timeslicing, ...}}
        self.multitenancy_scenarios = {}     # {device_name: {sq_id: {offset, duration}}}
        

        # ========== DEVICE PROFILE AND CATALOG INTEGRATION ==========
        # Load only the universal device types catalog at init time.
        # Deployment-specific profiles are populated when a graph arrives
        # (the graph carries its deployment_spec from compile time).
        from ticktalkpython.DeviceProfile import get_profile_manager
        self.device_profile_manager = get_profile_manager(
            device_types_path='device_types.yaml'
        )

        # ========== LOGGER INITIALIZATION ==========
        self.logger = DebugLogger.get_logger(f'RuntimeManager({ensemble_name})')
        self.logger.info(f"RuntimeManager initialized with {timing_config['profile']} timing profile")
        self.logger.info(f"Timing config - Heartbeat: {self.heartbeat_timeout}s, Grace: {self.grace_period_base}s, Stability: {self.stability_window}s")

        # ========== DEPLOYMENT SPECIFICATION ==========
        # deployment_spec is populated from graph.deployment_spec when a graph
        # message arrives. The Compiler embeds resolved device specs at compile
        # time, so no deployment YAML file is needed at runtime.


    def _load_deployment_spec_from_graph(self, graph):
        """
        Load deployment specification from a compiled graph.
        
        The Compiler embeds resolved device specs in graph.deployment_spec at
        compile time. This eliminates the need to load a separate deployment
        YAML at runtime and ensures consistency between compile-time estimates
        and runtime device configuration.
        
        Also populates the device_profile_manager with instance profiles so
        SmartMapper can look up device characteristics during placement.
        
        :param graph: Compiled TTGraph or CombinedGraph carrying deployment_spec
        """
        deployment_spec = getattr(graph, 'deployment_spec', {})
        
        if not deployment_spec:
            self.logger.warning("Graph has no deployment_spec - only dynamic device joining supported")
            return
        
        self.deployment_spec = deployment_spec
        
        # Populate the profile manager with resolved device instances from the graph
        # so SmartMapper._get_device_profile_for_mapping can look them up.
        from ticktalkpython.DeviceProfile import DeviceProfile
        for device_id, spec in deployment_spec.items():
            profile = DeviceProfile(
                name=device_id,
                cpu_speed=spec.get('cpu_speed', 1.0),
                memory_size=spec.get('memory_size', 1073741824),
                power_idle=spec.get('power_idle', 2.0),
                power_active=spec.get('power_active', 5.0),
                power_transmit=spec.get('power_transmit', 3.0),
                power_receive=spec.get('power_receive', 2.5),
                components=spec.get('components', {})
            )
            self.device_profile_manager.add_profile(profile)
        
        self.logger.info(f"Loaded deployment spec from graph: {len(deployment_spec)} devices")
        for device_id, spec in deployment_spec.items():
            self.logger.debug(f"  {device_id}: {spec['type']}, {spec['compute_slots']} slots, {spec['memory_mb']}MB")

        
    def _spec_to_ensemble_info(self, deployment_spec):
        """
        Convert deployment_spec to list of TTEnsembleInfo objects for mapping.
        
        This creates "virtual" TTEnsembleInfo objects from deployment specs
        for use in mapping algorithms that expect TTEnsembleInfo. These are
        NOT added to connected_ensembles - they're just for planning.
        
        :param deployment_spec: Dictionary of device specs {device_id: spec_dict}
        :return: List of TTEnsembleInfo objects
        """
        from .Query import TTEnsembleInfo
        
        ensemble_infos = []
        for device_id, spec in deployment_spec.items():
            # Build components dict matching TTEnsembleInfo format
            components = {
                'type': spec['type'],
                'cpu_cores': spec['cpu_cores'],
                'compute_slots': spec['compute_slots'],
                'memory_mb': spec['memory_mb'],
                'has_gpu': spec.get('has_gpu', False),
                **spec.get('components', {})
            }
            
            # Create TTEnsembleInfo (address=None is OK for planning)
            ensemble_info = TTEnsembleInfo(
                name=device_id,
                address=None,  # Not a real device, just for mapping
                components=components
            )
            
            ensemble_infos.append(ensemble_info)
        
        return ensemble_infos


    def _configure_runtime_adapter(self, graph, mapping, strategy: str, 
                                    placement_alternatives: Optional[Dict] = None,
                                    optimization_objective: str = "makespan"):
        """
        Configure RuntimeAdapter with deployment-specific parameters.
        
        This ensures runtime adaptation behavior matches the deployment strategy:
        - QPF: Uses constraints + stored placement alternatives
        - Static: Uses constraints + first compatible device
        - Random: No constraints + random selection
        - Trivial: Any available device
        
        :param graph: The deployed TTGraph (for constraint access)
        :param mapping: The deployed mapping {sq_name: device_id}
        :param strategy: Deployment strategy ('qpf', 'static', 'random', 'trivial')
        :param placement_alternatives: QPF-computed alternatives (only for QPF strategy)
        :param optimization_objective: 'makespan' or 'energy' (only for QPF strategy)
        """
        from ticktalkpython.RuntimeAdapter import RuntimeAdapter, DeploymentStrategy
        
        if self.runtime_adapter is None:
            # Build characterization data from graph
            characterization_data = {}
            for sq in graph.sqs:
                char_data = {'criticality': 'normal'}
                if hasattr(sq, 'criticality'):
                    char_data['criticality'] = sq.criticality
                if hasattr(sq, 'execution_time_estimates'):
                    char_data['execution_time_estimates'] = sq.execution_time_estimates
                characterization_data[sq.sq_name] = char_data
            
            self.runtime_adapter = RuntimeAdapter(
                characterization_data=characterization_data,
                initial_topology=self.connected_ensembles,      # Use actual connected devices, not spec
                initial_mapping=mapping,
                deployment_strategy=strategy,
                placement_alternatives=placement_alternatives or {},
                optimization_objective=optimization_objective,
                graph=graph
            )
            self.logger.info(f"RuntimeAdapter created with strategy={strategy}")
        else:
            # Update existing RuntimeAdapter with new deployment info
            self.runtime_adapter.deployment_strategy = DeploymentStrategy(strategy.lower())
            self.runtime_adapter.current_mapping = mapping.copy()
            self.runtime_adapter.graph = graph
            
            if strategy == 'qpf' and placement_alternatives:
                self.runtime_adapter.placement_alternatives = placement_alternatives
                self.runtime_adapter.optimization_objective = optimization_objective
            
            # Rebuild SQ constraints lookup from graph
            self.runtime_adapter.sq_constraints = {}
            for sq in graph.sqs:
                self.runtime_adapter.sq_constraints[sq.sq_name] = getattr(sq, 'constraints', None) or []
            
            self.logger.info(f"RuntimeAdapter updated with strategy={strategy}")


    
    def _record_deadline_result(self, sq_name: str, met: bool, actual_latency_us: int, budget_us: Optional[int] = None):
        """
        Record a deadline check result.
        
        :param sq_name: Name of the deadline SQ
        :param met: True if deadline was met
        :param actual_latency_us: Actual latency in microseconds
        :param budget_us: Deadline budget in microseconds
        """
        self.deadline_metrics['total_checks'] += 1
        
        if not met:
            self.deadline_metrics['total_misses'] += 1
            self.deadline_metrics['planb_executions'] += 1
            self.logger.warning(f"Deadline miss recorded: {sq_name} "
                               f"(latency={actual_latency_us}us, budget={budget_us}us)")
        
        # Store sample
        sample = {
            'sq_name': sq_name,
            'actual_us': actual_latency_us,
            'budget_us': budget_us,
            'met': met,
            'timestamp': time.time()
        }
        self.deadline_metrics['latency_samples'].append(sample)
        
        # Update worst/best case
        if actual_latency_us > self.deadline_metrics['worst_case_latency_us']:
            self.deadline_metrics['worst_case_latency_us'] = actual_latency_us
        if actual_latency_us < self.deadline_metrics['best_case_latency_us']:
            self.deadline_metrics['best_case_latency_us'] = actual_latency_us
        
        # Update per-SQ stats
        if sq_name not in self.deadline_metrics['per_sq_stats']:
            self.deadline_metrics['per_sq_stats'][sq_name] = {
                'checks': 0, 'misses': 0, 'latencies': []
            }
        
        sq_stats = self.deadline_metrics['per_sq_stats'][sq_name]
        sq_stats['checks'] += 1
        if not met:
            sq_stats['misses'] += 1
        sq_stats['latencies'].append(actual_latency_us)
        
        # Update overall miss rate
        total = self.deadline_metrics['total_checks']
        misses = self.deadline_metrics['total_misses']
        self.deadline_metrics['miss_rate'] = misses / total if total > 0 else 0.0
    
    def _get_deadline_statistics(self) -> dict:
        """
        Calculate comprehensive deadline statistics.
        
        :return: Dictionary with all deadline metrics including percentiles
        """
        stats = self.deadline_metrics.copy()
        
        # Calculate percentiles from samples
        latencies = [s['actual_us'] for s in stats['latency_samples']]
        if latencies:
            latencies_sorted = sorted(latencies)
            n = len(latencies_sorted)
            
            stats['latency_p50_us'] = latencies_sorted[int(n * 0.50)]
            stats['latency_p95_us'] = latencies_sorted[min(int(n * 0.95), n - 1)]
            stats['latency_p99_us'] = latencies_sorted[min(int(n * 0.99), n - 1)]
            stats['latency_avg_us'] = sum(latencies) / n
            stats['sample_count'] = n
        
        # Calculate per-SQ statistics
        for sq_name, sq_stats in stats['per_sq_stats'].items():
            sq_latencies = sq_stats['latencies']
            if sq_latencies:
                sq_stats['miss_rate'] = sq_stats['misses'] / sq_stats['checks'] if sq_stats['checks'] > 0 else 0.0
                sq_stats['latency_avg_us'] = sum(sq_latencies) / len(sq_latencies)
                sq_stats['latency_max_us'] = max(sq_latencies)
        
        return stats
    
    def get_deadline_summary(self) -> str:
        """
        Get human-readable deadline metrics summary.
        
        :return: Formatted string with deadline statistics
        """
        stats = self._get_deadline_statistics()
        
        summary_lines = [
            "=== Deadline Metrics Summary ===",
            f"Total Checks: {stats['total_checks']}",
            f"Total Misses: {stats['total_misses']}",
            f"Miss Rate: {stats['miss_rate']:.2%}",
            f"PlanB Executions: {stats['planb_executions']}",
            "",
            "Latency Statistics:",
            f"  Best Case: {stats['best_case_latency_us']:,.0f} us",
            f"  P50: {stats.get('latency_p50_us', 0):,.0f} us",
            f"  P95: {stats.get('latency_p95_us', 0):,.0f} us", 
            f"  P99: {stats.get('latency_p99_us', 0):,.0f} us",
            f"  Worst Case: {stats['worst_case_latency_us']:,.0f} us",
            "",
            "Per-SQ Statistics:"
        ]
        
        for sq_name, sq_stats in stats['per_sq_stats'].items():
            summary_lines.append(
                f"  {sq_name}: {sq_stats['checks']} checks, "
                f"{sq_stats.get('miss_rate', 0):.1%} miss rate, "
                f"avg={sq_stats.get('latency_avg_us', 0):,.0f}us"
            )
        
        return "\n".join(summary_lines)
    
    def _derive_timing_constants_from_app(self):
        """Derive all timing constants from application temporal requirements."""
        
        # Analyze application timing characteristics
        min_period = float('inf')
        has_deadlines = False
        
        if hasattr(self, 'instantiated_graphs'):
            for app_name, graph_data in self.instantiated_graphs.items():
                graph = graph_data.get('graph')
                if graph and hasattr(graph, 'sqs'):
                    for sq in graph.sqs:
                        if hasattr(sq, 'period'):
                            min_period = min(min_period, sq.period)
                        if hasattr(sq, 'deadline'):
                            has_deadlines = True
        
        # Derive timing profile based on application characteristics
        if has_deadlines and min_period < 10:  # Real-time system
            heartbeat = 2.0
            grace_multiplier = 4
            return {
                'heartbeat_interval': heartbeat,
                'grace_period_base': heartbeat * grace_multiplier,  # 8 seconds
                'stability_window': 120.0,      # 2 minutes stability
                'reliability_window': 300.0,    # 5 minutes reliability
                'profile': 'real-time'
            }
        elif min_period < 60:  # Fast periodic system  
            heartbeat = 5.0
            grace_multiplier = 6
            return {
                'heartbeat_interval': heartbeat,
                'grace_period_base': heartbeat * grace_multiplier,  # 30 seconds
                'stability_window': 300.0,      # 5 minutes stability
                'reliability_window': 1800.0,   # 30 minutes reliability
                'profile': 'fast-periodic'
            }
        else:  # Slower batch system
            heartbeat = 10.0
            grace_multiplier = 6
            return {
                'heartbeat_interval': heartbeat,
                'grace_period_base': heartbeat * grace_multiplier,  # 60 seconds
                'stability_window': 600.0,      # 10 minutes stability
                'reliability_window': 7200.0,   # 2 hours reliability
                'profile': 'batch-processing'
            }
    
    def _derive_reliability_window_from_app(self):
        """Derive reliability window from application temporal requirements."""
        
        # Analyze compiled application timing
        if hasattr(self, 'instantiated_graphs'):
            min_period = float('inf')
            has_deadlines = False
            
            for app_name, graph_data in self.instantiated_graphs.items():
                graph = graph_data.get('graph')
                if graph and hasattr(graph, 'sqs'):
                    for sq in graph.sqs:
                        # Check for timing constraints
                        if hasattr(sq, 'period'):
                            min_period = min(min_period, sq.period)
                        if hasattr(sq, 'deadline'):
                            has_deadlines = True
            
            # Derive window based on application characteristics
            if has_deadlines and min_period < 10:  # Real-time system (sub-10 second periods)
                reliability_window = 300    # 5 minutes
                self.logger.info("Real-time system detected - using 5-minute reliability window")
            elif min_period < 60:  # Fast periodic system (sub-minute periods)
                reliability_window = 1800   # 30 minutes  
                self.logger.info("Fast periodic system detected - using 30-minute reliability window")
            else:  # Slower system (minute+ periods)
                reliability_window = 7200   # 2 hours
                self.logger.info("Slower system detected - using 2-hour reliability window")
                
            return reliability_window
        
        # Default if no graphs available
        self.logger.info("No application graphs available - using default 1-hour reliability window")
        return 3600  # Default 1 hour

    def setup_proc_intfc(self, input_network_func, sim_process=None):
        '''
        Configure the interface to this process, meaning the callback functions
        for sending outputs to the other processes. This process needs a
        callback for each other runtime process, as it may receive inputs for
        any other process through the network.

        :param input_network_func: A callback function for providing
            ``Message`` inputs to the ``TTNetworkManager``

        :type input_network_func: functiond process that this class runs inside
            of. Mainly used for interrupting the simulated variant on input
            messages. dDefaults to None

        :param sim_process: A reference to the simulated process that this class
            runs inside of. Mainly used for interrupting the simulated variant
            on input messages. Defaults to None

        :type sim_process: ``simpy.Process`` | None
        '''
        self.input_network_func = input_network_func
        self.sim_process = sim_process

    def input_msg(self, message):
        '''
        Callback use to provide messages to this process's input queue.

        If this is a simulated environment, we interrupt the process, which is
        otherwise waiting indefinitely for data to arrive on the queue.

        :param message: The message intended for this same ensemble

        :type message: Message
        '''
        self.input_queue.put(message)
        if (self.sim is not None and self.sim_process is not None
                and self.sim.active_process != self.sim_process):
            self.logger.log(2, 'Interrupting!: t=%f', self.sim.now)
            # would this generate too many interrupts if there are many inputs
            # all at one time?
            self.sim_process.interrupt()


    def get_next_input(self):
        '''
        Pull the next input off the input queue.
        '''
        if self.sim is not None:
            return self.input_queue.get_nowait()
        else:
            # FIXME: timeout value should be more configurable
            return self.input_queue.get(block=True, timeout=1)

    def run_sim(self, sim):
        '''
        The main run loop for a runtime environment using simulated processes,
        which runs on a single core and can implement many ensembles. Must be
        run as a ``simpy.Process``
        '''
        try:
            import simpy
        except ImportError:
            self.logger.error("simpy module not installed - simulation not available")
            return

        self.sim = sim
        self.logger.debug('run sim loop RuntimeManager')
        next_msg = None
        try:
            while True:
                try:
                    next_msg = self.get_next_input()
                except queue.Empty:
                    try:
                        yield self.sim.timeout(math.inf)
                    except simpy.Interrupt:
                        continue
                except simpy.Interrupt:
                    continue
                if next_msg is not None:
                    self.logger.debug(f'*** Message: {next_msg}')
                    self.handle_message(next_msg)
                next_msg = None
        except KeyboardInterrupt:
            raise
        except GeneratorExit as e:
            self.logger.debug('runtime simpy generator exited')
            raise e
        except BaseException as e:
            self.logger.exception(f'Base exception {e}')
            raise

    def run_phy(self, input_network_func):
        '''
        The main run loop for a runtime environment using physical processes,
        which can take advantage of multi-core processors.
        '''
        self.logger.debug('run phy loop RuntimeManager')
        self.logger.debug(f'runtime mgr is on pid {os.getpid()}')
        self.input_network_func = input_network_func
        try:
            while True:
                try:
                    next_msg = self.get_next_input()
                except queue.Empty:
                    next_msg = None

                if next_msg is not None:
                    self.handle_message(next_msg)

        except (FinishedException, KeyboardInterrupt):
            return
        except Exception:
            self.logger.exception(f'RTM has ended')
            raise
        finally:
            del self.input_network_func

    def handle_message(self, msg):
        '''
        Respond to an incoming message meant for this process. If the message
        type and recipient do not match expectations, this will return without
        notification
        '''


        if not isinstance(msg, Message):
            return
        if not isinstance(msg.msg_type, RuntimeMsg):
            return
        if not msg.process_recipient == Recipient.ProcessRuntimeManager:
            return

        msg_type = msg.msg_type
        self.logger.debug('New message %s', msg)

        if msg_type == RuntimeMsg.LogOutputToken:
            # user-defined output function to log
            self.output_func(msg)

        elif msg_type == RuntimeMsg.DeviceHeartbeat:
            # Handle device heartbeat - payload is a dict with device status info
            heartbeat_data = msg.payload
            device_id = heartbeat_data.get('device_id') if isinstance(heartbeat_data, dict) else heartbeat_data
            self.device_heartbeats[device_id] = time.time()
            self.logger.debug(f"Heartbeat received from {device_id}")

        elif msg_type == RuntimeMsg.DeviceHealthQuery:
            pass

        elif msg_type == RuntimeMsg.DeviceHealthResponse:
            device_id = msg.payload.get('device_id')
            is_healthy = msg.payload.get('healthy', False)
            self._handle_health_response(device_id, is_healthy)

        elif msg_type == RuntimeMsg.DeviceStatusUpdate:
            device_id = msg.payload.get('device_id')
            new_state = msg.payload.get('state')
            self._update_device_state(device_id, new_state)
    
        elif msg_type == RuntimeMsg.DeviceStabilityConfirmed:
            device_id = msg.payload.get('device_id')
            self._handle_device_stability_confirmed(device_id)

        elif msg_type == RuntimeMsg.DeviceWelcome:
            # This is received if a device echoes back confirmation
            # (optional - for debugging/verification)
            device_id = msg.payload.get('device_id') if isinstance(msg.payload, dict) else None
            if device_id:
                self.logger.debug(f"DeviceWelcome acknowledged by {device_id}")    
    
        elif msg_type == RuntimeMsg.MigrationAssessmentRequest:
            device_id = msg.payload.get('device_id')
            self._assess_migration_opportunities(device_id)

        elif msg_type == RuntimeMsg.InstantiateAndMapGraph:
            # Instantiate the graph by mapping it to ensembles. Currently, that
            # mapping happens here at runtime, but it could be done statically
            # prior to this, so long as the set of ensembles in the expected
            # system match those that are actually connected by the time this
            # message arrives
            if not isinstance(msg.payload, tuple):
                graph:TTGraph = msg.payload
                qpf_mapping = None
            else:
                # QPF integration: payload can be (graph, mapping) or (graph, mapping, metadata)
                graph:TTGraph = msg.payload[0]
                qpf_mapping = msg.payload[1] if len(msg.payload) >= 2 else None

            assert isinstance(
                graph,
                TTGraph), 'Graph should be a TTGraph, output from the compiler'

            # Extract strategy from metadata if provided
            requested_strategy = None
            metadata = {}
            if isinstance(msg.payload, tuple) and len(msg.payload) >= 3 and isinstance(msg.payload[2], dict):
                metadata = msg.payload[2]
                requested_strategy = metadata.get('strategy')

            # Compute mapping based on strategy
            # Load deployment spec from the graph (compiled at compile time)
            if not self.deployment_spec:
                self._load_deployment_spec_from_graph(graph)
            
            # Use deployment_spec for mapping (design-time devices)
            available_for_mapping = self._spec_to_ensemble_info(self.deployment_spec) if self.deployment_spec else list(self.connected_ensembles.values())

            if not available_for_mapping:
                self.logger.error("No devices available for mapping (neither deployment_spec nor connected_ensembles populated)")
                return

            if qpf_mapping is not None:
                self.logger.info("Using QPF-optimized mapping")
                mapped_sqs = self._validate_mapping_against_spec(graph, qpf_mapping)
                if mapped_sqs is None:
                    self.logger.warning("QPF mapping validation failed, using static mapping")
                    mapped_sqs = Mapper.static_mapping(graph, available_for_mapping)
                    requested_strategy = 'static'
                else:
                    mapped_sqs = qpf_mapping  # Validated mapping
                    requested_strategy = 'qpf'
            elif requested_strategy == 'random':
                self.logger.info("Using random mapping")
                mapped_sqs = Mapper.TTMapper.random_mapping(graph, available_for_mapping)
            elif requested_strategy == 'trivial':
                self.logger.info("Using trivial mapping")
                mapped_sqs = Mapper.TTMapper.trivial_mapping(graph, available_for_mapping[0])
            elif requested_strategy == 'qpf' or (requested_strategy is None and self.deployment_spec):
                # QPF: explicitly requested via CLI, or default when deployment_spec exists
                objective = metadata.get('objective', 'makespan')
                self.logger.info(f"Using QPF optimization (objective={objective})")
                try:
                    from . import SmartMapper as SM
                    from .UnifiedGraph import UnifiedPlacementGraph
                    
                    applications = {graph.graph_name: {'graph': graph}}
                    ensembles = {ens.name: ens for ens in available_for_mapping}
                    
                    unified = UnifiedPlacementGraph(applications, ensembles)
                    mapper = SM.SmartMapper(unified)
                    mapped_sqs = mapper.optimize(objective=objective, trials=1000)
                    
                    if not mapped_sqs:
                        raise ValueError("QPF returned empty mapping")
                    
                    requested_strategy = 'qpf'
                    self.placement_alternatives = getattr(mapper, 'placement_alternatives', {})
                    self.optimization_objective = objective
                    self.logger.info(f"QPF optimization complete: {len(mapped_sqs)} SQs mapped")
                except Exception as e:
                    self.logger.warning(f"QPF optimization failed ({e}), falling back to static")
                    mapped_sqs = Mapper.static_mapping(graph, available_for_mapping)
                    requested_strategy = 'static'
            else:
                self.logger.info("Using static mapping")
                mapped_sqs = Mapper.static_mapping(graph, available_for_mapping)
                requested_strategy = 'static'

            # Populate metadata with resolved strategy for downstream consumers
            # (pending_graphs, _execute_graph_deployment, RuntimeAdapter)
            metadata['strategy'] = requested_strategy
            if requested_strategy == 'qpf':
                metadata['placement_alternatives'] = getattr(self, 'placement_alternatives', {})
                metadata['objective'] = getattr(self, 'optimization_objective', 'makespan')

            # Check if all required devices have joined
            required_devices = set(mapped_sqs.values())
            connected_devices = set(self.connected_ensembles.keys())

            if not required_devices.issubset(connected_devices):
                # Not all devices ready - add to pending graphs
                missing_devices = required_devices - connected_devices
                self.logger.info(f"Graph {graph.graph_name} requires devices not yet connected: {missing_devices}")
                self.logger.info(f"Adding to pending graphs, will execute when devices join")
                
                self.pending_graphs[graph.graph_name] = (graph, mapped_sqs, metadata)
                return  # Don't proceed with instantiation yet
            
            mapped_ports = Mapper.generate_mapping(graph, mapped_sqs)
            self.logger.debug(f'Mapped Ports: {mapped_ports}')

            # Single-app contention detection: parallel branches (fork-join)
            # may place multiple SQs on the same device with overlapping
            # execution windows. Detect and generate allocation_metadata.
            app_id = graph.graph_name or 'default'
            try:
                from . import SmartMapper as SM
                contention_mapper = SM.SmartMapper(None)
                contention_scenarios = contention_mapper.detect_single_app_contention(
                    graph, mapped_sqs,
                    device_profile_manager=self.device_profile_manager
                )
                if contention_scenarios:
                    self.multitenancy_scenarios = contention_scenarios
                    # Attempt concurrent upgrade (uses device_runtime_state)
                    single_app_mappings = {app_id: mapped_sqs}
                    self._upgrade_all_devices_to_concurrent_if_possible(single_app_mappings)
                    self.logger.info(f"Single-app contention analysis: "
                                   f"{len(contention_scenarios)} devices with contention")
            except Exception as e:
                self.logger.warning(f"Single-app contention detection failed: {e}")
                import traceback
                self.logger.debug(traceback.format_exc())

            # distribute the clocks to each ensemble. This is before sending SQs
            # because the SQ instantiation process often searches for a clock
            # that will be used for marking new TTTime's or setting local
            # timeouts. The clocks should already be known to those ensembles.
            # FIXME: only send the necessary clocks to each ensemble
            for ens_name in list(self.connected_ensembles.keys()):
                # send the default clock if none were specified in the program.
                clock_list = graph.clock_dictionary
                if 0 == len(clock_list):
                    # TODO: what are our default clocks? clock specification
                    # seems overengineered, ideally remove Clock notion
                    self.logger.warning(
                        'Clock Dictionary is empty. Execute at your own risk.')
                    clock_list = {'root_clock': Clock.TTClock.root()}

                msg_clocks_sync = Message(
                    SyncMsg.AddClocks, list(clock_list.values()),
                    Recipient.ProcessInputTokens)
                msg_clocks_execute = Message(
                    ExecuteMsg.AddClocks,
                    list(clock_list.values()),
                    Recipient.ProcessExecute)
                # Should the network manager have any knowledge of clocks?
                # potential TODO.
                network_payload = (ens_name,
                                   [msg_clocks_sync, msg_clocks_execute])
                network_msg = Message(NetMsg.ForwardNetworkMessage,
                                      network_payload,
                                      Recipient.ProcessNetwork)
                self.logger.info('Send clocks to Ensemble(%s)', ens_name)
                self.input_network_func(network_msg)
            self.logger.debug('Done sending clocks')

            # for each SQ, make a message to send the sync and execute parts.
            # Arc destinations should be held in the output_arc's list of
            # destinations, which go into the SQForward. Send the 3 messages to
            # the same recipient ensemble (all wrapped into an array of
            # Messages)
            for this_sq in graph.sqs:
                assert isinstance(this_sq, TTSQ), 'graph.sq_list should only contain TTSQ\'s'
                
                # key is SQ name, value is the name of the ensemble it should be mapped to
                ensemble_name = mapped_sqs[this_sq.sq_name]
                self.logger.debug(f'sending SQ {this_sq} to {ensemble_name}')

                # Get allocation metadata (None if SQ has no contention)
                allocation_metadata = self._get_allocation_metadata(
                    app_id, this_sq.sq_name, ensemble_name)

                msg_instatiate_sync = Message(
                    SyncMsg.InstantiateSQ, this_sq.generate_runtime_sqsync(),
                    Recipient.ProcessInputTokens)
                msg_instantiate_execute = Message(ExecuteMsg.InstantiateSQ,
                                                  (TTSQExecute(this_sq), allocation_metadata),
                                                  Recipient.ProcessExecute)
                msg_instantiate_forwarding = Message(
                    NetMsg.InstantiateSQ,
                    (this_sq.sq_name, mapped_ports[this_sq], allocation_metadata),
                    Recipient.ProcessNetwork)

                network_payload = (ensemble_name, [
                    msg_instatiate_sync, msg_instantiate_execute,
                    msg_instantiate_forwarding
                ])
                network_msg = Message(NetMsg.ForwardNetworkMessage,
                                      network_payload,
                                      Recipient.ProcessNetwork)
                self.input_network_func(network_msg)


            # Use strategy determined during mapping computation
            deployment_strategy = requested_strategy
            
            # Extract QPF-specific metadata (only meaningful for QPF)
            placement_alternatives = {}
            optimization_objective = 'makespan'
            if deployment_strategy == 'qpf':
                placement_alternatives = metadata.get('placement_alternatives', {})
                optimization_objective = metadata.get('objective', 'makespan')

            # Store deployment artifacts
            self.instantiated_graphs[graph.graph_name] = (graph, mapped_sqs)
            self.deployed_port_wiring[graph.graph_name] = {
                sq.sq_name: mapped_ports[sq] for sq in graph.sqs
            }
            
            # Store deployment metadata for runtime adaptation
            self.deployment_metadata[graph.graph_name] = {
                'strategy': deployment_strategy,
                'placement_alternatives': placement_alternatives,
                'objective': optimization_objective
            }
            
            # Configure RuntimeAdapter with deployment-specific parameters
            self._configure_runtime_adapter(
                graph=graph,
                mapping=mapped_sqs,
                strategy=deployment_strategy,
                placement_alternatives=placement_alternatives,
                optimization_objective=optimization_objective
            )

            self.logger.info(f"Graph {graph.graph_name} successfully instantiated with {deployment_strategy} strategy")


        elif msg_type == RuntimeMsg.InstantiateAndMapMultipleGraphs:
            # Batch deployment of multiple applications
            graphs, app_ids = msg.payload
            
            self.logger.info(f'Batch deploying {len(graphs)} applications: {app_ids}')
            
            # Build app_configs from inputs
            app_configs = {}
            for i, (graph, app_id) in enumerate(zip(graphs, app_ids)):
                assert isinstance(graph, TTGraph), f'Graph {i} should be a TTGraph'
                
                app_configs[app_id] = {
                    'graph': graph,
                }
                
                self.logger.debug(f"App {app_id}: graph={graph.graph_name}")
            
            # Store for later use
            self.app_configs.update(app_configs)
            
            # Load deployment spec from the first graph if not already loaded
            if not self.deployment_spec:
                first_graph = list(app_configs.values())[0]['graph']
                self._load_deployment_spec_from_graph(first_graph)
            
            # Get available devices for mapping
            if self.deployment_spec:
                available_ensemble_infos = self._spec_to_ensemble_info(self.deployment_spec)
                self.logger.info(f"Using {len(available_ensemble_infos)} devices from deployment spec for batch coordination")
            elif self.connected_ensembles:
                available_ensemble_infos = list(self.connected_ensembles.values())
                self.logger.info(f"Using {len(available_ensemble_infos)} connected devices for batch coordination")
            else:
                self.logger.error("No devices available for batch deployment (neither deployment_spec nor connected_ensembles)")
                return
            
            # Try coordinated QPF mapping first, fallback to static
            mapper = None
            try:
                from . import SmartMapper as SM
                from .UnifiedGraph import UnifiedPlacementGraph
                
                # Build unified graph with available devices
                unified_apps = {app_id: {'graph': cfg['graph']} 
                               for app_id, cfg in app_configs.items()}
                ensemble_dict = {ens.name: ens for ens in available_ensemble_infos}
                unified_graph = UnifiedPlacementGraph(unified_apps, ensemble_dict)
                
                mapper = SM.SmartMapper(unified_graph)
                all_mappings = mapper.coordinated_qpf_mapping(
                    graphs, 
                    available_ensemble_infos,
                    app_configs,
                    self.device_allocations,
                    device_profile_manager=self.device_profile_manager
                )
                deployment_strategy = 'qpf'
                self.logger.info("Using QPF-optimized coordinated mapping")
            except Exception as e:
                self.logger.warning(f"QPF coordination failed ({e}), using static fallback")
                import traceback
                self.logger.debug(f"QPF error details: {traceback.format_exc()}")
                all_mappings = Mapper.coordinated_static_mapping(
                    graphs,
                    available_ensemble_infos,
                    app_configs,
                    self.device_allocations
                )
                deployment_strategy = 'static'
            
            # Get placement alternatives from QPF mapper if available
            placement_alternatives = {}
            optimization_objective = 'makespan'
            if mapper is not None:
                if hasattr(mapper, 'get_placement_alternatives'):
                    placement_alternatives = mapper.get_placement_alternatives()
                if hasattr(mapper, 'get_optimization_objective'):
                    optimization_objective = mapper.get_optimization_objective() or 'makespan'

            # Retrieve multitenancy scenarios from mapper if computed
            if mapper is not None and hasattr(mapper, 'multitenancy_scenarios'):
                self.multitenancy_scenarios = mapper.multitenancy_scenarios
                self.logger.info(f"Retrieved {len(self.multitenancy_scenarios)} device scenarios")
            else:
                self.multitenancy_scenarios = {}
            
            # Upgrade shared devices to concurrent execution if possible (BEFORE deployment)
            self._upgrade_all_devices_to_concurrent_if_possible(all_mappings)

            # Check if all required devices are ready (for deployment_spec scenario)
            if self.deployment_spec:
                required_devices = set()
                for app_mapping in all_mappings.values():
                    required_devices.update(app_mapping.values())
                
                connected_devices = set(self.connected_ensembles.keys())
                missing_devices = required_devices - connected_devices
                
                if missing_devices:
                    self.logger.warning(f"Batch deployment requires devices not yet connected: {missing_devices}")
                    self.logger.info("Adding apps to pending_graphs, will deploy when devices join")
                    
                    # Store each app for later deployment
                    for app_id, app_mapping in all_mappings.items():
                        graph = app_configs[app_id]['graph']
                        graph_key = f"{app_id}_{graph.graph_name}"
                        metadata = {
                            'strategy': deployment_strategy,
                            'placement_alternatives': placement_alternatives,
                            'objective': optimization_objective,
                            'app_config': app_configs[app_id]
                        }
                        self.pending_graphs[graph_key] = (graph, app_mapping, metadata)
                    
                    self.logger.info(f"Queued {len(all_mappings)} apps for deployment when devices ready")
                    return

            # All devices are ready - deploy each app
            successfully_deployed = 0
            for app_id, app_mapping in all_mappings.items():
                try:
                    graph = app_configs[app_id]['graph']
                    self._deploy_single_app(app_id, graph, app_mapping, app_configs[app_id],
                                            deployment_strategy=deployment_strategy,
                                            placement_alternatives=placement_alternatives,
                                            optimization_objective=optimization_objective)
                    successfully_deployed += 1
                except Exception as e:
                    self.logger.error(f"Failed to deploy app {app_id}: {e}")
                    import traceback
                    self.logger.error(traceback.format_exc())
            
            self.logger.info(f'Successfully deployed {successfully_deployed}/{len(all_mappings)} applications')

        elif msg_type == RuntimeMsg.InstantiateAndMapCombinedGraph:
            # Deploy a CombinedGraph with proper multitenancy handling
            combined_graph, deployment_config = msg.payload
            
            self.logger.info(f'Deploying CombinedGraph: {combined_graph.graph_name}')
            self.logger.info(f'Applications: {combined_graph.app_ids}')
            
            # Verify this is a CombinedGraph
            if not hasattr(combined_graph, 'is_combined_graph') or not combined_graph.is_combined_graph():
                self.logger.error("Payload is not a CombinedGraph - use InstantiateAndMapGraph instead")
                return
            
            # Load deployment spec
            if not self.deployment_spec:
                self._load_deployment_spec_from_graph(combined_graph)
            
            # converts those specs into TTEnsembleInfo objects that SmartMapper can work with 
            if self.deployment_spec:
                available_ensemble_infos = self._spec_to_ensemble_info(self.deployment_spec)
            elif self.connected_ensembles:
                available_ensemble_infos = list(self.connected_ensembles.values())
            else:
                self.logger.error("No devices available for CombinedGraph deployment")
                return
            
            # Determine strategy and objective from deployment config
            requested_strategy = deployment_config.get('strategy', 'qpf')
            objective = deployment_config.get('objective', 'makespan')
            placement_alternatives = {}

            # CombinedGraph only supports QPF and static strategies
            if requested_strategy in ('random', 'trivial'):
                self.logger.warning(
                    f"Strategy '{requested_strategy}' not supported for CombinedGraph, "
                    f"falling back to static")
                requested_strategy = 'static'

            if requested_strategy == 'qpf':
                # QPF optimization
                try:
                    from . import SmartMapper as SM
                    
                    mapper = SM.SmartMapper(None)  # UnifiedGraph built internally
                    flat_mapping, app_mappings = mapper.optimize_combined_graph(
                        combined_graph,
                        available_ensemble_infos,
                        objective=objective,
                        trials=deployment_config.get('trials', 1000),
                        device_profile_manager=self.device_profile_manager
                    )
                    
                    deployment_strategy = 'qpf'
                    placement_alternatives = getattr(mapper, 'placement_alternatives', {})
                    
                    # Retrieve multitenancy scenarios
                    if hasattr(mapper, 'multitenancy_scenarios'):
                        self.multitenancy_scenarios = mapper.multitenancy_scenarios
                        self.logger.info(f"Retrieved {len(self.multitenancy_scenarios)} multitenancy scenarios")
                    
                except Exception as e:
                    self.logger.warning(f"CombinedGraph QPF failed ({e}), using static fallback")
                    import traceback
                    self.logger.debug(traceback.format_exc())
                    requested_strategy = 'static'  # Fall through to static below

            if requested_strategy != 'qpf':
                # Static fallback (explicit or after QPF failure)
                flat_mapping = {}
                for sq in combined_graph.sqs:
                    constraints = getattr(sq, 'constraints', []) or []
                    for ens in available_ensemble_infos:
                        if not constraints or Query.TTQuery(constraints, Query.QueryOp.AND).test(ens):
                            flat_mapping[sq.sq_name] = ens.name
                            break
                    else:
                        flat_mapping[sq.sq_name] = RUNTIME_MANAGER_ENSEMBLE_NAME
                
                app_mappings = combined_graph.decompose_mapping(flat_mapping)
                deployment_strategy = 'static'
            
            # Store app configs
            for app_id in combined_graph.app_ids:
                self.app_configs[app_id] = combined_graph.get_app_config(app_id)
            
            # Upgrade shared devices to concurrent if possible
            self._upgrade_all_devices_to_concurrent_if_possible(app_mappings)
            
            # Check if all devices are ready
            required_devices = set(flat_mapping.values())
            connected_devices = set(self.connected_ensembles.keys())
            missing_devices = required_devices - connected_devices
            
            if missing_devices:
                self.logger.warning(f"CombinedGraph requires devices not yet connected: {missing_devices}")
                # Queue for later deployment
                metadata = {
                    'strategy': deployment_strategy,
                    'flat_mapping': flat_mapping,
                    'app_mappings': app_mappings,
                    'placement_alternatives': placement_alternatives,
                    'objective': objective,
                }
                self.pending_graphs[combined_graph.graph_name] = (combined_graph, flat_mapping, metadata)
                return
            
            # Deploy the combined graph
            self._deploy_combined_graph(
                combined_graph, flat_mapping, app_mappings,
                deployment_strategy=deployment_strategy,
                placement_alternatives=placement_alternatives,
                optimization_objective=objective
            )

        elif msg_type == RuntimeMsg.DeployApp:
            # Incremental single-app deployment
            graph, app_config = msg.payload
            app_id = app_config.get('app_id', f'app_{len(self.applications)}')
            
            self.logger.info(f'Deploying app {app_id} incrementally')
            
            # Store app config
            self.app_configs[app_id] = app_config
            app_config['graph'] = graph
            
            # Try to place immediately
            placed = self._try_place_app(app_id, app_config)
            
            if not placed:
                self.logger.info(f'App {app_id} queued, waiting for resources')
                
                self.pending_apps.append({
                    'app_id': app_id,
                    'config': app_config,
                    'arrived_at': time.time()
                })
        
        elif msg_type == RuntimeMsg.TerminateApp:
            # Release resources for terminated app
            app_id = msg.payload
            self._terminate_app(app_id)


        elif msg_type == RuntimeMsg.ExecuteGraphOnInputs:
            # Start execution of the graph by sending the set of provided inputs
            # to all SQs that receive from graph inputs. Tokens will be produced
            # and percolate throughout the graph. Expected format is a graph and
            # a dictionary whose keys are input-arc symbols and values are
            # initial token values.
            graph = msg.payload[0]
            is_combined = hasattr(graph, 'is_combined_graph') and graph.is_combined_graph()
            assert isinstance(graph, TTGraph) or is_combined, (
                f'Graph should be a TTGraph or CombinedGraph, got {type(graph)}')
            input_dict = msg.payload[1]
            graph_name = graph.graph_name

            graph_data = self.instantiated_graphs.get(graph_name)
            if graph_data is None:
                if graph_name in self.pending_graphs:
                    self.pending_inputs[graph_name] = (graph, input_dict)
                    self.logger.info(
                        f"Graph '{graph_name}' not yet deployed — "
                        f"queuing input tokens for delivery after deployment")
                    return
                else:
                    self.logger.error(
                        f"Graph {graph_name} not found in instantiated_graphs "
                        f"or pending_graphs")
                    return
            _, mapping = graph_data

            self.logger.info(
                f"Starting execution of graph '{graph_name}' "
                f"with inputs {input_dict}", )

            arg_len = len(input_dict)
            params = graph.source_var_names()
            param_len = len(params)
            # check inputs vs. the input arcs
            assert arg_len == param_len, (
                f'The GRAPHified function {graph.graph_name} '
                f"has {param_len} parameters ({', '.join(params)}) "
                f"where only {arg_len} was provided "
                f"({(', ').join(input_dict)})")

            ipp_to_sq = graph.get_ipp_to_sq_dict()

            for argument in input_dict.keys():
                if argument not in params:
                    raise ValueError(f"Argument '{argument}' not found in "
                                     f"parameter list ({', '.join(params)}). "
                                     'kwargs currently not supported.')

            self.logger.debug('Input check passed')

            # find root clock for initial time values
            root_clock = None
            for clock_name in graph.clock_dictionary.keys():
                clock = graph.clock_dictionary.get(clock_name)
                if clock.is_root():
                    root_clock = clock

            # just assign to root clock if none is found
            if root_clock is None:
                self.logger.warning('No root clock specified, '
                                    'defaulting to the root clock.')
                root_clock = Clock.TTClock.root()

            # initial inputs carry infinite timestamps -- synchronization will
            # be trivial
            clock_spec = Clock.TTClockSpec.from_clock(root_clock)
            base_time = Time.TTTimeSpec.infinite(clock_spec)

            for argument, input_value in input_dict.items():
                dest_sqs = ipp_to_sq[argument]

                # create a token; we'll replicate it for each SQ
                base_tag = Tag.TTTag(context=Tag.DEFAULT_CONTEXT_ID)
                base_token = TTToken(input_value,
                                     base_time,
                                     is_streaming=False,
                                     tag=base_tag)

                for dest, port_num in dest_sqs:
                    # one output arc may have be used more than once in the same
                    # downstream SQ. We support this.

                    # duplicate the token and set tag components for where
                    # exactly this token should go
                    token_to_send = base_token.copy_token()
                    token_to_send.tag.sq = dest.sq_name
                    token_to_send.tag.p = port_num
                    token_to_send.tag.e = mapping[dest.sq_name]

                    # create a message to carry this token into the network
                    # interface on this ensemble then into the
                    # synchronization process on the recipient ensemble.
                    token_input_message = Message(
                        SyncMsg.InputToken, token_to_send,
                        Recipient.ProcessInputTokens)
                    network_msg_payload = (mapping[dest.sq_name],
                                            token_input_message)
                    token_input_network_message = Message(
                        NetMsg.ForwardNetworkMessage, network_msg_payload,
                        Recipient.ProcessNetwork)

                    self.input_network_func(token_input_network_message)

        elif msg_type == RuntimeMsg.JoinTickTalkSystem:
            # Enhanced device joining with capability assessment and optimization
            ensemble_info = msg.payload
            self._handle_enhanced_device_joining(ensemble_info)


        elif msg_type == RuntimeMsg.HandleDeviceFailure:
            # Device failure detected - use RuntimeAdapter to remap
            failed_device = msg.payload['device_id']
            current_time = msg.payload.get('timestamp', time.time())

            self.logger.warning(f"Device failure detected: {failed_device} at time {current_time}")
            
            if self.runtime_adapter is None:
                self.logger.error("No RuntimeAdapter available - cannot perform adaptive remapping")
                # Fall back to basic redistribution without adaptation
                self._handle_multiple_device_failures([failed_device])
                return

            # Update device state
            self._update_device_state(failed_device, 'FAILED')
            self._increment_failure_count(failed_device)

            # Get current mapping from all instantiated graphs and perform adaptation
            for graph_name, (graph, current_mapping) in self.instantiated_graphs.items():
                # Filter synthetic SQs — no runtime representation, cannot migrate
                synthetic_sqs = {'SUPER_TRIGGER', 'BARRIER_JOIN'}
                adaptable_mapping = {sq: dev for sq, dev in current_mapping.items()
                                    if sq not in synthetic_sqs}

                # Check if failed device has any real tasks from this graph
                affected_sqs = [sq for sq, device in adaptable_mapping.items() if device == failed_device]
                
                if not affected_sqs:
                    continue
                
                self.logger.info(f"Graph {graph_name}: {len(affected_sqs)} SQs affected by {failed_device} failure")
                
                # Call RuntimeAdapter's on_device_failure
                new_mapping = self.runtime_adapter.on_device_failure(
                    failed_device=failed_device,
                    current_mapping=adaptable_mapping,
                    available_topology=self.connected_ensembles,
                    device_states=self.device_states
                )

                if new_mapping is None:
                    self.logger.error(f"RuntimeAdapter could not generate valid remapping for {graph_name}")
                    continue
                
                self.logger.info(f"RuntimeAdapter generated new mapping for {graph_name}: {len(new_mapping)} SQs")

                # Redistribute SQs to new devices
                self._redistribute_sqs_after_failure(failed_device, new_mapping)
                
                # Merge synthetic SQs back for structural metadata
                full_mapping = dict(new_mapping)
                for sq, dev in current_mapping.items():
                    if sq in synthetic_sqs:
                        full_mapping[sq] = dev
                self.instantiated_graphs[graph_name] = (graph, full_mapping)
            
            self.logger.info(f"Successfully handled {failed_device} failure")

        # ========== DEADLINE METRICS HANDLERS (140-149) ==========
        elif msg_type == RuntimeMsg.DeadlineResult:
            # Handle deadline result from SQSync
            result = msg.payload
            self._record_deadline_result(
                sq_name=result.get('sq_name'),
                met=result.get('met'),
                actual_latency_us=result.get('actual_latency_us'),
                budget_us=result.get('budget_us')
            )

        elif msg_type == RuntimeMsg.DeadlineMetricsQuery:
            # Return deadline statistics
            stats = self._get_deadline_statistics()
            self.logger.info(f"Deadline metrics requested: {stats['total_checks']} checks, "
                            f"{stats['miss_rate']:.2%} miss rate")
            # Stats available via get_deadline_summary() or _get_deadline_statistics()

        elif msg_type == RuntimeMsg.EndExecution:
            raise FinishedException
        
        # Check device availability and pending apps at regular intervals
        current_time = time.time()
        if current_time - self.last_availability_check >= self.adaptation_interval:
            self._check_device_availability()
            self.last_availability_check = current_time

        # Check coordination (pending apps, preemption) at coordination intervals
        if current_time - self.last_coordination_check >= self.coordination_interval:
            self._process_pending_apps()
            self.last_coordination_check = current_time

    def _validate_qpf_mapping(self, graph, qpf_mapping):
        """Validate QPF mapping against current topology."""
        if not isinstance(qpf_mapping, dict):
            return None
    
        # Check that target devices are available
        available_devices = set(self.connected_ensembles.keys())
        target_devices = set(qpf_mapping.values())
    
        unavailable_devices = target_devices - available_devices
        if unavailable_devices:
            self.logger.error(f"QPF mapping targets unavailable devices: {unavailable_devices}")
            return None
    
        # Filter to only SQs in this graph
        graph_sq_names = {sq.sq_name for sq in graph.sqs}
        validated_mapping = {
            sq_name: device_name 
            for sq_name, device_name in qpf_mapping.items()
            if sq_name in graph_sq_names
        }
    
        return validated_mapping
    
    def _validate_mapping_against_spec(self, graph, mapping):
        """
        Validate a mapping against deployment_spec (design-time validation).
        
        This checks if the mapping references devices that SHOULD exist according
        to deployment.yaml, NOT whether they've physically joined (that happens later).
        
        :param graph: TTGraph to validate
        :param mapping: Proposed mapping {sq_name: device_id}
        :return: Validated mapping dict, or None if validation fails
        """
        if not isinstance(mapping, dict):
            self.logger.warning(f"Mapping is not a dict: {type(mapping)}")
            return None
        
        if not self.deployment_spec:
            # No deployment spec - fall back to runtime validation
            self.logger.info("No deployment_spec available, validating against connected_ensembles")
            return self._validate_qpf_mapping(graph, mapping)
        
        validated_mapping = {}
        
        for sq_name, device_name in mapping.items():
            # Check if device exists in deployment spec
            if device_name not in self.deployment_spec:
                self.logger.warning(
                    f"Mapping references device '{device_name}' not in deployment spec. "
                    f"Available devices: {list(self.deployment_spec.keys())}"
                )
                return None
            
            # Check if device type can run this SQ (constraint checking)
            device_spec = self.deployment_spec[device_name]
            
            # Find the SQ in graph
            sq = None
            for s in graph.sqs:
                if s.sq_name == sq_name:
                    sq = s
                    break
            
            if sq is None:
                self.logger.warning(f"SQ '{sq_name}' not found in graph")
                return None
            
            # Check constraints using TTPython's own Query system
            if hasattr(sq, 'constraints') and sq.constraints:
                from .Query import TTQuery, QueryOp, TTEnsembleInfo
                device_spec_obj = self.deployment_spec[device_name]
                components = device_spec_obj.get('components', {}) if isinstance(device_spec_obj, dict) else {}
                ens_info = TTEnsembleInfo(
                    name=device_name,
                    address="127.0.0.1:5000",
                    components=components
                )
                query = TTQuery(sq.constraints, QueryOp.AND)
                if not query.test(ens_info):
                    self.logger.warning(
                        f"SQ '{sq_name}' constraints not satisfied by device '{device_name}'"
                    )
                    return None
            
            validated_mapping[sq_name] = device_name
        
        self.logger.info(f"Mapping validated against deployment_spec: {len(validated_mapping)} SQs")
        return validated_mapping
    
    def _check_device_availability(self):
        """Device availability checking with false positive protection."""
        current_time = time.time()
    
        for device_id, last_heartbeat in self.device_heartbeats.items():
            time_since_heartbeat = current_time - last_heartbeat
            current_state = self.device_states.get(device_id, 'ACTIVE')
        
            # Apply exponential backoff based on reliability history
            consecutive_failures = self.device_reliability.get(device_id, 0)
            timeout_multiplier = min(2 ** consecutive_failures, 8)  # Max 8x timeout
            effective_timeout = self.heartbeat_timeout * timeout_multiplier
        
            if time_since_heartbeat > effective_timeout:
                if current_state == 'ACTIVE':
                    # Enter grace period - send health query
                    self._initiate_health_confirmation(device_id)
                    self._update_device_state(device_id, 'SUSPECTED')
                    self.logger.warning(f"Device {device_id} suspected failed, initiating confirmation")
                
                elif current_state == 'SUSPECTED':
                    # Check if grace period expired
                    grace_end = self.device_grace_periods.get(device_id, 0)
                    if current_time > grace_end:
                        # Confirm failure
                        self._update_device_state(device_id, 'FAILED')
                        self._increment_failure_count(device_id)
                    
                        if device_id in self.connected_ensembles:
                            self._handle_device_failure_confirmed(device_id)
                        
            else:
                # Device is responsive
                if current_state in ['SUSPECTED', 'FAILED']:
                    # Device reconnection detected
                    self._handle_device_reconnection(device_id)
                elif current_state == 'ACTIVE':
                    # Reset failure count on successful heartbeat
                    self.device_reliability[device_id] = 0
                elif current_state == 'REJOINING':
                    # Continue stability tracking
                    self._check_rejoining_device_stability(device_id)

        # Check rejoining device stability
        if current_time - self.last_stability_check >= self.stability_check_interval:
            for device_id in list(self.rejoining_devices.keys()):
                self._check_rejoining_device_stability(device_id)
            self.last_stability_check = current_time

    def _generate_device_unique_id(self, ensemble_info):
        """Generate cryptographically unique device identifier for lifetime tracking."""
        import hashlib
        import platform
        
        # Collect identifying information from ensemble
        id_components = [
            ensemble_info.name,                                    # Human-assigned name
            str(ensemble_info.address) if ensemble_info.address else 'no-network-addr',
            str(ensemble_info.components.get('type', 'unknown')), # Device type from catalog
            str(ensemble_info.components.get('mac_address', platform.node())), # Hardware identifier
            str(ensemble_info.components.get('serial_number', 'no-serial'))    # Hardware serial if available
        ]
        
        # Create deterministic hash that survives device reconnections
        id_string = "|".join(id_components)
        unique_hash = hashlib.sha256(id_string.encode()).hexdigest()
        unique_id = f"DEV_{unique_hash[:16]}"  # 20-character unique identifier
        
        self.logger.debug(f"Generated unique ID for {ensemble_info.name}: {unique_id}")
        self.logger.debug(f"ID components: {id_components}")
        
        return unique_id

    def _initialize_device_identity_tracking(self, device_name, ensemble_info):
        """Initialize comprehensive device identity and history tracking."""
        unique_id = self._generate_device_unique_id(ensemble_info)
        current_time = time.time()
        
        # Initialize device reliability history with unique identity
        if device_name not in self.device_reliability_history:
            self.device_reliability_history[device_name] = {
                'unique_id': unique_id,
                'total_failures': 0,
                'consecutive_failures': 0,
                'last_failure_time': None,
                'total_downtime_seconds': 0.0,
                'join_time': current_time,
                'last_heartbeat_time': current_time,
                'device_type': ensemble_info.components.get('type', 'unknown'),
                'network_address': str(ensemble_info.address) if ensemble_info.address else None,
                'reliability_score': 1.0,  # 1.0 = perfect, 0.0 = completely unreliable
                'failure_timestamps': []   # Full history of failure times
            }
        
        # Initialize device state tracking
        self.device_states[device_name] = 'ACTIVE'
        self.device_heartbeats[device_name] = current_time
        self.device_reliability[device_name] = 0  # Reset consecutive failures
        
        self.logger.info(f"Device identity tracking initialized: {device_name} (ID: {unique_id})")
        return unique_id

    def _recognize_rejoining_device(self, device_name, ensemble_info):
        """Recognize if rejoining device was previously part of cluster."""
        candidate_unique_id = self._generate_device_unique_id(ensemble_info)
        
        # Check if we have historical data for this unique device
        if device_name in self.device_reliability_history:
            stored_unique_id = self.device_reliability_history[device_name]['unique_id']
            
            if stored_unique_id == candidate_unique_id:
                # Same device rejoining - preserve reliability history
                history = self.device_reliability_history[device_name]
                downtime_duration = time.time() - (history.get('last_failure_time') or time.time())
                
                # Update downtime tracking
                if history.get('last_failure_time'):
                    history['total_downtime_seconds'] += downtime_duration
                    
                self.logger.info(f"Recognized rejoining device: {device_name} (ID: {candidate_unique_id})")
                self.logger.info(f"Historical reliability - Failures: {history['total_failures']}, Downtime: {history['total_downtime_seconds']:.1f}s")
                
                return True, history
            else:
                # Different device with same name - new device entirely
                self.logger.warning(f"Device name collision: {device_name} has different unique ID")
                self.logger.warning(f"Stored: {stored_unique_id}, Current: {candidate_unique_id}")
                return False, None
        
        # New device - no previous history
        self.logger.info(f"New device detected: {device_name} (ID: {candidate_unique_id})")
        return False, None

    def _update_device_reliability_score(self, device_name):
        """Calculate precise reliability score based on failure history and timing context."""
        if device_name not in self.device_reliability_history:
            return 1.0
        
        history = self.device_reliability_history[device_name]
        current_time = time.time()
        
        # Calculate time-based reliability metrics
        total_operational_time = current_time - history['join_time']
        uptime_ratio = 1.0 - (history['total_downtime_seconds'] / max(total_operational_time, 1.0))
        
        # Calculate failure frequency within reliability window
        recent_failures = [
            ts for ts in history['failure_timestamps'] 
            if current_time - ts <= self.reliability_time_window
        ]
        failure_frequency = len(recent_failures) / (self.reliability_time_window / 3600.0)  # failures per hour
        
        # Combine metrics for overall reliability score
        frequency_penalty = min(failure_frequency * 0.2, 0.8)  # Max 80% penalty for high failure frequency
        reliability_score = (uptime_ratio * 0.7) + ((1.0 - frequency_penalty) * 0.3)
        reliability_score = max(reliability_score, 0.1)  # Minimum 10% reliability
        
        history['reliability_score'] = reliability_score
        
        self.logger.debug(f"Device {device_name} reliability - Score: {reliability_score:.3f}, Uptime: {uptime_ratio:.3f}, Recent failures: {len(recent_failures)}")
        
        return reliability_score

    def _initiate_health_confirmation(self, device_id):
        """Send alternative health check to confirm device status."""
        current_time = time.time()
    
        # Set grace period
        grace_period = self.grace_period_base  # Use configured value
        self.device_grace_periods[device_id] = current_time + grace_period
    
        # Record health query
        self.pending_health_queries[device_id] = current_time
        self.logger.debug(f"Health confirmation initiated for {device_id}, grace period: {grace_period}s")

    def _handle_health_response(self, device_id, is_healthy):
        """Process health query response from device."""
        if is_healthy:
            self._handle_device_recovery(device_id)
            self.logger.info(f"Device {device_id} confirmed healthy via health query")
        else:
            self._update_device_state(device_id, 'FAILED')
            self._handle_device_failure_confirmed(device_id)
        
        # Clean up pending query
        self.pending_health_queries.pop(device_id, None)

    def _handle_device_recovery(self, device_id):
        """Handle device returning to healthy state - immediate recovery."""
        self._update_device_state(device_id, 'ACTIVE')
        self.device_reliability[device_id] = 0
        self.device_grace_periods.pop(device_id, None)
        self.pending_health_queries.pop(device_id, None)
    
        # Cancel any in-progress adaptations for this device
        if self.runtime_adapter and hasattr(self.runtime_adapter, 'cancel_ongoing_adaptation'):
            self.runtime_adapter.cancel_ongoing_adaptation(device_id)
    
        self.logger.info(f"Device {device_id} recovered quickly - no stability window required")

    def _handle_device_reconnection(self, device_id):
        """Handle device reconnection after being offline."""
        current_time = time.time()
    
        # Transition to REJOINING state
        self._update_device_state(device_id, 'REJOINING')
        self.rejoining_devices[device_id] = current_time
    
        # Initialize stability tracking in ensemble
        if device_id in self.connected_ensembles:
            ensemble_info = self.connected_ensembles[device_id]
            if hasattr(ensemble_info, 'start_stability_tracking'):
                ensemble_info.start_stability_tracking(current_time)
    
        self.logger.info(f"Device {device_id} reconnected - starting stability tracking")

    def _check_rejoining_device_stability(self, device_id):
        """Check if rejoining device has completed stability window."""
        if device_id not in self.rejoining_devices:
            return
        
        if device_id in self.connected_ensembles:
            ensemble_info = self.connected_ensembles[device_id]
            if (hasattr(ensemble_info, 'is_stability_window_complete') and 
                ensemble_info.is_stability_window_complete()):
                self._handle_device_stability_confirmed(device_id)

    def _handle_device_stability_confirmed(self, device_id):
        """Handle device completing stability window and becoming available."""
        # Transition to AVAILABLE state
        self._update_device_state(device_id, 'AVAILABLE')
    
        # Mark as migration eligible
        if device_id in self.connected_ensembles:
            ensemble_info = self.connected_ensembles[device_id]
            if hasattr(ensemble_info, 'mark_migration_eligible'):
                ensemble_info.mark_migration_eligible()
    
        # Clean up tracking
        self.rejoining_devices.pop(device_id, None)
    
        # Assess migration opportunities
        self._assess_migration_opportunities(device_id)
    
        self.logger.info(f"Device {device_id} stability confirmed - now available for migration")

    def _redistribute_sqs_after_failure(self, failed_device, new_mapping):
        """
        Redistribute SQs after device failure using stored deployment artifacts.
        
        INVARIANT: Port topology is immutable (from deployment).
        INVARIANT: Device addresses are resolved from current mapping.
        INVARIANT: Inactive SQs are explicitly tracked, never silently skipped.
        """
        if not new_mapping:
            self.logger.warning("No new mapping provided for redistribution")
            return
            
        self.logger.info(f"Redistributing {len(new_mapping)} SQs after {failed_device} failure")
        
        # Update the stored mapping in runtime_adapter
        if self.runtime_adapter and hasattr(self.runtime_adapter, 'current_mapping'):
            self.runtime_adapter.current_mapping = new_mapping
        
        # Track relocation outcomes
        relocated = []
        newly_inactive = []
        
        for sq_name, new_device in new_mapping.items():
            # Handle graceful degradation: explicit inactive tracking
            if new_device is None:
                newly_inactive.append(sq_name)
                continue
            
            # Find SQ object and graph
            sq_obj = None
            graph_name = None
            
            for gname, (graph, mapping) in self.instantiated_graphs.items():
                for sq in graph.sqs:
                    if sq.sq_name == sq_name:
                        sq_obj = sq
                        graph_name = gname
                        break
                if sq_obj:
                    break
            
            if sq_obj is None:
                self.logger.error(f"SQ {sq_name} not found in instantiated graphs")
                continue
            
            # Retrieve stored port wiring (topology from deployment)
            stored_ports = None
            if graph_name in self.deployed_port_wiring:
                stored_ports = self.deployed_port_wiring[graph_name].get(sq_name)
            
            if stored_ports is None:
                self.logger.error(f"No stored port wiring for SQ {sq_name}")
                continue
            
            # Resolve device addresses from current mapping
            resolved_ports = self._resolve_port_addresses(stored_ports, graph_name)
            
            # Send instantiation messages to new device
            msg_instantiate_sync = Message(
                SyncMsg.InstantiateSQ,
                sq_obj.generate_runtime_sqsync(),
                Recipient.ProcessInputTokens
            )
            msg_instantiate_execute = Message(
                ExecuteMsg.InstantiateSQ,
                (TTSQExecute(sq_obj), None),
                Recipient.ProcessExecute
            )
            msg_instantiate_forwarding = Message(
                NetMsg.InstantiateSQ,
                (sq_name, resolved_ports),
                Recipient.ProcessNetwork
            )
            
            network_payload = (new_device, [
                msg_instantiate_sync,
                msg_instantiate_execute,
                msg_instantiate_forwarding
            ])
            network_msg = Message(
                NetMsg.ForwardNetworkMessage,
                network_payload,
                Recipient.ProcessNetwork
            )
            
            if self.input_network_func:
                self.input_network_func(network_msg)
                relocated.append((sq_name, new_device))
            else:
                self.logger.error(f"Cannot relocate SQ {sq_name} - no network function")
        
        # Update inactive SQ tracking (explicit state, not silent skip)
        for sq_name in newly_inactive:
            for graph_name, (graph, _) in self.instantiated_graphs.items():
                if any(sq.sq_name == sq_name for sq in graph.sqs):
                    if graph_name not in self.inactive_sqs:
                        self.inactive_sqs[graph_name] = set()
                    self.inactive_sqs[graph_name].add(sq_name)
                    self.logger.info(f"SQ {sq_name} marked INACTIVE (graceful degradation)")
                    break
        
        # Update instantiated_graphs mapping
        for graph_name, (graph, old_mapping) in self.instantiated_graphs.items():
            updated_mapping = old_mapping.copy()
            for sq_name, new_device in new_mapping.items():
                if sq_name in updated_mapping and new_device is not None:
                    updated_mapping[sq_name] = new_device
            self.instantiated_graphs[graph_name] = (graph, updated_mapping)
        
        self.logger.info(f"Redistribution complete: {len(relocated)} relocated, {len(newly_inactive)} inactive")

    def _resolve_port_addresses(self, stored_ports, graph_name):
        """
        Resolve device addresses in stored port wiring from current mapping.
        
        Port TOPOLOGY is immutable (which SQ connects to which SQ).
        Port ADDRESSES are resolved from current placement at migration time.
        
        :param stored_ports: Port wiring from deployment (immutable topology)
        :param graph_name: Graph name to look up current mapping
        :return: Port wiring with addresses resolved to current placement
        """
        import copy
        
        if graph_name not in self.instantiated_graphs:
            self.logger.warning(f"Cannot resolve addresses - graph {graph_name} not found")
            return stored_ports
        
        _, current_mapping = self.instantiated_graphs[graph_name]
        inactive = self.inactive_sqs.get(graph_name, set())
        
        resolved_ports = []
        
        for port_info in stored_ports:
            resolved_port = copy.deepcopy(port_info)
            
            # Handle TTMappedPort objects (attribute-based)
            if hasattr(resolved_port, 'destination_sq') and hasattr(resolved_port, 'destination_device'):
                dest_sq = resolved_port.destination_sq
                if dest_sq in inactive:
                    self.logger.debug(f"Port destination {dest_sq} is inactive")
                elif dest_sq in current_mapping:
                    resolved_port.destination_device = current_mapping[dest_sq]
            
            # Handle tuple/list style: (dest_sq_name, dest_device, port_num, ...)
            elif isinstance(resolved_port, (list, tuple)) and len(resolved_port) >= 2:
                port_list = list(resolved_port)
                dest_sq = port_list[0]
                if isinstance(dest_sq, str):
                    if dest_sq in inactive:
                        self.logger.debug(f"Port destination {dest_sq} is inactive")
                    elif dest_sq in current_mapping:
                        port_list[1] = current_mapping[dest_sq]
                resolved_port = tuple(port_list) if isinstance(port_info, tuple) else port_list
            
            resolved_ports.append(resolved_port)
        
        return resolved_ports        

    def _assess_migration_opportunities(self, recovered_device):
        """Assess if tasks should be migrated back to recovered device."""
        if not self.runtime_adapter:
            return
        
        # Get current system mapping
        current_mapping = getattr(self.runtime_adapter, 'current_mapping', {})
        if not current_mapping:
            return
        
        # Check system load
        current_load = self._calculate_system_load()
        if current_load > 0.8:  # Don't migrate if system heavily loaded
            self.logger.info(f"System load too high ({current_load:.1f}) - skipping migration assessment")
            return
    
        # Calculate migration benefits
        migration_candidates = self.runtime_adapter.evaluate_migration_opportunities(
            recovered_device, current_mapping, self.connected_ensembles
        )
    
        if migration_candidates:
            self.logger.info(f"Found {len(migration_candidates)} migration candidates for {recovered_device}")
            # Execute gradual migration
            self._execute_gradual_migration(recovered_device, migration_candidates)
        else:
            self.logger.info(f"No beneficial migrations found for {recovered_device}")

    def _calculate_system_load(self):
        """Calculate current system load based on device utilization."""
        if (not self.runtime_adapter or 
            not hasattr(self.runtime_adapter, 'current_mapping') or 
            not self.runtime_adapter.current_mapping):
            return 0.0
        
        device_task_counts = {}
        for sq, device in self.runtime_adapter.current_mapping.items():
            device_task_counts[device] = device_task_counts.get(device, 0) + 1

        if not device_task_counts:
            return 0.0
        
        # Simple load metric: average tasks per device
        avg_load = sum(device_task_counts.values()) / len(self.connected_ensembles)
        max_capacity = 10  # Assume max 10 tasks per device
        return min(avg_load / max_capacity, 1.0)

    def _execute_gradual_migration(self, target_device, migration_candidates):
        """Execute gradual migration of tasks to recovered device."""
        if not self.runtime_adapter or not hasattr(self.runtime_adapter, 'current_mapping'):
            self.logger.warning("runtime_adapter or current_mapping not available")
            return
            
        # Sort by criticality and benefit
        sorted_candidates = sorted(migration_candidates, 
                                key=lambda x: (x['criticality_order'], x['benefit']), 
                                reverse=True)

        # Migrate one task at a time with monitoring
        for candidate in sorted_candidates[:3]:  # Limit to 3 initial migrations
            sq_name = candidate['sq_name']
            old_device = candidate['current_device']
        
            # Update mapping
            self.runtime_adapter.current_mapping[sq_name] = target_device
        
            self.logger.info(f"Migrated {sq_name}: {old_device} → {target_device} "
                            f"(benefit: {candidate['benefit']:.1f}%)")
        
            # Allow system to stabilize between migrations
            time.sleep(1)
    
        # Cancel any in-progress adaptations for this device
        # (Integration point for adaptation cancellation)

    def _handle_enhanced_device_joining(self, ensemble_info):
        """
        Enhanced device joining with deployment spec validation.
        
        This handles three cases:
        1. The RuntimeManager itself joining (coordinator, not a worker device)
        2. Pre-declared devices (in deployment.yaml) that are now physically connecting
        3. Dynamic devices (not in deployment.yaml) joining at runtime
        """
        device_name = ensemble_info.name
        
        # Case 1: RuntimeManager self-join — coordinator, not a worker device.
        # Just add to routing table. The RTM never gets SQs mapped to it and
        # its ensemble uses the original Component list (not the enhanced dict),
        # so it must not go through device validation.
        if device_name == RUNTIME_MANAGER_ENSEMBLE_NAME:
            self.logger.info(f"RuntimeManager '{device_name}' joining (coordinator)")
            self.connected_ensembles[device_name] = ensemble_info
            self._add_to_routing_table(ensemble_info)
            return True
        
        # Case 2: Pre-declared device from deployment.yaml
        if device_name in self.deployment_spec:
            self.logger.info(f"Pre-declared device '{device_name}' physically connecting")
            return self._handle_predeclared_device_joining(ensemble_info)
        
        # Case 3: Dynamic device joining at runtime
        self.logger.info(f"Dynamic device '{device_name}' joining (not in deployment spec)")
        return self._handle_dynamic_device_joining(ensemble_info)

    def _handle_predeclared_device_joining(self, ensemble_info):
        """
        Handle a pre-declared device (from deployment.yaml) physically joining.
        
        :param ensemble_info: TTEnsembleInfo from joining device
        :return: True if successful, False otherwise
        """
        device_name = ensemble_info.name
        expected_spec = self.deployment_spec[device_name]
        
        # Verify device capabilities match deployment spec
        if not self._verify_device_matches_spec(ensemble_info, expected_spec):
            self.logger.error(
                f"Device '{device_name}' capabilities don't match deployment spec! "
                f"Expected type: {expected_spec['type']}, "
                f"Got components: {ensemble_info.components}"
            )
            return False
        
        self.logger.info(f"Device '{device_name}' verified against deployment spec")
        
        # Add to connected_ensembles (NOW it's physically connected)
        self.connected_ensembles[device_name] = ensemble_info
        
        # Initialize device state as ACTIVE
        self._update_device_state(device_name, 'ACTIVE')
        
        # Initialize device identity tracking
        self._initialize_device_identity_tracking(device_name, ensemble_info)
        
        # Add to routing table
        self._add_to_routing_table(ensemble_info)
        
        # Initialize device runtime state for multitenancy
        device_components = ensemble_info.components or {}
        concurrent_capacity = device_components.get('concurrent_capacity',
                             device_components.get('cpu_cores', 1))
        supports_timeslicing = device_components.get('supports_timeslicing', True)
        
        self.device_runtime_state[device_name] = {
            'concurrent_capacity': concurrent_capacity,
            'supports_timeslicing': supports_timeslicing,
            'allocated_slots': {},
            'available_slots': concurrent_capacity
        }
        
        self.logger.info(f"Device '{device_name}' multitenancy: "
                        f"{concurrent_capacity} concurrent slots, "
                        f"timeslicing={'enabled' if supports_timeslicing else 'disabled'}")
        
        # Send welcome message with timing config
        self._send_device_welcome(ensemble_info)
        
        self.logger.info(f"Pre-declared device '{device_name}' now ACTIVE and ready")
        
        # Check if any pending graphs can now execute
        self._try_execute_pending_graphs()
        
        return True

    def _handle_dynamic_device_joining(self, ensemble_info):
        """
        Handle a dynamic device (NOT in deployment.yaml) joining at runtime.
        
        This is the original TTPython behavior - devices can join dynamically.
        
        :param ensemble_info: TTEnsembleInfo from joining device
        :return: True if successful, False otherwise
        """
        device_name = ensemble_info.name
        
        # Validate against universal catalog
        if not self._validate_device_capabilities(ensemble_info):
            self.logger.warning(f"Device {device_name} capability validation failed")
            return False
        
        # Initialize device identity and state tracking
        self._initialize_device_identity_tracking(device_name, ensemble_info)

        # Assess optimization benefit using QPF
        benefit = self._assess_new_device_optimization_benefit(ensemble_info)

        # Conservative integration decision. This must run before sending
        # DeviceWelcome, since both integration paths add the device's
        # routing-table entry that DeviceWelcome's ForwardNetworkMessage
        # relies on to resolve the device's network address.
        if self._should_integrate_device(device_name, benefit):
            self._integrate_new_device(ensemble_info, benefit)
        else:
            self._add_to_available_devices(ensemble_info)

        # Send DeviceWelcome with synchronized timing configuration
        self._send_device_welcome(ensemble_info)

        return True
    
    def _verify_device_matches_spec(self, ensemble_info, expected_spec):
        """
        Verify that a physically joining device matches its deployment spec.
        
        This ensures the device claiming to be "cav0" actually has the capabilities
        declared in deployment.yaml for "cav0".
        
        :param ensemble_info: TTEnsembleInfo from physically joining device
        :param expected_spec: Spec dict from deployment_spec
        :return: True if match, False otherwise
        """
        device_name = ensemble_info.name
        actual_components = ensemble_info.components
        
        # Check device type
        expected_type = expected_spec.get('type', 'unknown')
        actual_type = actual_components.get('type', 'unknown')
        
        if actual_type != expected_type:
            self.logger.warning(
                f"Device '{device_name}' type mismatch: "
                f"expected '{expected_type}', got '{actual_type}'"
            )
            # Allow type mismatch with warning (device might report type differently)
        
        # Check critical capabilities
        expected_slots = expected_spec.get('compute_slots', 1)
        actual_slots = actual_components.get('compute_slots', 1)
        
        if actual_slots < expected_slots:
            self.logger.error(
                f"Device '{device_name}' has fewer compute slots than expected: "
                f"expected {expected_slots}, got {actual_slots}"
            )
            return False
        
        expected_memory = expected_spec.get('memory_mb', 1024)
        actual_memory = actual_components.get('memory_mb', 1024)
        
        if actual_memory < expected_memory * 0.9:  # Allow 10% tolerance
            self.logger.error(
                f"Device '{device_name}' has less memory than expected: "
                f"expected {expected_memory}MB, got {actual_memory}MB"
            )
            return False
        
        # Check GPU requirement
        expected_gpu = expected_spec.get('has_gpu', False)
        actual_gpu = actual_components.get('has_gpu', False)
        
        if expected_gpu and not actual_gpu:
            self.logger.error(
                f"Device '{device_name}' missing required GPU: "
                f"expected GPU, got none"
            )
            return False
        
        self.logger.debug(f"Device '{device_name}' capabilities verified successfully")
        return True
    
    def _try_execute_pending_graphs(self):
        """
        Check if any pending graphs can now execute.
        
        Called whenever a device joins - checks if all required devices
        for pending graphs are now connected, and executes them if so.
        """
        if not self.pending_graphs:
            return
        
        self.logger.debug(f"Checking {len(self.pending_graphs)} pending graphs")
        
        graphs_to_execute = []
        
        for graph_name, (graph, mapped_sqs, metadata) in list(self.pending_graphs.items()):
            required_devices = set(mapped_sqs.values())
            connected_devices = set(self.connected_ensembles.keys())
            
            if required_devices.issubset(connected_devices):
                self.logger.info(
                    f"All devices ready for graph '{graph_name}': {required_devices}"
                )
                graphs_to_execute.append((graph_name, graph, mapped_sqs, metadata))
            else:
                missing = required_devices - connected_devices
                self.logger.debug(
                    f"Graph '{graph_name}' still waiting for devices: {missing}"
                )
        
        # Execute ready graphs
        for graph_name, graph, mapped_sqs, metadata in graphs_to_execute:
            self.logger.info(f"Executing pending graph '{graph_name}'")
            
            # Remove from pending
            del self.pending_graphs[graph_name]
            
            # Route to correct deployment method based on graph type
            is_combined = hasattr(graph, 'is_combined_graph') and graph.is_combined_graph()
            if is_combined:
                app_mappings = metadata.get('app_mappings', {})
                deployment_strategy = metadata.get('strategy', 'qpf')
                self._deploy_combined_graph(
                    graph, mapped_sqs, app_mappings,
                    deployment_strategy=deployment_strategy,
                    placement_alternatives=metadata.get('placement_alternatives', {}),
                    optimization_objective=metadata.get('objective', 'makespan')
                )
            else:
                self._execute_graph_deployment(graph, mapped_sqs, metadata)
            
            # Replay any input tokens that arrived before deployment completed
            if graph_name in self.pending_inputs:
                queued_graph, queued_inputs = self.pending_inputs.pop(graph_name)
                self.logger.info(
                    f"Replaying queued input tokens for '{graph_name}'")
                replay_msg = Message(
                    RuntimeMsg.ExecuteGraphOnInputs,
                    (queued_graph, queued_inputs),
                    Recipient.ProcessRuntimeManager)
                self.handle_message(replay_msg)

    def _execute_graph_deployment(self, graph, mapped_sqs, metadata):
        """
        Execute graph deployment after all devices are ready.
        
        This is the continuation of InstantiateAndMapGraph after devices have joined.
        """
        mapped_ports = Mapper.generate_mapping(graph, mapped_sqs)
        self.logger.debug(f'Mapped Ports: {mapped_ports}')

        # Single-app contention detection (same as InstantiateAndMapGraph path)
        app_id = graph.graph_name or 'default'
        try:
            from . import SmartMapper as SM
            contention_mapper = SM.SmartMapper(None)
            contention_scenarios = contention_mapper.detect_single_app_contention(
                graph, mapped_sqs,
                device_profile_manager=self.device_profile_manager
            )
            if contention_scenarios:
                self.multitenancy_scenarios = contention_scenarios
                single_app_mappings = {app_id: mapped_sqs}
                self._upgrade_all_devices_to_concurrent_if_possible(single_app_mappings)
                self.logger.info(f"Pending graph contention analysis: "
                               f"{len(contention_scenarios)} devices with contention")
        except Exception as e:
            self.logger.warning(f"Contention detection failed for pending graph: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())

        # Distribute clocks to each ensemble
        for ens_name in list(self.connected_ensembles.keys()):
            clock_list = graph.clock_dictionary
            if 0 == len(clock_list):
                self.logger.warning('Clock Dictionary is empty. Execute at your own risk.')
                from . import Clock
                clock_list = {'root_clock': Clock.TTClock.root()}

            from .IPC import Message, SyncMsg, ExecuteMsg, NetMsg, Recipient
            
            msg_clocks_sync = Message(
                SyncMsg.AddClocks, list(clock_list.values()),
                Recipient.ProcessInputTokens)
            msg_clocks_execute = Message(
                ExecuteMsg.AddClocks,
                list(clock_list.values()),
                Recipient.ProcessExecute)
            
            network_payload = (ens_name, [msg_clocks_sync, msg_clocks_execute])
            network_msg = Message(NetMsg.ForwardNetworkMessage,
                                network_payload,
                                Recipient.ProcessNetwork)
            self.logger.info('Send clocks to Ensemble(%s)', ens_name)
            self.input_network_func(network_msg)
        
        self.logger.debug('Done sending clocks')

        # Instantiate SQs on ensembles
        from .SQ import TTSQ
        from .SQExecute import TTSQExecute
        
        for this_sq in graph.sqs:
            assert isinstance(this_sq, TTSQ), 'graph.sq_list should only contain TTSQ\'s'
            
            ensemble_name = mapped_sqs[this_sq.sq_name]
            self.logger.debug(f'sending SQ {this_sq} to {ensemble_name}')

            # Get allocation metadata (None if SQ has no contention)
            allocation_metadata = self._get_allocation_metadata(
                app_id, this_sq.sq_name, ensemble_name)

            msg_instatiate_sync = Message(
                SyncMsg.InstantiateSQ, this_sq.generate_runtime_sqsync(),
                Recipient.ProcessInputTokens)
            msg_instantiate_execute = Message(ExecuteMsg.InstantiateSQ,
                                            (TTSQExecute(this_sq), allocation_metadata),
                                            Recipient.ProcessExecute)
            msg_instantiate_forwarding = Message(
                NetMsg.InstantiateSQ,
                (this_sq.sq_name, mapped_ports[this_sq], allocation_metadata),
                Recipient.ProcessNetwork)

            network_payload = (ensemble_name, [
                msg_instatiate_sync, msg_instantiate_execute,
                msg_instantiate_forwarding
            ])
            network_msg = Message(NetMsg.ForwardNetworkMessage,
                                network_payload,
                                Recipient.ProcessNetwork)
            self.input_network_func(network_msg)

        # Extract strategy and metadata
        requested_strategy = metadata.get('strategy', 'static')
        placement_alternatives = metadata.get('placement_alternatives', {})
        optimization_objective = metadata.get('objective', 'makespan')

        # Store deployment artifacts
        self.instantiated_graphs[graph.graph_name] = (graph, mapped_sqs)
        self.deployed_port_wiring[graph.graph_name] = {
            sq.sq_name: mapped_ports[sq] for sq in graph.sqs
        }
        
        # Store deployment metadata
        self.deployment_metadata[graph.graph_name] = {
            'strategy': requested_strategy,
            'placement_alternatives': placement_alternatives,
            'objective': optimization_objective
        }
        
        # Configure RuntimeAdapter
        self._configure_runtime_adapter(
            graph=graph,
            mapping=mapped_sqs,
            strategy=requested_strategy,
            placement_alternatives=placement_alternatives,
            optimization_objective=optimization_objective
        )
        
        self.logger.info(f"Graph {graph.graph_name} successfully deployed with {requested_strategy} strategy")

    def _send_device_welcome(self, ensemble_info):
        """
        Send DeviceWelcome message with synchronized timing configuration.
        
        This ensures the joining device uses the same heartbeat interval
        that RuntimeManager expects, based on application timing characteristics.
        """
        device_name = ensemble_info.name
        
        # Build timing configuration payload
        timing_config = {
            'device_id': device_name,
            'heartbeat_interval': self.heartbeat_timeout,
            'grace_period': self.grace_period_base,
            'stability_window': self.stability_window,
            'timing_profile': self._get_current_timing_profile()
        }
        
        # Create DeviceWelcome message
        welcome_msg = Message(
            RuntimeMsg.DeviceWelcome,
            timing_config,
            Recipient.ProcessRuntimeManager  # Will be intercepted by Ensemble
        )
        
        # Send via network to the joining device
        network_payload = (device_name, welcome_msg)
        network_msg = Message(
            NetMsg.ForwardNetworkMessage,
            network_payload,
            Recipient.ProcessNetwork
        )
        
        if self.input_network_func:
            self.input_network_func(network_msg)
            self.logger.info(f"DeviceWelcome sent to {device_name} with heartbeat_interval={self.heartbeat_timeout}s")
        else:
            self.logger.warning(f"Cannot send DeviceWelcome to {device_name} - no network function")

    def _get_current_timing_profile(self):
        """Get human-readable timing profile name for logging."""
        if self.heartbeat_timeout <= 2.0:
            return 'real-time'
        elif self.heartbeat_timeout <= 5.0:
            return 'fast-periodic'
        else:
            return 'batch-processing'

    def _validate_device_capabilities(self, ensemble_info):
        """Validate device capabilities against universal catalog."""
        device_type = ensemble_info.components.get('type', 'unknown')
        
        # Check if device type exists in loaded catalog
        if device_type in self.device_profile_manager.device_types:
            self.logger.info(f"Device {ensemble_info.name} type '{device_type}' validated")
            return True
        else:
            self.logger.warning(f"Device type '{device_type}' not recognized in catalog")
            return False

    def _assess_new_device_optimization_benefit(self, new_device):
        """Assess optimization benefit using device performance comparison."""
        if not self.runtime_adapter or not self.runtime_adapter.current_mapping:
            return 0.0
        
        try:
            # Get new device specs from catalog
            device_type = new_device.components.get('type', 'unknown')
            if device_type not in self.device_profile_manager.device_types:
                return 0.0
                
            new_device_specs = self.device_profile_manager.device_types[device_type]
            new_cpu_speed = new_device_specs.get('cpu_speed', 1.0)
            
            # Simple performance estimation: compare CPU speeds
            total_improvement = 0.0
            beneficial_tasks = 0
            
            for sq_name, current_device in self.runtime_adapter.current_mapping.items():
                # Get current device specs
                if current_device in self.connected_ensembles:
                    current_ensemble = self.connected_ensembles[current_device]
                    current_type = current_ensemble.components.get('type', 'raspberry_pi_4')
                    
                    if current_type in self.device_profile_manager.device_types:
                        current_specs = self.device_profile_manager.device_types[current_type]
                        current_cpu_speed = current_specs.get('cpu_speed', 1.0)
                        
                        # Calculate speedup potential
                        speedup = new_cpu_speed / current_cpu_speed
                        if speedup > 1.1:  # 10% improvement threshold
                            improvement = (speedup - 1.0) * 100
                            total_improvement += improvement
                            beneficial_tasks += 1
            
            avg_improvement = total_improvement / len(self.runtime_adapter.current_mapping) if self.runtime_adapter.current_mapping else 0.0
            
            self.logger.info(f"New device optimization assessment: {beneficial_tasks} beneficial tasks, avg benefit: {avg_improvement:.1f}%")
            return avg_improvement
            
        except Exception as e:
            self.logger.error(f"Optimization assessment failed: {e}")
            return 0.0


    def _calculate_task_migration_benefit(self, sq_name, current_device, target_device_type):
        """Calculate benefit of migrating task to new device type."""
        if not self.runtime_adapter:
            return 0.0
        
        # Get task characteristics
        task_char = self.runtime_adapter.characterization_data.get(sq_name, {})
        exec_estimates = task_char.get('execution_time_estimates', {})
    
        # Get execution times
        current_exec_time = exec_estimates.get('default', 0.01)  # Fallback
        target_exec_time = exec_estimates.get(target_device_type, exec_estimates.get('default', 0.01))
    
        if current_exec_time <= 0:
            return 0.0
        
        # Calculate improvement percentage
        improvement = ((current_exec_time - target_exec_time) / current_exec_time) * 100
        return max(improvement, 0.0)

    def _should_integrate_device(self, device_name, optimization_benefit):
        """Conservative decision on whether to immediately integrate device."""
        # Check system load
        current_load = self._calculate_system_load()
    
        # Conservative integration criteria
        if optimization_benefit >= 15.0:  # High benefit threshold
            if current_load < 0.7:  # System not heavily loaded
                self.logger.info(f"High benefit ({optimization_benefit:.1f}%) - immediate integration approved")
                return True
    
        self.logger.info(f"Conservative integration: benefit {optimization_benefit:.1f}%, "
                        f"load {current_load:.1f} - adding to available pool")
        return False

    def _integrate_new_device(self, ensemble_info, optimization_benefit):
        """Integrate new device with immediate task assignment opportunities."""
        device_name = ensemble_info.name
    
        # Add to connected ensembles
        self.connected_ensembles[device_name] = ensemble_info
    
        # Initialize resource ledger entry with device capacity
        self._initialize_resource_ledger_entry(device_name, ensemble_info)
    
        # Initialize device state as AVAILABLE
        self._update_device_state(device_name, 'AVAILABLE')
    
        # Add to routing table (existing functionality)
        self._add_to_routing_table(ensemble_info)
    
        # Assess immediate migration opportunities
        if self.runtime_adapter:
            migration_candidates = self.runtime_adapter.evaluate_new_device_migrations(
                device_name, self.connected_ensembles
            )
        
            if migration_candidates:
                self.logger.info(f"Found {len(migration_candidates)} immediate migration opportunities")
                # Execute conservative migration (1-2 tasks initially)
                self._execute_conservative_migration(device_name, migration_candidates[:2])
    
        self.logger.info(f"Device {device_name} integrated with immediate optimization")

        # Extract concurrent capacity from device's reported components
        device_components = ensemble_info.components or {}
        concurrent_capacity = device_components.get('concurrent_capacity', 
                             device_components.get('cpu_cores', 1))
        supports_timeslicing = device_components.get('supports_timeslicing', True)
        
        # Initialize device runtime state for multitenancy
        self.device_runtime_state[device_name] = {
            'concurrent_capacity': concurrent_capacity,
            'supports_timeslicing': supports_timeslicing,
            'allocated_slots': {},  # {slot_id: 'app_id_sq_name'}
            'available_slots': concurrent_capacity
        }
        
        self.logger.info(f"Device {device_name} multitenancy: {concurrent_capacity} concurrent slots, "
                        f"timeslicing={'enabled' if supports_timeslicing else 'disabled'}")

    def _add_to_available_devices(self, ensemble_info):
        """Add device to available pool without immediate task assignment."""
        device_name = ensemble_info.name
    
        # Add to connected ensembles
        self.connected_ensembles[device_name] = ensemble_info
    
        # Initialize resource ledger entry with device capacity
        self._initialize_resource_ledger_entry(device_name, ensemble_info)
    
        # Initialize device state as AVAILABLE
        self._update_device_state(device_name, 'AVAILABLE')
    
        # Add to routing table
        self._add_to_routing_table(ensemble_info)
    
        self.logger.info(f"Device {device_name} added to available pool - "
                        f"will be considered for future opportunities")
        
        # Extract concurrent capacity from device's reported components
        device_components = ensemble_info.components or {}
        concurrent_capacity = device_components.get('concurrent_capacity', 
                             device_components.get('cpu_cores', 1))
        supports_timeslicing = device_components.get('supports_timeslicing', True)
        
        # Initialize device runtime state for multitenancy
        self.device_runtime_state[device_name] = {
            'concurrent_capacity': concurrent_capacity,
            'supports_timeslicing': supports_timeslicing,
            'allocated_slots': {},  # {slot_id: 'app_id_sq_name'}
            'available_slots': concurrent_capacity
        }
        
        self.logger.info(f"Device {device_name} multitenancy: {concurrent_capacity} concurrent slots, "
                        f"timeslicing={'enabled' if supports_timeslicing else 'disabled'}")

    def _add_to_routing_table(self, ensemble_info):
        """Add device to routing table (existing functionality)."""
        # Original routing table logic
        add_to_routing_table_message = Message(
            NetMsg.AddRoutingTableEntry,
            (ensemble_info.name, ensemble_info.address),
            Recipient.ProcessNetwork)
        self.input_network_func(add_to_routing_table_message)

        # Propagate routing table
        propagate_routing_table_message = Message(
            NetMsg.PropagateRoutingTable,
            ensemble_info.name,
            Recipient.ProcessNetwork)
        self.logger.info(f'Sending PropagateRoutingTable message to device {ensemble_info.name}')
        self.input_network_func(propagate_routing_table_message)

    def _initialize_resource_ledger_entry(self, device_name, ensemble_info):
        """
        Initialize resource ledger entry for a device.
        
        Extracts hardware capacity from ensemble_info.components and creates
        a ledger entry tracking capacity and current usage.
        
        :param device_name: Device identifier
        :param ensemble_info: TTEnsembleInfo with components dict containing hardware_capacity
        """
        components = ensemble_info.components
        
        # Extract hardware capacity from components dict
        # These values come from Ensemble._discover_hardware_capacity()
        if isinstance(components, dict):
            cpu_cores = components.get('cpu_cores', 1)
            compute_slots = components.get('compute_slots', max(1, cpu_cores - 1))
            memory_mb = components.get('memory_mb', 1024)
        else:
            # Fallback for legacy components format (list)
            cpu_cores = 1
            compute_slots = 1
            memory_mb = 1024
            self.logger.warning(f"Device {device_name} has legacy components format, using defaults")
        
        # Initialize ledger entry
        self.resource_ledger[device_name] = {
            'capacity': {
                'cpu_cores': cpu_cores,
                'compute_slots': compute_slots,
                'memory_mb': memory_mb,
            },
            'used': {
                'compute_slots': 0,
                'memory_mb': 0,
            },
            'allocations': [],  # List of {'app_id': str, 'sq_name': str, 'slots': int, 'memory': int}
            'available_slots': compute_slots,
            'available_memory_mb': memory_mb,
        }
        
        self.logger.info(f"Resource ledger initialized for {device_name}: "
                        f"{compute_slots} slots, {memory_mb}MB memory")

    def _update_resource_ledger_allocation(self, device_name, app_id, sq_name, slots_used, memory_used):
        """
        Update resource ledger when an SQ is placed on a device.
        
        :param device_name: Target device
        :param app_id: Application identifier
        :param sq_name: SQ name
        :param slots_used: Compute slots consumed by this SQ
        :param memory_used: Memory consumed by this SQ (MB)
        """
        if device_name not in self.resource_ledger:
            self.logger.warning(f"Device {device_name} not in resource ledger")
            return False
        
        ledger = self.resource_ledger[device_name]
        
        # Check capacity
        if ledger['available_slots'] < slots_used:
            self.logger.warning(f"Insufficient slots on {device_name}: "
                              f"need {slots_used}, have {ledger['available_slots']}")
            return False
        
        if ledger['available_memory_mb'] < memory_used:
            self.logger.warning(f"Insufficient memory on {device_name}: "
                              f"need {memory_used}MB, have {ledger['available_memory_mb']}MB")
            return False
        
        # Update usage
        ledger['used']['compute_slots'] += slots_used
        ledger['used']['memory_mb'] += memory_used
        ledger['available_slots'] -= slots_used
        ledger['available_memory_mb'] -= memory_used
        
        # Record allocation
        ledger['allocations'].append({
            'app_id': app_id,
            'sq_name': sq_name,
            'slots': slots_used,
            'memory': memory_used,
        })
        
        self.logger.debug(f"Allocated on {device_name}: {app_id}/{sq_name} "
                         f"({slots_used} slots, {memory_used}MB)")
        return True

    def _release_resource_ledger_allocation(self, device_name, app_id, sq_name=None):
        """
        Release resources when an SQ or app is terminated.
        
        :param device_name: Device to release from
        :param app_id: Application identifier
        :param sq_name: Specific SQ to release, or None to release all for app
        """
        if device_name not in self.resource_ledger:
            return
        
        ledger = self.resource_ledger[device_name]
        
        # Find allocations to release
        to_release = []
        for alloc in ledger['allocations']:
            if alloc['app_id'] == app_id:
                if sq_name is None or alloc['sq_name'] == sq_name:
                    to_release.append(alloc)
        
        # Release each allocation
        for alloc in to_release:
            ledger['used']['compute_slots'] -= alloc['slots']
            ledger['used']['memory_mb'] -= alloc['memory']
            ledger['available_slots'] += alloc['slots']
            ledger['available_memory_mb'] += alloc['memory']
            ledger['allocations'].remove(alloc)
            
            self.logger.debug(f"Released on {device_name}: {alloc['app_id']}/{alloc['sq_name']}")

    def _get_device_available_capacity(self, device_name):
        """
        Get available capacity for a device.
        
        :param device_name: Device identifier
        :return: Dict with available_slots and available_memory_mb, or None if not found
        """
        if device_name not in self.resource_ledger:
            return None
        
        ledger = self.resource_ledger[device_name]
        return {
            'available_slots': ledger['available_slots'],
            'available_memory_mb': ledger['available_memory_mb'],
            'total_slots': ledger['capacity']['compute_slots'],
            'total_memory_mb': ledger['capacity']['memory_mb'],
        }

    def _execute_conservative_migration(self, target_device, migration_candidates):
        """Execute conservative migration to new device."""
        for candidate in migration_candidates:
            sq_name = candidate['sq_name']
            old_device = candidate['current_device']
        
            # Update mapping
            if (self.runtime_adapter and 
                hasattr(self.runtime_adapter, 'current_mapping') and
                self.runtime_adapter.current_mapping is not None):
                self.runtime_adapter.current_mapping[sq_name] = target_device
            
            self.logger.info(f"Conservative migration: {sq_name} {old_device} → {target_device} "
                            f"(benefit: {candidate['benefit']:.1f}%)")
        
            # Allow system to stabilize
            time.sleep(0.5)
    
    def _handle_device_failure_confirmed(self, device_id):
        """Handle confirmed device failure."""
        #if device_id in self.connected_ensembles:
        #    del self.connected_ensembles[device_id]
        
        # Clean up resource ledger to avoid historical ghost allocations
        if device_id in self.resource_ledger:
            del self.resource_ledger[device_id]
        
        # Trigger existing adaptation logic
        if self.runtime_adapter:
            self._handle_multiple_device_failures([device_id])

    def _update_device_state(self, device_id, new_state):
        """Update device state and notify TTEnsembleInfo."""
        old_state = self.device_states.get(device_id, 'ACTIVE')
        self.device_states[device_id] = new_state
    
        # Update TTEnsembleInfo if available
        if device_id in self.connected_ensembles:
            ensemble_info = self.connected_ensembles[device_id]
            if hasattr(ensemble_info, 'update_device_state'):
                ensemble_info.update_device_state(new_state)
    
        self.logger.debug(f"Device {device_id}: {old_state} → {new_state}")

    def _increment_failure_count(self, device_id):
        """Increment consecutive failure count for exponential backoff."""
        self.device_reliability[device_id] = self.device_reliability.get(device_id, 0) + 1

    def _handle_multiple_device_failures(self, failed_devices):
        """Handle multiple device failures using RuntimeAdapter."""
        if not self.runtime_adapter:
            self.logger.warning("RuntimeAdapter not available - cannot perform adaptive remapping")
            return
        
        for device_id in failed_devices:
            # Remove from connected ensembles
            if device_id in self.connected_ensembles:
                del self.connected_ensembles[device_id]
    
            # Trigger adaptation for each failed device
            for graph_name, (graph, mapping) in self.instantiated_graphs.items():
                # Filter synthetic SQs — no runtime representation, cannot migrate
                synthetic_sqs = {'SUPER_TRIGGER', 'BARRIER_JOIN'}
                adaptable_mapping = {sq: dev for sq, dev in mapping.items()
                                    if sq not in synthetic_sqs}
                
                new_mapping = self.runtime_adapter.on_device_failure(
                    device_id, adaptable_mapping, self.connected_ensembles, self.device_states
                )
                if new_mapping:
                    # Merge synthetic SQs back for structural metadata
                    full_mapping = dict(new_mapping)
                    for sq, dev in mapping.items():
                        if sq in synthetic_sqs:
                            full_mapping[sq] = dev
                    self.instantiated_graphs[graph_name] = (graph, full_mapping)
                    self.logger.info(f"Adapted graph {graph_name} after {device_id} failure")

    def _try_place_app(self, app_id, app_config):
        """
        Attempt to place an app on available resources.
        
        :return: True if placed successfully, False if queued
        """
        graph = app_config.get('graph')
        if graph is None:
            self.logger.error(f"No graph for app {app_id}")
            return False
        
        # Filter available ensembles
        available = list(self.connected_ensembles.values())
        
        if not available:
            return False
        
        # Try QPF first, fallback to static
        try:
            from . import SmartMapper as SM
            from .UnifiedGraph import UnifiedPlacementGraph
            
            unified_apps = {app_id: {'graph': graph}}
            ensemble_dict = {ens.name: ens for ens in available}
            unified_graph = UnifiedPlacementGraph(unified_apps, ensemble_dict)
            
            mapper = SM.SmartMapper(unified_graph)
            mappings = mapper.coordinated_qpf_mapping(
                [graph], available, {app_id: app_config}, self.device_allocations, device_profile_manager=self.device_profile_manager
            )
            
            if app_id in mappings:
                app_mapping = mappings[app_id]
            else:
                app_mapping = None
        except Exception as e:
            self.logger.warning(f"QPF failed for {app_id}: {e}")
            app_mapping = None
        
        if not app_mapping:
            app_mapping = Mapper.static_mapping(graph, available)
        
        if not app_mapping:
            return False
        
        # Deploy the app
        # Determine strategy and get placement alternatives
        if 'mappings' in dir() and app_id in mappings:
            # QPF was used
            deployment_strategy = 'qpf'
            placement_alternatives = mapper.get_placement_alternatives() if hasattr(mapper, 'get_placement_alternatives') else {}
            optimization_objective = mapper.get_optimization_objective() if hasattr(mapper, 'get_optimization_objective') else 'makespan'
        else:
            # Static fallback was used
            deployment_strategy = 'static'
            placement_alternatives = {}
            optimization_objective = 'makespan'
        
        # Deploy the app
        self._deploy_single_app(app_id, graph, app_mapping, app_config,
                                deployment_strategy=deployment_strategy,
                                placement_alternatives=placement_alternatives,
                                optimization_objective=optimization_objective or 'makespan')
        return True
    
    def _deploy_single_app(self, app_id, graph, mapping, app_config,
                           deployment_strategy: str = 'static',
                           placement_alternatives: Optional[Dict] = None,
                           optimization_objective: str = 'makespan'):
        """
        Deploy a single app with the given mapping.
        
        CRITICAL DESIGN NOTES:
        - This method assumes device_allocations has ALREADY been updated during the mapping phase
        - It does NOT update device_allocations (to avoid double allocation bug)
        - It only sends tasks to devices based on the provided mapping
        - Clocks are distributed only to devices used by this app (not all connected devices)
        
        :param app_id: Application identifier
        :param graph: TTGraph for this application
        :param mapping: SQ-to-device mapping {sq_name: device_name}
        :param app_config: App configuration dict
        :param deployment_strategy: 'qpf', 'static', 'random', or 'trivial'
        :param placement_alternatives: QPF placement alternatives (if QPF was used)
        :param optimization_objective: 'makespan' or 'energy'
        """
        self.logger.info(f'Deploying app {app_id} with {len(mapping)} SQs using {deployment_strategy} strategy')
        
        # Validate that all devices in mapping are actually connected
        required_devices = set(mapping.values())
        connected_devices = set(self.connected_ensembles.keys())
        missing_devices = required_devices - connected_devices
        
        if missing_devices:
            self.logger.error(f"Cannot deploy {app_id}: devices not connected: {missing_devices}")
            self.logger.error(f"Required: {required_devices}, Connected: {connected_devices}")
            raise RuntimeError(f"Deployment failed for {app_id}: devices {missing_devices} not ready")
        
        # Store in applications registry
        self.applications[app_id] = {
            'graph': graph,
            'mapped_sqs': mapping
        }
        
        # NOTE: We do NOT update device_allocations here
        # It has already been updated during the mapping phase (coordinated_qpf_mapping or coordinated_static_mapping)
        # This prevents the double allocation bug
        
        # Distribute clocks ONLY to devices used by this app (not all devices)
        devices_used = set(mapping.values())
        clock_list = graph.clock_dictionary
        if len(clock_list) == 0:
            from . import Clock
            clock_list = {'root_clock': Clock.TTClock.root()}
        
        self.logger.debug(f'Distributing clocks for {app_id} to devices: {devices_used}')
        for device_name in devices_used:
            msg_clocks_sync = Message(
                SyncMsg.AddClocks, list(clock_list.values()),
                Recipient.ProcessInputTokens)
            msg_clocks_execute = Message(
                ExecuteMsg.AddClocks, list(clock_list.values()),
                Recipient.ProcessExecute)
            
            network_payload = (device_name, [msg_clocks_sync, msg_clocks_execute])
            network_msg = Message(NetMsg.ForwardNetworkMessage,
                                network_payload, Recipient.ProcessNetwork)
            self.logger.debug(f'Sent clocks for {app_id} to device {device_name}')
            self.input_network_func(network_msg)
        
        # Distribute SQs to devices
        mapped_ports = Mapper.generate_mapping(graph, mapping)
        
        self.logger.debug(f'Distributing {len(graph.sqs)} SQs for {app_id}')
        for this_sq in graph.sqs:
            sq_name = this_sq.sq_name
            deployment_name = f"{app_id}_{sq_name}"  # Prefixed name for routing
            device_name = mapping[sq_name]
            
            # Generate runtime representations using original SQ object
            sq_sync = this_sq.generate_runtime_sqsync()
            sq_execute = TTSQExecute(this_sq)
            
            # Get allocation metadata for this SQ (if on shared device)
            allocation_metadata = self._get_allocation_metadata(app_id, this_sq.sq_name, device_name)
            
            msg_instantiate_sync = Message(
                SyncMsg.InstantiateSQ,
                sq_sync,
                Recipient.ProcessInputTokens)
            msg_instantiate_execute = Message(
                ExecuteMsg.InstantiateSQ,
                (sq_execute, allocation_metadata),
                Recipient.ProcessExecute)
            msg_instantiate_forwarding = Message(
                NetMsg.InstantiateSQ,
                (deployment_name, mapped_ports[this_sq], allocation_metadata),
                Recipient.ProcessNetwork)
            
            network_payload = (device_name, [
                msg_instantiate_sync, msg_instantiate_execute,
                msg_instantiate_forwarding
            ])
            network_msg = Message(NetMsg.ForwardNetworkMessage,
                                network_payload, Recipient.ProcessNetwork)
            self.logger.debug(f'Deployed SQ {deployment_name} to device {device_name}')
            self.input_network_func(network_msg)
       
        # Store deployment artifacts
        graph_key = f"{app_id}_{graph.graph_name}"
        self.instantiated_graphs[graph_key] = (graph, mapping)
        self.deployed_port_wiring[graph_key] = {
            sq.sq_name: mapped_ports[sq] for sq in graph.sqs
        }
        
        # Store deployment metadata for runtime adaptation
        self.deployment_metadata[graph_key] = {
            'strategy': deployment_strategy,
            'placement_alternatives': placement_alternatives or {},
            'objective': optimization_objective
        }
        
        # Configure RuntimeAdapter with deployment-specific parameters
        self._configure_runtime_adapter(
            graph=graph,
            mapping=mapping,
            strategy=deployment_strategy,
            placement_alternatives=placement_alternatives,
            optimization_objective=optimization_objective
        )
        
        self.logger.info(f'App {app_id} deployed successfully with {deployment_strategy} strategy')
        
        # Mark app as ACTIVE (all apps start executing immediately with pre-allocated resources)
        self.app_states[app_id] = 'ACTIVE'
        
        self.logger.info(f"App '{app_id}' deployed and ACTIVE")

    def _deploy_combined_graph(self, combined_graph, flat_mapping, app_mappings,
                               deployment_strategy: str = 'qpf',
                               placement_alternatives: Optional[Dict] = None,
                               optimization_objective: str = 'makespan'):
        """
        Deploy a CombinedGraph with multitenancy handling.
        
        :param combined_graph: CombinedGraph object
        :param flat_mapping: {prefixed_sq_name: device} mapping
        :param app_mappings: {app_id: {original_sq_name: device}} for multitenancy
        :param deployment_strategy: Strategy used for mapping
        :param placement_alternatives: QPF-computed ranked alternatives per task
        :param optimization_objective: 'makespan' or 'energy'
        """
        self.logger.info(f'Deploying CombinedGraph {combined_graph.graph_name} '
                        f'with {len(flat_mapping)} SQs using {deployment_strategy}')
        
        # Validate devices
        required_devices = set(flat_mapping.values())
        connected_devices = set(self.connected_ensembles.keys())
        missing = required_devices - connected_devices
        
        if missing:
            raise RuntimeError(f"Deployment failed: devices {missing} not connected")
        
        # Store in applications registry (one entry per sub-app)
        for app_id in combined_graph.app_ids:
            app_mapping = app_mappings.get(app_id, {})
            self.applications[app_id] = {
                'graph': combined_graph,
                'mapped_sqs': app_mapping,
                'is_combined': True,
                'combined_graph_name': combined_graph.graph_name
            }
        
        # Distribute clocks to all devices used
        devices_used = set(flat_mapping.values())
        clock_list = combined_graph.clock_dictionary
        if not clock_list:
            from . import Clock
            clock_list = {'root_clock': Clock.TTClock.root()}
        
        for device_name in devices_used:
            msg_clocks_sync = Message(
                SyncMsg.AddClocks, list(clock_list.values()),
                Recipient.ProcessInputTokens)
            msg_clocks_execute = Message(
                ExecuteMsg.AddClocks, list(clock_list.values()),
                Recipient.ProcessExecute)
            
            network_payload = (device_name, [msg_clocks_sync, msg_clocks_execute])
            network_msg = Message(NetMsg.ForwardNetworkMessage,
                                network_payload, Recipient.ProcessNetwork)
            self.input_network_func(network_msg)
        
        # Distribute SQs
        from . import Mapper
        mapped_ports = Mapper.generate_mapping(combined_graph, flat_mapping)
        
        for sq in combined_graph.sqs:
            sq_name = sq.sq_name
            
            # Skip synthetic SQs — no sq_sync/sq_execute representation
            if sq_name in ('SUPER_TRIGGER', 'BARRIER_JOIN'):
                self.logger.debug(f"Skipping synthetic SQ: {sq_name}")
                continue
            
            device_name = flat_mapping[sq_name]
            
            # Determine app_id for this SQ (for allocation metadata)
            app_id = combined_graph.get_app_for_sq(sq_name)
            original_name = combined_graph.get_original_sq_name(sq_name) if app_id else sq_name
            
            # Generate runtime representations
            sq_sync = sq.generate_runtime_sqsync() if hasattr(sq, 'generate_runtime_sqsync') else None
            sq_execute = TTSQExecute(sq) if not isinstance(sq, type(combined_graph.super_trigger)) else None
            
            # Get allocation metadata for multitenancy
            allocation_metadata = self._get_allocation_metadata(
                app_id or 'combined', original_name, device_name
            )
            
            msg_instantiate_sync = Message(
                SyncMsg.InstantiateSQ, sq_sync,
                Recipient.ProcessInputTokens)
            msg_instantiate_execute = Message(
                ExecuteMsg.InstantiateSQ, 
                (sq_execute, allocation_metadata),
                Recipient.ProcessExecute)
            msg_instantiate_forwarding = Message(
                NetMsg.InstantiateSQ,
                (sq_name, mapped_ports[sq], allocation_metadata),
                Recipient.ProcessNetwork)
            
            network_payload = (device_name, [
                msg_instantiate_sync, msg_instantiate_execute,
                msg_instantiate_forwarding
            ])
            network_msg = Message(NetMsg.ForwardNetworkMessage,
                                network_payload, Recipient.ProcessNetwork)
            self.logger.debug(f'Deployed SQ {sq_name} to {device_name}')
            self.input_network_func(network_msg)
        
        # Store deployment artifacts (full mapping for token routing)
        self.instantiated_graphs[combined_graph.graph_name] = (combined_graph, flat_mapping)
        self.deployed_port_wiring[combined_graph.graph_name] = {
            sq.sq_name: mapped_ports[sq] for sq in combined_graph.sqs
        }
        
        self.deployment_metadata[combined_graph.graph_name] = {
            'strategy': deployment_strategy,
            'placement_alternatives': placement_alternatives or {},
            'objective': optimization_objective,
            'app_ids': combined_graph.app_ids,
            'app_mappings': app_mappings,
            'is_combined': True
        }
        
        # Filter synthetic SQs — RuntimeAdapter cannot migrate them
        synthetic_sqs = {'SUPER_TRIGGER', 'BARRIER_JOIN'}
        real_mapping = {sq: dev for sq, dev in flat_mapping.items()
                       if sq not in synthetic_sqs}
        
        # Configure RuntimeAdapter (real SQs only)
        self._configure_runtime_adapter(
            graph=combined_graph,
            mapping=real_mapping,
            strategy=deployment_strategy,
            placement_alternatives=placement_alternatives or {},
            optimization_objective=optimization_objective
        )
        
        # Mark all sub-apps as ACTIVE
        for app_id in combined_graph.app_ids:
            self.app_states[app_id] = 'ACTIVE'
        
        self.logger.info(f"CombinedGraph '{combined_graph.graph_name}' deployed: "
                        f"{len(combined_graph.app_ids)} apps ACTIVE")

    
    def _upgrade_all_devices_to_concurrent_if_possible(self, all_mappings):
        """
        Upgrade all shared devices to concurrent execution before deployment.
        
        This runs ONCE after mapping but BEFORE any SQ deployment messages are sent.
        Checks each shared device to see if total SQs can fit in concurrent slots.
        
        :param all_mappings: {app_id: {sq_name: device_name}} - Complete mappings for all apps
        """
        if not self.multitenancy_scenarios:
            # No shared devices, nothing to upgrade
            return
        
        self.logger.info(f"Checking {len(self.multitenancy_scenarios)} shared devices for concurrent upgrade...")
        
        for device_name, device_scenarios in list(self.multitenancy_scenarios.items()):
            # Check if device has joined and has concurrent capacity
            if device_name not in self.device_runtime_state:
                self.logger.debug(f"Device '{device_name}' not yet joined, will use time-slicing")
                # Mark all scenarios as time-sliced
                for sq_id in device_scenarios.keys():
                    device_scenarios[sq_id]['mode'] = 'timesliced'
                continue
            
            device_state = self.device_runtime_state[device_name]
            concurrent_capacity = device_state.get('concurrent_capacity', 1)
            
            # Count total SQs on this device (across all apps)
            total_sqs = len(device_scenarios)
            
            # Check if upgrade is possible
            if total_sqs <= concurrent_capacity:
                # Can upgrade
                self.logger.info(f"Upgrading device '{device_name}' to concurrent execution: "
                               f"{total_sqs} SQs ≤ {concurrent_capacity} cores")
                
                
                core_slot = 0 # tracks which core index to assign next
                upgraded_scenarios = {} # new schedule replacing time-slicing
                
                for sq_id, allocation in device_scenarios.items():
                    upgraded_scenarios[sq_id] = {
                        'mode': 'concurrent',
                        'core_slot': core_slot,
                        'total_duration': allocation.get('total_duration', 0),
                    }
                    core_slot += 1
                
                # Replace scenarios with upgraded version
                self.multitenancy_scenarios[device_name] = upgraded_scenarios
                
                # Update device state
                device_state['allocation_mode'] = 'concurrent'
                device_state['allocated_cores'] = total_sqs # cores in use
                
                self.logger.info(f"Device '{device_name}' upgraded: {total_sqs} SQs on cores 0-{total_sqs-1}")
            else:
                # Cannot upgrade, keep time-slicing
                self.logger.info(f"Device '{device_name}' using time-slicing: "
                               f"{total_sqs} SQs > {concurrent_capacity} cores")
                
                # Mark time-slicing mode in scenarios
                for sq_id in device_scenarios.keys():
                    device_scenarios[sq_id]['mode'] = 'timesliced'
                
                device_state['allocation_mode'] = 'timesliced'

    def _get_allocation_metadata(self, app_id, sq_name, device_name):
        """
        Get allocation metadata for SQ instantiation.
        
        Returns allocation info (mode, core_slot or offset/duration) if device
        has multitenancy scenarios, otherwise returns None.
        """
        if device_name not in self.multitenancy_scenarios:
            # Not a shared device, no allocation metadata needed
            return None
        
        # IMPORTANT: sq_id format must be "app_id_original_sq_name"
        # CombinedGraph uses double underscore internally ("app_id__sq_name") but
        # callers must pass the original_sq_name (extracted via get_original_sq_name),
        # NOT the prefixed CombinedGraph name.
    

        sq_id = f"{app_id}_{sq_name}"
        device_scenarios = self.multitenancy_scenarios[device_name]
        
        if sq_id not in device_scenarios:
            # SQ not contending — runs unconstrained (normal with sparse scheduling)
            self.logger.debug(f"SQ '{sq_id}' not in contention scenarios for device '{device_name}' — runs unconstrained")
            return None
        
        allocation = device_scenarios[sq_id]
        
        # Return complete allocation metadata
        return {
            'mode': allocation.get('mode', 'timesliced'),
            'core_slot': allocation.get('core_slot'),           # Present if concurrent
            'total_duration': allocation.get('total_duration'),
            'device': device_name
        }
    
    def _terminate_app(self, app_id):
        """
        Terminate an app and release its resources.
        """
        if app_id not in self.applications:
            self.logger.warning(f"App {app_id} not found for termination")
            return
        
        self.logger.info(f'Terminating app {app_id}')
        
        # Release this app's allocations from each device
        empty_devices = []
        for device_id, alloc in self.device_allocations.items():
            apps = alloc.get('apps', {})
            if app_id in apps:
                del apps[app_id]
            if not apps:
                empty_devices.append(device_id)
        
        # Remove device entries with no remaining apps
        for device_id in empty_devices:
            del self.device_allocations[device_id]
        
        # Remove from applications
        del self.applications[app_id]
        if app_id in self.app_configs:
            del self.app_configs[app_id]
        
        self.logger.info(f'App {app_id} terminated, released {len(empty_devices)} devices')
        
        # Try to place pending apps
        self._process_pending_apps()
    
    def _process_pending_apps(self):
        """
        Process pending apps queue - try to place waiting apps.
        """
        if not self.pending_apps:
            return
               
        placed = []
        for pending in self.pending_apps:
            app_id = pending['app_id']
            app_config = pending['config']
            
            if self._try_place_app(app_id, app_config):
                placed.append(pending)
                self.logger.info(f'Placed pending app {app_id}')
        
        for p in placed:
            self.pending_apps.remove(p)
    
    @staticmethod
    def generate_end_message():
        return Message(RuntimeMsg.EndExecution, None, Recipient.ProcessRuntimeManager)