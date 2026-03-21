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

"""
SmartMapper - Intelligent placement optimization with single and multi-app support.

Supports two modes:
1. Single-app mode: Optimizes placement for one application (original QPF)
2. Multi-app mode: Uses MultiAppMapper for coordinated multi-app placement

Usage:
    # Single-app
    mapper = SmartMapper(unified_graph, deployment_yaml)
    mapping = mapper.optimize(objective='makespan')
    
    # Multi-app
    mapper = SmartMapper(unified_graph, deployment_yaml, multi_app=True)
    mappings = mapper.optimize_multi_app(strategy='greedy')
"""
import yaml
import random
from collections import defaultdict
from typing import Dict, List, Set, Tuple, Optional, Union
import networkx as nx
from intervaltree import IntervalTree
from . import Query

from .UnifiedGraph import UnifiedPlacementGraph, PlacementNode
from .ObjectiveCalculator import ObjectiveCalculator
from . import DebugLogger

logger = DebugLogger.get_logger('SmartMapper')


class SmartMapper:
    """
    Intelligent placement optimizer for single and multi-application scenarios.
    
    Single-app mode: Random search optimization for one application
    Multi-app mode: Delegates to MultiAppMapper for coordination
    """
    
    def __init__(self, unified_graph: Optional[UnifiedPlacementGraph], 
                 deployment_yaml: Optional[str] = None,
                 multi_app: Optional[bool] = None):
        """
        Initialize SmartMapper.
        
        :param unified_graph: UnifiedPlacementGraph
        :param deployment_yaml: Path to deployment YAML (for single-app filtering)
        :param multi_app: Force multi-app mode (auto-detects if None)
        """
        self.unified_graph = unified_graph
        self.calc = ObjectiveCalculator()

        self.placement_alternatives = {}  # {sq_name: [{'device': str, 'score': float}, ...]}
        self.optimization_objective = None  # Track which objective was used
        
        # Auto-detect mode if not specified
        num_apps = len(unified_graph.applications) if unified_graph is not None else 1
        if multi_app is None:
            multi_app = (num_apps > 1)
        
        self.multi_app = multi_app
        
        if self.multi_app:
            logger.info(f"SmartMapper initialized in MULTI-APP mode ({num_apps} applications)")
        else:
            logger.info(f"SmartMapper initialized in SINGLE-APP mode")
            if deployment_yaml:
                self._init_single_app(deployment_yaml)
            else:
                # Use all nodes if no deployment filtering
                self.candidate_nodes = unified_graph.nodes
                self.candidate_edges = unified_graph.edges
                logger.info(f"  {len(self.candidate_nodes)} placement options (no filtering)")
    
    def _init_single_app(self, deployment_yaml: str):
        """Initialize for single-app mode with deployment filtering."""
        self.active_ensemble_names = self._load_active_ensemble_names(deployment_yaml)
        self.candidate_nodes = self._filter_active_devices()
        self.candidate_edges = self._filter_active_edges()
        logger.info(f"  {len(self.candidate_nodes)} placement options after filtering")

    
    def _load_active_ensemble_names(self, deployment_yaml: str) -> List[str]:
        """Load active device names from deployment YAML."""
        with open(deployment_yaml, 'r') as f:
            data = yaml.safe_load(f)
        names = [dev['id'] for dev in data.get('devices', [])]
        logger.debug(f"Active ensemble names: {names}")
        return names
    
    def _filter_active_devices(self) -> List[PlacementNode]:
        """Filter nodes to only active devices."""
        return [
            node for node in self.unified_graph.nodes
            if node.device in self.active_ensemble_names
        ]
    
    def _filter_active_edges(self):
        """Filter edges to only active nodes."""
        return [
            (src, dst, sym) for src, dst, sym in self.unified_graph.edges
            iif src in self.candidate_nodes and dst in self.candidate_nodes
        ]
    
    # ========== SINGLE-APP OPTIMIZATION ==========
    
    def optimize(self, objective: str = "makespan", trials: int = 1000,
                 track_convergence: bool = False,
                 deadline_constrained: bool = False) -> Dict[str, str]:
        """
        Optimize placement for single application.
        
        Uses random search to find best placement according to objective.
        
        :param objective: 'makespan' or 'energy'
        :param trials: Number of random trials
        :param track_convergence: If True, store convergence history
        :param deadline_constrained: If True, reject placements that violate deadlines
        :return: Mapping {sq_name: device_name}
        """
        #if self.multi_app:
        #    raise ValueError("Use optimize_multi_app() for multi-app mode")
        
        if not self.candidate_nodes:
            logger.error("No feasible placements — cannot optimize")
            self.convergence_history = [] if track_convergence else None
            return {}
        
        self.deadline_constrained = deadline_constrained
        self.optimization_objective = objective
        
        logger.info(f"Starting single-app optimization: {objective}, {trials} trials, "
                   f"deadline_constrained={deadline_constrained}")
        
        best_mapping = None
        best_score = float('inf') if objective != "throughput" else float('-inf')
        initial_score = None
        
        # Convergence tracking
        convergence_history: List[Dict] = []
        improvement_trials: List[int] = []
        
        # Track deadline rejections
        deadline_rejections = 0
        
        for i in range(trials):
            mapping = self._random_valid_mapping()
            if not mapping:
                continue
            
            # Check deadline feasibility first
            if deadline_constrained and not self._is_deadline_feasible(mapping):
                deadline_rejections += 1
                continue
            
            score = self._evaluate(mapping, objective)
            
            if initial_score is None:
                initial_score = score
            
            improve = (objective != "throughput" and score < best_score) or \
                    (objective == "throughput" and score > best_score)
            
            if improve:
                best_score = score
                best_mapping = mapping
                if track_convergence:
                    improvement_trials.append(i + 1)
            
            if track_convergence:
                convergence_history.append({
                    'trial': i + 1,
                    'current_score': score,
                    'best_score': best_score,
                    'improved': improve
                })
            
            if (i + 1) % 200 == 0:
                logger.debug(f"  Trial {i+1}: best {objective} = {best_score:.6f}")
        
        if deadline_constrained:
            logger.info(f"Deadline-constrained optimization: {deadline_rejections} infeasible placements rejected")
        
        logger.info(f"Optimization complete: best {objective} = {best_score:.6f}")
        
        # Store convergence history
        if track_convergence:
            self.convergence_history = convergence_history
            self.convergence_summary = {
                'initial_score': initial_score,
                'final_score': best_score,
                'total_improvements': len(improvement_trials),
                'improvement_trials': improvement_trials,
                'first_improvement': improvement_trials[0] if improvement_trials else None,
                'last_improvement': improvement_trials[-1] if improvement_trials else None,
                'deadline_rejections': deadline_rejections if deadline_constrained else None
            }
        else:
            self.convergence_history = None
            self.convergence_summary = None
        
        # Compute placement alternatives for runtime adaptation
        if best_mapping:
            self.placement_alternatives = self._compute_placement_alternatives(objective)
        
        return best_mapping or {}
    
    def get_placement_alternatives(self) -> Dict[str, List[Dict]]:
        """
        Get the computed placement alternatives for runtime adaptation.
        
        Call this after optimize() to retrieve the ranked device preferences
        for each task. This data should be stored by RuntimeManager alongside
        the deployed_port_wiring for use during adaptation.
        
        :return: {sq_name: [{'device': str, 'score': float, 'rank': int}, ...]}
        """

        return self.placement_alternatives.copy()
    
    def get_optimization_objective(self) -> Optional[str]:
        """Get the objective used in the last optimization."""
        return self.optimization_objective
    
    def _random_valid_mapping(self) -> Dict[str, str]:
        """Generate random valid mapping for single app."""
        if not self.candidate_nodes:
            logger.warning("No placement options — returning empty mapping")
            return {}
        
        mapping = {}
        sq_to_nodes = defaultdict(list)
        for node in self.candidate_nodes:
            sq_to_nodes[node.sq.sq_name].append(node)
        
        for sq_name, nodes in sq_to_nodes.items():
            chosen = random.choice(nodes)
            mapping[sq_name] = chosen.device
        
        return mapping
    
    def _evaluate(self, mapping: Dict[str, str], objective: str) -> float:
        """Evaluate single-app mapping."""
        if not mapping:
            return float('inf') if objective != "throughput" else float('-inf')
        
        if objective == "makespan":
            return self.calc.calculate_makespan(mapping, self.unified_graph)
        elif objective == "energy":
            return self.calc.calculate_energy(mapping, self.unified_graph)
        else:
            raise ValueError(f"Unknown objective: {objective}")
        
    def _compute_placement_alternatives(self, objective: str, sample_trials: int = 100) -> Dict[str, List[Dict]]:
        """
        Compute ranked placement alternatives for each task.
        
        For each task, evaluates placement on each compatible device and ranks
        by objective score. This data is used during runtime adaptation to make
        informed device selection when the original device fails.
        
        :param objective: 'makespan' or 'energy'
        :param sample_trials: Number of samples to estimate device scores
        :return: {sq_name: [{'device': str, 'score': float, 'rank': int}, ...]}
        """
        alternatives = {}
        
        # Group nodes by SQ
        sq_to_nodes = defaultdict(list)
        for node in self.candidate_nodes:
            sq_to_nodes[node.sq.sq_name].append(node)
        
        # For each SQ, evaluate each possible device placement
        for sq_name, nodes in sq_to_nodes.items():
            device_scores = {}
            
            for node in nodes:
                device = node.device
                
                # Sample multiple mappings with this SQ fixed to this device
                scores = []
                for _ in range(min(sample_trials, 20)):
                    # Generate random mapping but fix this SQ to current device
                    mapping = self._random_valid_mapping()
                    if not mapping:
                        continue
                    mapping[sq_name] = device
                    
                    score = self._evaluate(mapping, objective)
                    if score != float('inf'):
                        scores.append(score)
                
                if scores:
                    # Use average score as device's score for this task
                    avg_score = sum(scores) / len(scores)
                    device_scores[device] = avg_score
            
            # Rank devices by score (lower is better for makespan/energy)
            ranked_devices = sorted(device_scores.items(), key=lambda x: x[1])
            
            alternatives[sq_name] = [
                {'device': device, 'score': score, 'rank': rank + 1}
                for rank, (device, score) in enumerate(ranked_devices)
            ]
        
        logger.debug(f"Computed placement alternatives for {len(alternatives)} tasks")
        return alternatives
    
    def _is_deadline_feasible(self, mapping: Dict[str, str]) -> bool:
        """
        Check if a placement satisfies all deadline requirements.
        
        :param mapping: Proposed placement {sq_name: device_name}
        :return: True if all deadlines can be met (or cannot be verified), False if definitely infeasible
        """
        for app_id, app_data in self.unified_graph.applications.items():
            graph = app_data['graph']
            
            if not hasattr(graph, 'deadline_requirements'):
                continue
            
            deadline_reqs = graph.deadline_requirements
            if not deadline_reqs.get('deadline_sqs'):
                continue
            
            for deadline_info in deadline_reqs['deadline_sqs']:
                sq_name = deadline_info['sq_name']
                budget_us = deadline_info.get('budget_us')
                
                if budget_us is None:
                    # Can't verify without static budget - assume feasible
                    logger.debug(f"No static budget for {sq_name}, skipping feasibility check")
                    continue
                
                path = deadline_reqs['deadline_paths'].get(sq_name, [])
                if not path:
                    continue
                
                estimated_latency_us = self._estimate_path_latency(path, mapping, graph)
                
                # Only reject if we have complete data AND it exceeds budget
                if estimated_latency_us is not None and estimated_latency_us > budget_us:
                    logger.debug(f"Deadline infeasible: {sq_name} path needs {estimated_latency_us}us, "
                                f"budget is {budget_us}us")
                    return False
        
        return True
    
    def _estimate_path_latency(self, path: List[str], mapping: Dict[str, str], graph) -> Optional[float]:
        """
        Estimate end-to-end latency for a deadline path.
        
        :param path: List of SQ names in topological order
        :param mapping: Placement {sq_name: device_name}
        :param graph: TTGraph containing the SQs
        :return: Estimated latency in microseconds, or None if cannot estimate
        """
        total_latency_us = 0.0
        prev_device = None
        
        for sq_name in path:
            device = mapping.get(sq_name)
            if not device:
                continue
            
            # Find SQ object
            sq = None
            for s in graph.sqs:
                if s.sq_name == sq_name:
                    sq = s
                    break
            
            if not sq:
                continue
            
            # Add execution time
            exec_estimates = getattr(sq, 'execution_time_estimates', {})
            exec_time_s = exec_estimates.get(device, exec_estimates.get('default', None))
            
            if exec_time_s is None:
                logger.warning(f"No execution time estimate for {sq_name} on {device}")
                # Cannot complete estimate without execution time
                return None
            
            total_latency_us += exec_time_s * 1_000_000
            
            # Add communication latency if device changed
            if prev_device and prev_device != device:
                comm_latency_us = self._estimate_communication_latency(prev_device, device)
                if comm_latency_us is not None:
                    total_latency_us += comm_latency_us
                # If comm_latency is None, we continue but estimate is incomplete
            
            prev_device = device
        
        return total_latency_us
    
    def _estimate_communication_latency(self, src_device: str, dst_device: str) -> Optional[float]:
        """
        Estimate communication latency between two devices using network topology data.
        
        :param src_device: Source device name
        :param dst_device: Destination device name
        :return: Estimated latency in microseconds, or None if cannot estimate
        """
        # Same device = negligible latency
        if src_device == dst_device:
            return 0.0
        
        # Try to get from unified graph's network topology (if it exists)
        network_latencies = getattr(self.unified_graph, 'network_latencies', None)
        if network_latencies is not None:
            key = (src_device, dst_device)
            reverse_key = (dst_device, src_device)
            
            if key in network_latencies:
                return network_latencies[key] * 1_000_000
            elif reverse_key in network_latencies:
                return network_latencies[reverse_key] * 1_000_000
        
        # Try to get from ensemble info network profiles
        ensembles = getattr(self.unified_graph, 'ensembles', {})
        src_ensemble = ensembles.get(src_device)
        dst_ensemble = ensembles.get(dst_device)
        
        if src_ensemble and dst_ensemble:
            # Check if network profile exists between these ensembles
            network_profiles = getattr(src_ensemble, 'network_profiles', None)
            if network_profiles:
                profile = network_profiles.get(dst_device)
                if profile:
                    latency = getattr(profile, 'latency', None)
                    if latency is not None:
                        return latency * 1_000_000
        
        # No data available - log warning and return None to indicate unknown
        logger.warning(f"No network latency data available for {src_device} -> {dst_device}")
        return None
    
    # ========== MULTI-APP COORDINATION ==========
    def coordinated_qpf_mapping(self, graphs, ensemble_infos, app_configs, 
                                 device_allocations=None, objective='makespan', 
                                 trials=1000, device_profile_manager=None):
        """
        Coordinated QPF mapping for multiple applications.
        
        Optimizes placement for each app, computing multitenancy
        scenarios for shared devices.
              
        :param graphs: List of TTGraph objects
        :param ensemble_infos: Available ensembles
        :param app_configs: {app_id: {'graph': TTGraph, 'hyperperiod': int}}
        :param device_allocations: Current allocation state (for incremental mode)
        :param objective: Optimization objective ('makespan' or 'energy')
        :param trials: Number of optimization trials per app
        :return: {app_id: {sq_name: device_name}}
        """
        from . import Mapper
        
        if device_allocations is None:
            device_allocations = {}
        
        # Store device_profile_manager for multitenancy calculations
        self.device_profile_manager = device_profile_manager
        
        all_mappings = {}
        
        for app_id in app_configs:
            app_config = app_configs[app_id]
            graph = app_config.get('graph')
            
            if graph is None:
                logger.warning(f"No graph found for app {app_id}, skipping")
                continue
            
            # All devices available — multitenancy handles resource sharing
            available_ensembles = list(ensemble_infos)
            
            if not available_ensembles:
                logger.warning(f"No available ensembles for app {app_id}")
                continue
            
            # Build single-app unified graph for optimization
            single_app_data = {app_id: {'graph': graph}}
            ensemble_dict = {ens.name: ens for ens in available_ensembles}
            
            try:
                single_unified = UnifiedPlacementGraph(single_app_data, ensemble_dict)
                
                # Optimize this single app
                self.unified_graph = single_unified
                self.candidate_nodes = single_unified.nodes
                self.candidate_edges = single_unified.edges
                self.multi_app = False  # Temporarily disable multi-app mode
                
                app_mapping = self.optimize(objective=objective, trials=trials)
                
                if app_mapping:
                    all_mappings[app_id] = app_mapping
                    
                    # Update device allocations (multi-app per device)
                    for sq_name, device_name in app_mapping.items():
                        if device_name not in device_allocations:
                            device_allocations[device_name] = {'apps': {}}
                        apps = device_allocations[device_name]['apps']
                        if app_id not in apps:
                            apps[app_id] = []
                        apps[app_id].append(f"{app_id}_{sq_name}")
                    
                    logger.info(f"QPF optimized app {app_id}: {len(app_mapping)} SQs, "
                               f"{objective} objective")
                else:
                    logger.warning(f"QPF optimization failed for {app_id}, using static fallback")
                    fallback = Mapper.coordinated_static_mapping(
                        [graph], available_ensembles, 
                        {app_id: app_config}, device_allocations
                    )
                    if app_id in fallback:
                        all_mappings[app_id] = fallback[app_id]
                        
            except Exception as e:
                logger.error(f"QPF optimization error for {app_id}: {e}, using static fallback")
                fallback = Mapper.coordinated_static_mapping(
                    [graph], available_ensembles,
                    {app_id: app_config}, device_allocations
                )
                if app_id in fallback:
                    all_mappings[app_id] = fallback[app_id]

            # Identify shared devices and compute multitenancy scenarios (after all apps mapped)
        self.multitenancy_scenarios = {}
        shared_devices = self._identify_shared_devices(all_mappings)
        if shared_devices:
            self.multitenancy_scenarios = self._compute_multitenancy_scenarios(
                shared_devices, all_mappings, app_configs)
            logger.info(f"Computed multitenancy scenarios for {len(shared_devices)} shared devices")
        else:
            logger.info("No shared devices - no multitenancy scenarios needed")
        
        return all_mappings
    

    # ========== COMBINED GRAPH OPTIMIZATION ==========
    def optimize_combined_graph(self, combined_graph, ensemble_infos, 
                                 objective: str = 'makespan',
                                 trials: int = 1000,
                                 device_profile_manager=None) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        """
        Optimize a CombinedGraph globally, then decompose for multitenancy.
        
        This implements: "combine early, optimize globally, decompose for contention"
        
        :param combined_graph: CombinedGraph from Combiner
        :param ensemble_infos: List of available TTEnsembleInfo objects
        :param objective: Optimization objective ('makespan' or 'energy')
        :param trials: Number of QPF optimization trials
        :param device_profile_manager: Optional device profile manager
        :return: Tuple of (flat_mapping, app_mappings)
                 flat_mapping: {prefixed_sq_name: device}
                 app_mappings: {app_id: {original_sq_name: device}}
        """
        from .UnifiedGraph import UnifiedPlacementGraph
        
        self.device_profile_manager = device_profile_manager
        
        logger.info(f"Optimizing CombinedGraph with {len(combined_graph.sqs)} SQs globally")
        logger.info(f"Applications: {combined_graph.app_ids}")
        
        # Build unified placement graph treating CombinedGraph as single app
        # This allows QPF to optimize across ALL SQs simultaneously

        unified_apps = {
            'combined': {
                'graph': combined_graph,
            }
        }
        ensemble_dict = {ens.name: ens for ens in ensemble_infos}
        
        try:
            unified_graph = UnifiedPlacementGraph(unified_apps, ensemble_dict) # bipartite graph needs visualization
            
            # Treat CombinedGraph as single app
            self.unified_graph = unified_graph # stores the UnifiedPlacementGraph object as an attribute on self
            self.candidate_nodes = unified_graph.nodes # access the nodes attribute of unified graph and store it as an instance attribute
            # an node here represents all feasible (SQ, device) placement options
            self.candidate_edges = unified_graph.edges # access the edges attribute of unified_graph and store it as an instance attribute
            # dataflow dependencies expanded across placement options
            self.multi_app = False
            
            # Run QPF optimization on unified graph (placement spcae) to return flat mapping
            logger.info(f"Running QPF optimization: {objective}, {trials} trials")
            flat_mapping = self.optimize(objective=objective, trials=trials)
            
            if not flat_mapping:
                logger.warning("QPF optimization failed, using static fallback")
                flat_mapping = self._static_mapping_for_combined(combined_graph, ensemble_infos)
            
        except Exception as e:
            logger.error(f"Combined graph optimization error: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            flat_mapping = self._static_mapping_for_combined(combined_graph, ensemble_infos)
        
        logger.info(f"Global optimization complete: {len(flat_mapping)} SQ placements")
        
        # Decompose flat mapping to per-app format for multitenancy
        app_mappings = combined_graph.decompose_mapping(flat_mapping)
        
        # Build app_configs for multitenancy calculations
        app_configs = combined_graph.build_app_configs_dict()
        
        # Identify shared devices and compute multitenancy scenarios
        self.multitenancy_scenarios = {}
        shared_devices = self._identify_shared_devices(app_mappings)
        
        if shared_devices:
            logger.info(f"Shared devices detected: {list(shared_devices.keys())}")
            self.multitenancy_scenarios = self._compute_multitenancy_scenarios(
                shared_devices, app_mappings, app_configs
            )
            logger.info(f"Computed multitenancy scenarios for {len(shared_devices)} shared devices")
        else:
            logger.info("No shared devices - no multitenancy scenarios needed")
        
        return flat_mapping, app_mappings
    
    def _static_mapping_for_combined(self, combined_graph, ensemble_infos) -> Dict[str, str]:
        """Fallback static mapping for combined graph."""
        from . import Mapper
        
        mapping = {}
        for sq in combined_graph.sqs:
            constraints = getattr(sq, 'constraints', []) or []
            candidates = [
                ens.name for ens in ensemble_infos
                if not constraints or Query.TTQuery(constraints, Query.QueryOp.AND).test(ens)
            ]
            
            if candidates:
                mapping[sq.sq_name] = candidates[0]
            else:
                from .Constants import RUNTIME_MANAGER_ENSEMBLE_NAME
                mapping[sq.sq_name] = RUNTIME_MANAGER_ENSEMBLE_NAME
        
        return mapping
    
 
    def _identify_shared_devices(self, all_mappings):
        """
        Identify devices hosting SQs from multiple apps at the same time.
        
        Returns: {'device_name': ['app1', 'app2']}
        """
        device_to_apps = {}
        
        for app_id, mapping in all_mappings.items():
            for sq_name, device_name in mapping.items():
                if device_name not in device_to_apps:
                    device_to_apps[device_name] = []
                if app_id not in device_to_apps[device_name]:
                    device_to_apps[device_name].append(app_id)
        
        # Return only devices with >1 app (shared devices)
        shared = {dev: apps for dev, apps in device_to_apps.items() if len(apps) > 1}
        logger.info(f"Shared devices identified: {list(shared.keys())}")
        return shared
    
    def _compute_app_execution_demand(self, app_id, device_name, all_mappings, app_configs):
        """
        Compute total execution time demand for an app on a specific device.
        Returns time in milliseconds per hyperperiod.
        """
        graph = app_configs[app_id]['graph']
        mapping = all_mappings[app_id]
        
        total_demand = 0
        for sq in graph.sqs:
            if mapping.get(sq.sq_name) == device_name:
                # Get execution time estimate (baseline)
                exec_time = sq.execution_time_estimates.get('default', 10)  # Default 10ms
                total_demand += exec_time
        
        return total_demand
    
    def _compute_multitenancy_scenarios(self, shared_devices, all_mappings, app_configs):
        """
        Contention-aware multitenancy scheduling.
        
        Instead of allocating time slots for ALL co-located SQs, this:
        1. Computes execution windows per app via topological DAG traversal
           (same max-over-predecessors principle as ObjectiveCalculator.calculate_makespan)
        2. Detects actual temporal contention (cross-app overlaps on shared devices)
        3. Generates scheduling directives ONLY for contending SQs
        
        SQs that don't contend run unconstrained — no metadata, fire on token arrival.
        
        Falls back to exhaustive scheduling if timing analysis fails.
        
        Returns: {device_name: {sq_id: {'offset': X, 'duration': Y}}}
                 Only contains entries for SQs that actually contend.
        """
        scenarios = {}
        
        # Collect all unique app_ids across shared devices
        all_app_ids = set()
        for app_list in shared_devices.values():
            all_app_ids.update(app_list)
        
        # Step 1: Compute execution windows for each app
        all_windows = {}
        for app_id in all_app_ids:
            graph = app_configs[app_id]['graph']
            mapping = all_mappings[app_id]
            all_windows[app_id] = self._compute_execution_windows(
                graph, mapping, app_configs)
        
        # Step 2: Detect actual contention per shared device
        contention_groups = self._detect_contention_groups(all_windows, shared_devices)
        
        # Step 3: Generate scheduling directives for contending SQs only
        for device_name, conflicts in contention_groups.items():
            device_schedule = {}
            
            # Collect all SQs involved in any conflict on this device
            contending_sqs = {}  # {(app_id, sq_name): window}
            for conflict in conflicts:
                app_a, sq_name_a, window_a = conflict['sq_a']
                app_b, sq_name_b, window_b = conflict['sq_b']
                contending_sqs[(app_a, sq_name_a)] = window_a
                contending_sqs[(app_b, sq_name_b)] = window_b
            
            # Sort by original ready_time to preserve dataflow ordering
            sorted_sqs = sorted(contending_sqs.items(),
                                key=lambda x: x[1]['ready_time'])
            
            # Compute time quantum for round-robin interleaving
            # Quantum = half the shortest exec time, floored at 0.5ms
            exec_times = [w['exec_time'] for _, w in sorted_sqs]
            quantum = min(exec_times) / 2
            quantum = max(quantum, 0.5)  # Minimum 0.5ms slice
            
            # Build round-robin schedule: each SQ gets interleaved slices
            # criticality can determine which sq can start first


            remaining = {}
            sq_order = []
            for (app_id, sq_name), window in sorted_sqs:
                sq_id = f"{app_id}_{sq_name}"
                remaining[sq_id] = window['exec_time']
                sq_order.append(sq_id)
                device_schedule[sq_id] = {
                    'slices': [],
                    'total_duration': window['exec_time'],
                    'quantum': quantum,
                }
            
            current_time = 0.0
            active_sqs = list(sq_order)
            
            while active_sqs:
                for sq_id in list(active_sqs):
                    if remaining[sq_id] <= 0:
                        active_sqs.remove(sq_id)
                        continue
                    
                    slice_duration = min(quantum, remaining[sq_id])
                    device_schedule[sq_id]['slices'].append(
                        (current_time, slice_duration))
                    remaining[sq_id] -= slice_duration
                    current_time += slice_duration
                    
                    if remaining[sq_id] <= 0:
                        active_sqs.remove(sq_id)
            
            # Validate: total schedule fits in hyperperiod
            hyperperiod = max(
                app_configs[aid]['hyperperiod']
                for aid in shared_devices[device_name])
            
            if current_time > hyperperiod:
                raise ValueError(
                    f"Device '{device_name}' round-robin schedule "
                    f"({current_time:.2f}ms) exceeds hyperperiod "
                    f"({hyperperiod}ms). Contending SQs: {sq_order}")
            
            scenarios[device_name] = device_schedule
            
            # Log schedule detail
            total_on_device = sum(
                1 for app_id in shared_devices[device_name]
                for sq_name, w in all_windows.get(app_id, {}).items()
                if w['device'] == device_name)
            
            slack = hyperperiod - current_time
            for sq_id, sched in device_schedule.items():
                logger.debug(
                    f"Round-robin: {sq_id} gets {len(sched['slices'])} slices, "
                    f"total={sched['total_duration']:.2f}ms, quantum={quantum:.2f}ms")
            
            logger.info(
                f"Device '{device_name}': {len(device_schedule)} SQs interleaved "
                f"(quantum={quantum:.2f}ms) out of {total_on_device} co-located "
                f"({total_on_device - len(device_schedule)} unconstrained, "
                f"slack={slack:.2f}ms, hyperperiod={hyperperiod}ms)")
        
        # Log devices with sharing but no contention
        for device_name in shared_devices:
            if device_name not in scenarios:
                logger.info(
                    f"Device '{device_name}': shared by "
                    f"{len(shared_devices[device_name])} apps but NO temporal "
                    f"contention — all SQs run unconstrained")
        
        return scenarios
    
    def _compute_execution_windows(self, graph, mapping, app_configs):
        """
        Compute per-SQ execution windows via topological DAG traversal.
        
        Uses the same max-over-predecessors principle as
        ObjectiveCalculator.calculate_makespan, but returns the full timing
        breakdown rather than just the final makespan value.
        
        Each app starts at time 0 (its trigger). The execution window represents
        when each SQ would arrive at its mapped device and how long it needs.
        
        Parallel SQs on the same device are allowed to have overlapping windows.
        Contention scheduling (not this method) decides how to resolve overlaps.
        
        :param graph: TTGraph or VirtualGraph with .sqs attribute
        :param mapping: {sq_name: device_name} (unprefixed names)
        :param app_configs: app_configs dict (for device profile lookup)
        :return: {sq_name: {'device': str, 'ready_time': float, 'exec_time': float,
                            'start_time': float, 'finish_time': float}}
        """
        # Reuse cached DAG from graph object
        G = graph.get_dag()
        
        if not G.nodes:
            return {}
        
        sq_by_name = {name: data['sq'] for name, data in G.nodes(data=True)}
        topo_order = list(nx.topological_sort(G))
        
        # Cache device profiles to avoid redundant lookups
        device_profiles = {}
        
        finish_time = {}
        windows = {}
        
        for sq_name in topo_order:
            sq = sq_by_name[sq_name]
            device = mapping.get(sq_name)
            
            if device is None:
                # Unmapped SQ (e.g., synthetic triggers) — skip but set finish=0
                finish_time[sq_name] = 0.0
                continue
            
            # Ready time = max over all predecessors' finish times
            ready_time = 0.0
            for pred_name in G.predecessors(sq_name):
                pred_finish = finish_time.get(pred_name, 0.0)
                pred_device = mapping.get(pred_name)
                
                if pred_device and pred_device != device:
                    try:
                        from .NetworkTopology import get_network_topology
                        topology = get_network_topology()
                        comm_time = topology.calculate_transfer_time(
                            pred_device, device, 1024)  # Conservative 1KB estimate
                    except Exception:
                        comm_time = 1.0  # Fallback: 1ms
                    ready_time = max(ready_time, pred_finish + comm_time)
                else:
                    ready_time = max(ready_time, pred_finish)
            
            # Let parallel SQs keep their natural overlapping windows.
            # Serialization is handled by contention scheduling, not here.
            start_time = ready_time
            
            # Get device profile (cached)
            if device not in device_profiles:
                device_profiles[device] = self._get_device_profile_for_mapping(
                    device, app_configs)
            profile = device_profiles[device]
            
            # Execution time on this device
            base_time = 10  # Default 10ms
            if hasattr(sq, 'execution_time_estimates') and sq.execution_time_estimates:
                base_time = sq.execution_time_estimates.get('default', 10)
            
            exec_time = profile.calculate_execution_time(base_time)
            exec_time *= 1.10  # 10% safety buffer
            
            finish_time[sq_name] = start_time + exec_time
            
            windows[sq_name] = {
                'device': device,
                'ready_time': ready_time,
                'exec_time': exec_time,
                'start_time': start_time,
                'finish_time': start_time + exec_time,
            }
        
        return windows
    
    def _detect_contention_groups(self, all_windows, shared_devices):
        """
        For each shared device, find ANY SQs whose execution windows overlap
        — regardless of which app they belong to.
        
        Uses IntervalTree for efficient interval overlap detection.
        
        :param all_windows: {app_id: {sq_name: window_dict}}
        :param shared_devices: {device_name: [app_id, ...]}
        :return: {device_name: [{'sq_a': (app, name, window), 'sq_b': ..., 
                                  'overlap': (start, end)}, ...]}
                 Only devices with actual contention are included.
        """
        contention_groups = {}
        
        for device_name, app_list in shared_devices.items():
            # Build ONE interval tree per device with ALL SQs
            tree = IntervalTree()
            all_sq_entries = []  # [(app_id, sq_name, window), ...]
            
            for app_id in app_list:
                for sq_name, window in all_windows.get(app_id, {}).items():
                    if window['device'] == device_name:
                        start = window['start_time']
                        end = window['finish_time']
                        if end > start:
                            tree.addi(start, end, (app_id, sq_name))
                            all_sq_entries.append((app_id, sq_name, window))
            
            # Find ALL overlapping pairs on this device
            device_conflicts = []
            seen_pairs = set()
            
            for app_a, sq_name_a, window_a in all_sq_entries:
                start_a = window_a['start_time']
                end_a = window_a['finish_time']
                
                overlaps = tree[start_a:end_a]
                
                for interval in overlaps:
                    app_b, sq_name_b = interval.data
                    
                    # Skip self
                    if app_a == app_b and sq_name_a == sq_name_b:
                        continue
                    
                    # Deduplicate: sort the pair so (A,B) and (B,A) are the same
                    pair_key = tuple(sorted([
                        (app_a, sq_name_a), (app_b, sq_name_b)
                    ]))
                    if pair_key in seen_pairs:
                        continue
                    seen_pairs.add(pair_key)
                    
                    overlap_start = max(start_a, interval.begin)
                    overlap_end = min(end_a, interval.end)
                    
                    # Look up window_b
                    window_b = all_windows[app_b][sq_name_b]
                    
                    device_conflicts.append({
                        'sq_a': (app_a, sq_name_a, window_a),
                        'sq_b': (app_b, sq_name_b, window_b),
                        'overlap': (overlap_start, overlap_end),
                    })
            
            if device_conflicts:
                contention_groups[device_name] = device_conflicts
                total_sqs = len(all_sq_entries)
                contending_sqs = set()
                for c in device_conflicts:
                    contending_sqs.add((c['sq_a'][0], c['sq_a'][1]))
                    contending_sqs.add((c['sq_b'][0], c['sq_b'][1]))
                logger.info(
                    f"Device '{device_name}': {len(device_conflicts)} contention "
                    f"points, {len(contending_sqs)} contending SQs "
                    f"out of {total_sqs} co-located")
        
        return contention_groups
    
    

    def _get_device_profile_for_mapping(self, device_name, app_configs):
        """
        Get device profile for execution time calculation during mapping.
        
        Uses device_profile_manager if available, otherwise returns default profile.
        """
        if self.device_profile_manager:
            try:
                return self.device_profile_manager.get_profile(device_name)
            except Exception as e:
                logger.warning(f"Could not get profile for '{device_name}': {e}, using default")
        
        # Fallback: create default profile
        from .DeviceProfile import DeviceProfile
        logger.debug(f"Using default profile for '{device_name}'")
        return DeviceProfile(
            name=device_name,
            cpu_speed=1.0,
            memory_size=1073741824,
            power_idle=2.0,
            power_active=5.0,
            power_transmit=3.0,
            power_receive=2.5,
            components={}
        )
    