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
ObjectiveCalculator - Calculate objective function values for placement mappings.

Implementation:
- calculate_makespan(): Total execution time using critical path with device queuing
- calculate_energy(): Total energy consumption (execution + communication)

These functions enable quantitative evaluation of placement quality.
'''

from typing import Dict, List, Set, Tuple
from collections import defaultdict, deque
import networkx as nx
from . import DebugLogger
from . import PlacementUtils

logger = DebugLogger.get_logger('ObjectiveCalculator')


class ObjectiveCalculator:
    """
    Calculate objective function values for application placements.
    
    Given a placement mapping (which device each SQ is assigned to) and the
    unified placement graph, calculate quantitative metrics like makespan
    and energy consumption.
    """
    
    def calculate_makespan(self, mapping: Dict[str, str], unified_graph) -> float:
        """
        Calculate makespan using critical path analysis with device queuing.
        
        Walks the DAG in topological order. For each SQ, computes the earliest
        possible start time considering: (1) all predecessors must have finished,
        (2) cross-device data transfers add communication latency,
        (3) the target device may be busy executing a previously scheduled SQ.
        The makespan is the latest finish time across all SQs.
    
        :param mapping: Dict mapping SQ names to device names
        :param unified_graph: UnifiedPlacementGraph
        :return: Makespan in seconds
        """
        if not mapping:
            return 0.0
    
        logger.debug(f"Calculating makespan for {len(mapping)} SQs")

        # Runtime adaptation support: filter out SQs on failed devices
        if hasattr(self, 'device_states'):
            available_mapping = {
                sq: device for sq, device in mapping.items()
                if self._is_device_available(device)
            }
            if len(available_mapping) != len(mapping):
                logger.warning(f"Filtered {len(mapping) - len(available_mapping)} "
                                f"SQs with unavailable devices")
                mapping = available_mapping
    
        # Retrieve the TTGraph from the unified placement graph
        app_id = next(iter(unified_graph.applications.keys()))
        app_data = unified_graph.applications[app_id]
    
        if isinstance(app_data, dict):
            ttgraph = app_data['graph']
        else:
            ttgraph = app_data.graph
    
        # Build a NetworkX DAG from the TTGraph for topological traversal
        G = ttgraph.get_dag()

        # Device queuing: tracks when each device becomes free after its last assigned SQ
        # SQs assigned to the same device must execute sequentially (no overlap)
        device_available_time = defaultdict(float)
        # Per-SQ finish times, used to propagate timing to downstream SQs
        finish_time = {}

        # Topological sort ensures every SQ is scheduled after all its predecessors
        topo_order = list(nx.topological_sort(G))
    
        for sq_name in topo_order:
            sq = G.nodes[sq_name]['sq']
            # Look up which device this SQ is assigned to in the current mapping
            target_device = mapping.get(sq_name, 'default')
        
            # Compute ready_time: the earliest this SQ could start based on predecessor completion
            # For each predecessor, account for cross-device communication latency
            ready_time = 0.0
            for pred_name in G.predecessors(sq_name):
                pred_sq = G.nodes[pred_name]['sq']
                pred_finish = finish_time[pred_name]
                pred_device = mapping.get(pred_name, 'default')
            
                if pred_device != target_device:
                    # Cross-device edge: add network transfer time (latency + data_size/bandwidth)
                    data_size = self._get_communication_data_size_from_graph(pred_name, sq_name, unified_graph)
                    comm_time = self._get_communication_time(
                        pred_device, target_device, data_size
                    )
                    ready_time = max(ready_time, pred_finish + comm_time)
                else:
                    # Same device: no communication cost, just wait for predecessor to finish
                    ready_time = max(ready_time, pred_finish)
        
            # Device queuing: SQ cannot start until the device is free from prior work
            # This is what causes fork-join serialization when branches share a device
            start_time = max(ready_time, device_available_time[target_device])
        
            # Look up execution time for this SQ on this specific device (from compile-time estimates)
            exec_time = self._get_execution_time(sq, target_device)
            finish_time[sq_name] = start_time + exec_time
        
            # Update device availability: this device is now busy until this SQ finishes
            device_available_time[target_device] = finish_time[sq_name]
        
            logger.debug(f"  {sq_name} @ {target_device}: "
                        f"ready={ready_time:.4f}s, start={start_time:.4f}s, "
                        f"finish={finish_time[sq_name]:.4f}s")
    
        # Makespan = the latest finish time across all SQs in the entire DAG
        makespan = max(finish_time.values()) if finish_time else 0.0
        logger.info(f"Calculated makespan: {makespan:.6f} seconds")
    
        return makespan
    
    def _get_execution_time(self, sq, device: str) -> float:
        """
        Get execution time for SQ on specified device.
        
        :param sq: SQ object with execution_time_estimates
        :param device: Target device name
        :return: Execution time in seconds
        """
        if hasattr(sq, 'execution_time_estimates'):
            # Try device-specific, then default, then fallback
            return sq.execution_time_estimates.get(
                device, 
                sq.execution_time_estimates.get('default', 0.001)
            )
        
        logger.warning(f"SQ {sq.sq_name} missing execution_time_estimates, using 1ms")
        return 0.001  # 1ms fallback
    
    def _get_communication_time(self, src_device: str, dst_device: str, 
                                data_size: int) -> float:
        """
        Calculate communication time between two devices.
        
        Uses NetworkTopology for realistic latency/bandwidth modeling.
        
        :param src_device: Source device name
        :param dst_device: Destination device name
        :param data_size: Data size in bytes
        :return: Communication time in seconds
        """
        if src_device == dst_device:
            # Same device - no network communication
            return 0.0
        
        # Use NetworkTopology for realistic delays
        try:
            from .NetworkTopology import get_network_topology
            topology = get_network_topology()
            
            # Calculate: latency + (data_size / bandwidth)
            transfer_time = topology.calculate_transfer_time(
                src_device, dst_device, data_size
            )
            
            return transfer_time
            
        except Exception as e:
            # Fallback if topology not available
            logger.debug(f"NetworkTopology not available, using fixed delay: {e}")
            return 0.005  # 5ms fixed delay
    
    def calculate_energy(self, mapping: Dict[str, str], unified_graph) -> float:
        """
        Calculate total energy consumption for a placement.
    
        Energy = Execution Energy + Communication Energy
    
        :param mapping: Dict mapping SQ names to device names
        :param unified_graph: UnifiedPlacementGraph
        :return: Total energy in joules
        """
        if not mapping:
            return 0.0
    
        logger.debug(f"Calculating energy for mapping with {len(mapping)} SQs")

        # Filter out unavailable devices from calculations
        if hasattr(self, 'device_states'):
            available_mapping = {
                sq: device for sq, device in mapping.items()
                if self._is_device_available(device)
            }

            if len(available_mapping) != len(mapping):
                logger.warning(f"Filtered {len(mapping) - len(available_mapping)} "
                                f"SQs with unavailable devices")
                mapping = available_mapping
    
        # Get application
        app_id = next(iter(unified_graph.applications.keys()))
        app_data = unified_graph.applications[app_id]
    
        # Extract graph
        if isinstance(app_data, dict):
            ttgraph = app_data['graph']
        else:
            ttgraph = app_data.graph
    
        # Handle sqs being list or dict
        sqs_list = ttgraph.sqs if isinstance(ttgraph.sqs, list) else list(ttgraph.sqs.values())
    
        total_energy = 0.0
    
        # 1. Execution Energy
        for sq in sqs_list:
            sq_name = sq.sq_name
            if sq_name not in mapping:
                continue
        
            device = mapping[sq_name]
        
            # Get execution energy for this SQ on this device
            if hasattr(sq, 'energy_cost_estimates'):
                energy = sq.energy_cost_estimates.get(device, 
                        sq.energy_cost_estimates.get('default', 0.0))
                total_energy += energy
                logger.debug(f"  {sq_name} @ {device}: {energy:.6f}J")
    
        # 2. Communication Energy
        comm_energy = 0.0
        ipp_to_sq = ttgraph.get_ipp_to_sq_dict()
    
        for sq in sqs_list:
            src_device = mapping.get(sq.sq_name, 'default')
        
            for opp in sq.opps:
                data_name = opp.data_name
                if data_name not in ipp_to_sq:
                    continue
            
                for dest_sq, port_num in ipp_to_sq[data_name]:
                    dst_device = mapping.get(dest_sq.sq_name, 'default')
                
                    # Only count cross-device communication
                    if src_device != dst_device:
                        # Calculate communication energy
                        data_size = self._get_communication_data_size_from_graph(sq.sq_name, dest_sq.sq_name, unified_graph)
                        comm_time = self._get_communication_time(
                            src_device, dst_device, data_size
                        )
                    
                        # Energy = (power_tx + power_rx) × time
                        try:
                            from .DeviceProfile import get_profile_manager
                            pm = get_profile_manager()
                        
                            src_profile = pm.profiles.get(src_device)
                            dst_profile = pm.profiles.get(dst_device)
                        
                            if src_profile and dst_profile:
                                power_total = (src_profile.power_transmit + 
                                            dst_profile.power_receive)
                                edge_energy = power_total * comm_time
                                comm_energy += edge_energy
                            else:
                                # Fallback: 1mJ per transfer
                                comm_energy += 0.001
                        except:
                            # Fallback: 1mJ per transfer
                            comm_energy += 0.001
    
        total_energy += comm_energy
    
        logger.info(f"Calculated energy: {total_energy:.6f} joules "
                f"(exec: {total_energy - comm_energy:.6f}J, comm: {comm_energy:.6f}J)")
    
        return total_energy
    
    def _get_communication_data_size_from_graph(self, src_sq_name, dst_sq_name, unified_graph):
        """Get communication data size from embedded graph analysis."""
        
        # Try to get from embedded communication analysis
        for app_id, app_data in unified_graph.applications.items():
            graph = app_data['graph'] if isinstance(app_data, dict) else app_data.graph
            
            # Check if graph has embedded communication data sizes
            if hasattr(graph, 'communication_data_sizes'):
                # Find the data flow between these SQs
                for sq in graph.sqs:
                    if sq.sq_name == src_sq_name and hasattr(sq, 'opps'):
                        for opp in sq.opps:
                            # Check if this output goes to the destination SQ
                            if hasattr(opp, 'estimated_data_size'):
                                # Verify this output is consumed by dst_sq_name
                                ipp_to_sq = graph.get_ipp_to_sq_dict()
                                if opp.data_name in ipp_to_sq:
                                    for dest_sq, port_num in ipp_to_sq[opp.data_name]:
                                        if dest_sq.sq_name == dst_sq_name:
                                            return opp.estimated_data_size
        
        # Fallback to reasonable default
        logger.debug(f"No embedded data size found for {src_sq_name} -> {dst_sq_name}, using 1KB fallback")
        return 1024  # 1KB fallback
    
    def calculate_deadline_slack(self, mapping: Dict[str, str], unified_graph,
                                  deadline: float) -> float:
        """
        Calculate margin before deadline (slack).
        
        Positive slack means completion before deadline (safe).
        Negative slack means deadline miss.
        
        :param mapping: Dict mapping SQ unique_ids to device names
        :param unified_graph: UnifiedPlacementGraph
        :param deadline: Deadline in seconds
        :return: Slack in seconds (deadline - makespan)
        """
        makespan = self.calculate_makespan(mapping, unified_graph)
        slack = deadline - makespan
        
        if slack >= 0:
            logger.info(f"Deadline slack: +{slack:.6f}s "
                       f"(SAFE - completes {slack:.6f}s before deadline)")
        else:
            logger.warning(f"Deadline slack: {slack:.6f}s "
                          f"(MISS - exceeds deadline by {-slack:.6f}s)")
        
        return slack
    
    def _is_device_available(self, device_name):
        """Check if device is available for calculations."""
        if hasattr(self, 'device_states'):
            state = self.device_states.get(device_name, 'ACTIVE')
            return state in ['ACTIVE', 'AVAILABLE', 'REJOINING']
        return True  # Default to available if no state tracking

    def set_device_states(self, device_states):
        """Set device states for state-aware calculations."""
        self.device_states = device_states

    def calculate_migration_impact(self, current_mapping, proposed_migration, unified_graph):
        """Calculate performance impact of proposed task migration."""
        # Create mapping with proposed migration
        new_mapping = current_mapping.copy()
        new_mapping[proposed_migration['sq_name']] = proposed_migration['target_device']
    
        # Calculate current and new performance
        current_makespan = self.calculate_makespan(current_mapping, unified_graph)
        new_makespan = self.calculate_makespan(new_mapping, unified_graph)
    
        # Calculate improvement
        if current_makespan > 0:
            improvement = ((current_makespan - new_makespan) / current_makespan) * 100
        else:
            improvement = 0.0
    
        return {
            'makespan_improvement': improvement,
            'current_makespan': current_makespan,
            'new_makespan': new_makespan,
            'beneficial': improvement >= 10.0  # 10% threshold
        }

    def assess_system_stability_impact(self, current_mapping, device_loads):
        """Assess if system can handle task migrations without instability."""
        max_device_load = max(device_loads.values()) if device_loads else 0
        avg_device_load = sum(device_loads.values()) / len(device_loads) if device_loads else 0
    
        # System is stable if max load < 80% and load is balanced
        load_imbalance = max_device_load - avg_device_load
    
        return {
            'stable': max_device_load < 0.8 and load_imbalance < 0.3,
            'max_load': max_device_load,
            'avg_load': avg_device_load,
            'load_imbalance': load_imbalance
        }
    
    def assess_new_device_impact(self, current_mapping, new_device_info, unified_graph):
        """Assess overall system impact of adding new device."""
        device_name = new_device_info.name
        device_type = new_device_info.components.get('type', 'default')
    
        # Calculate current system performance
        current_makespan = self.calculate_makespan(current_mapping, unified_graph)
        current_energy = self.calculate_energy(current_mapping, unified_graph)
    
        # Simulate optimal task redistribution with new device
        optimal_mapping = self._simulate_optimal_redistribution(
            current_mapping, device_name, device_type, unified_graph
        )
    
        # Calculate improved performance
        new_makespan = self.calculate_makespan(optimal_mapping, unified_graph)
        new_energy = self.calculate_energy(optimal_mapping, unified_graph)
    
        # Calculate improvements
        makespan_improvement = ((current_makespan - new_makespan) / current_makespan) * 100 if current_makespan > 0 else 0
        energy_improvement = ((current_energy - new_energy) / current_energy) * 100 if current_energy > 0 else 0
    
        return {
            'makespan_improvement': makespan_improvement,
            'energy_improvement': energy_improvement,
            'beneficial': makespan_improvement >= 10.0,
            'current_makespan': current_makespan,
            'optimal_makespan': new_makespan,
            'tasks_affected': len([k for k, v in optimal_mapping.items() if v != current_mapping.get(k)])
        }

    def _simulate_optimal_redistribution(self, current_mapping, new_device, new_device_type, unified_graph):
        """Simulate optimal task redistribution including new device."""
        optimal_mapping = current_mapping.copy()
    
        # Simple greedy redistribution simulation
        improvements_made = True
        while improvements_made:
            improvements_made = False
            best_benefit = 0
            best_migration = None
        
            for sq_name, current_device in optimal_mapping.items():
                # Skip if already on new device
                if current_device == new_device:
                    continue
                    
                # Check feasibility first, then calculate benefit
                if not self._is_migration_feasible(sq_name, new_device, unified_graph):
                    continue # Skip this migration if not feasible

                # Calculate benefit of moving to new device
                benefit = self._estimate_migration_benefit(sq_name, current_device, new_device_type, unified_graph)
                
                if benefit > best_benefit and benefit >= 10.0:
                    best_benefit = benefit
                    best_migration = sq_name
            
            if best_migration:
                optimal_mapping[best_migration] = new_device
                improvements_made = True
        
        return optimal_mapping
    
    def _estimate_migration_benefit(self, sq_name, current_device, target_device_type, unified_graph):
        """Estimate migration benefit using DeviceProfile specifications and constraint validation."""
        
        # FIRST: Check if migration is even feasible
        if not self._is_migration_feasible(sq_name, current_device, unified_graph):
            logger.debug(f"Migration not feasible: {sq_name} -> {target_device_type}")
            return 0.0  # No benefit if migration violates constraints
        
        try:
            from .DeviceProfile import get_profile_manager
            pm = get_profile_manager()
            
            # Get current device profile directly
            current_profile = pm.get_profile(current_device)
            current_cpu_speed = current_profile.cpu_speed
            
            # Get target device type specifications
            target_specs = pm.device_types.get(target_device_type, {'cpu_speed': 1.0})
            target_cpu_speed = target_specs.get('cpu_speed', 1.0)
            
            if current_cpu_speed <= 0:
                return 5.0  # Default if invalid specs
                
            # Calculate speedup ratio
            speedup_ratio = target_cpu_speed / current_cpu_speed
            cpu_improvement_percent = (speedup_ratio - 1.0) * 100
            
            # Apply task-specific factors based on embedded criticality analysis
            task_factor = self._get_task_computational_factor(sq_name, unified_graph)
            final_improvement = cpu_improvement_percent * task_factor
            
            return max(final_improvement, 0.0)
            
        except Exception as e:
            logger.warning(f"Could not load DeviceProfile for migration analysis: {e}")
            return 5.0  # Default fallback

    def _get_task_computational_factor(self, sq_name, unified_graph):
        """Get computational factor from embedded task criticality analysis."""
        
        try:
            # Find the SQ object with embedded criticality analysis
            for app_id, app_data in unified_graph.applications.items():
                graph = app_data['graph'] if isinstance(app_data, dict) else app_data.graph
                
                for sq in graph.sqs:
                    if sq.sq_name == sq_name:
                        # Use embedded criticality analysis
                        if hasattr(sq, 'criticality'):
                            if sq.criticality == 'essential':
                                return 1.0  # Essential tasks are CPU-intensive
                            elif sq.criticality == 'important':
                                return 0.8  # Important tasks moderately CPU-intensive
                            else:  # normal
                                return 0.6  # Normal tasks less CPU-intensive
                        
                        # Fallback to structural analysis if available
                        if hasattr(sq, 'dependency_metrics'):
                            bottleneck_score = sq.dependency_metrics.get('bottleneck_score', 0.0)
                            if bottleneck_score >= 4.0:
                                return 1.0  # High bottleneck = CPU-intensive
                            elif bottleneck_score >= 2.0:
                                return 0.8  # Medium bottleneck
                            else:
                                return 0.6  # Low bottleneck
        
        except Exception as e:
            logger.debug(f"Could not access embedded criticality for {sq_name}: {e}")
        
        return 0.7  # Default fallback
    
    def _find_sq_in_unified_graph(self, sq_name, unified_graph):
        """Find SQ object by name in unified graph."""
        for app_id, app_data in unified_graph.applications.items():
            graph = app_data['graph'] if isinstance(app_data, dict) else app_data.graph
            
            for sq in graph.sqs:
                if sq.sq_name == sq_name:
                    return sq
        return None

    def _get_device_ensemble_info(self, device_name, unified_graph):
        """Get ensemble info from unified graph - proper architectural approach."""
        try:
            # Access ensemble info through unified graph's ensemble registry
            if hasattr(unified_graph, 'ensembles') and device_name in unified_graph.ensembles:
                return unified_graph.ensembles[device_name]
            
            # Check if device_name is an alias or alternate reference
            for ens_name, ens_info in unified_graph.ensembles.items():
                if hasattr(ens_info, 'name') and ens_info.name == device_name:
                    return ens_info
            
            logger.warning(f"Device '{device_name}' not found in unified graph ensembles. "
                        f"Available: {list(unified_graph.ensembles.keys())}")
            return None
            
        except Exception as e:
            logger.warning(f"Could not access ensemble info for {device_name}: {e}")
            return None

    def _is_migration_feasible(self, sq_name, target_device, unified_graph):
        """Check if SQ can be placed on target device using PlacementUtils."""
        try:
            # Get SQ object
            sq = self._find_sq_in_unified_graph(sq_name, unified_graph)
            if not sq:
                logger.warning(f"Could not find SQ {sq_name} in unified graph")
                return True  # Default to feasible if SQ not found
            
            # Get target device ensemble info from unified graph
            target_ensemble_info = self._get_device_ensemble_info(target_device, unified_graph)
            if not target_ensemble_info:
                logger.debug(f"No ensemble info for {target_device}, assuming feasible")
                return True  # Default to feasible if no ensemble info
            
            # Use PlacementUtils to check constraints
            is_feasible = PlacementUtils.check_sq_constraints(sq, target_ensemble_info)
            
            if not is_feasible:
                logger.debug(f"Migration not feasible: {sq_name} -> {target_device} "
                            f"(constraints: {sq.constraints})")
            
            return is_feasible
            
        except Exception as e:
            logger.warning(f"Error checking migration feasibility for {sq_name} -> {target_device}: {e}")
            return True  # Default to feasible on error