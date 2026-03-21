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
RuntimeAdapter - Dynamic placement adaptation for device cluster changes.

Handles:
- Device failures (offline events)
- Device joins (new resources)
- Resource exhaustion (overload)
- Fast re-mapping (<200ms)
- Graceful degradation

Works with existing QPF infrastructure:
- Uses TaskCharacterizer metadata
- Compatible with UnifiedGraph mapping format
- Integrates with PlacementUtils constraint checking
"""

import yaml
import time
from typing import Dict, List, Set, Optional, Tuple, Any
from collections import defaultdict
from .Ensemble import TTEnsembleInfo
from enum import Enum

from . import DebugLogger
logger = DebugLogger.get_logger('RuntimeAdapter')


class DeploymentStrategy(Enum):
    """Deployment strategy determines runtime adaptation behavior."""
    QPF = "qpf"           # Constraint-aware + optimized placement alternatives
    STATIC = "static"     # Constraint-aware + first compatible device
    RANDOM = "random"     # No constraints + random selection
    TRIVIAL = "trivial"   # No constraints + any available device


class RuntimeAdapter:
    """
    Main runtime adaptation coordinator.
    Integrates all components for fast dynamic placement.
    """
    def __init__(self, characterization_data: Optional[Dict] = None, 
                 initial_topology: Optional[Dict] = None, 
                 initial_mapping: Optional[Dict[str, str]] = None, 
                 deployment_strategy: str = "static",
                 placement_alternatives: Optional[Dict[str, List[Dict]]] = None,
                 optimization_objective: str = "makespan",
                 graph: Optional[Any] = None):
        """
        Initialize RuntimeAdapter with strategy-aware adaptation support.
        
        :param characterization_data: Task profiles from TaskCharacterizer
        :param initial_topology: Dict of ensembles {device_id: TTEnsembleInfo}
        :param initial_mapping: Initial task placement {sq_name: device_id}
        :param deployment_strategy: One of 'qpf', 'static', 'random', 'trivial'
        :param placement_alternatives: QPF-computed ranked alternatives per task
                                       {sq_name: [{'device': str, 'score': float, 'rank': int}, ...]}
        :param optimization_objective: 'makespan' or 'energy' (for QPF selection)
        :param graph: The compiled TTGraph (needed for constraint access)
        """
        self.characterization_data = characterization_data or {}
        self.current_topology = initial_topology or {}
        self.current_mapping = initial_mapping or {}
        self.adaptation_count = 0
        self.remapping_times = []
        self.logger = logger
        
        # Strategy-aware adaptation
        self.deployment_strategy = DeploymentStrategy(deployment_strategy.lower())
        self.placement_alternatives = placement_alternatives or {}
        self.optimization_objective = optimization_objective
        self.graph = graph  # For constraint access
        
        # Build SQ constraints lookup from graph
        self.sq_constraints = {}
        if self.graph:
            for sq in self.graph.sqs:
                self.sq_constraints[sq.sq_name] = getattr(sq, 'constraints', None) or []
        
        # Use embedded characterization data
        self.task_profiles = self.characterization_data
        
        logger.info(f"RuntimeAdapter initialized: strategy={self.deployment_strategy.value}, "
                    f"{len(self.task_profiles)} tasks, {len(self.current_topology)} devices")
        if self.deployment_strategy == DeploymentStrategy.QPF:
            logger.info(f"  QPF mode: {len(self.placement_alternatives)} tasks with alternatives, "
                       f"objective={self.optimization_objective}")
            
    def _check_device_compatibility(self, sq_name: str, device_id: str, 
                                     ensemble_info) -> bool:
        """
        Check if a device satisfies the task's constraints.
        
        Uses TTQuery constraint checking (same as Mapper.py static_mapping).
        Only applies for QPF and Static strategies.
        
        :param sq_name: Task name
        :param device_id: Target device ID
        :param ensemble_info: TTEnsembleInfo for the device
        :return: True if device satisfies all constraints
        """
        if self.deployment_strategy in (DeploymentStrategy.RANDOM, DeploymentStrategy.TRIVIAL):
            # These strategies don't check constraints
            return True
        
        constraints = self.sq_constraints.get(sq_name, [])
        if not constraints:
            # No constraints = any device is compatible
            return True
        
        try:
            from ticktalkpython.Query import TTQuery, QueryOp
            query = TTQuery(constraints, QueryOp.AND)
            return query.test(ensemble_info)
        except Exception as e:
            self.logger.warning(f"Constraint check failed for {sq_name} on {device_id}: {e}")
            # Fail-safe: assume compatible if check fails
            return True
    
    def _filter_compatible_devices(self, sq_name: str, 
                                    available_devices: List[str],
                                    available_topology: Dict) -> List[str]:
        """
        Filter devices to only those satisfying task constraints.
        
        :param sq_name: Task name
        :param available_devices: List of candidate device IDs
        :param available_topology: {device_id: TTEnsembleInfo}
        :return: List of compatible device IDs
        """
        if self.deployment_strategy in (DeploymentStrategy.RANDOM, DeploymentStrategy.TRIVIAL):
            # No constraint filtering for these strategies
            return available_devices
        
        compatible = []
        for device_id in available_devices:
            ensemble_info = available_topology.get(device_id)
            if ensemble_info and self._check_device_compatibility(sq_name, device_id, ensemble_info):
                compatible.append(device_id)
        
        return compatible
    
    def _select_device_for_task(self, sq_name: str, 
                                 compatible_devices: List[str],
                                 device_loads: Dict[str, int]) -> Optional[str]:
        """
        Select the best device for a task based on deployment strategy.
        
        :param sq_name: Task name
        :param compatible_devices: Devices that satisfy constraints
        :param device_loads: Current task count per device {device_id: count}
        :return: Selected device ID or None
        """
        if not compatible_devices:
            return None
        
        if self.deployment_strategy == DeploymentStrategy.QPF:
            return self._select_device_qpf(sq_name, compatible_devices, device_loads)
        elif self.deployment_strategy == DeploymentStrategy.STATIC:
            return self._select_device_static(sq_name, compatible_devices, device_loads)
        elif self.deployment_strategy == DeploymentStrategy.RANDOM:
            return self._select_device_random(sq_name, compatible_devices, device_loads)
        elif self.deployment_strategy == DeploymentStrategy.TRIVIAL:
            return self._select_device_trivial(sq_name, compatible_devices)
        else:
            # Fallback to first available
            return compatible_devices[0]
    
    def _select_device_qpf(self, sq_name: str, 
                           compatible_devices: List[str],
                           device_loads: Dict[str, int]) -> Optional[str]:
        """
        QPF strategy: Select device with best pre-computed score.
        
        Uses placement alternatives computed during QPF optimization.
        Falls back to load-balanced selection if no alternatives available.
        """
        alternatives = self.placement_alternatives.get(sq_name, [])
        
        if alternatives:
            # Filter alternatives to only compatible and available devices
            for alt in alternatives:  # Already sorted by rank/score
                device = alt['device']
                if device in compatible_devices:
                    # Check load balancing - don't overload devices
                    if device_loads.get(device, 0) < 10:  # Max 10 tasks per device
                        self.logger.debug(f"QPF selection for {sq_name}: {device} "
                                         f"(rank={alt['rank']}, score={alt['score']:.4f})")
                        return device
        
        # Fallback: load-balanced selection among compatible devices
        self.logger.debug(f"QPF fallback to load-balanced for {sq_name}")
        return self._select_least_loaded(compatible_devices, device_loads)
    
    def _select_device_static(self, sq_name: str,
                              compatible_devices: List[str],
                              device_loads: Dict[str, int]) -> Optional[str]:
        """
        Static strategy: Select first compatible device with load balancing.
        
        Mimics Mapper.py static_mapping behavior but considers load.
        """
        # Sort by load, then take first (deterministic)
        sorted_by_load = sorted(compatible_devices, 
                                key=lambda d: device_loads.get(d, 0))
        
        if sorted_by_load:
            selected = sorted_by_load[0]
            self.logger.debug(f"Static selection for {sq_name}: {selected} "
                             f"(first compatible, load={device_loads.get(selected, 0)})")
            return selected
        return None
    
    def _select_device_random(self, sq_name: str,
                              compatible_devices: List[str],
                              device_loads: Dict[str, int]) -> Optional[str]:
        """
        Random strategy: Random selection with load balancing consideration.
        """
        import random
        
        # Filter out heavily loaded devices
        candidates = [d for d in compatible_devices 
                     if device_loads.get(d, 0) < 10]
        
        if not candidates:
            candidates = compatible_devices  # Fall back to all if all loaded
        
        if candidates:
            selected = random.choice(candidates)
            self.logger.debug(f"Random selection for {sq_name}: {selected}")
            return selected
        return None
    
    def _select_device_trivial(self, sq_name: str,
                               available_devices: List[str]) -> Optional[str]:
        """
        Trivial strategy: Any available device (no load balancing).
        """
        if available_devices:
            selected = available_devices[0]
            self.logger.debug(f"Trivial selection for {sq_name}: {selected}")
            return selected
        return None
    
    def _select_least_loaded(self, devices: List[str], 
                             device_loads: Dict[str, int]) -> Optional[str]:
        """Helper: Select least loaded device from list."""
        if not devices:
            return None
        
        min_load = min(device_loads.get(d, 0) for d in devices)
        least_loaded = [d for d in devices if device_loads.get(d, 0) == min_load]
        return least_loaded[0] if least_loaded else devices[0]
    
    def on_device_failure(self, failed_device: str, current_mapping: Dict[str, str],
                          available_topology: Dict, device_states: Optional[Dict] = None) -> Optional[Dict[str, Optional[str]]]:
        """
        Handle device failure with strategy-aware remapping.
        
        Adaptation behavior depends on deployment strategy:
        - QPF: Constraints → Load balance → Best from stored rankings
        - Static: Constraints → Load balance → First compatible
        - Random: Load balance → Random selection
        - Trivial: Any available device (no other considerations)
        
        :param failed_device: ID of the failed device
        :param current_mapping: Current task placement {sq_name: device_id}
        :param available_topology: Available devices {device_id: TTEnsembleInfo}
        :param device_states: Optional device states for validation
        :return: New mapping or None if adaptation failed
        """
        import time
        start_time = time.time()

        # Validate device is actually failed
        if device_states:
            device_state = device_states.get(failed_device, 'UNKNOWN')
            if device_state != 'FAILED':
                self.logger.info(f"Skipping adaptation for {failed_device} - state: {device_state}")
                # Convert to compatible return type (no changes needed)
                unchanged: Dict[str, Optional[str]] = dict(current_mapping)
                return unchanged

        # Identify orphaned tasks
        affected_sqs = [
            sq for sq, device in current_mapping.items()
            if device == failed_device
        ]

        if not affected_sqs:
            self.logger.info(f"No tasks on failed device {failed_device}")
            # Convert to compatible return type (no changes needed)
            unchanged: Dict[str, Optional[str]] = dict(current_mapping)
            return unchanged

        # Log orphaned SQs with criticality
        for sq in affected_sqs:
            crit = self.characterization_data.get(sq, {}).get('criticality', 'unknown')
            self.logger.info(
                f"[ADAPTATION] SQ orphaned: sq={sq}, failed_device={failed_device}, "
                f"criticality={crit}, strategy={self.deployment_strategy.value}"
            )
        
        self.logger.info(f"Found {len(affected_sqs)} tasks on failed device {failed_device}")

        # Get available devices (excluding failed)
        available_devices = [d for d in available_topology.keys() if d != failed_device]
        
        if not available_devices:
            self.logger.error("No available devices for remapping!")
            return None

        # Handle trivial strategy specially (no criticality, no constraints)
        if self.deployment_strategy == DeploymentStrategy.TRIVIAL:
            return self._trivial_adaptation(affected_sqs, current_mapping, available_devices)

        # For other strategies: prioritize by criticality
        essential_sqs = []
        important_sqs = []
        normal_sqs = []

        for sq_name in affected_sqs:
            criticality = self.characterization_data.get(sq_name, {}).get('criticality', 'normal')
            if criticality == 'essential':
                essential_sqs.append(sq_name)
            elif criticality == 'important':
                important_sqs.append(sq_name)
            else:
                normal_sqs.append(sq_name)

        # Create new mapping
        new_mapping: Dict[str, Optional[str]] = dict(current_mapping)
        for sq_name in affected_sqs:
            del new_mapping[sq_name]

        # Track device loads
        device_loads = {d: sum(1 for v in new_mapping.values() if v == d) 
                       for d in available_devices}

        # Remap by priority
        remapped_count = 0
        
        for sq_list, criticality_level in [(essential_sqs, 'essential'), 
                                        (important_sqs, 'important'), 
                                        (normal_sqs, 'normal')]:
            for sq_name in sq_list:
                # Filter to compatible devices (constraint checking)
                compatible_devices = self._filter_compatible_devices(
                    sq_name, available_devices, available_topology
                )
                
                if not compatible_devices:
                    self.logger.warning(
                        f"No compatible devices for {sq_name} ({criticality_level}) - "
                        f"marking as degraded"
                    )
                    new_mapping[sq_name] = None  # Graceful degradation
                    continue
                
                # Check capacity for normal tasks (graceful degradation)
                if criticality_level == 'normal':
                    if not self._has_remaining_capacity(device_loads):
                        new_mapping[sq_name] = None
                        self.logger.info(
                            f"[DEGRADED] SQ marked inactive: sq={sq_name}, "
                            f"criticality=normal, reason=capacity_exhausted"
                        )
                        continue
                
                # Select device based on strategy
                selected_device = self._select_device_for_task(
                    sq_name, compatible_devices, device_loads
                )
                
                if selected_device:
                    new_mapping[sq_name] = selected_device
                    device_loads[selected_device] = device_loads.get(selected_device, 0) + 1
                    remapped_count += 1
                    
                    self.logger.info(
                        f"[ADAPTATION] SQ reassigned: sq={sq_name}, "
                        f"new_device={selected_device}, criticality={criticality_level}, "
                        f"strategy={self.deployment_strategy.value}"
                    )
                else:
                    new_mapping[sq_name] = None
                    self.logger.warning(
                        f"Could not find device for {sq_name} ({criticality_level})"
                    )

        # Record adaptation metrics
        self.last_adaptation = {
            'device': failed_device,
            'timestamp': time.time(),
            'type': 'failure_recovery',
            'strategy': self.deployment_strategy.value,
            'remapped_count': remapped_count,
            'total_affected': len(affected_sqs)
        }

        elapsed_ms = (time.time() - start_time) * 1000
        self.remapping_times.append(elapsed_ms)
        self.adaptation_count += 1
        
        self.logger.info(
            f"Adaptation complete: {remapped_count}/{len(affected_sqs)} tasks remapped "
            f"in {elapsed_ms:.2f}ms using {self.deployment_strategy.value} strategy"
        )

        return new_mapping

    def _trivial_adaptation(self, affected_sqs: List[str], 
                            current_mapping: Dict[str, str],
                            available_devices: List[str]) -> Dict[str, Optional[str]]:
        """
        Trivial strategy adaptation: place all tasks on any available device.
        
        No criticality consideration, no constraints, no load balancing.
        Just survival mode.
        """
        new_mapping: Dict[str, Optional[str]] = dict(current_mapping)
        
        for sq_name in affected_sqs:
            del new_mapping[sq_name]
        
        # Just pick the first available device for all tasks
        target_device = available_devices[0]
        
        for sq_name in affected_sqs:
            new_mapping[sq_name] = target_device
            self.logger.info(
                f"[TRIVIAL] SQ reassigned: sq={sq_name}, new_device={target_device}"
            )
        
        return new_mapping

    def _has_remaining_capacity(self, device_loads: Dict[str, int], 
                                max_per_device: int = 10) -> bool:
        """Check if any device has remaining capacity."""
        return any(load < max_per_device for load in device_loads.values())
    
    def _check_device_capacity(self, available_devices):
        """Check if devices have capacity for more tasks."""
        max_tasks_per_device = 5  # Simple capacity limit
        current_loads = {d: 0 for d in available_devices}
    
        # Count current task assignments
        for device in self.current_mapping.values():
            if device in current_loads:
                current_loads[device] += 1
    
        # Return True if any device has capacity
        return any(load < max_tasks_per_device for load in current_loads.values())

        
    def get_current_mapping(self) -> Dict[str, str]:
        """
        Get current task placement.
        Compatible with existing mapping format.
        
        :return: {sq_name: device_name}
        """
        return self.current_mapping.copy()
    
    def cancel_ongoing_adaptation(self, device_id):
        """Cancel ongoing adaptation if device recovers quickly."""
        if hasattr(self, 'last_adaptation'):
            last_adapt = self.last_adaptation
            if (last_adapt.get('device') == device_id and 
                time.time() - last_adapt.get('timestamp', 0) < 5.0):  # 5 second window
                self.logger.info(f"Cancelling adaptation for recovered device {device_id}")
                return True
        return False
    
    def evaluate_migration_opportunities(self, recovered_device, current_mapping, available_topology):
        """Evaluate opportunities to migrate tasks back to recovered device."""
        migration_candidates = []
    
        # Get device capabilities
        if recovered_device not in available_topology:
            return migration_candidates
        
        recovered_ensemble = available_topology[recovered_device]
    
        # Assess each current task placement for migration potential
        for sq_name, current_device in current_mapping.items():
            if current_device == recovered_device:
                continue  # Already on recovered device
            
            # Get task characteristics
            task_criticality = self._get_task_criticality(sq_name)
        
            # Calculate migration benefit
            benefit = self._calculate_migration_benefit(
                sq_name, current_device, recovered_device, current_mapping
            )
        
            # Apply 10% threshold
            if benefit >= 10.0: # new performance data > current performance data
                migration_candidates.append({
                    'sq_name': sq_name,
                    'current_device': current_device,
                    'target_device': recovered_device,
                    'benefit': benefit,
                    'criticality': task_criticality,
                    'criticality_order': self._get_criticality_sort_order(task_criticality)
                })
    
        return migration_candidates

    def _calculate_migration_benefit(self, sq_name, current_device, target_device, current_mapping):
        """Calculate percentage benefit of migrating task to target device."""
        # Get task execution characteristics
        task_char = self.characterization_data.get(sq_name, {})
        exec_estimates = task_char.get('execution_time_estimates', {})
    
        # Get execution times
        current_exec_time = exec_estimates.get(current_device, exec_estimates.get('default', 0.01))
        target_exec_time = exec_estimates.get(target_device, exec_estimates.get('default', 0.01))
    
        if current_exec_time <= 0:
            return 0.0
        
        # Calculate improvement percentage
        improvement = ((current_exec_time - target_exec_time) / current_exec_time) * 100
    
        # Consider communication costs
        comm_penalty = self._calculate_communication_penalty(
            sq_name, current_device, target_device, current_mapping
        )
    
        net_benefit = improvement - comm_penalty
        return max(net_benefit, 0.0)

    def _calculate_communication_penalty(self, sq_name, current_device, target_device, current_mapping):
        """Calculate communication cost penalty for migration."""
        # Simple heuristic: penalty if migration increases inter-device communication
    
        # Count current same-device communications
        current_same_device = 0
        target_same_device = 0
    
        # This is simplified - in practice would analyze actual SQ dependencies
        for other_sq, other_device in current_mapping.items():
            if other_sq == sq_name:
                continue
            
            # Assume some probability of communication between tasks
            if other_device == current_device:
                current_same_device += 1
            elif other_device == target_device:
                target_same_device += 1
    
        # Penalty if migration reduces co-location
        if current_same_device > target_same_device:
            return 5.0  # 5% penalty for increased communication
    
        return 0.0

    def _get_task_criticality(self, sq_name):
        """Get task criticality from characterization data."""
        task_char = self.characterization_data.get(sq_name, {})
        return task_char.get('criticality', 'normal')

    def _get_criticality_sort_order(self, criticality):
        """Convert criticality to numeric sort order."""
        ranks = {'essential': 3, 'important': 2, 'normal': 1}
        return ranks.get(criticality, 1)

    def update_task_mapping(self, sq_name, new_device):
        """Update task mapping after migration."""
        if hasattr(self, 'current_mapping'):
            old_device = self.current_mapping.get(sq_name, 'unknown')
            self.current_mapping[sq_name] = new_device
            self.logger.debug(f"Task mapping updated: {sq_name} {old_device} → {new_device}")

    def evaluate_new_device_migrations(self, new_device, available_topology):
        """Evaluate migration opportunities for newly joined device."""
        migration_candidates = []
    
        if not hasattr(self, 'current_mapping') or not self.current_mapping:
            return migration_candidates
        
        # Get new device info
        if new_device not in available_topology:
            return migration_candidates
        
        new_ensemble = available_topology[new_device]
        device_type = new_ensemble.components.get('type', 'default')
    
        # Assess each current task for migration potential
        for sq_name, current_device in self.current_mapping.items():
            # Skip if already on new device
            if current_device == new_device:
                continue
            
            # Calculate migration benefit
            benefit = self._calculate_new_device_migration_benefit(
                sq_name, current_device, device_type
            )
        
            # Apply 10% threshold
            if benefit >= 10.0:
                task_criticality = self._get_task_criticality(sq_name)
            
                migration_candidates.append({
                    'sq_name': sq_name,
                    'current_device': current_device,
                    'target_device': new_device,
                    'benefit': benefit,
                    'criticality': task_criticality,
                    'criticality_order': self._get_criticality_sort_order(task_criticality)
                })
    
        # Sort by criticality and benefit
        migration_candidates.sort(
            key=lambda x: (x['criticality_order'], x['benefit']), 
            reverse=True
        )
    
        return migration_candidates

    def _calculate_new_device_migration_benefit(self, sq_name, current_device, new_device_type):
        """Calculate benefit of migrating task to new device type."""
        # Get task execution characteristics
        task_char = self.characterization_data.get(sq_name, {})
        exec_estimates = task_char.get('execution_time_estimates', {})
    
        # Get execution times  
        current_exec_time = exec_estimates.get('default', 0.01)  # Simplified fallback
        new_device_exec_time = exec_estimates.get(new_device_type, exec_estimates.get('default', 0.01))
    
        if current_exec_time <= 0:
            return 0.0
        
        # Calculate improvement percentage
        improvement = ((current_exec_time - new_device_exec_time) / current_exec_time) * 100
    
        # Conservative approach - slight penalty for disruption
        disruption_penalty = 2.0  # 2% penalty for migration disruption
        net_benefit = improvement - disruption_penalty
    
        return max(net_benefit, 0.0)
    
        return opportunities