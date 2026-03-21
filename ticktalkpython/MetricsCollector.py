# Copyright 2025 TTPython Extensions - Metrics Collection
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

"""
MetricsCollector - Centralized metrics collection for TTPython evaluation.

This module provides hook-based metrics collection that integrates with
RuntimeManager to capture:
- Deployment timing
- Execution timing (per-SQ and total makespan)
- Adaptation events (failures, remapping)
- Energy estimates

Place this file in: ticktalkpython/MetricsCollector.py
"""

import time
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any


@dataclass
class SQMetrics:
    """Metrics for a single SQ execution."""
    sq_name: str
    device: str
    execution_start: float = 0.0
    execution_end: float = 0.0
    tokens_received: int = 0
    tokens_produced: int = 0
    
    @property
    def execution_time_ms(self) -> float:
        if self.execution_end > 0 and self.execution_start > 0:
            return (self.execution_end - self.execution_start) * 1000
        return 0.0


@dataclass 
class AdaptationMetrics:
    """Metrics for a single adaptation event."""
    event_type: str  # 'device_failure', 'device_addition', 'migration'
    device_id: str
    timestamp: float
    adaptation_start: float = 0.0
    adaptation_end: float = 0.0
    affected_sqs: List[str] = field(default_factory=list)
    new_mapping: Dict[str, str] = field(default_factory=dict)
    
    @property
    def adaptation_time_ms(self) -> float:
        if self.adaptation_end > 0 and self.adaptation_start > 0:
            return (self.adaptation_end - self.adaptation_start) * 1000
        return 0.0


@dataclass
class ExecutionMetrics:
    """Aggregated metrics for a complete execution."""
    graph_name: str
    deployment_start: float = 0.0
    deployment_end: float = 0.0
    execution_start: float = 0.0
    execution_end: float = 0.0
    mapping: Dict[str, str] = field(default_factory=dict)
    sq_metrics: Dict[str, SQMetrics] = field(default_factory=dict)
    adaptation_events: List[AdaptationMetrics] = field(default_factory=list)
    deadlines_met: int = 0
    deadlines_missed: int = 0
    
    @property
    def deployment_time_ms(self) -> float:
        if self.deployment_end > 0 and self.deployment_start > 0:
            return (self.deployment_end - self.deployment_start) * 1000
        return 0.0
    
    @property
    def makespan_ms(self) -> float:
        if self.execution_end > 0 and self.execution_start > 0:
            return (self.execution_end - self.execution_start) * 1000
        return 0.0
    
    @property
    def total_adaptation_time_ms(self) -> float:
        return sum(e.adaptation_time_ms for e in self.adaptation_events)


class MetricsCollector:
    """
    Centralized metrics collection for TTPython evaluation.
    
    Provides hook methods called from RuntimeManager at key execution points.
    """
    
    def __init__(self, output_file: Optional[str] = None):
        self.output_file = output_file
        self.executions: Dict[str, ExecutionMetrics] = {}
        self.current_graph: Optional[str] = None
        self.current_adaptation: Optional[AdaptationMetrics] = None
        
        # Per-SQ timing tracking
        self.sq_start_times: Dict[str, float] = {}
        
        # Energy estimation (from device profiles)
        self.device_power: Dict[str, float] = {
            'raspberry_pi_4': 5.0,      # Watts
            'jetson_nano': 15.0,
            'nvidia_jetson_xavier': 30.0,
            'server_x86': 200.0
        }
        
        self.logger = None
    
    def set_logger(self, logger):
        """Set logger from RuntimeManager."""
        self.logger = logger
    
    def _log(self, level: str, msg: str):
        """Log with fallback to print."""
        if self.logger:
            getattr(self.logger, level)(f"[Metrics] {msg}")
        else:
            print(f"[Metrics][{level.upper()}] {msg}")
    
    # ========== HOOK METHODS ==========
    
    def on_graph_deployment_start(self, graph_name: str):
        """Called at start of InstantiateAndMapGraph."""
        self.current_graph = graph_name
        self.executions[graph_name] = ExecutionMetrics(graph_name=graph_name)
        self.executions[graph_name].deployment_start = time.time()
        self._log('info', f"Deployment started: {graph_name}")
    
    def on_graph_deployed(self, graph_name: str, mapping: Dict[str, str]):
        """Called at end of InstantiateAndMapGraph."""
        if graph_name not in self.executions:
            self.executions[graph_name] = ExecutionMetrics(graph_name=graph_name)
        
        metrics = self.executions[graph_name]
        metrics.deployment_end = time.time()
        metrics.mapping = mapping.copy()
        
        for sq_name, device in mapping.items():
            metrics.sq_metrics[sq_name] = SQMetrics(sq_name=sq_name, device=device)
        
        self._log('info', f"Deployment completed: {graph_name}, "
                         f"{len(mapping)} SQs, {metrics.deployment_time_ms:.2f}ms")
    
    def on_execution_started(self, graph_name: str):
        """Called when ExecuteGraphOnInputs begins."""
        if graph_name not in self.executions:
            self._log('warning', f"Execution started for unknown graph: {graph_name}")
            return
        
        self.executions[graph_name].execution_start = time.time()
        self.current_graph = graph_name
        self._log('info', f"Execution started: {graph_name}")
    
    def on_sq_output(self, sq_name: str, device: str, token_value: Any = None):
        """Called when LogOutputToken is received."""
        if self.current_graph and self.current_graph in self.executions:
            metrics = self.executions[self.current_graph]
            
            if sq_name in metrics.sq_metrics:
                metrics.sq_metrics[sq_name].tokens_produced += 1
                metrics.sq_metrics[sq_name].execution_end = time.time()
            
            metrics.execution_end = time.time()
    
    def on_adaptation_start(self, event_type: str, device_id: str, 
                            affected_sqs: Optional[List[str]] = None):
        """Called at start of HandleDeviceFailure."""
        self.current_adaptation = AdaptationMetrics(
            event_type=event_type,
            device_id=device_id,
            timestamp=time.time(),
            adaptation_start=time.time(),
            affected_sqs=affected_sqs or []
        )
        self._log('info', f"Adaptation started: {event_type} on {device_id}, "
                         f"affecting {len(affected_sqs or [])} SQs")
    
    def on_adaptation_end(self, new_mapping: Dict[str, str], success: bool = True):
        """Called at end of HandleDeviceFailure."""
        if self.current_adaptation:
            self.current_adaptation.adaptation_end = time.time()
            self.current_adaptation.new_mapping = new_mapping.copy()
            
            if self.current_graph and self.current_graph in self.executions:
                self.executions[self.current_graph].adaptation_events.append(
                    self.current_adaptation
                )
            
            self._log('info', f"Adaptation completed: {self.current_adaptation.adaptation_time_ms:.2f}ms, "
                             f"success={success}")
            self.current_adaptation = None
    
    def on_execution_ended(self, graph_name: str):
        """Called when EndExecution is received."""
        if graph_name not in self.executions:
            return
        
        metrics = self.executions[graph_name]
        if metrics.execution_end == 0:
            metrics.execution_end = time.time()
        
        self._log('info', f"Execution ended: {graph_name}, "
                         f"makespan={metrics.makespan_ms:.2f}ms, "
                         f"adaptations={len(metrics.adaptation_events)}")
    
    def on_deadline_check(self, sq_name: str, deadline: float, actual_time: float):
        """Called when a deadline is checked."""
        if self.current_graph and self.current_graph in self.executions:
            metrics = self.executions[self.current_graph]
            if actual_time <= deadline:
                metrics.deadlines_met += 1
            else:
                metrics.deadlines_missed += 1
    
    # ========== RESULTS EXPORT ==========
    
    def get_summary(self, graph_name: Optional[str] = None) -> Dict[str, Any]:
        """Get summary metrics."""
        if graph_name:
            if graph_name not in self.executions:
                return {}
            return self._metrics_to_dict(self.executions[graph_name])
        
        return {name: self._metrics_to_dict(m) for name, m in self.executions.items()}
    
    def _metrics_to_dict(self, metrics: ExecutionMetrics) -> Dict[str, Any]:
        """Convert ExecutionMetrics to dictionary."""
        return {
            'graph_name': metrics.graph_name,
            'deployment_time_ms': metrics.deployment_time_ms,
            'makespan_ms': metrics.makespan_ms,
            'mapping': metrics.mapping,
            'sq_count': len(metrics.sq_metrics),
            'sq_metrics': {
                name: {
                    'device': sq.device,
                    'execution_time_ms': sq.execution_time_ms,
                    'tokens_produced': sq.tokens_produced
                }
                for name, sq in metrics.sq_metrics.items()
            },
            'adaptation_count': len(metrics.adaptation_events),
            'total_adaptation_time_ms': metrics.total_adaptation_time_ms,
            'adaptation_events': [
                {
                    'event_type': e.event_type,
                    'device_id': e.device_id,
                    'adaptation_time_ms': e.adaptation_time_ms,
                    'affected_sqs': e.affected_sqs
                }
                for e in metrics.adaptation_events
            ],
            'deadlines_met': metrics.deadlines_met,
            'deadlines_missed': metrics.deadlines_missed,
            'energy_estimate_joules': self._estimate_energy(metrics)
        }
    
    def _estimate_energy(self, metrics: ExecutionMetrics) -> float:
        """Estimate energy consumption."""
        total_energy = 0.0
        for sq_name, sq_metrics in metrics.sq_metrics.items():
            device = sq_metrics.device
            power = self.device_power.get(device, 10.0)
            time_seconds = sq_metrics.execution_time_ms / 1000
            total_energy += power * time_seconds
        return total_energy
    
    def save_metrics(self, filepath: Optional[str] = None):
        """Save all metrics to JSON file."""
        filepath = filepath or self.output_file or './metrics_output.json'
        
        summary = self.get_summary()
        
        with open(filepath, 'w') as f:
            json.dump(summary, f, indent=2)
        
        self._log('info', f"Metrics saved to {filepath}")
        return filepath
    
    def reset(self):
        """Reset all metrics."""
        self.executions.clear()
        self.current_graph = None
        self.current_adaptation = None
        self.sq_start_times.clear()