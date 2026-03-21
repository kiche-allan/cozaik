# Copyright 2025 TTPython Extensions - Runtime Adaptation Evaluation
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
TTPython Runtime Adaptation Evaluation Framework
================================================

Evaluates runtime adaptation quality across deployment strategies using
actual TTPython language components.

STRATEGIES TESTED:
    QPF     - Constraint-aware + optimized placement alternatives
    Static  - Constraint-aware + first compatible device
    Random  - No constraints + random selection
    Trivial - No constraints + all on one device

EXPERIMENTS:
    1. Constraint Preservation (CV)
    2. Adaptation Quality (AQ) - QPF only
    3. Adaptation Latency (ALAT)
    4. Multi-Failure Resilience
    5. QPF Search Efficiency

Usage:
    python runtime_adaptation_evaluation.py \\
        --app evals/workloads/eval_city.py \\
        --deployment evals/deployments/cluster_c2_heterogeneous.yaml \\
        --trials 10 --experiment all \\
        --output evals/results/adaptation_eval.json
"""

import argparse
import sys
import os
import json
import time
import random
import statistics
import yaml
import hashlib
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple, Any, Mapping
from enum import Enum
from copy import deepcopy
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# =============================================================================
# LAZY IMPORTS
# =============================================================================

def get_ttpython_imports():
    """Lazily import TTPython components."""
    from ticktalkpython import DebugLogger
    from ticktalkpython import Graph
    from ticktalkpython.Compiler import TTCompile
    from ticktalkpython.Query import TTEnsembleInfo, TTQuery, QueryOp
    from ticktalkpython.DeviceProfile import initialize_profiles, get_profile_manager
    from ticktalkpython.NetworkTopology import initialize_topology, get_network_topology
    from ticktalkpython.ObjectiveCalculator import ObjectiveCalculator
    from ticktalkpython.UnifiedGraph import UnifiedPlacementGraph
    from ticktalkpython.RuntimeAdapter import RuntimeAdapter, DeploymentStrategy
    from ticktalkpython.SmartMapper import SmartMapper
    from ticktalkpython import Mapper

    return {
        'DebugLogger': DebugLogger,
        'Graph': Graph,
        'TTCompile': TTCompile,
        'TTEnsembleInfo': TTEnsembleInfo,
        'TTQuery': TTQuery,
        'QueryOp': QueryOp,
        'initialize_profiles': initialize_profiles,
        'get_profile_manager': get_profile_manager,
        'initialize_topology': initialize_topology,
        'get_network_topology': get_network_topology,
        'ObjectiveCalculator': ObjectiveCalculator,
        'UnifiedPlacementGraph': UnifiedPlacementGraph,
        'RuntimeAdapter': RuntimeAdapter,
        'DeploymentStrategy': DeploymentStrategy,
        'SmartMapper': SmartMapper,
        'Mapper': Mapper,
    }


# =============================================================================
# CONSTANTS AND ENUMS
# =============================================================================

class EvalStrategy(Enum):
    """Deployment/adaptation strategies to evaluate."""
    QPF = "qpf"
    STATIC = "static"
    RANDOM = "random"
    TRIVIAL = "trivial"

# ALAT compliance threshold (ms)
ALAT_THRESHOLD_MS = 200.0


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class TaskInfo:
    """Information about a task extracted from compiled graph."""
    sq_name: str
    constraints: List[Any] = field(default_factory=list)
    criticality: str = "normal"


@dataclass
class TrialResult:
    """Results from a single evaluation trial."""
    trial_id: int
    strategy: str
    seed: int

    # Pre-failure state
    pre_failure_makespan_ms: float = 0.0
    num_tasks: int = 0
    num_devices: int = 0

    # Failure scenario
    failed_device: str = ""
    tasks_on_failed_device: int = 0

    # Post-failure metrics
    post_failure_makespan_ms: float = 0.0
    degradation_ratio: float = 0.0
    adaptation_quality: float = 0.0
    adaptation_latency_ms: float = 0.0
    constraint_violations: int = 0

    # Task outcomes
    tasks_remapped: int = 0
    tasks_degraded: int = 0
    critical_total: int = 0
    critical_surviving: int = 0
    critical_availability_score: float = 0.0
    valid_critical_availability_score: float = 0.0

    # Validation
    behavior_validated: bool = False
    validation_details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MultiFailureTrajectory:
    """Trajectory of metrics as failures accumulate."""
    trial_id: int
    seed: int
    strategy: str
    snapshots: List[Dict[str, Any]] = field(default_factory=list)
    total_failures: int = 0
    collapse_point: Optional[int] = None


@dataclass
class ExperimentResult:
    """Aggregated results from an experiment."""
    experiment_name: str
    description: str
    timestamp: str
    app_path: str = ""
    deployment_path: str = ""
    num_trials: int = 0
    base_seed: int = 0
    results_by_strategy: Dict[str, List[Dict]] = field(default_factory=dict)
    summary: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass
class SearchEfficiencyResult:
    """Results from search efficiency analysis."""
    trial_id: int
    seed: int
    objective: str
    num_trials: int
    initial_score: float = 0.0
    final_score: float = 0.0
    quality_gain_pct: float = 0.0
    total_improvements: int = 0
    first_improvement_trial: Optional[int] = None
    last_improvement_trial: Optional[int] = None
    trials_to_90_pct: Optional[int] = None
    trials_to_95_pct: Optional[int] = None
    trials_to_99_pct: Optional[int] = None
    improvement_rate: float = 0.0
    efficiency_ratio: float = 0.0
    convergence_curve: List[float] = field(default_factory=list)


# =============================================================================
# PLACEMENT GENERATOR
# =============================================================================

class PlacementGenerator:
    """Generates task placements using different strategies."""

    def __init__(self, graph, ensemble_dict: Dict, deployment_path: str,
                 ttp_imports: Dict, objective: str = 'makespan'):
        self.graph = graph
        self.ensemble_dict = ensemble_dict
        self.deployment_path = deployment_path
        self.ttp = ttp_imports
        self.objective = objective
        self.tasks = self._extract_task_info()

    def _extract_task_info(self) -> Dict[str, TaskInfo]:
        tasks = {}
        for sq in self.graph.sqs:
            tasks[sq.sq_name] = TaskInfo(
                sq_name=sq.sq_name,
                constraints=getattr(sq, 'constraints', None) or [],
                criticality=getattr(sq, 'criticality', 'normal')
            )
        return tasks

    def generate_qpf_placement(self, trials: int = 1000) -> Tuple[Dict[str, str], Dict[str, List[Dict]]]:
        app_data = {'eval_app': {'graph': self.graph, 'priority': 0}}
        unified_graph = self.ttp['UnifiedPlacementGraph'](app_data, self.ensemble_dict)
        smart_mapper = self.ttp['SmartMapper'](unified_graph, self.deployment_path)
        mapping = smart_mapper.optimize(objective=self.objective, trials=trials)
        placement_alternatives = smart_mapper.get_placement_alternatives()
        return mapping, placement_alternatives

    def generate_static_placement(self) -> Dict[str, str]:
        mapping = self.ttp['Mapper'].static_mapping(
            self.graph, list(self.ensemble_dict.values())
        )
        return mapping

    def generate_random_placement(self) -> Dict[str, str]:
        devices = list(self.ensemble_dict.keys())
        mapping = {}
        for sq in self.graph.sqs:
            mapping[sq.sq_name] = random.choice(devices)
        return mapping

    def generate_trivial_placement(self) -> Dict[str, str]:
        devices = list(self.ensemble_dict.keys())
        target_device = devices[0]
        mapping = {}
        for sq in self.graph.sqs:
            mapping[sq.sq_name] = target_device
        return mapping

    def generate_placement(self, strategy: EvalStrategy,
                           qpf_trials: int = 1000) -> Tuple[Dict[str, str], Optional[Dict]]:
        if strategy == EvalStrategy.QPF:
            return self.generate_qpf_placement(qpf_trials)
        elif strategy == EvalStrategy.STATIC:
            return self.generate_static_placement(), None
        elif strategy == EvalStrategy.RANDOM:
            return self.generate_random_placement(), None
        elif strategy == EvalStrategy.TRIVIAL:
            return self.generate_trivial_placement(), None
        else:
            raise ValueError(f"Unknown strategy: {strategy}")


# =============================================================================
# METRICS CALCULATOR
# =============================================================================

class MetricsCalculator:
    """Calculates evaluation metrics using TTPython ObjectiveCalculator."""

    def __init__(self, graph, ensemble_dict: Dict, ttp_imports: Dict):
        self.graph = graph
        self.ensemble_dict = ensemble_dict
        self.ttp = ttp_imports
        self.calculator = ttp_imports['ObjectiveCalculator']()
        self.task_criticality = {}
        for sq in graph.sqs:
            self.task_criticality[sq.sq_name] = getattr(sq, 'criticality', 'normal')

    def compute_makespan(self, mapping: Mapping[str, Optional[str]]) -> float:
        active_mapping = {sq: dev for sq, dev in mapping.items() if dev is not None}
        if not active_mapping:
            return float('inf')
        app_data = {'eval_app': {'graph': self.graph, 'priority': 0}}
        active_devices = set(active_mapping.values())
        filtered_ensembles = {d: e for d, e in self.ensemble_dict.items()
                              if d in active_devices}
        if not filtered_ensembles:
            return float('inf')
        try:
            unified_graph = self.ttp['UnifiedPlacementGraph'](app_data, filtered_ensembles)
            makespan_sec = self.calculator.calculate_makespan(active_mapping, unified_graph)
            return makespan_sec * 1000
        except Exception as e:
            print(f"Warning: Makespan calculation failed: {e}")
            return float('inf')

    def compute_optimal_on_survivors(self, surviving_devices: List[str],
                                     qpf_trials: int = 500) -> float:
        survivor_ensembles = {d: e for d, e in self.ensemble_dict.items()
                              if d in surviving_devices}
        if not survivor_ensembles:
            return float('inf')
        try:
            app_data = {'eval_app': {'graph': self.graph, 'priority': 0}}
            unified_graph = self.ttp['UnifiedPlacementGraph'](app_data, survivor_ensembles)
            smart_mapper = self.ttp['SmartMapper'](unified_graph)
            optimal_mapping = smart_mapper.optimize(objective='makespan', trials=qpf_trials)
            if not optimal_mapping:
                return float('inf')
            return self.compute_makespan(optimal_mapping)
        except Exception as e:
            print(f"Warning: Optimal computation failed: {e}")
            return float('inf')

    def compute_critical_availability(self, mapping: Mapping[str, Optional[str]]) -> Tuple[int, int, float]:
        critical_total = sum(1 for crit in self.task_criticality.values()
                             if crit in ('essential', 'important'))
        critical_surviving = sum(1 for sq, crit in self.task_criticality.items()
                                 if crit in ('essential', 'important')
                                 and sq in mapping and mapping[sq] is not None)
        cas = critical_surviving / critical_total if critical_total > 0 else 1.0
        return critical_total, critical_surviving, cas

    def count_constraint_violations(self, mapping: Mapping[str, Optional[str]]) -> int:
        violations = 0
        for sq in self.graph.sqs:
            device = mapping.get(sq.sq_name)
            if device is None:
                continue
            constraints = getattr(sq, 'constraints', None) or []
            if not constraints:
                continue
            if device not in self.ensemble_dict:
                violations += 1
                continue
            try:
                query = self.ttp['TTQuery'](constraints, self.ttp['QueryOp'].AND)
                if not query.test(self.ensemble_dict[device]):
                    violations += 1
            except Exception as e:
                print(f"Warning: Constraint check failed for {sq.sq_name}: {e}")
        return violations


# =============================================================================
# ADAPTATION EXECUTOR
# =============================================================================

class AdaptationExecutor:
    """Executes adaptation using actual RuntimeAdapter."""

    def __init__(self, graph, ensemble_dict: Dict, ttp_imports: Dict):
        self.graph = graph
        self.ensemble_dict = ensemble_dict
        self.ttp = ttp_imports
        self.characterization_data = {}
        for sq in graph.sqs:
            char_data = {'criticality': getattr(sq, 'criticality', 'normal')}
            if hasattr(sq, 'embedded_characterization'):
                char_data.update(sq.embedded_characterization)
            self.characterization_data[sq.sq_name] = char_data

    def create_adapter(self, strategy: EvalStrategy,
                       initial_mapping: Mapping[str, Optional[str]],
                       placement_alternatives: Optional[Dict] = None,
                       max_tasks_per_device: int = 10) -> Any:
        adapter = self.ttp['RuntimeAdapter'](
            characterization_data=self.characterization_data,
            initial_topology=self.ensemble_dict,
            initial_mapping=initial_mapping,
            deployment_strategy=strategy.value,
            placement_alternatives=placement_alternatives or {},
            optimization_objective='makespan',
            graph=self.graph
        )
        adapter.max_tasks_per_device = max_tasks_per_device
        return adapter

    def execute_adaptation(self, adapter, failed_device: str,
                           current_mapping: Mapping[str, Optional[str]]) -> Tuple[Optional[Dict[str, Optional[str]]], float]:
        available_topology = {d: e for d, e in self.ensemble_dict.items()
                              if d != failed_device}
        device_states = {d: 'ACTIVE' for d in self.ensemble_dict.keys()}
        device_states[failed_device] = 'FAILED'
        start_time = time.perf_counter_ns()
        new_mapping = adapter.on_device_failure(
            failed_device=failed_device,
            current_mapping=current_mapping,
            available_topology=available_topology,
            device_states=device_states
        )
        end_time = time.perf_counter_ns()
        latency_ms = (end_time - start_time) / 1_000_000
        return new_mapping, latency_ms


# =============================================================================
# MAIN EVALUATOR
# =============================================================================

class RuntimeAdaptationEvaluator:
    """Main evaluation harness for runtime adaptation experiments."""

    def __init__(self, app_path: str, deployment_path: str,
                 device_types_path: str = 'device_types.yaml',
                 network_types_path: str = 'network_types.yaml',
                 base_seed: int = 42,
                 objective: str = 'makespan'):
        self.app_path = app_path
        self.deployment_path = deployment_path
        self.device_types_path = device_types_path
        self.network_types_path = network_types_path
        self.base_seed = base_seed
        self.objective = objective

        print("Loading TTPython components...")
        self.ttp = get_ttpython_imports()

        print("Initializing device profiles and network topology...")
        self.ttp['initialize_profiles'](device_types_path, deployment_path)
        self.ttp['initialize_topology'](network_types_path, deployment_path)

        print(f"Compiling application: {app_path}")
        self.graph = self.ttp['TTCompile'](app_path)

        with open(deployment_path, 'r') as f:
            self.deployment = yaml.safe_load(f)

        self.ensemble_dict = self._build_ensemble_dict()
        self.device_ids = list(self.ensemble_dict.keys())

        self.placement_gen = PlacementGenerator(
            self.graph, self.ensemble_dict, deployment_path, self.ttp, self.objective
        )
        self.metrics_calc = MetricsCalculator(
            self.graph, self.ensemble_dict, self.ttp
        )
        self.adaptation_exec = AdaptationExecutor(
            self.graph, self.ensemble_dict, self.ttp
        )

        print(f"Loaded {len(self.device_ids)} devices: {self.device_ids}")
        print(f"Graph has {len(self.graph.sqs)} tasks")

        crit_dist = {}
        for sq in self.graph.sqs:
            crit = getattr(sq, 'criticality', 'normal')
            crit_dist[crit] = crit_dist.get(crit, 0) + 1
        print(f"Criticality distribution: {crit_dist}")

    def _build_ensemble_dict(self) -> Dict:
        ensemble_dict = {}
        for i, device in enumerate(self.deployment.get('devices', [])):
            ens_info = self.ttp['TTEnsembleInfo'](
                name=device['id'],
                address=f"127.0.0.1:{5000 + i}",
                components=device.get('components', {})
            )
            ensemble_dict[device['id']] = ens_info
        return ensemble_dict

    def _select_failed_device(self, mapping: Dict[str, str],
                              mode: str = 'random') -> str:
        if mode == 'most_loaded':
            device_load = {}
            for sq, dev in mapping.items():
                device_load[dev] = device_load.get(dev, 0) + 1
            return max(device_load.keys(), key=lambda d: device_load.get(d, 0))
        elif mode == 'has_tasks':
            devices_with_tasks = list(set(mapping.values()))
            return random.choice(devices_with_tasks)
        else:
            return random.choice(self.device_ids)

    def run_single_trial(self, strategy: EvalStrategy, trial_id: int,
                         seed: int, failure_mode: str = 'has_tasks',
                         capacity_limit: int = 10,
                         compute_aq: bool = False) -> TrialResult:
        random.seed(seed)
        result = TrialResult(
            trial_id=trial_id, strategy=strategy.value, seed=seed,
            num_tasks=len(self.graph.sqs), num_devices=len(self.device_ids)
        )
        mapping, placement_alternatives = self.placement_gen.generate_placement(strategy)
        if not mapping:
            print(f"  Warning: Empty mapping for {strategy.value}")
            return result

        result.pre_failure_makespan_ms = self.metrics_calc.compute_makespan(mapping)
        failed_device = self._select_failed_device(mapping, failure_mode)
        result.failed_device = failed_device
        result.tasks_on_failed_device = sum(1 for dev in mapping.values() if dev == failed_device)

        adapter = self.adaptation_exec.create_adapter(
            strategy, mapping, placement_alternatives, capacity_limit
        )
        new_mapping, latency_ms = self.adaptation_exec.execute_adaptation(
            adapter, failed_device, mapping
        )
        if new_mapping is None:
            print(f"  Warning: Adaptation failed for {strategy.value}")
            return result

        result.post_failure_makespan_ms = self.metrics_calc.compute_makespan(new_mapping)
        result.adaptation_latency_ms = latency_ms

        if result.pre_failure_makespan_ms > 0:
            result.degradation_ratio = result.post_failure_makespan_ms / result.pre_failure_makespan_ms

        if compute_aq and strategy == EvalStrategy.QPF:
            surviving = [d for d in self.device_ids if d != failed_device]
            optimal = self.metrics_calc.compute_optimal_on_survivors(surviving)
            if optimal > 0 and optimal != float('inf'):
                result.adaptation_quality = result.post_failure_makespan_ms / optimal

        crit_total, crit_surviving, cas = self.metrics_calc.compute_critical_availability(new_mapping)
        result.critical_total = crit_total
        result.critical_surviving = crit_surviving
        result.critical_availability_score = cas

        result.constraint_violations = self.metrics_calc.count_constraint_violations(new_mapping)
        result.tasks_remapped = sum(1 for sq, dev in new_mapping.items()
                                    if dev is not None and mapping.get(sq) == failed_device)
        result.tasks_degraded = sum(1 for dev in new_mapping.values() if dev is None)
        return result

    # =========================================================================
    # EXPERIMENT 1: Constraint Preservation
    # =========================================================================

    def experiment_constraint_preservation(self, num_trials: int = 10) -> ExperimentResult:
        """
        Verify that constraint-aware strategies (QPF, Static) maintain zero
        violations after adaptation, while baselines (Random, Trivial) do not.
        """
        print("\n" + "=" * 70)
        print("EXPERIMENT 1: Constraint Preservation")
        print("=" * 70)

        results_by_strategy = {s.value: [] for s in EvalStrategy}
        for strategy in EvalStrategy:
            print(f"\nTesting {strategy.value}...")
            for trial in range(num_trials):
                seed = self.base_seed + trial
                random.seed(seed)
                result = self.run_single_trial(strategy, trial + 1, seed)
                results_by_strategy[strategy.value].append(asdict(result))
                print(f"  Trial {trial + 1}: violations={result.constraint_violations}")

        summary = {}
        for strategy_name, results in results_by_strategy.items():
            violations = [r['constraint_violations'] for r in results]
            summary[strategy_name] = {
                'total_violations': sum(violations),
                'avg_violations': statistics.mean(violations) if violations else 0,
                'max_violations': max(violations) if violations else 0,
                'zero_violation_trials': sum(1 for v in violations if v == 0)
            }

        return ExperimentResult(
            experiment_name="constraint_preservation",
            description="Verifies constraint compliance per strategy after adaptation",
            timestamp=datetime.now().isoformat(),
            app_path=self.app_path, deployment_path=self.deployment_path,
            num_trials=num_trials * len(EvalStrategy),
            base_seed=self.base_seed,
            results_by_strategy=results_by_strategy, summary=summary
        )

    # =========================================================================
    # EXPERIMENT 2: Adaptation Quality (AQ) — QPF only
    # =========================================================================

    def experiment_adaptation_quality(self, num_trials: int = 10) -> ExperimentResult:
        """
        Measure how close QPF's runtime adaptation is to a full
        re-optimization on surviving devices. AQ=1.0 is optimal.
        """
        print("\n" + "=" * 70)
        print("EXPERIMENT 2: Adaptation Quality (AQ) - QPF only")
        print("=" * 70)

        results_by_strategy = {'qpf': []}
        for trial in range(num_trials):
            seed = self.base_seed + trial
            result = self.run_single_trial(
                EvalStrategy.QPF, trial + 1, seed, compute_aq=True
            )
            results_by_strategy['qpf'].append(asdict(result))
            print(f"  Trial {trial + 1}: AQ={result.adaptation_quality:.3f}, "
                  f"DR={result.degradation_ratio:.3f}")

        aqs = [r['adaptation_quality'] for r in results_by_strategy['qpf']
               if r['adaptation_quality'] > 0 and r['adaptation_quality'] < float('inf')]
        summary = {
            'qpf': {
                'mean_aq': statistics.mean(aqs) if aqs else float('inf'),
                'std_aq': statistics.stdev(aqs) if len(aqs) > 1 else 0,
                'min_aq': min(aqs) if aqs else float('inf'),
                'max_aq': max(aqs) if aqs else float('inf'),
            }
        }

        return ExperimentResult(
            experiment_name="adaptation_quality",
            description="QPF adaptation quality vs optimal recomputation",
            timestamp=datetime.now().isoformat(),
            app_path=self.app_path, deployment_path=self.deployment_path,
            num_trials=num_trials, base_seed=self.base_seed,
            results_by_strategy=results_by_strategy, summary=summary
        )

    # =========================================================================
    # EXPERIMENT 3: Adaptation Latency (ALAT)
    # =========================================================================

    def experiment_adaptation_overhead(self, num_trials: int = 20) -> ExperimentResult:
        """
        Measure end-to-end computation time for on_device_failure().
        """
        print("\n" + "=" * 70)
        print("EXPERIMENT 3: Adaptation Latency (ALAT)")
        print("=" * 70)

        results_by_strategy = {s.value: [] for s in EvalStrategy}
        for strategy in EvalStrategy:
            print(f"\nTesting {strategy.value}...")
            for trial in range(num_trials):
                seed = self.base_seed + trial
                result = self.run_single_trial(strategy, trial + 1, seed)
                results_by_strategy[strategy.value].append(asdict(result))

            alats = [r['adaptation_latency_ms'] for r in results_by_strategy[strategy.value]]
            print(f"  ALAT: mean={statistics.mean(alats):.2f}ms, "
                  f"p95={sorted(alats)[int(len(alats)*0.95)]:.2f}ms")

        summary = {}
        for strategy_name, results in results_by_strategy.items():
            alats = [r['adaptation_latency_ms'] for r in results if r['adaptation_latency_ms'] > 0]
            if alats:
                sorted_alats = sorted(alats)
                summary[strategy_name] = {
                    'mean_alat_ms': statistics.mean(alats),
                    'std_alat_ms': statistics.stdev(alats) if len(alats) > 1 else 0,
                    'p50_alat_ms': sorted_alats[len(alats) // 2],
                    'p95_alat_ms': sorted_alats[int(len(alats) * 0.95)],
                    'p99_alat_ms': sorted_alats[int(len(alats) * 0.99)],
                    'max_alat_ms': max(alats),
                    'compliance_pct': sum(1 for a in alats if a < ALAT_THRESHOLD_MS) / len(alats) * 100
                }

        return ExperimentResult(
            experiment_name="adaptation_overhead",
            description="End-to-end adaptation latency measurement",
            timestamp=datetime.now().isoformat(),
            app_path=self.app_path, deployment_path=self.deployment_path,
            num_trials=num_trials * len(EvalStrategy),
            base_seed=self.base_seed,
            results_by_strategy=results_by_strategy, summary=summary
        )

    # =========================================================================
    # EXPERIMENT 4: Multi-Failure Resilience
    # =========================================================================

    def experiment_sequential_multi_failure(self, num_trials: int = 5) -> ExperimentResult:
        """
        Sequentially fail devices and track CAS trajectory until collapse.
        """
        print("\n" + "=" * 70)
        print("EXPERIMENT 4: Multi-Failure Resilience")
        print("=" * 70)

        results_by_strategy = {'qpf': []}
        max_failures = len(self.device_ids) - 1

        for trial in range(num_trials):
            seed = self.base_seed + trial
            random.seed(seed)
            print(f"\nTrial {trial + 1} (up to {max_failures} failures):")

            mapping, alts = self.placement_gen.generate_qpf_placement(trials=500)
            current_mapping: Dict[str, Optional[str]] = dict(mapping)

            trajectory = MultiFailureTrajectory(
                trial_id=trial + 1, seed=seed, strategy='qpf'
            )
            failed_so_far = []

            for failure_num in range(1, max_failures + 1):
                remaining = [d for d in self.device_ids if d not in failed_so_far]
                if not remaining:
                    break
                next_fail = random.choice(remaining)
                failed_so_far.append(next_fail)

                adapter = self.adaptation_exec.create_adapter(
                    EvalStrategy.QPF, current_mapping, alts
                )
                new_mapping, latency = self.adaptation_exec.execute_adaptation(
                    adapter, next_fail, current_mapping
                )
                if new_mapping is None:
                    break
                current_mapping = new_mapping

                crit_total, crit_surviving, cas = self.metrics_calc.compute_critical_availability(current_mapping)
                pre_makespan = self.metrics_calc.compute_makespan(mapping)
                post_makespan = self.metrics_calc.compute_makespan(current_mapping)
                dr = post_makespan / pre_makespan if pre_makespan > 0 else float('inf')
                tasks_active = sum(1 for dev in current_mapping.values() if dev is not None)

                snapshot = {
                    'failure_num': failure_num, 'failed_device': next_fail,
                    'cas': cas, 'dr': dr, 'tasks_active': tasks_active,
                    'latency_ms': latency
                }
                trajectory.snapshots.append(snapshot)
                print(f"  Failure {failure_num} ({next_fail}): CAS={cas:.3f}, DR={dr:.3f}, active={tasks_active}")

                if cas < 0.5 and trajectory.collapse_point is None:
                    trajectory.collapse_point = failure_num

            trajectory.total_failures = len(failed_so_far)
            results_by_strategy['qpf'].append(asdict(trajectory))

        collapse_points = [t['collapse_point'] for t in results_by_strategy['qpf']
                           if t['collapse_point'] is not None]
        summary = {
            'qpf': {
                'num_trials': num_trials,
                'max_failures_tested': max_failures,
                'avg_collapse_point': statistics.mean(collapse_points) if collapse_points else None,
                'min_collapse_point': min(collapse_points) if collapse_points else None,
                'trials_without_collapse': sum(1 for t in results_by_strategy['qpf']
                                               if t['collapse_point'] is None)
            }
        }

        return ExperimentResult(
            experiment_name="sequential_multi_failure",
            description="CAS trajectory under sequential device failures",
            timestamp=datetime.now().isoformat(),
            app_path=self.app_path, deployment_path=self.deployment_path,
            num_trials=num_trials, base_seed=self.base_seed,
            results_by_strategy=results_by_strategy, summary=summary
        )

    # =========================================================================
    # EXPERIMENT 5: QPF Search Efficiency
    # =========================================================================

    def experiment_search_efficiency(self, num_trials: int = 10,
                                     qpf_trials_list: Optional[List[int]] = None) -> ExperimentResult:
        """
        Analyze QPF convergence speed and quality gain across different
        search iteration budgets.
        """
        print("\n" + "=" * 70)
        print("EXPERIMENT 5: QPF Search Efficiency")
        print("=" * 70)

        if qpf_trials_list is None:
            qpf_trials_list = [100, 500, 1000, 2000]

        results_by_trials = {}
        for qpf_trials in qpf_trials_list:
            print(f"\nTesting with {qpf_trials} QPF trials...")
            results_by_trials[str(qpf_trials)] = []
            for trial in range(num_trials):
                seed = self.base_seed + trial
                random.seed(seed)
                result = self._analyze_search_efficiency(qpf_trials, seed, trial + 1)
                results_by_trials[str(qpf_trials)].append(asdict(result))
                print(f"  Trial {trial + 1}: quality_gain={result.quality_gain_pct:.1f}%, "
                      f"improvements={result.total_improvements}, "
                      f"last_improve={result.last_improvement_trial}")

        summary = {}
        for trials_str, results in results_by_trials.items():
            quality_gains = [r['quality_gain_pct'] for r in results]
            improvements = [r['total_improvements'] for r in results]
            last_improves = [r['last_improvement_trial'] for r in results if r['last_improvement_trial']]
            trials_90 = [r['trials_to_90_pct'] for r in results if r['trials_to_90_pct']]

            summary[f'qpf_{trials_str}_trials'] = {
                'mean_quality_gain_pct': statistics.mean(quality_gains) if quality_gains else 0,
                'mean_improvements': statistics.mean(improvements) if improvements else 0,
                'mean_last_improvement': statistics.mean(last_improves) if last_improves else None,
                'mean_trials_to_90_pct': statistics.mean(trials_90) if trials_90 else None,
                'improvement_rate': statistics.mean(improvements) / int(trials_str) * 100 if improvements else 0
            }

        return ExperimentResult(
            experiment_name="search_efficiency",
            description="QPF search efficiency and convergence analysis",
            timestamp=datetime.now().isoformat(),
            app_path=self.app_path, deployment_path=self.deployment_path,
            num_trials=num_trials * len(qpf_trials_list),
            base_seed=self.base_seed,
            results_by_strategy=results_by_trials, summary=summary
        )

    def _analyze_search_efficiency(self, qpf_trials: int, seed: int,
                                    trial_id: int) -> SearchEfficiencyResult:
        random.seed(seed)
        app_data = {'eval_app': {'graph': self.graph, 'priority': 0}}
        unified_graph = self.ttp['UnifiedPlacementGraph'](app_data, self.ensemble_dict)
        smart_mapper = self.ttp['SmartMapper'](unified_graph, self.deployment_path)
        mapping = smart_mapper.optimize(
            objective=self.objective, trials=qpf_trials, track_convergence=True
        )

        convergence_history = smart_mapper.convergence_history
        convergence_summary = smart_mapper.convergence_summary

        result = SearchEfficiencyResult(
            trial_id=trial_id, seed=seed,
            objective=self.objective, num_trials=qpf_trials
        )

        if not convergence_history or not convergence_summary:
            return result

        result.initial_score = convergence_summary.get('initial_score', 0)
        result.final_score = convergence_summary.get('final_score', 0)
        result.total_improvements = convergence_summary.get('total_improvements', 0)
        result.first_improvement_trial = convergence_summary.get('first_improvement')
        result.last_improvement_trial = convergence_summary.get('last_improvement')

        if result.initial_score and result.initial_score != float('inf'):
            result.quality_gain_pct = ((result.initial_score - result.final_score)
                                       / result.initial_score * 100)
        result.improvement_rate = result.total_improvements / qpf_trials * 100
        if qpf_trials > 0:
            result.efficiency_ratio = result.quality_gain_pct / qpf_trials

        if result.initial_score and result.final_score:
            total_improvement = result.initial_score - result.final_score
            threshold_90 = result.initial_score - (total_improvement * 0.90)
            threshold_95 = result.initial_score - (total_improvement * 0.95)
            threshold_99 = result.initial_score - (total_improvement * 0.99)

            for entry in convergence_history:
                best = entry['best_score']
                trial = entry['trial']
                if result.trials_to_90_pct is None and best <= threshold_90:
                    result.trials_to_90_pct = trial
                if result.trials_to_95_pct is None and best <= threshold_95:
                    result.trials_to_95_pct = trial
                if result.trials_to_99_pct is None and best <= threshold_99:
                    result.trials_to_99_pct = trial

        sample_rate = max(1, qpf_trials // 100)
        result.convergence_curve = [
            convergence_history[i]['best_score']
            for i in range(0, len(convergence_history), sample_rate)
        ]
        return result

    # =========================================================================
    # RUN ALL
    # =========================================================================

    def run_all_experiments(self, trials: int = 10) -> Dict[str, ExperimentResult]:
        results = {}
        results['constraint_preservation'] = self.experiment_constraint_preservation(trials)
        results['adaptation_quality'] = self.experiment_adaptation_quality(trials)
        results['adaptation_overhead'] = self.experiment_adaptation_overhead(trials * 2)
        results['sequential_multi_failure'] = self.experiment_sequential_multi_failure(max(trials // 2, 3))
        results['search_efficiency'] = self.experiment_search_efficiency(trials)
        return results


# =============================================================================
# OUTPUT
# =============================================================================

def write_results(results: Dict[str, ExperimentResult], output_path: str):
    """Write results to JSON file."""
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)

    serializable = {}
    for name, exp in results.items():
        serializable[name] = asdict(exp)

    with open(output_path, 'w') as f:
        json.dump(serializable, f, indent=2, default=str)
    print(f"\nResults written to: {output_path}")

    txt_path = output_path.replace('.json', '.txt')
    with open(txt_path, 'w') as f:
        for name, exp in results.items():
            f.write(f"EXPERIMENT: {name}\n")
            f.write(f"  Description: {exp.description}\n")
            f.write(f"  Trials: {exp.num_trials}\n")
            f.write(f"  Summary:\n")
            for key, val in exp.summary.items():
                f.write(f"    {key}: {val}\n")
            f.write("\n")
    print(f"Summary written to: {txt_path}")


def print_executive_summary(results: Dict[str, ExperimentResult]):
    """Print executive summary of all experiment results."""
    print("\n" + "=" * 70)
    print("EXECUTIVE SUMMARY")
    print("=" * 70)

    if 'constraint_preservation' in results:
        exp = results['constraint_preservation']
        print(f"\n1. CONSTRAINT PRESERVATION:")
        for strat, stats in exp.summary.items():
            total = stats.get('total_violations', 0)
            zero = stats.get('zero_violation_trials', 0)
            print(f"   {strat}: {total} total violations, {zero} zero-violation trials")

    if 'adaptation_quality' in results:
        exp = results['adaptation_quality']
        qpf_stats = exp.summary.get('qpf', {})
        aq = qpf_stats.get('mean_aq', 'N/A')
        std = qpf_stats.get('std_aq', 0)
        print(f"\n2. ADAPTATION QUALITY (QPF):")
        if isinstance(aq, float):
            print(f"   AQ = {aq:.3f} +/- {std:.3f} (1.0 = optimal)")
        else:
            print(f"   AQ = {aq}")

    if 'adaptation_overhead' in results:
        exp = results['adaptation_overhead']
        print(f"\n3. ADAPTATION LATENCY:")
        for strat, stats in exp.summary.items():
            mean = stats.get('mean_alat_ms', 0)
            p95 = stats.get('p95_alat_ms', 0)
            compliance = stats.get('compliance_pct', 0)
            print(f"   {strat}: mean={mean:.2f}ms, p95={p95:.2f}ms, <200ms={compliance:.0f}%")

    if 'sequential_multi_failure' in results:
        exp = results['sequential_multi_failure']
        qpf_stats = exp.summary.get('qpf', {})
        collapse = qpf_stats.get('avg_collapse_point', 'never')
        no_collapse = qpf_stats.get('trials_without_collapse', 0)
        print(f"\n4. MULTI-FAILURE RESILIENCE:")
        print(f"   QPF: avg collapse at {collapse} failures")
        print(f"   Trials without collapse: {no_collapse}")

    if 'search_efficiency' in results:
        exp = results['search_efficiency']
        print(f"\n5. SEARCH EFFICIENCY:")
        for config, stats in exp.summary.items():
            gain = stats.get('mean_quality_gain_pct', 0)
            rate = stats.get('improvement_rate', 0)
            last = stats.get('mean_last_improvement', 'N/A')
            print(f"   {config}: gain={gain:.1f}%, rate={rate:.2f}%, converged@{last}")

    print("\n" + "=" * 70)


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='TTPython Runtime Adaptation Evaluation',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--app', type=str, required=True,
                        help='Path to TTPython application (.py file)')
    parser.add_argument('--deployment', type=str, required=True,
                        help='Path to deployment YAML')
    parser.add_argument('--device-types', type=str, default='device_types.yaml',
                        help='Path to device_types.yaml')
    parser.add_argument('--network-types', type=str, default='network_types.yaml',
                        help='Path to network_types.yaml')
    parser.add_argument('--objective', type=str, default='makespan',
                        choices=['makespan', 'energy'],
                        help='Optimization objective (default: makespan)')
    parser.add_argument('--experiment', type=str, default='all',
                        choices=['all', 'constraints', 'quality', 'overhead',
                                 'multifailure', 'search_efficiency'],
                        help='Which experiment to run')
    parser.add_argument('--trials', type=int, default=10,
                        help='Number of trials per experiment')
    parser.add_argument('--seed', type=int, default=42,
                        help='Base random seed for reproducibility')
    parser.add_argument('--output', type=str,
                        default='./evals/results/adaptation_eval.json',
                        help='Output JSON file path')

    args = parser.parse_args()

    print("=" * 70)
    print("TTPython Runtime Adaptation Evaluation")
    print("=" * 70)
    print(f"Application:  {args.app}")
    print(f"Deployment:   {args.deployment}")
    print(f"Device Types: {args.device_types}")
    print(f"Experiment:   {args.experiment}")
    print(f"Trials:       {args.trials}")
    print(f"Seed:         {args.seed}")
    print(f"Output:       {args.output}")
    print()

    evaluator = RuntimeAdaptationEvaluator(
        app_path=args.app,
        deployment_path=args.deployment,
        device_types_path=args.device_types,
        network_types_path=args.network_types,
        base_seed=args.seed,
        objective=args.objective
    )

    results = {}
    if args.experiment == 'all':
        results = evaluator.run_all_experiments(args.trials)
    elif args.experiment == 'constraints':
        results['constraint_preservation'] = evaluator.experiment_constraint_preservation(args.trials)
    elif args.experiment == 'quality':
        results['adaptation_quality'] = evaluator.experiment_adaptation_quality(args.trials)
    elif args.experiment == 'overhead':
        results['adaptation_overhead'] = evaluator.experiment_adaptation_overhead(args.trials)
    elif args.experiment == 'multifailure':
        results['sequential_multi_failure'] = evaluator.experiment_sequential_multi_failure(args.trials)
    elif args.experiment == 'search_efficiency':
        results['search_efficiency'] = evaluator.experiment_search_efficiency(args.trials)

    write_results(results, args.output)
    print_executive_summary(results)
    print("\nEvaluation complete!")
    return 0


if __name__ == '__main__':
    sys.exit(main())
