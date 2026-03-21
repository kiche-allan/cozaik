#!/usr/bin/env python3
"""
TTPython QPF Evaluation Runner
===============================

Runs placement quality experiments using TTPython's own compilation,
placement, and analysis infrastructure. Every metric is computed by
TTPython's internal APIs — nothing is reimplemented or approximated.

API usage:
    - Compilation:       TTCompile() from ticktalkpython.Compiler
    - QPF placement:     SmartMapper.optimize() from ticktalkpython.SmartMapper
    - Random placement:  TTMapper.random_mapping() from ticktalkpython.Mapper
    - Static placement:  static_mapping() from ticktalkpython.Mapper
    - Greedy placement:  Implemented here using ObjectiveCalculator + constraint system
    - Makespan metric:   ObjectiveCalculator.calculate_makespan()
    - Energy metric:     ObjectiveCalculator.calculate_energy()
    - Contention:        SmartMapper.detect_single_app_contention()
    - DAG structure:     TTGraph.get_dag() via networkx

Results are stored as timestamped JSON files with full provenance:
    evals/results/<timestamp>_<experiment>/
        metadata.json           — what ran, when, cluster config, git hash
        <workload>_<strategy>.json  — per-trial data with full mappings
        summary.json            — aggregated cross-workload comparison

Usage:
    # Run all strategies on all workloads (default experiment)
    python evaluation.py \\
        --deployment evals/deployments/cluster_c2_heterogeneous.yaml \\
        --workloads evals/workloads/eval_etl.py evals/workloads/eval_stats.py \\
        --strategies qpf_makespan qpf_energy random static greedy \\
        --trials 30 --qpf-trials 1000

    # Quick single test
    python evaluation.py \\
        --deployment evals/deployments/cluster_c2_heterogeneous.yaml \\
        --workloads evals/workloads/eval_etl.py \\
        --strategies qpf_makespan random \\
        --trials 5

    # Use pre-compiled pickles
    python evaluation.py \\
        --deployment evals/deployments/cluster_c2_heterogeneous.yaml \\
        --pickles output/eval_etl.pickle output/eval_stats.pickle \\
        --strategies qpf_makespan random static greedy \\
        --trials 30
"""

import argparse
import sys
import os
import json
import time
import random
import logging
import subprocess
import hashlib
import statistics
import platform
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from collections import defaultdict
from copy import deepcopy

import yaml
import networkx as nx

# ============================================================================
# TTPython imports — these are the language's own APIs
# ============================================================================
from ticktalkpython import DebugLogger
from ticktalkpython import Graph
from ticktalkpython.Compiler import TTCompile
from ticktalkpython.DeviceProfile import (
    DeviceProfileManager,
    initialize_profiles,
    get_profile_manager,
)
from ticktalkpython.NetworkTopology import (
    NetworkTopology,
    initialize_topology,
    get_network_topology,
)
from ticktalkpython.ObjectiveCalculator import ObjectiveCalculator
from ticktalkpython.UnifiedGraph import UnifiedPlacementGraph
from ticktalkpython.SmartMapper import SmartMapper
from ticktalkpython.Mapper import TTMapper, static_mapping
from ticktalkpython.Query import TTEnsembleInfo

logger = logging.getLogger('TTPython.Evaluation')


# ============================================================================
# INFRASTRUCTURE SETUP
# ============================================================================

def setup_infrastructure(device_types_path: str,
                         network_types_path: str,
                         deployment_path: str) -> Tuple[DeviceProfileManager, NetworkTopology]:
    """
    Initialize TTPython's device profile and network topology subsystems.

    These are the same subsystems that runrtm.py initializes at startup.
    Calling them here ensures evaluation uses identical device speed estimates
    and network cost models as the real runtime.
    """
    initialize_profiles(device_types_path, deployment_path)
    profile_manager = get_profile_manager()

    initialize_topology(network_types_path, deployment_path)
    topology = get_network_topology()

    return profile_manager, topology


def build_ensemble_dict(deployment_path: str) -> Dict[str, TTEnsembleInfo]:
    """
    Build ensemble info dict from deployment YAML.

    This is the same construction that RuntimeManager performs when devices
    connect. Components are read from the YAML so that constraint-based
    placement (TTConstraint) works correctly.
    """
    with open(deployment_path, 'r') as f:
        deployment = yaml.safe_load(f)

    ensemble_dict = {}
    for device in deployment.get('devices', []):
        components = device.get('components', {})
        ens_info = TTEnsembleInfo(
            name=device['id'],
            address=f"127.0.0.1:5000",  # placeholder — not used in analytical mode
            components=components
        )
        ensemble_dict[device['id']] = ens_info

    return ensemble_dict


def build_unified_graph(graph: Graph.TTGraph,
                        ensemble_dict: Dict[str, TTEnsembleInfo],
                        app_name: str = 'eval_app') -> UnifiedPlacementGraph:
    """
    Build the UnifiedPlacementGraph that SmartMapper and ObjectiveCalculator
    operate on. This is the same graph structure used in the real pipeline.
    """
    app_data = {
        app_name: {
            'graph': graph,
            'priority': 0
        }
    }
    return UnifiedPlacementGraph(app_data, ensemble_dict)


# ============================================================================
# COMPILATION
# ============================================================================

def compile_workload(app_path: str, deployment_path: str) -> Graph.TTGraph:
    """
    Compile a TTPython workload using the language's own compiler.

    This is identical to running: python compile.py <app_path> --deployment <yaml>
    """
    logger.info(f"Compiling: {app_path}")
    graph = TTCompile(app_path, deployment_path=deployment_path)
    logger.info(f"Compiled: {len(graph.sqs)} SQs")
    return graph


def load_pickle(pickle_path: str) -> Graph.TTGraph:
    """Load a pre-compiled graph from pickle."""
    import pickle
    with open(pickle_path, 'rb') as f:
        graph = pickle.load(f)
    logger.info(f"Loaded pickle: {pickle_path} ({len(graph.sqs)} SQs)")
    return graph


# ============================================================================
# PLACEMENT STRATEGIES
# ============================================================================
# Each strategy calls TTPython's own placement APIs.
# The only addition is Greedy, which uses the same ObjectiveCalculator
# and constraint system but with a different selection policy.
# ============================================================================

def generate_placement(strategy: str,
                       graph: Graph.TTGraph,
                       unified_graph: UnifiedPlacementGraph,
                       ensemble_dict: Dict[str, TTEnsembleInfo],
                       deployment_path: str,
                       seed: int,
                       qpf_trials: int = 1000) -> Dict[str, Any]:
    """
    Generate a placement mapping using the specified strategy.

    Returns a dict containing:
        - 'mapping': {sq_name: device_name}
        - 'convergence': convergence data (QPF only)
        - 'objective_used': which objective function was optimized
    """
    random.seed(seed)

    result = {
        'mapping': {},
        'convergence': None,
        'convergence_summary': None,
        'objective_used': strategy,
    }

    if strategy == 'random':
        ensemble_list = list(ensemble_dict.values())
        result['mapping'] = TTMapper.random_mapping(graph, ensemble_list)

    elif strategy == 'static':
        ensemble_list = list(ensemble_dict.values())
        result['mapping'] = static_mapping(graph, ensemble_list)

    elif strategy == 'greedy':
        result['mapping'] = _greedy_mapping(graph, unified_graph, ensemble_dict)

    elif strategy.startswith('qpf_'):
        objective = strategy.replace('qpf_', '')  # 'makespan' or 'energy'
        smart_mapper = SmartMapper(unified_graph, deployment_path)
        result['mapping'] = smart_mapper.optimize(
            objective=objective,
            trials=qpf_trials,
            track_convergence=True
        )
        result['convergence'] = smart_mapper.convergence_history
        result['convergence_summary'] = smart_mapper.convergence_summary

    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    return result


def _greedy_mapping(graph: Graph.TTGraph,
                    unified_graph: UnifiedPlacementGraph,
                    ensemble_dict: Dict[str, TTEnsembleInfo]) -> Dict[str, str]:
    """
    Greedy baseline: for each SQ in topological order, assign it to the
    compatible device that gives the lowest execution time for THAT SQ.

    This is the strategy QPF is designed to beat — locally optimal per-task
    decisions that create globally suboptimal placements through device
    contention (piling parallel tasks onto the fastest device).

    Uses TTPython's own:
        - TTGraph.get_dag() for topological ordering
        - TTConstraint for compatibility checking
        - sq.execution_time_estimates for per-device execution times
          (same source as ObjectiveCalculator._get_execution_time)
    """
    from ticktalkpython import Query

    dag = graph.get_dag()
    topo_order = list(nx.topological_sort(dag))

    mapping = {}

    for sq_name in topo_order:
        sq = dag.nodes[sq_name]['sq']
        constraints = getattr(sq, 'constraints', []) or []

        # Find compatible devices (same constraint check as SmartMapper)
        candidates = []
        for ens_name, ens_info in ensemble_dict.items():
            if not constraints or Query.TTQuery(constraints, Query.QueryOp.AND).test(ens_info):
                candidates.append(ens_name)

        if not candidates:
            from ticktalkpython.Constants import RUNTIME_MANAGER_ENSEMBLE_NAME
            mapping[sq_name] = RUNTIME_MANAGER_ENSEMBLE_NAME
            continue

        # Greedy: pick device with lowest execution time for this SQ
        # Uses the same execution_time_estimates that ObjectiveCalculator uses
        best_device = candidates[0]
        best_time = float('inf')

        if hasattr(sq, 'execution_time_estimates') and sq.execution_time_estimates:
            for candidate in candidates:
                exec_time = sq.execution_time_estimates.get(
                    candidate,
                    sq.execution_time_estimates.get('default', 0.001)
                )
                if exec_time < best_time:
                    best_time = exec_time
                    best_device = candidate
        else:
            # No per-device estimates — fall back to fastest cpu_speed
            profile_manager = get_profile_manager()
            best_speed = -1
            for candidate in candidates:
                profile = profile_manager.profiles.get(candidate)
                if profile and profile.cpu_speed > best_speed:
                    best_speed = profile.cpu_speed
                    best_device = candidate

        mapping[sq_name] = best_device

    return mapping


# ============================================================================
# METRICS COLLECTION
# ============================================================================
# All metrics come from TTPython's ObjectiveCalculator and SmartMapper.
# ============================================================================

def evaluate_placement(mapping: Dict[str, str],
                       graph: Graph.TTGraph,
                       unified_graph: UnifiedPlacementGraph,
                       deployment_path: str,
                       profile_manager: DeviceProfileManager) -> Dict[str, Any]:
    """
    Evaluate a placement mapping using TTPython's own metric functions.

    Returns comprehensive metrics dictionary.
    """
    calculator = ObjectiveCalculator()

    # Makespan and energy — from TTPython's critical-path analysis
    makespan_sec = calculator.calculate_makespan(mapping, unified_graph)
    energy_joules = calculator.calculate_energy(mapping, unified_graph)

    # Device load distribution
    device_load = defaultdict(list)
    for sq_name, device in mapping.items():
        device_load[device].append(sq_name)

    device_distribution = {
        device: {
            'sq_count': len(sqs),
            'sq_names': sqs
        }
        for device, sqs in device_load.items()
    }

    # Contention analysis — using SmartMapper's own detection
    smart_mapper = SmartMapper(unified_graph, deployment_path)
    contention_scenarios = smart_mapper.detect_single_app_contention(
        graph, mapping, device_profile_manager=profile_manager
    )

    contention_info = {}
    for device, schedule in contention_scenarios.items():
        # schedule is {sq_id: {'mode': 'timesliced', 'total_duration': float}}
        contention_info[device] = {
            'contending_sqs': list(schedule.keys()),
            'num_contending': len(schedule),
            'total_contention_time': sum(
                s.get('total_duration', 0) for s in schedule.values()
            ),
        }

    # Effective parallelism:
    # (sum of all individual SQ execution times) / makespan
    # Higher = more parallelism exploited
    # Uses ObjectiveCalculator's _get_execution_time for consistency
    total_sequential_time = 0.0
    dag = graph.get_dag()
    for sq_name in mapping:
        if sq_name in dag.nodes:
            sq = dag.nodes[sq_name]['sq']
            device = mapping[sq_name]
            exec_time = calculator._get_execution_time(sq, device)
            total_sequential_time += exec_time

    effective_parallelism = (total_sequential_time / makespan_sec) if makespan_sec > 0 else 0.0

    return {
        'makespan_sec': makespan_sec,
        'makespan_ms': makespan_sec * 1000,
        'energy_joules': energy_joules,
        'device_distribution': device_distribution,
        'contention_devices': len(contention_scenarios),
        'contention_info': contention_info,
        'effective_parallelism': effective_parallelism,
        'total_sequential_time_sec': total_sequential_time,
        'num_sqs': len(mapping),
        'num_devices_used': len(device_load),
    }


# ============================================================================
# TRIAL RUNNER
# ============================================================================

def run_single_trial(trial_id: int,
                     strategy: str,
                     graph: Graph.TTGraph,
                     unified_graph: UnifiedPlacementGraph,
                     ensemble_dict: Dict[str, TTEnsembleInfo],
                     deployment_path: str,
                     profile_manager: DeviceProfileManager,
                     qpf_trials: int) -> Dict[str, Any]:
    """
    Run one evaluation trial: generate placement, evaluate metrics.

    Each trial gets a unique seed (trial_id) for reproducibility.
    Deterministic strategies (static, greedy) produce the same mapping
    regardless of seed — this is recorded honestly in the results.
    """
    seed = trial_id

    # Generate placement
    placement_result = generate_placement(
        strategy, graph, unified_graph, ensemble_dict,
        deployment_path, seed=seed, qpf_trials=qpf_trials
    )
    mapping = placement_result['mapping']

    if not mapping:
        logger.warning(f"Trial {trial_id}: empty mapping for {strategy}")
        return {'trial_id': trial_id, 'error': 'empty_mapping'}

    # Evaluate the placement
    metrics = evaluate_placement(
        mapping, graph, unified_graph, deployment_path, profile_manager
    )

    # Assemble trial record
    trial_record = {
        'trial_id': trial_id,
        'seed': seed,
        'strategy': strategy,
        'timestamp': datetime.now().isoformat(),
        'mapping': mapping,
        'metrics': metrics,
    }

    # Include convergence data for QPF (but not the full per-trial history — too large)
    if placement_result['convergence_summary']:
        trial_record['convergence_summary'] = placement_result['convergence_summary']

    return trial_record


def run_experiment_cell(workload_name: str,
                        strategy: str,
                        graph: Graph.TTGraph,
                        unified_graph: UnifiedPlacementGraph,
                        ensemble_dict: Dict[str, TTEnsembleInfo],
                        deployment_path: str,
                        profile_manager: DeviceProfileManager,
                        num_trials: int,
                        qpf_trials: int) -> Dict[str, Any]:
    """
    Run all trials for one workload × strategy cell.

    For deterministic strategies (static, greedy), runs 1 trial and reports
    it honestly — repeating identical results would be misleading.

    For stochastic strategies (qpf_*, random), runs num_trials with
    different seeds.
    """
    is_deterministic = strategy in ('static', 'greedy')
    effective_trials = 1 if is_deterministic else num_trials

    trials = []
    for i in range(effective_trials):
        trial_id = i + 1
        print(f"    Trial {trial_id}/{effective_trials}...", end=' ', flush=True)

        trial = run_single_trial(
            trial_id, strategy, graph, unified_graph,
            ensemble_dict, deployment_path, profile_manager, qpf_trials
        )
        trials.append(trial)

        if 'error' not in trial:
            m = trial['metrics']
            print(f"makespan={m['makespan_ms']:.2f}ms  energy={m['energy_joules']:.4f}J")
        else:
            print(f"ERROR: {trial['error']}")

    # Aggregate statistics
    valid_trials = [t for t in trials if 'error' not in t]
    if not valid_trials:
        return {'workload': workload_name, 'strategy': strategy, 'error': 'all_trials_failed'}

    makespans = [t['metrics']['makespan_ms'] for t in valid_trials]
    energies = [t['metrics']['energy_joules'] for t in valid_trials]

    aggregate = {
        'workload': workload_name,
        'strategy': strategy,
        'is_deterministic': is_deterministic,
        'num_trials': len(valid_trials),
        'makespan_ms': {
            'mean': statistics.mean(makespans),
            'median': statistics.median(makespans),
            'std': statistics.stdev(makespans) if len(makespans) > 1 else 0.0,
            'min': min(makespans),
            'max': max(makespans),
        },
        'energy_joules': {
            'mean': statistics.mean(energies),
            'median': statistics.median(energies),
            'std': statistics.stdev(energies) if len(energies) > 1 else 0.0,
            'min': min(energies),
            'max': max(energies),
        },
        'contention_devices_mean': statistics.mean(
            [t['metrics']['contention_devices'] for t in valid_trials]
        ),
        'effective_parallelism_mean': statistics.mean(
            [t['metrics']['effective_parallelism'] for t in valid_trials]
        ),
        'devices_used_mean': statistics.mean(
            [t['metrics']['num_devices_used'] for t in valid_trials]
        ),
    }

    return {
        'workload': workload_name,
        'strategy': strategy,
        'aggregate': aggregate,
        'trials': trials,
    }


# ============================================================================
# RESULT STORAGE
# ============================================================================

def get_git_hash() -> str:
    """Get current git commit hash for provenance tracking."""
    try:
        result = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            capture_output=True, text=True, timeout=5
        )
        return result.stdout.strip() if result.returncode == 0 else 'unknown'
    except Exception:
        return 'unknown'


def create_results_directory(experiment_name: str, base_dir: str = 'evals/results') -> str:
    """
    Create timestamped results directory.

    Format: evals/results/YYYY-MM-DD_HH-MM-SS_<experiment_name>/
    """
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    dir_name = f"{timestamp}_{experiment_name}"
    full_path = os.path.join(base_dir, dir_name)
    os.makedirs(full_path, exist_ok=True)
    return full_path


def write_metadata(results_dir: str,
                   experiment_name: str,
                   deployment_path: str,
                   workload_names: List[str],
                   strategies: List[str],
                   num_trials: int,
                   qpf_trials: int,
                   profile_manager: DeviceProfileManager):
    """Write experiment metadata for provenance."""
    device_profiles = {}
    for name, profile in profile_manager.profiles.items():
        device_profiles[name] = {
            'type': getattr(profile, 'device_type', 'unknown'),
            'cpu_speed': profile.cpu_speed,
            'memory_size': profile.memory_size,
            'supports_concurrent': getattr(profile, 'supports_concurrent', False),
        }

    metadata = {
        'experiment': experiment_name,
        'timestamp': datetime.now().isoformat(),
        'git_hash': get_git_hash(),
        'python_version': platform.python_version(),
        'platform': platform.platform(),
        'deployment': os.path.basename(deployment_path),
        'deployment_path': deployment_path,
        'device_profiles': device_profiles,
        'workloads': workload_names,
        'strategies': strategies,
        'trials_per_stochastic_cell': num_trials,
        'qpf_internal_trials': qpf_trials,
        'notes': 'Deterministic strategies (static, greedy) run 1 trial. '
                 'Stochastic strategies (qpf_*, random) run N trials with seed=trial_id.',
    }

    path = os.path.join(results_dir, 'metadata.json')
    with open(path, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"\nMetadata written: {path}")


def write_cell_results(results_dir: str, cell_data: Dict[str, Any]):
    """Write results for one workload × strategy cell."""
    workload = cell_data['workload']
    strategy = cell_data['strategy']
    filename = f"{workload}_{strategy}.json"
    path = os.path.join(results_dir, filename)

    # Strip raw convergence history from trials to keep files manageable
    # (summaries are preserved)
    output = deepcopy(cell_data)
    for trial in output.get('trials', []):
        trial.pop('convergence', None)

    with open(path, 'w') as f:
        json.dump(output, f, indent=2, default=str)

    return path


def write_summary(results_dir: str, all_results: List[Dict[str, Any]]):
    """
    Write cross-workload summary comparing all strategies.

    This is the data that feeds the paper's tables and figures.
    """
    summary = {
        'timestamp': datetime.now().isoformat(),
        'comparison': []
    }

    for cell in all_results:
        if 'error' in cell:
            continue
        agg = cell.get('aggregate', {})
        summary['comparison'].append({
            'workload': cell['workload'],
            'strategy': cell['strategy'],
            'is_deterministic': agg.get('is_deterministic', False),
            'num_trials': agg.get('num_trials', 0),
            'makespan_ms_mean': agg.get('makespan_ms', {}).get('mean', 0),
            'makespan_ms_std': agg.get('makespan_ms', {}).get('std', 0),
            'energy_joules_mean': agg.get('energy_joules', {}).get('mean', 0),
            'energy_joules_std': agg.get('energy_joules', {}).get('std', 0),
            'contention_devices_mean': agg.get('contention_devices_mean', 0),
            'effective_parallelism_mean': agg.get('effective_parallelism_mean', 0),
            'devices_used_mean': agg.get('devices_used_mean', 0),
        })

    path = os.path.join(results_dir, 'summary.json')
    with open(path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"Summary written: {path}")

    # Also print a human-readable comparison table
    print("\n" + "=" * 90)
    print("  RESULTS SUMMARY")
    print("=" * 90)
    print(f"  {'Workload':<20} {'Strategy':<18} {'Makespan(ms)':<16} "
          f"{'Energy(J)':<14} {'Contention':<12} {'Parallelism'}")
    print(f"  {'-' * 86}")

    for row in sorted(summary['comparison'],
                      key=lambda r: (r['workload'], r['makespan_ms_mean'])):
        ms = row['makespan_ms_mean']
        ms_std = row['makespan_ms_std']
        en = row['energy_joules_mean']
        cont = row['contention_devices_mean']
        par = row['effective_parallelism_mean']
        n = row['num_trials']

        ms_str = f"{ms:.2f}" if n == 1 else f"{ms:.2f}±{ms_std:.2f}"
        print(f"  {row['workload']:<20} {row['strategy']:<18} {ms_str:<16} "
              f"{en:<14.4f} {cont:<12.1f} {par:.2f}")

    print("=" * 90)


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='TTPython QPF Evaluation Runner',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full experiment
  python evaluation.py \\
      --deployment evals/deployments/cluster_c2_heterogeneous.yaml \\
      --workloads evals/workloads/eval_etl.py evals/workloads/eval_stats.py \\
      --strategies qpf_makespan qpf_energy random static greedy \\
      --trials 30

  # Quick test
  python evaluation.py \\
      --deployment evals/deployments/cluster_c2_heterogeneous.yaml \\
      --workloads evals/workloads/eval_etl.py \\
      --strategies qpf_makespan random \\
      --trials 3

  # Pre-compiled pickles
  python evaluation.py \\
      --deployment evals/deployments/cluster_c2_heterogeneous.yaml \\
      --pickles output/eval_etl.pickle output/eval_stats.pickle \\
      --strategies qpf_makespan random static greedy \\
      --trials 30
        """)

    # Input: workloads (source files or pickles)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('--workloads', nargs='+',
                             help='Paths to TTPython workload .py files')
    input_group.add_argument('--pickles', nargs='+',
                             help='Paths to pre-compiled .pickle files')

    # Deployment
    parser.add_argument('--deployment', required=True,
                        help='Path to deployment YAML')

    # Infrastructure config
    parser.add_argument('--device-types', default='device_types.yaml',
                        help='Path to device_types.yaml')
    parser.add_argument('--network-types', default='network_types.yaml',
                        help='Path to network_types.yaml')

    # Strategies
    parser.add_argument('--strategies', nargs='+',
                        default=['qpf_makespan', 'qpf_energy', 'random', 'static', 'greedy'],
                        choices=['qpf_makespan', 'qpf_energy', 'random', 'static', 'greedy'],
                        help='Placement strategies to evaluate')

    # Trials
    parser.add_argument('--trials', type=int, default=30,
                        help='Trials per stochastic strategy (default: 30)')
    parser.add_argument('--qpf-trials', type=int, default=1000,
                        help='QPF internal optimization trials (default: 1000)')

    # Experiment name
    parser.add_argument('--experiment', default='placement_quality',
                        help='Experiment name for results directory')

    # Output
    parser.add_argument('--results-dir', default='evals/results',
                        help='Base directory for results')

    # Logging
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Enable debug logging')

    args = parser.parse_args()

    # Setup logging
    DebugLogger.set_base_logger_info()
    if args.debug:
        DebugLogger.set_base_logger_debug()

    print("=" * 60)
    print("  TTPython QPF Evaluation Runner")
    print("=" * 60)

    # ----------------------------------------------------------------
    # Step 1: Initialize TTPython infrastructure
    # ----------------------------------------------------------------
    print(f"\nDeployment: {args.deployment}")
    profile_manager, topology = setup_infrastructure(
        args.device_types, args.network_types, args.deployment
    )
    ensemble_dict = build_ensemble_dict(args.deployment)

    print(f"Devices: {list(ensemble_dict.keys())}")
    for name, profile in profile_manager.profiles.items():
        print(f"  {name}: cpu_speed={profile.cpu_speed}x, "
              f"memory={profile.memory_size / (1024**3):.1f}GB")

    # ----------------------------------------------------------------
    # Step 2: Compile or load workloads
    # ----------------------------------------------------------------
    workloads = {}  # {name: TTGraph}

    if args.workloads:
        for app_path in args.workloads:
            name = os.path.splitext(os.path.basename(app_path))[0]
            graph = compile_workload(app_path, args.deployment)
            workloads[name] = graph
    else:
        for pickle_path in args.pickles:
            name = os.path.splitext(os.path.basename(pickle_path))[0]
            graph = load_pickle(pickle_path)
            workloads[name] = graph

    print(f"\nWorkloads: {list(workloads.keys())}")
    for name, graph in workloads.items():
        print(f"  {name}: {len(graph.sqs)} SQs")

    print(f"Strategies: {args.strategies}")
    print(f"Trials per stochastic cell: {args.trials}")
    print(f"QPF internal trials: {args.qpf_trials}")

    # ----------------------------------------------------------------
    # Step 3: Create results directory
    # ----------------------------------------------------------------
    results_dir = create_results_directory(args.experiment, args.results_dir)
    print(f"\nResults directory: {results_dir}")

    # Write metadata
    write_metadata(
        results_dir, args.experiment, args.deployment,
        list(workloads.keys()), args.strategies,
        args.trials, args.qpf_trials, profile_manager
    )

    # ----------------------------------------------------------------
    # Step 4: Run all workload × strategy cells
    # ----------------------------------------------------------------
    all_results = []
    total_cells = len(workloads) * len(args.strategies)
    cell_num = 0

    for workload_name, graph in workloads.items():
        # Build unified graph for this workload
        unified_graph = build_unified_graph(graph, ensemble_dict, workload_name)

        for strategy in args.strategies:
            cell_num += 1
            print(f"\n[{cell_num}/{total_cells}] {workload_name} × {strategy}")
            print(f"  {'─' * 50}")

            # Need to reinitialize profiles for each cell because
            # SmartMapper may modify global state
            initialize_profiles(args.device_types, args.deployment)
            profile_manager = get_profile_manager()

            cell_result = run_experiment_cell(
                workload_name, strategy, graph, unified_graph,
                ensemble_dict, args.deployment, profile_manager,
                args.trials, args.qpf_trials
            )

            # Write individual cell results
            path = write_cell_results(results_dir, cell_result)
            print(f"  Results: {path}")

            all_results.append(cell_result)

    # ----------------------------------------------------------------
    # Step 5: Write summary
    # ----------------------------------------------------------------
    write_summary(results_dir, all_results)

    print(f"\nAll results saved to: {results_dir}")
    print("Done.")


if __name__ == '__main__':
    main()
