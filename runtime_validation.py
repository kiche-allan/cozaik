#!/usr/bin/env python3
"""
TTPython Runtime Validation
=============================

Extracts placement mappings from analytical evaluation results and
orchestrates deployment on the physical cluster for predicted-vs-actual
comparison.

Workflow:
    1. Reads analytical results from evaluation.py JSON files
    2. Extracts one representative mapping per workload × strategy
    3. Saves each mapping as a standalone JSON file
    4. Prints deployment commands for each run
    5. After all runs complete, collects latency logs and compares
       actual runtime performance against analytical predictions

Usage:
    # Step 1: Extract mappings from analytical results
    python runtime_validation.py extract \\
        --results-dir evals/results/2026-03-13_01-54-37_placement_quality \\
        --workloads eval_etl eval_stats \\
        --strategies qpf_makespan static random \\
        --output-dir evals/runtime_mappings

    # Step 2: Deploy each mapping on the cluster (manual, one at a time)
    #   The script prints the exact commands for each run.
    #   Each run writes latency data to a JSON log file.

    # Step 3: Analyze collected runtime logs
    python runtime_validation.py analyze \\
        --logs-dir evals/runtime_logs \\
        --analytical-dir evals/results/2026-03-13_01-54-37_placement_quality \\
        --output evals/results/runtime_validation_results.json
"""

import argparse
import json
import os
import sys
import statistics
from datetime import datetime
from collections import defaultdict
from typing import Dict, Any, List


# ============================================================================
# EXTRACT: Pull mappings from analytical results
# ============================================================================

def extract_mappings(results_dir: str,
                     workloads: List[str],
                     strategies: List[str],
                     output_dir: str):
    """
    Extract one representative mapping per workload × strategy from
    analytical evaluation results.

    For stochastic strategies (QPF, Random): picks the trial with
    median makespan (most representative).
    For deterministic strategies (Static): uses the single trial.
    """
    os.makedirs(output_dir, exist_ok=True)

    manifest = {
        'extracted_at': datetime.now().isoformat(),
        'source_dir': results_dir,
        'runs': []
    }

    for workload in workloads:
        for strategy in strategies:
            filename = f"{workload}_{strategy}.json"
            filepath = os.path.join(results_dir, filename)

            if not os.path.exists(filepath):
                print(f"  SKIP: {filename} not found")
                continue

            with open(filepath) as f:
                cell_data = json.load(f)

            trials = cell_data.get('trials', [])
            valid_trials = [t for t in trials if 'error' not in t]

            if not valid_trials:
                print(f"  SKIP: {filename} has no valid trials")
                continue

            # Pick the median-makespan trial (most representative)
            sorted_trials = sorted(valid_trials,
                                   key=lambda t: t['metrics']['makespan_ms'])
            median_idx = len(sorted_trials) // 2
            representative = sorted_trials[median_idx]

            mapping = representative['mapping']
            predicted_makespan = representative['metrics']['makespan_ms']
            predicted_energy = representative['metrics']['energy_joules']

            # Save mapping file
            run_id = f"{workload}_{strategy}"
            mapping_file = os.path.join(output_dir, f"mapping_{run_id}.json")

            mapping_data = {
                'run_id': run_id,
                'workload': workload,
                'strategy': strategy,
                'predicted_makespan_ms': predicted_makespan,
                'predicted_energy_joules': predicted_energy,
                'trial_id': representative['trial_id'],
                'seed': representative.get('seed', 0),
                'mapping': mapping,
                'device_distribution': representative['metrics'].get('device_distribution', {}),
                'extracted_from': filepath,
            }

            with open(mapping_file, 'w') as f:
                json.dump(mapping_data, f, indent=2)

            manifest['runs'].append({
                'run_id': run_id,
                'workload': workload,
                'strategy': strategy,
                'mapping_file': mapping_file,
                'predicted_makespan_ms': predicted_makespan,
                'num_sqs': len(mapping),
            })

            print(f"  {run_id}: {len(mapping)} SQs, "
                  f"predicted={predicted_makespan:.1f}ms → {mapping_file}")

    # Save manifest
    manifest_path = os.path.join(output_dir, 'manifest.json')
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)

    print(f"\nManifest: {manifest_path}")
    print(f"Total runs to deploy: {len(manifest['runs'])}")

    # Print deployment commands
    print("\n" + "=" * 70)
    print("  DEPLOYMENT COMMANDS")
    print("  Run each one at a time on the cluster.")
    print("=" * 70)

    for run in manifest['runs']:
        run_id = run['run_id']
        workload = run['workload']
        strategy = run['strategy']
        predicted = run['predicted_makespan_ms']

        print(f"\n--- {run_id} (predicted: {predicted:.1f}ms) ---")
        print(f"# Terminal 1 (mid VM RTM):")
        print(f"cd ~/ttpython")
        print(f"python3 runrtm.py output/{workload}.pickle 9000 "
              f"--ip 10.0.0.1 --timeout 300 -s 60 "
              f"--mapping mappings/mapping_{run_id}.json "
              f"--run-label {run_id}")
        print()
        print(f"# Terminal 2-4: same device commands as before")


# ============================================================================
# ANALYZE: Compare predicted vs actual
# ============================================================================

def analyze_results(logs_dir: str,
                    analytical_dir: str,
                    output_path: str):
    """
    Analyze runtime validation logs and compare against analytical predictions.

    Reads JSON log files produced by the modified sink SQs during cluster runs.
    Compares actual measured latencies against predicted makespans.
    """
    results = {
        'analyzed_at': datetime.now().isoformat(),
        'logs_dir': logs_dir,
        'analytical_dir': analytical_dir,
        'runs': []
    }

    # Find all runtime log files (JSONL format — one JSON entry per line)
    log_files = sorted([
        f for f in os.listdir(logs_dir)
        if f.startswith('runtime_log_') and f.endswith('.jsonl')
    ])

    if not log_files:
        print(f"No runtime log files found in {logs_dir}")
        print("Expected files named: runtime_log_<run_label>.jsonl")
        print("These are produced by the modified sink SQs during cluster runs.")
        return

    for log_file in log_files:
        log_path = os.path.join(logs_dir, log_file)

        # Parse JSONL: each line is one latency measurement
        entries = []
        with open(log_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

        if not entries:
            print(f"  {log_file}: no valid entries")
            continue

        # Extract metadata from first entry
        first = entries[0]
        run_label = first.get('run_label', '')
        workload = first.get('workload', 'unknown')

        # Infer strategy from run_label (format: eval_etl_qpf_makespan)
        strategy = 'unknown'
        if run_label:
            # Strip workload prefix to get strategy
            prefix = workload + '_'
            if run_label.startswith(prefix):
                strategy = run_label[len(prefix):]
            run_id = run_label
        else:
            run_id = log_file.replace('runtime_log_', '').replace('.jsonl', '')

        latencies = [e['latency_ms'] for e in entries if 'latency_ms' in e]
        predicted_ms = first.get('predicted_ms', 0)

        if not latencies:
            print(f"  {run_id}: no latency data")
            continue

        # If predicted not in log, try to load from analytical results
        if predicted_ms == 0:
            analytical_file = os.path.join(analytical_dir, f"{workload}_{strategy}.json")
            if os.path.exists(analytical_file):
                with open(analytical_file) as f:
                    analytical = json.load(f)
                agg = analytical.get('aggregate', {})
                predicted_ms = agg.get('makespan_ms', {}).get('mean', 0)

        # Compute actual statistics
        n = len(latencies)
        actual_mean = statistics.mean(latencies)
        actual_median = statistics.median(latencies)
        actual_std = statistics.stdev(latencies) if n > 1 else 0
        actual_min = min(latencies)
        actual_max = max(latencies)
        sorted_lat = sorted(latencies)
        actual_p25 = sorted_lat[int(n * 0.25)]
        actual_p75 = sorted_lat[int(n * 0.75)]

        # Overhead ratio
        overhead_ratio = actual_median / predicted_ms if predicted_ms > 0 else 0

        run_result = {
            'run_id': run_id,
            'workload': workload,
            'strategy': strategy,
            'predicted_makespan_ms': predicted_ms,
            'actual': {
                'n': n,
                'mean_ms': round(actual_mean, 2),
                'median_ms': round(actual_median, 2),
                'std_ms': round(actual_std, 2),
                'min_ms': round(actual_min, 2),
                'max_ms': round(actual_max, 2),
                'p25_ms': round(actual_p25, 2),
                'p75_ms': round(actual_p75, 2),
            },
            'overhead_ratio': round(overhead_ratio, 2),
        }
        results['runs'].append(run_result)

        print(f"  {run_id}: predicted={predicted_ms:.1f}ms, "
              f"actual_median={actual_median:.1f}ms, "
              f"overhead={overhead_ratio:.2f}x, N={n}")

    # Cross-strategy comparison per workload
    print("\n" + "=" * 80)
    print("  RUNTIME VALIDATION: PREDICTED vs ACTUAL")
    print("=" * 80)
    print(f"  {'Run':<30} {'Predicted(ms)':<16} {'Actual Med(ms)':<16} "
          f"{'Overhead':<10} {'N'}")
    print(f"  {'-' * 78}")

    for run in sorted(results['runs'], key=lambda r: (r['workload'], r['actual']['median_ms'])):
        print(f"  {run['run_id']:<30} {run['predicted_makespan_ms']:<16.1f} "
              f"{run['actual']['median_ms']:<16.1f} "
              f"{run['overhead_ratio']:<10.2f}x {run['actual']['n']}")

    # Per-workload comparison: does QPF beat Static/Random in reality?
    workloads_seen = set(r['workload'] for r in results['runs'])
    for wl in sorted(workloads_seen):
        wl_runs = [r for r in results['runs'] if r['workload'] == wl]
        if len(wl_runs) > 1:
            print(f"\n  {wl} — Strategy Ranking (by actual median latency):")
            for i, run in enumerate(sorted(wl_runs, key=lambda r: r['actual']['median_ms'])):
                marker = " ← BEST" if i == 0 else ""
                print(f"    {i+1}. {run['strategy']}: {run['actual']['median_ms']:.1f}ms "
                      f"(predicted: {run['predicted_makespan_ms']:.1f}ms){marker}")

    # Save results
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved: {output_path}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='TTPython Runtime Validation',
        formatter_class=argparse.RawDescriptionHelpFormatter)

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Extract command
    extract_parser = subparsers.add_parser('extract',
        help='Extract mappings from analytical results')
    extract_parser.add_argument('--results-dir', required=True,
        help='Path to analytical results directory')
    extract_parser.add_argument('--workloads', nargs='+', required=True,
        help='Workload names (e.g., eval_etl eval_stats)')
    extract_parser.add_argument('--strategies', nargs='+',
        default=['qpf_makespan', 'static', 'random'],
        help='Strategies to extract')
    extract_parser.add_argument('--output-dir', default='evals/runtime_mappings',
        help='Output directory for mapping files')

    # Analyze command
    analyze_parser = subparsers.add_parser('analyze',
        help='Analyze runtime validation logs')
    analyze_parser.add_argument('--logs-dir', required=True,
        help='Directory containing runtime_log_*.json files')
    analyze_parser.add_argument('--analytical-dir', required=True,
        help='Path to analytical results for comparison')
    analyze_parser.add_argument('--output', default='evals/results/runtime_validation.json',
        help='Output path for analysis results')

    args = parser.parse_args()

    if args.command == 'extract':
        extract_mappings(args.results_dir, args.workloads,
                        args.strategies, args.output_dir)
    elif args.command == 'analyze':
        analyze_results(args.logs_dir, args.analytical_dir, args.output)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
