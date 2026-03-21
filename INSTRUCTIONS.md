# INSTRUCTIONS: Running All Evaluation Experiments

**Project:** Cozaik  
**Component:** TTPython — Quantitative Placement Framework (QPF) Evaluation  

---

## Prerequisites

### Software

- Python 3.10+ with conda or virtualenv
- TTPython runtime installed (`pip install -e .` from repo root)
- Required Python packages: `multiprocess`, `pyyaml`, `intervaltree`, `timedinput`

### Repository Structure

```
ticktalkpython-new/
├── ticktalkpython/          # TTPython runtime
│   ├── Engine.py            # SQ execution (PersistentPool, SharedThreadEngine)
│   ├── RuntimeManager.py    # RTM: deployment, adaptation, heartbeat
│   ├── SmartMapper.py       # QPF optimizer + contention detection
│   ├── Combiner.py          # Multitenancy CombinedGraph
│   └── ...
├── evals/
│   ├── workloads/           # Evaluation workload files
│   │   ├── eval_etl.py      # 19 SQs, sequential chain
│   │   ├── eval_stats.py    # 18 SQs, fork-join
│   │   ├── eval_pred.py     # 30 SQs, diamond DAG
│   │   ├── eval_train.py    # 17 SQs, fork-join training
│   │   ├── eval_city.py     # 45 SQs, dual-source parallel
│   │   ├── eval_taxi.py     # 35 SQs, composite ETL+analytics
│   │   ├── eval_grid.py     # 38 SQs, 4-branch parallel
│   │   └── eval_fit.py      # 42 SQs, 5-branch parallel
│   ├── deployments/
│   │   ├── cluster_c2_heterogeneous.yaml  # 3-device heterogeneous
│   │   └── cluster_c1_homogeneous.yaml    # 3-device homogeneous
│   ├── results/             # Experiment outputs (JSON)
│   └── runtime_mappings/    # Extracted placement mappings
├── evaluation.py            # QPF analytical experiment runner
├── runtime_adaptation_evaluation.py  # Adaptation experiment runner
├── runtime_validation.py    # Mapping extractor + runtime log analyzer
├── calibrate_device.py      # Device speed calibration
├── run_full_validation.sh   # Full automation (compile → analyze → deploy)
├── compile.py               # TTPython compiler
├── runrtm.py                # Runtime Manager launcher
├── runens.py                # Device ensemble launcher
├── riotbench_provider.py    # RIoTBench data provider (SenML CSV reader)
├── device_types.yaml        # Device type definitions with speed multipliers
└── network_types.yaml       # Network link type definitions
```

### Cluster Setup (for runtime experiments)

Three VMs in the same datacenter:

| Device ID | Role | vCPUs | RAM | CPU Type | device_types.yaml entry |
|-----------|------|-------|-----|----------|------------------------|
| edge0 | Device ensemble | 1 | 1 GB | Shared | edge_vm_calibrated (1.58x) |
| mid0 | Device ensemble | 2 | 4 GB | Shared | mid_vm_calibrated (2.64x) |
| cloud0 | RTM + Device ensemble | 4 | 8 GB | Dedicated | cloud_vm_calibrated (2.82x) |

SSH key authentication must be configured for passwordless access to all VMs.
TTPython must be installed on all VMs at `~/ttpython/`.

---

## 1. Device Speed Calibration

Run on each VM before first use:

```bash
python calibrate_device.py --name edge_vm --type raspberry_pi_4 --iterations 2000
python calibrate_device.py --name mid_vm --type intel_nuc_i5 --iterations 2000
python calibrate_device.py --name cloud_vm --type server_x86 --iterations 2000
```

This produces `calibration_results_<name>.json` and `profile_<name>.yaml`. Use the resulting speed multiplier to update `device_types.yaml`.

---

## 2. Compilation

All workloads must be compiled before any experiment. Run from repo root:

```bash
for WL in eval_etl eval_stats eval_pred eval_train eval_city eval_taxi eval_grid eval_fit; do
    python compile.py evals/workloads/${WL}.py -o ./output/ -g \
        --deployment evals/deployments/cluster_c2_heterogeneous.yaml
done
```

Each produces `output/<workload>.pickle` and `output/<workload>.png` (DAG visualization).

**Important:** Recompilation changes SQ indices. All mappings and runtime logs from previous compilations become invalid.

---

## 3. QPF Placement Quality (Analytical)

### Heterogeneous cluster — all strategies

```bash
python evaluation.py \
    --workloads evals/workloads/eval_etl.py evals/workloads/eval_stats.py \
               evals/workloads/eval_pred.py evals/workloads/eval_train.py \
               evals/workloads/eval_city.py evals/workloads/eval_taxi.py \
               evals/workloads/eval_grid.py evals/workloads/eval_fit.py \
    --deployment evals/deployments/cluster_c2_heterogeneous.yaml \
    --strategies qpf_makespan random static greedy \
    --trials 30 --qpf-trials 1000
```

### Homogeneous cluster

```bash
python evaluation.py \
    --workloads evals/workloads/eval_etl.py evals/workloads/eval_stats.py \
               evals/workloads/eval_pred.py evals/workloads/eval_train.py \
               evals/workloads/eval_city.py evals/workloads/eval_taxi.py \
               evals/workloads/eval_grid.py evals/workloads/eval_fit.py \
    --deployment evals/deployments/cluster_c1_homogeneous.yaml \
    --strategies qpf_makespan random static greedy \
    --trials 30 --qpf-trials 1000
```

### Energy objective

```bash
python evaluation.py \
    --workloads evals/workloads/eval_etl.py evals/workloads/eval_stats.py \
               evals/workloads/eval_pred.py evals/workloads/eval_train.py \
               evals/workloads/eval_city.py evals/workloads/eval_taxi.py \
               evals/workloads/eval_grid.py evals/workloads/eval_fit.py \
    --deployment evals/deployments/cluster_c2_heterogeneous.yaml \
    --strategies qpf_makespan qpf_energy \
    --trials 30 --qpf-trials 1000
```

### QPF iteration sensitivity

```bash
for ITERS in 100 500 1000 5000; do
    python evaluation.py \
        --workloads evals/workloads/eval_city.py evals/workloads/eval_grid.py \
                   evals/workloads/eval_fit.py \
        --deployment evals/deployments/cluster_c2_heterogeneous.yaml \
        --strategies qpf_makespan \
        --trials 30 --qpf-trials $ITERS \
        --experiment "sensitivity_${ITERS}"
done
```

### Output

Results are saved automatically to `evals/results/<timestamp>_<experiment>/` containing per-workload JSON files and a `summary.json` with aggregated comparisons.

---

## 4. Random Search vs Greedy

No separate run needed. The QPF evaluation (Section 3) already includes both `qpf_makespan` (random search) and `greedy` results in the same output. Compare them from `summary.json`.

---

## 5. Runtime Adaptation (Analytical)

### All experiments, all workloads

```bash
for WL in eval_etl eval_stats eval_pred eval_train eval_city eval_taxi eval_grid eval_fit; do
    python runtime_adaptation_evaluation.py \
        --app evals/workloads/${WL}.py \
        --deployment evals/deployments/cluster_c2_heterogeneous.yaml \
        --device-types device_types.yaml \
        --experiment all \
        --trials 10 --seed 42 \
        --output evals/results/adaptation_eval_${WL}.json
done
```

### Single experiment

```bash
python runtime_adaptation_evaluation.py \
    --app evals/workloads/eval_city.py \
    --deployment evals/deployments/cluster_c2_heterogeneous.yaml \
    --device-types device_types.yaml \
    --experiment constraints \
    --trials 10 --seed 42 \
    --output evals/results/adaptation_constraints_city.json
```

Available experiments: `constraints`, `quality`, `overhead`, `multifailure`, `search_efficiency`, `all`.

---

## 6. Runtime Validation (Physical Cluster)

### Automated — all workloads × all strategies

```bash
./run_full_validation.sh
```

This runs the complete pipeline: compile → analytical experiments → extract mappings → push to VMs → deploy each workload × strategy → collect logs → consolidate.

To skip compilation and analytical experiments (when already done):

```bash
./run_full_validation.sh --resume
```

**Important:** Never use `--resume` after recompiling workloads. Stale mappings cause deployment failures.

The script configuration is at the top of `run_full_validation.sh`:
```
SSH_KEY="$HOME/.ssh/ttpython_cluster"
CLOUD0_IP="<CLOUD0_IP>"
MID0_IP="<MID0_IP>"
EDGE0_IP="<EDGE0_IP>"
WORKLOADS="eval_etl eval_stats eval_pred eval_train eval_city eval_taxi eval_grid eval_fit"
STRATEGIES="qpf_makespan static random greedy"
```

Update the IPs if the cluster changes.

### Manual single-workload deployment

#### Step 1: Extract a mapping from analytical results

```bash
python runtime_validation.py extract \
    --results-dir evals/results/<timestamp>_placement_quality \
    --workloads eval_stats \
    --strategies qpf_makespan static \
    --output-dir evals/runtime_mappings
```

#### Step 2: Push to VMs

```bash
SSH_KEY="$HOME/.ssh/ttpython_cluster"
for IP in <CLOUD0_IP> <MID0_IP> <EDGE0_IP>; do
    rsync -avz --quiet \
        -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=no" \
        ticktalkpython/ runrtm.py runens.py riotbench_provider.py \
        device_types.yaml network_types.yaml evals/ output/ \
        root@$IP:~/ttpython/
done

scp -i $SSH_KEY evals/runtime_mappings/mapping_*.json \
    root@<CLOUD0_IP>:~/ttpython/mappings/
```

#### Step 3: Deploy using 4 terminals

**Terminal 1 — SSH into cloud0, run RTM:**
```bash
ssh -i ~/.ssh/ttpython_cluster root@<CLOUD0_IP>
cd ~/ttpython
export TTPYTHON_RUN_LABEL=eval_stats_qpf_makespan
python3 runrtm.py output/eval_stats.pickle 9000 \
    --ip <CLOUD0_IP> --timeout 600 -s 60 \
    --mapping mappings/mapping_eval_stats_qpf_makespan.json \
    --run-label eval_stats_qpf_makespan
```
Hit enter when prompted ("wait for X secs for devices to connect").

**Terminal 2 — SSH into cloud0, run cloud0 device:**
```bash
ssh -i ~/.ssh/ttpython_cluster root@<CLOUD0_IP>
cd ~/ttpython
export TTPYTHON_RUN_LABEL=eval_stats_qpf_makespan
export TTPYTHON_DEVICE_TYPE=cloud_vm_calibrated
python3 runens.py cloud0 9002 --ip <CLOUD0_IP> \
    --rtm_ip <CLOUD0_IP> --rtm_port 9000 \
    --device-type cloud_vm_calibrated --timeout 600
```

**Terminal 3 — SSH into mid0, run mid0 device:**
```bash
ssh -i ~/.ssh/ttpython_cluster root@<MID0_IP>
cd ~/ttpython
export TTPYTHON_RUN_LABEL=eval_stats_qpf_makespan
export TTPYTHON_DEVICE_TYPE=mid_vm_calibrated
python3 runens.py mid0 9002 --ip <MID0_IP> \
    --rtm_ip <CLOUD0_IP> --rtm_port 9000 \
    --device-type mid_vm_calibrated --timeout 600
```

**Terminal 4 — SSH into edge0, run edge0 device:**
```bash
ssh -i ~/.ssh/ttpython_cluster root@<EDGE0_IP>
cd ~/ttpython
export TTPYTHON_RUN_LABEL=eval_stats_qpf_makespan
export TTPYTHON_DEVICE_TYPE=edge_vm_calibrated
python3 runens.py edge0 9002 --ip <EDGE0_IP> \
    --rtm_ip <CLOUD0_IP> --rtm_port 9000 \
    --device-type edge_vm_calibrated --timeout 600
```

Wait for latency messages to appear. Ctrl+C all terminals when done.

#### Step 4: Collect logs

```bash
for IP in <CLOUD0_IP> <MID0_IP> <EDGE0_IP>; do
    scp -i ~/.ssh/ttpython_cluster \
        root@$IP:~/ttpython/runtime_log_*.jsonl ./runtime_logs/
done
```

#### Step 5: Analyze

```bash
python runtime_validation.py analyze \
    --logs-dir ./runtime_logs \
    --analytical-dir evals/results/<timestamp>_placement_quality \
    --output evals/results/runtime_validation.json
```

---

## 7. Contention Detection and Resolution (Physical Cluster)

### Prerequisites

`ticktalkpython/Engine.py` must include the per-SQ timing instrumentation (`_log_sq_timing` function and `time.perf_counter()` calls in `assign_func`, `SharedProcessEngine.execute_job`, and `SharedThreadEngine.execute_job`). Push the updated Engine.py to all VMs before running.

### Deploy

Use the 4-terminal method from Section 6, but **without** `--mapping` and with a contention-specific run label:

**Terminal 1 — SSH into cloud0, run RTM (no --mapping flag):**
```bash
ssh -i ~/.ssh/ttpython_cluster root@<CLOUD0_IP>
cd ~/ttpython
export TTPYTHON_RUN_LABEL=sq_timing_eval_city
python3 runrtm.py output/eval_city.pickle 9000 \
    --ip <CLOUD0_IP> --timeout 600 -s 30 \
    --run-label sq_timing_eval_city
```

Terminals 2-4 same as Section 6 but with `export TTPYTHON_RUN_LABEL=sq_timing_eval_city`.

Wait 2-3 minutes for data to flow. Ctrl+C all terminals.

### Collect timing data

```bash
mkdir -p sq_timing_results
scp -i ~/.ssh/ttpython_cluster \
    root@<CLOUD0_IP>:~/ttpython/sq_timing_sq_timing_eval_city.jsonl \
    ./sq_timing_results/from_cloud0.jsonl
scp -i ~/.ssh/ttpython_cluster \
    root@<MID0_IP>:~/ttpython/sq_timing_sq_timing_eval_city.jsonl \
    ./sq_timing_results/from_mid0.jsonl
scp -i ~/.ssh/ttpython_cluster \
    root@<EDGE0_IP>:~/ttpython/sq_timing_sq_timing_eval_city.jsonl \
    ./sq_timing_results/from_edge0.jsonl
```

### Analyze timing data

```python
import json, statistics
from collections import defaultdict

devices = {
    'sq_timing_results/from_cloud0.jsonl': 'cloud0',
    'sq_timing_results/from_mid0.jsonl': 'mid0',
    'sq_timing_results/from_edge0.jsonl': 'edge0',
}
all_data = []
for fn, device in devices.items():
    with open(fn) as f:
        for line in f:
            if line.strip():
                e = json.loads(line.strip())
                e['device'] = device
                all_data.append(e)

for device in ['edge0', 'mid0', 'cloud0']:
    entries = [e for e in all_data if e['device'] == device]
    modes = defaultdict(list)
    for e in entries:
        modes[e['mode']].append(e['execution_ms'])
    print(f'=== {device} ({len(entries)} entries) ===')
    for mode, lats in sorted(modes.items()):
        std = statistics.stdev(lats) if len(lats) > 1 else 0
        print(f'  {mode}: n={len(lats)} mean={statistics.mean(lats):.3f}ms std={std:.3f}ms')
```

### What to look for

- edge0 (1 core): all entries should be `timesliced`
- cloud0 (4 cores): mix of `concurrent` and `timesliced`
- Concurrent SQs should show lower standard deviation than timesliced SQs on the same device

---

## 8. Runtime Failure Injection (Physical Cluster)

1. Deploy a workload using the 4-terminal method from Section 6, **without** `--mapping`
2. Wait 2 minutes for steady-state data flow
3. Check Terminal 1 (RTM) for any "Migrated" messages to see where the source SQ ended up
4. **Do not kill** the device hosting the source SQ (pipeline cannot recover without data source)
5. **Do not kill** cloud0 if it hosts the RTM (adaptation cannot happen without the RTM)
6. Ctrl+C the terminal of the device to kill
7. Watch Terminal 1 for "Device X suspected failed" and adaptation messages
8. Wait 2 more minutes for recovery
9. Ctrl+C remaining terminals
10. Collect logs from all VMs (same scp commands as Section 6)

**Note:** Detection latency depends on the `-s` flag in runrtm.py. With `-s 30`, detection takes 30-40 seconds. With `-s 60`, it takes 60-70 seconds.

---

## 9. Cleaning Up Between Runs

```bash
SSH_KEY="$HOME/.ssh/ttpython_cluster"
for IP in <CLOUD0_IP> <MID0_IP> <EDGE0_IP>; do
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no root@$IP "\
        pkill -9 -f 'runrtm.py|runens.py' 2>/dev/null; \
        fuser -k 9000/udp 9000/tcp 9002/udp 9002/tcp 2>/dev/null; \
        rm -f ~/ttpython/runtime_log_*.jsonl ~/ttpython/sq_timing_*.jsonl; \
        true"
done
```

Wait 5 seconds before starting a new run.

---

## 10. Reproducing All Results

```bash
# Step 1: Compile all workloads
for WL in eval_etl eval_stats eval_pred eval_train eval_city eval_taxi eval_grid eval_fit; do
    python compile.py evals/workloads/${WL}.py -o ./output/ -g \
        --deployment evals/deployments/cluster_c2_heterogeneous.yaml
done

# Step 2: QPF analytical — heterogeneous
python evaluation.py \
    --workloads evals/workloads/eval_etl.py evals/workloads/eval_stats.py \
               evals/workloads/eval_pred.py evals/workloads/eval_train.py \
               evals/workloads/eval_city.py evals/workloads/eval_taxi.py \
               evals/workloads/eval_grid.py evals/workloads/eval_fit.py \
    --deployment evals/deployments/cluster_c2_heterogeneous.yaml \
    --strategies qpf_makespan qpf_energy random static greedy \
    --trials 30 --qpf-trials 1000

# Step 3: QPF analytical — homogeneous
python evaluation.py \
    --workloads evals/workloads/eval_etl.py evals/workloads/eval_stats.py \
               evals/workloads/eval_pred.py evals/workloads/eval_train.py \
               evals/workloads/eval_city.py evals/workloads/eval_taxi.py \
               evals/workloads/eval_grid.py evals/workloads/eval_fit.py \
    --deployment evals/deployments/cluster_c1_homogeneous.yaml \
    --strategies qpf_makespan random static greedy \
    --trials 30 --qpf-trials 1000

# Step 4: Runtime adaptation — analytical
for WL in eval_etl eval_stats eval_pred eval_train eval_city eval_taxi eval_grid eval_fit; do
    python runtime_adaptation_evaluation.py \
        --app evals/workloads/${WL}.py \
        --deployment evals/deployments/cluster_c2_heterogeneous.yaml \
        --device-types device_types.yaml \
        --experiment all --trials 10 --seed 42 \
        --output evals/results/adaptation_eval_${WL}.json
done

# Step 5: Full runtime validation (requires cluster)
./run_full_validation.sh

# Step 6: Contention timing (requires cluster, manual 4-terminal deployment)
# See Section 7
```
