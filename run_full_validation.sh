#!/bin/bash
# ============================================================================
# TTPython Runtime Validation — Full Automation
# ============================================================================
# Runs the complete evaluation pipeline:
#   1. Compile all workloads
#   2. Run analytical experiments (evaluation.py)
#   3. Extract mappings from analytical results
#   4. Push code + pickles + mappings to all VMs
#   5. Deploy each workload × strategy on the cluster
#   6. Collect logs
#
# Prerequisites:
#   - SSH key auth set up: ssh-copy-id -i ~/.ssh/ttpython_cluster root@<IP>
#   - TTPython installed on all VMs
#   - Run from repo root on your PC (WSL)
#
# Usage:
#   chmod +x run_full_validation.sh
#   ./run_full_validation.sh
# ============================================================================

set -e  # Exit on error

# Avoid UnicodeEncodeError from Python scripts printing box-drawing
# characters on Windows consoles using a non-UTF-8 codepage.
export PYTHONIOENCODING=utf-8

# ======================== CONFIGURATION ========================
SSH_KEY="$HOME/.ssh/ttpython_cluster"
SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=10"

CLOUD0_IP="188.166.146.111"
MID0_IP="159.65.24.36"
EDGE0_IP="159.65.31.18"

DEPLOYMENT="evals/deployments/cluster_c2_heterogeneous.yaml"
WORKLOADS="eval_etl eval_stats eval_pred eval_train"
STRATEGIES="qpf_makespan static random greedy"

RTM_TIMEOUT=180      # Total timeout for RTM (seconds)
RTM_SUB_TIME=30      # Time to wait for devices to connect
STREAM_TIME=60       # Stream duration (-s flag)
DEVICE_TIMEOUT=180   # Device ensemble timeout

RESULTS_DIR=""       # Set after analytical experiments run
MAPPINGS_DIR="evals/runtime_mappings"
LOGS_DIR="runtime_logs_automated"
TIMESTAMP=$(date +%Y-%m-%d_%H-%M-%S)

# ======================== HELPER FUNCTIONS ========================

log() {
    echo "[$(date +%H:%M:%S)] $1"
}

ssh_cmd() {
    local ip=$1
    shift
    ssh $SSH_OPTS root@$ip "$@"
}

scp_to() {
    local ip=$1
    local src=$2
    local dst=$3
    scp $SSH_OPTS "$src" root@$ip:"$dst"
}

scp_from() {
    local ip=$1
    local src=$2
    local dst=$3
    timeout 15 scp $SSH_OPTS root@$ip:"$src" "$dst" 2>/dev/null || true
}

kill_leftover_processes() {
    log "    Killing leftover TTPython processes on all VMs..."
    for IP in $CLOUD0_IP $MID0_IP $EDGE0_IP; do
        ssh_cmd $IP "pkill -f 'runrtm.py\|runens.py' 2>/dev/null; sleep 1" &
    done
    wait
    sleep 2
}

check_connectivity() {
    log "Checking cluster connectivity..."
    for IP in $CLOUD0_IP $MID0_IP $EDGE0_IP; do
        if ssh_cmd $IP "echo ok" > /dev/null 2>&1; then
            log "  $IP: OK"
        else
            log "  $IP: FAILED — aborting"
            exit 1
        fi
    done
    log "All VMs reachable."
}

# ======================== STEP 1: COMPILE ========================

compile_workloads() {
    log "========== STEP 1: Compiling all workloads =========="
    for WL in $WORKLOADS; do
        log "  Compiling $WL..."
        python compile.py evals/workloads/${WL}.py -o ./output/ \
            --deployment $DEPLOYMENT 2>&1 | grep -E "SQs analyzed|Compilation successful|instances"
    done
    log "Compilation complete."
}

# ======================== STEP 2: ANALYTICAL EXPERIMENTS ========================

run_analytical() {
    log "========== STEP 2: Running analytical experiments =========="
    
    local wl_args=""
    for WL in $WORKLOADS; do
        wl_args="$wl_args evals/workloads/${WL}.py"
    done
    
    python evaluation.py \
        --deployment $DEPLOYMENT \
        --workloads $wl_args \
        --strategies $STRATEGIES \
        --trials 30 --qpf-trials 1000
    
    # Find the results directory (most recent)
    RESULTS_DIR=$(ls -td evals/results/*_placement_quality 2>/dev/null | head -1)
    
    if [ -z "$RESULTS_DIR" ]; then
        log "ERROR: No results directory found"
        exit 1
    fi
    
    log "Analytical results: $RESULTS_DIR"
}

# ======================== STEP 3: EXTRACT MAPPINGS ========================

extract_mappings() {
    log "========== STEP 3: Extracting mappings =========="
    
    local wl_args=""
    for WL in $WORKLOADS; do
        wl_args="$wl_args $WL"
    done
    
    python runtime_validation.py extract \
        --results-dir "$RESULTS_DIR" \
        --workloads $wl_args \
        --strategies $STRATEGIES \
        --output-dir $MAPPINGS_DIR
    
    log "Mappings extracted to $MAPPINGS_DIR"
}

# ======================== STEP 4: PUSH TO VMS ========================

push_to_vms() {
    log "========== STEP 4: Pushing code and data to VMs =========="
    
    for IP in $CLOUD0_IP $MID0_IP $EDGE0_IP; do
        log "  Pushing to $IP..."

        # Push core code (tar over ssh — rsync unavailable in this shell)
        ssh_cmd $IP "mkdir -p ~/ttpython"
        tar --exclude='*.pdf' --exclude='*.docx' --exclude='*.pickle' \
            --exclude='*.png' --exclude='riot' --exclude='.git' \
            --exclude='__pycache__' --exclude='runtime_logs*' \
            --exclude='evals/results' --exclude='Lib' --exclude='Scripts' \
            --exclude='share' -cf - . \
            | ssh $SSH_OPTS root@$IP "tar -xf - -C ~/ttpython"

        # Ensure riotbench_provider is at root level
        scp_to $IP "evals/workloads/riotbench_provider.py" "~/ttpython/riotbench_provider.py"
        
        # Ensure import os is in runrtm.py
        ssh_cmd $IP "grep -q '^import os' ~/ttpython/runrtm.py || sed -i '1s/^/import os\n/' ~/ttpython/runrtm.py"
    done
    
    # Push pickles to RTM (cloud0)
    log "  Pushing pickles to cloud0..."
    ssh_cmd $CLOUD0_IP "mkdir -p ~/ttpython/output ~/ttpython/mappings"
    for WL in $WORKLOADS; do
        scp_to $CLOUD0_IP "./output/${WL}.pickle" "~/ttpython/output/${WL}.pickle"
    done
    
    # Push mappings to RTM (cloud0)
    log "  Pushing mappings to cloud0..."
    for f in $MAPPINGS_DIR/mapping_*.json; do
        scp_to $CLOUD0_IP "$f" "~/ttpython/mappings/$(basename $f)"
    done
    
    log "Push complete."
}

# ======================== STEP 5: DEPLOY AND COLLECT ========================

run_single_deployment() {
    local workload=$1
    local strategy=$2
    local run_label="${workload}_${strategy}"
    
    # Read predicted makespan from mapping file
    local mapping_file="$MAPPINGS_DIR/mapping_${run_label}.json"
    if [ ! -f "$mapping_file" ]; then
        log "    SKIP: $mapping_file not found"
        return
    fi
    
    local predicted_ms=$(python -c "
import json
with open('$mapping_file') as f:
    d = json.load(f)
print(f\"{d.get('predicted_makespan_ms', 0):.1f}\")
")
    
    log "  ---- $run_label (predicted: ${predicted_ms}ms) ----"
    
    # Skip if we already have data for this run
    if [ -f "${LOGS_DIR}/runtime_log_${run_label}.jsonl" ]; then
        local existing=$(wc -l < "${LOGS_DIR}/runtime_log_${run_label}.jsonl" 2>/dev/null || echo "0")
        if [ "$existing" -gt "10" ]; then
            log "    SKIP: Already have $existing entries. Delete ${LOGS_DIR}/runtime_log_${run_label}.jsonl to rerun."
            return
        fi
    fi
    
    # Kill any leftover processes from previous run
    kill_leftover_processes
    
    # Clean old logs for this run on all VMs
    for IP in $CLOUD0_IP $MID0_IP $EDGE0_IP; do
        ssh_cmd $IP "rm -f ~/ttpython/runtime_log_${run_label}.jsonl ~/ttpython/runtime_log_${workload}.jsonl" 2>/dev/null || true
    done
    
    # Start RTM on cloud0 (background)
    log "    Starting RTM on cloud0..."
    ssh_cmd $CLOUD0_IP "
        cd ~/ttpython
        export TTPYTHON_RUN_LABEL=${run_label}
        export TTPYTHON_PREDICTED_MS=${predicted_ms}
        echo '' | python3 runrtm.py output/${workload}.pickle 9000 \
            --ip ${CLOUD0_IP} --timeout ${RTM_TIMEOUT} -s ${STREAM_TIME} \
            --mapping mappings/mapping_${run_label}.json \
            --run-label ${run_label} \
            > /tmp/rtm_${run_label}.log 2>&1
    " &
    local RTM_PID=$!
    
    # Wait for RTM to initialize
    log "    Waiting ${RTM_SUB_TIME}s for RTM to initialize..."
    sleep 15
    
    # Start cloud0 device (background)
    log "    Starting cloud0 device..."
    ssh_cmd $CLOUD0_IP "
        cd ~/ttpython
        export TTPYTHON_RUN_LABEL=${run_label}
        export TTPYTHON_PREDICTED_MS=${predicted_ms}
        echo '' | python3 runens.py cloud0 9002 \
            --ip ${CLOUD0_IP} --rtm_ip ${CLOUD0_IP} --rtm_port 9000 \
            --device-type cloud_vm_calibrated --timeout ${DEVICE_TIMEOUT} \
            > /tmp/cloud0_${run_label}.log 2>&1
    " &
    local CLOUD0_PID=$!
    
    # Start mid0 device (background)
    log "    Starting mid0 device..."
    ssh_cmd $MID0_IP "
        cd ~/ttpython
        export TTPYTHON_RUN_LABEL=${run_label}
        export TTPYTHON_PREDICTED_MS=${predicted_ms}
        echo '' | python3 runens.py mid0 9002 \
            --ip ${MID0_IP} --rtm_ip ${CLOUD0_IP} --rtm_port 9000 \
            --device-type mid_vm_calibrated --timeout ${DEVICE_TIMEOUT} \
            > /tmp/mid0_${run_label}.log 2>&1
    " &
    local MID0_PID=$!
    
    # Start edge0 device (background)
    log "    Starting edge0 device..."
    ssh_cmd $EDGE0_IP "
        cd ~/ttpython
        export TTPYTHON_RUN_LABEL=${run_label}
        export TTPYTHON_PREDICTED_MS=${predicted_ms}
        echo '' | python3 runens.py edge0 9002 \
            --ip ${EDGE0_IP} --rtm_ip ${CLOUD0_IP} --rtm_port 9000 \
            --device-type edge_vm_calibrated --timeout ${DEVICE_TIMEOUT} \
            > /tmp/edge0_${run_label}.log 2>&1
    " &
    local EDGE0_PID=$!
    
    # Wait for all processes to complete
    log "    Waiting for deployment to complete (up to ${RTM_TIMEOUT}s)..."
    wait $RTM_PID 2>/dev/null || true
    wait $CLOUD0_PID 2>/dev/null || true
    wait $MID0_PID 2>/dev/null || true
    wait $EDGE0_PID 2>/dev/null || true
    
    log "    Run complete. Collecting logs..."
    
    # Collect JSONL logs from all VMs (sink could be on any device)
    for IP in $CLOUD0_IP $MID0_IP $EDGE0_IP; do
        scp_from $IP "~/ttpython/runtime_log_${run_label}.jsonl" \
            "${LOGS_DIR}/runtime_log_${run_label}_from_$(echo $IP | tr '.' '_').jsonl"
    done
    
    # Collect RTM log for debugging
    scp_from $CLOUD0_IP "/tmp/rtm_${run_label}.log" "${LOGS_DIR}/debug_rtm_${run_label}.log"
    scp_from $CLOUD0_IP "/tmp/cloud0_${run_label}.log" "${LOGS_DIR}/debug_cloud0_${run_label}.log"
    scp_from $MID0_IP "/tmp/mid0_${run_label}.log" "${LOGS_DIR}/debug_mid0_${run_label}.log"
    scp_from $EDGE0_IP "/tmp/edge0_${run_label}.log" "${LOGS_DIR}/debug_edge0_${run_label}.log"
    
    # Merge JSONL logs (sink could be on any device)
    cat ${LOGS_DIR}/runtime_log_${run_label}_from_*.jsonl \
        2>/dev/null | sort -u > ${LOGS_DIR}/runtime_log_${run_label}.jsonl 2>/dev/null || true
    
    # Report
    local n_entries=$(wc -l < "${LOGS_DIR}/runtime_log_${run_label}.jsonl" 2>/dev/null || echo "0")
    log "    Collected $n_entries latency entries for $run_label"
    
    # Brief pause between runs to let ports free up
    log "    Cooling down (10s)..."
    sleep 10
}

run_all_deployments() {
    log "========== STEP 5: Running cluster deployments =========="
    
    mkdir -p $LOGS_DIR
    
    local total=0
    for WL in $WORKLOADS; do
        for STRAT in $STRATEGIES; do
            total=$((total + 1))
        done
    done
    
    local current=0
    for WL in $WORKLOADS; do
        for STRAT in $STRATEGIES; do
            current=$((current + 1))
            log ""
            log "[$current/$total] Deploying $WL × $STRAT"
            run_single_deployment $WL $STRAT
        done
    done
    
    log "All deployments complete."
}

# ======================== STEP 6: CONSOLIDATE RESULTS ========================

consolidate_results() {
    log "========== STEP 6: Consolidating results =========="
    
    local output_dir="evals/results/${TIMESTAMP}_runtime_validation"
    mkdir -p "$output_dir"
    
    # Copy analytical results
    if [ -n "$RESULTS_DIR" ] && [ -d "$RESULTS_DIR" ]; then
        cp -r "$RESULTS_DIR" "$output_dir/analytical/"
    fi
    
    # Copy runtime logs
    cp -r "$LOGS_DIR" "$output_dir/runtime/"
    
    # Copy mappings
    cp -r "$MAPPINGS_DIR" "$output_dir/mappings/"
    
    # Generate summary
    log "Generating summary..."
    
    python << PYEOF
import json, os, statistics

logs_dir = "$LOGS_DIR"
strategies = "$STRATEGIES".split()
workloads = "$WORKLOADS".split()

print("=" * 90)
print("  RUNTIME VALIDATION SUMMARY")
print("=" * 90)
print(f"  {'Workload':<15} {'Strategy':<18} {'Predicted(ms)':<16} {'Actual Med(ms)':<16} {'N':<6} {'Ratio'}")
print(f"  {'-' * 80}")

results = []
for wl in workloads:
    for strat in strategies:
        run_label = f"{wl}_{strat}"
        logfile = os.path.join(logs_dir, f"runtime_log_{run_label}.jsonl")
        
        if not os.path.exists(logfile):
            print(f"  {wl:<15} {strat:<18} {'—':<16} {'NO DATA':<16} {'—':<6} {'—'}")
            continue
        
        entries = []
        with open(logfile) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        entries.append(json.loads(line))
                    except:
                        continue
        
        latencies = [e['latency_ms'] for e in entries if 'latency_ms' in e]
        if not latencies:
            print(f"  {wl:<15} {strat:<18} {'—':<16} {'EMPTY':<16} {'0':<6} {'—'}")
            continue
        
        predicted = entries[0].get('predicted_ms', 0)
        median_lat = statistics.median(latencies)
        ratio = median_lat / predicted if predicted > 0 else 0
        
        print(f"  {wl:<15} {strat:<18} {predicted:<16.1f} {median_lat:<16.1f} {len(latencies):<6} {ratio:.2f}x")
        
        results.append({
            'workload': wl, 'strategy': strat,
            'predicted_ms': predicted, 'actual_median_ms': median_lat,
            'n': len(latencies), 'ratio': ratio
        })

# Per-workload ranking
print()
for wl in workloads:
    wl_results = [r for r in results if r['workload'] == wl and r['n'] > 0]
    if len(wl_results) > 1:
        print(f"  {wl} — Actual ranking:")
        for i, r in enumerate(sorted(wl_results, key=lambda x: x['actual_median_ms'])):
            marker = " <-- BEST" if i == 0 else ""
            print(f"    {i+1}. {r['strategy']}: {r['actual_median_ms']:.1f}ms{marker}")
        print()

# Save JSON
with open(os.path.join("$output_dir", "runtime_validation_summary.json"), 'w') as f:
    json.dump(results, f, indent=2)

print("=" * 90)
PYEOF
    
    log "Results consolidated in: $output_dir"
    log ""
    log "Files:"
    ls -la "$output_dir"/runtime/runtime_log_*.jsonl 2>/dev/null | awk '{print "  " $NF " (" $5 " bytes)"}'
}

# ======================== MAIN ========================

main() {
    log "=============================================="
    log "  TTPython Full Validation Pipeline"
    log "  Timestamp: $TIMESTAMP"
    log "=============================================="
    log ""
    
    cd $(pwd)
    
    check_connectivity
    
    # If --resume flag passed, skip steps 1-4
    if [ "${1:-}" = "--resume" ]; then
        RESULTS_DIR=$(ls -td evals/results/*_placement_quality 2>/dev/null | head -1)
        log "RESUMING: Skipping compile/analytical/extract/push"
        log "Using existing results: $RESULTS_DIR"
    else
        log ""
        compile_workloads
        
        log ""
        run_analytical
        
        log ""
        extract_mappings
        
        log ""
        push_to_vms
    fi
    
    log ""
    run_all_deployments
    
    log ""
    consolidate_results
    
    log ""
    log "=============================================="
    log "  PIPELINE COMPLETE"
    log "  Analytical results: $RESULTS_DIR"
    log "  Runtime logs: $LOGS_DIR"
    log "  Consolidated: evals/results/${TIMESTAMP}_runtime_validation"
    log "=============================================="
}

# Run it
main "$@" 2>&1 | tee "validation_run_${TIMESTAMP}.log"
