#!/bin/bash
# ============================================================================
# TTPython Cluster — Quick Reference & Setup
# ============================================================================
# Save this at your repo root. Run commands from WSL.
# ============================================================================

# ======================== CLUSTER IPS ========================
# edge0:   <EDGE0_IP>  (1 vCPU, 1GB,  London, Shared)
# mid0:    <MID0_IP>  (2 vCPU, 4GB,  London, Shared)
# cloud0:  <CLOUD0_IP> (4 vCPU, 8GB,  London, Dedicated CPU-Optimized)

# ======================== SSH LOGIN ========================
# Terminal 1 (RTM — runs on cloud0):
#   ssh root@<CLOUD0_IP>
#
# Terminal 2 (cloud0 device):
#   ssh root@<CLOUD0_IP>
#
# Terminal 3 (mid0 device):
#   ssh root@<MID0_IP>
#
# Terminal 4 (edge0 device):
#   ssh root@<EDGE0_IP>

# ======================== QUICK CONNECTIVITY CHECK ========================
check_cluster() {
    echo "Checking cluster connectivity..."
    echo -n "  edge0 (<EDGE0_IP>): "
    ping -c 1 -W 2 <EDGE0_IP> 2>/dev/null | grep "time=" | awk '{print $7}' || echo "UNREACHABLE"
    echo -n "  mid0  (<MID0_IP>): "
    ping -c 1 -W 2 <MID0_IP> 2>/dev/null | grep "time=" | awk '{print $7}' || echo "UNREACHABLE"
    echo -n "  cloud0 (<CLOUD0_IP>): "
    ping -c 1 -W 2 <CLOUD0_IP> 2>/dev/null | grep "time=" | awk '{print $7}' || echo "UNREACHABLE"
}

# ======================== PUSH CODE TO ALL VMS ========================
push_code() {
    echo "Pushing code to all VMs..."
    for IP in <EDGE0_IP> <MID0_IP> <CLOUD0_IP>; do
        echo "  → $IP"
        rsync -avz --quiet \
            --exclude='*.pdf' --exclude='*.docx' --exclude='*.pickle' \
            --exclude='*.png' --exclude='riot/' --exclude='.git/' \
            --exclude='__pycache__/' --exclude='runtime_logs/' \
            --exclude='evals/results/' \
            ./ root@$IP:~/ttpython/
    done
    echo "Done."
}

# ======================== PUSH PICKLES TO RTM (cloud0) ========================
push_pickles() {
    echo "Pushing pickles to cloud0 (RTM)..."
    scp ./output/*.pickle root@<CLOUD0_IP>:~/ttpython/output/
    echo "Done."
}

# ======================== PUSH MAPPINGS TO RTM (cloud0) ========================
push_mappings() {
    echo "Pushing mappings to cloud0 (RTM)..."
    scp -r evals/runtime_mappings/* root@<CLOUD0_IP>:~/ttpython/mappings/
    echo "Done."
}

# ======================== COLLECT LOGS FROM ALL VMS ========================
collect_logs() {
    mkdir -p runtime_logs
    echo "Collecting runtime logs..."
    for IP in <EDGE0_IP> <MID0_IP> <CLOUD0_IP>; do
        echo "  ← $IP"
        scp root@$IP:~/ttpython/runtime_log_*.jsonl ./runtime_logs/ 2>/dev/null
    done
    # Also grab local logs
    mv runtime_log_*.jsonl ./runtime_logs/ 2>/dev/null
    echo "Logs in ./runtime_logs/:"
    ls -la runtime_logs/*.jsonl 2>/dev/null || echo "  (none found)"
}

# ======================== COMPILE ALL WORKLOADS ========================
compile_all() {
    DEPLOYMENT=${1:-evals/deployments/cluster_c2_heterogeneous.yaml}
    echo "Compiling all workloads with deployment: $DEPLOYMENT"
    for WL in eval_etl eval_stats eval_pred eval_train; do
        echo "  Compiling $WL..."
        python compile.py evals/workloads/${WL}.py -o ./output/ -g --deployment $DEPLOYMENT 2>&1 | grep "SQs analyzed\|Compilation successful"
    done
    echo "Done."
}

# ======================== USAGE ========================
# Source this file, then call functions:
#   source cluster.sh
#   check_cluster
#   push_code
#   compile_all
#   push_pickles
#   collect_logs
