#!/usr/bin/env bash
# run-scaling-benchmark.sh — strong-scaling then weak-scaling microbatch
# benchmarks for both baseline (no SGX) and confidential (SGX enclave) modes.
#
# Usage (from project root):
#   sudo bash benchmark/scripts/run-scaling-benchmark.sh

set -euo pipefail

# ---- Cluster topology --------------------------------------------------
MASTER_HOST="${MASTER_HOST:-10.233.26.43}"
SUPERVISOR_HOSTS="${SUPERVISOR_HOSTS:-10.233.26.41,10.233.26.42,10.233.26.45}"
SCALE_VALUES="${SCALE_VALUES:-3}"

# ---- SSH / container config --------------------------------------------
SSH_USER="${SSH_USER:-luca}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_ed25519}"
SSH_PORT="${SSH_PORT:-22}"
CONTAINER_USER="${CONTAINER_USER:-dev}"
CONTAINER_PORT="${CONTAINER_PORT:-2222}"
UI_PORT="${UI_PORT:-18080}"

# ---- Scaling sweep -----------------------------------------------------
# Parallelism values used for both strong and weak sweeps.
PARALLELISMS="${PARALLELISMS:-4 8 16}"

# Strong scaling: fixed total batch sizes (GB) swept across all parallelisms.
STRONG_BATCH_SIZES_GB="${STRONG_BATCH_SIZES_GB:-1,2,5}"

# Weak scaling: per-worker batch size (GB). Total = per_worker * parallelism.
# e.g. 0.125 GB/worker × (4,8,16) workers → 0.5, 1.0, 2.0 GB total.
WEAK_PER_WORKER_GB="${WEAK_PER_WORKER_GB:-0.125}"

# ---- Repetitions / timing ----------------------------------------------
RUNS_PER_SIZE="${RUNS_PER_SIZE:-3}"
HOURS_PER_GB="${HOURS_PER_GB:-2.0}"

# ---- Archive labels ----------------------------------------------------
LABEL_STRONG="${LABEL_STRONG:-scaling-strong}"
LABEL_WEAK="${LABEL_WEAK:-scaling-weak}"

# ---- Paths -------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GRID_SEARCH="$PROJECT_ROOT/examples/scripts/run-grid-search.py"

# ---- Helpers -----------------------------------------------------------
_run() {
  local label="$1"
  shift
  echo ""
  echo "========================================================"
  echo "  $label"
  echo "========================================================"
  sudo JAVA_TOOL_OPTIONS="-XX:-UseContainerSupport" \
    python3 "$GRID_SEARCH" \
    --master-host "$MASTER_HOST" \
    --supervisor-hosts "$SUPERVISOR_HOSTS" \
    --scale-values "$SCALE_VALUES" \
    --ssh-user "$SSH_USER" \
    --ssh-key "$SSH_KEY" \
    --ssh-port "$SSH_PORT" \
    --num-users 10000000 \
    --num-keys 1000000 \
    --mu 0 \
    --container-user "$CONTAINER_USER" \
    --container-port "$CONTAINER_PORT" \
    --ui-port "$UI_PORT" \
    --ground-truth false \
    --hours-per-gb "$HOURS_PER_GB" \
    --runs-per-size "$RUNS_PER_SIZE" \
    "$@"
}

# ---- Banner ------------------------------------------------------------
echo "========================================================"
echo "  Scaling Benchmark"
echo "  Master:       $MASTER_HOST"
echo "  Supervisors:  $SUPERVISOR_HOSTS  (scale-values=$SCALE_VALUES)"
echo "  Parallelisms: $PARALLELISMS"
echo "  Strong sizes: $STRONG_BATCH_SIZES_GB GB  (runs/size=$RUNS_PER_SIZE)"
echo "  Weak GB/wkr:  $WEAK_PER_WORKER_GB GB  (runs/size=$RUNS_PER_SIZE)"
echo "========================================================"

# ========================================================================
# Phase 1 — Strong scaling
# Fixed total batch size; vary parallelism to see throughput increase.
# ========================================================================

_run "Strong scaling — Baseline (no SGX)" \
  --mode microbatch-baseline \
  --parallelisms $PARALLELISMS \
  --batch-sizes-gb "$STRONG_BATCH_SIZES_GB" \
  --label "$LABEL_STRONG"

_run "Strong scaling — Enclave (SGX)" \
  --mode microbatch-enclave \
  --parallelisms $PARALLELISMS \
  --batch-sizes-gb "$STRONG_BATCH_SIZES_GB" \
  --label "$LABEL_STRONG"

# ========================================================================
# Phase 2 — Weak scaling
# Per-worker batch size is fixed; total batch grows with parallelism.
# ========================================================================

_run "Weak scaling — Baseline (no SGX)" \
  --mode microbatch-baseline \
  --no-build \
  --parallelisms $PARALLELISMS \
  --per-worker-batch-gb "$WEAK_PER_WORKER_GB" \
  --label "$LABEL_WEAK"

_run "Weak scaling — Enclave (SGX)" \
  --mode microbatch-enclave \
  --no-build \
  --parallelisms $PARALLELISMS \
  --per-worker-batch-gb "$WEAK_PER_WORKER_GB" \
  --label "$LABEL_WEAK"

echo ""
echo "========================================================"
echo "  All scaling benchmarks complete."
echo "========================================================"
