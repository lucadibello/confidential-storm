#!/usr/bin/env bash
#
# run-experiment.sh — Generic experiment runner for the Synthetic DP Histogram benchmark.
#
# Usage:
#   ./scripts/run-experiment.sh <max_time_steps> <run_id> [options]
#
# Required arguments:
#   max_time_steps   Number of micro-batches (e.g. 100 or 1000)
#   run_id           Identifier for this run (used in output filename)
#
# Options (override defaults via environment variables):
#   NUM_USERS         Number of unique users          (default: 10000000)
#   NUM_KEYS          Number of distinct keys          (default: 1000000)
#   BATCH_SIZE        Records emitted per batch        (default: 20000)
#   SLEEP_MS          Delay between batches in ms      (default: 100)
#   RUNTIME_SECONDS   Total topology runtime           (default: 120)
#   PARALLELISM       Bolt parallelism hint            (default: 8)
#   SEED              Random seed                      (default: 42)
#   MU                Key selection threshold           (default: 50)
#   EXTRA_FLAGS       Additional flags passed to the topology (e.g. "--test")
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
JAR="$PROJECT_DIR/host/target/synthetic-dp-histogram-host-1.0-SNAPSHOT.jar"
TOPOLOGY_CLASS="ch.usi.inf.examples.synthetic_dp.host.SyntheticTopology"

# --- Required arguments ---
if [ $# -lt 2 ]; then
    echo "Usage: $0 <max_time_steps> <run_id> [options]"
    echo ""
    echo "Example: $0 100 1"
    echo "Example: NUM_USERS=100000 PARALLELISM=1 $0 100 1"
    exit 1
fi

MAX_TIME_STEPS="$1"
RUN_ID="$2"
shift 2

# --- Configurable parameters (with defaults) ---
NUM_USERS="${NUM_USERS:-10000000}"
NUM_KEYS="${NUM_KEYS:-1000000}"
BATCH_SIZE="${BATCH_SIZE:-20000}"
SLEEP_MS="${SLEEP_MS:-100}"
RUNTIME_SECONDS="${RUNTIME_SECONDS:-120}"
PARALLELISM="${PARALLELISM:-8}"
SEED="${SEED:-42}"
MU="${MU:-50}"
EXTRA_FLAGS="${EXTRA_FLAGS:-}"

# --- Verify JAR exists ---
if [ ! -f "$JAR" ]; then
    echo "ERROR: JAR not found at $JAR"
    echo "Run 'make build' first."
    exit 1
fi

# --- Compute local-ttl (runtime + 30s buffer for startup/shutdown) ---
LOCAL_TTL=$(( RUNTIME_SECONDS + 30 ))

# --- Print configuration ---
echo "=============================================="
echo " Synthetic DP Histogram — Run $RUN_ID"
echo "=============================================="
echo " Users:           $NUM_USERS"
echo " Keys:            $NUM_KEYS"
echo " Micro-batches:   $MAX_TIME_STEPS"
echo " Runtime:         ${RUNTIME_SECONDS}s ($(echo "scale=1; $RUNTIME_SECONDS / 60" | bc)m)"
echo " Parallelism:     $PARALLELISM"
echo " Batch size:      $BATCH_SIZE"
echo " Sleep:           ${SLEEP_MS}ms"
echo " Seed:            $SEED"
echo " Mu:              $MU"
echo " Output:          data/synthetic-report-run${RUN_ID}.txt"
if [ -n "$EXTRA_FLAGS" ]; then
echo " Extra flags:     $EXTRA_FLAGS"
fi
echo "=============================================="
echo ""

# --- Run ---
cd "$PROJECT_DIR"

sudo storm local \
    --local-ttl "$LOCAL_TTL" \
    "$JAR" \
    "$TOPOLOGY_CLASS" \
    -- \
    --num-users "$NUM_USERS" \
    --num-keys "$NUM_KEYS" \
    --batch-size "$BATCH_SIZE" \
    --sleep-ms "$SLEEP_MS" \
    --runtime-seconds "$RUNTIME_SECONDS" \
    --run-id "$RUN_ID" \
    --seed "$SEED" \
    --mu "$MU" \
    --max-time-steps "$MAX_TIME_STEPS" \
    --parallelism "$PARALLELISM" \
    $EXTRA_FLAGS

echo ""
echo "Run $RUN_ID complete. Results: data/synthetic-report-run${RUN_ID}.txt"
