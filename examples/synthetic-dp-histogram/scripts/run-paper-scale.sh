#!/usr/bin/env bash
#
# run-paper-scale.sh — Full paper-scale run (Section 5.1).
#
# Usage:
#   ./scripts/run-paper-scale.sh <max_time_steps> <run_id>
#
# Examples:
#   ./scripts/run-paper-scale.sh 100 1    # 100 micro-batches, run 1
#   ./scripts/run-paper-scale.sh 1000 4   # 1000 micro-batches, run 4
#
# Scale: 10M users, 1M keys, parallelism=8
#
# Runtime guidance (SGX enclave, 30-core server):
#   100  micro-batches: ~60-90 minutes
#   1000 micro-batches: ~6-10 hours (Algorithm 3 is CPU-intensive)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ $# -lt 2 ]; then
    echo "Usage: $0 <max_time_steps> <run_id>"
    echo ""
    echo "Examples:"
    echo "  $0 100 1     # 100 micro-batches, run 1"
    echo "  $0 1000 4    # 1000 micro-batches, run 4"
    exit 1
fi

MAX_TIME_STEPS="$1"
RUN_ID="$2"

# Runtime scales with micro-batches:
#   100  steps -> 1 hour  (3600s)
#   1000 steps -> 6 hours (21600s)
if [ "$MAX_TIME_STEPS" -ge 1000 ]; then
    RUNTIME=21600
else
    RUNTIME=3600
fi

echo "=== Paper Scale (10M users, 1M keys, ${MAX_TIME_STEPS} micro-batches) ==="
echo ""

NUM_USERS=10000000 \
NUM_KEYS=1000000 \
RUNTIME_SECONDS="$RUNTIME" \
GROUND_TRUTH=false \
"$SCRIPT_DIR/run-experiment.sh" "$MAX_TIME_STEPS" "$RUN_ID"
