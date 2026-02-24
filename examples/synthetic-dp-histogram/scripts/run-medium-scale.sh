#!/usr/bin/env bash
#
# run-medium-scale.sh — Medium-scale validation run (15 minutes).
#
# Purpose: Validate correctness at a meaningful scale before committing
# to full paper-scale runs. Uses full parallelism.
#
# Scale: 1M users, 1M keys, parallelism=8, 100 micro-batches
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Medium Scale (15 min, 1M users/keys) ==="
echo ""

NUM_USERS=1000000 \
NUM_KEYS=1000000 \
BATCH_SIZE=10000 \
SLEEP_MS=50 \
RUNTIME_SECONDS=900 \
PARALLELISM=4 \
"$SCRIPT_DIR/run-experiment.sh" 100 1
