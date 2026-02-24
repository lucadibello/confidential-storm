#!/usr/bin/env bash
#
# run-quick-test.sh — Quick smoke test (2 minutes, minimal scale).
#
# Purpose: Verify the topology starts, processes data, and writes output.
# NOT suitable for accuracy comparison with the paper.
#
# Scale: 10k users, 10k keys, parallelism=1, 100 micro-batches
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Quick Test (2 min, reduced scale, parallelism=1) ==="
echo ""

NUM_USERS=10000 \
NUM_KEYS=10000 \
BATCH_SIZE=5000 \
SLEEP_MS=50 \
RUNTIME_SECONDS=120 \
PARALLELISM=2 \
EXTRA_FLAGS="--test" \
"$SCRIPT_DIR/run-experiment.sh" 100 1
