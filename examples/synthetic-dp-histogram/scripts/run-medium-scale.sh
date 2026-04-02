#!/usr/bin/env bash
#
# run-medium-scale.sh - Medium-scale validation run.
#
# Scale: 1M users, 1M keys, parallelism=4, 100 time-steps
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Medium Scale (1M users/keys) ==="
echo ""

NUM_USERS=1000000 \
NUM_KEYS=1000000 \
RUNTIME_SECONDS=1800 \
PARALLELISM=4 \
GROUND_TRUTH=false \
"$SCRIPT_DIR/run-experiment.sh" 100 1
