#!/bin/bash
# Full paper-scale experiment (1-2 hours, 10M users/1M keys)
# Usage: ./run-paper-scale.sh <max_time_steps> <run_id>
# Example for 100 micro-batches: ./run-paper-scale.sh 100 1
# Example for 1000 micro-batches: ./run-paper-scale.sh 1000 1

set -e

MAX_TIME_STEPS=${1:-100}
RUN_ID=${2:-1}

echo "Running FULL PAPER-SCALE experiment (10M users, 1M keys)..."
echo "This will take 1-2 hours depending on hardware."
echo "Configuration: ${MAX_TIME_STEPS} micro-batches, run ${RUN_ID}"
echo ""

# Paper specifications: 10M users, 1M keys
# Runtime: 2 hours to ensure all data is processed
./scripts/run-experiment.sh ${MAX_TIME_STEPS} ${RUN_ID} 10000000 1000000 7200
