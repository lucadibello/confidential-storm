#!/bin/bash
# This runs a scaled-down version to verify the infrastructure works

set -e

echo "Running quick test (5 minutes, scaled-down paper settings)..."

MAX_TIME_STEPS=100       # micro-batches
RUN_ID=1
NUM_USERS=10000          # 1/1000 of paper's 10M
NUM_KEYS=1000            # 1/1000 of paper's 1M
DURATION_SECONDS=300     # 5 minutes wall time
BATCH_SIZE=5000          # events per tick (~500k total over 100 ticks)
SLEEP_MS=25              # small pause between batches
MU=20                    # lower threshold to match the smaller scale

# run experiment with specified parameters
./scripts/run-experiment.sh \
  ${MAX_TIME_STEPS} \
  ${RUN_ID} \
  ${NUM_USERS} \
  ${NUM_KEYS} \
  ${DURATION_SECONDS} \
  ${BATCH_SIZE} \
  ${SLEEP_MS} \
  ${MU}
