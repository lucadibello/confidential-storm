#!/bin/bash
# Run synthetic DP histogram experiment with configurable parameters
# Usage: ./run-experiment.sh <max_time_steps> <run_id> [num_users] [num_keys] [runtime_seconds]
#
# Paper configurations:
#   - 100 micro-batches: ./run-experiment.sh 100 1 10000000 1000000 7200
#   - 1000 micro-batches: ./run-experiment.sh 1000 1 10000000 1000000 7200

set -e

# Parse arguments
MAX_TIME_STEPS=${1:-100}
RUN_ID=${2:-1}
NUM_USERS=${3:-10000000}
NUM_KEYS=${4:-1000000}
RUNTIME_SECONDS=${5:-7200}  # 2 hours default for full paper scale

# Derived parameters
BATCH_SIZE=${6:-20000}
SLEEP_MS=${7:-50}
MU=${8:-50}

# Choose tick frequency so we produce roughly MAX_TIME_STEPS micro-batches within the runtime.
# Ceil division to avoid under-shooting the requested steps.
TICK_SECONDS=$(( (RUNTIME_SECONDS + MAX_TIME_STEPS - 1) / MAX_TIME_STEPS ))
if [ "${TICK_SECONDS}" -lt 1 ]; then
  TICK_SECONDS=1
fi
MIN_RUNTIME=$(( TICK_SECONDS * MAX_TIME_STEPS + 1 ))
if [ "${RUNTIME_SECONDS}" -lt "${MIN_RUNTIME}" ]; then
  echo "Extending runtime to ${MIN_RUNTIME}s so ${MAX_TIME_STEPS} ticks can occur at tick=${TICK_SECONDS}s"
  RUNTIME_SECONDS=${MIN_RUNTIME}
fi

echo "============================================"
echo "Synthetic DP Histogram Benchmark"
echo "============================================"
echo "Configuration:"
echo "  Max Time Steps: ${MAX_TIME_STEPS}"
echo "  Run ID: ${RUN_ID}"
echo "  Num Users: ${NUM_USERS}"
echo "  Num Keys: ${NUM_KEYS}"
echo "  Runtime: ${RUNTIME_SECONDS}s ($(echo "scale=2; ${RUNTIME_SECONDS}/60" | bc) min)"
echo "  Batch Size: ${BATCH_SIZE}"
echo "  Sleep (ms): ${SLEEP_MS}"
echo "  Mu (threshold): ${MU}"
echo "  Tick seconds: ${TICK_SECONDS}"
echo "============================================"

# Build if needed
if [ ! -f "host/target/synthetic-dp-histogram-host-1.0-SNAPSHOT.jar" ]; then
    echo "Building project..."
    make build
fi

# Prepare output directory
mkdir -p data

# Run topology
echo "Starting topology (this may take a while)..."
sudo storm local --local-ttl ${RUNTIME_SECONDS} \
  -Dsynthetic.num.users=${NUM_USERS} \
  -Dsynthetic.num.keys=${NUM_KEYS} \
  -Dsynthetic.batch.size=${BATCH_SIZE} \
  -Dsynthetic.sleep.ms=${SLEEP_MS} \
  -Dsynthetic.runtime.seconds=${RUNTIME_SECONDS} \
  -Dsynthetic.tick.seconds=${TICK_SECONDS} \
  -Dsynthetic.run.id=${RUN_ID} \
  -Ddp.max.time.steps=${MAX_TIME_STEPS} \
  -Ddp.mu=${MU} \
  host/target/synthetic-dp-histogram-host-1.0-SNAPSHOT.jar \
  ch.usi.inf.examples.synthetic_dp.host.SyntheticTopology \
  --num-users ${NUM_USERS} \
  --num-keys ${NUM_KEYS} \
  --batch-size ${BATCH_SIZE} \
  --sleep-ms ${SLEEP_MS} \
  --run-id ${RUN_ID} \
  --runtime-seconds ${RUNTIME_SECONDS}

echo "============================================"
echo "Experiment completed!"
echo "Results: data/synthetic-report-run${RUN_ID}.txt"
echo "============================================"
