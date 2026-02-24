#!/usr/bin/env bash
#
# run-all-experiments.sh — Run all 6 paper-scale experiments.
#
# Produces:
#   Runs 1-3: 100 micro-batches  (3 runs, averaged for Table 1 comparison)
#   Runs 4-6: 1000 micro-batches (3 runs, averaged for Table 1 comparison)
#
# Total estimated time: 20-36 hours on a 30-core SGX server.
#
# Each run uses a different random seed to ensure statistical independence.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "========================================================"
echo " Synthetic DP Histogram — Full Experiment Suite"
echo " 6 runs: 3 × 100 micro-batches + 3 × 1000 micro-batches"
echo "========================================================"
echo ""

SEEDS=(42 137 256)

# --- 100 micro-batches (runs 1-3) ---
for i in 0 1 2; do
    RUN_ID=$((i + 1))
    echo ""
    echo ">>> Run $RUN_ID / 6: 100 micro-batches (seed=${SEEDS[$i]})"
    echo ""
    SEED="${SEEDS[$i]}" "$SCRIPT_DIR/run-paper-scale.sh" 100 "$RUN_ID"
    echo ""
    echo "<<< Run $RUN_ID / 6 complete"
    echo ""
done

# --- 1000 micro-batches (runs 4-6) ---
for i in 0 1 2; do
    RUN_ID=$((i + 4))
    echo ""
    echo ">>> Run $RUN_ID / 6: 1000 micro-batches (seed=${SEEDS[$i]})"
    echo ""
    SEED="${SEEDS[$i]}" "$SCRIPT_DIR/run-paper-scale.sh" 1000 "$RUN_ID"
    echo ""
    echo "<<< Run $RUN_ID / 6 complete"
    echo ""
done

echo ""
echo "========================================================"
echo " All 6 runs complete."
echo " Results in: data/synthetic-report-run{1..6}.txt"
echo ""
echo " Analyze with:"
echo "   python3 scripts/analyze-results.py --config both"
echo "========================================================"
