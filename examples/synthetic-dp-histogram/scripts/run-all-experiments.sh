#!/bin/bash
# Run all paper experiments (3 runs each for 100 and 1000 micro-batches)
# This reproduces Table 1 from the paper Section 5.1

set -e

echo "============================================"
echo "Running ALL paper experiments"
echo "This will reproduce Table 1 from Section 5.1"
echo "WARNING: This takes 12-18 hours total"
echo "============================================"

# 100 micro-batches - 3 runs
for run in 1 2 3; do
    echo ""
    echo "=== 100 micro-batches, Run ${run}/3 ==="
    ./scripts/run-paper-scale.sh 100 ${run}
    echo "Completed run ${run} for 100 micro-batches"
    sleep 30  # Brief pause between runs
done

# 1000 micro-batches - 3 runs
for run in 4 5 6; do
    echo ""
    echo "=== 1000 micro-batches, Run $((run-3))/3 ==="
    ./scripts/run-paper-scale.sh 1000 ${run}
    echo "Completed run $((run-3)) for 1000 micro-batches"
    sleep 30  # Brief pause between runs
done

echo ""
echo "============================================"
echo "All experiments completed!"
echo "Results in data/ directory:"
echo "  100 micro-batches: synthetic-report-run{1,2,3}.txt"
echo "  1000 micro-batches: synthetic-report-run{4,5,6}.txt"
echo ""
echo "Run analysis: python3 scripts/analyze-results.py"
echo "============================================"
