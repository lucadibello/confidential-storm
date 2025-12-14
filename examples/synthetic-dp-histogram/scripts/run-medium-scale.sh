#!/bin/bash
# Medium-scale test (10-15 minutes, 1M users/keys)

set -e

echo "Running medium-scale experiment (15 minutes, 1M users/keys)..."
./scripts/run-experiment.sh 100 1 1000000 1000000 900
