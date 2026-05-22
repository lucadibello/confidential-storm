#!/usr/bin/env bash
# Checks if the host CPU supports SGX2 by running cpuid inside a minimal Docker container.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKERFILE="$SCRIPT_DIR/../../docker/Dockerfile.cpuid"
IMAGE="sgx-cpuid-check"

echo "[check-sgx2] Building cpuid image..."
docker build -f "$DOCKERFILE" -t "$IMAGE" "$(dirname "$DOCKERFILE")" --quiet

echo "[check-sgx2] Running cpuid..."
OUTPUT=$(docker run --rm --privileged "$IMAGE")
echo "$OUTPUT"

if echo "$OUTPUT" | grep -qi "sgx2 supported *= *true"; then
    echo ""
    echo "[check-sgx2] SGX2 is SUPPORTED on this machine."
    exit 0
else
    echo ""
    echo "[check-sgx2] SGX2 is NOT supported on this machine."
    exit 1
fi
