#!/usr/bin/env bash
# Downloads, compiles, and runs Intel's test-sgx.c to check for SGX2 support.
set -euo pipefail

SGX_REPO_URL="https://github.com/ayeks/SGX-hardware.git"
TMPDIR_SGX=$(mktemp -d)
trap 'rm -rf "$TMPDIR_SGX"' EXIT

REPO_DIR="$TMPDIR_SGX/SGX-hardware"
BIN="$TMPDIR_SGX/test-sgx"

echo "[check-sgx2] Cloning SGX-hardware repository..."
git clone --depth 1 "$SGX_REPO_URL" "$REPO_DIR"

echo "[check-sgx2] Compiling..."
gcc "$REPO_DIR"/*.c -o "$BIN"

echo "[check-sgx2] Running..."
OUTPUT=$("$BIN" 2>&1)
echo "$OUTPUT"

if echo "$OUTPUT" | grep -q "sgx 2 supported: 1"; then
    echo ""
    echo "[check-sgx2] SGX2 is SUPPORTED on this machine."
    exit 0
else
    echo ""
    echo "[check-sgx2] SGX2 is NOT supported on this machine."
    exit 1
fi
