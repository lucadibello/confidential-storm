#!/usr/bin/env bash
# Downloads, compiles, and runs Intel's test-sgx.c to check for SGX2 support.
set -euo pipefail

SGX_BASE_URL="https://raw.githubusercontent.com/ayeks/SGX-hardware/refs/heads/master"
TMPDIR_SGX=$(mktemp -d)
trap 'rm -rf "$TMPDIR_SGX"' EXIT

SRC="$TMPDIR_SGX/test-sgx.c"
HDR="$TMPDIR_SGX/test-sgx.h"
BIN="$TMPDIR_SGX/test-sgx"

echo "[check-sgx2] Downloading test-sgx.c and test-sgx.h..."
curl -fsSL "$SGX_BASE_URL/test-sgx.c" -o "$SRC"
curl -fsSL "$SGX_BASE_URL/test-sgx.h" -o "$HDR"

echo "[check-sgx2] Compiling..."
gcc "$SRC" -o "$BIN"

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
