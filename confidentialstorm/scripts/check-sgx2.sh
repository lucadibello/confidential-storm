#!/usr/bin/env bash
# Pre-flight check: verifies SGX2 support and all prerequisites for the devcontainer.
# Checks are derived from devcontainer.json (devices, network) and the base Dockerfile.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKERFILE="$SCRIPT_DIR/../../docker/Dockerfile.cpuid"
IMAGE="sgx-cpuid-check"

PASS=0
FAIL=0

_ok()      { printf "  %-50s \033[32mPASS\033[0m%s\n" "$1" "${2:+  ($2)}"; PASS=$((PASS+1)); }
_fail()    { printf "  %-50s \033[31mFAIL\033[0m%s\n" "$1" "${2:+  ($2)}"; FAIL=$((FAIL+1)); }
_section() { printf "\n── %s\n" "$1"; }

# ── 1. CPU / SGX hardware (via Docker + cpuid) ───────────────────────────────
_section "CPU / SGX hardware"

printf "  Building cpuid image (first run may take a moment)...\n"
if docker build -f "$DOCKERFILE" -t "$IMAGE" "$(dirname "$DOCKERFILE")" --quiet >/dev/null 2>&1; then
    CPUID_OUT=$(docker run --rm --privileged "$IMAGE" 2>/dev/null || true)

    if echo "$CPUID_OUT" | grep -qi "sgx2 supported *= *true"; then
        _ok "SGX2 CPU support"
    else
        _fail "SGX2 CPU support" "not reported by cpuid"
    fi

    if echo "$CPUID_OUT" | grep -qi "sgx1 supported *= *true"; then
        _ok "SGX1 CPU support"
    else
        _fail "SGX1 CPU support" "not reported by cpuid"
    fi
else
    _fail "cpuid Docker image build" "docker build failed"
fi

# ── 2. SGX kernel devices (mapped by devcontainer.json) ──────────────────────
_section "SGX kernel devices"

for dev in /dev/sgx_enclave /dev/sgx_provision /dev/sgx_vepc; do
    if [ -c "$dev" ]; then
        _ok "$dev"
    else
        _fail "$dev" "character device not found"
    fi
done

# ── 3. Intel SGX SDK ─────────────────────────────────────────────────────────
_section "Intel SGX SDK"

if [ -f /opt/intel/sgxsdk/version ]; then
    _ok "Intel SGX SDK" "$(cat /opt/intel/sgxsdk/version)"
elif command -v sgx_edger8r &>/dev/null; then
    _ok "Intel SGX SDK (edger8r in PATH)" "$(sgx_edger8r --version 2>&1 | head -1)"
else
    _fail "Intel SGX SDK" "/opt/intel/sgxsdk not found and sgx_edger8r not in PATH"
fi

# ── 4. SGX PSW + DCAP packages ───────────────────────────────────────────────
_section "SGX PSW / DCAP packages"

for pkg in libsgx-enclave-common libsgx-launch libsgx-epid libsgx-quote-ex libsgx-dcap-ql; do
    if dpkg -l "$pkg" 2>/dev/null | grep -q "^ii"; then
        ver=$(dpkg -l "$pkg" 2>/dev/null | awk '/^ii/{print $3}' | head -1)
        _ok "deb: $pkg" "$ver"
    else
        _fail "deb: $pkg" "not installed"
    fi
done

# ── 5. SGX services ──────────────────────────────────────────────────────────
_section "SGX services"

if systemctl is-active --quiet aesmd 2>/dev/null; then
    _ok "AESMD service" "active"
elif [ -S /var/run/aesmd/aesm.socket ]; then
    _ok "AESMD socket" "socket present (service status unknown)"
else
    _fail "AESMD service" "not running — sudo systemctl start aesmd"
fi

# ── 6. Docker ────────────────────────────────────────────────────────────────
_section "Docker"

if docker info &>/dev/null; then
    _ok "Docker daemon" "$(docker version --format '{{.Server.Version}}' 2>/dev/null)"
else
    _fail "Docker daemon" "not reachable — is Docker running?"
fi

if docker network inspect storm-net &>/dev/null; then
    _ok "Docker network: storm-net"
else
    _fail "Docker network: storm-net" "create with: docker network create storm-net"
fi

# ── Summary ──────────────────────────────────────────────────────────────────
printf "\n%s\n" "────────────────────────────────────────────────────────"
printf "  Passed: \033[32m%d\033[0m   Failed: \033[31m%d\033[0m\n" "$PASS" "$FAIL"
printf "%s\n" "────────────────────────────────────────────────────────"

if [ "$FAIL" -gt 0 ]; then
    printf "  One or more checks failed — fix them before launching the devcontainer.\n\n"
    exit 1
fi
printf "  All checks passed — the devcontainer should start successfully.\n\n"
exit 0
