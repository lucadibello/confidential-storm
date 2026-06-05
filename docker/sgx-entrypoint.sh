#!/bin/bash
# SGX device-access shim, layered in front of the stock Storm entrypoint.
set -e

if [ "$(id -u)" = "0" ]; then
  for dev in /dev/sgx/enclave /dev/sgx/provision /dev/sgx_enclave /dev/sgx_provision; do
    [ -e "$dev" ] || continue
    # Prefer group-scoped access; fall back to world rw if the sgx group or the
    # chmod is unavailable for some reason.  Never abort startup on failure.
    if chgrp sgx "$dev" 2>/dev/null && chmod 0660 "$dev" 2>/dev/null; then
      :
    else
      chmod o+rw "$dev" 2>/dev/null || true
    fi
  done
fi

exec /docker-entrypoint.sh "$@"
