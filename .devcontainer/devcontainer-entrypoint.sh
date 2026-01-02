#!/usr/bin/env bash
set -euo pipefail

log() {
  echo "[entrypoint] $*"
}

REMOTE_USER="${DEVCONTAINER_USER:-${REMOTE_USER:-dev}}"
if ! id -u "${REMOTE_USER}" >/dev/null 2>&1; then
  log "remote user '${REMOTE_USER}' not found; defaulting to root"
  REMOTE_USER="root"
fi

# Validate sshd config if available
if command -v sshd >/dev/null 2>&1; then
  if ! sshd -t 2>/dev/null; then
    log "sshd configuration test failed" >&2
    exit 1
  fi
fi

# Setup git config for the dev user
if [ -n "${GIT_NAME:-}" ] && [ -n "${GIT_EMAIL:-}" ]; then
  sudo -u "$REMOTE_USER" git config --global user.name "$GIT_NAME"
  sudo -u "$REMOTE_USER" git config --global user.email "$GIT_EMAIL"
  log "git config set for ${REMOTE_USER}"
fi

# Ensure .ssh directory has correct ownership
if [ -d "/home/${REMOTE_USER}/.ssh" ]; then
  chown -R "${REMOTE_USER}:${REMOTE_USER}" "/home/${REMOTE_USER}/.ssh" 2>/dev/null || true
fi

# Start AESM service (Intel SGX)
if [ -x /opt/intel/sgx-aesm-service/aesm/aesm_service ]; then
  log "starting AESM service"
  # Ensure runtime directory exists with correct permissions
  mkdir -p /var/run/aesmd
  chown aesmd:aesmd /var/run/aesmd
  chmod 755 /var/run/aesmd

  # Start AESM service in background
  LD_LIBRARY_PATH=/opt/intel/sgx-aesm-service/aesm /opt/intel/sgx-aesm-service/aesm/aesm_service &

  # Wait briefly for service to start
  sleep 2

  # Verify service is running
  if pgrep -x aesm_service >/dev/null 2>&1; then
    log "AESM service started successfully"
  else
    log "WARNING: AESM service failed to start" >&2
  fi
fi

# Bootstrap headless Neovim watchdog script
cat >/usr/local/bin/nvim-server.sh <<'EOS'
#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  if [[ -n "${NVIM_PID:-}" ]]; then
    kill "${NVIM_PID}" 2>/dev/null || true
  fi
  exit 0
}
trap cleanup SIGTERM SIGINT

SOCK="${NVIM_LISTEN_ADDRESS:-/tmp/nvim.sock}"
IS_TCP=0
if [[ "${SOCK}" != /* ]] && [[ "${SOCK}" == *:* ]]; then
  IS_TCP=1
fi
if [[ "${IS_TCP}" -eq 0 ]]; then
  mkdir -p "$(dirname "${SOCK}")"
fi

while :; do
  if [[ "${IS_TCP}" -eq 0 ]]; then
    rm -f "${SOCK}" || true
  fi
  nvim --headless --listen "${SOCK}" &
  NVIM_PID=$!
  wait "${NVIM_PID}" || true
  sleep 1
done
EOS
chmod +x /usr/local/bin/nvim-server.sh

NVIM_ENV_PRESERVE="NVIM_LISTEN_ADDRESS"
if [ -n "${SSH_AUTH_SOCK:-}" ]; then
  NVIM_ENV_PRESERVE="${NVIM_ENV_PRESERVE},SSH_AUTH_SOCK"
fi

SOCK_DESCR="${NVIM_LISTEN_ADDRESS:-/tmp/nvim.sock}"
if pgrep -u "${REMOTE_USER}" -f 'nvim --headless --listen' >/dev/null 2>&1; then
  log "nvim server already running for ${REMOTE_USER}; skipping start"
else
  log "starting nvim server on ${SOCK_DESCR}"
  if [ "${REMOTE_USER}" = "root" ]; then
    /usr/local/bin/nvim-server.sh &
  else
    sudo --preserve-env="${NVIM_ENV_PRESERVE}" -u "${REMOTE_USER}" /usr/local/bin/nvim-server.sh &
  fi
  log "nvim server started (pid $!)"
fi

# Exec original CMD (sshd -D -e by default)
exec "$@"
