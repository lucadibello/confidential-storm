#!/usr/bin/env bash
set -euo pipefail

REMOTE_USER="${DEVCONTAINER_USER:-${REMOTE_USER:-dev}}"
if ! id -u "${REMOTE_USER}" >/dev/null 2>&1; then
  echo "[entrypoint] remote user '${REMOTE_USER}' not found; defaulting to root"
  REMOTE_USER="root"
fi

if command -v sshd >/dev/null 2>&1; then
  if ! sshd -t; then
    echo "[entrypoint] sshd configuration test failed" >&2
    exit 1
  fi
fi

# setup git config for the dev user
if [ -n "${GIT_NAME:-}" ] && [ -n "${GIT_EMAIL:-}" ]; then
  sudo -H -u "$REMOTE_USER" git config --global user.name "$GIT_NAME"
  sudo -H -u "$REMOTE_USER" git config --global user.email "$GIT_EMAIL"
fi

# Bootstrap headless Neovim watchdog script
cat >/usr/local/bin/nvim-server.sh <<'EOS'
#!/usr/bin/env bash
set -euo pipefail
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
  nvim --headless --listen "${SOCK}" || true
  sleep 1
done
EOS
chmod +x /usr/local/bin/nvim-server.sh

NVIM_ENV_PRESERVE="NVIM_LISTEN_ADDRESS"
if [ -n "${SSH_AUTH_SOCK:-}" ]; then
  NVIM_ENV_PRESERVE="${NVIM_ENV_PRESERVE},SSH_AUTH_SOCK"
fi

SOCK_DESCR="${NVIM_LISTEN_ADDRESS:-/tmp/nvim.sock}"
NVIM_SERVER_PID=""
if pgrep -u "${REMOTE_USER}" -f 'nvim --headless --listen' >/dev/null 2>&1; then
  echo "[entrypoint] nvim server already running for ${REMOTE_USER}; skipping start"
else
  echo "[entrypoint] starting nvim server on ${SOCK_DESCR}"
  if [ "${REMOTE_USER}" = "root" ]; then
    /usr/local/bin/nvim-server.sh &
    NVIM_SERVER_PID=$!
  else
    sudo --preserve-env="${NVIM_ENV_PRESERVE}" -u "${REMOTE_USER}" /usr/local/bin/nvim-server.sh &
    NVIM_SERVER_PID=$!
  fi
  echo "[entrypoint] nvim server started (pid ${NVIM_SERVER_PID})"
fi

# Exec original CMD (sshd -D -e by default)
exec "$@"
