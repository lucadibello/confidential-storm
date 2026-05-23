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

# Exec original CMD (sshd -D -e by default)
exec "$@"
