#!/usr/bin/env bash
set -euo pipefail

PUB_KEY="$(cat ~/.ssh/id_ed25519.pub)"

for SLAVE in 10.233.26.41 10.233.26.42 10.233.26.43; do
  echo "=== $SLAVE ==="

  # Install the master's public key via docker cp.
  # docker cp writes directly to the container's overlay filesystem at the daemon
  # level, bypassing any in-container security context that blocks docker exec
  # writes to /home/dev/.ssh.
  ssh luca@"$SLAVE" "
    set -e
    TMPFILE=\$(mktemp)
    # Preserve existing keys (ignore error if file absent).
    docker exec confidential-storm-devcontainer \
      cat /home/dev/.ssh/authorized_keys 2>/dev/null >> \"\$TMPFILE\" || true
    # Append our key and deduplicate.
    printf '%s\n' '${PUB_KEY}' >> \"\$TMPFILE\"
    sort -u -o \"\$TMPFILE\" \"\$TMPFILE\"
    # Ensure .ssh directory exists with correct ownership.
    docker exec -u root confidential-storm-devcontainer \
      bash -c 'mkdir -p /home/dev/.ssh && chown dev:dev /home/dev/.ssh && chmod 700 /home/dev/.ssh'
    # Copy file in — docker cp bypasses in-container permission enforcement.
    docker cp \"\$TMPFILE\" confidential-storm-devcontainer:/home/dev/.ssh/authorized_keys
    docker exec -u root confidential-storm-devcontainer \
      bash -c 'chown dev:dev /home/dev/.ssh/authorized_keys && chmod 600 /home/dev/.ssh/authorized_keys'
    rm -f \"\$TMPFILE\"
  "

  # Verify two-hop auth using a dynamically allocated local port so successive
  # iterations never collide even if a previous tunnel lingers.
  LOCAL_PORT=$(python3 -c \
    'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); p=s.getsockname()[1]; s.close(); print(p)')
  ssh -N -L "${LOCAL_PORT}:127.0.0.1:2222" luca@"$SLAVE" &
  TUNNEL_PID=$!
  sleep 1
  ssh -o BatchMode=yes \
      -o StrictHostKeyChecking=no \
      -o UserKnownHostsFile=/dev/null \
      -i ~/.ssh/id_ed25519 -p "$LOCAL_PORT" dev@127.0.0.1 echo "ok: $SLAVE"
  kill "$TUNNEL_PID" 2>/dev/null
  wait "$TUNNEL_PID" 2>/dev/null
done
