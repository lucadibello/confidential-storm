PUB_KEY="$(cat ~/.ssh/id_ed25519.pub)"

for SLAVE in 10.233.26.41 10.233.26.42 10.233.26.43; do
  echo "=== $SLAVE ==="

  # Install the master's public key inside the slave devcontainer (runs on slave)
  ssh luca@"$SLAVE" \
    "docker exec confidential-storm-devcontainer \
        bash -c 'mkdir -p /home/dev/.ssh && chmod 700 /home/dev/.ssh && \
          echo \"${PUB_KEY}\" >> /home/dev/.ssh/authorized_keys && \
          sort -u -o /home/dev/.ssh/authorized_keys 
/home/dev/.ssh/authorized_keys && \
          chmod 600 /home/dev/.ssh/authorized_keys'"
  # Copy the master's public key to the slave's SSH server (runs on master)
  ssh-copy-id -i ~/.ssh/id_ed25519.pub -p 22 luca@"$SLAVE"

  # Verify the two-hop auth (all three commands run locally on the master)
  ssh -N -L 12222:127.0.0.1:2222 luca@"$SLAVE" &
  TUNNEL_PID=$!
  sleep 1
  ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
    -i ~/.ssh/id_ed25519 -p 12222 dev@127.0.0.1 echo "ok: $SLAVE"
  kill "$TUNNEL_PID" 2>/dev/null
  wait "$TUNNEL_PID" 2>/dev/null
done
