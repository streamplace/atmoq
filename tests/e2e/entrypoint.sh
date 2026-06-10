#!/bin/bash
set -euo pipefail

wait_for() {
  local url=$1 name=$2
  for _ in $(seq 1 120); do
    if curl -sf "$url" >/dev/null 2>&1; then
      echo "[entrypoint] $name ready"
      return 0
    fi
    sleep 0.5
  done
  echo "[entrypoint] timed out waiting for $name at $url" >&2
  exit 1
}

echo "[entrypoint] starting PLC + PDS"
node /app/harness/start-network.mjs &
wait_for http://localhost:2583/xrpc/_health "PDS"

echo "[entrypoint] starting indigo relay"
DATABASE_URL="sqlite:///data/relay/relay.sqlite" \
RELAY_PERSIST_DIR=/data/relay/persist \
RELAY_PLC_HOST=http://localhost:2582 \
RELAY_ADMIN_PASSWORD=admin \
RELAY_ALLOW_INSECURE_HOSTS=true \
RELAY_API_BIND=:2470 \
relay serve &
wait_for http://localhost:2470/xrpc/_health "relay"

echo "[entrypoint] requesting crawl of local PDS"
curl -sf -u admin:admin -X POST \
  -H 'Content-Type: application/json' \
  -d '{"hostname":"http://localhost:2583"}' \
  http://localhost:2470/admin/pds/requestCrawl

touch /tmp/ready
echo "[entrypoint] harness ready"

# exit if either background service dies
wait -n
exit 1
