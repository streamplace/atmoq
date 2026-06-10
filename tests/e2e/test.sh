#!/usr/bin/env bash
# Smoke test: build the harness image, run it, drive writes into the PDS,
# capture the indigo relay's firehose, and verify the capture matches.
set -euo pipefail
cd "$(dirname "$0")"

IMAGE=lastproto-e2e
NAME=lastproto-e2e-run

docker build -t "$IMAGE" .

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" -p 2470:2470 -p 2582:2582 -p 2583:2583 "$IMAGE" >/dev/null

cleanup() {
  status=$?
  if [ $status -ne 0 ]; then
    echo "--- container logs (tail) ---"
    docker logs --tail 100 "$NAME" || true
  fi
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  exit $status
}
trap cleanup EXIT

echo "waiting for harness..."
for _ in $(seq 1 240); do
  if docker exec "$NAME" test -f /tmp/ready 2>/dev/null; then break; fi
  if [ -z "$(docker ps -q -f name=$NAME)" ]; then
    echo "container exited during startup" >&2
    exit 1
  fi
  sleep 0.5
done
docker exec "$NAME" test -f /tmp/ready

echo "driving writes..."
docker exec "$NAME" node /app/harness/driver.mjs >/tmp/lastproto-driver.json
# give the relay a moment to ingest everything
sleep 2

echo "capturing relay firehose..."
docker exec "$NAME" node /app/harness/capture.mjs >/tmp/lastproto-capture.jsonl

echo "verifying..."
docker cp /tmp/lastproto-driver.json "$NAME":/tmp/driver.json >/dev/null
docker cp /tmp/lastproto-capture.jsonl "$NAME":/tmp/capture.jsonl >/dev/null
docker exec "$NAME" node /app/harness/verify.mjs /tmp/driver.json /tmp/capture.jsonl
