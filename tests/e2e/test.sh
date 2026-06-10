#!/usr/bin/env bash
# Smoke test: build the harness image, run it, drive writes into the PDS,
# capture the indigo relay's firehose, and verify the capture matches.
set -euo pipefail
cd "$(dirname "$0")"

IMAGE=lastproto-e2e
NAME=lastproto-e2e-run

docker build -t "$IMAGE" -f Dockerfile ../..

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

echo "starting MoQ tail (live subscriber)..."
docker exec -d "$NAME" bash -c \
  'moq-tail http://localhost:4443 --idle-ms 8000 >/tmp/moq.jsonl 2>/tmp/moq-tail.log'

echo "driving writes..."
docker exec "$NAME" node /app/harness/driver.mjs >/tmp/lastproto-driver.json
# give the relays a moment to ingest everything
sleep 2

echo "capturing relay firehose..."
docker exec "$NAME" node /app/harness/capture.mjs >/tmp/lastproto-capture.jsonl

echo "capturing PDS firehose directly (ground truth)..."
docker exec "$NAME" bash -c \
  'ws-tail ws://localhost:2583 --cursor 0 --idle-ms 3000 >/tmp/pds.jsonl 2>/dev/null'

echo "verifying indigo capture against driver expectations..."
docker cp /tmp/lastproto-driver.json "$NAME":/tmp/driver.json >/dev/null
docker cp /tmp/lastproto-capture.jsonl "$NAME":/tmp/capture.jsonl >/dev/null
docker exec "$NAME" node /app/harness/verify.mjs /tmp/driver.json /tmp/capture.jsonl

echo "verifying MoQ passthrough is byte-identical to the PDS firehose..."
# the detached moq-tail exits after 8s idle; make sure it's flushed
sleep 7
docker exec "$NAME" node /app/harness/diff-frames.mjs /tmp/pds.jsonl /tmp/moq.jsonl --min-overlap=8
