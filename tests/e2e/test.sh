#!/usr/bin/env bash
# Smoke test: build the harness image, run it, drive writes into the PDS,
# capture the indigo relay's firehose, and verify the capture matches.
set -euo pipefail
cd "$(dirname "$0")"

IMAGE=atmoq-e2e
NAME=atmoq-e2e-run

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

echo "starting MoQ tail (live subscriber, Rust client)..."
docker exec -d "$NAME" bash -c \
  'atmoq firehose --moq-host http://localhost:4443 --raw --idle-ms 8000 >/tmp/moq.jsonl 2>/tmp/moq-tail.log'

echo "starting MoQ tail (Go client)..."
docker exec -d "$NAME" bash -c \
  'atmoq-firehose-go --insecure --raw --idle-ms 8000 moqt://localhost:4443 >/tmp/moq-go.jsonl 2>/tmp/moq-go-tail.log'

echo "starting MoQ tail (TS client)..."
# Cert pinning (serverCertificateHashes) instead of --insecure: the polyfill's
# rejectUnauthorized path is experimental and fails the WT handshake; moq-relay
# serves its current cert hash over plain HTTP for exactly this.
docker exec -d "$NAME" bash -c \
  'node /app/ts/cmd/atmoq-firehose.mjs moqt://localhost:4443 \
     --cert-hash "$(curl -s http://localhost:4443/certificate.sha256)" \
     --raw --idle-ms 8000 >/tmp/moq-ts.jsonl 2>/tmp/moq-ts-tail.log'

echo "driving writes..."
docker exec "$NAME" node /app/harness/driver.mjs >/tmp/atmoq-driver.json
# give the relays a moment to ingest everything
sleep 2

echo "capturing relay firehose..."
docker exec "$NAME" node /app/harness/capture.mjs >/tmp/atmoq-capture.jsonl

echo "capturing PDS firehose directly (ground truth)..."
docker exec "$NAME" bash -c \
  'atmoq firehose --relay-host ws://localhost:2583 --cursor 0 --raw --idle-ms 3000 >/tmp/pds.jsonl 2>/dev/null'

echo "verifying indigo capture against driver expectations..."
docker cp /tmp/atmoq-driver.json "$NAME":/tmp/driver.json >/dev/null
docker cp /tmp/atmoq-capture.jsonl "$NAME":/tmp/capture.jsonl >/dev/null
docker exec "$NAME" node /app/harness/verify.mjs /tmp/driver.json /tmp/capture.jsonl

echo "verifying MoQ passthrough is byte-identical to the PDS firehose..."
# the detached moq-tails exit after 8s idle; make sure they're flushed
sleep 7
docker exec "$NAME" node /app/harness/diff-frames.mjs /tmp/pds.jsonl /tmp/moq.jsonl --min-overlap=8

echo "verifying the Go client sees the same bytes..."
docker exec "$NAME" bash -c 'cat /tmp/moq-go-tail.log >&2 || true; test -s /tmp/moq-go.jsonl' \
  || { echo "FAIL: Go tail produced no frames" >&2; exit 1; }
docker exec "$NAME" node /app/harness/diff-frames.mjs /tmp/pds.jsonl /tmp/moq-go.jsonl --min-overlap=8

echo "verifying the TS client sees the same bytes..."
docker exec "$NAME" bash -c 'cat /tmp/moq-ts-tail.log >&2 || true; test -s /tmp/moq-ts.jsonl' \
  || { echo "FAIL: TS tail produced no frames" >&2; exit 1; }
docker exec "$NAME" node /app/harness/diff-frames.mjs /tmp/pds.jsonl /tmp/moq-ts.jsonl --min-overlap=8
