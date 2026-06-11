# atmoq task runner — https://just.systems

default:
    @just --list

# Build the e2e harness Docker image (PLC + PDS + indigo relay oracle + atmoq)
build:
    docker build -t atmoq-e2e -f tests/e2e/Dockerfile .

# Run unit tests + the e2e differential harness
test: test-unit test-e2e

# Rust unit tests
test-unit:
    cargo test

# e2e differential harness (Docker)
test-e2e:
    tests/e2e/test.sh

# Relay the live Bluesky firehose through a public MoQ relay
live-relay scope="atmoq-demo" relay_url="https://cdn.moq.dev/anon":
    cargo run --release --bin atmoq -- relay --moq-host {{relay_url}}/{{scope}}

# Tail a live broadcast back from the public MoQ relay
live-tail scope="atmoq-demo" relay_url="https://cdn.moq.dev/anon":
    cargo run --release --bin atmoq -- firehose --moq-host {{relay_url}}/{{scope}}

# Serve MoQ subscribers directly from this box (dev TLS; use --tls-cert/key in prod)
live-serve bind="[::]:4443":
    cargo run --release --bin atmoq -- serve --server-bind '{{bind}}' --tls-generate localhost

# Same pair via Cloudflare's relay (draft-07 dialect; v4 bind for v4-only hosts)
live-relay-cf scope="atmoq-demo":
    cargo run --release --bin atmoq -- relay --moq-host https://relay.cloudflare.mediaoverquic.com --dialect ietf-07 --broadcast {{scope}} --client-bind 0.0.0.0:0

live-tail-cf scope="atmoq-demo":
    cargo run --release --bin atmoq -- firehose --moq-host https://relay.cloudflare.mediaoverquic.com --dialect ietf-07 --broadcast {{scope}} --client-bind 0.0.0.0:0

# Remove harness containers and image
clean:
    -docker rm -f atmoq-e2e-run
    -docker rmi atmoq-e2e
