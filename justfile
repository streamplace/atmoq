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

# Remove harness containers and image
clean:
    -docker rm -f atmoq-e2e-run
    -docker rmi atmoq-e2e
