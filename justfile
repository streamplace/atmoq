# lastproto task runner — https://just.systems

default:
    @just --list

# Build the e2e harness Docker image (PLC + PDS + indigo relay oracle + lastproto)
build:
    docker build -t lastproto-e2e -f tests/e2e/Dockerfile .

# Run unit tests + the e2e differential harness
test: test-unit test-e2e

# Rust unit tests
test-unit:
    cargo test

# e2e differential harness (Docker)
test-e2e:
    tests/e2e/test.sh

# Remove harness containers and image
clean:
    -docker rm -f lastproto-e2e-run
    -docker rmi lastproto-e2e
