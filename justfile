# lastproto task runner — https://just.systems

default:
    @just --list

# Build the e2e harness Docker image (PLC + PDS + indigo relay oracle)
build:
    docker build -t lastproto-e2e tests/e2e

# Run the full test suite (currently: the e2e differential harness)
test:
    tests/e2e/test.sh

# Remove harness containers and image
clean:
    -docker rm -f lastproto-e2e-run
    -docker rmi lastproto-e2e
