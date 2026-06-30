# atmoq monorepo task runner — https://just.systems
#
# Layout: rust/ (Rust workspace + vendored moq-net), go/ (Go client),
# ts/ (TypeScript client, browser + server via WebTransport), docs/, tests/e2e/.
# Each language keeps its own release cadence: Rust tags rust-vX.Y.Z via
# release-plz; Go tags go/vX.Y.Z; TS tags ts/vX.Y.Z — all via `just *-release`.

default:
    @just --list

# Install git hooks (rustfmt + gofmt on commit) for this clone
install-hooks:
    git config core.hooksPath .githooks
    @echo "pre-commit rustfmt + gofmt hook enabled"

# --- top-level ----------------------------------------------------------

# Run unit tests for every language
test: rust-test-unit go-test ts-test

# --- Rust (rust/) -------------------------------------------------------
# Run cargo from rust/ — the workspace root after the monorepo move.

# Rust unit tests
rust-test-unit:
    cd rust && cargo test --workspace --locked

# Build the e2e harness Docker image (PLC + PDS + indigo relay oracle + atmoq)
rust-build-e2e:
    docker build -t atmoq-e2e -f tests/e2e/Dockerfile .

# Run unit tests + the e2e differential harness
rust-test: rust-test-unit rust-test-e2e

# e2e differential harness (Docker)
rust-test-e2e:
    tests/e2e/test.sh

# Relay the live Bluesky firehose through a public MoQ relay
rust-live-relay scope="atmoq-demo" relay_url="https://cdn.moq.dev/anon":
    cd rust && cargo run --release --bin atmoq -- relay --moq-host {{relay_url}}/{{scope}}

# Tail a live broadcast back from the public MoQ relay
rust-live-tail scope="atmoq-demo" relay_url="https://cdn.moq.dev/anon":
    cd rust && cargo run --release --bin atmoq -- firehose --moq-host {{relay_url}}/{{scope}}

# Serve MoQ subscribers directly from this box (dev TLS; use --tls-cert/key in prod)
rust-live-serve bind="[::]:4443":
    cd rust && cargo run --release --bin atmoq -- serve --server-bind '{{bind}}' --tls-generate localhost

# Same pair via Cloudflare's relay (draft-07 dialect; v4 bind for v4-only hosts)
rust-live-relay-cf scope="atmoq-demo":
    cd rust && cargo run --release --bin atmoq -- relay --moq-host https://relay.cloudflare.mediaoverquic.com --dialect ietf-07 --broadcast {{scope}} --client-bind 0.0.0.0:0

rust-live-tail-cf scope="atmoq-demo":
    cd rust && cargo run --release --bin atmoq -- firehose --moq-host https://relay.cloudflare.mediaoverquic.com --dialect ietf-07 --broadcast {{scope}} --client-bind 0.0.0.0:0

# --- Go (go/) -----------------------------------------------------------

# Go unit tests
go-test:
    cd go && go test ./...

# Build, vet, and test the Go client
go-check:
    cd go && go build ./... && go vet ./... && go test ./...

# Format all Go sources
go-fmt:
    cd go && gofmt -w .

# Tail a relay's atproto firehose over MoQ (default: streamplace.network)
go-firehose relay="moqt://streamplace.network":
    cd go && go run ./cmd/atmoq-firehose {{relay}}

# Tidy go.mod/go.sum, then fail if either changed (run before releasing)
go-verify-tidy:
    #!/usr/bin/env bash
    set -euo pipefail
    cd go
    go mod tidy
    if ! git diff --quiet -- go.mod go.sum; then
        echo "go.mod/go.sum are not tidy — commit what 'go mod tidy' just produced:" >&2
        git --no-pager diff -- go.mod go.sum >&2
        exit 1
    fi

# Cut a Go release: `just go-release 0.0.1` validates, tags go/v0.0.1, and
# pushes it. A Go module in a subdirectory MUST use slash-prefixed tags
# (go/vX.Y.Z) for the proxy to resolve versions — go-vX.Y.Z would not work.
# Consumers then `go get github.com/streamplace/atmoq/go@v0.0.1`.
go-release version:
    #!/usr/bin/env bash
    set -euo pipefail

    ver="{{version}}"
    ver="${ver#v}"
    if [[ ! "$ver" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "version must be semver X.Y.Z (e.g. 0.0.1), got '{{version}}'" >&2
        exit 1
    fi
    tag="go/v$ver"

    if ! git remote get-url origin >/dev/null 2>&1; then
        echo "no 'origin' remote set" >&2
        exit 1
    fi

    if [[ -n "$(git status --porcelain)" ]]; then
        echo "working tree is dirty; commit or stash before releasing" >&2
        exit 1
    fi
    if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
        echo "tag $tag already exists" >&2
        exit 1
    fi

    branch="$(git rev-parse --abbrev-ref HEAD)"
    just go-check
    just go-verify-tidy

    echo "==> tagging $tag on '$branch' and pushing to origin"
    git tag -a "$tag" -m "atmoq go client $tag"
    git push origin "$branch"
    git push origin "$tag"

    echo "==> released $tag"
    echo "    go get github.com/streamplace/atmoq/go@$tag"

# --- TypeScript (ts/) ---------------------------------------------------

# TypeScript unit tests
ts-test:
    cd ts && npx vitest run

# Type-check (tsc --noEmit)
ts-check:
    cd ts && npx tsc --noEmit

# Build to dist/
ts-build:
    cd ts && npx tsc

# Install dependencies
ts-install:
    cd ts && npm install

# Cut a TS release: `just ts-release 0.0.1` validates, tags ts/v0.0.1, and
# publishes to npm as @streamplace/atmoq. Requires an npm auth token
# (npm login or NPM_CONFIG_REGISTRY + token in env).
ts-release version:
    #!/usr/bin/env bash
    set -euo pipefail

    ver="{{version}}"
    ver="${ver#v}"
    if [[ ! "$ver" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        echo "version must be semver X.Y.Z (e.g. 0.0.1), got '{{version}}'" >&2
        exit 1
    fi
    tag="ts/v$ver"

    if [[ -n "$(git status --porcelain)" ]]; then
        echo "working tree is dirty; commit or stash before releasing" >&2
        exit 1
    fi
    if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
        echo "tag $tag already exists" >&2
        exit 1
    fi

    branch="$(git rev-parse --abbrev-ref HEAD)"
    just ts-check
    just ts-test

    # Bump the version in package.json, build, and publish.
    ( cd ts && npm version "$ver" --no-git-tag-version )
    just ts-build
    ( cd ts && npm publish --access public )

    git add ts/package.json ts/package-lock.json
    git commit -m "ts: release v$ver"

    echo "==> tagging $tag on '$branch' and pushing to origin"
    git tag -a "$tag" -m "atmoq ts client $tag"
    git push origin "$branch"
    git push origin "$tag"

    echo "==> released $tag"
    echo "    npm install @streamplace/atmoq@$ver"

# --- housekeeping -------------------------------------------------------

# Remove e2e harness containers and image
clean:
    -docker rm -f atmoq-e2e-run
    -docker rmi atmoq-e2e
