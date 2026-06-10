# E2E harness: local atproto network (PLC + PDS) + indigo relay (the oracle)
# + lastproto binaries, all in one container on localhost so DID-document URLs
# resolve everywhere.
#
# Build context is the REPO ROOT (see test.sh / justfile):
#   docker build -f tests/e2e/Dockerfile .
#
# Ports: 2582 PLC, 2583 PDS, 2470 relay API/firehose, 2471 relay metrics.

FROM rust:1.95-bookworm AS rustbuild
# moq-relay first: heavy, rarely changes, caches independently of our code
RUN cargo install moq-relay --version 0.12.9 --locked --root /moq
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --bins \
    && mkdir -p /out && cp target/release/lastproto /out/

FROM golang:1.26-trixie AS gobuild
# Pinned to the commit checked out in ~/code/indigo during planning.
ARG INDIGO_COMMIT=0eb7e0ea71bc9ef74c68aec126f8a753493d3813
RUN git clone https://github.com/bluesky-social/indigo /indigo \
    && cd /indigo \
    && git checkout "$INDIGO_COMMIT"
WORKDIR /indigo
# The relay's HostChecker hard-codes an SSRF-protected (public-IP-only) HTTP
# transport, which rejects our localhost PDS during requestCrawl. The slurper
# already skips SSRF protection for no-SSL hosts; this patch makes the host
# checker match. Dial policy only — validation behavior is untouched.
RUN sed -i '/util\/ssrf/d; /Transport:.*PublicOnlyTransport/d' cmd/relay/relay/host_checker.go \
    && go build -o /out/relay ./cmd/relay

FROM node:22-bookworm
COPY --from=gobuild /out/relay /usr/local/bin/relay
COPY --from=rustbuild /out/* /moq/bin/moq-relay /usr/local/bin/

WORKDIR /app
COPY tests/e2e/package.json tests/e2e/tsconfig.json ./
COPY tests/e2e/dev-env ./dev-env
# `npm install` runs the prepare script (tsc -> dev-env/dist)
RUN npm install
COPY tests/e2e/harness ./harness
COPY tests/e2e/moq-relay.toml ./moq-relay.toml
COPY tests/e2e/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh && mkdir -p /data/relay/persist

EXPOSE 2470 2471 2582 2583
ENTRYPOINT ["/entrypoint.sh"]
