# Production image for the `atmoq` binary. Build context is the repo root:
#   docker build -t atmoq .
# Published to ghcr.io/streamplace/atmoq by .github/workflows/docker.yml.

FROM rust:1.95-bookworm AS build
WORKDIR /src
# Only the manifests + sources are needed (see .dockerignore). moq-net is
# vendored in-tree at vendor/moq-net (see Cargo.toml [patch.crates-io]), so there
# are no git deps to fetch; --locked pins Cargo.lock.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY vendor ./vendor
RUN cargo build --release --locked --bin atmoq \
    && cp target/release/atmoq /usr/local/bin/atmoq

FROM debian:bookworm-slim
# ca-certificates: atmoq makes outbound TLS to bsky.network and MoQ relays
# (rustls + aws-lc-rs is statically linked, so nothing else is required).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/atmoq /usr/local/bin/atmoq
# Links the GHCR package to the repo and surfaces logs at info by default.
LABEL org.opencontainers.image.source=https://github.com/streamplace/atmoq
ENV RUST_LOG=info
# `atmoq <subcommand>`: `docker run ghcr.io/streamplace/atmoq relay --moq-host ...`
ENTRYPOINT ["atmoq"]
CMD ["--help"]
