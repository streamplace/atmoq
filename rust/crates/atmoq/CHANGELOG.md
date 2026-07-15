# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/streamplace/atmoq/compare/v0.0.2...v0.1.0) - 2026-07-14

First release on the unified train: this version ships simultaneously to
crates.io (`atmoq`), npm (`@streamplace/atmoq`), and as the Go module tag
`go/v0.1.0` — one version, one commit, three ecosystems.

### Other

- vendor patched moq-net/moq-native in-tree, published as atmoq-moq-net /
  atmoq-moq-native, so `cargo install atmoq` works from crates.io again
- DRISL-strict validation at relay ingest and in all three clients (Rust, TS, Go)
- store: v2 record format, size-bounded GC (--max-store-bytes), torn-tail
  truncation, disk reads outside the store lock, resilient startup
- serve: atomic state-file writes; never reuse the in-progress group's
  sequence across restarts
- relay: stop republishing upstream error frames; detect seq regression
- router: fix unused-watcher race; configurable per-DID track cap
- go: client hardened against hostile sizes, stalls, and shutdown leaks
- ts: group-boundary frame loss fixed, close() race fixed, certHashes connect
  option, @atproto/lex-cbor, pure-JSONL CLI output
- e2e: diff all three clients against PDS ground truth

### Other

- selective sync via on-demand per-DID tracks
- Tier B disk-served deep replay + 72h indigo-matching defaults
- --group-store-dir for restart-survivable replay (Tier A)
- disk-backed, group-aligned firehose store (Phase 2 substrate)
- durable monotonic group ids across restart
- --replay-window-secs for deeper late-join replay
- note the Windows/IPv4 --client-bind workaround
- we're not rainbow

## [0.0.1](https://github.com/streamplace/atmoq/compare/v0.0.0...v0.0.1) - 2026-06-11

### Other

- rustfmt + fix clippy lints
- Handle Cloudflare's single-publisher-per-namespace semantics
- Draft-07 resilience + goat-style --ops
- Add draft-07 dialect: atmoq now works through Cloudflare's relay
- Rename project: lastproto -> atmoq
