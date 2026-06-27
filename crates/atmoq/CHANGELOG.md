# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.2](https://github.com/streamplace/atmoq/compare/v0.0.1...v0.0.2) - 2026-06-27

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
