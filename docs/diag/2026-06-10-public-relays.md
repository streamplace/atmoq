# Public relay compatibility findings — 2026-06-10

First run of the per-relay compatibility checks (decision 0001), using the
prototype `relay` (wss://bsky.network ingest) and `moq-tail` binaries,
moq-net 0.1.10 / moq-native 0.17.0.

## kixelated — cdn.moq.dev ✅

- URL: `https://cdn.moq.dev/anon/<scope>`, anonymous under `/anon` (use
  unguessable scopes; the path bypasses auth by design).
- **Works end-to-end**: live bsky.network frames published and tailed back,
  byte-exact, ordered, with group rotation (observed join at group 23 of a
  running stream — late-join lands on the latest group boundary as expected).
- Negotiates moq-lite natively. No flags needed beyond defaults.
- **Byte-exactness proven on the public path**: parallel captures of
  wss://bsky.network directly (ws-tail) and through cdn.moq.dev
  (relay → moq-tail) diffed with diff-frames.mjs: 62 overlapping live
  mainnet frames byte-identical.

## Cloudflare — relay.cloudflare.mediaoverquic.com ❌ (today, from here)

- Connection requires `--client-bind 0.0.0.0:0` on IPv4-only hosts (their
  AAAA records win otherwise and quinn errors with NetworkUnreachable).
- TLS + h3 handshake succeed, then the session dies during WebTransport
  establishment: `web_transport_quinn: failed to read capsule e=UnexpectedEnd`
  followed by `closed by peer: 0`. Same failure with:
  - default version offer and `--client-version moq-transport-14` (their
    [feature matrix](https://developers.cloudflare.com/moq/feature-matrix/)
    says draft-07 + draft-14),
  - scoped path (`/<scope>`) and bare path,
  - `moqt://` raw-QUIC scheme.
- Working hypothesis: **WebTransport-layer draft skew** — failure happens at
  capsule read, *before* any MOQT version negotiation could matter, and the
  raw-QUIC ALPN path is refused too. Cloudflare's relay is a fork of older
  kixelated code and may speak an older WebTransport draft / ALPN set than
  web-transport-quinn's current ratified-spec implementation.
- Next steps: capture qlog/quinn traces; test their JS client (`@moq/net`
  has a DRAFT_07 codepath the Rust crate doesn't) against the same endpoint
  to separate "endpoint is picky" from "endpoint is down for everyone";
  ask in the MoQ Discord/Slack — kixelated told Eli moq-lite would be fully
  compatible with Cloudflare's relays, so either this is a regression, a
  missing knob, or interop work that's still landing.

## Scorecard

| Capability | cdn.moq.dev | Cloudflare |
|---|---|---|
| Connect (IPv4) | ✅ default | ✅ with `--client-bind 0.0.0.0:0` |
| Connect (IPv6) | ✅ | untested (no v6 here) |
| Session establish | ✅ | ❌ WT capsule failure |
| Publish / subscribe / announce | ✅ | — |
| Byte-exact passthrough | ✅ | — |
| Auth model | `/anon` prefix, JWT otherwise | none (unguessable names) |
