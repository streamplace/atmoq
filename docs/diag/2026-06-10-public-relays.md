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
- ~~Working hypothesis: WebTransport-layer draft skew.~~ **CONFIRMED
  2026-06-11: the endpoint speaks draft-07 only.** cloudflare/moq-rs's
  README says it plainly: main targets draft-14, but "Cloudflare's current
  production deployment" uses the `draft-ietf-moq-transport-07` branch.
  Verified empirically from this host:
  - their **draft-07 branch** `moq-clock-ietf` (pub + sub): **works
    end-to-end** through relay.cloudflare.mediaoverquic.com;
  - their **main branch (draft-14)** `moq-clock-ietf`: connects, then
    `session error: connection error: closed` — the same post-handshake
    rejection our moq-net client gets.
  So the [feature matrix](https://developers.cloudflare.com/moq/feature-matrix/)
  describes moq-rs `main`, not the deployed relay, and *no* draft-14 client
  (including Cloudflare's own) can use the public endpoint today.
- Implications for atmoq: keep moq-net as the only backend for now. When
  Cloudflare deploys draft-14, re-test moq-net as-is (it offers Draft14+ in
  negotiation). If Cloudflare reach matters before then, the options are a
  draft-07 backend behind our (tiny) transport seam — likely throwaway
  work — or kixelated's JS `@moq/net` DRAFT_07 codepath for browser-side
  consumers. Worth asking kixelated/Cloudflare about the draft-14 rollout
  timeline before building anything.

## Scorecard

| Capability | cdn.moq.dev | Cloudflare |
|---|---|---|
| Connect (IPv4) | ✅ default | ✅ with `--client-bind 0.0.0.0:0` |
| Connect (IPv6) | ✅ | untested (no v6 here) |
| Session establish | ✅ | ❌ draft-07 only (confirmed with CF's own clients) |
| Publish / subscribe / announce | ✅ | ✅ via draft-07 clients only |
| Byte-exact passthrough | ✅ | untested (needs draft-07 backend) |
| Auth model | `/anon` prefix, JWT otherwise | none (unguessable names) |
