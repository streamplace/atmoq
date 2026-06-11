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
- **RESOLVED 2026-06-11: atmoq now speaks draft-07.** `--dialect ietf-07`
  links the maintenance-branch cloudflare/moq-rs crates (pinned by rev in
  Cargo.toml) behind the same publish/consume seam as moq-net. Verified
  live: bsky.network → atmoq → relay.cloudflare.mediaoverquic.com → atmoq
  firehose, **240 overlapping mainnet frames byte-identical** vs a direct
  WS capture. Drop the dialect when Cloudflare deploys draft-14 — moq-net
  should then connect as-is (offers Draft14+).
- **Cloudflare session behavior (2026-06-11)**: the relay closes *whole
  sessions* rather than failing individual subscribes — `Closed(404)` when
  no publisher has announced the namespace, `Closed(0)` when the publisher
  goes away mid-subscription. Both now handled by reconnect loops on both
  sides of the 07 dialect (verified: 2,485 live frames across a publisher
  kill/restart, seq strictly increasing, 5 reconnects absorbed).
- **`wrong size` on reused namespaces (2026-06-11)**: Cloudflare caches
  groups per namespace, and namespaces outlive sessions (no auth). A
  restarted publisher that restarts group IDs at 0 collides with the
  previous run's cached groups, and subscribers read a stale/live mix ->
  `wrong size` / garbage frames. Fixed by seeding each session's group IDs
  from epoch millis so they never collide across restarts. The deeper issue
  is inherent to a zero-auth relay: *anyone* can publish into your
  namespace (including a second copy of your own relay) — unguessable
  scopes, and eventually consumer-side validation (M2), are the defenses.

- **Single publisher per namespace (2026-06-11)**: Cloudflare enforces
  first-come-first-served — a second publisher's ANNOUNCE on a claimed
  namespace gets `Closed(0)` immediately; the holder is unaffected. Not
  auth (anyone can claim a *free* namespace), but it does prevent the
  two-publisher corruption mode. Claim release: instant on clean exit,
  **~16s after unclean death** (QUIC idle timeout) — so a supervisor
  restart or a hot-standby second relay takes over within ~16s. atmoq's
  publisher waits out a 500ms post-connect probation so an about-to-be-
  rejected session can't silently swallow frames, then retries every 2s —
  i.e. running two `atmoq relay`s on one namespace is now a working
  primary/standby pair, not an error.

## Scorecard

| Capability | cdn.moq.dev (`--dialect lite`) | Cloudflare (`--dialect ietf-07`) |
|---|---|---|
| Connect (IPv4) | ✅ default | ✅ with `--client-bind 0.0.0.0:0` |
| Connect (IPv6) | ✅ | untested (no v6 here) |
| Session establish | ✅ | ✅ (draft-07 dialect; draft-14 rejected, incl. CF's own client) |
| Publish / subscribe | ✅ | ✅ |
| Byte-exact passthrough | ✅ (62 live frames) | ✅ (240 live frames) |
| Churn resilience | ✅ resubscribe + dedupe | ✅ reconnect loops both sides + dedupe |
| Auth model | `/anon` prefix, JWT otherwise | none (unguessable names) |
