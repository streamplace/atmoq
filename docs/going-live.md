# Going live: rebroadcasting an existing relay over MoQ

Short answer: **yes, you can do this today, with one binary and zero
infrastructure of your own.**

```bash
atmoq relay \
  --relay-host wss://bsky.network \
  --moq-host https://cdn.moq.dev/anon/<unguessable-scope> \
  --cursor-file /var/lib/atmoq/cursor
```

Anyone, anywhere, can then consume the firehose over MoQ:

```bash
atmoq firehose --moq-host https://cdn.moq.dev/anon/<unguessable-scope>
```

The relay is a verbatim passthrough (frames byte-identical to the upstream,
proven by `tests/e2e` and live diffs against bsky.network), reconnects both
legs automatically, resumes the upstream from a persisted cursor, and
consumers survive publisher restarts.

## What "live" costs, by tier

### Tier 0 — what works right now (one process, public CDN)

- Any box with outbound UDP/443 (QUIC) or TCP/443 (WebSocket fallback —
  moq-native races both, so even UDP-hostile networks work).
- Mainnet firehose is roughly 5–15 Mbps sustained, bursty. The MoQ side
  pushes the same volume once; **the CDN absorbs all subscriber fan-out** —
  that's the entire point, your egress doesn't scale with audience.
- systemd unit or container; `--cursor-file` on persistent disk. On restart
  the upstream replays from the cursor (bsky.network's backfill window is
  ~72h) and duplicates are dropped, so brief downtime loses nothing.

### Tier 1 — things to fix before telling other people to consume it

- **Broadcast authenticity.** `/anon` on cdn.moq.dev and all of Cloudflare's
  relay are unauthenticated: anyone who learns your scope can publish a
  *competing* broadcast under the same name. Commit frames are
  self-certifying (signed by account keys), but `#account`/`#identity` are
  hop-by-hop trusted — a squatter could inject fake takedowns. Fixes, in
  increasing order of effort:
  1. unguessable scope shared out-of-band (what we do now);
  2. a JWT-scoped path from kixelated's `moq-token` (cdn.moq.dev supports
     this — ask him for a signing root for a `atmoq/` prefix);
  3. run your own `moq-relay` (it's a small Rust binary; the e2e container
     runs one) and let the public CDNs peer/cache in front later.
- **Courtesy.** cdn.moq.dev is a free 3-node preview CDN. A 24/7 full-mainnet
  firehose is exactly the kind of traffic the giants *say* they want to
  subsidize — but tell kixelated before parking it there permanently.
- **Monitoring**: the relay logs seq progress; metrics (lag vs upstream,
  group rate, reconnect counts) are an easy next addition.

### Tier 2 — what makes it a *relay* rather than a re-broadcaster

Today the binary is a passthrough of one upstream. The plan (PLAN.md M2/M3)
turns it into a true at-sync relay: multi-host ingest, §4.5 validation
(signatures, op inversion), its own sequence numbers, per-type tracks, and
the legacy WS output for drop-in indigo compatibility. None of that blocks
Tier 0/1 — passthrough of an already-validating relay (bsky.network) is a
legitimate distribution node, since at-sync consumers must re-validate
everything themselves anyway (§5.4).

## Known constraints

- Consumers need moq-net/moq-lite client libs (Rust today; kixelated's JS
  `@moq/net` should interop — untested, good first TS task).
- Late joiners start at the latest group boundary (`--group-size` frames of
  replay at most); deeper backfill = re-sync from PDS/relay per decision
  0001. MoQ relays may also drop groups under congestion (~30s cache), so
  lossless consumers should track seq and re-sync on gaps — which at-sync
  requires of them anyway.
- Cloudflare's relay doesn't accept our sessions yet (WebTransport-layer
  failure, see docs/diag/2026-06-10-public-relays.md) — cdn.moq.dev only
  for now.
