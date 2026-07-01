# Selective sync: per-DID tracks for subset consumers

Goal: let a consumer subscribe to the firehose events of a **known, small set of
accounts** — e.g. the ~50 people in a room, or one app's users — instead of the
whole ~40M-account firehose, and have it work **from a browser over
WebTransport, through a generic (atproto-unaware) MoQ relay**.

This is the selective-sync problem ATOM §4.2.2 gestured at but mis-specified (it
assumed in-track subgroup filtering MOQT doesn't have — see
`docs/atom-spec-notes.md` §4). This is the design that actually fits MoQ.

## The constraint (why you can't just filter `all`)

A MoQ relay routes by **track**, never by object contents. There is no
SUBSCRIBE-time filter for "the subset of a track matching key X" (no
subgroup/content filter through draft-17). So:

- A generic relay **cannot** carve a per-account slice out of the aggregate
  `all` track. The bytes for one account are not addressable within `all`.
- The only native selectivity axis is the **track name** (within an announced
  namespace).

So a per-account view has to be **its own track**, produced by the only party
that understands accounts: the **origin** (atmoq). The relay stays dumb.

## Model: demand-driven per-DID tracks

Namespace `at/firehose/{host}` (as today). Two kinds of track:

- `all` — the aggregate firehose. Announced, always published; what full
  consumers and downstream bridges use.
- `{did}` — one track per account, **materialized on demand**: created only when
  someone subscribes, populated by filtering the firehose, torn down when the
  last subscriber leaves.

Flow:

1. A selective consumer **subscribes to `at/firehose/{host}/{did}` by known
   name** — it already knows the DIDs it wants (the room's members), so no
   discovery / ANNOUNCE of per-DID tracks is needed.
2. The subscribe propagates upstream; a relay forwards it via standard
   demand-driven upstream subscription.
3. atmoq fulfills it: moq-net's `BroadcastProducer::dynamic()` surfaces the
   request through `requested_track()`, handing back a `TrackProducer`
   **preconfigured with the requested track name** (the DID). atmoq validates it
   as a DID and registers a filter: every firehose event whose repo/subject is
   that DID is also written to this track (with its own group rotation).
4. When the track's subscribers all leave, `TrackProducer::unused()` fires and
   atmoq drops the filter and the track.

The relay only ever sees opaque track names; **all atproto awareness lives at
the origin.**

### Why this dodges the 40M-track fear

- **No per-DID ANNOUNCE.** Consumers subscribe by known name, so we never
  advertise (or even instantiate) 40M tracks.
- **Nothing idle is materialized.** A `{did}` track exists only while subscribed.
- Relay and origin state scale with **active demand** — how many distinct DIDs
  are being watched right now — not with the account count.

## Honest costs / limits

- **No relay-side sharing with `all`.** A watched DID's events traverse
  origin→relay twice (once in `all`, once in `{did}`). Cheap per-DID; it just
  means per-DID tracks aren't free riders on the aggregate.
- **No merged-subset subscribe.** "50 users" = 50 subscriptions / 50 tracks.
  Fine for ~50; it gets ugly as the subset approaches the whole network — past
  some fraction, just take `all` and filter client-side.
- **Origin fan-out cost** is O(events): a hashset membership check per event plus
  a write per matched (DID, event). Scales to thousands of watched DIDs
  comfortably — it's the same per-event decode atmoq already does for `t`/`seq`.
- **Sparse `at-seq`.** A per-DID stream has gaps where other accounts' events
  were. Fine: at-sync §4.3 permits seq gaps, and correctness is per-account
  `rev` continuity, which is preserved (single relay sequence space, in order per
  DID).
- **Backfill.** v1 is live-edge per DID. Deep history for a newly-watched DID
  comes from its PDS (decision 0001), or later from the `all` disk store filtered
  on replay (see `replay.md`) — a refinement, not v1.
- **Cloudflare's draft-07 relay** doesn't follow the moq-lite demand-driven model
  (no ANNOUNCE, namespace-cached — see `docs/diag`); per-DID-on-demand through CF
  needs verifying. moq-lite relays (cdn.moq.dev, self-hosted `atmoq serve`) do it
  natively.

## Browser / WebTransport

The motivating use case is a **browser** subscribing to a handful of accounts
(the room), not 40M into a tab.

- **Transport is already there.** moq-native — which `atmoq serve` runs on —
  speaks **WebTransport (HTTP/3)** alongside raw QUIC (see `rs/moq-native`
  `lib.rs`: "WebTransport (HTTP/3)" / "Raw QUIC"). So a browser can connect to an
  `atmoq serve` instance today; raw-QUIC is merely the path atmoq-go happens to
  use.
- **Client.** A browser uses kixelated's **moq.js** (`@kixelated/moq`) for the
  WebTransport + moq-lite layer, then a thin decode shim turns each frame (one
  at-sync DAG-CBOR message, byte-identical to a subscribeRepos payload) into
  events. No QUIC/transport code to write — only the decode. A small `atmoq-js`
  could package the per-DID subscribe + decode, mirroring atmoq-go.
- **Shape.** The browser subscribes to the room's `{did}` tracks (one per
  member) and merges the low-rate streams locally. Bandwidth is ~the sum of those
  accounts' activity — trivially browser-feasible, versus the 2–3 Mbps full
  firehose.

## Build scope (atmoq)

**No moq-net changes** — `BroadcastProducer::dynamic()` / `requested_track()` and
WebTransport are already in the pinned moq-net. The work is atmoq-side:

1. **Serve `all` via a dynamic broadcast.** Keep the current `all` track; also
   call `broadcast.dynamic()` and spawn a task looping `requested_track()`.
2. **Per-request filter.** For each requested track: parse the name as a DID
   (reject malformed). Create a small per-DID publisher (frame → group rotation,
   like the `all` pump but its own groups). Register it in a
   `HashMap<Did, PerDidTrack>`.
3. **Fan-out in the pump.** For each firehose frame, decode the repo/subject DID
   (`Frame::decode` + `field("repo")` for `#commit`, `field("did")` for
   `#identity`/`#account`); if that DID is in the active map, also write the frame
   to its track.
4. **Lifecycle.** The per-DID task awaits `track.unused()`; on fire, drop the
   track and remove it from the map. Bound the number of concurrent per-DID
   tracks (config) to cap origin cost.
5. **(Optional) atmoq-js** — a thin browser package: WebTransport via moq.js + the
   CBOR decode + a `subscribeDids([...])` helper.

Estimate: a few hundred lines + tests for steps 1–4 (the moq-net dynamic API does
the hard part); `atmoq-js` is a separate, small JS effort. No protocol or moq-net
changes.

## Status

**Built** (commit ad4edcb): `src/router.rs` serves on-demand per-DID tracks via
moq-net's dynamic broadcast, and `atmoq firehose --wanted-dids did:...,did:...`
consumes them. The concurrent-track bound from step 4 is configurable via
`--max-did-tracks` (default 10,000); note it is a **global** cap, not
per-session — a per-session bound needs session identity plumbed through
moq-net's dynamic track requests, which it doesn't expose today. The browser
client shipped separately as `@streamplace/atmoq` (ts/), which subscribes to
per-DID tracks the same way (any track name in `subscribe()`).
