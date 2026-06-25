# Late-join / resume over MoQ (no FETCH)

Goal: give MoQ firehose subscribers a WS-relay-like resume experience — reconnect
and pick up near where they left off — without FETCH (decision 0001). indigo's WS
relay default replay window is **72h** (disk-backed); moq-net's in-RAM group cache
is **5s**. This doc tracks closing that gap incrementally.

Resume works through `SUBSCRIBE(start_group=G)`: moq-lite's publisher does
`track.start_at(start_group)` and streams forward from any retained group. FETCH
would only add random-access range fetch, which we don't need.

## Phase 1 — "minutes, in RAM" (implemented, branch state)

Lets a consumer reconnect (relay process staying up) and replay the last N
seconds, vs the 5s default.

- **moq-net** (`docs/design/moq-net-max-group-age.patch`, applied via a temporary
  local `[patch.crates-io]` to `../moq-net`): adds
  `TrackProducer::set_max_group_age(Duration)` overriding the hardcoded 5s
  `MAX_GROUP_AGE`. Additive, keeps the 5s default, unit-tested
  (`set_max_group_age_extends_retention`). **Upstream this** (moq-net's own
  "configurable cache size" TODO sits on that const) and drop the patch.
- **atmoq**: `--replay-window-secs` (default 60) on `serve`/`relay`, applied to
  the published track via `apply_replay_window`. RAM-bounded; deep backfill stays
  a PDS re-sync.
- **atmoq-go**: `SubscribeFrom(broadcast, track, startGroup)` — `start_group` is an
  `Option<u64>` on the wire (0=None, else value-1), unit-tested. A consumer resumes
  by remembering the last group `ReadFrame` returned.
- **streamplace**: `relayCursor` tracks the high-water MoQ group (in-memory);
  `connectRelayMoq` resumes via `SubscribeFrom` on reconnect; replayed overlap is
  absorbed by the commit-CID deduper.

Scope/limits: resumes within a relay process run, not across a **relay** restart
(the RAM window is lost then anyway, and group ids aren't yet durable — see
Phase 2). If the requested group has aged out, moq-lite jumps forward to the
oldest retained group ≥ G, leaving a gap the consumer must repair via PDS re-sync.

### Validation done
- moq-net retention override: unit test passes.
- `atmoq serve --replay-window-secs 60` runs; moq-net's own Rust client streams
  from it.
- atmoq-go `SubscribeFrom` wire encoding: unit test passes.
- streamplace builds + moq/dedup tests pass against the local atmoq-go.

End-to-end (atmoq-go consumer ↔ locally-built atmoq serve) is **blocked** by the
version-drift issue below, not by the replay code.

## ⚠️ Prerequisite blocker: moq-net/moq-native version drift

A consumer using **atmoq-go** cannot establish a session with a relay built from
current atmoq `main` (moq-net 0.1.10 / moq-native 0.17), though it works fine
against the **deployed** streamplace.network relay.

Evidence:
- atmoq-go plain `Subscribe` (bytes unchanged by replay work) works vs
  streamplace.network, fails vs a fresh local `atmoq serve` build.
- moq-net's own Rust client works vs the same local build (server is fine).
- Server-side trace shows the relay never surfaces atmoq-go's session at all
  (no "subscriber connected"); the QUIC/ALPN handshake (`moq-lite-04`) succeeds,
  but the raw-QUIC moq-lite **session** is never accepted, so nothing reads the
  SUBSCRIBE stream and it resets with code 0.
- `Cargo.lock` history shows moq-net drifting 0.1.5 → 0.1.9 → 0.1.10; atmoq-go was
  reverse-engineered against the line the deployed relay runs.

Root cause: a moq-native/moq-net change to raw-QUIC **session establishment**
(atmoq-go speaks moq-lite directly over raw QUIC — no WebTransport/h3). This is
exactly the "kixelated iterates fast; pin per release" risk in decision 0001.

Decision needed (Eli): standardize on a version line —
1. Pin atmoq to the moq-net/moq-native the deployed relay + atmoq-go already
   speak, and apply the retention patch there; or
2. Update atmoq-go to current moq-net/moq-native session establishment
   (re-reverse-engineer the raw-QUIC handshake, or adopt whatever it now requires).

Until resolved, deploying a relay from current `main` would break streamplace's
moqt firehose path. Replay can't be e2e-demoed locally through atmoq-go either.

## Phase 2 — "hours/days, on disk" (design)

Needed for real indigo parity; the substantive work.

- **Durable, monotonic group IDs across restart.** Persist the high-water group
  seq (extend the cursor-file mechanism) and seed the first group via
  `create_group(Group{ sequence })` (moq-net already supports an explicit
  sequence) instead of `append_group()`. Prereq for any cross-restart group
  cursor. Cheap; no fork.
- **Disk-backed, group-aligned segment log + age GC** (mirrors indigo's
  diskpersist: log files + a small index, delete refs older than retention).
- **Replay-publisher seam** — the hard part. moq-net's `Track` is a shared
  in-RAM fan-out cache that evicts on age; you can't serve deep history from it
  (wrong memory profile, shared across subscribers). Options:
  1. extend moq-net with a pluggable/disk-backed group source (best; coordinate
     with kixelated alongside the retention patch), or
  2. a custom lite publisher path that owns the disk log and seams disk→live per
     subscription.
  Scope this with a spike before committing; it gates Phase 2.

## Phase 3 — interop + deep tail

- **`at-seq → group` index** (sqlite sidecar): lets a consumer holding a WS-style
  `at-seq` cursor resume on MoQ (cross-transport migration). Not needed for
  MoQ-native resume (the consumer cursors on the group it last saw).
- **Per-account desync → PDS re-sync** (streamplace side, transport-agnostic):
  when replay lands past the expected `at-seq`/`rev` for a DID, re-fetch that repo
  from its PDS. The true recovery path for anything past the retention horizon.
  Tracked separately.
