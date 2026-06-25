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
- atmoq-go `SubscribeFrom` wire encoding: unit test passes.
- streamplace builds + moq/dedup tests pass against the local atmoq-go.
- **Full end-to-end (atmoq-go ↔ a current-main `atmoq serve` build with
  `--replay-window-secs 60`):** tail the live edge and record group G=2786; wait
  6s (past the old 5s window); then `SubscribeFrom(2786)` returns first group
  **2786** while a concurrent fresh live `Subscribe` returns **2940** — i.e. the
  resume replayed ~154 groups (~2,400 frames) the default 5s window would have
  dropped. Run: `serve --tls-cert … --tls-key … --server-bind 0.0.0.0:4443
  --replay-window-secs 60`, consumer dials `moqt://<real-dns-host>:4443`.

### Non-issue (earlier misdiagnosis): real TLS required, not version drift
An earlier draft of this doc claimed a moq-net/moq-native "version-drift blocker"
stopped atmoq-go from connecting to a current-main build. **That was wrong.** The
failure was entirely the dev TLS path: `serve --tls-generate localhost` + a
consumer dialing `moqt://127.0.0.1:4443` with `Options{Insecure:true}` (cert host
≠ dialed IP). With a real cert + matching DNS (no `Insecure`), atmoq-go connects
to a current-main build and the e2e above passes. Follow-up (minor, separate from
replay): atmoq-go's `Insecure`/self-signed path doesn't work for an IP / hostname
mismatch — fine for prod (real certs), worth fixing for local dev.

## Phase 2 — on disk (implemented through Tier A)

- **Durable, monotonic group IDs across restart — DONE.** `--group-seq-file`
  persists the high-water group seq; the first group after restart is seeded at
  `seed + 1` via `create_group(Group{ sequence })`. Persisted on creation so a
  sequence is never reused for different content. Verified: a run ending at group
  940 continued at 1369 (not 0) after restart.
- **Disk-backed, group-aligned segment store — DONE** (`src/store.rs`).
  Append-only segment files keyed by group seq, index rebuilt on open, whole-
  segment age GC (active segment kept), torn-tail tolerance, max-seq recovery.
  Unit-tested (roundtrip+reopen, GC, time filter).
- **Tier A — restart-survivable replay window — DONE.** `--group-store-dir`:
  each completed group is appended to disk; on startup the in-window groups are
  reloaded into the live track (original sequences, fresh cache timestamps), so a
  relay restart no longer drops the window. GC on a 30s timer. The store also
  supplies the durable seed. Verified end-to-end: consumer noted group 416,
  relay was fully restarted, run 2 logged `reloaded ... reloaded=1011`, and
  `SubscribeFrom(416)` returned group 416 — a window Phase 1 lost on restart.
  Bounded by RAM (the reloaded window lives in the track), so this reaches
  minutes, not 72h.

### Tier B — deep, disk-served window (beyond RAM) — remaining, needs moq-net

For hours/days without holding the window in RAM, the publisher must serve old
groups straight from disk on a `SUBSCRIBE(start_group=G)` rather than reloading
them into the track. moq-net's `Track` is a shared in-RAM fan-out cache that
evicts on age, so this needs a moq-net hook. Proposed API (coordinate with
kixelated, alongside the `set_max_group_age` patch):

```rust
/// A fallback source of historical groups, consulted when a requested group is
/// no longer in the in-RAM cache.
pub trait GroupSource: Send + Sync {
    fn group(&self, seq: u64) -> Option<Vec<Bytes>>; // None = not stored
    fn oldest(&self) -> Option<u64>;
}
impl TrackProducer { pub fn set_group_source(&mut self, src: Arc<dyn GroupSource>); }
```

Integration point is the publisher's per-subscription `run_track`
(`lite/publisher.rs`): before `track.start_at(start_group)` serves from RAM,
serve `[max(start_group, source.oldest()) .. oldest_in_ram)` from the source as
ordinary group streams, then continue live. atmoq implements `GroupSource` over
`store::GroupStore` (read-by-seq already exists). The disk read is async vs
`poll_recv_group`'s sync `Poll`, so the cleanest seam is in `run_track` (its own
async task) rather than inside the shared `TrackConsumer` poll. Scope with a
spike before committing.

## Phase 3 — interop + deep tail

- **`at-seq → group` index** (sqlite sidecar): lets a consumer holding a WS-style
  `at-seq` cursor resume on MoQ (cross-transport migration). Not needed for
  MoQ-native resume (the consumer cursors on the group it last saw).
- **Per-account desync → PDS re-sync** (streamplace side, transport-agnostic):
  when replay lands past the expected `at-seq`/`rev` for a DID, re-fetch that repo
  from its PDS. The true recovery path for anything past the retention horizon.
  Tracked separately.
