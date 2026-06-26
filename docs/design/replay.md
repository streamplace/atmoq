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

- **moq-net** (`docs/design/moq-net-replay.patch`, applied via a temporary
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

## Phase 2 — on disk (implemented: Tier A + Tier B)

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

### Tier B — deep, disk-served window (beyond RAM) — DONE

For hours/days without holding the window in RAM, the publisher serves old groups
straight from disk on `SUBSCRIBE(start_group=G)` rather than reloading them into
the track. Implemented via a small moq-net hook + an atmoq `GroupSource`:

- **moq-net** (in `docs/design/moq-net-replay.patch`; see `../moq-net/UPSTREAM.md`):
  a `GroupSource` trait (`group(seq) -> Option<Vec<Bytes>>`, `oldest()`),
  `State.group_source` + `TrackProducer::set_group_source`, `TrackConsumer::oldest()`
  (the in-RAM cache floor) + `group_source()`. In `lite/publisher.rs::run_track`,
  when a subscriber's `start_group` predates the cache floor, it serves
  `[max(start_group, source.oldest()) .. cache_floor)` from the source as ordinary
  group streams — one open uni-stream at a time, so the subscriber's flow control
  paces the backfill — then joins the live loop. The trait is sync and only called
  on the backfill task (never the hot live loop); the cache floor is re-read each
  iteration so the backfill converges on the eviction boundary and hands off live
  without a gap.
- **atmoq**: `StoreSource` implements `GroupSource` over the shared
  `Arc<Mutex<store::GroupStore>>` (the same store the pump appends to;
  `read`-by-seq and `oldest_seq` already exist). `build_lite_publisher` wires it
  onto the track via `set_group_source`.

**Decoupled windows (the key knob).** Tier A's `--replay-window-secs` drives the
*RAM* cache age *and* the startup reload, so it stays small (the in-RAM fast
path). `--backfill-window-secs` governs the *disk* retention (GC) and thus the
depth the `GroupSource` serves — independent of RAM.

**Defaults match indigo's relay out of the box** (Eli's call 2026-06-26 — same
behavior from both without flags): the disk store is **on by default** at
`--group-store-dir data/atmoq/store` (mirroring indigo's `--persist-dir
data/relay/persist`), and `--backfill-window-secs` defaults to **259200 (72h)** —
indigo's `RELAY_REPLAY_WINDOW`. `--replay-window-secs` stays at 60 as the RAM
fast path; deeper resumes (up to 72h) are served from disk. So a bare `atmoq
serve` / `atmoq relay` gives a consumer the same 72h resume horizon a WS relay
does. (To run RAM-only/ephemeral, the disk store is structural now — point it at
a throwaway dir; there's deliberately no off switch, matching indigo.)

### Validation done (Tier B)
End-to-end against a local `atmoq serve` (`--group-size 16 --replay-window-secs 8
--backfill-window-secs 3600 --group-store-dir …`, upstream `wss://bsky.network`),
consumer = atmoq-go v0.0.2:
- Record live-edge group **G=366**; wait **16s** (2× the 8s RAM window, so G is
  evicted from RAM but kept on disk); then `SubscribeFrom(366)` returns first
  group **366** while a concurrent fresh live `Subscribe` is at edge **836** — i.e.
  the resume replayed from disk **470 groups behind the live edge**, far beyond
  what RAM held.
- **Control** (same test, no `--group-store-dir`): `SubscribeFrom(178)` returns
  **441** (the ~8s RAM cache floor, ~179 groups behind edge **620**) — G is lost
  and the consumer must repair the `[178..441)` gap via PDS re-sync. This is the
  gap Tier B closes.

Limits: a group GC'd off the tail of the disk window between `oldest()` and the
read is skipped (a small gap the consumer repairs via PDS re-sync); backfill of a
very deep resume is bounded by disk read + wire throughput, which must exceed the
live firehose rate to converge (it does in practice — disk replay outruns realtime).

## Phase 3 — interop + deep tail

- **`at-seq → group` index** (sqlite sidecar): lets a consumer holding a WS-style
  `at-seq` cursor resume on MoQ (cross-transport migration). Not needed for
  MoQ-native resume (the consumer cursors on the group it last saw).
- **Per-account desync → PDS re-sync** (streamplace side, transport-agnostic):
  when replay lands past the expected `at-seq`/`rev` for a DID, re-fetch that repo
  from its PDS. The true recovery path for anything past the retention horizon.
  Tracked separately.
