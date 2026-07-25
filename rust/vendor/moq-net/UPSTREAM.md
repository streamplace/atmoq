# Upstreaming notes

This is a vendored copy of [`moq-net`](https://crates.io/crates/moq-net) `0.1.10`
(kixelated's [moq-dev/moq](https://github.com/moq-dev/moq) lite stack) with four
changes intended to go upstream. The repo is laid out so each change diffs
cleanly on top of the pristine base:

1. `vendor moq-net 0.1.10 (unmodified, from crates.io)` — pristine base.
2. Configurable per-track group retention (below).
3. Pluggable `GroupSource` for a deep, out-of-RAM replay window (below).
4. Cancel-safe `Writer` writes, fixing group-stream corruption under priority
   churn + backpressure (below).
5. In-flight group gauge, making a stuck subscriber's retained-group backlog
   observable (below).

All are additive except (4), which is a bug fix.

So that `cargo install atmoq` works before these land upstream, this copy is
published to crates.io under the fork name
[`atmoq-moq-net`](https://crates.io/crates/atmoq-moq-net) (the lib name stays
`moq_net`; consumers use cargo dependency renaming). Its sibling
`vendor/moq-native` republishes an *unmodified* `moq-native` 0.17.0 as
`atmoq-moq-native` purely so its `moq-net` dependency resolves to this fork —
types must be identical across the `moq-native` API boundary. Both fork crates
get deprecated the moment upstream ships an equivalent.

## Change: configurable per-track group retention

`src/model/track.rs` hardcodes how long the track cache retains groups:

```rust
/// Groups older than this are evicted from the track cache (unless they are the max_sequence group).
// TODO: Replace with a configurable cache size.
const MAX_GROUP_AGE: Duration = Duration::from_secs(5);
```

This change makes it overridable per track without altering the default
behavior, directly addressing that `TODO`:

- adds `State.max_group_age: Option<Duration>` (default `None`),
- `evict_expired` uses `self.max_group_age.unwrap_or(MAX_GROUP_AGE)`,
- adds `TrackProducer::set_max_group_age(Duration)`,
- adds a unit test, `set_max_group_age_extends_retention`.

`None` preserves the existing 5s behavior exactly, so it's backward compatible.

### Why

A publisher that wants a deeper late-join / replay window than 5s (e.g. an
atproto firehose relay letting consumers resume after a disconnect) needs to
trade memory for retention depth. See the consumer's design notes in
`streamplace/atmoq` at `docs/design/replay.md`.

### Possible upstream shapes

The minimal change here is a per-track setter. A maintainer might instead prefer
a builder-style API or a cache-size (bytes/count) bound rather than age — hence
capturing it as an isolated, easy-to-rebase commit rather than a fork.

## Change: pluggable `GroupSource` for a deep, out-of-RAM replay window

The retention change above buys depth by holding more groups in RAM, which caps
the practical window at minutes. To serve a window of hours/days, the publisher
needs to serve historical groups from a pluggable (e.g. disk-backed) source
instead of the in-RAM cache. This change adds that hook:

- adds a `GroupSource` trait (`fn group(seq) -> Option<Vec<Bytes>>`,
  `fn oldest() -> Option<u64>`) in `src/model/track.rs`,
- adds `State.group_source: Option<Arc<dyn GroupSource>>` (default `None`) and
  `TrackProducer::set_group_source(Arc<dyn GroupSource>)`,
- adds two `TrackConsumer` accessors: `oldest()` (the in-RAM cache floor) and
  `group_source()`,
- in `lite/publisher.rs::run_track`: when a subscriber's `SUBSCRIBE(start_group=G)`
  predates the cache floor, serve `[max(G, source.oldest()) .. cache_floor)`
  straight from the source — one open uni-stream at a time, so the subscriber's
  flow control paces the backfill — before joining the live stream. The cache
  floor is re-read each iteration so the backfill converges on the (slowly
  advancing) eviction boundary and hands off to the live loop without a gap.
- adds unit tests `group_source_plumbing` and `oldest_returns_min_live_sequence`.

`None` (no source set) preserves the existing behavior exactly: a `start_group`
below the cache simply jumps forward to the oldest cached group, as before.

### Why

A persistent replay window (e.g. an atproto firehose relay matching indigo's
72h WS-relay window) can't fit in RAM. The publisher already has the bytes on
disk; this lets it stream them on resume without a separate FETCH round-trip.
Validated end-to-end in `streamplace/atmoq` (a disk store serving a resume from
a group 470 groups / ~16s behind the live edge, with only an 8s RAM window).

### Possible upstream shapes

The trait is intentionally tiny and synchronous (the read happens on the
backfill task, never in the hot live loop). A maintainer might prefer an async
trait, a `Stream`-returning API, or folding the source into the existing
`Track` cache as a tiered backing store — hence keeping it an isolated,
easy-to-rebase commit rather than a fork.

## Change: cancel-safe `Writer::write` (bug fix, 0.1.11)

`src/coding/writer.rs` previously wrote through the transport adapter's
`write_buf`. `web-transport-quinn` (0.11.9) overrides the trait's (cancel-safe)
default with a zero-copy version that `copy_to_bytes(..)` — i.e. **advances the
caller's buffer** — *before* awaiting the QUIC write:

```rust
async fn write_buf<B: Buf + Send>(&mut self, buf: &mut B) -> Result<usize, Self::Error> {
    let size = buf.chunk().len();
    let chunk = buf.copy_to_bytes(size); // caller's cursor advanced here
    self.write_chunk(chunk).await?;      // cancellation point: bytes lost
    Ok(size)
}
```

`serve_group` in `src/lite/publisher.rs` races `stream.write_all(&mut chunk)`
against priority-change futures in a `select!`. When a priority wakeup fires
while the write is parked on flow control, the future is dropped and the
consumed-but-unwritten bytes vanish; every subsequent byte in the group stream
lands at the wrong offset, corrupting all remaining frames. In practice this
triggered when a deep-resume backfill caught up to the live edge (many
concurrent `serve_group` tasks → priority churn, saturated send window →
backpressure), splicing atproto firehose frames.

The fix routes `Writer::write` / `Writer::encode` through `SendStream::write`
(borrowed slice) and advances the buffer only after the transport accepts the
bytes, restoring the cancel-safety the trait's own default provides.

**Upstream note:** the underlying bug is in `web-transport-quinn`'s `write_buf`
override (or, arguably, in `select!`-racing a non-cancel-safe write). Worth
filing against both: web-transport-quinn's override violates the implicit
cancel-safety contract of the trait default, and every `moq-net` release built
on it can corrupt streams under priority churn + backpressure.

## Change: in-flight group gauge (`src/inflight.rs`)

`run_track` in `src/lite/publisher.rs` spawns one `serve_group` task per group
into an **unbounded** `FuturesUnordered`. Each pending task holds a
`GroupConsumer`, which keeps that group's frames alive *past the track cache's
own age-based eviction*. A subscriber that stops draining — flow-control
stalled, or its own downstream blocked — therefore pins group memory for as
long as it stays connected, and memory grows with the publish rate.

Nothing about that backlog is observable from outside the process: QUIC
multiplexes every session onto one UDP socket, so there is no socket, no fd,
and no queue depth to inspect. Diagnosing an instance of this in production
(streamplace.network, 2026-07: 128 GB RSS over 20h, ~370 KB retained per group
rotated) required walking a 122 GiB heap by hand.

This change is purely additive and does not alter behavior:

- adds `src/inflight.rs` with process-global `groups_in_flight()` /
  `groups_in_flight_max()` accessors and an RAII `InFlightGroup` guard,
- `run_track` holds a guard alongside each queued `serve_group` task, so the
  count decrements on completion *and* on cancellation,
- adds two unit tests.

The guard is deliberately global rather than per-session: the sessions that
would own it are constructed inside the accept path with no handle reaching the
embedding application. A per-subscription breakdown is the natural follow-up
once someone needs to attribute a backlog to a specific peer.

**Upstream note:** the gauge is a diagnostic for a real unboundedness. The
actual fix is to bound the queue and drop the oldest pending group under
pressure — which MoQ explicitly permits a relay to do under congestion — but
that changes delivery behavior, so it is deliberately a separate change from
merely being able to see the problem.
