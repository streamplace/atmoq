//! atmoq: atproto firehose over MoQ transport.
//!
//! CLI shape follows goat (github.com/bluesky-social/goat) where the
//! commands overlap: `atmoq firehose` streams events like
//! `goat firehose`, accepting either a WebSocket relay (--relay-host) or a
//! MoQ relay (--moq-host) as the source. `atmoq relay` is the bridge:
//! it consumes a WebSocket firehose and republishes it over MoQ.

use atmoq::{dialect07, frame::Frame, ingest, json::cbor_to_json, store::GroupStore};
use base64::Engine;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Adapts the disk-backed [`GroupStore`] to moq-net's [`moq_net::GroupSource`]
/// so the lite publisher can serve a deep (disk-served) replay window for
/// subscribers resuming from a group that's already aged out of the RAM cache.
/// Shares the same store the pump writes to (`Arc<Mutex<..>>`); reads only take
/// the lock for the brief synchronous seek+read, never across an await.
struct StoreSource(Arc<Mutex<GroupStore>>);

impl moq_net::GroupSource for StoreSource {
    fn group(&self, sequence: u64) -> Option<Vec<Bytes>> {
        match self.0.lock().unwrap().read(sequence) {
            Ok(frames) => frames,
            Err(err) => {
                tracing::warn!(?err, sequence, "group source read failed");
                None
            }
        }
    }

    fn oldest(&self) -> Option<u64> {
        self.0.lock().unwrap().oldest_seq()
    }
}

#[derive(Parser)]
#[command(name = "atmoq", version, about)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Stream repo and identity events (from a WebSocket or MoQ relay)
    Firehose(FirehoseArgs),
    /// Bridge a WebSocket firehose onto a MoQ broadcast (via a MoQ relay)
    Relay(RelayArgs),
    /// Bridge a WebSocket firehose and serve MoQ subscribers directly —
    /// no external MoQ relay needed
    Serve(ServeArgs),
}

/// Which MoQ wire protocol to speak. Public relays differ (docs/diag):
/// cdn.moq.dev speaks moq-lite; Cloudflare's production relay only speaks
/// draft-ietf-moq-transport-07.
#[derive(clap::ValueEnum, Clone, Copy, PartialEq)]
enum Dialect {
    /// moq-lite via moq-net (kixelated) — cdn.moq.dev, local moq-relay
    Lite,
    /// draft-ietf-moq-transport-07 — Cloudflare's production relay
    #[value(name = "ietf-07")]
    Ietf07,
}

#[derive(Parser)]
struct FirehoseArgs {
    /// Consume over WebSocket from this relay/PDS instead of MoQ
    /// (e.g. wss://bsky.network)
    #[arg(long, conflicts_with = "moq_host")]
    relay_host: Option<String>,
    /// MoQ relay URL to consume from
    #[arg(long, default_value = "https://streamplace.network")]
    moq_host: url::Url,
    /// Broadcast path under the MoQ connection URL's scope
    #[arg(long, default_value = "atproto")]
    broadcast: String,
    /// Track name within the broadcast
    #[arg(long, default_value = "atproto")]
    track: String,
    /// Cursor to consume at (WebSocket source only)
    #[arg(long, requires = "relay_host")]
    cursor: Option<i64>,
    /// Include raw frame bytes as base64 in output
    #[arg(long)]
    raw: bool,
    /// Don't print event payloads, just type/seq
    #[arg(long, short = 'q')]
    quiet: bool,
    /// Exit after this many events (0 = run forever)
    #[arg(long, default_value_t = 0)]
    limit: usize,
    /// Exit after this many milliseconds without an event (0 = never)
    #[arg(long, default_value_t = 0)]
    idle_ms: u64,
    /// Print individual record operations (decoded from blocks) instead of
    /// whole events
    #[arg(long, alias = "records")]
    ops: bool,
    /// MoQ wire protocol (use ietf-07 for Cloudflare's relay)
    #[arg(long, value_enum, default_value_t = Dialect::Lite)]
    dialect: Dialect,
    #[command(flatten)]
    client: moq_native::ClientConfig,
}

#[derive(Parser)]
struct RelayArgs {
    /// Method, hostname, and port of the upstream WebSocket relay/PDS
    #[arg(long, default_value = "wss://bsky.network")]
    relay_host: String,
    /// MoQ relay URL to publish through
    #[arg(long)]
    moq_host: url::Url,
    /// Broadcast path published under the connection URL's scope
    #[arg(long, default_value = "atproto")]
    broadcast: String,
    /// Track name within the broadcast
    #[arg(long, default_value = "atproto")]
    track: String,
    /// Frames per MoQ group (late-join replay depth / drop granularity)
    #[arg(long, default_value_t = 64)]
    group_size: usize,
    /// In-RAM replay window in seconds: the fast path for resuming subscribers
    /// near the live edge, held in memory. Deeper resumes are served from disk
    /// (see --backfill-window-secs / --group-store-dir), so this stays small.
    /// moq-net's hardcoded default is 5s; 0 keeps it. This trades memory for the
    /// depth served without a disk read.
    #[arg(long, default_value_t = 60)]
    replay_window_secs: u64,
    /// Upstream cursor to resume from (overridden by --cursor-file if present)
    #[arg(long)]
    cursor: Option<i64>,
    /// Persist the upstream cursor here; resume from it on restart
    #[arg(long)]
    cursor_file: Option<std::path::PathBuf>,
    /// Persist the high-water MoQ group sequence here so group ids stay
    /// monotonic across restarts (keeps consumer group cursors valid)
    #[arg(long)]
    group_seq_file: Option<std::path::PathBuf>,
    /// Disk store directory for the replay window: groups are persisted here and
    /// served from disk for deep resumes, reloaded on restart so a restart
    /// doesn't drop the window, and used as the durable group-sequence seed
    /// (supersedes --group-seq-file). On by default, mirroring indigo's relay
    /// --persist-dir (data/relay/persist).
    #[arg(long, default_value = "data/atmoq/store")]
    group_store_dir: std::path::PathBuf,
    /// Deep replay window in seconds: how long groups are kept on disk and
    /// served straight from disk to subscribers resuming from behind the RAM
    /// window (requires --group-store-dir). Defaults to 259200 (72h), matching
    /// indigo's relay replay window; the firehose held this long is reachable on
    /// resume without holding it all in RAM.
    #[arg(long, default_value_t = 259200)]
    backfill_window_secs: u64,
    /// MoQ wire protocol (use ietf-07 for Cloudflare's relay)
    #[arg(long, value_enum, default_value_t = Dialect::Lite)]
    dialect: Dialect,
    #[command(flatten)]
    client: moq_native::ClientConfig,
}

#[derive(Parser)]
struct ServeArgs {
    /// Method, hostname, and port of the upstream WebSocket relay/PDS
    #[arg(long, default_value = "wss://bsky.network")]
    relay_host: String,
    /// Broadcast path served to subscribers
    #[arg(long, default_value = "atproto")]
    broadcast: String,
    /// Track name within the broadcast
    #[arg(long, default_value = "atproto")]
    track: String,
    /// Public hostname (used by the landing page and redirects)
    #[arg(long, default_value = "localhost")]
    host: String,
    /// Bind for the plain-HTTP -> HTTPS redirect (empty string to disable)
    #[arg(long, default_value = "[::]:80")]
    web_bind: String,
    /// Bind for the HTTPS landing page (empty string to disable; requires
    /// --tls-cert/--tls-key)
    #[arg(long, default_value = "[::]:443")]
    web_tls_bind: String,
    /// Frames per MoQ group (late-join replay depth)
    #[arg(long, default_value_t = 64)]
    group_size: usize,
    /// In-RAM replay window in seconds: the fast path for resuming subscribers
    /// near the live edge, held in memory. Deeper resumes are served from disk
    /// (see --backfill-window-secs / --group-store-dir), so this stays small.
    /// moq-net's hardcoded default is 5s; 0 keeps it. This trades memory for the
    /// depth served without a disk read.
    #[arg(long, default_value_t = 60)]
    replay_window_secs: u64,
    /// Upstream cursor to resume from (overridden by --cursor-file if present)
    #[arg(long)]
    cursor: Option<i64>,
    /// Persist the upstream cursor here; resume from it on restart
    #[arg(long)]
    cursor_file: Option<std::path::PathBuf>,
    /// Persist the high-water MoQ group sequence here so group ids stay
    /// monotonic across restarts (keeps consumer group cursors valid)
    #[arg(long)]
    group_seq_file: Option<std::path::PathBuf>,
    /// Disk store directory for the replay window: groups are persisted here and
    /// served from disk for deep resumes, reloaded on restart so a restart
    /// doesn't drop the window, and used as the durable group-sequence seed
    /// (supersedes --group-seq-file). On by default, mirroring indigo's relay
    /// --persist-dir (data/relay/persist).
    #[arg(long, default_value = "data/atmoq/store")]
    group_store_dir: std::path::PathBuf,
    /// Deep replay window in seconds: how long groups are kept on disk and
    /// served straight from disk to subscribers resuming from behind the RAM
    /// window (requires --group-store-dir). Defaults to 259200 (72h), matching
    /// indigo's relay replay window; the firehose held this long is reachable on
    /// resume without holding it all in RAM.
    #[arg(long, default_value_t = 259200)]
    backfill_window_secs: u64,
    #[command(flatten)]
    server: moq_native::ServerConfig,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("install rustls crypto provider");
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().cmd {
        Cmd::Firehose(args) => firehose(args).await,
        Cmd::Relay(args) => relay(args).await,
        Cmd::Serve(args) => serve(args).await,
    }
}

/// (group sequence if from MoQ, raw frame bytes)
type Item = (Option<u64>, Vec<u8>);

async fn firehose(args: FirehoseArgs) -> anyhow::Result<()> {
    let (tx, rx) = mpsc::channel::<Item>(256);
    if args.relay_host.is_none() {
        let moq_url = args.moq_host.clone();
        if args.dialect == Dialect::Ietf07 {
            tracing::info!(url = %moq_url, namespace = %args.broadcast, "consuming from MoQ relay (draft-07)");
            tokio::spawn(dialect07::subscribe_loop(
                moq_url,
                args.client.bind,
                args.broadcast.clone(),
                args.track.clone(),
                tx,
            ));
        } else {
            let client = args.client.clone().init()?;
            let origin = moq_net::Origin::random().produce();
            let consumer = origin.consume();
            let _session = client.with_consume(origin).connect(moq_url.clone()).await?;
            tracing::info!(url = %moq_url, broadcast = %args.broadcast, "consuming from MoQ relay");
            let broadcast_name = args.broadcast.clone();
            let track_name = args.track.clone();
            tokio::spawn(async move {
                moq_reader(consumer, broadcast_name, track_name, tx).await;
                drop(_session);
            });
        }
    } else {
        let upstream = args.relay_host.clone().expect("relay_host checked above");
        let cursor = args.cursor;
        tokio::spawn(async move {
            let (ftx, mut frx) = mpsc::channel(256);
            let ingest = tokio::spawn(async move {
                if let Err(err) = ingest::subscribe_repos(&upstream, cursor, ftx).await {
                    tracing::error!(?err, "upstream error");
                }
            });
            while let Some(f) = frx.recv().await {
                if tx.send((None, f.raw)).await.is_err() {
                    return;
                }
            }
            let _ = ingest.await;
        });
    }
    print_events(rx, &args).await
}

/// Resilient MoQ track reader: resubscribes across publisher restarts,
/// skips groups truncated by a publisher dying mid-write.
async fn moq_reader(
    consumer: moq_net::OriginConsumer,
    broadcast_name: String,
    track_name: String,
    tx: mpsc::Sender<Item>,
) {
    loop {
        let Some(broadcast) = consumer.announced_broadcast(&broadcast_name).await else {
            tracing::info!("origin closed");
            return;
        };
        let mut track = match broadcast.subscribe_track(&moq_net::Track {
            name: track_name.clone(),
            priority: 0,
        }) {
            Ok(t) => t,
            Err(err) => {
                tracing::warn!(?err, "subscribe failed; retrying");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };
        tracing::info!("subscribed");
        'groups: loop {
            let mut group = match track.next_group().await {
                Ok(Some(g)) => g,
                Ok(None) => {
                    tracing::warn!("track ended; waiting for publisher to return");
                    break 'groups;
                }
                Err(err) => {
                    tracing::warn!(?err, "group error; resubscribing");
                    break 'groups;
                }
            };
            let sequence = group.sequence;
            loop {
                match group.read_frame().await {
                    Ok(Some(data)) => {
                        if tx.send((Some(sequence), data.to_vec())).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        tracing::warn!(?err, "frame error; skipping rest of group");
                        break;
                    }
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

async fn print_events(mut rx: mpsc::Receiver<Item>, args: &FirehoseArgs) -> anyhow::Result<()> {
    let b64 = base64::engine::general_purpose::STANDARD;
    let mut count = 0usize;
    let mut last_seq = i64::MIN;
    loop {
        let item = if args.idle_ms > 0 {
            match tokio::time::timeout(std::time::Duration::from_millis(args.idle_ms), rx.recv())
                .await
            {
                Ok(i) => i,
                Err(_) => {
                    tracing::info!(count, "idle timeout reached");
                    return Ok(());
                }
            }
        } else {
            rx.recv().await
        };
        let Some((group, raw)) = item else {
            return Ok(());
        };
        let (header, payload) = match Frame::decode(&raw) {
            Ok(hp) => hp,
            Err(err) => {
                tracing::warn!(?err, "skipping unparseable frame");
                continue;
            }
        };
        let frame = match Frame::parse(raw.clone()) {
            Ok(f) => f,
            Err(err) => {
                tracing::warn!(?err, "skipping invalid frame");
                continue;
            }
        };
        if let Some(s) = frame.seq {
            if s <= last_seq {
                continue; // group replay after a resubscribe
            }
            last_seq = s;
        }
        if args.ops {
            if frame.t.as_deref() == Some("#commit") {
                match print_ops(&payload, args.quiet) {
                    Ok(n) => count += n,
                    Err(err) => tracing::warn!(?err, seq = ?frame.seq, "failed to decode ops"),
                }
            }
        } else {
            if !args.quiet {
                let mut line = serde_json::json!({ "t": frame.t, "seq": frame.seq });
                if let Some(g) = group {
                    line["group"] = g.into();
                }
                if args.raw {
                    line["raw"] = b64.encode(&raw).into();
                } else {
                    line["header"] = cbor_to_json(&header);
                    line["payload"] = cbor_to_json(&payload);
                }
                println!("{line}");
            }
            count += 1;
        }
        if args.limit > 0 && count >= args.limit {
            return Ok(());
        }
    }
}

/// goat-style `--ops`: print one line per record operation in a #commit,
/// with the record decoded from the message's CAR blocks.
fn print_ops(payload: &ciborium::Value, quiet: bool) -> anyhow::Result<usize> {
    use anyhow::Context;
    use atmoq::{car, frame::field, json::cid_string};

    let blocks = field(payload, "blocks")
        .and_then(|v| v.as_bytes())
        .context("commit missing blocks")?;
    let blocks = car::blocks(blocks)?;
    let ops = field(payload, "ops")
        .and_then(|v| v.as_array())
        .context("commit missing ops")?;

    let str_field = |key: &str| {
        field(payload, key)
            .and_then(|v| v.as_text())
            .map(str::to_owned)
    };
    let repo = str_field("repo");
    let rev = str_field("rev");
    let time = str_field("time");
    let seq = field(payload, "seq")
        .and_then(|v| v.as_integer())
        .map(|i| i128::from(i) as i64);

    let mut printed = 0usize;
    for op in ops {
        let cid_bytes = field(op, "cid").and_then(|v| match v {
            ciborium::Value::Tag(42, inner) => inner.as_bytes().cloned(),
            _ => None,
        });
        let cid_bytes = cid_bytes.map(|b| b.strip_prefix(&[0x00]).map(<[u8]>::to_vec).unwrap_or(b));
        let record = cid_bytes
            .as_ref()
            .and_then(|c| blocks.get(c))
            .and_then(|data| ciborium::de::from_reader::<ciborium::Value, _>(data.as_slice()).ok())
            .map(|v| cbor_to_json(&v));
        if !quiet {
            let line = serde_json::json!({
                "action": field(op, "action").and_then(|v| v.as_text()),
                "path": field(op, "path").and_then(|v| v.as_text()),
                "cid": cid_bytes.as_deref().map(cid_string),
                "record": record,
                "seq": seq,
                "repo": repo,
                "rev": rev,
                "time": time,
            });
            println!("{line}");
        }
        printed += 1;
    }
    Ok(printed)
}

/// Extend the relay's late-join/resume window past moq-net's hardcoded 5s by
/// retaining groups in the track cache for `secs` (0 keeps the moq-net default).
/// Groups live in RAM, so the window is a memory-vs-depth tradeoff; deeper
/// backfill is a PDS re-sync, not transport replay.
fn apply_replay_window(track: &mut moq_net::TrackProducer, secs: u64) -> anyhow::Result<()> {
    if secs > 0 {
        track.set_max_group_age(std::time::Duration::from_secs(secs))?;
        tracing::info!(replay_window_secs = secs, "replay window set");
    }
    Ok(())
}

/// Load the persisted high-water MoQ group sequence, if any. Used to keep group
/// ids monotonic across a relay restart so a consumer's group cursor stays valid
/// (prerequisite for any disk-backed replay store).
fn load_group_seq(path: &Option<std::path::PathBuf>) -> Option<u64> {
    let path = path.as_ref()?;
    let seq = std::fs::read_to_string(path)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    tracing::info!(group_seq = seq, path = %path.display(), "resuming group sequence");
    Some(seq)
}

/// Persist the high-water group sequence (best-effort, no fsync — mirrors the
/// cursor file). Called on each group creation.
fn persist_group_seq(path: &Option<std::path::PathBuf>, seq: u64) {
    let Some(path) = path else { return };
    if let Err(err) = std::fs::write(path, format!("{seq}\n")) {
        tracing::warn!(?err, path = %path.display(), "failed to persist group sequence");
    }
}

/// Create the first group of a run. With a persisted seed we continue at
/// `seed + 1` (via an explicit sequence) so ids never go backwards across a
/// restart; otherwise moq-net's default starts at 0. The chosen sequence is
/// persisted immediately.
fn make_first_group(
    track: &mut moq_net::TrackProducer,
    seed: Option<u64>,
    group_seq_file: &Option<std::path::PathBuf>,
) -> anyhow::Result<moq_net::GroupProducer> {
    let group = match seed {
        Some(prev) => track.create_group(moq_net::Group { sequence: prev + 1 })?,
        None => track.append_group()?,
    };
    persist_group_seq(group_seq_file, group.sequence);
    Ok(group)
}

/// Milliseconds since the unix epoch (wall clock), for group timestamps + GC.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Repopulate the live track from disk on startup so a relay restart doesn't
/// drop the in-RAM replay window: every stored group newer than the window is
/// re-created (with its original sequence) into the track. Reloaded groups get a
/// fresh cache timestamp, so they live for another full window. No-op without a
/// finite window.
fn reload_window(
    track: &mut moq_net::TrackProducer,
    store: &GroupStore,
    window_secs: u64,
) -> anyhow::Result<()> {
    if window_secs == 0 {
        return Ok(());
    }
    let cutoff = now_ms().saturating_sub(window_secs * 1000);
    let mut reloaded = 0u64;
    for seq in store.groups_since(cutoff)? {
        if let Some(frames) = store.read(seq)? {
            let mut g = track.create_group(moq_net::Group { sequence: seq })?;
            for f in frames {
                g.write_frame(f)?;
            }
            g.finish()?;
            reloaded += 1;
        }
    }
    if reloaded > 0 {
        tracing::info!(reloaded, "reloaded replay window from disk");
    }
    Ok(())
}

/// Build a Lite publisher: open the optional disk store, reload the in-window
/// groups into the track (restart-survivable replay), seed the high-water group
/// sequence (store wins over the lightweight seq file), and create the first
/// live group. Shared by `serve` and `relay`.
fn build_lite_publisher(
    mut track: moq_net::TrackProducer,
    group_seq_file: Option<std::path::PathBuf>,
    group_store_dir: &std::path::Path,
    window_secs: u64,
) -> anyhow::Result<FramePublisher> {
    // The disk store is always on for Lite serve/relay (mirrors indigo's
    // always-persisting relay); the directory has a default.
    let store = Some(Arc::new(Mutex::new(GroupStore::open(group_store_dir)?)));
    if let Some(s) = &store {
        // Reload only the *RAM* window into the live cache (Tier A); the deeper
        // disk history is served on demand via the group source below.
        reload_window(&mut track, &s.lock().unwrap(), window_secs)?;
        // Tier B: let the publisher serve groups older than the RAM cache
        // straight from disk for resuming subscribers.
        track.set_group_source(Arc::new(StoreSource(s.clone())))?;
    }
    let seed = store
        .as_ref()
        .and_then(|s| s.lock().unwrap().max_seq())
        .or_else(|| load_group_seq(&group_seq_file));
    let group = make_first_group(&mut track, seed, &group_seq_file)?;
    Ok(FramePublisher::Lite {
        track,
        group,
        count: 0,
        group_seq_file,
        store,
        group_frames: Vec::new(),
    })
}

/// Dialect-agnostic frame publisher with internal group rotation.
enum FramePublisher {
    Lite {
        track: moq_net::TrackProducer,
        group: moq_net::GroupProducer,
        count: usize,
        /// Where to persist the high-water group sequence so ids stay monotonic
        /// across restarts (None = ephemeral, restarts at 0).
        group_seq_file: Option<std::path::PathBuf>,
        /// Disk store for restart-survivable replay (None = RAM only). Shared
        /// (`Arc<Mutex>`) with the lite publisher's group source, which reads it
        /// to serve the deep replay window. Touched per-rotation, not per-frame.
        store: Option<Arc<Mutex<GroupStore>>>,
        /// Frames of the current (not-yet-finished) group, buffered so the
        /// completed group can be appended to `store` on rotation.
        group_frames: Vec<Bytes>,
    },
    Ietf07(Box<dialect07::ResilientPublisher>),
}

impl FramePublisher {
    async fn write(&mut self, data: Vec<u8>, group_size: usize) -> anyhow::Result<()> {
        match self {
            FramePublisher::Lite {
                track,
                group,
                count,
                group_seq_file,
                store,
                group_frames,
            } => {
                let b = Bytes::from(data);
                group.write_frame(b.clone())?;
                if store.is_some() {
                    group_frames.push(b);
                }
                *count += 1;
                if *count >= group_size {
                    let finished_seq = group.sequence;
                    let mut old = std::mem::replace(group, track.append_group()?);
                    old.finish()?;
                    *count = 0;
                    // Persist the completed group to disk before recording the
                    // new sequence, so the store never advertises a group it
                    // hasn't durably stored.
                    if let Some(store) = store {
                        store
                            .lock()
                            .unwrap()
                            .append(finished_seq, now_ms(), group_frames)?;
                        group_frames.clear();
                    }
                    // Persist on creation (not finish): a consumer only resumes
                    // from a group it actually received, and seeding from
                    // persisted+1 guarantees we never reuse a sequence for
                    // different content across a restart.
                    persist_group_seq(group_seq_file, group.sequence);
                    tracing::debug!(sequence = group.sequence, "rotated group");
                }
                Ok(())
            }
            FramePublisher::Ietf07(p) => p.write(Bytes::from(data), group_size).await,
        }
    }

    /// Drop stored groups older than the replay window (best-effort).
    fn gc(&mut self, window_secs: u64) {
        if let FramePublisher::Lite {
            store: Some(store), ..
        } = self
        {
            if window_secs == 0 {
                return;
            }
            let cutoff = now_ms().saturating_sub(window_secs * 1000);
            if let Err(err) = store.lock().unwrap().gc(cutoff) {
                tracing::warn!(?err, "group store gc failed");
            }
        }
    }

    fn finish(self) -> anyhow::Result<()> {
        match self {
            FramePublisher::Lite {
                mut track,
                mut group,
                ..
            } => {
                group.finish()?;
                track.finish()?;
                Ok(())
            }
            // dropping the subgroup/track writers closes them
            FramePublisher::Ietf07(_) => Ok(()),
        }
    }
}

async fn relay(args: RelayArgs) -> anyhow::Result<()> {
    // _keepalive holds whatever must not drop for publishing to continue
    // (lite: the reconnecting session + origin producer).
    let (publisher, _keepalive): (FramePublisher, Box<dyn std::any::Any>) = match args.dialect {
        Dialect::Lite => {
            let client = args.client.clone().init()?;
            let origin = moq_net::Origin::random().produce();
            let mut broadcast = moq_net::Broadcast::new().produce();
            let mut track = broadcast.create_track(moq_net::Track {
                name: args.track.clone(),
                priority: 0,
            })?;
            apply_replay_window(&mut track, args.replay_window_secs)?;
            origin.publish_broadcast(&args.broadcast, broadcast.consume());
            // auto-reconnecting session: publishing resumes after drops
            let session = client
                .with_publish(origin.consume())
                .reconnect(args.moq_host.clone());
            let publisher = build_lite_publisher(
                track,
                args.group_seq_file.clone(),
                &args.group_store_dir,
                args.replay_window_secs,
            )?;
            (publisher, Box::new((session, origin, broadcast)))
        }
        Dialect::Ietf07 => (
            FramePublisher::Ietf07(Box::new(dialect07::ResilientPublisher::new(
                args.moq_host.clone(),
                args.client.bind,
                args.broadcast.clone(),
                args.track.clone(),
            ))),
            Box::new(()),
        ),
    };
    tracing::info!(url = %args.moq_host, broadcast = %args.broadcast, "publishing to MoQ relay");

    let last_seq = load_initial_cursor(&args.cursor_file, args.cursor)?;
    let rx = spawn_ingest(args.relay_host.clone(), last_seq.clone());
    // Disk retention / deep-replay depth: defaults to the RAM window (Tier A).
    let backfill_secs = args.backfill_window_secs;
    pump(
        rx,
        publisher,
        args.group_size,
        backfill_secs,
        args.cursor_file,
        last_seq,
    )
    .await
}

/// Serve MoQ subscribers directly from this process: the firehose broadcast
/// lives in a local origin and accepted sessions may only consume it
/// (`with_publish` only — no session can publish into us, so there is no
/// namespace to squat).
async fn serve(args: ServeArgs) -> anyhow::Result<()> {
    let origin = moq_net::Origin::random().produce();
    let mut broadcast = moq_net::Broadcast::new().produce();
    let mut track = broadcast.create_track(moq_net::Track {
        name: args.track.clone(),
        priority: 0,
    })?;
    apply_replay_window(&mut track, args.replay_window_secs)?;
    origin.publish_broadcast(&args.broadcast, broadcast.consume());
    let publisher = build_lite_publisher(
        track,
        args.group_seq_file.clone(),
        &args.group_store_dir,
        args.replay_window_secs,
    )?;

    // human-facing web frontend: :80 redirect + TLS landing page
    if let Ok(bind) = args.web_bind.parse::<std::net::SocketAddr>() {
        let host = args.host.clone();
        tokio::spawn(async move {
            if let Err(err) = atmoq::web::serve_redirect(bind, host).await {
                tracing::warn!(?err, "http redirect server failed");
            }
        });
    }
    if let Ok(bind) = args.web_tls_bind.parse::<std::net::SocketAddr>() {
        match (args.server.tls.cert.first(), args.server.tls.key.first()) {
            (Some(cert), Some(key)) => {
                let page = atmoq::web::landing_page(&args.host, &args.broadcast, &args.track);
                let (cert, key) = (cert.clone(), key.clone());
                tokio::spawn(async move {
                    if let Err(err) = atmoq::web::serve_landing(bind, &cert, &key, page).await {
                        tracing::warn!(?err, "https landing server failed");
                    }
                });
            }
            _ => tracing::info!("no --tls-cert/--tls-key; skipping https landing page (dev mode)"),
        }
    }

    let mut server = args.server.init()?;
    tracing::info!(broadcast = %args.broadcast, "serving MoQ subscribers");
    tokio::spawn(async move {
        while let Some(request) = server.accept().await {
            let consume = origin.consume();
            tokio::spawn(async move {
                match request.with_publish(consume).ok().await {
                    Ok(session) => {
                        tracing::info!("subscriber connected");
                        if let Err(err) = session.closed().await {
                            tracing::debug!(?err, "subscriber session ended");
                        }
                    }
                    Err(err) => tracing::warn!(?err, "session rejected"),
                }
            });
        }
        tracing::warn!("server accept loop ended");
    });

    let last_seq = load_initial_cursor(&args.cursor_file, args.cursor)?;
    let rx = spawn_ingest(args.relay_host.clone(), last_seq.clone());
    // Disk retention / deep-replay depth: defaults to the RAM window (Tier A).
    let backfill_secs = args.backfill_window_secs;
    let result = pump(
        rx,
        publisher,
        args.group_size,
        backfill_secs,
        args.cursor_file,
        last_seq,
    )
    .await;
    drop(broadcast);
    result
}

type SharedSeq = std::sync::Arc<std::sync::atomic::AtomicI64>;

/// Cursor file takes precedence over the flag.
fn load_initial_cursor(
    cursor_file: &Option<std::path::PathBuf>,
    cursor: Option<i64>,
) -> anyhow::Result<SharedSeq> {
    let initial = match cursor_file {
        Some(path) => match std::fs::read_to_string(path) {
            Ok(s) => {
                let c = s.trim().parse::<i64>()?;
                tracing::info!(cursor = c, path = %path.display(), "resuming from cursor file");
                Some(c)
            }
            Err(_) => cursor,
        },
        None => cursor,
    };
    Ok(std::sync::Arc::new(std::sync::atomic::AtomicI64::new(
        initial.unwrap_or(-1),
    )))
}

/// Upstream ingest with reconnect + cursor resume (at-sync §4.3); frames at
/// or below the last relayed seq are dropped downstream as duplicates.
fn spawn_ingest(upstream: String, last_seq: SharedSeq) -> mpsc::Receiver<Frame> {
    let (tx, rx) = mpsc::channel(256);
    tokio::spawn(async move {
        let mut backoff = 1u64;
        loop {
            let cursor = match last_seq.load(Ordering::Relaxed) {
                -1 => None,
                s => Some(s),
            };
            let started = std::time::Instant::now();
            match ingest::subscribe_repos(&upstream, cursor, tx.clone()).await {
                Ok(()) => tracing::warn!("upstream ended; reconnecting"),
                Err(err) => tracing::warn!(?err, "upstream error; reconnecting"),
            }
            if tx.is_closed() {
                return;
            }
            if started.elapsed() > std::time::Duration::from_secs(60) {
                backoff = 1;
            }
            tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(60);
        }
    });
    rx
}

/// Main pump: upstream frames → MoQ publisher, with seq dedupe, periodic
/// cursor persistence, and a clean group finish on ctrl-c.
async fn pump(
    mut rx: mpsc::Receiver<Frame>,
    mut publisher: FramePublisher,
    group_size: usize,
    window_secs: u64,
    cursor_file: Option<std::path::PathBuf>,
    last_seq: SharedSeq,
) -> anyhow::Result<()> {
    let mut total = 0u64;
    let mut cursor_tick = tokio::time::interval(std::time::Duration::from_secs(5));
    cursor_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // GC the disk store on a slower cadence than cursor flushes.
    let mut gc_tick = tokio::time::interval(std::time::Duration::from_secs(30));
    gc_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let frame = tokio::select! {
            f = rx.recv() => f,
            _ = cursor_tick.tick() => {
                persist_cursor(&cursor_file, &last_seq);
                continue;
            }
            _ = gc_tick.tick() => {
                publisher.gc(window_secs);
                continue;
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("interrupted; finishing group");
                break;
            }
        };
        let Some(frame) = frame else { break };
        if let Some(seq) = frame.seq {
            if seq <= last_seq.load(Ordering::Relaxed) {
                continue; // reconnect replay duplicate
            }
            last_seq.store(seq, Ordering::Relaxed);
        }
        publisher.write(frame.raw, group_size).await?;
        total += 1;
        if total.is_multiple_of(100) {
            tracing::info!(total, t = ?frame.t, seq = ?frame.seq, "relaying");
        }
    }

    publisher.finish()?;
    persist_cursor(&cursor_file, &last_seq);
    tracing::info!(total, "shutting down");
    Ok(())
}

fn persist_cursor(path: &Option<std::path::PathBuf>, last_seq: &std::sync::atomic::AtomicI64) {
    let Some(path) = path else { return };
    let seq = last_seq.load(Ordering::Relaxed);
    if seq < 0 {
        return;
    }
    if let Err(err) = std::fs::write(path, format!("{seq}\n")) {
        tracing::warn!(?err, path = %path.display(), "failed to persist cursor");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_seq_persist_roundtrip() {
        let dir = std::env::temp_dir().join(format!("atmoq-gseq-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = Some(dir.join("gseq.txt"));

        // No file yet -> no seed (fresh start at 0).
        assert_eq!(load_group_seq(&path), None);

        // Persist then load round-trips, and trailing newline/whitespace is fine.
        persist_group_seq(&path, 1234);
        assert_eq!(load_group_seq(&path), Some(1234));
        persist_group_seq(&path, 2_000_000);
        assert_eq!(load_group_seq(&path), Some(2_000_000));

        // None path is a no-op (never panics, never reads).
        persist_group_seq(&None, 5);
        assert_eq!(load_group_seq(&None), None);

        std::fs::remove_dir_all(&dir).ok();
    }
}
