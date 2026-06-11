//! atmoq: atproto firehose over MoQ transport.
//!
//! CLI shape follows goat (github.com/bluesky-social/goat) where the
//! commands overlap: `atmoq firehose` streams events like
//! `goat firehose`, accepting either a WebSocket relay (--relay-host) or a
//! MoQ relay (--moq-host) as the source. `atmoq relay` is the bridge:
//! it consumes a WebSocket firehose and republishes it over MoQ.

use atmoq::{dialect07, frame::Frame, ingest, json::cbor_to_json};
use base64::Engine;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;

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
    /// Bridge a WebSocket firehose onto a MoQ broadcast
    Relay(RelayArgs),
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
    /// Method, hostname, and port of WebSocket relay/PDS instance
    #[arg(long, default_value = "wss://bsky.network")]
    relay_host: String,
    /// MoQ relay URL; if set, consume over MoQ instead of WebSocket
    #[arg(long)]
    moq_host: Option<url::Url>,
    /// Broadcast path under the MoQ connection URL's scope
    #[arg(long, default_value = "firehose")]
    broadcast: String,
    /// Track name within the broadcast
    #[arg(long, default_value = "firehose")]
    track: String,
    /// Cursor to consume at (WebSocket source only)
    #[arg(long)]
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
    #[arg(long, default_value = "firehose")]
    broadcast: String,
    /// Track name within the broadcast
    #[arg(long, default_value = "firehose")]
    track: String,
    /// Frames per MoQ group (late-join replay depth / drop granularity)
    #[arg(long, default_value_t = 64)]
    group_size: usize,
    /// Upstream cursor to resume from (overridden by --cursor-file if present)
    #[arg(long)]
    cursor: Option<i64>,
    /// Persist the upstream cursor here; resume from it on restart
    #[arg(long)]
    cursor_file: Option<std::path::PathBuf>,
    /// MoQ wire protocol (use ietf-07 for Cloudflare's relay)
    #[arg(long, value_enum, default_value_t = Dialect::Lite)]
    dialect: Dialect,
    #[command(flatten)]
    client: moq_native::ClientConfig,
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
    }
}

/// (group sequence if from MoQ, raw frame bytes)
type Item = (Option<u64>, Vec<u8>);

async fn firehose(args: FirehoseArgs) -> anyhow::Result<()> {
    let (tx, rx) = mpsc::channel::<Item>(256);
    if let Some(moq_url) = args.moq_host.clone() {
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
        let upstream = args.relay_host.clone();
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

/// Dialect-agnostic frame publisher with internal group rotation.
enum FramePublisher {
    Lite {
        track: moq_net::TrackProducer,
        group: moq_net::GroupProducer,
        count: usize,
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
            } => {
                group.write_frame(Bytes::from(data))?;
                *count += 1;
                if *count >= group_size {
                    let mut old = std::mem::replace(group, track.append_group()?);
                    old.finish()?;
                    *count = 0;
                    tracing::debug!(sequence = group.sequence, "rotated group");
                }
                Ok(())
            }
            FramePublisher::Ietf07(p) => p.write(Bytes::from(data), group_size).await,
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
    let (mut publisher, _keepalive): (FramePublisher, Box<dyn std::any::Any>) = match args.dialect {
        Dialect::Lite => {
            let client = args.client.clone().init()?;
            let origin = moq_net::Origin::random().produce();
            let mut broadcast = moq_net::Broadcast::new().produce();
            let mut track = broadcast.create_track(moq_net::Track {
                name: args.track.clone(),
                priority: 0,
            })?;
            origin.publish_broadcast(&args.broadcast, broadcast.consume());
            // auto-reconnecting session: publishing resumes after drops
            let session = client
                .with_publish(origin.consume())
                .reconnect(args.moq_host.clone());
            let group = track.append_group()?;
            (
                FramePublisher::Lite {
                    track,
                    group,
                    count: 0,
                },
                Box::new((session, origin, broadcast)),
            )
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

    // cursor: file takes precedence over flag
    let initial = match &args.cursor_file {
        Some(path) => match std::fs::read_to_string(path) {
            Ok(s) => {
                let c = s.trim().parse::<i64>()?;
                tracing::info!(cursor = c, path = %path.display(), "resuming from cursor file");
                Some(c)
            }
            Err(_) => args.cursor,
        },
        None => args.cursor,
    };
    let last_seq = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(initial.unwrap_or(-1)));

    // upstream ingest with reconnect + cursor resume (at-sync §4.3); frames
    // at or below the last relayed seq are dropped as reconnect duplicates
    let (tx, mut rx) = mpsc::channel(256);
    let upstream = args.relay_host.clone();
    let ingest_seq = last_seq.clone();
    tokio::spawn(async move {
        let mut backoff = 1u64;
        loop {
            let cursor = match ingest_seq.load(Ordering::Relaxed) {
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

    let mut total = 0u64;
    let mut cursor_tick = tokio::time::interval(std::time::Duration::from_secs(5));
    cursor_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let frame = tokio::select! {
            f = rx.recv() => f,
            _ = cursor_tick.tick() => {
                persist_cursor(&args.cursor_file, &last_seq);
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
        publisher.write(frame.raw, args.group_size).await?;
        total += 1;
        if total.is_multiple_of(100) {
            tracing::info!(total, t = ?frame.t, seq = ?frame.seq, "relaying");
        }
    }

    publisher.finish()?;
    persist_cursor(&args.cursor_file, &last_seq);
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
