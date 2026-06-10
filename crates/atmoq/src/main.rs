//! atmoq: atproto firehose over MoQ transport.
//!
//! CLI shape follows goat (github.com/bluesky-social/goat) where the
//! commands overlap: `atmoq firehose` streams events like
//! `goat firehose`, accepting either a WebSocket relay (--relay-host) or a
//! MoQ relay (--moq-host) as the source. `atmoq relay` is the bridge:
//! it consumes a WebSocket firehose and republishes it over MoQ.

use base64::Engine;
use bytes::Bytes;
use clap::{Parser, Subcommand};
use atmoq::{frame::Frame, ingest, json::cbor_to_json};
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
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
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
        if args.limit > 0 && count >= args.limit {
            return Ok(());
        }
    }
}

async fn relay(args: RelayArgs) -> anyhow::Result<()> {
    let client = args.client.init()?;
    let origin = moq_net::Origin::random().produce();
    let mut broadcast = moq_net::Broadcast::new().produce();
    let mut track = broadcast.create_track(moq_net::Track {
        name: args.track.clone(),
        priority: 0,
    })?;
    origin.publish_broadcast(&args.broadcast, broadcast.consume());

    // auto-reconnecting session: publishing resumes after relay-side drops
    let session = client
        .with_publish(origin.consume())
        .reconnect(args.moq_host.clone());
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

    let mut group = track.append_group()?;
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
        group.write_frame(Bytes::from(frame.raw))?;
        total += 1;
        if total % 100 == 0 {
            tracing::info!(total, t = ?frame.t, seq = ?frame.seq, "relaying");
        }
        if group.frame_count() >= args.group_size {
            group.finish()?;
            group = track.append_group()?;
            tracing::debug!(sequence = group.sequence, "rotated group");
        }
    }

    group.finish()?;
    track.finish()?;
    persist_cursor(&args.cursor_file, &last_seq);
    drop(session);
    tracing::info!(total, "shutting down");
    Ok(())
}

fn persist_cursor(
    path: &Option<std::path::PathBuf>,
    last_seq: &std::sync::atomic::AtomicI64,
) {
    let Some(path) = path else { return };
    let seq = last_seq.load(Ordering::Relaxed);
    if seq < 0 {
        return;
    }
    if let Err(err) = std::fs::write(path, format!("{seq}\n")) {
        tracing::warn!(?err, path = %path.display(), "failed to persist cursor");
    }
}
