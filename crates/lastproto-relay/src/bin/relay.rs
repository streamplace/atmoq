//! lastproto relay prototype: subscribe to an at-sync firehose over WebSocket
//! and republish each frame, byte-for-byte, on a MoQ broadcast.
//!
//! One combined-order track carries every event type (PLAN.md §3.2: the
//! combined track is canonical; per-type split tracks come later). Groups
//! rotate every `--group-size` frames; late joiners start at the latest
//! group boundary and recover anything earlier from the PDS fleet
//! (docs/decisions/0001).

use bytes::Bytes;
use clap::Parser;
use lastproto_relay::ingest;
use tokio::sync::mpsc;

#[derive(Parser)]
struct Args {
    /// Upstream firehose host, e.g. wss://bsky.network or ws://localhost:2583
    upstream: String,
    /// MoQ relay URL to publish through, e.g. https://cdn.moq.dev/anon/<scope>
    /// or http://localhost:4443 (dev: cert fingerprint auto-fetched)
    moq_url: url::Url,
    /// Broadcast path published under the connection URL's scope
    #[arg(long, default_value = "firehose")]
    broadcast: String,
    /// Track name within the broadcast
    #[arg(long, default_value = "firehose")]
    track: String,
    /// Frames per MoQ group (late-join replay depth / drop granularity)
    #[arg(long, default_value_t = 64)]
    group_size: usize,
    /// Upstream cursor to resume from
    #[arg(long)]
    cursor: Option<i64>,
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
    let args = Args::parse();

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
        .reconnect(args.moq_url.clone());
    tracing::info!(url = %args.moq_url, broadcast = %args.broadcast, "publishing to MoQ relay");

    // upstream ingest with reconnect + cursor resume (at-sync §4.3); frames
    // at or below the last relayed seq are dropped as reconnect duplicates
    let last_seq = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(
        args.cursor.unwrap_or(-1),
    ));
    let (tx, mut rx) = mpsc::channel(256);
    let upstream = args.upstream.clone();
    let ingest_seq = last_seq.clone();
    tokio::spawn(async move {
        let mut backoff = 1u64;
        loop {
            let cursor = match ingest_seq.load(std::sync::atomic::Ordering::Relaxed) {
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
    loop {
        let frame = tokio::select! {
            f = rx.recv() => f,
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("interrupted; finishing group");
                break;
            }
        };
        let Some(frame) = frame else { break };
        if let Some(seq) = frame.seq {
            if seq <= last_seq.load(std::sync::atomic::Ordering::Relaxed) {
                continue; // reconnect replay duplicate
            }
            last_seq.store(seq, std::sync::atomic::Ordering::Relaxed);
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
    drop(session);
    tracing::info!(total, "shutting down");
    Ok(())
}
