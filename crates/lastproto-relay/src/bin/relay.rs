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

    let session = client
        .with_publish(origin.consume())
        .connect(args.moq_url.clone())
        .await?;
    tracing::info!(url = %args.moq_url, broadcast = %args.broadcast, "publishing to MoQ relay");

    let (tx, mut rx) = mpsc::channel(256);
    let upstream = args.upstream.clone();
    let cursor = args.cursor;
    let mut ingest_task =
        tokio::spawn(async move { ingest::subscribe_repos(&upstream, cursor, tx).await });

    let mut group = track.append_group()?;
    let mut total = 0u64;
    loop {
        tokio::select! {
            frame = rx.recv() => {
                let Some(frame) = frame else { break };
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
            res = &mut ingest_task => {
                res??;
                break;
            }
        }
    }

    group.finish()?;
    track.finish()?;
    drop(session);
    tracing::info!(total, "upstream ended; relay shutting down");
    Ok(())
}
