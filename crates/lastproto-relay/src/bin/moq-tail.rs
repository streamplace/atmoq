//! Tail a lastproto MoQ broadcast, emitting the same JSONL format as ws-tail:
//! `{"t":"#commit","seq":123,"group":0,"raw":"<base64 frame bytes>"}`.
//!
//! Differential testing: for the same upstream, `raw` values here must be
//! byte-identical to a ws-tail capture (see tests/e2e/harness/diff-frames.mjs).

use base64::Engine;
use clap::Parser;
use lastproto_relay::frame::Frame;
use tokio::sync::mpsc;

#[derive(Parser)]
struct Args {
    /// MoQ relay URL, e.g. https://cdn.moq.dev/anon/<scope> or http://localhost:4443
    moq_url: url::Url,
    /// Broadcast path under the connection URL's scope
    #[arg(long, default_value = "firehose")]
    broadcast: String,
    /// Track name within the broadcast
    #[arg(long, default_value = "firehose")]
    track: String,
    /// Exit after this many frames (0 = run until the track ends)
    #[arg(long, default_value_t = 0)]
    limit: usize,
    /// Exit after this many milliseconds without a frame (0 = never)
    #[arg(long, default_value_t = 0)]
    idle_ms: u64,
    /// Omit raw bytes from output
    #[arg(long)]
    no_raw: bool,
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
    let consumer = origin.consume();
    let _session = client
        .with_consume(origin)
        .connect(args.moq_url.clone())
        .await?;
    tracing::info!(url = %args.moq_url, broadcast = %args.broadcast, "waiting for broadcast");

    let broadcast = consumer
        .announced_broadcast(&args.broadcast)
        .await
        .ok_or_else(|| anyhow::anyhow!("origin closed before broadcast appeared"))?;
    let mut track = broadcast.subscribe_track(&moq_net::Track {
        name: args.track.clone(),
        priority: 0,
    })?;
    tracing::info!("subscribed");

    let (tx, mut rx) = mpsc::channel::<(u64, Vec<u8>)>(256);
    let reader = tokio::spawn(async move {
        while let Some(mut group) = track.next_group().await? {
            let sequence = group.sequence;
            while let Some(data) = group.read_frame().await? {
                if tx.send((sequence, data.to_vec())).await.is_err() {
                    return Ok(());
                }
            }
        }
        anyhow::Ok(())
    });

    let b64 = base64::engine::general_purpose::STANDARD;
    let mut count = 0usize;
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
        let Some((group, raw)) = item else { break };
        let (t, seq) = match Frame::parse(raw.clone()) {
            Ok(f) => (f.t, f.seq),
            Err(err) => {
                tracing::warn!(?err, "frame failed to parse");
                (None, None)
            }
        };
        let mut line = serde_json::json!({ "t": t, "seq": seq, "group": group });
        if !args.no_raw {
            line["raw"] = serde_json::Value::String(b64.encode(&raw));
        }
        println!("{line}");
        count += 1;
        if args.limit > 0 && count >= args.limit {
            return Ok(());
        }
    }
    reader.await??;
    tracing::info!(count, "track ended");
    Ok(())
}
