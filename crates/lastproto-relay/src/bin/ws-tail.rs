//! Tail an at-sync firehose over WebSocket, emitting one JSON line per frame:
//! `{"t":"#commit","seq":123,"raw":"<base64 frame bytes>"}`.
//!
//! Ground-truth capture for differential tests (the MoQ tail must produce
//! byte-identical `raw` values for the same upstream).

use base64::Engine;
use clap::Parser;
use lastproto_relay::ingest;
use tokio::sync::mpsc;

#[derive(Parser)]
struct Args {
    /// Upstream host, e.g. wss://bsky.network or ws://localhost:2583
    upstream: String,
    /// Resume from this cursor
    #[arg(long)]
    cursor: Option<i64>,
    /// Exit after this many frames (0 = run forever)
    #[arg(long, default_value_t = 0)]
    limit: usize,
    /// Omit raw bytes from output
    #[arg(long)]
    no_raw: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::ring::default_provider()
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

    let (tx, mut rx) = mpsc::channel(256);
    let upstream = args.upstream.clone();
    let ingest =
        tokio::spawn(async move { ingest::subscribe_repos(&upstream, args.cursor, tx).await });

    let b64 = base64::engine::general_purpose::STANDARD;
    let mut count = 0usize;
    while let Some(frame) = rx.recv().await {
        let mut line = serde_json::json!({ "t": frame.t, "seq": frame.seq });
        if !args.no_raw {
            line["raw"] = serde_json::Value::String(b64.encode(&frame.raw));
        }
        println!("{line}");
        count += 1;
        if args.limit > 0 && count >= args.limit {
            return Ok(());
        }
    }
    ingest.await??;
    Ok(())
}
