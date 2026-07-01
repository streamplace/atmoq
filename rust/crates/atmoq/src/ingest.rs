//! Upstream firehose ingest: a WebSocket client for
//! `com.atproto.sync.subscribeRepos` yielding parsed [`Frame`]s.

use crate::frame::Frame;
use anyhow::{Context, Result};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

/// Connect to an upstream firehose and stream frames into a channel.
/// `upstream` is the host base (e.g. `wss://bsky.network` or
/// `ws://localhost:2583`); the XRPC path is appended here.
pub async fn subscribe_repos(
    upstream: &str,
    cursor: Option<i64>,
    tx: mpsc::Sender<Frame>,
) -> Result<()> {
    let mut url = Url::parse(upstream).context("parsing upstream URL")?;
    url.set_path("/xrpc/com.atproto.sync.subscribeRepos");
    if let Some(c) = cursor {
        url.set_query(Some(&format!("cursor={c}")));
    }

    tracing::info!(%url, "connecting to upstream firehose");
    let (ws, _resp) = tokio_tungstenite::connect_async(url.as_str())
        .await
        .context("websocket connect")?;
    let (_write, mut read) = ws.split();

    while let Some(msg) = read.next().await {
        match msg.context("websocket read")? {
            Message::Binary(data) => match Frame::parse(data) {
                Ok(frame) => {
                    if tx.send(frame).await.is_err() {
                        // receiver dropped; shut down quietly
                        return Ok(());
                    }
                }
                Err(err) => {
                    // Rejected: invalid DRISL or not at-sync-shaped. atmoq is
                    // DRISL-strict by design (see drisl.rs) — the frame is not
                    // republished; the relay carries valid frames only.
                    tracing::warn!(err = format!("{err:#}"), "rejecting frame");
                }
            },
            Message::Ping(_) | Message::Pong(_) => {}
            Message::Close(reason) => {
                tracing::info!(?reason, "upstream closed connection");
                break;
            }
            other => {
                tracing::warn!(?other, "ignoring non-binary message");
            }
        }
    }
    Ok(())
}
