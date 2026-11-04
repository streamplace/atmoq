//! Upstream firehose ingest: a WebSocket client for
//! `com.atproto.sync.subscribeRepos` yielding parsed [`Frame`]s.

use crate::frame::Frame;
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use url::Url;

/// Bound on TCP + TLS + WebSocket upgrade.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// We never write otherwise, so without pings a peer that vanished without a
/// FIN leaves the socket ESTABLISHED forever (prod, 2026-10-28).
const PING_INTERVAL: Duration = Duration::from_secs(15);
/// Drop the connection after this long with nothing received, pongs included.
const IDLE_TIMEOUT: Duration = Duration::from_secs(45);

/// Connect to an upstream firehose and stream frames into a channel.
/// `upstream` is the host base (e.g. `wss://bsky.network` or
/// `ws://localhost:2583`); the XRPC path is appended here.
pub async fn subscribe_repos(
    upstream: &str,
    cursor: Option<i64>,
    tx: mpsc::Sender<Frame>,
    metrics: &crate::metrics::Metrics,
) -> Result<()> {
    let mut url = Url::parse(upstream).context("parsing upstream URL")?;
    url.set_path("/xrpc/com.atproto.sync.subscribeRepos");
    if let Some(c) = cursor {
        url.set_query(Some(&format!("cursor={c}")));
    }

    tracing::info!(%url, "connecting to upstream firehose");
    let (ws, _resp) = tokio::time::timeout(
        CONNECT_TIMEOUT,
        tokio_tungstenite::connect_async(url.as_str()),
    )
    .await
    .context("websocket connect timed out")?
    .context("websocket connect")?;
    relay_frames(ws, tx, metrics).await
}

/// Read frames until the connection closes, errors, or goes idle.
async fn relay_frames<S>(
    ws: WebSocketStream<S>,
    tx: mpsc::Sender<Frame>,
    metrics: &crate::metrics::Metrics,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut write, mut read) = ws.split();
    let mut ping = tokio::time::interval_at(Instant::now() + PING_INTERVAL, PING_INTERVAL);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Re-armed lazily when it fires, rather than reset on every frame.
    let mut last_rx = Instant::now();
    let idle = tokio::time::sleep(IDLE_TIMEOUT);
    tokio::pin!(idle);

    loop {
        let msg = tokio::select! {
            // Buffered frames win over the idle check after a pump stall.
            biased;
            msg = read.next() => msg,
            () = &mut idle => {
                let deadline = last_rx + IDLE_TIMEOUT;
                if Instant::now() < deadline {
                    idle.as_mut().reset(deadline);
                    continue;
                }
                anyhow::bail!("upstream sent nothing for {IDLE_TIMEOUT:?}; assuming the connection is dead");
            }
            _ = ping.tick() => {
                // A socket that can't take a ping is dead too.
                tokio::time::timeout(PING_INTERVAL, write.send(Message::Ping(Vec::new())))
                    .await
                    .context("websocket ping timed out")?
                    .context("websocket ping")?;
                continue;
            }
        };
        let Some(msg) = msg else { break };
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
                    // republished; the relay carries valid frames only. Counted
                    // because a rising reject rate means either an upstream
                    // change or a validator bug, and both are silent otherwise.
                    metrics
                        .frames_rejected_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
        // Stamped after handling, so pump backpressure doesn't count as silence.
        last_rx = Instant::now();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// In-memory WebSocket pair, so the paused clock can skip to the timeouts.
    async fn ws_pair() -> (
        WebSocketStream<tokio::io::DuplexStream>,
        WebSocketStream<tokio::io::DuplexStream>,
    ) {
        let (client_io, server_io) = tokio::io::duplex(1 << 16);
        let server = tokio::spawn(tokio_tungstenite::accept_async(server_io));
        let (client, _) = tokio_tungstenite::client_async("ws://upstream/", client_io)
            .await
            .unwrap();
        (client, server.await.unwrap().unwrap())
    }

    #[tokio::test(start_paused = true)]
    async fn silent_upstream_is_dropped() {
        // The prod shape: upgraded, then nothing ever arrives, not even pongs.
        let (client, _server_never_polled) = ws_pair().await;
        let (tx, _rx) = mpsc::channel(1);
        let started = Instant::now();
        let result = relay_frames(client, tx, &Default::default()).await;
        assert!(result.is_err(), "a silent upstream must end with an error");
        assert_eq!(started.elapsed(), IDLE_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn quiet_but_live_upstream_is_kept() {
        // A quiet dev PDS that answers pings must stay connected.
        let (client, mut server) = ws_pair().await;
        tokio::spawn(async move { while server.next().await.is_some() {} });
        let (tx, _rx) = mpsc::channel(1);
        let metrics = Default::default();
        let run = relay_frames(client, tx, &metrics);
        assert!(
            tokio::time::timeout(IDLE_TIMEOUT * 10, run).await.is_err(),
            "a ponging upstream must not be dropped"
        );
    }
}
