//! draft-ietf-moq-transport-07 dialect, for Cloudflare's production relay.
//!
//! Cloudflare's public endpoint (relay.cloudflare.mediaoverquic.com) only
//! speaks draft-07 today, which moq-net deliberately doesn't implement
//! (docs/diag/2026-06-10-public-relays.md). This module wraps the
//! maintenance-branch cloudflare/moq-rs crates behind the same
//! publish/consume shapes the moq-net (lite) path uses. Mapping: our
//! `--broadcast` string becomes the draft-07 track *namespace*; groups are
//! one subgroup each (subgroup_id 0).
//!
//! v1 limitations vs the lite path: no session auto-reconnect (the process
//! exits on session error and systemd/the harness restarts it; upstream
//! cursor state still makes that lossless), and no resubscribe-on-churn.

use anyhow::{Context, Result};
use bytes::Bytes;
use moq_native_07::quic;
use moq_transport_07::{
    coding::Tuple,
    serve,
    session::{Publisher, Subscriber},
};
use tokio::sync::mpsc;

macro_rules! connect {
    ($url:expr, $bind:expr) => {{
        let tls = moq_native_07::tls::Args::default()
            .load()
            .context("loading TLS config")?;
        let quic = quic::Endpoint::new(quic::Config { bind: $bind, tls })?;
        quic.client.connect($url).await.context("quic connect")?
    }};
}

/// Publisher handle: owns the track writer plus the objects that must stay
/// alive for the session/announce tasks to keep serving.
pub struct Publisher07 {
    groups: serve::SubgroupsWriter,
    current: Option<serve::SubgroupWriter>,
    group_id: u64,
    count: usize,
    // serve-layer writes succeed even after the session dies (they buffer
    // locally), so liveness is signalled out-of-band by the session task
    dead: std::sync::Arc<std::sync::atomic::AtomicBool>,
    // keep the tracks writer alive; dropping it closes the track
    _tracks: serve::TracksWriter,
}

pub async fn publish(
    url: &url::Url,
    bind: std::net::SocketAddr,
    namespace: &str,
    track: &str,
) -> Result<Publisher07> {
    let session = connect!(url, bind);
    let (session, mut publisher) = Publisher::connect(session)
        .await
        .context("draft-07 publisher setup")?;

    let (mut tracks, _, reader) = serve::Tracks {
        namespace: Tuple::from_utf8_path(namespace),
    }
    .produce();
    let track = tracks.create(track).context("failed to create track")?;
    let groups = track.groups()?;

    let dead = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let dead_flag = dead.clone();
    tokio::spawn(async move {
        tokio::select! {
            res = session.run() => tracing::warn!(?res, "draft-07 session ended"),
            res = publisher.announce(reader) => tracing::warn!(?res, "draft-07 announce ended"),
        }
        dead_flag.store(true, std::sync::atomic::Ordering::Relaxed);
    });

    // Group IDs must be unique across publisher restarts: relays cache
    // groups per namespace, and an unauthenticated namespace outlives any
    // one session. Restarting at 0 makes subscribers read a mix of stale
    // cached groups and live ones ("wrong size" errors). Epoch millis is
    // monotonic enough across restarts.
    let group_base = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before 1970")
        .as_millis() as u64;

    Ok(Publisher07 {
        groups,
        current: None,
        group_id: group_base,
        count: 0,
        dead,
        _tracks: tracks,
    })
}

impl Publisher07 {
    pub fn is_dead(&self) -> bool {
        self.dead.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn write(&mut self, data: Bytes, group_size: usize) -> Result<()> {
        if self.current.is_none() || self.count >= group_size {
            self.current = Some(self.groups.create(serve::Subgroup {
                group_id: self.group_id,
                subgroup_id: 0,
                priority: 0,
            })?);
            tracing::debug!(group = self.group_id, "rotated group");
            self.group_id += 1;
            self.count = 0;
        }
        self.current
            .as_mut()
            .expect("subgroup just created")
            .write(data)?;
        self.count += 1;
        Ok(())
    }
}

/// Reconnecting publisher: Cloudflare's relay closes whole sessions (e.g.
/// code 0 on idle/peer loss, 404 on missing tracks), so publishing must be
/// prepared to rebuild the session at any time. Frames buffered upstream
/// during the outage are not lost (channel backpressure + cursor resume).
pub struct ResilientPublisher {
    url: url::Url,
    bind: std::net::SocketAddr,
    namespace: String,
    track: String,
    inner: Option<Publisher07>,
}

impl ResilientPublisher {
    pub fn new(
        url: url::Url,
        bind: std::net::SocketAddr,
        namespace: String,
        track: String,
    ) -> Self {
        Self {
            url,
            bind,
            namespace,
            track,
            inner: None,
        }
    }

    pub async fn write(&mut self, data: Bytes, group_size: usize) -> Result<()> {
        loop {
            if self.inner.as_ref().map(|p| p.is_dead()).unwrap_or(true) {
                if self.inner.take().is_some() {
                    tracing::warn!("draft-07 session died; reconnecting");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
                match publish(&self.url, self.bind, &self.namespace, &self.track).await {
                    Ok(p) => {
                        // Probation: an ANNOUNCE rejection (e.g. namespace
                        // already claimed by another publisher) lands a few
                        // hundred ms after connect. Writing before then
                        // silently loses frames into a doomed session.
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        if p.is_dead() {
                            tracing::warn!(
                                "draft-07 announce rejected (namespace claimed?); retrying"
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                            continue;
                        }
                        tracing::info!("draft-07 session established");
                        self.inner = Some(p);
                    }
                    Err(err) => {
                        tracing::warn!(?err, "draft-07 connect failed; retrying");
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        continue;
                    }
                }
            }
            match self
                .inner
                .as_mut()
                .expect("publisher just ensured")
                .write(data.clone(), group_size)
            {
                Ok(()) => return Ok(()),
                Err(err) => {
                    tracing::warn!(?err, "draft-07 write failed; reconnecting");
                    self.inner = None;
                }
            }
        }
    }
}

/// Subscribe with reconnect + backoff until the receiver is dropped.
/// Duplicate frames from replay after a reconnect are deduped downstream
/// by at-sync seq.
pub async fn subscribe_loop(
    url: url::Url,
    bind: std::net::SocketAddr,
    namespace: String,
    track: String,
    tx: mpsc::Sender<(Option<u64>, Vec<u8>)>,
) {
    let mut backoff = 1u64;
    loop {
        let started = std::time::Instant::now();
        match subscribe(&url, bind, &namespace, &track, tx.clone()).await {
            Ok(()) => tracing::warn!("draft-07 track ended; reconnecting"),
            Err(err) => tracing::warn!(?err, "draft-07 subscribe error; reconnecting"),
        }
        if tx.is_closed() {
            return;
        }
        if started.elapsed() > std::time::Duration::from_secs(60) {
            backoff = 1;
        }
        tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}

/// Subscribe and feed `(group, raw frame)` items into `tx` until the session
/// ends or the receiver is dropped.
pub async fn subscribe(
    url: &url::Url,
    bind: std::net::SocketAddr,
    namespace: &str,
    track: &str,
    tx: mpsc::Sender<(Option<u64>, Vec<u8>)>,
) -> Result<()> {
    let session = connect!(url, bind);
    let (session, mut subscriber) = Subscriber::connect(session)
        .await
        .context("draft-07 subscriber setup")?;

    let (prod, sub) =
        serve::Track::new(Tuple::from_utf8_path(namespace), track.to_string()).produce();

    tokio::spawn(async move {
        tokio::select! {
            res = session.run() => tracing::warn!(?res, "draft-07 session ended"),
            res = subscriber.subscribe(prod) => tracing::warn!(?res, "draft-07 subscribe ended"),
        }
    });

    tracing::info!("draft-07 subscribed");
    match sub.mode().await.context("track mode")? {
        serve::TrackReaderMode::Subgroups(mut subgroups) => {
            let mut group = 0u64;
            while let Some(mut subgroup) = subgroups.next().await? {
                while let Some(object) = subgroup.read_next().await? {
                    if tx.send((Some(group), object.to_vec())).await.is_err() {
                        return Ok(());
                    }
                }
                group += 1;
            }
        }
        serve::TrackReaderMode::Stream(mut stream) => {
            let mut group = 0u64;
            while let Some(mut subgroup) = stream.next().await? {
                while let Some(object) = subgroup.read_next().await? {
                    if tx.send((Some(group), object.to_vec())).await.is_err() {
                        return Ok(());
                    }
                }
                group += 1;
            }
        }
        serve::TrackReaderMode::Datagrams(_) => {
            anyhow::bail!("datagram mode unsupported (frames exceed MTU)");
        }
    }
    Ok(())
}
