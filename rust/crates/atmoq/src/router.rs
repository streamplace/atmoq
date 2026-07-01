//! On-demand per-DID firehose tracks (selective sync).
//!
//! The aggregate `all` track carries every account's events. For consumers that
//! want only a known, small set of accounts (e.g. the ~50 people in a room),
//! this serves a **per-DID track** addressed by the account's DID as the track
//! name — materialized on demand and torn down when no longer subscribed.
//!
//! MoQ filters only by track (no in-track/subgroup content filter), so the
//! per-account view has to be its own track produced by the origin — the only
//! atproto-aware party. A generic relay just routes the opaque track name. See
//! `docs/design/selective-sync.md`.
//!
//! Mechanism: a consumer subscribes to track name `<did>` within the firehose
//! broadcast. moq-net's dynamic broadcast surfaces that as a [`requested_track`]
//! with the producer preconfigured to the requested name; we register it and
//! fan matching firehose frames into it. moq-net itself drops the track from its
//! lookup once unused; we mirror that in our routing map via `unused()`.
//!
//! [`requested_track`]: moq_net::BroadcastDynamic::requested_track

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;

use crate::frame::{field, Frame};

/// Cap on concurrently-served per-DID tracks — a safety bound on origin
/// memory/CPU. Distinct subscribers requesting the same DID share one track, so
/// this counts unique watched accounts, not subscriptions.
const MAX_DID_TRACKS: usize = 10_000;

/// A per-DID track and its in-progress group, owned by the router.
struct DidTrack {
    track: moq_net::TrackProducer,
    group: moq_net::GroupProducer,
    count: usize,
}

/// Routes firehose frames to the per-DID tracks downstream consumers have
/// subscribed to. Cheap when nothing is subscribed (a single empty-map check).
#[derive(Clone)]
pub struct DidRouter {
    active: Arc<Mutex<HashMap<String, DidTrack>>>,
    group_size: usize,
}

impl DidRouter {
    /// Attach to a broadcast's dynamic producer and start accepting per-DID
    /// track requests. `replay_window_secs` is applied to each per-DID track
    /// (the in-RAM resume window, matching the aggregate track).
    pub fn spawn(
        mut dynamic: moq_net::BroadcastDynamic,
        group_size: usize,
        replay_window_secs: u64,
    ) -> Self {
        let active: Arc<Mutex<HashMap<String, DidTrack>>> = Arc::new(Mutex::new(HashMap::new()));
        let router = DidRouter {
            active: active.clone(),
            group_size,
        };

        tokio::spawn(async move {
            loop {
                let mut producer = match dynamic.requested_track().await {
                    Ok(p) => p,
                    Err(err) => {
                        tracing::debug!(?err, "dynamic broadcast closed; stopping DID router");
                        return;
                    }
                };
                // The producer is preconfigured with the requested track name,
                // which for selective sync is the account's DID.
                let did = producer.name.clone();
                if !is_did(&did) {
                    tracing::debug!(track = %did, "rejecting non-DID track request");
                    producer.abort(moq_net::Error::NotFound).ok();
                    continue;
                }
                if replay_window_secs > 0 {
                    producer
                        .set_max_group_age(Duration::from_secs(replay_window_secs))
                        .ok();
                }
                let group = match producer.append_group() {
                    Ok(g) => g,
                    Err(err) => {
                        tracing::warn!(?err, %did, "failed to open per-DID group");
                        continue;
                    }
                };

                {
                    let mut map = active.lock().unwrap();
                    if map.len() >= MAX_DID_TRACKS {
                        tracing::warn!(%did, max = MAX_DID_TRACKS, "per-DID track cap reached; rejecting");
                        producer.abort(moq_net::Error::NotFound).ok();
                        continue;
                    }
                    map.insert(
                        did.clone(),
                        DidTrack {
                            track: producer.clone(),
                            group,
                            count: 0,
                        },
                    );
                }
                tracing::info!(%did, "serving per-DID track");

                // Mirror moq-net's own unused-track cleanup: when the last
                // subscriber leaves, drop the track from our routing map (which
                // drops the producer and closes the track).
                let active = active.clone();
                let watch = producer.clone();
                tokio::spawn(async move {
                    let _ = watch.unused().await;
                    active.lock().unwrap().remove(&did);
                    tracing::info!(%did, "per-DID track idle; dropped");
                });
            }
        });

        router
    }

    /// Fan a firehose frame out to its account's per-DID track, if one is being
    /// served. A no-op (single map check) when no per-DID tracks are active or
    /// when the frame's account isn't subscribed.
    pub fn route(&self, raw: &[u8]) {
        let mut map = self.active.lock().unwrap();
        if map.is_empty() {
            return;
        }
        let Some(did) = event_did(raw) else { return };
        let Some(track) = map.get_mut(&did) else {
            return;
        };
        if track
            .group
            .write_frame(Bytes::copy_from_slice(raw))
            .is_err()
        {
            return; // track closing; the unused() watcher will remove it
        }
        track.count += 1;
        if track.count >= self.group_size {
            if let Ok(next) = track.track.append_group() {
                let mut old = std::mem::replace(&mut track.group, next);
                old.finish().ok();
                track.count = 0;
            }
        }
    }
}

/// The account a firehose event belongs to: `repo` for `#commit`, otherwise
/// `did` (`#identity` / `#account` / `#sync`). None if the payload has neither.
fn event_did(raw: &[u8]) -> Option<String> {
    let (_, payload) = Frame::decode(raw).ok()?;
    let v = field(&payload, "repo").or_else(|| field(&payload, "did"))?;
    v.as_text().map(str::to_owned)
}

/// Light DID syntax gate so junk track names can't spin up tracks.
fn is_did(name: &str) -> bool {
    name.starts_with("did:") && (7..=256).contains(&name.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ciborium::value::Value;

    fn frame_bytes(t: &str, did_key: &str, did: &str) -> Vec<u8> {
        // Frames must be valid DRISL now, so keys go in bytewise-encoded order
        // ("t" before "op"; payload sorted below), as real PDS encoders emit.
        let header = Value::Map(vec![
            (Value::Text("t".into()), Value::Text(t.into())),
            (Value::Text("op".into()), Value::Integer(1.into())),
        ]);
        let mut entries = vec![
            (Value::Text(did_key.into()), Value::Text(did.into())),
            (Value::Text("seq".into()), Value::Integer(42.into())),
        ];
        entries.sort_by_key(|(k, _)| {
            let mut b = Vec::new();
            ciborium::ser::into_writer(k, &mut b).unwrap();
            b
        });
        let payload = Value::Map(entries);
        let mut raw = Vec::new();
        ciborium::ser::into_writer(&header, &mut raw).unwrap();
        ciborium::ser::into_writer(&payload, &mut raw).unwrap();
        raw
    }

    #[test]
    fn event_did_reads_repo_then_did() {
        let commit = frame_bytes("#commit", "repo", "did:plc:alice");
        assert_eq!(event_did(&commit).as_deref(), Some("did:plc:alice"));

        let identity = frame_bytes("#identity", "did", "did:plc:bob");
        assert_eq!(event_did(&identity).as_deref(), Some("did:plc:bob"));
    }

    #[test]
    fn event_did_none_without_account() {
        let header = Value::Map(vec![(Value::Text("op".into()), Value::Integer(1.into()))]);
        let payload = Value::Map(vec![(Value::Text("seq".into()), Value::Integer(1.into()))]);
        let mut raw = Vec::new();
        ciborium::ser::into_writer(&header, &mut raw).unwrap();
        ciborium::ser::into_writer(&payload, &mut raw).unwrap();
        assert_eq!(event_did(&raw), None);
    }

    #[test]
    fn is_did_gate() {
        assert!(is_did("did:plc:abc123"));
        assert!(is_did("did:web:example.com"));
        assert!(!is_did("all"));
        assert!(!is_did("did:")); // too short
        assert!(!is_did(""));
    }
}
