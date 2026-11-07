use std::time::Duration;

use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use web_async::FuturesExt;
use web_transport_trait::Stats;

use crate::{
	AsPath, BroadcastConsumer, Error, Origin, OriginConsumer, OriginList, StatsHandle as MoqStats, Track,
	TrackConsumer,
	coding::{Stream, Writer},
	lite::{
		self,
		priority::{Priority, PriorityHandle, PriorityQueue},
	},
	model::{Group, GroupConsumer},
};

use super::Version;

/// Groups packed into each replay stream; replay throughput per round trip.
const REPLAY_GROUPS_PER_STREAM: u64 = 16;

pub(super) struct PublisherConfig<S: web_transport_trait::Session> {
	pub session: S,
	/// The origin we read local broadcasts from. None gives this session a
	/// dummy, immediately-closed origin (i.e. nothing to publish).
	pub origin: Option<OriginConsumer>,
	/// Stats aggregator for this session's egress. Use [`MoqStats::default`]
	/// to opt out.
	pub stats: MoqStats,
	pub version: Version,
}

pub(super) struct Publisher<S: web_transport_trait::Session> {
	session: S,
	origin: OriginConsumer,
	stats: MoqStats,
	/// Per-session egress broadcast-subscription tracker. Each downstream
	/// subscription holds a guard so `broadcasts - broadcasts_closed` counts
	/// the distinct sessions (viewers) watching each broadcast.
	broadcasts: crate::SessionBroadcasts,
	self_origin: Origin,
	priority: PriorityQueue,
	version: Version,
}

impl<S: web_transport_trait::Session> Publisher<S> {
	pub fn new(config: PublisherConfig<S>) -> Self {
		// Default to a dummy origin that is immediately closed.
		let origin = config.origin.unwrap_or_else(|| Origin::random().produce().consume());
		// Identity stamped onto outbound announce hops. Derived from the
		// origin we're consuming so it matches the local relay identity
		// across every session, required for cross-session loop detection.
		let self_origin = *origin;
		let broadcasts = config.stats.publisher_broadcasts();
		Self {
			session: config.session,
			origin,
			stats: config.stats,
			broadcasts,
			self_origin,
			priority: Default::default(),
			version: config.version,
		}
	}

	pub async fn run(mut self) -> Result<(), Error> {
		loop {
			let mut stream = Stream::accept(&self.session, self.version).await?;

			// To avoid cloning the origin, we process each control stream in received order.
			// This adds some head-of-line blocking but it delays an expensive clone.
			let kind = stream.reader.decode().await?;

			if let Err(err) = match kind {
				lite::ControlType::Announce => self.recv_announce(stream).await,
				lite::ControlType::Subscribe => self.recv_subscribe(stream).await,
				lite::ControlType::Probe => {
					self.recv_probe(stream);
					Ok(())
				}
				lite::ControlType::Goaway => {
					tracing::info!("received goaway stream");
					Ok(())
				}
				lite::ControlType::Session | lite::ControlType::Fetch => Err(Error::UnexpectedStream),
			} {
				tracing::warn!(%err, "control stream error");
			}
		}
	}

	fn recv_probe(&self, mut stream: Stream<S, Version>) {
		let session = self.session.clone();
		let version = self.version;

		web_async::spawn(async move {
			match Self::run_probe(&session, &mut stream, version).await {
				Ok(()) => {
					tracing::debug!("probe stream closed");
				}
				Err(err) => {
					tracing::warn!(%err, "probe stream error");
					stream.writer.abort(&err);
				}
			}
		});
	}

	async fn run_probe(session: &S, stream: &mut Stream<S, Version>, _version: Version) -> Result<(), Error> {
		const PROBE_INTERVAL: Duration = Duration::from_millis(100);
		const PROBE_MAX_AGE: Duration = Duration::from_secs(10);
		const PROBE_MAX_DELTA: f64 = 0.25;

		let mut last_sent: Option<(u64, tokio::time::Instant)> = None;
		let mut interval = tokio::time::interval(PROBE_INTERVAL);

		loop {
			tokio::select! {
				res = stream.reader.closed() => return res,
				_ = interval.tick() => {}
			}

			let Some(bitrate) = session.stats().estimated_send_rate() else {
				continue;
			};

			let should_send = match last_sent {
				None => true,
				Some((0, _)) => bitrate > 0,
				Some((prev, at)) => {
					let elapsed = at.elapsed().as_secs_f64();
					let t = elapsed.clamp(PROBE_INTERVAL.as_secs_f64(), PROBE_MAX_AGE.as_secs_f64());
					let range = PROBE_MAX_AGE.as_secs_f64() - PROBE_INTERVAL.as_secs_f64();
					let threshold = PROBE_MAX_DELTA * (PROBE_MAX_AGE.as_secs_f64() - t) / range;
					let change = (bitrate as f64 - prev as f64).abs() / prev as f64;
					change >= threshold
				}
			};

			if should_send {
				let rtt = session.stats().rtt().map(|d| d.as_millis() as u64);
				stream.writer.encode(&lite::Probe { bitrate, rtt }).await?;
				last_sent = Some((bitrate, tokio::time::Instant::now()));
			}
		}
	}

	pub async fn recv_announce(&mut self, mut stream: Stream<S, Version>) -> Result<(), Error> {
		let interest = stream.reader.decode::<lite::AnnounceInterest>().await?;
		let prefix = interest.prefix.to_owned();
		let exclude_hop = interest.exclude_hop;

		let mut origin = self.origin.scope(&[prefix.as_path()]).ok_or(Error::Unauthorized)?;

		let version = self.version;
		let self_origin = self.self_origin;
		let stats = self.stats.clone();
		web_async::spawn(async move {
			if let Err(err) = Self::run_announce(
				&mut stream,
				&mut origin,
				&prefix,
				self_origin,
				exclude_hop,
				stats,
				version,
			)
			.await
			{
				match &err {
					Error::Cancel | Error::Transport(_) => {
						tracing::debug!(prefix = %origin.absolute(prefix), "announcing cancelled");
					}
					err => {
						tracing::warn!(%err, prefix = %origin.absolute(prefix), "announcing error");
					}
				}

				stream.writer.abort(&err);
			}
		});

		Ok(())
	}

	async fn run_announce(
		stream: &mut Stream<S, Version>,
		origin: &mut OriginConsumer,
		prefix: impl AsPath,
		self_origin: Origin,
		// Peer's session-level origin id, sent in AnnounceInterest. We skip
		// forwarding announces whose hop chain already contains this id, so
		// reflected announces (cluster loops) never hit the wire. Zero means
		// the peer didn't set it (Lite03 or earlier), pass through.
		exclude_hop: u64,
		stats: MoqStats,
		version: Version,
	) -> Result<(), Error> {
		let prefix = prefix.as_path();

		// Per-path stats guards: dropping the guard records `broadcasts_closed`.
		// The origin contract guarantees announce/unannounce toggles per path, so a
		// new active announcement must always be for a path with no live guard.
		let mut stats_guards: std::collections::HashMap<crate::PathOwned, crate::PublisherStats> =
			std::collections::HashMap::new();

		match version {
			Version::Lite01 | Version::Lite02 => {
				let mut init = Vec::new();

				// Send ANNOUNCE_INIT as the first message with all currently active paths
				// We use `try_next()` to synchronously get the initial updates.
				while let Some((path, active)) = origin.try_announced() {
					let suffix = path.strip_prefix(&prefix).expect("origin returned invalid path");

					if active.is_some() {
						tracing::debug!(broadcast = %origin.absolute(&path), "announce");
						let absolute = origin.absolute(&path).to_owned();
						let guard = stats.broadcast(&absolute).publisher();
						let prev = stats_guards.insert(absolute, guard);
						debug_assert!(prev.is_none(), "origin announced a path that was already active");
						init.push(suffix.to_owned());
					} else {
						// A potential race.
						tracing::debug!(broadcast = %origin.absolute(&path), "unannounce");
						stats_guards.remove(&origin.absolute(&path).to_owned());
						init.retain(|path| path != &suffix);
					}
				}

				let announce_init = lite::AnnounceInit { suffixes: init };
				stream.writer.encode(&announce_init).await?;
			}
			_ => {
				// Lite03+: no more announce init.
			}
		}

		// Send updates as they arrive.
		loop {
			tokio::select! {
				biased;
				res = stream.reader.closed() => return res,
				announced = origin.announced() => {
					match announced {
						Some((path, active)) => {
							let suffix = path.strip_prefix(&prefix).expect("origin returned invalid path").to_owned();

							if let Some(active) = active {
								// Skip if the peer asked us to exclude announces whose hop chain
								// contains their id — they already saw this broadcast upstream.
								if exclude_hop != 0 && active.hops.iter().any(|h| h.id == exclude_hop) {
									tracing::debug!(
										broadcast = %origin.absolute(&path),
										%exclude_hop,
										"skipping announce per peer's exclude_hop",
									);
									continue;
								}
								// Defense in depth: never echo an announce that already passed
								// through us. The subscriber should drop these before they reach
								// our origin, but if one slips through, don't propagate the loop.
								if active.hops.contains(&self_origin) {
									tracing::debug!(
										broadcast = %origin.absolute(&path),
										"skipping reflected announce",
									);
									continue;
								}
								tracing::debug!(broadcast = %origin.absolute(&path), "announce");
								// Append our origin id to the hops so the next relay can detect loops.
								// If the chain is already at MAX_HOPS, skip the announce — this link is
								// effectively unreachable and the peer will eventually prune the loop.
								let mut hops = active.hops.clone();
								if hops.push(self_origin).is_err() {
									tracing::warn!(
										broadcast = %origin.absolute(&path),
										"dropping announce; hop chain at MAX_HOPS (possible loop)",
									);
									continue;
								}
								let absolute = origin.absolute(&path).to_owned();
								let guard = stats.broadcast(&absolute).publisher();
								let prev = stats_guards.insert(absolute, guard);
								debug_assert!(prev.is_none(), "origin announced a path that was already active");
								let msg = lite::Announce::Active { suffix, hops };
								stream.writer.encode(&msg).await?;
							} else {
								tracing::debug!(broadcast = %origin.absolute(&path), "unannounce");
								stats_guards.remove(&origin.absolute(&path).to_owned());
								// An ended announce doesn't need hops — the receiver matches on path only.
								let msg = lite::Announce::Ended {
									suffix,
									hops: OriginList::new(),
								};
								stream.writer.encode(&msg).await?;
							}
						},
						None => {
							stream.writer.finish()?;
							return stream.writer.closed().await;
						}
					}
				}
			}
		}
	}

	pub async fn recv_subscribe(&mut self, mut stream: Stream<S, Version>) -> Result<(), Error> {
		let subscribe = stream.reader.decode::<lite::Subscribe>().await?;

		let id = subscribe.id;
		let track = subscribe.track.clone();
		let absolute = self.origin.absolute(&subscribe.broadcast).to_owned();

		tracing::info!(%id, broadcast = %absolute, %track, "subscribed started");

		// We just received a subscribe for this exact path, so by definition the peer has
		// already seen an announcement for it — synchronous lookup is appropriate here.
		let broadcast = self.origin.get_broadcast(&subscribe.broadcast);
		let priority = self.priority.clone();
		let version = self.version;

		// Per-track subscription guard (bumps `subscriptions`). The per-(session,
		// broadcast) `broadcasts` sentinel that counts viewers is taken inside
		// `run_subscribe`, only once the subscription is validated and active, so
		// a stale/invalid SUBSCRIBE isn't counted as a viewer.
		let track_stats = self.stats.broadcast(&absolute).publisher_track(&track);
		let broadcasts = self.broadcasts.clone();

		let session = self.session.clone();
		web_async::spawn(async move {
			if let Err(err) = Self::run_subscribe(
				session,
				&mut stream,
				&subscribe,
				broadcast,
				priority,
				(track_stats, broadcasts, absolute.clone()),
				version,
			)
			.await
			{
				match &err {
					// TODO better classify WebTransport errors.
					Error::Cancel | Error::Transport(_) => {
						tracing::info!(%id, broadcast = %absolute, %track, "subscribed cancelled")
					}
					err => {
						tracing::warn!(%id, broadcast = %absolute, %track, %err, "subscribed error")
					}
				}
				stream.writer.abort(&err);
			} else {
				tracing::info!(%id, broadcast = %absolute, %track, "subscribed complete")
			}
		});

		Ok(())
	}

	async fn run_subscribe(
		session: S,
		stream: &mut Stream<S, Version>,
		subscribe: &lite::Subscribe<'_>,
		consumer: Option<BroadcastConsumer>,
		priority: PriorityQueue,
		// The track guard (bumps `subscriptions`), the per-session broadcast
		// tracker, and the broadcast path. The `broadcasts` sentinel is taken
		// below, after the subscription is validated, and held for its lifetime.
		stats: (crate::PublisherTrack, crate::SessionBroadcasts, crate::PathOwned),
		version: Version,
	) -> Result<(), Error> {
		let (track_stats, broadcasts, absolute) = stats;
		let track = Track {
			name: subscribe.track.to_string(),
			priority: subscribe.priority,
		};

		let broadcast = consumer.ok_or(Error::NotFound)?;
		let track = broadcast.subscribe_track(&track)?;

		// Subscription is now active: count this session as a viewer of the
		// broadcast. Dropping this guard (subscription end) releases it.
		let _broadcast_sub = broadcasts.subscribe(&absolute);

		// TODO wait until track.info() to get the *real* priority

		let info = lite::SubscribeOk {
			priority: track.priority,
			ordered: false,
			max_latency: std::time::Duration::ZERO,
			start_group: None,
			end_group: None,
		};

		stream.writer.encode(&lite::SubscribeResponse::Ok(info)).await?;

		// Track-level subscriber priority. SUBSCRIBE_UPDATE messages broadcast new values
		// to both run_track (so future groups inherit the new priority) and serve_group
		// tasks (so in-flight groups update via PriorityHandle::set_track).
		let (track_priority_tx, track_priority_rx) = tokio::sync::watch::channel(track.priority);
		let track_stats = std::sync::Arc::new(track_stats);

		tokio::select! {
			res = Self::run_track(session, track, subscribe, priority, track_stats, track_priority_rx, version) => res?,
			res = Self::run_subscribe_updates(&mut stream.reader, &track_priority_tx) => res?,
		}

		stream.writer.finish()?;
		stream.writer.closed().await
	}

	async fn run_subscribe_updates<R: web_transport_trait::RecvStream>(
		reader: &mut crate::coding::Reader<R, Version>,
		priority_tx: &tokio::sync::watch::Sender<u8>,
	) -> Result<(), Error> {
		while let Some(upd) = reader.decode_maybe::<lite::SubscribeUpdate>().await? {
			let _ = priority_tx.send(upd.priority);
		}
		Ok(())
	}

	async fn run_track(
		session: S,
		mut track: TrackConsumer,
		subscribe: &lite::Subscribe<'_>,
		priority: PriorityQueue,
		track_stats: std::sync::Arc<crate::PublisherTrack>,
		mut track_priority: tokio::sync::watch::Receiver<u8>,
		version: Version,
	) -> Result<(), Error> {
		let mut tasks = FuturesUnordered::new();

		// Replay an explicit start_group in order from the cache, or the publisher's group
		// source (e.g. disk), up to the in-progress group, then go live from there. Replay
		// keeps one stream in flight so bytes land in order (concurrent streams deadlock
		// in-order readers on connection flow control) and packs several groups per stream
		// under the first one's sequence (unpacked, each costs a round trip: slower than live).
		let mut start = track.latest();
		if let Some(requested) = subscribe.start_group {
			let source = track.group_source();
			let oldest = source.as_ref().and_then(|s| s.oldest()).or_else(|| track.oldest());
			let mut next = oldest.map_or(requested, |oldest| requested.max(oldest));
			// Re-read the live edge each pass: replay outruns the live rate, so `next` converges on it.
			while let Some(latest) = track.latest().filter(|&latest| next < latest) {
				let end = latest.min(next.saturating_add(REPLAY_GROUPS_PER_STREAM));
				let mut producer = None;
				for sequence in next..end {
					// Cached groups below the live edge are complete; older ones come from the source.
					if let Some(mut group) = track.get_group(sequence).now_or_never().transpose()?.flatten() {
						while let Some(frame) = group.read_frame().await? {
							producer
								.get_or_insert_with(|| Group { sequence }.produce())
								.write_frame(frame)?;
						}
					} else if let Some(frames) = source.as_ref().and_then(|s| s.group(sequence)) {
						for frame in frames {
							producer
								.get_or_insert_with(|| Group { sequence }.produce())
								.write_frame(frame)?;
						}
					}
				}
				next = end;
				let Some(mut producer) = producer else {
					continue;
				};
				producer.finish()?;

				let msg = lite::Group {
					subscribe: subscribe.id,
					sequence: producer.sequence,
				};
				let current_priority = *track_priority.borrow_and_update();
				let handle = priority.insert(Priority::new(current_priority, producer.sequence));
				Self::serve_group(
					session.clone(),
					msg,
					handle,
					producer.consume(),
					track_stats.clone(),
					track_priority.clone(),
					version,
				)
				.await?;
			}
			start = Some(next);
		}
		if let Some(start) = start {
			track.start_at(start);
		}

		loop {
			let group = tokio::select! {
				// Poll all active group futures; never matches but keeps them running.
				true = async {
					while tasks.next().await.is_some() {}
					false
				} => unreachable!(),
				Some(group) = track.recv_group().transpose() => group,
				else => return Ok(()),
			}?;

			let sequence = group.sequence;
			tracing::debug!(subscribe = %subscribe.id, track = %track.name, sequence, "serving group");

			let msg = lite::Group {
				subscribe: subscribe.id,
				sequence,
			};

			// Use the latest priority for new groups so SUBSCRIBE_UPDATE applies to them too.
			let current_priority = *track_priority.borrow_and_update();
			let handle = priority.insert(Priority::new(current_priority, sequence));
			// `tasks` is unbounded, and each pending entry holds a GroupConsumer
			// that keeps the group's frames alive past the track cache's own
			// eviction. A subscriber that stops draining therefore pins group
			// memory for as long as it stays connected. The guard rides along
			// with the task so the count drops on completion *and* on
			// cancellation. See crate::inflight.
			let in_flight = crate::InFlightGroup::new();
			tasks.push(
				Self::serve_group(
					session.clone(),
					msg,
					handle,
					group,
					track_stats.clone(),
					track_priority.clone(),
					version,
				)
				.map(move |_| drop(in_flight)),
			);
		}
	}

	async fn serve_group(
		session: S,
		msg: lite::Group,
		mut priority: PriorityHandle,
		mut group: GroupConsumer,
		track_stats: std::sync::Arc<crate::PublisherTrack>,
		mut track_priority: tokio::sync::watch::Receiver<u8>,
		version: Version,
	) -> Result<(), Error> {
		let stream = session.open_uni().await.map_err(Error::from_transport)?;

		let mut stream = Writer::new(stream, version);
		stream.set_priority(priority.current());
		stream.encode(&lite::DataType::Group).await?;
		stream.encode(&msg).await?;
		track_stats.group();

		loop {
			let frame = tokio::select! {
				biased;
				_ = stream.closed() => return Err(Error::Cancel),
				frame = group.next_frame() => frame,
				new_pri = priority.next() => {
					stream.set_priority(new_pri);
					continue;
				}
				Ok(()) = track_priority.changed() => {
					priority.set_track(*track_priority.borrow_and_update());
					continue;
				}
			};

			let mut frame = match frame? {
				Some(frame) => frame,
				None => break,
			};

			stream.encode(&frame.size).await?;
			track_stats.frame();

			loop {
				let chunk = tokio::select! {
					biased;
					_ = stream.closed() => return Err(Error::Cancel),
					chunk = frame.read_chunk() => chunk,
					new_pri = priority.next() => {
						stream.set_priority(new_pri);
						continue;
					}
					Ok(()) = track_priority.changed() => {
						priority.set_track(*track_priority.borrow_and_update());
						continue;
					}
				};

				match chunk? {
					Some(mut chunk) => {
						let n = chunk.len() as u64;
						loop {
							tokio::select! {
								biased;
								result = stream.write_all(&mut chunk) => {
									result?;
									break;
								}
								new_pri = priority.next() => {
									stream.set_priority(new_pri);
								}
								Ok(()) = track_priority.changed() => {
									priority.set_track(*track_priority.borrow_and_update());
								}
							}
						}
						track_stats.bytes(n);
					}
					None => break,
				}
			}
		}

		stream.finish()?;
		stream.closed().await?;

		tracing::debug!(sequence = %msg.sequence, "finished group");

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::coding::Decode;
	use bytes::{Buf, Bytes};
	use std::sync::{Arc, Mutex};

	const RTT: Duration = Duration::from_millis(100);

	#[derive(Debug)]
	struct FakeError;

	impl std::fmt::Display for FakeError {
		fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
			write!(f, "fake transport error")
		}
	}

	impl std::error::Error for FakeError {}

	impl web_transport_trait::Error for FakeError {
		fn session_error(&self) -> Option<(u32, String)> {
			None
		}
	}

	#[derive(Default)]
	struct Wire {
		/// Bytes written to each uni stream, in open order.
		streams: Vec<Vec<u8>>,
		/// Streams opened but not yet acked, and the most seen at once.
		unacked: usize,
		max_unacked: usize,
	}

	#[derive(Clone, Default)]
	struct RttSession {
		wire: Arc<Mutex<Wire>>,
	}

	impl web_transport_trait::Session for RttSession {
		type SendStream = RttStream;
		type RecvStream = NoRecv;
		type Error = FakeError;

		async fn accept_uni(&self) -> Result<Self::RecvStream, Self::Error> {
			std::future::pending().await
		}

		async fn accept_bi(&self) -> Result<(Self::SendStream, Self::RecvStream), Self::Error> {
			std::future::pending().await
		}

		async fn open_bi(&self) -> Result<(Self::SendStream, Self::RecvStream), Self::Error> {
			std::future::pending().await
		}

		async fn open_uni(&self) -> Result<Self::SendStream, Self::Error> {
			let mut wire = self.wire.lock().unwrap();
			wire.streams.push(Vec::new());
			wire.unacked += 1;
			wire.max_unacked = wire.max_unacked.max(wire.unacked);
			Ok(RttStream {
				wire: self.wire.clone(),
				index: wire.streams.len() - 1,
				finished: false,
			})
		}

		fn send_datagram(&self, _payload: Bytes) -> Result<(), Self::Error> {
			Ok(())
		}

		async fn recv_datagram(&self) -> Result<Bytes, Self::Error> {
			std::future::pending().await
		}

		fn max_datagram_size(&self) -> usize {
			1200
		}

		fn protocol(&self) -> Option<&str> {
			None
		}

		fn close(&self, _code: u32, _reason: &str) {}

		async fn closed(&self) -> Self::Error {
			std::future::pending().await
		}
	}

	/// A send stream whose `closed()` resolves one RTT after `finish()`, like a QUIC peer's ack.
	struct RttStream {
		wire: Arc<Mutex<Wire>>,
		index: usize,
		finished: bool,
	}

	impl web_transport_trait::SendStream for RttStream {
		type Error = FakeError;

		async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
			self.wire.lock().unwrap().streams[self.index].extend_from_slice(buf);
			Ok(buf.len())
		}

		fn set_priority(&mut self, _order: u8) {}

		fn finish(&mut self) -> Result<(), Self::Error> {
			self.finished = true;
			Ok(())
		}

		fn reset(&mut self, _code: u32) {}

		async fn closed(&mut self) -> Result<(), Self::Error> {
			if !self.finished {
				std::future::pending::<()>().await;
			}
			tokio::time::sleep(RTT).await;
			self.wire.lock().unwrap().unacked -= 1;
			Ok(())
		}
	}

	struct NoRecv;

	impl web_transport_trait::RecvStream for NoRecv {
		type Error = FakeError;

		async fn read(&mut self, _dst: &mut [u8]) -> Result<Option<usize>, Self::Error> {
			std::future::pending().await
		}

		fn stop(&mut self, _code: u32) {}

		async fn closed(&mut self) -> Result<(), Self::Error> {
			std::future::pending().await
		}
	}

	/// Stored groups `[0, end)`, each holding one frame with its own sequence; `missing` is absent.
	struct Source {
		end: u64,
		missing: u64,
	}

	impl crate::GroupSource for Source {
		fn group(&self, sequence: u64) -> Option<Vec<Bytes>> {
			(sequence < self.end && sequence != self.missing).then(|| vec![Bytes::from(sequence.to_string())])
		}

		fn oldest(&self) -> Option<u64> {
			Some(0)
		}
	}

	// A resume must not cost a round trip per group (slower than live on high-latency
	// links), and must keep one stream in flight so groups land in order: concurrent
	// streams deadlock in-order readers on connection flow control.
	#[tokio::test(start_paused = true)]
	async fn replay_packs_groups_into_one_stream_at_a_time() {
		const STORED: u64 = 64;
		const LIVE: u64 = STORED + 8;
		let version = Version::Lite04;

		let mut producer = Track::new("t").produce();
		// Like atmoq's store, the source also holds the completed groups still cached.
		// Group 0 is missing, so the first pack is labeled with group 1.
		producer
			.set_group_source(Arc::new(Source { end: LIVE, missing: 0 }))
			.unwrap();
		let mut cached: Vec<_> = (STORED..LIVE)
			.map(|sequence| {
				let mut group = producer.create_group(Group { sequence }).unwrap();
				group.write_frame(Bytes::from(sequence.to_string())).unwrap();
				group.finish().unwrap();
				group
			})
			.collect();
		cached.push(producer.create_group(Group { sequence: LIVE }).unwrap());

		let session = RttSession::default();
		let subscribe = lite::Subscribe {
			id: 0,
			broadcast: crate::Path::new("b"),
			track: "t".into(),
			priority: 0,
			ordered: true,
			max_latency: Duration::ZERO,
			start_group: Some(0),
			end_group: None,
		};
		let stats = Arc::new(MoqStats::default().broadcast("b").publisher_track("t"));
		let (_priority_tx, priority_rx) = tokio::sync::watch::channel(0);

		let start = tokio::time::Instant::now();
		let run = Publisher::run_track(
			session.clone(),
			producer.consume(),
			&subscribe,
			PriorityQueue::default(),
			stats,
			priority_rx,
			version,
		);
		// The in-progress group's stream opens once replay has drained.
		let replayed = async {
			loop {
				let last = session.wire.lock().unwrap().streams.last().cloned();
				if last.and_then(|s| parse(&s, version)).map(|(label, _)| label) == Some(LIVE) {
					break;
				}
				tokio::time::sleep(Duration::from_millis(1)).await;
			}
		};
		tokio::select! {
			res = run => panic!("run_track ended early: {res:?}"),
			_ = replayed => {}
		}

		// Unpacked replay takes LIVE round trips; packing must cut that well down.
		assert!(
			start.elapsed() < RTT * (LIVE as u32) / 4,
			"replay of {LIVE} groups took {:?} at {RTT:?} RTT",
			start.elapsed()
		);

		let wire = session.wire.lock().unwrap();
		assert_eq!(wire.max_unacked, 1, "replay streams must not overlap");

		let (labels, frames): (Vec<u64>, Vec<Vec<String>>) =
			wire.streams.iter().map(|s| parse(s, version).unwrap()).unzip();
		assert_eq!(labels.first(), Some(&1), "labels {labels:?}");
		assert!(labels.windows(2).all(|w| w[0] < w[1]), "labels {labels:?}");
		let expected: Vec<String> = (1..LIVE).map(|s| s.to_string()).collect();
		assert_eq!(frames.concat(), expected);
	}

	/// Decode a group stream into its sequence label and frames.
	fn parse(stream: &[u8], version: Version) -> Option<(u64, Vec<String>)> {
		let mut buf = Bytes::copy_from_slice(stream);
		lite::DataType::decode(&mut buf, version).ok()?;
		let label = lite::Group::decode(&mut buf, version).ok()?.sequence;
		let mut frames = Vec::new();
		while buf.has_remaining() {
			let size = u64::decode(&mut buf, version).ok()? as usize;
			frames.push(String::from_utf8(buf.split_to(size).to_vec()).ok()?);
		}
		Some((label, frames))
	}
}
