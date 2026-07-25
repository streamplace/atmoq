//! Process-wide gauge for groups queued but not yet delivered to subscribers.
//!
//! The lite publisher serves each group on its own QUIC stream, spawned into an
//! unbounded [`futures::stream::FuturesUnordered`] in `run_track`. A subscriber
//! that stops draining — flow-control stalled, or its own downstream blocked —
//! cannot be served, so those tasks accumulate. Each one holds a
//! [`crate::GroupConsumer`], which keeps that group's frames alive **past the
//! track cache's own eviction**. Memory then grows with the publish rate for as
//! long as the subscriber stays connected and stuck.
//!
//! Nothing about that is visible from outside the process: QUIC multiplexes
//! every session onto one UDP socket, so there is no socket, no fd, and no
//! queue depth to inspect. This gauge is the only way to see it.
//!
//! It is a global rather than per-session because the sessions that would own
//! it are created deep inside the session accept path, with no handle reaching
//! the embedding application. A per-subscription breakdown would be better and
//! is the natural follow-up if this ever needs to attribute the backlog to a
//! specific peer; the total is enough to answer "is anything backed up".

use std::sync::atomic::{AtomicU64, Ordering};

static GROUPS_IN_FLIGHT: AtomicU64 = AtomicU64::new(0);
static GROUPS_IN_FLIGHT_MAX: AtomicU64 = AtomicU64::new(0);

/// Groups currently queued for delivery across all subscriptions.
///
/// Steady state for a healthy subscriber is a small number — the publisher
/// keeps up with the group rate. A value that climbs without bound is a stuck
/// subscriber pinning group memory, and is the signature to alert on.
pub fn groups_in_flight() -> u64 {
	GROUPS_IN_FLIGHT.load(Ordering::Relaxed)
}

/// High-water mark of [`groups_in_flight`] since process start.
///
/// Retained separately because the interesting event is usually over by the
/// time anyone looks: a subscriber can back up, accumulate, and disconnect
/// between two scrapes, leaving the instantaneous gauge at zero and no trace
/// that anything happened.
pub fn groups_in_flight_max() -> u64 {
	GROUPS_IN_FLIGHT_MAX.load(Ordering::Relaxed)
}

/// Counts one group as in flight for as long as this guard lives.
///
/// Held by the task serving the group, so the decrement happens on completion
/// *and* on cancellation (the task being dropped when a session ends) — a
/// manual decrement on the success path would leak the count on every
/// disconnect, which is precisely the case being measured.
#[derive(Debug)]
pub struct InFlightGroup(());

impl InFlightGroup {
	/// Record a group as queued for delivery.
	pub fn new() -> Self {
		let now = GROUPS_IN_FLIGHT.fetch_add(1, Ordering::Relaxed) + 1;
		// Not a CAS loop: a lost race under-reports the peak by at most the
		// concurrent increment, and this is a diagnostic, not an invariant.
		GROUPS_IN_FLIGHT_MAX.fetch_max(now, Ordering::Relaxed);
		Self(())
	}
}

impl Default for InFlightGroup {
	fn default() -> Self {
		Self::new()
	}
}

impl Drop for InFlightGroup {
	fn drop(&mut self) {
		GROUPS_IN_FLIGHT.fetch_sub(1, Ordering::Relaxed);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// Serialized: the counters are process-global, so these cannot run
	// concurrently with each other and stay deterministic.
	#[test]
	fn guard_tracks_and_releases() {
		let base = groups_in_flight();
		{
			let _a = InFlightGroup::new();
			assert_eq!(groups_in_flight(), base + 1);
			{
				let _b = InFlightGroup::new();
				assert_eq!(groups_in_flight(), base + 2);
				assert!(groups_in_flight_max() >= base + 2);
			}
			assert_eq!(groups_in_flight(), base + 1);
		}
		assert_eq!(groups_in_flight(), base);
	}

	#[test]
	fn max_is_monotonic() {
		let peak = {
			let _a = InFlightGroup::new();
			let _b = InFlightGroup::new();
			groups_in_flight_max()
		};
		// Dropping the guards must not walk the high-water mark back down.
		assert_eq!(groups_in_flight_max(), peak);
	}
}
