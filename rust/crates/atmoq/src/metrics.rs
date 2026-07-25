//! Process metrics in Prometheus text exposition format.
//!
//! Scraped by VictoriaMetrics from the admin listener (`--metrics-bind`), which
//! is deliberately *not* the public :80/:443 landing page: `serve` faces the
//! open internet, and subscriber counts / cursor position / store size are
//! operational detail, not public information. Bind it to a private interface.
//!
//! Everything here is a plain atomic counter or gauge. The exposition format for
//! counters and gauges is four lines of text per metric, so a client crate would
//! buy escaping rules we don't need (no dynamic labels — every series here is
//! label-free) at the cost of a dependency in the hot path's crate. Histograms
//! would change that calculus; if we ever want latency distributions, take the
//! dep rather than hand-rolling buckets.
//!
//! Counters are monotonic and never reset within a process lifetime. Pairs like
//! `subscribers_connected_total` / `subscribers_disconnected_total` are exposed
//! rather than a single gauge so a scrape gap can't lose an increment — the
//! live count is the difference, computed in the query
//! (`atmoq_subscribers_connected_total - atmoq_subscribers_disconnected_total`),
//! and `atmoq_subscribers_live` is also exported directly for convenience.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// All relay counters. Cheap to bump (relaxed atomics) and safe to share across
/// tasks behind an `Arc`.
#[derive(Default, Debug)]
pub struct Metrics {
    // -- upstream ingest ---------------------------------------------------
    /// Frames accepted from upstream and republished.
    pub frames_total: AtomicU64,
    /// Frames rejected at ingest for invalid DRISL / shape (see drisl.rs).
    pub frames_rejected_total: AtomicU64,
    /// Frames dropped as at-or-below the persisted cursor (reconnect replay).
    pub frames_dropped_below_cursor_total: AtomicU64,
    /// Upstream error frames (op == -1) surfaced and not republished.
    pub upstream_error_frames_total: AtomicU64,
    /// Upstream websocket connects (first connect included, so reconnects = n-1).
    pub upstream_connects_total: AtomicU64,
    /// Latest upstream sequence number seen. The lag-vs-upstream signal: compare
    /// against another relay's value, or watch it stop advancing.
    pub upstream_seq: AtomicI64,
    /// Wall-clock ms of the last relayed frame. Drives the liveness check —
    /// "the process is up" and "the firehose is flowing" are different claims,
    /// and only this one distinguishes them.
    pub last_frame_unix_ms: AtomicU64,

    // -- MoQ publish -------------------------------------------------------
    /// Groups rotated (i.e. completed and published).
    pub groups_total: AtomicU64,
    /// Sequence of the most recently created group.
    pub group_seq: AtomicU64,

    // -- subscribers -------------------------------------------------------
    /// Sessions accepted. QUIC multiplexes every subscriber onto one UDP
    /// socket, so this count is not recoverable from `ss`, fd counts, or any
    /// other outside-the-process source — if it isn't counted here it cannot be
    /// known at all.
    pub subscribers_connected_total: AtomicU64,
    /// Sessions closed, for any reason.
    pub subscribers_disconnected_total: AtomicU64,
    /// Sessions rejected before setup completed.
    pub subscribers_rejected_total: AtomicU64,

    // -- selective sync ----------------------------------------------------
    /// Per-DID tracks materialized on demand.
    pub did_tracks_opened_total: AtomicU64,
    /// Per-DID tracks torn down when their last subscriber left.
    pub did_tracks_closed_total: AtomicU64,
    /// Per-DID track requests refused because the cap was reached.
    pub did_tracks_refused_total: AtomicU64,

    // -- disk store --------------------------------------------------------
    /// Bytes appended to the group store.
    pub store_append_bytes_total: AtomicU64,
    /// Group appends that failed (full disk, bad volume). Replay degrades but
    /// the relay keeps serving live, so this is silent without a metric.
    pub store_append_failures_total: AtomicU64,
    /// Groups currently in the store index.
    pub store_groups_indexed: AtomicU64,
    /// Total size of store segments on disk.
    pub store_bytes: AtomicU64,
    /// Segments deleted by GC (age or size budget).
    pub store_gc_segments_removed_total: AtomicU64,

    // -- deep replay (Tier B) ----------------------------------------------
    /// Groups served to resuming subscribers from disk.
    pub backfill_groups_served_total: AtomicU64,
    /// Groups a resuming subscriber asked for that the store no longer had.
    pub backfill_groups_missing_total: AtomicU64,
}

/// One exported series.
enum Kind {
    Counter,
    Gauge,
}

impl Metrics {
    /// Record that a frame was relayed: bumps the counter, the upstream
    /// sequence, and the liveness timestamp in one call so they can't drift.
    pub fn record_frame(&self, seq: Option<i64>) {
        self.frames_total.fetch_add(1, Ordering::Relaxed);
        if let Some(seq) = seq {
            self.upstream_seq.store(seq, Ordering::Relaxed);
        }
        self.last_frame_unix_ms.store(now_ms(), Ordering::Relaxed);
    }

    /// Live subscriber count. Saturating: the two counters are bumped from
    /// different tasks, so a disconnect can briefly be observed before its
    /// connect on a weakly-ordered read.
    pub fn subscribers_live(&self) -> u64 {
        self.subscribers_connected_total
            .load(Ordering::Relaxed)
            .saturating_sub(self.subscribers_disconnected_total.load(Ordering::Relaxed))
    }

    /// Live per-DID track count (same saturating caveat as above).
    pub fn did_tracks_live(&self) -> u64 {
        self.did_tracks_opened_total
            .load(Ordering::Relaxed)
            .saturating_sub(self.did_tracks_closed_total.load(Ordering::Relaxed))
    }

    /// Milliseconds since the last relayed frame, or `None` before the first
    /// one (startup, when "stale" would be a false alarm).
    pub fn frame_age_ms(&self) -> Option<u64> {
        match self.last_frame_unix_ms.load(Ordering::Relaxed) {
            0 => None,
            ms => Some(now_ms().saturating_sub(ms)),
        }
    }

    /// Render the full exposition. Allocates a fresh String per scrape, which
    /// at a handful of series and a 15s scrape interval is not worth pooling.
    pub fn encode(&self) -> String {
        let mut out = String::with_capacity(4096);
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);

        let series: &[(&str, Kind, &str, u64)] = &[
            (
                "frames_total",
                Kind::Counter,
                "Frames relayed downstream.",
                g(&self.frames_total),
            ),
            (
                "frames_rejected_total",
                Kind::Counter,
                "Frames rejected at ingest as invalid.",
                g(&self.frames_rejected_total),
            ),
            (
                "frames_dropped_below_cursor_total",
                Kind::Counter,
                "Frames dropped as at-or-below the cursor.",
                g(&self.frames_dropped_below_cursor_total),
            ),
            (
                "upstream_error_frames_total",
                Kind::Counter,
                "Upstream error frames received.",
                g(&self.upstream_error_frames_total),
            ),
            (
                "upstream_connects_total",
                Kind::Counter,
                "Upstream websocket connections opened.",
                g(&self.upstream_connects_total),
            ),
            (
                "groups_total",
                Kind::Counter,
                "MoQ groups completed and published.",
                g(&self.groups_total),
            ),
            (
                "group_seq",
                Kind::Gauge,
                "Sequence of the most recent MoQ group.",
                g(&self.group_seq),
            ),
            (
                "subscribers_connected_total",
                Kind::Counter,
                "Subscriber sessions accepted.",
                g(&self.subscribers_connected_total),
            ),
            (
                "subscribers_disconnected_total",
                Kind::Counter,
                "Subscriber sessions closed.",
                g(&self.subscribers_disconnected_total),
            ),
            (
                "subscribers_rejected_total",
                Kind::Counter,
                "Subscriber sessions rejected during setup.",
                g(&self.subscribers_rejected_total),
            ),
            (
                "subscribers_live",
                Kind::Gauge,
                "Subscriber sessions currently connected.",
                self.subscribers_live(),
            ),
            (
                "did_tracks_opened_total",
                Kind::Counter,
                "Per-DID tracks materialized.",
                g(&self.did_tracks_opened_total),
            ),
            (
                "did_tracks_closed_total",
                Kind::Counter,
                "Per-DID tracks torn down.",
                g(&self.did_tracks_closed_total),
            ),
            (
                "did_tracks_refused_total",
                Kind::Counter,
                "Per-DID track requests refused at the cap.",
                g(&self.did_tracks_refused_total),
            ),
            (
                "did_tracks_live",
                Kind::Gauge,
                "Per-DID tracks currently served.",
                self.did_tracks_live(),
            ),
            (
                "store_append_bytes_total",
                Kind::Counter,
                "Bytes appended to the group store.",
                g(&self.store_append_bytes_total),
            ),
            (
                "store_append_failures_total",
                Kind::Counter,
                "Group store appends that failed.",
                g(&self.store_append_failures_total),
            ),
            (
                "store_groups_indexed",
                Kind::Gauge,
                "Groups currently in the store index.",
                g(&self.store_groups_indexed),
            ),
            (
                "store_bytes",
                Kind::Gauge,
                "Group store size on disk in bytes.",
                g(&self.store_bytes),
            ),
            (
                "store_gc_segments_removed_total",
                Kind::Counter,
                "Store segments deleted by GC.",
                g(&self.store_gc_segments_removed_total),
            ),
            (
                "backfill_groups_served_total",
                Kind::Counter,
                "Groups served from disk to resuming subscribers.",
                g(&self.backfill_groups_served_total),
            ),
            (
                "backfill_groups_missing_total",
                Kind::Counter,
                "Groups requested on resume that the store no longer had.",
                g(&self.backfill_groups_missing_total),
            ),
        ];

        for (name, kind, help, value) in series {
            let kind = match kind {
                Kind::Counter => "counter",
                Kind::Gauge => "gauge",
            };
            out.push_str(&format!(
                "# HELP atmoq_{name} {help}\n# TYPE atmoq_{name} {kind}\natmoq_{name} {value}\n"
            ));
        }

        // Signed, so it can't ride in the table above.
        out.push_str(&format!(
            "# HELP atmoq_upstream_seq Latest upstream sequence number seen.\n\
             # TYPE atmoq_upstream_seq gauge\n\
             atmoq_upstream_seq {}\n",
            self.upstream_seq.load(Ordering::Relaxed)
        ));

        // Seconds since the last frame: the alertable freshness signal. Absent
        // before the first frame rather than exported as a misleading zero.
        if let Some(age) = self.frame_age_ms() {
            out.push_str(&format!(
                "# HELP atmoq_last_frame_age_seconds Seconds since the last relayed frame.\n\
                 # TYPE atmoq_last_frame_age_seconds gauge\n\
                 atmoq_last_frame_age_seconds {:.3}\n",
                age as f64 / 1000.0
            ));
        }

        if let Some(rss) = process_rss_bytes() {
            out.push_str(&format!(
                "# HELP atmoq_process_resident_bytes Process resident set size.\n\
                 # TYPE atmoq_process_resident_bytes gauge\n\
                 atmoq_process_resident_bytes {rss}\n"
            ));
        }

        out
    }
}

/// Resident set size from `/proc/self/statm` (field 2 is resident pages).
/// Linux-only; returns None elsewhere, which simply omits the series.
pub fn process_rss_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    // 4 KiB is the page size on every platform this binary ships to; calling
    // sysconf(_SC_PAGESIZE) would mean libc, which the crate otherwise avoids.
    Some(pages * 4096)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_counts_are_differences() {
        let m = Metrics::default();
        for _ in 0..5 {
            m.subscribers_connected_total
                .fetch_add(1, Ordering::Relaxed);
        }
        m.subscribers_disconnected_total
            .fetch_add(2, Ordering::Relaxed);
        assert_eq!(m.subscribers_live(), 3);
    }

    #[test]
    fn live_count_saturates_rather_than_wrapping() {
        // A disconnect observed before its connect must not underflow into
        // u64::MAX and page someone at 3am.
        let m = Metrics::default();
        m.subscribers_disconnected_total
            .fetch_add(1, Ordering::Relaxed);
        assert_eq!(m.subscribers_live(), 0);
    }

    #[test]
    fn frame_age_absent_until_first_frame() {
        let m = Metrics::default();
        assert_eq!(m.frame_age_ms(), None);
        m.record_frame(Some(42));
        assert!(m.frame_age_ms().is_some());
        assert_eq!(m.upstream_seq.load(Ordering::Relaxed), 42);
        assert_eq!(m.frames_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn exposition_shape() {
        let m = Metrics::default();
        m.record_frame(Some(7));
        let text = m.encode();
        assert!(text.contains("# TYPE atmoq_frames_total counter"));
        assert!(text.contains("\natmoq_frames_total 1\n"));
        assert!(text.contains("# TYPE atmoq_subscribers_live gauge"));
        assert!(text.contains("\natmoq_upstream_seq 7\n"));
        // Every metric line must carry the atmoq_ prefix.
        for line in text
            .lines()
            .filter(|l| !l.starts_with('#') && !l.is_empty())
        {
            assert!(line.starts_with("atmoq_"), "unprefixed series: {line}");
        }
    }
}
