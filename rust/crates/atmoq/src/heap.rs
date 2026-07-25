//! Heap profiling and allocator statistics, behind the `profiling` feature.
//!
//! Without the feature this is a set of stubs: the binary keeps the system
//! allocator, `/debug/heap` answers 501, and no jemalloc gauges are exported.
//! With it, jemalloc becomes the global allocator (see main.rs) and two things
//! become available:
//!
//! * **Allocator gauges** — `allocated` (live bytes the program asked for) vs
//!   `resident` (pages the allocator holds from the OS). The *ratio* is the
//!   answer to "is this a leak or fragmentation", which took a 122 GiB heap
//!   walk and five experiments to guess at during the 2026-07 relay-fra0
//!   incident. `allocated` tracking RSS means genuine retention; `resident`
//!   climbing while `allocated` stays flat means the allocator is holding
//!   pages it can't return.
//!
//! * **`/debug/heap`** — a jemalloc heap profile naming the call sites holding
//!   live memory, which is the direct answer rather than an inference.
//!
//! Profiling is armed statically by the `_rjem_malloc_conf` symbol in main.rs,
//! so **no environment variable is required** — building with the feature is
//! enough. That is deliberate: jemalloc reads its config before main, and
//! tikv-jemalloc-sys prefixes its symbols, so the variable is
//! `_RJEM_MALLOC_CONF` rather than the `MALLOC_CONF` everyone reaches for.
//! Setting the obvious one leaves profiling silently disarmed.
//!
//! The sample interval is ~512 KiB allocated (`lg_prof_sample:19`) — cheap
//! enough to leave on permanently, and ample to localize a leak of the scale
//! we care about. The output is jemalloc's own format, not pprof: read it with
//! `jeprof --collapsed <binary> <profile>`, which also feeds a flamegraph.

/// Live allocator statistics, in bytes.
#[derive(Debug, Clone, Copy)]
pub struct Stats {
    /// Bytes currently allocated to the application.
    pub allocated: u64,
    /// Bytes in active pages backing allocations.
    pub active: u64,
    /// Bytes the allocator holds resident from the OS.
    pub resident: u64,
    /// Bytes mapped into the address space.
    pub mapped: u64,
    /// Bytes unmapped but retained for reuse.
    pub retained: u64,
}

#[cfg(feature = "profiling")]
mod imp {
    use super::Stats;
    use anyhow::{bail, Context, Result};

    /// Whether jemalloc actually started with profiling enabled. Normally true
    /// whenever the feature is compiled in, since main.rs arms it statically;
    /// it can still be false if something overrode the config via
    /// `_RJEM_MALLOC_CONF`.
    pub fn armed() -> bool {
        // SAFETY: reading a bool-typed mallctl by its documented name.
        unsafe { tikv_jemalloc_ctl::raw::read::<bool>(b"opt.prof\0") }.unwrap_or(false)
    }

    /// Dump a heap profile and return its contents.
    pub fn dump() -> Result<String> {
        if !armed() {
            bail!(
                "jemalloc profiling is compiled in but not armed — something \
                 overrode the built-in config; check _RJEM_MALLOC_CONF in the \
                 environment (note the prefix: MALLOC_CONF has no effect)"
            );
        }
        // jemalloc writes the profile to a path we hand it; there is no API to
        // stream it back, so it goes via a temp file we immediately reclaim.
        let path = std::env::temp_dir().join(format!("atmoq-heap-{}.prof", std::process::id()));
        let path_str = path.to_str().context("profile path is not utf-8")?;
        let c_path =
            std::ffi::CString::new(path_str).context("profile path has an interior nul")?;
        let ptr = c_path.as_ptr();
        // SAFETY: `prof.dump` takes a `*const c_char`; `c_path` outlives the call.
        unsafe { tikv_jemalloc_ctl::raw::write(b"prof.dump\0", ptr) }
            .context("jemalloc prof.dump failed")?;
        let out = std::fs::read_to_string(&path)
            .with_context(|| format!("reading heap profile from {}", path.display()))?;
        std::fs::remove_file(&path).ok();
        Ok(out)
    }

    /// Read allocator statistics. jemalloc's stats are cached behind an epoch
    /// that has to be advanced first, or every read returns the same numbers.
    pub fn stats() -> Option<Stats> {
        tikv_jemalloc_ctl::epoch::advance().ok()?;
        Some(Stats {
            allocated: tikv_jemalloc_ctl::stats::allocated::read().ok()? as u64,
            active: tikv_jemalloc_ctl::stats::active::read().ok()? as u64,
            resident: tikv_jemalloc_ctl::stats::resident::read().ok()? as u64,
            mapped: tikv_jemalloc_ctl::stats::mapped::read().ok()? as u64,
            retained: tikv_jemalloc_ctl::stats::retained::read().ok()? as u64,
        })
    }
}

#[cfg(not(feature = "profiling"))]
mod imp {
    use super::Stats;
    use anyhow::{bail, Result};

    pub fn armed() -> bool {
        false
    }

    pub fn dump() -> Result<String> {
        bail!("built without the `profiling` feature; rebuild with --features profiling")
    }

    pub fn stats() -> Option<Stats> {
        None
    }
}

pub use imp::{armed, dump, stats};

/// Whether the `profiling` feature was compiled in at all. Distinguishes "not
/// built for this" from "built but not armed", which are different operator
/// mistakes with different fixes.
pub const COMPILED_IN: bool = cfg!(feature = "profiling");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stubs_are_consistent_with_the_feature_flag() {
        assert_eq!(COMPILED_IN, cfg!(feature = "profiling"));
        if !COMPILED_IN {
            assert!(!armed());
            assert!(stats().is_none());
            assert!(dump().is_err());
        }
    }
}
