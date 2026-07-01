//! Disk-backed, group-aligned store for the firehose: every MoQ group's frames
//! are appended to rotating segment files, indexed by group sequence, and
//! garbage-collected by age. This is the durability substrate for replay:
//!
//! - it recovers the high-water group sequence after a restart (so ids stay
//!   monotonic — see `max_seq`), and
//! - it lets recent groups be reloaded into the live track on startup so a
//!   relay restart doesn't drop the replay window (Tier A), and
//! - it is the source a deeper disk-served replay window would read from
//!   (Tier B; needs a moq-net publisher hook — see docs/design/replay.md).
//!
//! Record format (little-endian) within a segment file:
//!   [group_seq: u64][created_ms: u64][frame_count: u32]
//!   then frame_count × ([frame_len: u32][frame bytes])
//!
//! Segments are named `<first_seq:020>.seg` so they sort lexicographically by
//! starting sequence. GC deletes whole segments once their newest group is older
//! than the retention window.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use bytes::Bytes;

/// Rotate to a new segment once the current one passes this size.
const SEGMENT_TARGET_BYTES: u64 = 64 * 1024 * 1024;

/// Where a group lives on disk.
#[derive(Clone, Copy)]
struct Location {
    segment: usize, // index into `segments`
    offset: u64,
}

struct Segment {
    path: PathBuf,
    first_seq: u64,
    newest_ms: u64, // created_ms of the newest group in this segment
    bytes: u64,
}

/// A disk-backed group store. Single-writer (the pump loop); reads recover the
/// index built at open time.
pub struct GroupStore {
    dir: PathBuf,
    segments: Vec<Segment>,
    index: BTreeMap<u64, Location>,
    writer: Option<BufWriter<File>>, // appends to the last segment
    max_seq: Option<u64>,
    segment_target: u64,
}

impl GroupStore {
    /// Open (creating if needed) a store at `dir`, scanning existing segments to
    /// rebuild the index and recover the high-water sequence.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_segment_target(dir, SEGMENT_TARGET_BYTES)
    }

    fn open_with_segment_target(dir: impl AsRef<Path>, segment_target: u64) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)
            .with_context(|| format!("creating store dir {}", dir.display()))?;

        let mut store = GroupStore {
            dir,
            segments: Vec::new(),
            index: BTreeMap::new(),
            writer: None,
            max_seq: None,
            segment_target,
        };
        store.scan()?;
        Ok(store)
    }

    /// The highest group sequence on disk, used to seed monotonic ids on restart.
    pub fn max_seq(&self) -> Option<u64> {
        self.max_seq
    }

    /// The lowest group sequence still on disk, or None if empty. This is the
    /// floor of the deep (disk-served) replay window — the oldest group a
    /// resuming subscriber can be backfilled from.
    pub fn oldest_seq(&self) -> Option<u64> {
        self.index.keys().next().copied()
    }

    /// Append a group's frames. `created_ms` is the wall-clock time used for GC.
    pub fn append(&mut self, seq: u64, created_ms: u64, frames: &[Bytes]) -> Result<()> {
        self.ensure_writer(seq)?;
        let seg_idx = self.segments.len() - 1;
        let offset = self.segments[seg_idx].bytes;

        let mut rec = Vec::with_capacity(20);
        rec.extend_from_slice(&seq.to_le_bytes());
        rec.extend_from_slice(&created_ms.to_le_bytes());
        rec.extend_from_slice(&(frames.len() as u32).to_le_bytes());
        for f in frames {
            rec.extend_from_slice(&(f.len() as u32).to_le_bytes());
            rec.extend_from_slice(f);
        }

        let w = self
            .writer
            .as_mut()
            .expect("writer present after ensure_writer");
        w.write_all(&rec)?;
        w.flush()?; // best-effort durability, no fsync (matches the cursor file)

        let seg = &mut self.segments[seg_idx];
        seg.bytes += rec.len() as u64;
        seg.newest_ms = created_ms;
        self.index.insert(
            seq,
            Location {
                segment: seg_idx,
                offset,
            },
        );
        self.max_seq = Some(self.max_seq.map_or(seq, |m| m.max(seq)));
        Ok(())
    }

    /// Read back a group's frames, or None if not stored (evicted or never seen).
    pub fn read(&self, seq: u64) -> Result<Option<Vec<Bytes>>> {
        let Some(loc) = self.index.get(&seq).copied() else {
            return Ok(None);
        };
        let path = &self.segments[loc.segment].path;
        let mut f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        f.seek(SeekFrom::Start(loc.offset))?;
        let (rec_seq, _ms, frames) = read_record(&mut f)?
            .ok_or_else(|| anyhow::anyhow!("truncated record for group {seq}"))?;
        anyhow::ensure!(rec_seq == seq, "index/segment mismatch: {rec_seq} != {seq}");
        Ok(Some(frames))
    }

    /// Group sequences whose group is newer than `cutoff_ms`, ascending — the
    /// set to reload into the live track on startup (the in-window groups).
    pub fn groups_since(&self, cutoff_ms: u64) -> Result<Vec<u64>> {
        let mut out = Vec::new();
        for (&seq, loc) in &self.index {
            let path = &self.segments[loc.segment].path;
            let mut f = File::open(path)?;
            f.seek(SeekFrom::Start(loc.offset))?;
            // Read just the header to get created_ms cheaply.
            let mut hdr = [0u8; 20];
            if f.read_exact(&mut hdr).is_err() {
                continue;
            }
            let ms = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
            if ms >= cutoff_ms {
                out.push(seq);
            }
        }
        out.sort_unstable();
        Ok(out)
    }

    /// Delete whole segments whose newest group is older than `cutoff_ms`. The
    /// active (last) segment is never deleted.
    pub fn gc(&mut self, cutoff_ms: u64) -> Result<usize> {
        let mut removed = 0;
        // Keep the last segment (it's the write target); drop older fully-expired ones.
        while self.segments.len() > 1 {
            let seg = &self.segments[0];
            if seg.newest_ms >= cutoff_ms {
                break; // segments are in ascending time order; nothing older follows
            }
            let first = seg.first_seq;
            let next_first = self.segments[1].first_seq;
            fs::remove_file(&seg.path).ok();
            self.index
                .retain(|&seq, _| seq < first || seq >= next_first);
            self.segments.remove(0);
            // Indices in `index` referenced segment 0; everything shifts down by one.
            for loc in self.index.values_mut() {
                loc.segment = loc.segment.saturating_sub(1);
            }
            removed += 1;
        }
        Ok(removed)
    }

    fn ensure_writer(&mut self, next_seq: u64) -> Result<()> {
        let rotate = match self.segments.last() {
            None => true,
            Some(seg) => seg.bytes >= self.segment_target,
        };
        if rotate {
            // Finish the previous writer.
            if let Some(mut w) = self.writer.take() {
                w.flush()?;
            }
            let path = self.dir.join(format!("{next_seq:020}.seg"));
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("opening segment {}", path.display()))?;
            self.segments.push(Segment {
                path,
                first_seq: next_seq,
                newest_ms: 0,
                bytes: 0,
            });
            self.writer = Some(BufWriter::new(file));
        } else if self.writer.is_none() {
            // Reopen the last segment for appending (e.g. right after scan()).
            let path = self.segments.last().unwrap().path.clone();
            let file = OpenOptions::new().append(true).open(&path)?;
            self.writer = Some(BufWriter::new(file));
        }
        Ok(())
    }

    /// Rebuild segments + index by scanning the directory.
    fn scan(&mut self) -> Result<()> {
        let mut paths: Vec<PathBuf> = fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().map(|x| x == "seg").unwrap_or(false))
            .collect();
        paths.sort();

        for path in paths {
            let mut f = File::open(&path)?;
            let len = f.metadata()?.len();
            let mut offset = 0u64;
            let mut first_seq = None;
            let mut newest_ms = 0u64;
            loop {
                match read_record_header(&mut f)? {
                    None => break,
                    Some((seq, ms, body_len)) => {
                        first_seq.get_or_insert(seq);
                        newest_ms = ms;
                        self.index.insert(
                            seq,
                            Location {
                                segment: self.segments.len(),
                                offset,
                            },
                        );
                        self.max_seq = Some(self.max_seq.map_or(seq, |m| m.max(seq)));
                        offset += 20 + body_len;
                        f.seek(SeekFrom::Start(offset))?;
                    }
                }
            }
            // Tolerate a torn tail record (crash mid-append) — but truncate it
            // away. The writer reopens segments with O_APPEND, which writes at
            // the *physical* EOF; if torn bytes were left in place, every new
            // record would land past the offset the index records for it,
            // making all post-restart groups unreadable (and a second restart
            // would regress max_seq, re-issuing group ids for new content).
            if offset < len {
                drop(f);
                let file = OpenOptions::new().write(true).open(&path)?;
                file.set_len(offset)
                    .with_context(|| format!("truncating torn tail of {}", path.display()))?;
                file.sync_all().ok();
            }
            self.segments.push(Segment {
                first_seq: first_seq.unwrap_or(0),
                newest_ms,
                bytes: offset,
                path,
            });
        }
        Ok(())
    }
}

/// Read a full record (seq, created_ms, frames) at the reader's position.
fn read_record<R: Read>(r: &mut R) -> Result<Option<(u64, u64, Vec<Bytes>)>> {
    let mut hdr = [0u8; 20];
    if !read_full_or_eof(r, &mut hdr)? {
        return Ok(None);
    }
    let seq = u64::from_le_bytes(hdr[0..8].try_into().unwrap());
    let ms = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
    let count = u32::from_le_bytes(hdr[16..20].try_into().unwrap()) as usize;
    let mut frames = Vec::with_capacity(count);
    for _ in 0..count {
        let mut lenb = [0u8; 4];
        if !read_full_or_eof(r, &mut lenb)? {
            return Ok(None); // torn record
        }
        let n = u32::from_le_bytes(lenb) as usize;
        let mut buf = vec![0u8; n];
        if !read_full_or_eof(r, &mut buf)? {
            return Ok(None);
        }
        frames.push(Bytes::from(buf));
    }
    Ok(Some((seq, ms, frames)))
}

/// Read just a record's header and return (seq, created_ms, body_len) where
/// body_len is the size of the frames section, so the caller can skip it.
fn read_record_header<R: Read + Seek>(r: &mut R) -> Result<Option<(u64, u64, u64)>> {
    let start = r.stream_position()?;
    let mut hdr = [0u8; 20];
    if !read_full_or_eof(r, &mut hdr)? {
        return Ok(None);
    }
    let seq = u64::from_le_bytes(hdr[0..8].try_into().unwrap());
    let ms = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
    let count = u32::from_le_bytes(hdr[16..20].try_into().unwrap()) as usize;
    let mut body_len = 0u64;
    for _ in 0..count {
        let mut lenb = [0u8; 4];
        if !read_full_or_eof(r, &mut lenb)? {
            // Torn tail: rewind so this partial record is ignored.
            r.seek(SeekFrom::Start(start))?;
            return Ok(None);
        }
        let n = u32::from_le_bytes(lenb) as u64;
        body_len += 4 + n;
        r.seek(SeekFrom::Current(n as i64))?;
    }
    Ok(Some((seq, ms, body_len)))
}

/// Returns Ok(true) if `buf` was fully read, Ok(false) on EOF before it filled
/// (clean end of file when read==0, a torn tail record when read>0 — callers
/// treat both as "no more record here").
fn read_full_or_eof<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<bool> {
    let mut read = 0;
    while read < buf.len() {
        match r.read(&mut buf[read..])? {
            0 => return Ok(false),
            n => read += n,
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> PathBuf {
        std::env::temp_dir().join(format!(
            "atmoq-store-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn frames(parts: &[&str]) -> Vec<Bytes> {
        parts
            .iter()
            .map(|s| Bytes::from(s.as_bytes().to_vec()))
            .collect()
    }

    #[test]
    fn append_read_roundtrip_and_recover() {
        let dir = tmp();
        {
            let mut s = GroupStore::open(&dir).unwrap();
            s.append(10, 1000, &frames(&["a", "bb"])).unwrap();
            s.append(11, 1100, &frames(&["ccc"])).unwrap();
            s.append(12, 1200, &frames(&[])).unwrap(); // empty group
            assert_eq!(s.max_seq(), Some(12));
            assert_eq!(s.read(11).unwrap().unwrap(), frames(&["ccc"]));
            assert!(s.read(99).unwrap().is_none());
        }
        // Reopen: index + max_seq recovered from disk.
        let s = GroupStore::open(&dir).unwrap();
        assert_eq!(s.max_seq(), Some(12));
        assert_eq!(s.read(10).unwrap().unwrap(), frames(&["a", "bb"]));
        assert_eq!(s.read(12).unwrap().unwrap(), frames(&[]));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn gc_drops_old_segments_keeps_active() {
        let dir = tmp();
        // segment_target = 1 byte forces a new segment per append.
        let mut s = GroupStore::open_with_segment_target(&dir, 1).unwrap();
        s.append(1, 1000, &frames(&["a"])).unwrap();
        s.append(2, 2000, &frames(&["b"])).unwrap();
        s.append(3, 3000, &frames(&["c"])).unwrap();

        // GC everything older than 2500ms: group 1 (1000) and 2 (2000) expire,
        // but the active (last) segment holding group 3 is always kept.
        let removed = s.gc(2500).unwrap();
        assert_eq!(removed, 2);
        assert!(s.read(1).unwrap().is_none());
        assert!(s.read(2).unwrap().is_none());
        assert_eq!(s.read(3).unwrap().unwrap(), frames(&["c"]));
        assert_eq!(s.max_seq(), Some(3));

        // Survives reopen with the surviving group intact.
        let s2 = GroupStore::open(&dir).unwrap();
        assert_eq!(s2.read(3).unwrap().unwrap(), frames(&["c"]));
        assert!(s2.read(1).unwrap().is_none());
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn torn_tail_is_truncated_and_appends_stay_readable() {
        let dir = tmp();
        {
            let mut s = GroupStore::open(&dir).unwrap();
            s.append(1, 1000, &frames(&["a"])).unwrap();
            s.append(2, 2000, &frames(&["bb"])).unwrap();
        }
        // Simulate a crash mid-append: a partial record at the tail (a full
        // header claiming 3 frames, but only part of the first frame).
        let seg = fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.extension().map(|x| x == "seg").unwrap_or(false))
            .unwrap();
        let mut torn = Vec::new();
        torn.extend_from_slice(&3u64.to_le_bytes()); // seq
        torn.extend_from_slice(&3000u64.to_le_bytes()); // created_ms
        torn.extend_from_slice(&3u32.to_le_bytes()); // frame_count
        torn.extend_from_slice(&100u32.to_le_bytes()); // frame len...
        torn.extend_from_slice(b"only-part"); // ...but not the bytes
        {
            let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
            f.write_all(&torn).unwrap();
        }

        // Restart 1: the torn record is ignored AND physically truncated, so
        // new appends land exactly where the index says they do.
        {
            let mut s = GroupStore::open(&dir).unwrap();
            assert_eq!(s.max_seq(), Some(2));
            assert!(s.read(3).unwrap().is_none());
            s.append(3, 3100, &frames(&["ccc"])).unwrap();
            assert_eq!(s.read(3).unwrap().unwrap(), frames(&["ccc"]));
        }

        // Restart 2: everything written after the torn tail is still visible —
        // no max_seq regression, no index/segment mismatch.
        let s = GroupStore::open(&dir).unwrap();
        assert_eq!(s.max_seq(), Some(3));
        assert_eq!(s.read(1).unwrap().unwrap(), frames(&["a"]));
        assert_eq!(s.read(2).unwrap().unwrap(), frames(&["bb"]));
        assert_eq!(s.read(3).unwrap().unwrap(), frames(&["ccc"]));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn groups_since_filters_by_time() {
        let dir = tmp();
        let mut s = GroupStore::open(&dir).unwrap();
        s.append(1, 1000, &frames(&["x"])).unwrap();
        s.append(2, 2000, &frames(&["y"])).unwrap();
        s.append(3, 3000, &frames(&["z"])).unwrap();
        assert_eq!(s.groups_since(2000).unwrap(), vec![2, 3]);
        assert_eq!(s.groups_since(0).unwrap(), vec![1, 2, 3]);
        fs::remove_dir_all(&dir).ok();
    }
}
