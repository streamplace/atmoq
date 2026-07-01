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
//! Record format (little-endian) within a `.seg2` segment file:
//!   [group_seq: u64][created_ms: u64][frame_count: u32][body_len: u32]
//!   then frame_count × ([frame_len: u32][frame bytes]), body_len bytes total
//!
//! `body_len` lets the startup scan skip a whole record with one bounds check
//! instead of walking every frame — at the 72h retention default a store
//! holds hundreds of millions of frames, and the v1 format's
//! read-4-bytes-then-seek per frame turned restart into minutes of syscalls.
//! Legacy `.seg` segments (v1: same header without body_len) remain readable;
//! the writer only appends to `.seg2`, rotating early if the newest segment
//! is v1.
//!
//! Segments are named `<first_seq:020>.seg2` so they sort lexicographically by
//! starting sequence. GC deletes whole segments once their newest group is
//! older than the retention window.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use bytes::Bytes;

/// Rotate to a new segment once the current one passes this size.
const SEGMENT_TARGET_BYTES: u64 = 64 * 1024 * 1024;

const V1_HEADER: u64 = 20;
const V2_HEADER: u64 = 24;

/// Where a group lives on disk. `created_ms` is duplicated here so time-window
/// queries (groups_since) never touch the disk.
#[derive(Clone, Copy)]
struct Location {
    segment: usize, // index into `segments`
    offset: u64,
    created_ms: u64,
}

struct Segment {
    path: PathBuf,
    first_seq: u64,
    newest_ms: u64, // created_ms of the newest group in this segment
    bytes: u64,
    v2: bool,
}

/// A resolved on-disk position of a group, detached from the store so the
/// file read can happen without holding the store's lock.
pub struct GroupLocation {
    path: PathBuf,
    offset: u64,
    v2: bool,
}

/// Read a group's frames at a previously-located position. Standalone so
/// callers can do the (blocking) file I/O after releasing the store lock; if
/// GC deleted the segment in between, this errors and the caller treats the
/// group as evicted.
pub fn read_group_at(loc: &GroupLocation, seq: u64) -> Result<Vec<Bytes>> {
    let mut f = File::open(&loc.path).with_context(|| format!("opening {}", loc.path.display()))?;
    f.seek(SeekFrom::Start(loc.offset))?;
    let (rec_seq, _ms, frames) = read_record(&mut f, loc.v2)?
        .ok_or_else(|| anyhow::anyhow!("truncated record for group {seq}"))?;
    anyhow::ensure!(rec_seq == seq, "index/segment mismatch: {rec_seq} != {seq}");
    Ok(frames)
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

        let body_len: u64 = frames.iter().map(|f| 4 + f.len() as u64).sum();
        let mut rec = Vec::with_capacity(V2_HEADER as usize + body_len as usize);
        rec.extend_from_slice(&seq.to_le_bytes());
        rec.extend_from_slice(&created_ms.to_le_bytes());
        rec.extend_from_slice(&(frames.len() as u32).to_le_bytes());
        rec.extend_from_slice(&(body_len as u32).to_le_bytes());
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
                created_ms,
            },
        );
        self.max_seq = Some(self.max_seq.map_or(seq, |m| m.max(seq)));
        Ok(())
    }

    /// Read back a group's frames, or None if not stored (evicted or never seen).
    pub fn read(&self, seq: u64) -> Result<Option<Vec<Bytes>>> {
        let Some(loc) = self.locate(seq) else {
            return Ok(None);
        };
        read_group_at(&loc, seq).map(Some)
    }

    /// Where to find a group on disk, or None if not stored. Cheap (index
    /// only) — callers holding a lock around the store can locate under the
    /// lock and do the actual file I/O after releasing it (see
    /// [`read_group_at`]).
    pub fn locate(&self, seq: u64) -> Option<GroupLocation> {
        let loc = self.index.get(&seq)?;
        let seg = &self.segments[loc.segment];
        Some(GroupLocation {
            path: seg.path.clone(),
            offset: loc.offset,
            v2: seg.v2,
        })
    }

    /// Group sequences whose group is newer than `cutoff_ms`, ascending — the
    /// set to reload into the live track on startup (the in-window groups).
    /// Served entirely from the in-memory index.
    pub fn groups_since(&self, cutoff_ms: u64) -> Result<Vec<u64>> {
        Ok(self
            .index
            .iter()
            .filter(|(_, loc)| loc.created_ms >= cutoff_ms)
            .map(|(&seq, _)| seq)
            .collect())
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
            // Also rotate away from a legacy v1 segment: formats never mix
            // within one file.
            Some(seg) => seg.bytes >= self.segment_target || !seg.v2,
        };
        if rotate {
            // Finish the previous writer.
            if let Some(mut w) = self.writer.take() {
                w.flush()?;
            }
            let path = self.dir.join(format!("{next_seq:020}.seg2"));
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
                v2: true,
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

    /// Rebuild segments + index by scanning the directory. `.seg2` records are
    /// skipped whole via the header's body_len; legacy `.seg` records are
    /// walked frame-by-frame (buffered).
    fn scan(&mut self) -> Result<()> {
        let mut paths: Vec<PathBuf> = fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.extension()
                    .map(|x| x == "seg" || x == "seg2")
                    .unwrap_or(false)
            })
            .collect();
        // Sort by segment name (first sequence), not full path string, so a
        // .seg and .seg2 pair orders by sequence.
        paths.sort_by_key(|p| p.file_stem().map(|s| s.to_os_string()));

        for path in paths {
            let v2 = path.extension().map(|x| x == "seg2").unwrap_or(false);
            let file = File::open(&path)?;
            let len = file.metadata()?.len();
            let mut r = BufReader::new(file);
            let header_len = if v2 { V2_HEADER } else { V1_HEADER };
            let mut offset = 0u64;
            let mut first_seq = None;
            let mut newest_ms = 0u64;
            loop {
                if offset + header_len > len {
                    break; // clean EOF or torn header
                }
                let mut hdr = [0u8; V2_HEADER as usize];
                let hdr = &mut hdr[..header_len as usize];
                r.read_exact(hdr)?;
                let seq = u64::from_le_bytes(hdr[0..8].try_into().unwrap());
                let ms = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
                let count = u32::from_le_bytes(hdr[16..20].try_into().unwrap()) as u64;
                let body_len = if v2 {
                    let claimed = u32::from_le_bytes(hdr[20..24].try_into().unwrap()) as u64;
                    if offset + header_len + claimed > len {
                        break; // torn body
                    }
                    r.seek_relative(claimed as i64)?;
                    claimed
                } else {
                    // v1: walk the frames to find the record's extent.
                    match walk_v1_body(&mut r, offset + header_len, count, len)? {
                        Some(b) => b,
                        None => break, // torn body
                    }
                };
                first_seq.get_or_insert(seq);
                newest_ms = ms;
                self.index.insert(
                    seq,
                    Location {
                        segment: self.segments.len(),
                        offset,
                        created_ms: ms,
                    },
                );
                self.max_seq = Some(self.max_seq.map_or(seq, |m| m.max(seq)));
                offset += header_len + body_len;
            }
            // Tolerate a torn tail record (crash mid-append) — but truncate it
            // away. The writer reopens segments with O_APPEND, which writes at
            // the *physical* EOF; if torn bytes were left in place, every new
            // record would land past the offset the index records for it,
            // making all post-restart groups unreadable (and a second restart
            // would regress max_seq, re-issuing group ids for new content).
            if offset < len {
                drop(r);
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
                v2,
            });
        }
        Ok(())
    }
}

/// Walk a v1 record's frames (buffered, seeking over bodies) and return the
/// body length, or None if the record runs past the end of the file (torn).
/// `body_start` is the file offset just past the record header; `len` is the
/// file's total length.
fn walk_v1_body(
    r: &mut BufReader<File>,
    body_start: u64,
    count: u64,
    len: u64,
) -> Result<Option<u64>> {
    let mut cursor = body_start;
    for _ in 0..count {
        if cursor + 4 > len {
            return Ok(None);
        }
        let mut lenb = [0u8; 4];
        r.read_exact(&mut lenb)?;
        let n = u32::from_le_bytes(lenb) as u64;
        if cursor + 4 + n > len {
            return Ok(None);
        }
        r.seek_relative(n as i64)?;
        cursor += 4 + n;
    }
    Ok(Some(cursor - body_start))
}

/// Read a full record (seq, created_ms, frames) at the reader's position.
fn read_record<R: Read>(r: &mut R, v2: bool) -> Result<Option<(u64, u64, Vec<Bytes>)>> {
    let header_len = if v2 { V2_HEADER } else { V1_HEADER } as usize;
    let mut hdr = [0u8; V2_HEADER as usize];
    if !read_full_or_eof(r, &mut hdr[..header_len])? {
        return Ok(None);
    }
    let seq = u64::from_le_bytes(hdr[0..8].try_into().unwrap());
    let ms = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
    let count = u32::from_le_bytes(hdr[16..20].try_into().unwrap()) as usize;
    let mut frames = Vec::with_capacity(count.min(4096));
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

    /// Hand-encode a legacy v1 record for upgrade tests.
    fn v1_record(seq: u64, ms: u64, parts: &[&str]) -> Vec<u8> {
        let mut rec = Vec::new();
        rec.extend_from_slice(&seq.to_le_bytes());
        rec.extend_from_slice(&ms.to_le_bytes());
        rec.extend_from_slice(&(parts.len() as u32).to_le_bytes());
        for p in parts {
            rec.extend_from_slice(&(p.len() as u32).to_le_bytes());
            rec.extend_from_slice(p.as_bytes());
        }
        rec
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
        // header claiming a body, but only part of it present).
        let seg = fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.extension().map(|x| x == "seg2").unwrap_or(false))
            .unwrap();
        let mut torn = Vec::new();
        torn.extend_from_slice(&3u64.to_le_bytes()); // seq
        torn.extend_from_slice(&3000u64.to_le_bytes()); // created_ms
        torn.extend_from_slice(&3u32.to_le_bytes()); // frame_count
        torn.extend_from_slice(&312u32.to_le_bytes()); // body_len...
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
    fn legacy_v1_segments_stay_readable_and_writer_rotates_away() {
        let dir = tmp();
        fs::create_dir_all(&dir).unwrap();
        // A legacy .seg file with two v1 records, as the old writer laid out.
        let mut v1 = Vec::new();
        v1.extend_from_slice(&v1_record(5, 1000, &["aa", "b"]));
        v1.extend_from_slice(&v1_record(6, 1100, &["cc"]));
        fs::write(dir.join(format!("{:020}.seg", 5)), &v1).unwrap();

        let mut s = GroupStore::open(&dir).unwrap();
        assert_eq!(s.max_seq(), Some(6));
        assert_eq!(s.read(5).unwrap().unwrap(), frames(&["aa", "b"]));
        assert_eq!(s.read(6).unwrap().unwrap(), frames(&["cc"]));
        assert_eq!(s.groups_since(1050).unwrap(), vec![6]);

        // Appending rotates to a fresh .seg2 rather than mixing formats.
        s.append(7, 1200, &frames(&["dd"])).unwrap();
        assert_eq!(s.read(7).unwrap().unwrap(), frames(&["dd"]));
        assert!(dir.join(format!("{:020}.seg2", 7)).exists());

        // Everything survives a reopen.
        let s2 = GroupStore::open(&dir).unwrap();
        assert_eq!(s2.max_seq(), Some(7));
        assert_eq!(s2.read(5).unwrap().unwrap(), frames(&["aa", "b"]));
        assert_eq!(s2.read(7).unwrap().unwrap(), frames(&["dd"]));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn legacy_v1_torn_tail_is_truncated() {
        let dir = tmp();
        fs::create_dir_all(&dir).unwrap();
        let mut v1 = Vec::new();
        v1.extend_from_slice(&v1_record(1, 1000, &["aa"]));
        let full = v1_record(2, 1100, &["bbbb"]);
        v1.extend_from_slice(&full[..full.len() - 2]); // torn mid-frame
        let path = dir.join(format!("{:020}.seg", 1));
        fs::write(&path, &v1).unwrap();

        let s = GroupStore::open(&dir).unwrap();
        assert_eq!(s.max_seq(), Some(1));
        assert_eq!(s.read(1).unwrap().unwrap(), frames(&["aa"]));
        // The torn record was truncated off the file.
        let expected = v1_record(1, 1000, &["aa"]).len() as u64;
        assert_eq!(fs::metadata(&path).unwrap().len(), expected);
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
