//! Minimal CAR (Content-Addressable aRchive) block reader, per at-repo §5:
//! LEB128-length-prefixed header, then `varint(len) | cid | block` entries.
//! Enough to look up record blocks by CID for display (`--ops`); MST
//! verification is out of scope here (that's the M2 validation milestone).

use anyhow::{bail, Result};
use std::collections::HashMap;

/// Parse all blocks, keyed by binary CID. Tolerant per at-repo §5.3:
/// duplicate blocks are deduplicated, a truncated trailing entry stops
/// parsing rather than failing.
pub fn blocks(data: &[u8]) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
    let mut cur = data;
    let header_len = varint(&mut cur)? as usize;
    if header_len > cur.len() {
        bail!("CAR header length {header_len} exceeds input");
    }
    cur = &cur[header_len..]; // header CBOR (version, roots) — not needed

    let mut out = HashMap::new();
    while !cur.is_empty() {
        let Ok(entry_len) = varint(&mut cur) else { break };
        let entry_len = entry_len as usize;
        if entry_len > cur.len() {
            break; // truncated trailing entry
        }
        let (entry, rest) = cur.split_at(entry_len);
        cur = rest;
        let mut e = entry;
        let Ok(cid) = read_cid(&mut e) else { continue };
        out.insert(cid, e.to_vec());
    }
    Ok(out)
}

/// Read a binary CIDv1 (version, codec, multihash) off the front of `cur`,
/// returning its bytes.
fn read_cid(cur: &mut &[u8]) -> Result<Vec<u8>> {
    let start = *cur;
    let version = varint(cur)?;
    if version != 1 {
        bail!("unsupported CID version {version}");
    }
    let _codec = varint(cur)?;
    let _hash_code = varint(cur)?;
    let hash_len = varint(cur)? as usize;
    if hash_len > cur.len() {
        bail!("truncated multihash");
    }
    *cur = &cur[hash_len..];
    let cid_len = start.len() - cur.len();
    Ok(start[..cid_len].to_vec())
}

fn varint(cur: &mut &[u8]) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let Some((&byte, rest)) = cur.split_first() else {
            bail!("truncated varint");
        };
        *cur = rest;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            bail!("varint overflow");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_varint(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                out.push(byte);
                return;
            }
            out.push(byte | 0x80);
        }
    }

    fn fake_cid(seed: u8) -> Vec<u8> {
        let mut cid = vec![0x01, 0x71, 0x12, 0x20];
        cid.extend([seed; 32]);
        cid
    }

    #[test]
    fn parses_blocks() {
        let mut car = Vec::new();
        let header = b"\xa2"; // placeholder CBOR; contents are skipped
        write_varint(&mut car, header.len() as u64);
        car.extend(header);

        for seed in [1u8, 2] {
            let cid = fake_cid(seed);
            let data = vec![seed; 5];
            write_varint(&mut car, (cid.len() + data.len()) as u64);
            car.extend(&cid);
            car.extend(&data);
        }

        let blocks = blocks(&car).unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[&fake_cid(1)], vec![1u8; 5]);
        assert_eq!(blocks[&fake_cid(2)], vec![2u8; 5]);
    }

    #[test]
    fn tolerates_truncation() {
        let mut car = Vec::new();
        write_varint(&mut car, 1);
        car.push(0xa0);
        let cid = fake_cid(3);
        write_varint(&mut car, (cid.len() + 10) as u64);
        car.extend(&cid);
        car.extend([3u8; 4]); // claims 10 data bytes, has 4
        let blocks = blocks(&car).unwrap();
        assert!(blocks.is_empty());
    }
}
