//! DRISL validation: <https://dasl.ing/drisl.html>
//!
//! DRISL is a deterministic CBOR profile (a subset of CBOR/c,
//! draft-rundgren-cbor-core) — the encoding atproto records and at-sync frames
//! are supposed to use. atmoq takes the opinionated position that everything
//! across the stack only works on valid DRISL: the relay rejects invalid DRISL
//! at ingest (a deliberate semantic difference from the upstream lenient
//! WebSocket relay, as a forcing function for ecosystem strictness), and the
//! clients reject it at decode.
//!
//! The rules enforced here, per the DRISL spec and CBOR/c which it inherits:
//! - definite lengths only (no indefinite-length items, no break code)
//! - minimal-length ("preferred") encoding of every int and length argument
//! - map keys must be text strings, unique, and sorted in bytewise
//!   lexicographic order of their encoded bytes (for text keys this is the
//!   same order as DAG-CBOR's length-first rule)
//! - floats must be 64-bit (never half/single precision); NaN and ±Infinity
//!   are rejected (negative zero is the only allowed special value)
//! - tag 42 (CID) is the only allowed tag; its content must be a byte string
//!   with the historical 0x00 multibase prefix
//! - the only allowed simple values are false, true, and null
//! - text strings must be valid UTF-8
//!
//! Validation is a single pass over the raw bytes that also returns the end
//! offset of each item — which is exactly what at-sync frame parsing needs to
//! locate the header/payload boundary.
//!
//! This is a line-for-line sibling of the TypeScript validator
//! (`ts/src/drisl.ts`); keep the two in sync. Canonical implementations to
//! cross-check against: <https://github.com/hyphacoop/go-dasl> (Go) and
//! <https://github.com/n0-computer/dasl> (Rust).

use std::fmt;

/// A DRISL violation, with the byte offset where it was found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrislError {
    pub offset: usize,
    pub message: String,
}

impl fmt::Display for DrislError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid DRISL at byte {}: {}", self.offset, self.message)
    }
}

impl std::error::Error for DrislError {}

fn err<T>(offset: usize, message: impl Into<String>) -> Result<T, DrislError> {
    Err(DrislError {
        offset,
        message: message.into(),
    })
}

/// Deeply nested documents are rejected rather than risking stack exhaustion
/// (the validator recurses per level). Real atproto records nest a handful of
/// levels; 128 matches serde_ipld_dagcbor's recursion limit. Keep in sync
/// with ts/src/drisl.ts.
const MAX_DEPTH: usize = 128;

/// Validate one complete DRISL item starting at `offset`; returns the offset
/// just past the item.
pub fn validate(data: &[u8], offset: usize) -> Result<usize, DrislError> {
    validate_item(data, offset, 0)
}

/// Validate that `data` is exactly one complete DRISL item — no trailing bytes.
pub fn validate_exact(data: &[u8]) -> Result<(), DrislError> {
    let end = validate(data, 0)?;
    if end != data.len() {
        return err(
            end,
            format!("{} trailing byte(s) after item", data.len() - end),
        );
    }
    Ok(())
}

/// Read the argument (value or length) for an initial byte, enforcing minimal
/// encoding. Returns (value, offset just past the argument).
fn read_arg(data: &[u8], offset: usize, what: &str) -> Result<(u64, usize), DrislError> {
    let ai = data[offset] & 0x1f;
    if ai < 24 {
        return Ok((ai as u64, offset + 1));
    }
    if ai > 27 {
        // 28-30 are reserved; 31 is indefinite-length / break.
        return if ai == 31 {
            err(offset, format!("indefinite-length {what} is not allowed"))
        } else {
            err(offset, format!("reserved additional-info value {ai}"))
        };
    }
    let width = 1usize << (ai - 24); // 24→1, 25→2, 26→4, 27→8 bytes
    if offset + 1 + width > data.len() {
        return err(offset, format!("truncated {what} argument"));
    }
    let mut value = 0u64;
    for i in 0..width {
        value = (value << 8) | data[offset + 1 + i] as u64;
    }
    let minimal = match ai {
        24 => 24,
        25 => 256,
        26 => 65536,
        _ => 4294967296,
    };
    if value < minimal {
        return err(
            offset,
            format!("non-minimal encoding of {what} {value} ({width}-byte argument)"),
        );
    }
    Ok((value, offset + 1 + width))
}

fn validate_item(data: &[u8], offset: usize, depth: usize) -> Result<usize, DrislError> {
    if depth > MAX_DEPTH {
        return err(offset, format!("nesting deeper than {MAX_DEPTH}"));
    }
    let Some(&initial) = data.get(offset) else {
        return err(offset, "truncated: expected an item");
    };
    let major = initial >> 5;

    match major {
        // unsigned int / negative int
        0 | 1 => {
            let what = if major == 0 { "uint" } else { "negint" };
            Ok(read_arg(data, offset, what)?.1)
        }
        // byte string / text string
        2 | 3 => {
            let what = if major == 2 {
                "byte string length"
            } else {
                "text string length"
            };
            let (len, end) = read_arg(data, offset, what)?;
            let len = usize::try_from(len)
                .ok()
                .filter(|l| end.checked_add(*l).is_some_and(|e| e <= data.len()));
            let Some(len) = len else {
                return err(end.min(data.len()), "truncated string body");
            };
            if major == 3 && std::str::from_utf8(&data[end..end + len]).is_err() {
                return err(end, "text string is not valid UTF-8");
            }
            Ok(end + len)
        }
        // array
        4 => {
            let (count, end) = read_arg(data, offset, "array length")?;
            let mut cursor = end;
            for _ in 0..count {
                cursor = validate_item(data, cursor, depth + 1)?;
            }
            Ok(cursor)
        }
        // map
        5 => {
            let (count, end) = read_arg(data, offset, "map length")?;
            let mut cursor = end;
            let mut prev_key: Option<(usize, usize)> = None;
            for _ in 0..count {
                let key_start = cursor;
                let Some(&kb) = data.get(key_start) else {
                    return err(key_start, "truncated: expected a map key");
                };
                if kb >> 5 != 3 {
                    return err(key_start, "map key is not a text string");
                }
                let key_end = validate_item(data, key_start, depth + 1)?;
                if let Some((ps, pe)) = prev_key {
                    use std::cmp::Ordering;
                    match data[ps..pe].cmp(&data[key_start..key_end]) {
                        Ordering::Equal => return err(key_start, "duplicate map key"),
                        Ordering::Greater => {
                            return err(
                                key_start,
                                "map keys are not in bytewise lexicographic order",
                            )
                        }
                        Ordering::Less => {}
                    }
                }
                prev_key = Some((key_start, key_end));
                cursor = validate_item(data, key_end, depth + 1)?;
            }
            Ok(cursor)
        }
        // tag
        6 => {
            let (tag, end) = read_arg(data, offset, "tag")?;
            if tag != 42 {
                return err(
                    offset,
                    format!("tag {tag} is not allowed (only tag 42/CID)"),
                );
            }
            match data.get(end) {
                Some(&b) if b >> 5 == 2 => {}
                _ => return err(end.min(data.len()), "tag 42 content must be a byte string"),
            }
            let content_end = validate_item(data, end, depth + 1)?;
            // The byte string body starts after its own head; check the 0x00 prefix.
            let (_, body_start) = read_arg(data, end, "byte string length")?;
            if content_end == body_start || data[body_start] != 0x00 {
                return err(body_start, "tag 42 CID must start with the 0x00 prefix");
            }
            Ok(content_end)
        }
        // simple values and floats
        7 => match initial {
            0xf4 | 0xf5 | 0xf6 => Ok(offset + 1), // false, true, null
            0xfb => {
                // 64-bit float — the only float width DRISL allows.
                let Some(bytes) = data.get(offset + 1..offset + 9) else {
                    return err(offset, "truncated float64");
                };
                let f = f64::from_be_bytes(bytes.try_into().unwrap());
                if f.is_nan() {
                    return err(offset, "NaN is not allowed");
                }
                if f.is_infinite() {
                    return err(offset, "infinity is not allowed");
                }
                Ok(offset + 9)
            }
            0xf9 => err(
                offset,
                "half-precision float is not allowed (floats must be 64-bit)",
            ),
            0xfa => err(
                offset,
                "single-precision float is not allowed (floats must be 64-bit)",
            ),
            0xf7 => err(offset, "undefined is not allowed"),
            0xff => err(offset, "unexpected break code"),
            _ => {
                let v = if initial & 0x1f == 24 {
                    data.get(offset + 1).copied().unwrap_or(0)
                } else {
                    initial & 0x1f
                };
                err(
                    offset,
                    format!("simple value {v} is not allowed (only false/true/null)"),
                )
            }
        },
        _ => unreachable!("major is 3 bits"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f64_bytes(f: f64) -> Vec<u8> {
        let mut out = vec![0xfb];
        out.extend_from_slice(&f.to_be_bytes());
        out
    }

    #[test]
    fn accepts_valid_documents() {
        // Mirrors ts/test/drisl.test.ts — shared wire-contract vectors.
        let valid: Vec<(&str, Vec<u8>)> = vec![
            ("uint 0", vec![0x00]),
            ("uint 23", vec![0x17]),
            ("uint 24", vec![0x18, 0x18]),
            ("uint 256", vec![0x19, 0x01, 0x00]),
            ("uint 65536", vec![0x1a, 0x00, 0x01, 0x00, 0x00]),
            (
                "uint 2^32",
                vec![0x1b, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00],
            ),
            ("negint -1", vec![0x20]),
            ("bytes", vec![0x43, 1, 2, 3]),
            ("text 'abc'", vec![0x63, 0x61, 0x62, 0x63]),
            ("array [1,2]", vec![0x82, 0x01, 0x02]),
            ("empty map", vec![0xa0]),
            ("sorted map", vec![0xa2, 0x61, 0x61, 0x01, 0x61, 0x62, 0x02]),
            (
                "length-first keys {t, op}",
                vec![0xa2, 0x61, 0x74, 0x01, 0x62, 0x6f, 0x70, 0x02],
            ),
            ("false", vec![0xf4]),
            ("true", vec![0xf5]),
            ("null", vec![0xf6]),
            ("float64 1.5", f64_bytes(1.5)),
            ("float64 -0.0", f64_bytes(-0.0)),
            (
                "tag 42 CID",
                vec![0xd8, 0x2a, 0x45, 0x00, 0x01, 0x71, 0x12, 0x20],
            ),
        ];
        for (name, data) in valid {
            assert!(
                validate_exact(&data).is_ok(),
                "{name}: {:?}",
                validate_exact(&data)
            );
        }
    }

    #[test]
    fn rejects_invalid_documents() {
        let invalid: Vec<(&str, Vec<u8>, &str)> = vec![
            ("non-minimal uint 1-byte", vec![0x18, 0x17], "non-minimal"),
            (
                "non-minimal uint 2-byte",
                vec![0x19, 0x00, 0xff],
                "non-minimal",
            ),
            (
                "non-minimal string length",
                vec![0x78, 0x03, 0x61, 0x62, 0x63],
                "non-minimal",
            ),
            ("indefinite array", vec![0x9f, 0x01, 0xff], "indefinite"),
            (
                "indefinite map",
                vec![0xbf, 0x61, 0x61, 0x01, 0xff],
                "indefinite",
            ),
            ("bare break", vec![0xff], "break"),
            ("float16", vec![0xf9, 0x3c, 0x00], "half-precision"),
            (
                "float32",
                vec![0xfa, 0x3f, 0xc0, 0x00, 0x00],
                "single-precision",
            ),
            ("float64 NaN", f64_bytes(f64::NAN), "NaN"),
            ("float64 Inf", f64_bytes(f64::INFINITY), "infinity"),
            ("undefined", vec![0xf7], "undefined"),
            ("simple 19", vec![0xf3], "simple value"),
            (
                "unsorted keys {b, a}",
                vec![0xa2, 0x61, 0x62, 0x01, 0x61, 0x61, 0x02],
                "order",
            ),
            (
                "longer key first {op, t}",
                vec![0xa2, 0x62, 0x6f, 0x70, 0x01, 0x61, 0x74, 0x02],
                "order",
            ),
            (
                "duplicate keys",
                vec![0xa2, 0x61, 0x61, 0x01, 0x61, 0x61, 0x02],
                "duplicate",
            ),
            ("int map key", vec![0xa1, 0x01, 0x02], "not a text string"),
            ("tag 0", vec![0xc0, 0x60], "tag 0"),
            ("tag 2 bignum", vec![0xc2, 0x41, 0x01], "tag 2"),
            (
                "tag 42 non-bytes",
                vec![0xd8, 0x2a, 0x61, 0x61],
                "byte string",
            ),
            (
                "tag 42 no 0x00 prefix",
                vec![0xd8, 0x2a, 0x42, 0x01, 0x71],
                "0x00 prefix",
            ),
            ("invalid UTF-8", vec![0x62, 0xc3, 0x28], "UTF-8"),
            ("truncated arg", vec![0x19, 0x01], "truncated"),
            ("truncated string", vec![0x63, 0x61, 0x62], "truncated"),
            ("truncated array", vec![0x82, 0x01], "truncated"),
            ("empty input", vec![], "truncated"),
            ("trailing bytes", vec![0x01, 0x02], "trailing"),
        ];
        for (name, data, needle) in invalid {
            let e = validate_exact(&data).unwrap_err();
            assert!(
                e.message.contains(needle),
                "{name}: expected {needle:?} in {:?}",
                e.message
            );
        }
    }

    #[test]
    fn returns_item_end_offsets() {
        // {"a": 1} then null — the header/payload boundary use case.
        let doc = [0xa1, 0x61, 0x61, 0x01, 0xf6];
        assert_eq!(validate(&doc, 0).unwrap(), 4);
        assert_eq!(validate(&doc, 4).unwrap(), 5);
    }

    #[test]
    fn rejects_pathological_nesting() {
        let mut deep = vec![0x81; 2000];
        deep.push(0x00);
        let e = validate_exact(&deep).unwrap_err();
        assert!(e.message.contains("nesting"));
    }
}
