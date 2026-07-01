//! at-sync wire frames: two concatenated CBOR objects (header, then payload).
//!
//! The relay is a passthrough for valid frames: they are republished
//! byte-for-byte, and we only decode the handful of fields needed for routing
//! and group bookkeeping (header `op`/`t`, payload `seq`). Both objects are
//! validated as DRISL (see [`crate::drisl`]) before any value decoding —
//! atmoq rejects invalid DRISL at ingest by design, a deliberate semantic
//! difference from the upstream lenient WebSocket relay. Semantic validation
//! (signatures, MST inversion) per at-sync §4.5 still lands with milestone M2.

use crate::drisl;
use anyhow::{bail, Context, Result};
use ciborium::Value;
use std::io::Cursor;

/// Minimally decoded view of one firehose frame. `raw` is the original wire
/// bytes (header + payload), suitable for byte-exact republication.
#[derive(Debug, Clone)]
pub struct Frame {
    /// Frame operation: 1 = message, -1 = error.
    pub op: i64,
    /// Message type with leading '#' (e.g. "#commit"), present when op == 1.
    pub t: Option<String>,
    /// Sequence number, present on all at-sync message payloads.
    pub seq: Option<i64>,
    pub raw: Vec<u8>,
}

impl Frame {
    /// Decode the two CBOR objects (header, payload) of a wire frame, after
    /// validating both as DRISL. The validator also pins the exact
    /// header/payload boundary and rejects trailing bytes.
    pub fn decode(raw: &[u8]) -> Result<(Value, Value)> {
        let header_end = drisl::validate(raw, 0).context("frame header")?;
        if header_end >= raw.len() {
            bail!("frame has 1 CBOR item, expected header + payload");
        }
        let payload_end = drisl::validate(raw, header_end).context("frame payload")?;
        if payload_end != raw.len() {
            bail!(
                "trailing bytes after frame payload: {} of {}",
                raw.len() - payload_end,
                raw.len()
            );
        }
        let header: Value = ciborium::de::from_reader(Cursor::new(&raw[..header_end]))
            .context("decoding frame header")?;
        let payload: Value = ciborium::de::from_reader(Cursor::new(&raw[header_end..]))
            .context("decoding frame payload")?;
        Ok((header, payload))
    }

    pub fn parse(raw: Vec<u8>) -> Result<Frame> {
        let (header, payload) = Frame::decode(&raw)?;

        let op = field(&header, "op")
            .and_then(Value::as_integer)
            .map(i128::from)
            .context("frame header missing integer 'op'")? as i64;
        let t = field(&header, "t")
            .and_then(|v| v.as_text())
            .map(str::to_owned);
        if op == 1 && t.is_none() {
            bail!("op=1 frame missing 't'");
        }
        let seq = field(&payload, "seq")
            .and_then(Value::as_integer)
            .map(|i| i128::from(i) as i64);

        Ok(Frame { op, t, seq, raw })
    }
}

pub fn field<'a>(v: &'a Value, key: &str) -> Option<&'a Value> {
    v.as_map()?
        .iter()
        .find(|(k, _)| k.as_text() == Some(key))
        .map(|(_, v)| v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(v: &Value) -> Vec<u8> {
        let mut out = Vec::new();
        ciborium::ser::into_writer(v, &mut out).unwrap();
        out
    }

    fn frame_bytes(op: i64, t: Option<&str>, payload: Value) -> Vec<u8> {
        // DRISL key order: "t" (1 byte) sorts before "op" (2 bytes), which is
        // also how real PDS encoders emit the header.
        let mut header = Vec::new();
        if let Some(t) = t {
            header.push((Value::Text("t".into()), Value::Text(t.into())));
        }
        header.push((Value::Text("op".into()), Value::Integer(op.into())));
        let mut raw = encode(&Value::Map(header));
        raw.extend(encode(&payload));
        raw
    }

    #[test]
    fn parses_commit_frame() {
        let payload = Value::Map(vec![
            (Value::Text("seq".into()), Value::Integer(42.into())),
            (
                Value::Text("repo".into()),
                Value::Text("did:plc:abc".into()),
            ),
        ]);
        let raw = frame_bytes(1, Some("#commit"), payload);
        let f = Frame::parse(raw.clone()).unwrap();
        assert_eq!(f.op, 1);
        assert_eq!(f.t.as_deref(), Some("#commit"));
        assert_eq!(f.seq, Some(42));
        assert_eq!(f.raw, raw);
    }

    #[test]
    fn parses_error_frame() {
        let payload = Value::Map(vec![(
            Value::Text("error".into()),
            Value::Text("FutureCursor".into()),
        )]);
        let f = Frame::parse(frame_bytes(-1, None, payload)).unwrap();
        assert_eq!(f.op, -1);
        assert_eq!(f.t, None);
        assert_eq!(f.seq, None);
    }

    #[test]
    fn rejects_trailing_garbage() {
        let payload = Value::Map(vec![(Value::Text("seq".into()), Value::Integer(1.into()))]);
        let mut raw = frame_bytes(1, Some("#sync"), payload);
        raw.push(0x00);
        assert!(Frame::parse(raw).is_err());
    }

    #[test]
    fn rejects_missing_type() {
        let payload = Value::Map(vec![]);
        assert!(Frame::parse(frame_bytes(1, None, payload)).is_err());
    }

    #[test]
    fn rejects_invalid_drisl() {
        // Out-of-order header keys ("op" before "t") — decodable CBOR, but
        // not DRISL, so the frame is rejected at ingest.
        let mut raw = encode(&Value::Map(vec![
            (Value::Text("op".into()), Value::Integer(1.into())),
            (Value::Text("t".into()), Value::Text("#commit".into())),
        ]));
        raw.extend(encode(&Value::Map(vec![])));
        let err = Frame::parse(raw).unwrap_err();
        assert!(err.to_string().contains("header"), "{err:#}");

        // A float16 in the payload — valid CBOR, invalid DRISL.
        let mut raw = encode(&Value::Map(vec![(
            Value::Text("t".into()),
            Value::Text("#seq".into()),
        )]));
        raw.extend([0xf9, 0x3c, 0x00]);
        assert!(Frame::decode(&raw).is_err());
    }
}
