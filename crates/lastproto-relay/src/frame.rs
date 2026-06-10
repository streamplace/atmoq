//! at-sync wire frames: two concatenated CBOR objects (header, then payload).
//!
//! The prototype relay is a passthrough: frames are republished byte-for-byte,
//! and we only decode the handful of fields needed for routing and group
//! bookkeeping (header `op`/`t`, payload `seq`). Full deterministic-CBOR
//! validation per at-sync §4.5 lands with the validation milestone (M2).

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
    /// Decode the two CBOR objects (header, payload) of a wire frame.
    pub fn decode(raw: &[u8]) -> Result<(Value, Value)> {
        let mut cursor = Cursor::new(raw);
        let header: Value =
            ciborium::de::from_reader(&mut cursor).context("decoding frame header")?;
        let payload: Value =
            ciborium::de::from_reader(&mut cursor).context("decoding frame payload")?;
        if cursor.position() != raw.len() as u64 {
            bail!(
                "trailing bytes after frame payload: {} of {}",
                raw.len() as u64 - cursor.position(),
                raw.len()
            );
        }
        Ok((header, payload))
    }

    pub fn parse(raw: Vec<u8>) -> Result<Frame> {
        let (header, payload) = Frame::decode(&raw)?;

        let op = map_get(&header, "op")
            .and_then(Value::as_integer)
            .map(i128::from)
            .context("frame header missing integer 'op'")? as i64;
        let t = map_get(&header, "t")
            .and_then(|v| v.as_text())
            .map(str::to_owned);
        if op == 1 && t.is_none() {
            bail!("op=1 frame missing 't'");
        }
        let seq = map_get(&payload, "seq")
            .and_then(Value::as_integer)
            .map(|i| i128::from(i) as i64);

        Ok(Frame { op, t, seq, raw })
    }
}

fn map_get<'a>(v: &'a Value, key: &str) -> Option<&'a Value> {
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
        let mut header = vec![(Value::Text("op".into()), Value::Integer(op.into()))];
        if let Some(t) = t {
            header.push((Value::Text("t".into()), Value::Text(t.into())));
        }
        let mut raw = encode(&Value::Map(header));
        raw.extend(encode(&payload));
        raw
    }

    #[test]
    fn parses_commit_frame() {
        let payload = Value::Map(vec![
            (Value::Text("seq".into()), Value::Integer(42.into())),
            (Value::Text("repo".into()), Value::Text("did:plc:abc".into())),
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
}
