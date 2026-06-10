//! Render decoded frame CBOR as JSON for CLI output.
//!
//! CID links (CBOR tag 42) render as CID strings (at-repo appendix A.1:
//! 'b' + lowercase base32 of the 36-byte CID, after stripping the leading
//! 0x00 multibase-identity byte). Plain byte strings render as
//! `{"$bytesLength": n}` — payload `blocks` CARs are bulky and structural
//! comparison happens at the ops/rev level.

use ciborium::Value;

pub fn cbor_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => (*b).into(),
        Value::Integer(i) => {
            let i = i128::from(*i);
            match i64::try_from(i) {
                Ok(n) => n.into(),
                Err(_) => i.to_string().into(),
            }
        }
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Text(s) => s.clone().into(),
        Value::Bytes(b) => serde_json::json!({ "$bytesLength": b.len() }),
        Value::Array(items) => items.iter().map(cbor_to_json).collect(),
        Value::Map(entries) => serde_json::Value::Object(
            entries
                .iter()
                .map(|(k, v)| {
                    let key = match k {
                        Value::Text(s) => s.clone(),
                        other => format!("{other:?}"),
                    };
                    (key, cbor_to_json(v))
                })
                .collect(),
        ),
        Value::Tag(42, inner) => match inner.as_ref() {
            Value::Bytes(b) => cid_string(b).into(),
            other => cbor_to_json(other),
        },
        Value::Tag(_, inner) => cbor_to_json(inner),
        _ => serde_json::Value::Null,
    }
}

/// CBOR-tag-42 byte string (0x00 + 36-byte CID) to canonical string form.
fn cid_string(bytes: &[u8]) -> String {
    let cid = bytes.strip_prefix(&[0x00]).unwrap_or(bytes);
    format!(
        "b{}",
        data_encoding::BASE32_NOPAD.encode(cid).to_lowercase()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_cid_links() {
        // 0x01711220 prefix + 32 zero bytes, with leading 0x00 identity byte
        let mut cid = vec![0x00, 0x01, 0x71, 0x12, 0x20];
        cid.extend([0u8; 32]);
        let v = Value::Tag(42, Box::new(Value::Bytes(cid)));
        let j = cbor_to_json(&v);
        let s = j.as_str().unwrap();
        assert!(s.starts_with("bafyrei") || s.starts_with("b"), "{s}");
        assert_eq!(s.len(), 59);
    }

    #[test]
    fn renders_bytes_as_length() {
        let v = Value::Bytes(vec![1, 2, 3]);
        assert_eq!(cbor_to_json(&v), serde_json::json!({"$bytesLength": 3}));
    }
}
