//! Runs the relay-conformance corpus through atmoq's real ingest decoder.
//!
//! `Frame::parse` is exactly what `ingest::subscribe_repos` calls on each
//! upstream binary message; a parse error is what makes atmoq drop (reject) a
//! frame. So this reports atmoq's true decode-layer verdict per case.
//!
//!   cargo run --example relay_conformance -- tests/relay-conformance/corpus.json
//!
//! Emits one JSON line per case: {"id","outcome":"accept|reject","detail"}.

use atmoq::frame::Frame;
use serde::Deserialize;
use std::fs;

#[derive(Deserialize)]
struct SeqFrame {
    hex: String,
}

#[derive(Deserialize)]
struct Case {
    id: String,
    // #account/#commit corpora carry `hex`; the sync-1.1 corpus carries a
    // `frames` sequence (setup commits + the frame under test). atmoq is
    // stateless (envelope-only), so only the last frame — the one under test —
    // matters to its verdict.
    hex: Option<String>,
    frames: Option<Vec<SeqFrame>>,
}

impl Case {
    fn test_hex(&self) -> &str {
        if let Some(h) = &self.hex {
            h
        } else {
            &self
                .frames
                .as_ref()
                .expect("case has hex or frames")
                .last()
                .expect("frames non-empty")
                .hex
        }
    }
}

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: relay_conformance <corpus.json>");
    let cases: Vec<Case> =
        serde_json::from_str(&fs::read_to_string(&path).expect("read corpus")).expect("parse json");

    for c in &cases {
        let raw = data_encoding::HEXLOWER
            .decode(c.test_hex().as_bytes())
            .expect("hex decode");
        let (outcome, detail) = match Frame::parse(raw) {
            Ok(f) => ("accept", format!("t={:?} seq={:?}", f.t, f.seq)),
            Err(e) => ("reject", format!("{e:#}")),
        };
        println!(
            "{}",
            serde_json::json!({"id": c.id, "outcome": outcome, "detail": detail})
        );
    }
}
