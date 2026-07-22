// The malformed-frame corpus.
//
// A subscribeRepos wire frame is two concatenated CBOR objects: a *header*
// ({op, t}) and a *payload* (the event body). We build a valid baseline of
// each and then apply exactly one defect per case, so any relay behavior is
// attributable to that single defect.
//
// Design choices that keep results clean and comparable:
//   1. Every payload is an otherwise-COMPLETE `#account` event — all required
//      fields present (seq, did, time, active) — so no relay rejects merely
//      because a lexicon-required field is missing. The defect is the only
//      anomaly. (rsky's #account struct requires active+time with no serde
//      default; forgetting them silently turns every case into a "missing
//      field" reject that masks the real defect.)
//   2. #account is signature-free (no CAR, no commit signature), so a reject
//      can only mean "the encoding/shape was rejected".
//   3. Where a value-level defect has no natural home in a typed field (a
//      float, a stray tag), we carry it in an extra field `x`. Relays that
//      only strict-decode *known* fields (indigo, rsky) skip `x`; a relay that
//      validates the whole frame (atmoq/DRISL) inspects it. That contrast is
//      itself a finding — a malformed value inside a forward-compat field.
//   4. Two controls (float64, tag-42 CID) are valid DRISL and MUST be accepted.
//
// Each case: { id, layer, title, note, expect: {strict, tolerant}, build(ctx) }
// ctx = { did, seq() }. expect.strict = a DRISL-strict relay's correct action;
// expect.tolerant = the forward-compat reading. "either" marks the cases where
// both are defensible — exactly the WG's open question.

import {
  bytes, uint, negint, bstr, tstr, bool, float64, map, mapRaw, cid, concat,
} from "./cbor.mjs";

const enc = new TextEncoder();
const TIME = "2026-07-19T00:00:00.000Z";

// Header {t, op}: "t" (0x61 74) sorts before "op" (0x62 6f70) bytewise.
const HEADER = (t) => mapRaw([["t", tstr(t)], ["op", uint(1)]]);

// A complete #account body. `override` swaps a field's raw value bytes; `extra`
// appends entries. Keys are auto-sorted (valid DRISL) unless a case builds the
// map raw on purpose.
const account = (ctx, { override = {}, extra = [] } = {}) => {
  const fields = {
    seq: uint(ctx.seq()),
    did: tstr(ctx.did),
    time: tstr(TIME),
    active: bool(true),
    ...override,
  };
  return map([...Object.entries(fields), ...extra]);
};
const acctFrame = (ctx, opts) => bytes(HEADER("#account"), account(ctx, opts));

const DUMMY_MH = bytes(0x01, 0x71, 0x12, 0x20, ...new Array(32).fill(0));

export const CASES = [
  // ---- Layer 0: frame framing --------------------------------------------
  {
    id: "framing/single-object", layer: "framing",
    title: "header only, no payload object",
    note: "One CBOR item where the frame contract requires two.",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => HEADER("#account"),
  },
  {
    id: "framing/trailing-bytes", layer: "framing",
    title: "valid frame + one trailing byte",
    note: "Extra byte after the payload item within the same message.",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => bytes(acctFrame(ctx), 0x00),
  },
  {
    id: "framing/empty-message", layer: "framing",
    title: "zero-length binary message",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => bytes(),
  },

  // ---- Layer 1: CBOR well-formedness (not decodable) ---------------------
  {
    id: "cbor/truncated-payload", layer: "cbor",
    title: "payload truncated mid-item",
    note: "Header ok; payload claims a 4-byte string but supplies 2.",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => bytes(HEADER("#account"), 0xa1, tstr("did"), 0x64, 0x61, 0x62),
  },
  {
    id: "cbor/reserved-ai-payload", layer: "cbor",
    title: "reserved additional-info 28 in payload",
    note: "Initial byte 0x1c — AI 28 is reserved/undefined in CBOR.",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => bytes(HEADER("#account"), 0x1c),
  },
  {
    id: "cbor/bare-break-payload", layer: "cbor",
    title: "bare break code (0xff) as payload",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => bytes(HEADER("#account"), 0xff),
  },
  {
    id: "cbor/garbage-payload", layer: "cbor",
    title: "random non-CBOR bytes as payload",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => bytes(HEADER("#account"), 0xde, 0xad, 0xbe, 0xef),
  },

  // ---- Layer 2: DRISL determinism (valid CBOR, non-canonical) ------------
  {
    id: "drisl/unordered-keys-payload", layer: "drisl",
    title: "payload map keys out of DRISL order",
    note: "Complete #account, keys in reverse (active,time,seq,did).",
    expect: { strict: "reject", tolerant: "either" },
    build: (ctx) => bytes(HEADER("#account"), mapRaw([
      ["active", bool(true)], ["time", tstr(TIME)],
      ["seq", uint(ctx.seq())], ["did", tstr(ctx.did)],
    ])),
  },
  {
    id: "drisl/unordered-keys-header", layer: "drisl",
    title: "header keys out of DRISL order (op before t)",
    note: "op sorts after t but is emitted first.",
    expect: { strict: "reject", tolerant: "either" },
    build: (ctx) => bytes(mapRaw([["op", uint(1)], ["t", tstr("#account")]]), account(ctx)),
  },
  {
    id: "drisl/duplicate-key-payload", layer: "drisl",
    title: "duplicate map key in payload",
    note: "seq appears twice.",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => bytes(HEADER("#account"), mapRaw([
      ["seq", uint(ctx.seq())], ["seq", uint(999)],
      ["did", tstr(ctx.did)], ["time", tstr(TIME)], ["active", bool(true)],
    ])),
  },
  {
    id: "drisl/nonminimal-int-payload", layer: "drisl",
    title: "non-minimal integer encoding in payload",
    note: "seq value 1 encoded as a 2-byte uint (0x19 0x00 0x01).",
    expect: { strict: "reject", tolerant: "either" },
    build: (ctx) => acctFrame(ctx, { override: { seq: bytes(0x19, 0x00, 0x01) } }),
  },
  {
    id: "drisl/nonminimal-len-payload", layer: "drisl",
    title: "non-minimal string length in payload",
    note: "3-char did value length encoded in 1 extra byte (0x78 0x03).",
    expect: { strict: "reject", tolerant: "either" },
    build: (ctx) => acctFrame(ctx, { override: { did: bytes(0x78, 0x03, ...enc.encode("abc")) } }),
  },
  {
    id: "drisl/indefinite-map-payload", layer: "drisl",
    title: "indefinite-length map in payload",
    note: "0xbf ... 0xff instead of a definite count.",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => bytes(HEADER("#account"), 0xbf,
      tstr("seq"), uint(ctx.seq()), tstr("did"), tstr(ctx.did),
      tstr("time"), tstr(TIME), tstr("active"), bool(true), 0xff),
  },
  {
    id: "drisl/indefinite-str-payload", layer: "drisl",
    title: "indefinite-length text string in payload",
    note: "did value as a chunked indefinite string.",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => acctFrame(ctx, {
      override: { did: bytes(0x7f, tstr("did:"), tstr("x"), 0xff) },
    }),
  },

  // ---- Layer 2b: DRISL float / simple-value rules (carried in field x) ---
  {
    id: "drisl/float16-payload", layer: "drisl-float",
    title: "float16 value in a (forward-compat) field",
    note: "Half-precision (0xf9); DRISL allows only 64-bit floats.",
    expect: { strict: "reject", tolerant: "either" },
    build: (ctx) => acctFrame(ctx, { extra: [["x", bytes(0xf9, 0x3c, 0x00)]] }),
  },
  {
    id: "drisl/float32-payload", layer: "drisl-float",
    title: "float32 value in a field",
    expect: { strict: "reject", tolerant: "either" },
    build: (ctx) => acctFrame(ctx, { extra: [["x", bytes(0xfa, 0x3f, 0x80, 0x00, 0x00)]] }),
  },
  {
    id: "drisl/float64-ok-payload", layer: "drisl-float",
    title: "float64 value in a field (CONTROL: valid DRISL)",
    note: "64-bit float is legal DRISL; a strict relay must NOT reject this.",
    expect: { strict: "accept", tolerant: "accept" },
    build: (ctx) => acctFrame(ctx, { extra: [["x", float64(1.5)]] }),
  },
  {
    id: "drisl/nan-payload", layer: "drisl-float",
    title: "float64 NaN in a field",
    note: "Right width, forbidden value.",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => acctFrame(ctx, { extra: [["x", bytes(0xfb, 0x7f, 0xf8, 0, 0, 0, 0, 0, 0)]] }),
  },
  {
    id: "drisl/infinity-payload", layer: "drisl-float",
    title: "float64 +Infinity in a field",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => acctFrame(ctx, { extra: [["x", bytes(0xfb, 0x7f, 0xf0, 0, 0, 0, 0, 0, 0)]] }),
  },
  {
    id: "drisl/undefined-payload", layer: "drisl-float",
    title: "CBOR undefined (0xf7) as a field value",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => acctFrame(ctx, { extra: [["x", bytes(0xf7)]] }),
  },
  {
    id: "drisl/simple-value-payload", layer: "drisl-float",
    title: "disallowed simple value simple(19) (0xf3)",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => acctFrame(ctx, { extra: [["x", bytes(0xf3)]] }),
  },

  // ---- Layer 2c: DRISL tag & key-type rules ------------------------------
  {
    id: "drisl/tag-0-payload", layer: "drisl-tag",
    title: "CBOR tag 0 (datetime) in a field",
    note: "Only tag 42 is allowed anywhere in atproto data.",
    expect: { strict: "reject", tolerant: "either" },
    build: (ctx) => acctFrame(ctx, { extra: [["x", bytes(0xc0, tstr("2026-07-19T00:00:00Z"))]] }),
  },
  {
    id: "drisl/tag-2-bignum-payload", layer: "drisl-tag",
    title: "CBOR tag 2 (bignum) in a field",
    expect: { strict: "reject", tolerant: "either" },
    build: (ctx) => acctFrame(ctx, { extra: [["x", bytes(0xc2, bstr(bytes(0x01, 0x00)))]] }),
  },
  {
    id: "drisl/tag-42-ok-payload", layer: "drisl-tag",
    title: "CBOR tag 42 CID in a field (CONTROL: valid DRISL)",
    note: "Tag 42 wrapping a 0x00-prefixed byte string is the only legal tag.",
    expect: { strict: "accept", tolerant: "accept" },
    build: (ctx) => acctFrame(ctx, { extra: [["x", cid(DUMMY_MH)]] }),
  },
  {
    id: "drisl/tag-42-no-prefix-payload", layer: "drisl-tag",
    title: "tag 42 whose byte string lacks the 0x00 prefix",
    expect: { strict: "reject", tolerant: "either" },
    build: (ctx) => acctFrame(ctx, { extra: [["x", bytes(0xd8, 0x2a, bstr(bytes(0x01, 0x71)))]] }),
  },
  {
    id: "drisl/int-map-key-payload", layer: "drisl-tag",
    title: "integer map key in payload",
    note: "DRISL requires text-string keys; this map is {1: 2}.",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => bytes(HEADER("#account"), bytes(0xa1, uint(1), uint(2))),
  },
  {
    id: "drisl/invalid-utf8-payload", layer: "drisl-tag",
    title: "invalid UTF-8 in the did text string",
    note: "did value bytes 0xc3 0x28 are not valid UTF-8.",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => acctFrame(ctx, { override: { did: bytes(0x62, 0xc3, 0x28) } }),
  },

  // ---- Layer 3: at-sync frame semantics (valid DRISL, odd shape) ---------
  {
    id: "sync/unknown-type", layer: "at-sync",
    title: "unknown message type #futurething",
    note: "THE forward-compat case: an event type a relay predates.",
    expect: { strict: "either", tolerant: "accept" },
    build: (ctx) => bytes(HEADER("#futurething"), map([
      ["seq", uint(ctx.seq())], ["did", tstr(ctx.did)], ["time", tstr(TIME)],
    ])),
  },
  {
    id: "sync/unknown-field", layer: "at-sync",
    title: "known type with an unknown extra field",
    note: "Forward-compat: #account gains a future field. Preserved? Dropped?",
    expect: { strict: "accept", tolerant: "accept" },
    build: (ctx) => acctFrame(ctx, { extra: [["zfuture", tstr("hello from 2027")]] }),
  },
  {
    id: "sync/missing-seq", layer: "at-sync",
    title: "#account payload missing required seq",
    expect: { strict: "reject", tolerant: "either" },
    build: (ctx) => bytes(HEADER("#account"), map([
      ["did", tstr(ctx.did)], ["time", tstr(TIME)], ["active", bool(true)],
    ])),
  },
  {
    id: "sync/wrong-type-seq", layer: "at-sync",
    title: "seq encoded as a text string",
    note: "Field present but wrong CBOR type.",
    expect: { strict: "reject", tolerant: "either" },
    build: (ctx) => acctFrame(ctx, { override: { seq: tstr("42") } }),
  },
  {
    id: "sync/op-1-no-t", layer: "at-sync",
    title: "op:1 header with no t",
    expect: { strict: "reject", tolerant: "reject" },
    build: (ctx) => bytes(mapRaw([["op", uint(1)]]), account(ctx)),
  },
];
