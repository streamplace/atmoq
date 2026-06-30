// Pure decode of at-sync firehose frames — no I/O, so it tests without a relay.
//
// Each MoQ frame from an atmoq firehose is one complete at-sync message:
// a DAG-CBOR header object followed by a DAG-CBOR payload object, byte-identical
// to a `com.atproto.sync.subscribeRepos` WebSocket message. This mirrors the Go
// client's frame semantics (go/client.go §frameItem) and the atproto firehose
// subscription repo message format.
//
// The header is a CBOR map with a `t` (type) field; the payload that follows
// is event-specific (a CAR block for commits, typed CBOR for others). We decode
// only the header here — the payload bytes are passed through raw for the
// consumer to handle, matching how indigo and the Go client treat them.

import * as dagCbor from "@ipld/dag-cbor";

/** at-sync message type tags (the `t` field in the header object). */
export type MessageType =
  | "#commit"
  | "#identity"
  | "#account"
  | "#handle"
  | "#tombstone"
  | "#info"
  | "#seq"
  | "#labels"
  | "#blob";

/** Raw header object as decoded from DAG-CBOR (the first object in the frame). */
export interface FrameHeader {
  /** Message type discriminator, e.g. `#commit`. */
  t: MessageType | string;
  /** Other fields are type-specific and passed through untyped. */
  [key: string]: unknown;
}

/** A decoded at-sync message: the header plus the raw payload bytes. */
export interface AtSyncMessage {
  /** Decoded DAG-CBOR header object. */
  header: FrameHeader;
  /** Raw payload bytes (the second CBOR object) — type-specific. */
  payload: Uint8Array;
  /** Sequence number of the MoQ group this frame arrived in. */
  group: number;
  /** Sequence number of the frame within its group. */
  frame: number;
}

/**
 * Decode a raw at-sync frame (one MoQ frame's bytes) into header + payload.
 *
 * The frame is two consecutive DAG-CBOR objects. We scan the first item to find
 * the boundary (CBOR is self-delimiting), then decode the header and slice off
 * the remaining bytes as the payload.
 *
 * @throws if the frame is too short or not valid DAG-CBOR.
 */
export function decodeFrame(
  data: Uint8Array,
  group = 0,
  frame = 0,
): AtSyncMessage {
  const headerLen = scanCborItem(data);
  const headerBytes = data.subarray(0, headerLen);
  const payload = data.subarray(headerLen);

  const header = dagCbor.decode<FrameHeader>(headerBytes);

  return { header, payload, group, frame };
}

// --- CBOR item-length scanner -------------------------------------------
//
// CBOR (RFC 8949) is self-delimiting: from the initial byte you can determine
// how many bytes each item occupies. We scan one top-level item to find the
// boundary between the two DAG-CBOR objects in a frame, then decode each half
// separately. This avoids the "too many terminals" error that @ipld/dag-cbor
// (via cborg strict mode) throws when trailing data follows the first item.
//
// DAG-CBOR is canonical (no indefinite-length encoding), so the scan is
// straightforward. We handle all major types; the only one that recurses is
// containers (arrays and maps).

/** Return the total byte length of the first CBOR item in `buf`. */
function scanCborItem(buf: Uint8Array, offset = 0): number {
  if (offset >= buf.length) throw new RangeError("CBOR: buffer underrun");

  const first = buf[offset];
  const majorType = first >> 5;
  const ai = first & 0x1f;

  // Resolve the "additional information" into a value + the header bytes
  // it consumed (excluding the content that follows).
  const [val, headerLen] = resolveAi(buf, offset, ai);
  let contentLen = 0;

  switch (majorType) {
    case 0: // unsigned int — value is in the header, no content
    case 1: // negative int — same
    case 7: // float / simple — same (DAG-CBOR uses 0/1/2/4/8-byte forms)
      contentLen = 0;
      break;
    case 2: // byte string
    case 3: // text string
      contentLen = Number(val);
      break;
    case 4: // array — `val` items follow
      for (let i = 0; i < Number(val); i++) {
        contentLen += scanCborItem(buf, offset + headerLen + contentLen);
      }
      break;
    case 5: // map — `val` key-value pairs follow (2 * val items)
      for (let i = 0; i < Number(val); i++) {
        contentLen += scanCborItem(buf, offset + headerLen + contentLen); // key
        contentLen += scanCborItem(buf, offset + headerLen + contentLen); // value
      }
      break;
    case 6: // tag — one item follows (the tagged value)
      contentLen = scanCborItem(buf, offset + headerLen);
      break;
    default:
      throw new Error(`CBOR: unsupported major type ${majorType}`);
  }

  return headerLen + contentLen;
}

/**
 * Resolve the CBOR "additional information" field into [value, headerBytes].
 * `headerBytes` is the size of the initial byte(s) (1 + any argument bytes).
 */
function resolveAi(
  buf: Uint8Array,
  offset: number,
  ai: number,
): [bigint, number] {
  if (ai < 24) return [BigInt(ai), 1];
  if (ai === 24) return [BigInt(buf[offset + 1]), 2];
  if (ai === 25) {
    return [BigInt((buf[offset + 1] << 8) | buf[offset + 2]), 3];
  }
  if (ai === 26) {
    const v =
      (buf[offset + 1] * 2 ** 24) +
      (buf[offset + 2] << 16) +
      (buf[offset + 3] << 8) +
      buf[offset + 4];
    return [BigInt(v >>> 0), 5];
  }
  if (ai === 27) {
    let v = 0n;
    for (let i = 1; i <= 8; i++) v = (v << 8n) | BigInt(buf[offset + i]);
    return [v, 9];
  }
  // 28-30 are reserved; 31 is indefinite-length (not in canonical DAG-CBOR).
  throw new Error(`CBOR: unsupported additional info ${ai}`);
}
