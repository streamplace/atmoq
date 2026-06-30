// Pure decode of at-sync firehose frames — no I/O, so it tests without a relay.
//
// Each MoQ frame from an atmoq firehose is one complete at-sync message:
// a DAG-CBOR header object followed by a DAG-CBOR payload object, byte-identical
// to a `com.atproto.sync.subscribeRepos` WebSocket message.
//
// The header is a CBOR map with a `t` (type) field; the payload that follows
// is event-specific (a CAR block for commits, typed CBOR for others). We decode
// only the header here — the payload bytes are passed through raw for the
// consumer to handle, matching how indigo and the Go client treat them.
//
// CBOR decoding uses @atproto/lex-cbor, the first-party atproto data model
// codec. It handles CID tag 42 natively (decoding to Cid objects with
// .toString()) and decodeAll() yields consecutive CBOR items from a buffer.

import { decodeAll, encode, type LexValue } from "@atproto/lex-cbor";

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
 * The frame is two consecutive DAG-CBOR objects. We decode the header via
 * @atproto/lex-cbor (which handles CID tag 42 natively), then find the
 * boundary by re-encoding the header — valid because DAG-CBOR is canonical
 * (the encoding is deterministic), so the re-encoded length equals the
 * original. The remaining bytes are the raw payload.
 *
 * @throws if the frame is too short or not valid DAG-CBOR.
 */
export function decodeFrame(
  data: Uint8Array,
  group = 0,
  frame = 0,
): AtSyncMessage {
  const items = [...decodeAll<LexValue>(data)];
  if (items.length < 2) {
    throw new Error(
      `atmoq: frame has ${items.length} CBOR item(s), expected at least 2`,
    );
  }
  const header = items[0] as FrameHeader;

  // Find the header's byte boundary by re-encoding it. DAG-CBOR is canonical,
  // so encode(header).length is the exact number of bytes the header occupied
  // in the original frame. The payload is everything after that.
  const headerBytes = encode(header as LexValue);
  const payload = data.subarray(headerBytes.length);

  return { header, payload, group, frame };
}
