// Pure decode of at-sync firehose frames — no I/O, so it tests without a relay.
//
// Each MoQ frame from an atmoq firehose is one complete at-sync message:
// a DRISL header object followed by a DRISL payload object, byte-identical
// to a `com.atproto.sync.subscribeRepos` WebSocket message.
//
// atmoq is DRISL-strict end to end (see drisl.ts): both objects are validated
// against the DRISL profile before any value decoding, and frames that fail
// validation are rejected with InvalidDrislError. Validation is also what
// locates the header/payload boundary — the validator returns the exact end
// offset of the header object, so no re-encoding is involved.
//
// Value decoding uses @atproto/lex-cbor, the first-party atproto data model
// codec: it handles CID tag 42 natively (decoding to Cid objects with
// .toString()) and — on the decode side — accepts float64, which DRISL allows.

import { decode, type LexValue } from "@atproto/lex-cbor";
import { validateDrisl, InvalidDrislError } from "./drisl.js";

export { InvalidDrislError };

/** Thrown when a frame is structurally not an at-sync message (but may be valid DRISL). */
export class InvalidFrameError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidFrameError";
  }
}

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

/** Raw header object as decoded from DRISL (the first object in the frame). */
export interface FrameHeader {
  /** Message type discriminator, e.g. `#commit`. Absent on error frames (op -1). */
  t?: MessageType | string;
  /** Other fields are type-specific and passed through untyped. */
  [key: string]: unknown;
}

/** A decoded at-sync message: the header plus the raw payload bytes. */
export interface AtSyncMessage {
  /** Decoded DRISL header object. */
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
 * Both CBOR objects are validated as DRISL; the validator's end offset for the
 * header object is the exact header/payload boundary. Trailing bytes after the
 * payload object are rejected, matching the Rust reference (frame.rs).
 *
 * @throws {InvalidDrislError} if either object is not valid DRISL.
 * @throws {InvalidFrameError} if the frame is not two objects with a map header.
 */
export function decodeFrame(
  data: Uint8Array,
  group = 0,
  frame = 0,
): AtSyncMessage {
  const headerEnd = validateDrisl(data, 0);
  if (headerEnd >= data.length) {
    throw new InvalidFrameError(
      "atmoq: frame has 1 CBOR item, expected header + payload",
    );
  }
  const payloadEnd = validateDrisl(data, headerEnd);
  if (payloadEnd !== data.length) {
    throw new InvalidDrislError(
      `${data.length - payloadEnd} trailing byte(s) after payload`,
      payloadEnd,
    );
  }

  const header = decode<LexValue>(data.subarray(0, headerEnd));
  if (
    typeof header !== "object" ||
    header === null ||
    Array.isArray(header) ||
    header instanceof Uint8Array
  ) {
    throw new InvalidFrameError("atmoq: frame header is not a map");
  }

  const payload = data.subarray(headerEnd, payloadEnd);
  return { header: header as FrameHeader, payload, group, frame };
}
