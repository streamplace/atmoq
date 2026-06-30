// QUIC variable-length integers (RFC 9000 §16), the encoding moq-lite uses for
// every length, id, and sequence number on the wire. Ported from go/varint.go
// to keep a shared wire contract across the Rust, Go, and TS clients.
//
// The two most significant bits of the first byte select a 1, 2, 4, or 8 byte
// form:
//   0b00xxxxxx  -> 1 byte  (0 .. 2^6-1)
//   0b01xxxxxx  -> 2 bytes (0 .. 2^14-1)
//   0b10xxxxxx  -> 4 bytes (0 .. 2^30-1)
//   0b11xxxxxx  -> 8 bytes (0 .. 2^62-1)

/** Largest value encodable as a given varint length. */
export const MAX_U6 = (1 << 6) - 1;
export const MAX_U14 = (1 << 14) - 1;
export const MAX_U30 = (1 << 30) - 1;
export const MAX_U62 = 2n ** 62n - 1n;

/** Number of bytes needed to encode `v` as a QUIC varint. */
export function size(v: number | bigint): 1 | 2 | 4 | 8 {
  const b = typeof v === "number" ? BigInt(v) : v;
  if (b < 0n) throw new RangeError("varint cannot be negative");
  if (b <= BigInt(MAX_U6)) return 1;
  if (b <= BigInt(MAX_U14)) return 2;
  if (b <= BigInt(MAX_U30)) return 4;
  if (b <= MAX_U62) return 8;
  throw new RangeError("varint value exceeds 2^62-1");
}

/** Encode `v` as a QUIC varint, appending to `dst` (or a fresh array). */
export function encode(dst: number[] | Uint8Array, v: number | bigint): Uint8Array {
  const b = typeof v === "number" ? BigInt(v) : v;
  if (b < 0n) throw new RangeError("varint cannot be negative");
  if (b > MAX_U62) throw new RangeError("varint value exceeds 2^62-1");

  const len = size(b);
  const out = new Uint8Array(len);
  switch (len) {
    case 1:
      out[0] = Number(b);
      break;
    case 2:
      out[0] = 0x40 | Number(b >> 8n);
      out[1] = Number(b & 0xffn);
      break;
    case 4:
      out[0] = 0x80 | Number(b >> 24n);
      out[1] = Number((b >> 16n) & 0xffn);
      out[2] = Number((b >> 8n) & 0xffn);
      out[3] = Number(b & 0xffn);
      break;
    case 8:
      out[0] = 0xc0 | Number(b >> 56n);
      out[1] = Number((b >> 48n) & 0xffn);
      out[2] = Number((b >> 40n) & 0xffn);
      out[3] = Number((b >> 32n) & 0xffn);
      out[4] = Number((b >> 24n) & 0xffn);
      out[5] = Number((b >> 16n) & 0xffn);
      out[6] = Number((b >> 8n) & 0xffn);
      out[7] = Number(b & 0xffn);
      break;
  }
  return concat(dst, out);
}

/** Encode a length-prefixed string (varint length + UTF-8 bytes). */
export function encodeString(dst: number[] | Uint8Array, s: string): Uint8Array {
  const bytes = new TextEncoder().encode(s);
  const withLen = encode(dst, bytes.length);
  return concat(withLen, bytes);
}

/**
 * Encode an `Option<u64>` the way moq-lite does (coding/encode.rs):
 * `None` is the varint `0`; `Some(v)` is the varint `v+1`. The +1 cannot
 * overflow in practice (group sequences are far below 2^62-1).
 */
export function encodeOption(
  dst: number[] | Uint8Array,
  v: bigint | number | undefined,
): Uint8Array {
  if (v === undefined) return encode(dst, 0);
  const b = typeof v === "number" ? BigInt(v) : v;
  return encode(dst, b + 1n);
}

/** Decode a QUIC varint from `buf` at `offset`. Returns `[value, bytesRead]`. */
export function decode(buf: Uint8Array, offset = 0): [bigint, number] {
  if (offset >= buf.length) throw new RangeError("varint: buffer underrun");
  const first = buf[offset];
  const len = 1 << (first >> 6); // 1, 2, 4, or 8
  if (offset + len > buf.length) throw new RangeError("varint: truncated");

  let val = BigInt(first & 0x3f);
  for (let i = 1; i < len; i++) {
    val = (val << 8n) | BigInt(buf[offset + i]);
  }
  return [val, len];
}

// --- internal helpers ---------------------------------------------------

/** Concatenate a previous array/Uint8Array with new bytes. */
function concat(prev: number[] | Uint8Array, next: Uint8Array): Uint8Array {
  const prevBytes = prev instanceof Uint8Array ? prev : new Uint8Array(prev);
  const out = new Uint8Array(prevBytes.length + next.length);
  out.set(prevBytes, 0);
  out.set(next, prevBytes.length);
  return out;
}
