// Minimal CAR (Content-Addressable aRchive) block reader, per at-repo §5:
// LEB128-length-prefixed header, then varint(len) | cid | block entries.
// Enough to look up record blocks by CID for display (`--ops`); MST
// verification is out of scope here (that's the M2 validation milestone).
//
// Ported from rust/crates/atmoq/src/car.rs.
//
// IMPORTANT: blocks are keyed by a hex string (not Uint8Array) because
// JavaScript Map uses reference equality for object keys — two Uint8Arrays
// with identical content are NOT equal under Map.get().

/** A map of hex-encoded CID → block bytes. */
export type CarBlocks = Map<string, Uint8Array>;

/**
 * Parse all blocks from a CAR byte array, keyed by hex-encoded CID.
 * Tolerant per at-repo §5.3: duplicate blocks are deduplicated, a truncated
 * trailing entry stops parsing rather than failing.
 */
export function parseCarBlocks(data: Uint8Array): CarBlocks {
  let offset = 0;

  // LEB128 varint for the header length.
  const headerResult = readVarint(data, offset);
  if (!headerResult) throw new Error("CAR: truncated header varint");
  const [headerLen, hRead] = headerResult;
  offset += hRead; // readVarint returns bytesRead (relative), not absolute
  if (Number(headerLen) > data.length - offset) {
    throw new Error(`CAR header length ${headerLen} exceeds input`);
  }
  offset += Number(headerLen); // skip header CBOR (version, roots)

  const blocks: CarBlocks = new Map();
  while (offset < data.length) {
    const result = readVarint(data, offset);
    if (!result) break;
    const [entryLen, eRead] = result;
    offset += eRead; // relative → absolute
    if (Number(entryLen) > data.length - offset) {
      break; // truncated trailing entry
    }
    const entry = data.subarray(offset, offset + Number(entryLen));
    offset += Number(entryLen);

    const cidResult = readCid(entry, 0);
    if (!cidResult) continue;
    const [cidBytes, cidEnd] = cidResult;
    const block = entry.subarray(cidEnd);
    // Key by hex string: Map uses reference equality for objects, so
    // Uint8Array keys would never match on lookup with a different array.
    blocks.set(toHex(cidBytes), block);
  }
  return blocks;
}

/**
 * Read a binary CIDv1 (version, codec, multihash) off the front of `buf`,
 * returning [cidBytes, bytesRead] or undefined if invalid.
 */
function readCid(
  buf: Uint8Array,
  offset: number,
): [Uint8Array, number] | undefined {
  const start = offset;
  const version = readVarint(buf, offset);
  if (!version) return undefined;
  const [v, vRead] = version;
  if (v !== 1n) return undefined; // unsupported CID version
  offset += vRead;

  // codec
  const codec = readVarint(buf, offset);
  if (!codec) return undefined;
  offset += codec[1];

  // hash code
  const hashCode = readVarint(buf, offset);
  if (!hashCode) return undefined;
  offset += hashCode[1];

  // hash length
  const hashLen = readVarint(buf, offset);
  if (!hashLen) return undefined;
  const [hl, hlRead] = hashLen;
  offset += hlRead;

  if (Number(hl) > buf.length - offset) return undefined;
  offset += Number(hl);

  const cidLen = offset - start;
  return [buf.subarray(start, start + cidLen), cidLen];
}

/**
 * Read a LEB128 unsigned varint (the multiformats/protobuf varint, NOT the
 * QUIC varint used elsewhere in this package). Returns [value, bytesRead]
 * or undefined if truncated.
 */
export function readVarint(
  buf: Uint8Array,
  offset: number,
): [bigint, number] | undefined {
  let value = 0n;
  let shift = 0n;
  let pos = offset;
  while (pos < buf.length) {
    const byte = buf[pos];
    pos++;
    value |= BigInt(byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) {
      return [value, pos - offset];
    }
    shift += 7n;
    if (shift >= 64n) return undefined; // overflow
  }
  return undefined; // truncated
}

/** Encode bytes as a lowercase hex string. */
function toHex(bytes: Uint8Array): string {
  let hex = "";
  for (const b of bytes) {
    hex += b.toString(16).padStart(2, "0");
  }
  return hex;
}
