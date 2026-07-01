// DRISL validation: https://dasl.ing/drisl.html
//
// DRISL is a deterministic CBOR profile (a subset of CBOR/c,
// draft-rundgren-cbor-core) — the encoding atproto records and at-sync frames
// are supposed to use. atmoq takes the opinionated position that everything
// across the stack only works on valid DRISL: the relay rejects invalid DRISL
// at ingest, and clients reject it at decode. This module is the client-side
// validator.
//
// The rules enforced here, per the DRISL spec and CBOR/c which it inherits:
//   - definite lengths only (no indefinite-length items, no break code)
//   - minimal-length ("preferred") encoding of every int and length argument
//   - map keys must be text strings, unique, and sorted in bytewise
//     lexicographic order of their encoded bytes (for text keys this is the
//     same order as DAG-CBOR's length-first rule)
//   - floats must be 64-bit (never half/single precision); NaN and ±Infinity
//     are rejected (negative zero is the only allowed special value)
//   - tag 42 (CID) is the only allowed tag; its content must be a byte string
//     with the historical 0x00 multibase prefix
//   - the only allowed simple values are false, true, and null
//   - text strings must be valid UTF-8
//
// Validation is a single pass over the raw bytes that also returns the end
// offset of the first CBOR item — which is exactly what at-sync frame parsing
// needs to split the header object from the payload object without re-encoding
// (re-encoding is how you get silently corrupt payload boundaries).
//
// Canonical implementations this mirrors: https://github.com/hyphacoop/go-dasl
// (Go) and https://github.com/n0-computer/dasl (Rust).

/** Thrown when bytes are not valid DRISL. Carries the offset of the violation. */
export class InvalidDrislError extends Error {
  /** Byte offset in the input where the violation was found. */
  readonly offset: number;

  constructor(message: string, offset: number) {
    super(`invalid DRISL at byte ${offset}: ${message}`);
    this.name = "InvalidDrislError";
    this.offset = offset;
  }
}

// Deeply nested documents are rejected rather than risking stack exhaustion
// (the validator recurses per level). Real atproto records nest a handful of
// levels; 128 matches serde_ipld_dagcbor's recursion limit. Keep in sync with
// the Rust sibling (rust/crates/atmoq/src/drisl.rs).
const MAX_DEPTH = 128;

const utf8 = new TextDecoder("utf-8", { fatal: true });

/**
 * Validate one complete DRISL item starting at `offset`.
 *
 * @returns the offset just past the item.
 * @throws {InvalidDrislError} on any DRISL violation or truncation.
 */
export function validateDrisl(data: Uint8Array, offset = 0): number {
  return validateItem(data, offset, 0);
}

/**
 * Validate that `data` is exactly one complete DRISL item — no trailing bytes.
 *
 * @throws {InvalidDrislError} on any DRISL violation, truncation, or trailing data.
 */
export function assertDrisl(data: Uint8Array): void {
  const end = validateDrisl(data, 0);
  if (end !== data.length) {
    throw new InvalidDrislError(
      `${data.length - end} trailing byte(s) after item`,
      end,
    );
  }
}

/** Read the argument (value or length) for an initial byte, enforcing minimal encoding. */
function readArg(
  data: Uint8Array,
  offset: number,
  what: string,
): { value: bigint; end: number } {
  const ai = data[offset] & 0x1f;
  if (ai < 24) return { value: BigInt(ai), end: offset + 1 };
  if (ai > 27) {
    // 28-30 are reserved; 31 is indefinite-length / break.
    throw new InvalidDrislError(
      ai === 31
        ? `indefinite-length ${what} is not allowed`
        : `reserved additional-info value ${ai}`,
      offset,
    );
  }
  const width = 1 << (ai - 24); // 24→1, 25→2, 26→4, 27→8 bytes
  if (offset + 1 + width > data.length) {
    throw new InvalidDrislError(`truncated ${what} argument`, offset);
  }
  let value = 0n;
  for (let i = 0; i < width; i++) {
    value = (value << 8n) | BigInt(data[offset + 1 + i]);
  }
  const minimal =
    ai === 24 ? 24n : ai === 25 ? 256n : ai === 26 ? 65536n : 4294967296n;
  if (value < minimal) {
    throw new InvalidDrislError(
      `non-minimal encoding of ${what} ${value} (${width}-byte argument)`,
      offset,
    );
  }
  return { value, end: offset + 1 + width };
}

/** Compare two encoded-key byte ranges bytewise-lexicographically. */
function compareEncoded(
  data: Uint8Array,
  aStart: number,
  aEnd: number,
  bStart: number,
  bEnd: number,
): number {
  const aLen = aEnd - aStart;
  const bLen = bEnd - bStart;
  const n = Math.min(aLen, bLen);
  for (let i = 0; i < n; i++) {
    const d = data[aStart + i] - data[bStart + i];
    if (d !== 0) return d;
  }
  return aLen - bLen;
}

function validateItem(data: Uint8Array, offset: number, depth: number): number {
  if (depth > MAX_DEPTH) {
    throw new InvalidDrislError(`nesting deeper than ${MAX_DEPTH}`, offset);
  }
  if (offset >= data.length) {
    throw new InvalidDrislError("truncated: expected an item", offset);
  }
  const initial = data[offset];
  const major = initial >> 5;

  switch (major) {
    case 0: // unsigned int
    case 1: {
      // negative int
      return readArg(data, offset, major === 0 ? "uint" : "negint").end;
    }
    case 2: // byte string
    case 3: {
      // text string
      const { value, end } = readArg(
        data,
        offset,
        major === 2 ? "byte string length" : "text string length",
      );
      const len = Number(value);
      if (end + len > data.length) {
        throw new InvalidDrislError("truncated string body", end);
      }
      if (major === 3) {
        try {
          utf8.decode(data.subarray(end, end + len));
        } catch {
          throw new InvalidDrislError("text string is not valid UTF-8", end);
        }
      }
      return end + len;
    }
    case 4: {
      // array
      const { value, end } = readArg(data, offset, "array length");
      let cursor = end;
      for (let i = 0n; i < value; i++) {
        cursor = validateItem(data, cursor, depth + 1);
      }
      return cursor;
    }
    case 5: {
      // map
      const { value, end } = readArg(data, offset, "map length");
      let cursor = end;
      let prevKeyStart = -1;
      let prevKeyEnd = -1;
      for (let i = 0n; i < value; i++) {
        const keyStart = cursor;
        if (keyStart >= data.length) {
          throw new InvalidDrislError("truncated: expected a map key", keyStart);
        }
        if (data[keyStart] >> 5 !== 3) {
          throw new InvalidDrislError("map key is not a text string", keyStart);
        }
        const keyEnd = validateItem(data, keyStart, depth + 1);
        if (prevKeyStart >= 0) {
          const cmp = compareEncoded(
            data,
            prevKeyStart,
            prevKeyEnd,
            keyStart,
            keyEnd,
          );
          if (cmp === 0) {
            throw new InvalidDrislError("duplicate map key", keyStart);
          }
          if (cmp > 0) {
            throw new InvalidDrislError(
              "map keys are not in bytewise lexicographic order",
              keyStart,
            );
          }
        }
        prevKeyStart = keyStart;
        prevKeyEnd = keyEnd;
        cursor = validateItem(data, keyEnd, depth + 1);
      }
      return cursor;
    }
    case 6: {
      // tag
      const { value, end } = readArg(data, offset, "tag");
      if (value !== 42n) {
        throw new InvalidDrislError(
          `tag ${value} is not allowed (only tag 42/CID)`,
          offset,
        );
      }
      if (end >= data.length || data[end] >> 5 !== 2) {
        throw new InvalidDrislError(
          "tag 42 content must be a byte string",
          end,
        );
      }
      const contentEnd = validateItem(data, end, depth + 1);
      // The byte string body starts after its own head; check the 0x00 prefix.
      const bodyStart = readArg(data, end, "byte string length").end;
      if (contentEnd === bodyStart || data[bodyStart] !== 0x00) {
        throw new InvalidDrislError(
          "tag 42 CID must start with the 0x00 prefix",
          bodyStart,
        );
      }
      return contentEnd;
    }
    case 7: {
      // simple values and floats
      const ai = initial & 0x1f;
      if (initial === 0xf4 || initial === 0xf5 || initial === 0xf6) {
        return offset + 1; // false, true, null
      }
      if (initial === 0xfb) {
        // 64-bit float — the only float width DRISL allows.
        if (offset + 9 > data.length) {
          throw new InvalidDrislError("truncated float64", offset);
        }
        const view = new DataView(data.buffer, data.byteOffset + offset + 1, 8);
        const f = view.getFloat64(0);
        if (Number.isNaN(f)) {
          throw new InvalidDrislError("NaN is not allowed", offset);
        }
        if (!Number.isFinite(f)) {
          throw new InvalidDrislError("infinity is not allowed", offset);
        }
        return offset + 9;
      }
      if (initial === 0xf9 || initial === 0xfa) {
        throw new InvalidDrislError(
          `${initial === 0xf9 ? "half" : "single"}-precision float is not allowed (floats must be 64-bit)`,
          offset,
        );
      }
      if (initial === 0xf7) {
        throw new InvalidDrislError("undefined is not allowed", offset);
      }
      if (initial === 0xff) {
        throw new InvalidDrislError("unexpected break code", offset);
      }
      throw new InvalidDrislError(
        `simple value ${ai === 24 ? data[offset + 1] : ai} is not allowed (only false/true/null)`,
        offset,
      );
    }
    default:
      // Unreachable: major is 3 bits.
      throw new InvalidDrislError(`unknown major type ${major}`, offset);
  }
}
