// Shared frame decoding/normalization for firehose captures.
//
// All capture paths (indigo WS, direct PDS WS, atmoq WS, atmoq MoQ)
// funnel through `normalizeFrame` so their outputs are directly diffable.
import { cborDecodeMulti } from "@atproto/common";

const normalize = (v) => {
  if (v instanceof Uint8Array) return { $bytesLength: v.byteLength };
  if (v && typeof v === "object") {
    if (v.asCID === v) return v.toString(); // multiformats CID
    if (Array.isArray(v)) return v.map(normalize);
    return Object.fromEntries(
      Object.entries(v).map(([k, val]) => [k, normalize(val)]),
    );
  }
  return v;
};

/** Decode one wire frame (header+payload CBOR) into normalized JSON. */
export const normalizeFrame = (bytes) => {
  const [header, payload] = cborDecodeMulti(bytes);
  return { header: normalize(header), payload: normalize(payload) };
};
