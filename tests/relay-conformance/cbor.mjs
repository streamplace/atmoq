// Minimal DRISL/DAG-CBOR byte builders.
//
// We hand-build CBOR bytes rather than lean on a library so we can emit both
// *valid* DRISL and deliberately *invalid* encodings (non-minimal ints,
// unsorted keys, wrong float widths, forbidden tags, indefinite lengths) that
// no conformant encoder would ever produce. Every helper returns a Uint8Array;
// concatenate with `bytes(...)`.

export const bytes = (...parts) =>
  Uint8Array.from(parts.flatMap((p) => (typeof p === "number" ? [p] : [...p])));

// --- minimal (canonical) argument encoding for a given major type ---------
const arg = (major, n) => {
  const mt = major << 5;
  if (n < 24) return bytes(mt | n);
  if (n < 0x100) return bytes(mt | 24, n);
  if (n < 0x10000) return bytes(mt | 25, n >> 8, n & 0xff);
  if (n < 0x100000000)
    return bytes(mt | 26, (n >>> 24) & 0xff, (n >> 16) & 0xff, (n >> 8) & 0xff, n & 0xff);
  // 64-bit
  const hi = Math.floor(n / 0x100000000);
  const lo = n >>> 0;
  return bytes(mt | 27, (hi >>> 24) & 0xff, (hi >> 16) & 0xff, (hi >> 8) & 0xff, hi & 0xff,
    (lo >>> 24) & 0xff, (lo >> 16) & 0xff, (lo >> 8) & 0xff, lo & 0xff);
};

export const uint = (n) => arg(0, n);
export const negint = (n) => arg(1, -1 - n); // n is the actual negative value
export const bstr = (b) => bytes(arg(2, b.length), b);
export const tstr = (s) => {
  const b = new TextEncoder().encode(s);
  return bytes(arg(3, b.length), b);
};
export const bool = (v) => bytes(v ? 0xf5 : 0xf4);
export const nul = () => bytes(0xf6);

export const float64 = (n) => {
  const buf = new ArrayBuffer(8);
  new DataView(buf).setFloat64(0, n, false);
  return bytes(0xfb, new Uint8Array(buf));
};

export const array = (items) => bytes(arg(4, items.length), ...items);

// DRISL map: keys must be text, unique, sorted by bytewise-lexicographic order
// of their *encoded* bytes. We sort here so callers get canonical output; the
// invalid-order cases bypass this by building the map bytes directly.
export const map = (entries) => {
  const encoded = entries.map(([k, v]) => [tstr(k), v]);
  encoded.sort((a, b) => cmpBytes(a[0], b[0]));
  return bytes(arg(5, encoded.length), ...encoded.flatMap(([k, v]) => [k, v]));
};

// A map with keys in exactly the given order, no sorting (for invalid cases).
export const mapRaw = (entries) =>
  bytes(arg(5, entries.length), ...entries.flatMap(([k, v]) => [tstr(k), v]));

// CID as tag 42 -> byte string with historical 0x00 multibase prefix.
export const cid = (multihashBytes) =>
  bytes(0xd8, 0x2a, bstr(bytes(0x00, multihashBytes)));

export const cmpBytes = (a, b) => {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) if (a[i] !== b[i]) return a[i] - b[i];
  return a.length - b.length;
};

export const concat = (...arrs) => bytes(...arrs);
