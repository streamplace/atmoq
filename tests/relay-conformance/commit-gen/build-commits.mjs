// Generate real, signed #commit firehose frames with one targeted defect each.
//
// Unlike the #account corpus (hand-built CBOR), a #commit carries a CAR of
// blocks — a signed commit object, the record blocks, and MST nodes — with
// content-addressed CIDs. To test record- and repo-level defects we build a
// genuine repo with @atproto/repo and a keypair we control, so signatures and
// CIDs are valid; then we perturb exactly one thing. The signing key is emitted
// alongside the corpus (as a did:key) so each relay harness can verify the
// commit signature the way the relay would after resolving identity.
//
// Output: corpus-commit.json — [{id, layer, title, note, expect, hex, signingKey, repoDid}]

import { writeFileSync } from "node:fs";
import { Secp256k1Keypair } from "@atproto/crypto";
import { cborEncode, TID } from "@atproto/common";
import {
  Repo, MemoryBlockstore, blocksToCarFile, cidForRecord, WriteOpAction, readCar,
} from "@atproto/repo";
import * as cbor from "@ipld/dag-cbor";
import { CID } from "multiformats/cid";

const REPO_DID = "did:plc:conformancecommit000000000";
const NOW = "2026-07-19T00:00:00.000Z";
const COLLECTION = "app.bsky.feed.post";

// Build a fresh single-commit (init) repo containing the given records, and
// return everything needed to assemble a firehose #commit payload.
async function buildCommit(records, keypair) {
  const storage = new MemoryBlockstore();
  const writes = records.map((record, i) => ({
    action: WriteOpAction.Create,
    collection: COLLECTION,
    rkey: `3l${String(i).padStart(6, "0")}aaaa22`, // stable pseudo-TID rkeys
    record,
  }));
  const commit = await Repo.formatInitCommit(storage, REPO_DID, keypair, writes);
  const ops = [];
  for (const w of writes) {
    ops.push({
      action: "create",
      path: `${w.collection}/${w.rkey}`,
      cid: await cidForRecord(w.record),
    });
  }
  const blocks = new (commit.newBlocks.constructor)();
  blocks.addMap(commit.newBlocks);
  blocks.addMap(commit.relevantBlocks);
  const car = await blocksToCarFile(commit.cid, blocks);
  return { commit, ops, car };
}

// Assemble the firehose #commit payload object (canonical field set from the
// PDS sequencer's formatSeqCommit + the seq/time the sequencer adds).
function commitPayload({ commit, ops, car }, overrides = {}) {
  return {
    seq: 100,
    rebase: false,
    tooBig: false,
    repo: REPO_DID,
    commit: commit.cid,
    rev: commit.rev,
    since: commit.since ?? null,
    blocks: car,
    ops,
    blobs: [],
    time: NOW,
    ...overrides,
  };
}

const HEADER = { op: 1, t: "#commit" };
const frameHex = (payloadBytes) =>
  Buffer.concat([Buffer.from(cborEncode(HEADER)), Buffer.from(payloadBytes)]).toString("hex");
const canonicalFrame = (payload) => frameHex(cborEncode(payload));

const validRecord = () => ({
  $type: COLLECTION,
  text: "hello from a conformance commit",
  createdAt: NOW,
});

const keypair = await Secp256k1Keypair.create({ exportable: true });
const signingKey = keypair.did(); // did:key form the relays resolve identity to

const cases = [];
const add = (c) => cases.push({ repoDid: REPO_DID, signingKey, ...c });

// ---- control: a fully valid commit -------------------------------------
{
  const built = await buildCommit([validRecord()], keypair);
  add({
    id: "commit/valid", layer: "commit", title: "valid signed #commit (CONTROL)",
    note: "One create op; correct CAR, CIDs, signature. Must be accepted.",
    expect: { strict: "accept", tolerant: "accept" },
    hex: canonicalFrame(commitPayload(built)),
  });
}

// ---- record missing $type (the reported case) ---------------------------
{
  const built = await buildCommit([{ text: "no type here", createdAt: NOW }], keypair);
  add({
    id: "commit/record-no-type", layer: "commit-record",
    title: "create op whose record has no $type",
    note: "Record is valid CBOR with correct CID & signature, but omits $type.",
    expect: { strict: "reject", tolerant: "either" },
    hex: canonicalFrame(commitPayload(built)),
  });
}

// ---- record with an unknown $type (forward-compat) ----------------------
{
  const built = await buildCommit(
    [{ $type: "com.example.futurerecord", text: "from the future", createdAt: NOW }],
    keypair,
  );
  add({
    id: "commit/record-unknown-type", layer: "commit-record",
    title: "create op whose record has an unknown $type / NSID",
    note: "Relays are app-agnostic; an unknown record type should pass through.",
    expect: { strict: "accept", tolerant: "accept" },
    hex: canonicalFrame(commitPayload(built)),
  });
}

// ---- record that is not a CBOR map --------------------------------------
try {
  const built = await buildCommit([["not", "a", "map"]], keypair);
  add({
    id: "commit/record-not-map", layer: "commit-record",
    title: "create op whose record is a CBOR array, not a map",
    note: "atproto records must be maps; this one is a list.",
    expect: { strict: "reject", tolerant: "reject" },
    hex: canonicalFrame(commitPayload(built)),
  });
} catch (e) {
  console.error("skip record-not-map:", e.message);
}

// ---- envelope key order (valid CAR, non-DRISL outer payload) ------------
{
  const built = await buildCommit([validRecord()], keypair);
  const p = commitPayload(built);
  // Encode the payload map with keys in insertion order (NOT sorted) by using a
  // Map, which dag-cbor rejects... so hand-encode via a non-sorting encoder.
  // Simplest: take the canonical bytes and prepend a deliberately mis-ordered
  // duplicate is wrong; instead emit with a reversed-key raw map.
  const raw = rawMapReorder(p, ["time", "seq", "repo", "commit", "rev", "since", "blocks", "ops", "blobs", "rebase", "tooBig"]);
  add({
    id: "commit/envelope-unordered", layer: "commit-envelope",
    title: "#commit envelope map keys out of DRISL order",
    note: "CAR/signature valid; only the outer payload map is mis-sorted.",
    expect: { strict: "reject", tolerant: "either" },
    hex: frameHex(raw),
  });
}

// ---- envelope carries a float16 field -----------------------------------
{
  const built = await buildCommit([validRecord()], keypair);
  const bytes = cborEncode(commitPayload(built));
  // append is not valid; instead re-encode with an extra float16 field appended
  // into the map. Build raw: bump map count, add key "x" + float16 at the front
  // of the entries (x sorts first among these short keys is false; we place raw).
  const raw = injectFloat16(commitPayload(built));
  add({
    id: "commit/envelope-float16", layer: "commit-envelope",
    title: "#commit envelope carries a float16 field",
    note: "Half-precision float in the outer payload; DRISL allows only float64.",
    expect: { strict: "reject", tolerant: "either" },
    hex: frameHex(raw),
  });
}

// ---- tooBig flag set ----------------------------------------------------
{
  const built = await buildCommit([validRecord()], keypair);
  add({
    id: "commit/too-big", layer: "commit-repo",
    title: "#commit with tooBig=true",
    note: "Deprecated oversized-commit flag; some relays reject it outright.",
    expect: { strict: "reject", tolerant: "either" },
    hex: canonicalFrame(commitPayload(built, { tooBig: true })),
  });
}

// ---- CAR block CID mismatch (corrupt a record block body) ---------------
{
  const built = await buildCommit([validRecord()], keypair);
  const badCar = await corruptRecordBlock(built.car);
  add({
    id: "commit/cid-mismatch", layer: "commit-repo",
    title: "CAR block whose bytes no longer hash to its CID",
    note: "A record block body is flipped; content-address integrity breaks.",
    expect: { strict: "reject", tolerant: "reject" },
    hex: canonicalFrame(commitPayload({ ...built, car: badCar })),
  });
}

// ---- missing referenced block (op points at absent CID) -----------------
{
  const built = await buildCommit([validRecord()], keypair);
  const strippedCar = await dropRecordBlock(built.car);
  add({
    id: "commit/missing-block", layer: "commit-repo",
    title: "create op references a record CID absent from the CAR",
    note: "Record block removed; the MST op points at a block that isn't there.",
    expect: { strict: "reject", tolerant: "either" },
    hex: canonicalFrame(commitPayload({ ...built, car: strippedCar })),
  });
}

// ---- bad signature ------------------------------------------------------
{
  const wrongKey = await Secp256k1Keypair.create();
  const built = await buildCommit([validRecord()], wrongKey); // signed by the WRONG key
  add({
    id: "commit/bad-signature", layer: "commit-repo",
    title: "#commit signed by the wrong key",
    note: "CAR/CIDs valid; signature does not verify against the repo's identity.",
    expect: { strict: "reject", tolerant: "either" },
    hex: canonicalFrame(commitPayload(built)),
  });
}

writeFileSync(new URL("../corpus-commit.json", import.meta.url), JSON.stringify(cases, null, 2));
console.error(`wrote ${cases.length} commit cases; signingKey=${signingKey}`);

// ---------------------------------------------------------------------------
// helpers that hand-encode non-canonical CBOR / mutate CAR bytes
// ---------------------------------------------------------------------------

// Re-encode an object as a definite map but with keys in the given order (any
// keys not listed are appended in their natural order). Values are dag-cbor.
function rawMapReorder(obj, order) {
  const keys = Object.keys(obj);
  const ordered = [...order.filter((k) => keys.includes(k)), ...keys.filter((k) => !order.includes(k))];
  const parts = [mapHeader(ordered.length)];
  for (const k of ordered) {
    parts.push(cborEncode(k), cborEncode(obj[k]));
  }
  return concatBytes(parts);
}

function injectFloat16(obj) {
  // Build a DRISL-canonical map (keys sorted by encoded-key bytes) whose only
  // defect is one extra field carrying a float16 value — so a strict validator
  // rejects on the float width, not on key order.
  const pairs = Object.keys(obj).map((k) => [cborEncode(k), cborEncode(obj[k])]);
  pairs.push([cborEncode("zz16"), new Uint8Array([0xf9, 0x3c, 0x00])]); // float16 1.0
  pairs.sort((a, b) => cmpBytes(a[0], b[0]));
  return concatBytes([mapHeader(pairs.length), ...pairs.flat()]);
}

function cmpBytes(a, b) {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) if (a[i] !== b[i]) return a[i] - b[i];
  return a.length - b.length;
}

function mapHeader(n) {
  if (n < 24) return new Uint8Array([0xa0 | n]);
  if (n < 256) return new Uint8Array([0xb8, n]);
  return new Uint8Array([0xb9, n >> 8, n & 0xff]);
}
function concatBytes(arrs) {
  const total = arrs.reduce((a, b) => a + b.length, 0);
  const out = new Uint8Array(total);
  let o = 0;
  for (const a of arrs) { out.set(a, o); o += a.length; }
  return out;
}

// Read the CAR, flip one byte in the first non-commit (record) block body, and
// re-serialize with the SAME (now wrong) CIDs so integrity checks fail.
async function corruptRecordBlock(carBytes) {
  const { roots, blocks } = await readCar(carBytes);
  const rootStr = roots[0].toString();
  let target = null;
  for (const cid of blocks.cids()) {
    if (cid.toString() !== rootStr) { target = cid; break; }
  }
  const body = blocks.get(target);
  const flipped = new Uint8Array(body);
  flipped[flipped.length - 1] ^= 0xff;
  return rebuildCar(roots[0], blocks, { [target.toString()]: flipped });
}

async function dropRecordBlock(carBytes) {
  const { roots, blocks } = await readCar(carBytes);
  const rootStr = roots[0].toString();
  let drop = null;
  for (const cid of blocks.cids()) {
    if (cid.toString() !== rootStr) { drop = cid.toString(); break; }
  }
  return rebuildCar(roots[0], blocks, {}, new Set([drop]));
}

// Rebuild a CARv1 from a BlockMap with optional body overrides and drops,
// preserving each block's original CID bytes (so overrides = integrity break).
function rebuildCar(root, blocks, overrides = {}, drop = new Set()) {
  const parts = [];
  const headerBytes = cborEncode({ version: 1, roots: [root] });
  parts.push(varint(headerBytes.length), headerBytes);
  for (const cid of blocks.cids()) {
    const s = cid.toString();
    if (drop.has(s)) continue;
    const body = overrides[s] ?? blocks.get(cid);
    const cidBytes = cid.bytes;
    const len = cidBytes.length + body.length;
    parts.push(varint(len), cidBytes, body);
  }
  return concatBytes(parts.map((p) => (p instanceof Uint8Array ? p : Uint8Array.from(p))));
}

function varint(n) {
  const out = [];
  while (n >= 0x80) { out.push((n & 0x7f) | 0x80); n >>>= 7; }
  out.push(n);
  return Uint8Array.from(out);
}
