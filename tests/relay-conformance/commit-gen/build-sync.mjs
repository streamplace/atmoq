// Generate sync-1.1 (at-synchronization) compliance cases.
//
// These probe the rules that make sync 1.1 verifiable-without-fetching: the
// `#sync` event, and the §4.5 commit checks that only fire on a SECOND commit
// once the relay holds prior repo state — prevData presence & correctness, rev
// ordering, and the deprecated rebase/tooBig flags. So most cases carry a
// SEQUENCE: a valid setup commit to establish state, then the commit under test.
//
// Built on real repos (@atproto/repo) with a controlled keypair. Output:
// corpus-sync.json — [{id, layer, title, note, expect, signingKey, repoDid,
//   frames:[{role:"setup"|"test", hex}]}]  (verdict = behavior on the last frame)

import { writeFileSync } from "node:fs";
import { Secp256k1Keypair } from "@atproto/crypto";
import { cborEncode, cborDecode, TID } from "@atproto/common";
import {
  Repo, MemoryBlockstore, BlockMap, blocksToCarFile, cidForRecord, WriteOpAction, signCommit,
} from "@atproto/repo";

const REPO_DID = "did:plc:conformancesync0000000000";
const NOW = "2026-07-19T00:00:00.000Z";
const COL = "app.bsky.feed.post";
const rec = (t) => ({ $type: COL, text: t, createdAt: NOW });
const rkeyFor = (n) => `3lsync${String(n).padStart(5, "0")}zz`;

const keypair = await Secp256k1Keypair.create({ exportable: true });
const signingKey = keypair.did();

const HEADER_COMMIT = cborEncode({ op: 1, t: "#commit" });
const HEADER_SYNC = cborEncode({ op: 1, t: "#sync" });
const concat = (a, b) => Buffer.concat([Buffer.from(a), Buffer.from(b)]);
const hexFrame = (header, payloadBytes) => concat(header, payloadBytes).toString("hex");

// Build a 2-commit repo and return the pieces needed to assemble both frames.
async function buildPair() {
  const storage = new MemoryBlockstore();
  const c1 = await Repo.formatInitCommit(storage, REPO_DID, keypair, [
    { action: WriteOpAction.Create, collection: COL, rkey: rkeyFor(1), record: rec("first") },
  ]);
  const repo = await Repo.createFromCommit(storage, c1);
  const c2 = await repo.formatCommit(
    [{ action: WriteOpAction.Create, collection: COL, rkey: rkeyFor(2), record: rec("second") }],
    keypair,
  );
  const c1Data = cborDecode(c1.newBlocks.get(c1.cid)).data; // MST root after c1 = prevData for c2
  const c2Commit = cborDecode(c2.newBlocks.get(c2.cid));    // signed commit object of c2
  const op1 = { action: "create", path: `${COL}/${rkeyFor(1)}`, cid: await cidForRecord(rec("first")) };
  const op2 = { action: "create", path: `${COL}/${rkeyFor(2)}`, cid: await cidForRecord(rec("second")) };
  return { storage, c1, c2, c1Data, c2Commit, op1, op2 };
}

async function carFor(commit) {
  const blocks = new BlockMap();
  blocks.addMap(commit.newBlocks);
  blocks.addMap(commit.relevantBlocks);
  return blocksToCarFile(commit.cid, blocks);
}

// #commit payload from a CommitData + explicit ops/prevData/overrides.
async function commitPayload(commit, ops, prevData, overrides = {}) {
  return {
    seq: 100, rebase: false, tooBig: false, repo: REPO_DID,
    commit: commit.cid, rev: commit.rev, since: commit.since ?? null,
    blocks: await carFor(commit), ops, blobs: [], time: NOW,
    ...(prevData !== undefined ? { prevData } : {}),
    ...overrides,
  };
}

const cases = [];
const add = (c) => cases.push({ repoDid: REPO_DID, signingKey, ...c });

// A reusable valid setup commit (c1) + its frame, shared by the sequence cases.
async function setupFrame(pair) {
  const p = await commitPayload(pair.c1, [pair.op1], undefined); // c1 is first: no prevData
  return { role: "setup", hex: hexFrame(HEADER_COMMIT, cborEncode(p)) };
}

// ---- #sync event: valid ------------------------------------------------
{
  const pair = await buildPair();
  const blocks = new BlockMap();
  blocks.set(pair.c1.cid, pair.c1.newBlocks.get(pair.c1.cid)); // just the signed commit block
  const car = await blocksToCarFile(pair.c1.cid, blocks);
  const payload = { seq: 100, did: REPO_DID, rev: pair.c1.rev, blocks: car, time: NOW };
  add({
    id: "sync11/sync-event-valid", layer: "sync-event",
    title: "valid #sync event (CONTROL)",
    note: "sync-1.1's compact repo-state announcement; carries only the signed commit block.",
    expect: { strict: "accept", tolerant: "accept" },
    frames: [{ role: "test", hex: hexFrame(HEADER_SYNC, cborEncode(payload)) }],
  });
}

// ---- #sync event: wrong-key signature ----------------------------------
{
  const wrong = await Secp256k1Keypair.create();
  const storage = new MemoryBlockstore();
  const c1 = await Repo.formatInitCommit(storage, REPO_DID, wrong, [
    { action: WriteOpAction.Create, collection: COL, rkey: rkeyFor(1), record: rec("x") },
  ]);
  const blocks = new BlockMap();
  blocks.set(c1.cid, c1.newBlocks.get(c1.cid));
  const car = await blocksToCarFile(c1.cid, blocks);
  const payload = { seq: 100, did: REPO_DID, rev: c1.rev, blocks: car, time: NOW };
  add({
    id: "sync11/sync-event-bad-sig", layer: "sync-event",
    title: "#sync event signed by the wrong key",
    note: "Commit block in the #sync CAR does not verify against the repo identity.",
    expect: { strict: "reject", tolerant: "either" },
    frames: [{ role: "test", hex: hexFrame(HEADER_SYNC, cborEncode(payload)) }],
  });
}

// ---- second commit: valid (control) ------------------------------------
{
  const pair = await buildPair();
  const p = await commitPayload(pair.c2, [pair.op2], pair.c1Data); // correct prevData
  add({
    id: "sync11/commit2-valid", layer: "sync-commit2",
    title: "valid second commit with correct prevData (CONTROL)",
    note: "Setup commit establishes state; the test commit chains correctly.",
    expect: { strict: "accept", tolerant: "accept" },
    frames: [await setupFrame(pair), { role: "test", hex: hexFrame(HEADER_COMMIT, cborEncode(p)) }],
  });
}

// ---- second commit: missing prevData -----------------------------------
{
  const pair = await buildPair();
  const p = await commitPayload(pair.c2, [pair.op2], undefined); // prevData omitted
  add({
    id: "sync11/commit2-missing-prevdata", layer: "sync-commit2",
    title: "second commit omits prevData",
    note: "sync-1.1 requires prevData once prior state exists (enables op inversion).",
    expect: { strict: "reject", tolerant: "either" },
    frames: [await setupFrame(pair), { role: "test", hex: hexFrame(HEADER_COMMIT, cborEncode(p)) }],
  });
}

// ---- second commit: wrong prevData -------------------------------------
{
  const pair = await buildPair();
  const p = await commitPayload(pair.c2, [pair.op2], pair.c2.cid); // wrong CID (not the prev MST root)
  add({
    id: "sync11/commit2-wrong-prevdata", layer: "sync-commit2",
    title: "second commit with a mismatched prevData CID",
    note: "prevData present but points at the wrong root; inversion should not verify.",
    expect: { strict: "reject", tolerant: "either" },
    frames: [await setupFrame(pair), { role: "test", hex: hexFrame(HEADER_COMMIT, cborEncode(p)) }],
  });
}

// ---- second commit: rev rollback ---------------------------------------
{
  const pair = await buildPair();
  // Re-sign c2 with rev == c1.rev (not strictly greater than `since`) -> stale.
  const unsigned = {
    did: REPO_DID, version: 3, data: pair.c2Commit.data,
    rev: pair.c1.rev, prev: pair.c2Commit.prev ?? null,
  };
  const resigned = await signCommit(unsigned, keypair);
  const newCommitBytes = cborEncode(resigned);
  const newCid = await cidForRecord(resigned); // CID of the re-signed commit block
  // Rebuild c2's CAR with the re-signed commit block swapped in for the old one.
  const blocks = new BlockMap();
  blocks.addMap(pair.c2.newBlocks);
  blocks.addMap(pair.c2.relevantBlocks);
  blocks.delete?.(pair.c2.cid);
  const rebuilt = new BlockMap();
  for (const { cid, bytes } of blocks.entries()) {
    if (cid.equals(pair.c2.cid)) continue;
    rebuilt.set(cid, bytes);
  }
  rebuilt.set(newCid, newCommitBytes);
  const car = await blocksToCarFile(newCid, rebuilt);
  const p = {
    seq: 100, rebase: false, tooBig: false, repo: REPO_DID,
    commit: newCid, rev: pair.c1.rev, since: pair.c2.since ?? null,
    blocks: car, ops: [pair.op2], blobs: [], time: NOW, prevData: pair.c1Data,
  };
  add({
    id: "sync11/commit2-rev-rollback", layer: "sync-commit2",
    title: "second commit whose rev is not greater than the last seen",
    note: "rev == prior rev; sync-1.1 requires strictly increasing rev.",
    expect: { strict: "reject", tolerant: "either" },
    frames: [await setupFrame(pair), { role: "test", hex: hexFrame(HEADER_COMMIT, cborEncode(p)) }],
  });
}

// ---- rebase flag (deprecated in sync-1.1) ------------------------------
{
  const pair = await buildPair();
  const p = await commitPayload(pair.c1, [pair.op1], undefined, { rebase: true });
  add({
    id: "sync11/commit-rebase-flag", layer: "sync-commit2",
    title: "#commit with the deprecated rebase flag set",
    note: "sync-1.1 removed rebase; strict relays reject it.",
    expect: { strict: "reject", tolerant: "either" },
    frames: [{ role: "test", hex: hexFrame(HEADER_COMMIT, cborEncode(p)) }],
  });
}

writeFileSync(new URL("../corpus-sync.json", import.meta.url), JSON.stringify(cases, null, 2));
console.error(`wrote ${cases.length} sync-1.1 cases; signingKey=${signingKey}`);
