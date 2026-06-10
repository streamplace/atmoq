// Verifies a firehose capture (JSONL from capture.mjs) against the driver's
// expectations (JSON from driver.mjs).
//
//   node verify.mjs <driver.json> <capture.jsonl>
//
// Asserts, for the driven account:
//   - an #identity and an #account (active=true) event exist
//   - the concatenated #commit ops match expectedOps exactly, in order
//   - seq is strictly increasing across all frames
//   - rev is strictly increasing across the account's commits
//   - every commit declares prevData and ops-array invariants from at-sync §4.4.2
import fs from "node:fs";

const [driverPath, capturePath] = process.argv.slice(2);
const driver = JSON.parse(fs.readFileSync(driverPath, "utf8"));
const frames = fs
  .readFileSync(capturePath, "utf8")
  .split("\n")
  .filter(Boolean)
  .map((l) => JSON.parse(l));

const failures = [];
const check = (cond, msg) => {
  if (!cond) failures.push(msg);
};

check(frames.length > 0, "capture is empty");

// seq strictly increasing across the whole stream
let lastSeq = 0;
for (const f of frames) {
  const seq = f.payload?.seq;
  if (seq === undefined) continue;
  check(seq > lastSeq, `seq not strictly increasing: ${lastSeq} -> ${seq}`);
  lastSeq = seq;
}

const ofType = (t) => frames.filter((f) => f.header.t === t);
const forDid = (fs_, did) =>
  fs_.filter((f) => (f.payload.did ?? f.payload.repo) === did);

const identities = forDid(ofType("#identity"), driver.did);
check(identities.length >= 1, `no #identity event for ${driver.did}`);

const accounts = forDid(ofType("#account"), driver.did);
check(accounts.length >= 1, `no #account event for ${driver.did}`);
check(
  accounts.some((f) => f.payload.active === true),
  `no active=true #account event for ${driver.did}`,
);

const commits = forDid(ofType("#commit"), driver.did);
check(commits.length > 0, `no #commit events for ${driver.did}`);

// rev strictly increasing, sync-v1.1 field invariants
let lastRev = "";
for (const c of commits) {
  const p = c.payload;
  check(typeof p.rev === "string" && p.rev > lastRev, `rev not increasing: ${lastRev} -> ${p.rev}`);
  lastRev = p.rev;
  // prevData is required except on a repo's first commit (since == null)
  if (p.since != null) {
    check(p.prevData !== undefined, `commit seq=${p.seq} missing prevData`);
  }
  check(p.tooBig === false, `commit seq=${p.seq} has tooBig=${p.tooBig}`);
  check(Array.isArray(p.ops) && p.ops.length <= 200, `commit seq=${p.seq} bad ops array`);
}

// ops, concatenated across commits in order, match the driver's expectations
const gotOps = commits.flatMap((c) =>
  c.payload.ops.map(({ action, path }) => ({ action, path })),
);
const want = driver.expectedOps;
check(
  JSON.stringify(gotOps) === JSON.stringify(want),
  `ops mismatch:\n  want ${JSON.stringify(want)}\n  got  ${JSON.stringify(gotOps)}`,
);

if (failures.length) {
  console.error(`FAIL (${failures.length}):`);
  for (const f of failures) console.error(`  - ${f}`);
  process.exit(1);
}
console.log(
  `PASS: ${frames.length} frames (${commits.length} commits, ${identities.length} identity, ${accounts.length} account) for ${driver.did}`,
);
