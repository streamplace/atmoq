// Emit the malformed-frame corpus to JSON (id, metadata, hex-encoded bytes).
// Consumed by the per-relay decode harnesses (e.g. the Rust atmoq harness) and
// by the injection harness. Synthetic cases use a placeholder DID; the
// injection harness overrides ctx.did with a real captured one at run time.
import { writeFileSync } from "node:fs";
import { CASES } from "./cases.mjs";

const hex = (u8) => Buffer.from(u8).toString("hex");

const makeCtx = (did) => {
  let n = 100;
  return { did, seq: () => n++ };
};

const did = process.argv[2] ?? "did:plc:conformance000000000000000";
const out = CASES.map((c) => {
  const ctx = makeCtx(did);
  return {
    id: c.id,
    layer: c.layer,
    title: c.title,
    note: c.note ?? null,
    expect: c.expect,
    hex: hex(c.build(ctx)),
  };
});

const path = process.argv[3] ?? new URL("./corpus.json", import.meta.url).pathname;
writeFileSync(path, JSON.stringify(out, null, 2));
console.error(`wrote ${out.length} cases to ${path}`);
