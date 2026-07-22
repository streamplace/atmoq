// Merge per-relay harness outputs into one differential table.
//
//   node aggregate.mjs           # markdown table to stdout
//
// Reads corpus.json + results/<relay>.jsonl (one {"id","outcome",...} per
// line). Each relay's *decode verdict* (accept/reject) is the same axis; what
// a "reject" MEANS differs per relay and is annotated in POLICY below.
import { readFileSync, existsSync } from "node:fs";

const dir = new URL(".", import.meta.url).pathname;
// Optional args: <corpus.json> <result-suffix>. Defaults = the #account corpus.
//   node aggregate.mjs                              # corpus.json  -> results/<relay>.jsonl
//   node aggregate.mjs corpus-commit.json -commit   # -> results/<relay>-commit.jsonl
const corpusFile = process.argv[2] ?? "corpus.json";
const suffix = process.argv[3] ?? "";
const corpus = JSON.parse(readFileSync(dir + corpusFile));

// Order matters for the columns.
const RELAYS = ["atmoq", "indigo", "rsky", "hydrant", "zlay"];

// What a decode-level "reject" does to the upstream connection, and any
// caveats — sourced from each relay's ingest code (see README / findings doc).
const POLICY = {
  atmoq:
    "reject = drop this frame, connection stays up (ingest.rs). Validates " +
    "encoding (DRISL) only; no signature/MST/schema checks yet.",
  indigo:
    "reject at CBOR-decode = DROP THE WHOLE UPSTREAM CONNECTION and reconnect " +
    "(consumer.go -> slurper redialer). Repo/MST/sig failures instead drop one event.",
  rsky:
    "reject = drop this event, connection stays up (manager.rs). Header uses " +
    "lenient ciborium, body uses stricter dag-cbor. Default lenient mode still " +
    "publishes signature/MST failures.",
  hydrant:
    "reject at CBOR-decode = DROP THE WHOLE UPSTREAM CONNECTION and reconnect " +
    "(firehose.rs: break Err); unknown op/type instead skip one frame. Repo/sig/MST " +
    "failures drop one event. STRICTEST default: always resolves the signing key, so " +
    "sig-verify AND MST inversion run by default (validation.rs); it also decodes " +
    "record CBOR and uniquely enforces prevData correctness. verify_cids OFF by default.",
  zlay:
    "reject = drop this frame, connection stays up (subscriber.zig: return). Zig, on " +
    "the zat SDK. zat.cbor is a STRICT whole-frame DAG-CBOR decoder (rejects all DRISL " +
    "violations — even float64 entirely), so it matches atmoq on encoding. But the " +
    "commit/sync path is FAIL-OPEN: on cache-miss OR any sig/structure/MST failure it " +
    "FORWARDS the frame UNVALIDATED and re-resolves the key in the background — it never " +
    "drops a commit on bad crypto. Only #sync structural checks and the frame_worker " +
    "stale-rev check are hard drops; prevData/since are advisory (stat-only).",
};

const load = (relay) => {
  const p = `${dir}results/${relay}${suffix}.jsonl`;
  if (!existsSync(p)) return null;
  const m = new Map();
  for (const line of readFileSync(p, "utf8").split("\n").filter(Boolean)) {
    const o = JSON.parse(line);
    m.set(o.id, o);
  }
  return m;
};

const results = Object.fromEntries(RELAYS.map((r) => [r, load(r)]));

const mark = (o) => {
  if (!o) return "·";
  if (o.outcome === "accept") return "accept";
  if (o.outcome === "skip") return "skip";
  return "**reject**";
};

// --- table ---
const rows = [];
rows.push(`| Case | Layer | Expect (strict) | ${RELAYS.join(" | ")} |`);
rows.push(`|---|---|---|${RELAYS.map(() => "---").join("|")}|`);
for (const c of corpus) {
  const cells = RELAYS.map((r) => mark(results[r]?.get(c.id)));
  rows.push(
    `| \`${c.id}\` | ${c.layer} | ${c.expect.strict} | ${cells.join(" | ")} |`,
  );
}

console.log("## Decode verdict per relay\n");
console.log(rows.join("\n"));

console.log("\n## What `reject` means, per relay\n");
for (const r of RELAYS) console.log(`- **${r}** — ${POLICY[r]}`);

// --- agreement summary ---
console.log("\n## Disagreements (relays that differ on a case)\n");
let anyDisagree = false;
for (const c of corpus) {
  const verdicts = RELAYS.map((r) => results[r]?.get(c.id)?.outcome ?? null).filter(Boolean);
  const uniq = new Set(verdicts);
  if (uniq.size > 1) {
    anyDisagree = true;
    const detail = RELAYS.map((r) => {
      const o = results[r]?.get(c.id);
      return o ? `${r}=${o.outcome}` : `${r}=?`;
    }).join(", ");
    console.log(`- \`${c.id}\` (${c.title}): ${detail}`);
  }
}
if (!anyDisagree) console.log("_(none, or not all relay results present yet)_");
