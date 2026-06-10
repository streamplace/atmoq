// Compare two frame captures for passthrough equality.
//
//   node diff-frames.mjs <a.jsonl> <b.jsonl> [--min-overlap=N]
//
// Inputs are JSONL with a `raw` (base64) field per line (ws-tail/moq-tail
// format). The two captures may start/end at different stream positions;
// we align on the first frame of the shorter-suffix stream and require the
// overlapping region to match byte-for-byte, with at least N overlapping
// frames (default 1).
import fs from "node:fs";

const args = process.argv.slice(2).filter((a) => !a.startsWith("--"));
const opts = Object.fromEntries(
  process.argv
    .slice(2)
    .filter((a) => a.startsWith("--"))
    .map((a) => a.replace(/^--/, "").split("=")),
);
const minOverlap = Number(opts["min-overlap"] ?? 1);

const load = (p) =>
  fs
    .readFileSync(p, "utf8")
    .split("\n")
    .filter(Boolean)
    .map((l) => {
      const { raw } = JSON.parse(l);
      if (!raw) throw new Error(`${p}: line missing 'raw'`);
      return raw;
    });

const [aPath, bPath] = args;
const a = load(aPath);
const b = load(bPath);

// align: find the first frame of `b` within `a` (or vice versa)
const align = (xs, ys) => {
  const start = xs.indexOf(ys[0]);
  return start === -1 ? null : { xs, ys, start };
};
const m = align(a, b) ?? align(b, a);
if (!m) {
  console.error(`FAIL: no common frame between ${aPath} (${a.length}) and ${bPath} (${b.length})`);
  process.exit(1);
}

const overlap = Math.min(m.xs.length - m.start, m.ys.length);
if (overlap < minOverlap) {
  console.error(`FAIL: overlap ${overlap} < required ${minOverlap}`);
  process.exit(1);
}
for (let i = 0; i < overlap; i++) {
  if (m.xs[m.start + i] !== m.ys[i]) {
    console.error(`FAIL: frames diverge at overlap index ${i}`);
    process.exit(1);
  }
}
console.log(
  `PASS: ${overlap} overlapping frames byte-identical (${aPath}: ${a.length} frames, ${bPath}: ${b.length} frames)`,
);
