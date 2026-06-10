// Normalize a base64-raw-frame capture (ws-tail / moq-tail JSONL with a
// `raw` field) into the same normalized JSONL that capture.mjs emits.
//
//   node normalize.mjs < tail.jsonl > normalized.jsonl
import readline from "node:readline";
import { normalizeFrame } from "./frames.mjs";

const rl = readline.createInterface({ input: process.stdin });
for await (const line of rl) {
  if (!line.trim()) continue;
  const { raw } = JSON.parse(line);
  if (!raw) throw new Error("input line has no 'raw' field");
  console.log(JSON.stringify(normalizeFrame(Buffer.from(raw, "base64"))));
}
