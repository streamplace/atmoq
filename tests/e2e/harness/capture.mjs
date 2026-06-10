// Captures a firehose (com.atproto.sync.subscribeRepos) to JSONL on stdout.
// One line per frame: {"header": {...}, "payload": {...}}, with CIDs rendered
// as strings and byte strings replaced by their length (the `blocks` CAR data
// is bulky; structural comparison happens at the ops/rev level for now).
//
// Exits after `--idle-ms` (default 3000) with no new frames, or `--max-ms`.
// This is the capture half of the differential test: today it reads indigo's
// WS output; the same normalized form will be produced from atmoq's WS and
// MoQ outputs later.
import WebSocket from "ws";
import { normalizeFrame } from "./frames.mjs";

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => a.replace(/^--/, "").split("=")),
);
const url =
  args.url ??
  "ws://localhost:2470/xrpc/com.atproto.sync.subscribeRepos?cursor=0";
const idleMs = Number(args["idle-ms"] ?? 3000);
const maxMs = Number(args["max-ms"] ?? 30000);

const ws = new WebSocket(url);
let idleTimer;
const bump = () => {
  clearTimeout(idleTimer);
  idleTimer = setTimeout(() => process.exit(0), idleMs);
};
setTimeout(() => process.exit(0), maxMs);
ws.on("open", bump);
ws.on("error", (err) => {
  console.error(`websocket error: ${err.message}`);
  process.exit(1);
});
ws.on("message", (data) => {
  bump();
  console.log(JSON.stringify(normalizeFrame(new Uint8Array(data))));
});
