#!/usr/bin/env node
// Command atmoq-firehose is a minimal consumer that pulls the atproto firehose
// from a MoQ relay and prints one line per frame. It exercises the
// @streamplace/atmoq TypeScript client, independent of indigo/goat.
//
//	atmoq-firehose moqt://streamplace.network
//
// Usage:
//   atmoq-firehose [url] [flags]
//
// Flags:
//   --broadcast <name>   broadcast name (default: atproto)
//   --track <name>       track name (default: atproto)
//   --limit <n>          exit after N frames (0 = run forever)
//   --ops                print one line per record op in #commit (goat --ops)
//   --raw                one JSON line per frame with base64 raw bytes
//   --json               pretty-print decoded header + payload as JSON
//   --insecure           allow self-signed certs (Node polyfill only)
//   --cert-hash <hex>    pin the server cert by SHA-256 hash (dev TLS)
//   --idle-ms <n>        exit after N ms without a frame (--raw path)
//   -h, --help           show this help

import {
  connect,
  parseCarBlocks,
  InvalidDrislError,
  InvalidFrameError,
} from "@streamplace/atmoq";
import { decode } from "@atproto/lex-cbor";

// stdout is exclusively the frame/op output stream (JSONL, pipeable to jq);
// everything else goes to stderr. @moq/net logs its protocol chatter via
// console.debug/console.log, which Node sends to stdout — rebind those so
// library logging can't corrupt the output stream.
console.log = console.error.bind(console);
console.debug = console.error.bind(console);
console.info = console.error.bind(console);
const stdout = (line) => process.stdout.write(line + "\n");

const args = process.argv.slice(2);

function parseFlags(args) {
  const flags = {
    broadcast: "atproto",
    track: "atproto",
    limit: 0,
    ops: false,
    raw: false,
    json: false,
    insecure: false,
    certHash: "",
    idleMs: 0,
    help: false,
    positional: [],
  };

  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    switch (a) {
      case "--broadcast":
        flags.broadcast = args[++i];
        break;
      case "--track":
        flags.track = args[++i];
        break;
      case "--limit":
        flags.limit = parseInt(args[++i], 10);
        break;
      case "--ops":
        flags.ops = true;
        break;
      case "--raw":
        flags.raw = true;
        break;
      case "--json":
        flags.json = true;
        break;
      case "--insecure":
        flags.insecure = true;
        break;
      case "--cert-hash":
        flags.certHash = args[++i];
        break;
      case "--idle-ms":
        flags.idleMs = parseInt(args[++i], 10);
        break;
      case "--help":
      case "-h":
        flags.help = true;
        break;
      default:
        if (a.startsWith("--")) {
          console.error(`unknown flag: ${a}`);
          process.exit(2);
        }
        flags.positional.push(a);
        break;
    }
  }
  return flags;
}

const HELP = `atmoq-firehose — pull the atproto firehose over MoQ

Usage:
  atmoq-firehose [url] [flags]

Flags:
  --broadcast <name>   broadcast name (default: atproto)
  --track <name>       track name (default: atproto)
  --limit <n>          exit after N frames (0 = run forever)
  --ops                print one line per record op in #commit (goat --ops)
  --raw                one JSON line per frame with base64 raw bytes
  --json               pretty-print decoded header + payload as JSON
  --insecure           allow self-signed certs (Node polyfill only)
  --cert-hash <hex>    pin the server cert by SHA-256 hash (dev TLS)
  --idle-ms <n>        exit after N ms without a frame (--raw path; 0 = never)
  -h, --help           show this help

Examples:
  atmoq-firehose moqt://streamplace.network
  atmoq-firehose --ops --limit 20
  atmoq-firehose moqt://localhost:4443 --insecure --limit 10
  atmoq-firehose --json --limit 5
`;

async function main() {
  const flags = parseFlags(args);

  if (flags.help) {
    process.stdout.write(HELP);
    return;
  }

  const target = flags.positional[0] || "moqt://streamplace.network";

  console.error(`connecting to ${target}...`);
  const opts = { insecure: flags.insecure };
  if (flags.certHash) {
    // Pin the server cert by SHA-256 hash (hex) — the reliable dev-TLS path
    // on both Node and browsers; moq relays serve theirs at
    // http://host:port/certificate.sha256.
    const value = Uint8Array.from(
      flags.certHash.trim().match(/../g).map((b) => parseInt(b, 16)),
    );
    opts.certHashes = [{ algorithm: "sha-256", value }];
  }
  const sess = await connect(target, opts);
  console.error(`connected (version: ${sess.version})`);

  const sub = sess.subscribe(flags.broadcast, flags.track);
  console.error(
    `subscribed to broadcast=${flags.broadcast} track=${flags.track}`,
  );

  let count = 0;

  // Optional idle timeout (like the Rust CLI's --idle-ms): resolve undefined
  // if no frame arrives within the window, ending the capture cleanly.
  const withIdle = (promise) => {
    if (!flags.idleMs) return promise;
    let timer;
    return Promise.race([
      promise,
      new Promise((resolve) => {
        timer = setTimeout(() => resolve(undefined), flags.idleMs);
      }),
    ]).finally(() => clearTimeout(timer));
  };

  // --raw wants the original undecoded bytes; use readFrame() for that path.
  // Otherwise use the decoded async iterator.
  if (flags.raw) {
    for (;;) {
      const frame = await withIdle(sub.readFrame());
      if (!frame) break;
      // JSONL with a base64 `raw` field — the shape the e2e diff harness and
      // the Rust CLI's --raw output share.
      stdout(
        JSON.stringify({
          group: frame.group,
          raw: Buffer.from(frame.data).toString("base64"),
        }),
      );
      count++;
      if (flags.limit > 0 && count >= flags.limit) break;
    }
  } else {
    let rejected = 0;
    let done = false;
    while (!done) {
      try {
        for await (const msg of sub) {
          if (flags.ops) {
            // goat-style --ops: one line per record op in #commit, with the
            // record decoded from the message's CAR blocks.
            if (msg.header.t === "#commit") {
              const opsPrinted = printOps(msg.payload);
              count += opsPrinted;
            }
          } else if (flags.json) {
            stdout(
              JSON.stringify(
                {
                  group: msg.group,
                  frame: msg.frame,
                  type: msg.header.t,
                  header: cborToJson(msg.header),
                  seq: peekPayloadSeq(msg.payload),
                  payloadBytes: msg.payload.length,
                },
                null,
                2,
              ),
            );
            count++;
          } else {
            // Default: one compact JSON line per frame, like the Go CLI.
            stdout(
              JSON.stringify({
                group: msg.group,
                type: msg.header.t,
                seq: peekPayloadSeq(msg.payload),
                bytes: msg.payload.length,
              }),
            );
            count++;
          }

          if (flags.limit > 0 && count >= flags.limit) break;
        }
        done = true;
      } catch (err) {
        // atmoq is DRISL-strict: a frame that isn't valid DRISL is rejected.
        // The frame is already consumed and the subscription stays open, so we
        // log the rejection and keep reading from the next frame.
        if (
          err instanceof InvalidDrislError ||
          err instanceof InvalidFrameError
        ) {
          rejected++;
          console.error(`atmoq-firehose: rejected frame: ${err.message}`);
          continue;
        }
        throw err;
      }
    }
    if (rejected > 0) {
      console.error(`atmoq-firehose: rejected ${rejected} invalid frame(s)`);
    }
  }

  sub.close();
  sess.close();
  console.error(`disconnected after ${count} frame(s)/op(s)`);
}

// goat-style --ops: print one line per record operation in a #commit, with the
// record decoded from the message's CAR blocks. Ported from the Rust
// atmoq-cli's print_ops (main.rs). Returns the number of ops printed.
function printOps(payload) {
  let commit;
  try {
    commit = decode(payload);
  } catch {
    console.error("warning: failed to decode commit payload");
    return 0;
  }

  const blocksBytes = commit.blocks;
  const ops = commit.ops;
  if (!blocksBytes || !ops) {
    console.error("warning: commit missing blocks or ops");
    return 0;
  }

  // Parse the CAR blocks (CID hex → block bytes).
  let blocks;
  try {
    blocks = parseCarBlocks(blocksBytes);
  } catch (e) {
    console.error(`warning: failed to parse CAR blocks: ${e.message}`);
    return 0;
  }

  const repo = commit.repo;
  const rev = commit.rev;
  const time = commit.time;
  const seq = commit.seq;

  let printed = 0;
  for (const op of ops) {
    // @atproto/lex-cbor decodes CBOR tag 42 (CID) to a Cid object with a
    // .toString() that yields the canonical base32 form (bafyrei...).
    const cid = op.cid;
    let record = null;
    if (cid) {
      // The CAR blocks are keyed by hex string of the raw CID bytes.
      // @atproto/lex-cbor's Cid has a .bytes property (the raw CIDv1 bytes).
      const cidBytes = cid.bytes ?? (cid instanceof Uint8Array ? cid : null);
      if (cidBytes) {
        // Strip the 0x00 multibase-identity prefix if present.
        const rawCid = cidBytes[0] === 0 ? cidBytes.subarray(1) : cidBytes;
        const hexKey = Buffer.from(rawCid).toString("hex");
        const blockBytes = blocks.get(hexKey);
        if (blockBytes) {
          try {
            record = cborToJson(decode(blockBytes));
          } catch {
            // leave record null
          }
        }
      }
    }

    stdout(
      JSON.stringify({
        action: op.action,
        path: op.path,
        cid: cidToString(cid),
        record,
        seq,
        repo,
        rev,
        time,
      }),
    );
    printed++;
  }
  return printed;
}

// Render a Cid (from @atproto/lex-cbor tag 42 decode) as a string.
// @atproto/lex-cbor decodes tag 42 to a Cid object from @atproto/lex-data,
// which has a .toString() yielding the canonical base32 form (bafyrei...).
function cidToString(cid) {
  if (!cid) return null;
  if (typeof cid.toString === "function") {
    const s = cid.toString();
    // Cid.toString() returns the canonical CID string.
    if (s && s !== "[object Object]") return s;
  }
  return null;
}

// Render a decoded CBOR value as JSON, converting CIDs to strings and byte
// arrays to { $bytesLength: n } (matching the Rust CLI's cbor_to_json).
function cborToJson(v) {
  if (v === null || v === undefined) return null;

  // Cid objects (from @atproto/lex-cbor tag 42 decode) have a .toString()
  // that yields the canonical base32 CID string.
  if (typeof v === "object" && typeof v.toString === "function") {
    const s = v.toString();
    if (s && s.startsWith("bafy") || (s && s.startsWith("bafk"))) {
      return s;
    }
  }

  if (v instanceof Uint8Array) {
    return { $bytesLength: v.length };
  }
  if (Array.isArray(v)) {
    return v.map(cborToJson);
  }
  if (typeof v === "object") {
    const out = {};
    for (const [k, val] of Object.entries(v)) {
      out[k] = cborToJson(val);
    }
    return out;
  }
  // BigInts that exceed Number.MAX_SAFE_INTEGER stay as strings.
  if (typeof v === "bigint") {
    return v > Number.MAX_SAFE_INTEGER || v < -Number.MAX_SAFE_INTEGER
      ? v.toString()
      : Number(v);
  }
  return v;
}

// Best-effort decode of the `seq` field from the payload CBOR. The payload is
// the second object in an at-sync frame; for most message types it has a
// top-level `seq` field. If the payload isn't decodable or has no seq, returns
// null. This is purely for human-friendly output — consumers should decode the
// payload themselves per the atproto spec.
function peekPayloadSeq(payload) {
  try {
    const decoded = decode(payload);
    if (decoded && typeof decoded === "object" && "seq" in decoded) {
      return decoded.seq;
    }
    return null;
  } catch {
    return null;
  }
}

main().catch((err) => {
  console.error(`atmoq-firehose: ${err.message || err}`);
  process.exit(1);
});
