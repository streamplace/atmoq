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
//   --raw                print raw frame bytes as base64 (one per line)
//   --json               pretty-print decoded header + payload as JSON
//   --insecure           allow self-signed certs (Node polyfill only)
//   -h, --help           show this help

import { connect, parseCarBlocks } from "@streamplace/atmoq";
import * as dagCbor from "@ipld/dag-cbor";

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
  --raw                print raw frame bytes as base64
  --json               pretty-print decoded header + payload as JSON
  --insecure           allow self-signed certs (Node polyfill only)
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
  const sess = await connect(target, { insecure: flags.insecure });
  console.error(`connected (version: ${sess.version})`);

  const sub = sess.subscribe(flags.broadcast, flags.track);
  console.error(
    `subscribed to broadcast=${flags.broadcast} track=${flags.track}`,
  );

  let count = 0;

  // --raw wants the original undecoded bytes; use readFrame() for that path.
  // Otherwise use the decoded async iterator.
  if (flags.raw) {
    for (;;) {
      const frame = await sub.readFrame();
      if (!frame) break;
      console.log(Buffer.from(frame.data).toString("base64"));
      count++;
      if (flags.limit > 0 && count >= flags.limit) break;
    }
  } else {
    for await (const msg of sub) {
      if (flags.ops) {
        // goat-style --ops: one line per record op in #commit, with the
        // record decoded from the message's CAR blocks.
        if (msg.header.t === "#commit") {
          const opsPrinted = printOps(msg.payload);
          count += opsPrinted;
        }
      } else if (flags.json) {
        console.log(
          JSON.stringify(
            {
              group: msg.group,
              frame: msg.frame,
              type: msg.header.t,
              header: msg.header,
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
        console.log(
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
    commit = dagCbor.decode(payload);
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

  // Parse the CAR blocks (CID → block bytes).
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
    // The CID is a CBOR tag 42 (byte string). @ipld/dag-cbor decodes tag 42
    // to a CID object (multiformats CID), which has a .bytes property.
    let record = null;
    const cid = op.cid;
    if (cid) {
      // CID objects from @ipld/dag-cbor have a .bytes property (the raw CID
      // bytes). We look up the block by these bytes in the CAR map.
      // The CAR blocks are keyed by hex string (Uint8Array keys don't work
      // with Map — reference equality, not value equality).
      const cidKey = cid.bytes ?? (cid instanceof Uint8Array ? cid : null);
      if (cidKey) {
        // Strip the 0x00 multibase-identity prefix if present, then hex-encode.
        const rawCid = cidKey[0] === 0 ? cidKey.subarray(1) : cidKey;
        const hexKey = Buffer.from(rawCid).toString("hex");
        const blockBytes = blocks.get(hexKey);
        if (blockBytes) {
          try {
            record = cborToJson(dagCbor.decode(blockBytes));
          } catch {
            // leave record null
          }
        }
      }
    }

    console.log(
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

// Convert a CID (from @ipld/dag-cbor tag 42 decode) to a string.
// @ipld/dag-cbor decodes tag 42 to a multiformats CID object, which has
// a .toString() that yields the canonical base32 form (bafy...).
function cidToString(cid) {
  if (!cid) return null;
  if (typeof cid.toString === "function") {
    const s = cid.toString();
    if (s !== "[object Object]") return s;
  }
  // Fallback: if it's raw bytes, encode as base32 (at-repo appendix A.1).
  if (cid.bytes instanceof Uint8Array) {
    const raw = cid.bytes[0] === 0 ? cid.bytes.subarray(1) : cid.bytes;
    return "b" + base32Nopad(raw).toLowerCase();
  }
  return null;
}

// Base32 (no padding) encoder, matching the Rust implementation's
// data_encoding::BASE32_NOPAD.
function base32Nopad(bytes) {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
  let bits = 0;
  let value = 0;
  let output = "";
  for (const byte of bytes) {
    value = (value << 8) | byte;
    bits += 8;
    while (bits >= 5) {
      output += alphabet[(value >>> (bits - 5)) & 31];
      bits -= 5;
    }
  }
  if (bits > 0) {
    output += alphabet[(value << (5 - bits)) & 31];
  }
  return output;
}

// Render a decoded CBOR value as JSON, converting CIDs to strings and byte
// arrays to { $bytesLength: n } (matching the Rust CLI's cbor_to_json).
function cborToJson(v) {
  // CID objects (from dag-cbor tag 42) have a .code and .bytes property.
  if (v && typeof v === "object" && !(v instanceof Uint8Array) && !Array.isArray(v)) {
    // Check if it's a CID (multiformats CID has .code and .version)
    if ("code" in v && "version" in v && "bytes" in v) {
      return cidToString(v);
    }
    // Regular object — recurse over entries.
    const out = {};
    for (const [k, val] of Object.entries(v)) {
      out[k] = cborToJson(val);
    }
    return out;
  }
  if (v instanceof Uint8Array) {
    return { $bytesLength: v.length };
  }
  if (Array.isArray(v)) {
    return v.map(cborToJson);
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
    const decoded = dagCbor.decode(payload);
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
