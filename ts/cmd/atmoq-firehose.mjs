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
//   --raw                print raw frame bytes as base64 (one per line)
//   --json               pretty-print decoded header + payload as JSON
//   --insecure           allow self-signed certs (Node polyfill only)
//   -h, --help           show this help

import { connect } from "@streamplace/atmoq";
import * as dagCbor from "@ipld/dag-cbor";

const args = process.argv.slice(2);

function parseFlags(args) {
  const flags = {
    broadcast: "atproto",
    track: "atproto",
    limit: 0,
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
  --raw                print raw frame bytes as base64
  --json               pretty-print decoded header + payload as JSON
  --insecure           allow self-signed certs (Node polyfill only)
  -h, --help           show this help

Examples:
  atmoq-firehose moqt://streamplace.network
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

  // For --insecure on Node, we need a WebTransport polyfill that can skip cert
  // verification. Browsers use serverCertificateHashes instead (see README).
  let transport;
  if (flags.insecure) {
    try {
      const { WebTransport } = await import("@fails-components/webtransport");
      const httpsUrl = target.replace(/^moq[tlps]?:\/\//, "https://");
      transport = new WebTransport(httpsUrl, { rejectUnauthorized: false });
    } catch {
      console.error(
        "atmoq-firehose: --insecure requires a WebTransport polyfill.\n" +
          "Install one:  npm install -D @fails-components/webtransport\n" +
          "(browsers use serverCertificateHashes — see ts/README.md)",
      );
      process.exit(1);
    }
  }

  console.error(`connecting to ${target}...`);
  const sess = await connect(target, { transport });
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
      const payloadSeq = peekPayloadSeq(msg.payload);

      if (flags.json) {
        console.log(
          JSON.stringify(
            {
              group: msg.group,
              frame: msg.frame,
              type: msg.header.t,
              header: msg.header,
              seq: payloadSeq,
              payloadBytes: msg.payload.length,
            },
            null,
            2,
          ),
        );
      } else {
        // Default: one compact JSON line per frame, like the Go CLI.
        console.log(
          JSON.stringify({
            group: msg.group,
            type: msg.header.t,
            seq: payloadSeq,
            bytes: msg.payload.length,
          }),
        );
      }

      count++;
      if (flags.limit > 0 && count >= flags.limit) break;
    }
  }

  sub.close();
  sess.close();
  console.error(`disconnected after ${count} frame(s)`);
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
