# @streamplace/atmoq

A TypeScript client for the [atproto](https://atproto.com) firehose carried over
[MoQ](https://moq.dev) transport — the browser + server consumer side of
[atmoq](https://github.com/streamplace/atmoq).

It speaks kixelated's **moq-lite** protocol over [WebTransport](https://developer.mozilla.org/en-US/docs/Web/API/WebTransport)
— no hand-rolled QUIC. The transport layer is [kixelated's `@moq/net`](https://www.npmjs.com/package/@moq/net)
(MIT/Apache-2.0); this package is a thin domain layer that dials a relay,
subscribes to the atproto broadcast, and decodes each MoQ frame into an at-sync
message byte-identical to a `com.atproto.sync.subscribeRepos` WebSocket message.

Runs in **both** the browser (native WebTransport) and Node (via a WebTransport
polyfill, passed to `connect()`).

## Install

```sh
npm install @streamplace/atmoq
```

## Usage

```typescript
import { connect } from "@streamplace/atmoq";

const sess = await connect("moqt://streamplace.network");
const sub = sess.subscribe(); // defaults: broadcast "atproto", track "atproto"

for await (const msg of sub) {
  // msg.header.t   — the message type ("#commit", "#identity", "#account", ...)
  // msg.payload    — raw CBOR payload bytes (same as a subscribeRepos message)
  // msg.group      — MoQ group sequence number
  console.log(msg.header.t, msg.payload.length, "group=", msg.group);
}
```

### Low-level frame access (no decode)

```typescript
const sub = sess.subscribe();
const { data, group } = await sub.readFrame();
// `data` is the raw at-sync message bytes (CBOR header + CBOR payload)
```

### Insecure dev relays (self-signed certs)

The browser `WebTransport` API has no global "skip verification" flag. For dev
relays, construct a `WebTransport` with `serverCertificateHashes` and pass it
via `connect()`'s `transport` option:

```typescript
const wt = new WebTransport("https://localhost:4443", {
  serverCertificateHashes: [{ algorithm: "sha-256", value: hashBytes }],
});
const sess = await connect("moqt://localhost:4443", { transport: wt });
```

On Node, pass a polyfill transport configured with `rejectUnauthorized: false`.

## API

### `connect(url, opts?) → Promise<Session>`

Establish a MoQ session. `url` accepts `moqt://`, `moql://`, `moq://`,
`moqs://`, or bare `host[:port]` (default port 443).

### `Session`

- `subscribe(broadcast?, track?) → Subscription` — subscribe to a track (defaults: `"atproto"`/`"atproto"`)
- `version: string` — the negotiated moq-lite/moq-transport ALPN
- `close()` — tear down the session
- `closed: Promise<void>` — resolves when the session ends

### `Subscription`

- `readFrame() → Promise<{ data, group, frame } | undefined>` — raw frame bytes
- `readMessage() → Promise<AtSyncMessage | undefined>` — decoded at-sync message
- `[Symbol.asyncIterator]()` — `for await` loop over decoded messages
- `close()` — end the subscription

### `AtSyncMessage`

- `header: { t: string, ... }` — decoded DAG-CBOR header object
- `payload: Uint8Array` — raw payload bytes (type-specific)
- `group: number` — MoQ group sequence
- `frame: number` — frame sequence within the group

## Scope

Consumer (subscribe) path only: connect, subscribe to a track, and read frames
from the live edge. No publishing, no ANNOUNCE-based discovery, no cursor/replay
(subscriptions start at the publisher's latest group). Mirrors the scope of the
[Go client](../go).

## Transport dependency

[`@moq/net`](https://www.npmjs.com/package/@moq/net) is pinned at `0.1.6`
(pre-1.0). It handles WebTransport, the moq-lite wire protocol, and stream
multiplexing. We track upstream and pin exact versions; bump deliberately.
