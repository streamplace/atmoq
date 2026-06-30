# atmoq: Streamplace's atproto-over-media-over-quic-transport implementation

An [atproto](https://atproto.com) relay that speaks [MoQ](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/)
to its subscribers, implementing the ideas in
[ATOM (draft-nandakumar-atproto-atom)](https://datatracker.ietf.org/doc/draft-nandakumar-atproto-atom/).

This is a **polyglot monorepo**: the reference relay/server is Rust, the
consumer client is Go, and a TypeScript client (browser + server) is included.
Each language keeps its own version and release cadence.

## Layout

```
atmoq/
├── rust/          Rust workspace: the `atmoq` binary (firehose bridge, server, relay)
│   ├── crates/       the atmoq crate
│   ├── vendor/       patched moq-net (see rust/Cargo.toml [patch.crates-io])
│   ├── Cargo.toml    workspace root
│   ├── release-plz.toml
│   └── Dockerfile     production image (ghcr.io/streamplace/atmoq)
├── go/            Go client: subscribe to an atmoq firehose over MoQ
│   ├── client.go     dial, subscribe, read frames
│   ├── varint.go     MoQ varint helpers
│   └── cmd/atmoq-firehose/  demo consumer CLI
├── ts/            TypeScript client: browser + server via WebTransport
│   ├── src/          transport (over @moq/net), frame decode, varint
│   └── test/         vitest unit tests
├── tests/e2e/     Dockerized differential harness (PLC + PDS + indigo oracle + MoQ leg)
├── docs/          specs, design notes, decision records, compatibility findings
└── justfile       task runner for all three languages
```

## Rust: the `atmoq` binary

Status: early prototype. One binary with a
[goat](https://github.com/bluesky-social/goat)-shaped CLI:

```
atmoq firehose                                  # tail the atproto firehose over MoQ
                                                 # (default: https://streamplace.network)
atmoq firehose --ops                            # ...as individual record operations
atmoq firehose --relay-host wss://bsky.network  # plain WS consumer, like goat firehose
atmoq serve                                     # host your own: WS ingest -> MoQ fanout,
                                                 # with a landing page (see docs/going-live.md)
atmoq relay --moq-host https://cdn.moq.dev/anon/<scope>    # bridge through a public MoQ relay
atmoq relay --moq-host https://relay.cloudflare.mediaoverquic.com \
            --dialect ietf-07 --broadcast <scope>          # ...including Cloudflare's (draft-07)
```

On Windows (and other hosts where a wildcard IPv6 socket can't reach IPv4),
add `--client-bind 0.0.0.0:0` if you see `sendmsg error ... 10049` /
`AddrNotAvailable`.

Frames are republished byte-for-byte — verified against the live Bluesky
firehose through both kixelated's public CDN (moq-lite) and Cloudflare's
public relay (draft-07 dialect). Both legs auto-reconnect on the lite path,
the upstream cursor persists via `--cursor-file`, and consumers survive
publisher restarts. See [docs/going-live.md](docs/going-live.md) for running
this as a service.

## Go: the consumer client

A Go client for the atproto firehose carried over MoQ transport — the
consumer side of `atmoq`. It speaks kixelated's **moq-lite** protocol
(draft 03/04) directly over raw QUIC.

```go
import "github.com/streamplace/atmoq/go"

sess, err := atmoq.Dial(ctx, "moqt://streamplace.network", nil)
if err != nil { /* ... */ }
defer sess.Close()

sub, err := sess.Subscribe(ctx, atmoq.DefaultBroadcast, atmoq.DefaultTrack)
if err != nil { /* ... */ }
defer sub.Close()

for {
    frame, group, err := sub.ReadFrame(ctx) // frame is raw at-sync message bytes
    if err != nil { break }
    _ = group
    // decode frame as { CBOR header, CBOR payload }
}
```

Consumer (subscribe) path only: connect, subscribe to a track, and read
frames from the live edge. No publishing, no ANNOUNCE-based discovery, no
cursor/replay. See [go/](go/) for the full README and API.

## TypeScript: the browser + server client

A TypeScript consumer that runs in both the browser (via native WebTransport)
and Node (via a WebTransport polyfill). The transport layer is delegated to
[`@moq/net`](https://www.npmjs.com/package/@moq/net); this package is a thin
domain layer that dials a relay, subscribes, and decodes each MoQ frame into an
at-sync message byte-identical to a `subscribeRepos` WebSocket message.

```typescript
import { connect } from "@streamplace/atmoq";

const sess = await connect("moqt://streamplace.network");
const sub = sess.subscribe(); // defaults: broadcast "atproto", track "atproto"

for await (const msg of sub) {
  console.log(msg.header.t, msg.payload.length); // "#commit", 1234
}
```

See [ts/](ts/) for the full README and API. Same consumer-only scope as Go.

## Releasing

Each language releases independently with distinct tag prefixes, so a fix in one
never forces a version bump in another:

| Language   | Tag format     | Mechanism                          | Install                                            |
|------------|----------------|------------------------------------|----------------------------------------------------|
| Rust       | `rust-vX.Y.Z`  | release-plz (rust/release-plz.toml) | prebuilt binaries on the GitHub release            |
| Go         | `go/vX.Y.Z`    | `just go-release X.Y.Z` (manual)   | `go get github.com/streamplace/atmoq/go@vX.Y.Z`    |
| TypeScript | `ts/vX.Y.Z`    | `just ts-release X.Y.Z` (manual)    | `npm install @streamplace/atmoq@X.Y.Z`             |

> **Note for Go consumers:** the module path changed from
> `github.com/streamplace/atmoq-go` to `github.com/streamplace/atmoq/go`
> when the client moved into this monorepo. Update your imports and
> `go get` the new path. The standalone `atmoq-go` repo is archived.

## Tasks

```
just test            # cargo test (rust/) + go test (go/) + vitest (ts/)
just rust-test       # cargo test + Dockerized e2e harness
just go-check        # go build + vet + test
just ts-check        # tsc --noEmit
just ts-test         # vitest
just live-relay      # wss://bsky.network -> cdn.moq.dev/anon/atmoq-demo
just live-tail       # cdn.moq.dev -> stdout, from anywhere
just live-relay-cf   # same, via Cloudflare's relay (draft-07)
just live-tail-cf
just install-hooks   # enable rustfmt + gofmt pre-commit hook
```

- [PLAN.md](PLAN.md) — implementation plan, milestones, open questions
- [docs/atom-spec-notes.md](docs/atom-spec-notes.md) — review of the ATOM draft against
  the atproto specs; intended deviations
- [docs/decisions/](docs/decisions/) — decision records (transport stack, etc.)
- [docs/diag/](docs/diag/) — public-relay compatibility findings
- [tests/e2e/](tests/e2e/) — Dockerized differential test harness
