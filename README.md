# atmoq: Streamplace's atproto-over-media-over-quic-transport implementation

An [atproto](https://atproto.com) relay that speaks [MoQ](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/)
to its subscribers, implementing the ideas in
[ATOM (draft-nandakumar-atproto-atom)](https://datatracker.ietf.org/doc/draft-nandakumar-atproto-atom/).
Rust first; TypeScript (browser + server) to follow.

Status: early prototype. One binary, `atmoq`, with a
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

```
just live-relay      # wss://bsky.network -> cdn.moq.dev/anon/atmoq-demo
just live-tail       # cdn.moq.dev -> stdout, from anywhere
just live-relay-cf   # same, via Cloudflare's relay (draft-07)
just live-tail-cf
just test            # cargo test + Dockerized e2e (PLC + PDS + indigo oracle + MoQ leg)
```

- [PLAN.md](PLAN.md) — implementation plan, milestones, open questions
- [docs/atom-spec-notes.md](docs/atom-spec-notes.md) — review of the ATOM draft against
  the atproto specs; intended deviations
- [docs/decisions/](docs/decisions/) — decision records (transport stack, etc.)
- [docs/diag/](docs/diag/) — public-relay compatibility findings
- [tests/e2e/](tests/e2e/) — Dockerized differential test harness
