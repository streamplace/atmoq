# lastproto: Streamplace's atproto-over-media-over-quic-transport implementation

An [atproto](https://atproto.com) relay that speaks [MoQ](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/)
to its subscribers, implementing the ideas in
[ATOM (draft-nandakumar-atproto-atom)](https://datatracker.ietf.org/doc/draft-nandakumar-atproto-atom/).
Rust first; TypeScript (browser + server) to follow.

Status: early prototype. A passthrough relay (`relay`) ingests a
`com.atproto.sync.subscribeRepos` WebSocket firehose and republishes frames
byte-for-byte on a MoQ broadcast; `moq-tail` / `ws-tail` capture either side
for differential verification. Verified end-to-end against the live Bluesky
firehose through kixelated's public CDN:

```
just live-relay   # wss://bsky.network -> cdn.moq.dev/anon/lastproto-demo
just live-tail    # cdn.moq.dev -> stdout, from anywhere
just test         # cargo test + Dockerized e2e (PLC + PDS + indigo oracle + MoQ leg)
```

- [PLAN.md](PLAN.md) — implementation plan, milestones, open questions
- [docs/atom-spec-notes.md](docs/atom-spec-notes.md) — review of the ATOM draft against
  the atproto specs; intended deviations
- [docs/decisions/](docs/decisions/) — decision records (transport stack, etc.)
- [docs/diag/](docs/diag/) — public-relay compatibility findings
- [tests/e2e/](tests/e2e/) — Dockerized differential test harness
