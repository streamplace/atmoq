# lastproto: Streamplace's atproto-over-media-over-quic-transport implementation

An [atproto](https://atproto.com) relay that speaks [MoQ](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/)
to its subscribers, implementing the ideas in
[ATOM (draft-nandakumar-atproto-atom)](https://datatracker.ietf.org/doc/draft-nandakumar-atproto-atom/).
Rust first; TypeScript (browser + server) to follow.

Status: planning.

- [PLAN.md](PLAN.md) — implementation plan, milestones, open questions
- [docs/atom-spec-notes.md](docs/atom-spec-notes.md) — review of the ATOM draft against
  the atproto specs; intended deviations
