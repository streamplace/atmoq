# atmoq implementation plan

An atproto relay over MoQ transport, implementing the ideas in
[ATOM (draft-nandakumar-atproto-atom-00)](https://www.ietf.org/archive/id/draft-nandakumar-atproto-atom-00.txt),
in Rust, with a TypeScript implementation (browser + server) to follow.

Status: **prototype** — the `atmoq` CLI bridges a WS firehose onto MoQ byte-exactly
(see README); validation/sequencing milestones below are still ahead. See
[docs/atom-spec-notes.md](docs/atom-spec-notes.md) for a detailed review of the ATOM
draft against the atproto specs, including the places where we intend to deviate.

## 1. Source documents

| Document | Role |
|---|---|
| [draft-nandakumar-atproto-atom-00](https://www.ietf.org/archive/id/draft-nandakumar-atproto-atom-00.txt) | The MoQ mapping we're implementing. Written by MoQ experts, light on atproto details; we follow its broad contours and fix the minutiae. |
| [draft-holmgren-at-repository-02](https://www.ietf.org/archive/id/draft-holmgren-at-repository-02.txt) (2026-06-04) | Authoritative: repo format v3, MST, deterministic CBOR, CAR serialization, TIDs, signatures. |
| [draft-holmgren-at-synchronization-00](https://www.ietf.org/archive/id/draft-holmgren-at-synchronization-00.txt) (2026-06-04) | Authoritative: the firehose. Message types (`#commit`/`#sync`/`#account`/`#identity`), frame format, cursor semantics, **commit validation (§4.5)** and operation inversion (§4.1.2), re-synchronization. This is the spec for "what a relay does"; ATOM only changes "how the bytes move". Split out of the repository draft days ago — ATOM-00 predates it and cites the older combined doc. |
| [draft-newbold-at-architecture-00](https://www.ietf.org/archive/id/draft-newbold-at-architecture-00.txt) | Informational: network roles, DIDs/handles, relay responsibilities. |
| [draft-ietf-moq-transport](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/) | MOQT itself. ATOM is written against -16 semantics (PUBLISH_NAMESPACE / SUBSCRIBE_NAMESPACE / FETCH / subgroups / extension headers); the draft is at -17 as of March 2026. **Decided 2026-06-10: we build on [moq-lite](https://moq.dev/) instead, translating ATOM's concepts — see [decision 0001](docs/decisions/0001-transport-stack.md).** |

Reference implementation we're replacing: **indigo's relay** (`~/code/indigo/cmd/relay`,
~8k LOC Go). Its pipeline is the behavioral spec for everything transport-independent:
slurper (upstream WS subscriptions, per-host cursors, rate limits) → ingest/verify
(at-sync §4.5 validation: signature, rev ordering, prevData chaining, MST operation
inversion) → sequencer + 72h disk event log → fan-out. Key files:
`relay/slurper.go`, `relay/ingest.go`, `relay/verify.go`, `relay/broadcast.go`,
`stream/persist/diskpersist/diskpersist.go`, and a reusable test harness in
`cmd/relay/testing/` (fake-PDS producer, JSON scenario runner).

## 2. What we're building

A relay in the at-synchronization sense — subscribes to PDS hosts, validates everything,
re-broadcasts an aggregated totally-ordered firehose — where the *downstream* side is
MOQT instead of WebSocket, and (eventually) the repo-fetch path is MOQT instead of HTTP
CAR exports.

Crucially, **no PDS speaks MoQ today and none will for a while**. ATOM describes PDSes
publishing `at/firehose/{host}/...` namespaces directly; in reality our relay is the
bridge:

```
                          ┌──────────────────────── atmoq relay ───────────────────────┐
 PDS A ──WS firehose──▶   │ ingest (WS client)                                              │
 PDS B ──WS firehose──▶   │   → validate (at-sync §4.5: sig, rev, prevData, op inversion)   │
 PDS C ──WS firehose──▶   │   → sequence (monotonic seq, group-aligned disk log)            │
                          │   → publish:                                                    │
                          │       • MoQ firehose tracks (ATOM §4.2.1 concepts on moq-lite)  │
                          │       • legacy WS subscribeRepos (compat, drop-in for indigo)   │
                          │       • XRPC ops endpoints (listHosts, requestCrawl, ...)       │
                          └─────────────────────────────────────────────────────────────────┘
                                          │                          │
                                MoQ subscribers              existing AppViews etc.
                          (incl. downstream MoQ relays)        (unchanged, WS)
```

Keeping the legacy WS output is deliberate: it makes atmoq a drop-in indigo
replacement, gives the ecosystem a migration path, and — most importantly for us — makes
differential testing trivial (same events out both pipes, byte-comparable against
indigo). The validation/sequencing core is transport-agnostic; transports are adapters.

### Non-goals

- Not a PDS, AppView, labeler, or PLC server.
- No label streams (`com.atproto.label.subscribeLabels`) initially.
- No archival storage beyond the replay window (indigo parity: ~72h, configurable).
- Not trying to upstream protocol changes through the IETF ourselves — but we should
  file issues against the ATOM draft as we hit problems (the authors asked for exactly
  this kind of implementation experience; see spec notes doc).

## 3. Architecture

### 3.1 Workspace layout (proposed)

```
atmoq/
├── crates/
│   ├── atmoq-repo      # atproto data layer: deterministic CBOR, CID, TID, NSID,
│   │                       # CAR read/write, MST + operation inversion, commit sig verify.
│   │                       # (Or a thin wrapper over an existing crate — see §6 Q5.)
│   ├── atmoq-sync      # at-sync semantics: message types, frame codec, validation
│   │                       # state machine (§4.5), account/host status model. Pure logic,
│   │                       # no I/O — this is the part the TS impl will mirror.
│   ├── atmoq-atom      # ATOM mapping: track/namespace naming, group/object layout,
│   │                       # extension headers (at-seq, at-event-type, ...), priorities,
│   │                       # cursor⇄(group,object) index. Transport-agnostic data plane.
│   ├── atmoq     # The binary: WS ingest (slurper), identity resolution +
│   │                       # caching, sequencer, disk event log, MOQT publisher, legacy
│   │                       # WS server, XRPC + admin HTTP API, metrics.
│   └── atmoq-client    # Consumer library: subscribe via MOQT, gap-detect, FETCH
│                           # recovery, re-materialize the standard event stream. Used by
│                           # the e2e tests; the seed of the future TS client API.
├── docs/
└── tests/e2e/              # dev-env + indigo differential harness (see §5)
```

### 3.2 Data plane decisions (ATOM §4, concretized)

- **Track layout (MVP)**: aggregated relay namespace `at/firehose/{relay-host}` with the
  four event-type tracks, track name `all`. All four tracks share the **single relay
  sequence space**; every object carries `at-seq`. Consumers needing total order (which
  is most of them — an `#account` takedown must gate later `#commit`s) merge by
  `at-seq`. We will additionally publish a single combined-order `firehose` track so
  simple consumers don't have to merge; split tracks are the optimization, not the
  source of truth. Per-DID track names and per-PDS namespaces: deferred (see spec
  notes §3).
- **Object payload**: exactly the deterministic-CBOR payload object of the
  at-sync message (no header object — `t` moves to the `at-event-type` extension
  header). This keeps payloads byte-identical to what WS consumers see after the header,
  and keeps validation code shared between both outputs.
- **Groups**: fixed event-count groups (start with 1000/group, configurable). Because
  at-sync §4.3 explicitly permits seq gaps, ATOM's arithmetic cursor mapping is
  unsound; objects are packed densely and `at-seq` (carried in the payload) is
  authoritative. Group boundaries align with disk-log segment files.
- **Recovery (no FETCH — decision 0001)**: MoQ subscribers join at the live edge; a
  consumer that detects it missed events (seq/rev discontinuity per account) re-syncs
  affected accounts from the PDS fleet per at-sync §4.6 — the path every at-sync
  consumer needs anyway. Full cursor replay remains available on the legacy WS output.
  Whether the MoQ side also exposes a replay window (group history) is a tuning
  question, not a correctness requirement.
- **Repo sync tracks (ATOM §4.2.2) and blob tracks (§4.2.3)**: phase 2. The subgroup
  scheme as drafted doesn't work (MOQT has no subgroup filtering in SUBSCRIBE) and blob
  group-IDs truncate CIDs to 8 bytes; both need redesign. Details in spec notes §4–5.
- **Priorities**: adopt ATOM Table 2 as defaults. Note priorities affect delivery order
  under congestion only; correctness never depends on them (consumers re-order by
  `at-seq`).

### 3.3 Control plane / operations

Parity with indigo where it's ecosystem-facing:

- XRPC: `com.atproto.sync.subscribeRepos` (legacy WS out), `listHosts`,
  `getHostStatus`, `requestCrawl`, `listRepos`, `getRepoStatus`, `getRepo`
  (redirect to PDS), `getLatestCommit`.
- Admin API (basic-auth): takedown/reverse, domain bans, host limits, rate limits.
- Host lifecycle: statuses (active/idle/offline/throttled/banned), per-host cursor
  persistence (~4s flush like indigo), health checks, new-host-per-day limits,
  baseline vs trusted quotas.
- Account state: local status overriding upstream status (at-sync §2.2 hop-by-hop
  semantics), LRU account cache.
- Validation: strict mode per at-sync §4.5 (steps 1–6, including op inversion and the
  key-rotation retry on signature failure), plus a `--lenient` mode like indigo's for
  transitional PDSes.
- Storage: SQLite for host/account/repo-state tables (Postgres later if needed);
  append-only segment files for the event log, group-aligned.

### 3.4 TypeScript implementation (later, but shapes today's decisions)

- Keep `atmoq-sync` and `atmoq-atom` free of I/O and OS dependencies so their
  logic ports cleanly; encode all of their behavior in **language-agnostic test vectors**
  (JSON/CBOR fixture files in this repo) that both implementations must pass.
- The TS package targets one API across browser (WebTransport) and server (raw QUIC or
  WebTransport). This constrains the MoQ library choice (§6 Q1) — whatever we pick must
  have a credible browser story.
- wasm-bindgen of the Rust core into the TS package is a fallback option, not the plan
  of record (browser bundle size, and we want an idiomatic TS consumer API).

## 4. Milestones

**M0 — Spikes & decisions (timeboxed).** Transport stack is decided
([moq-lite, decision 0001](docs/decisions/0001-transport-stack.md)); remaining spikes:
(a) moq toy / proto-diag — publish a stream of sequenced CBOR frames using kixelated's
libraries through **Cloudflare's public relay** (and moq.dev's, to compare), subscribe
from native + browser, confirm where per-event metadata should live and what each
relay's dialect tolerates; (b) evaluate atproto crates (§6 Q5) by running their
MST/CAR code against the interop test vectors. Outcomes recorded in `docs/decisions/`.

**M1 — atproto data layer.** `atmoq-repo` (build or wrap): deterministic CBOR
encode/verify, CID, TID, CAR streaming reader/writer, MST construction + **operation
inversion**, commit signature verify (p256 + k256, low-S). Validated against
[bluesky-social/atproto-interop-tests](https://github.com/bluesky-social/atproto-interop-tests)
and CARs exported from the dev-env PDS.

**M2 — Ingest + validation + sequencing.** The transport-independent relay core:
WS slurper (multi-host, cursors, reconnect, rate limits), identity resolution/cache
(did:plc via PLC directory, did:web), at-sync §4.5 pipeline, account/host state in
SQLite, sequencer + group-aligned disk log with replay. Exit criterion: can shadow a
real PDS (or dev-env PDS) and validate everything indigo validates, with a scenario-test
suite ported from `indigo/cmd/relay/testing/`.

**M3 — Outputs.** (a) Legacy WS `subscribeRepos` including cursor backfill semantics —
at this point atmoq is a usable indigo replacement; (b) MoQ publisher: broadcast
announce, four event tracks + combined track, group rotation; (c) `atmoq-client`
consumer able to reconstruct the identical event stream from MoQ (gap detection +
per-account PDS re-sync per decision 0001); (d) `diag` mode: the same publisher/
consumer pair pointed at third-party public relays (Cloudflare first, then moq.dev),
verifying end-to-end behavior over infrastructure we don't operate and mapping
per-relay dialect differences.

**M4 — E2E + differential testing.** See §5. CI-gated.

**M5 — Ops hardening.** XRPC + admin endpoints, metrics (Prometheus), structured
logging, config, deploy story (single static binary + container), soak test against the
live network (subscribe to a handful of real PDSes; later, a full-network crawl).

**M6 — Phase 2 protocol surface.** Repo-sync tracks (redesigned per spec notes §4 —
this is the genuinely novel/exciting part: verifiable per-record sync with MST proof
paths over FETCH), blob tracks (redesigned per spec notes §5), MoQ relay-to-relay
fan-out (a downstream atmoq subscribing to an upstream one over MOQT instead of WS).

**M7 — TypeScript client.** Port `atmoq-sync`/`atmoq-atom` logic against the
shared test vectors; browser + server transport adapters; consumer API mirroring
`atmoq-client`.

## 5. End-to-end testing strategy

Components on hand:

- **`~/streamplace/js/dev-env`** (atcute-derived, MIT): spins up an in-memory PLC +
  PDS (`@atproto/pds` ^0.4.214) with no Docker, auto-ports, temp dirs. No seed helpers —
  we drive writes via plain XRPC (`com.atproto.server.createAccount`, `applyWrites`,
  etc.), which we'd want scripted anyway. Vendor a copy into `tests/e2e/` (third copy in
  the family — Cardcore, Streamplace, now here; fine for now, consider extracting
  later).
- **indigo's relay** as the oracle: run `cmd/relay` (or its `testing.SimpleRelay`)
  against the same PDS.

The core test is differential:

```
dev-env (PLC + PDS) ──┬──▶ indigo relay ──WS──▶ capture A
                      └──▶ atmoq    ──WS──▶ capture B   (legacy output)
                                        ──MOQT─▶ atmoq-client ──▶ capture C
write script ──XRPC──▶ PDS
assert: A ≡ B ≡ C  (event-by-event: type, did, rev, ops, blocks; seq monotonic per-stream)
```

Plus:

- **Scenario tests** ported from indigo's JSON scenarios (invalid sigs, bad MST
  inversions, rev rollbacks, future TIDs, oversized commits → assert drop/accept parity
  with indigo, strict and lenient).
- **Churn tests**: kill/restart consumers mid-stream (cursor resumption over both
  transports), kill/restart the PDS (host status transitions), induced gaps (FETCH
  recovery path).
- **Identity tests**: handle change, signing-key rotation mid-stream (the §4.5
  refresh-and-retry path), account deactivate/takedown gating subsequent commits.
- **Interop vectors** at the data layer (M1) shared with the future TS impl.
- **Public-relay compatibility suites** (decision 0001 update): a `atmoq diag`
  mode that publishes synthetic, self-verifying tracks (sequenced CBOR frames)
  through a *third-party public MoQ relay*, subscribes from another process/network,
  and verifies delivery, ordering, late-join, and cache behavior. Run the same suite
  against each public relay to empirically map dialect differences (protocol subset,
  ANNOUNCE support, auth/abuse model, size limits, retention). **Cloudflare's relay
  (`relay.cloudflare.mediaoverquic.com`, draft-07 subset, kixelated's libs interop
  with it) is the first target**; moq.dev's relay second. This is how we prove the
  techniques work over infrastructure we don't operate — free unmetered global
  fan-out while the giants are subsidizing MoQ adoption is a strategic goal of the
  project.

## 6. Open architectural questions

Flagged for discussion before/while M0 — roughly ordered by how much they block.

1. ~~**Which MoQ stack?**~~ **Decided 2026-06-10**: kixelated's
   [moq-dev/moq](https://github.com/moq-dev/moq) (moq-lite), staying close to his spec;
   no FETCH, and backfill comes from the PDS fleet rather than selective block sync.
   See [decision 0001](docs/decisions/0001-transport-stack.md).
2. ~~**Spec fidelity vs. fixing it.**~~ **Decided 2026-06-10**: implement the corrected
   profile and feed issues back to the ATOM authors rather than matching the draft's
   bugs.
3. **Total order vs. per-type tracks.** Plan currently says: combined track is canonical, per-type tracks are an optimization sharing the same seq space. Alternative: per-type only (pure ATOM) and push merge complexity to every consumer. Any reason to prefer the latter?
4. **How much indigo parity is in scope?** Sibling-relay admin forwarding, domain bans, trusted-host quota tiers — full parity, or just enough to run honestly (my assumption: takedowns/bans/limits yes, sibling forwarding later)?
5. **Build vs. reuse the atproto data layer.** Candidates: [rsky](https://github.com/blacksky-algorithms/rsky) (Blacksky's Rust atproto; includes `rsky-relay`, repo/MST/crypto crates — also worth studying as the "other Rust relay", though it's WS-based), atrium ecosystem crates, [atproto-repo](https://crates.io/crates/atproto-repo). Operation inversion against the *new* June 2026 sync draft may not exist anywhere yet — evaluate in M0, expect to own at least that piece.
6. **Is the legacy WS output a permanent feature or a testing/migration scaffold?** Affects how much we invest in its performance (backfill replay from disk, slow-consumer handling).
7. **Live-network ambition.** Is the goal a full-network relay (Bluesky mainnet scale: relay ingest is modest — tens of MB/s — but host count and account-state cardinality are real) or primarily Streamplace-network scale with full-network as a stretch? Affects storage choices (SQLite vs Postgres, 2M-entry account caches) and M5 scope.
8. **Repo/blob tracks priority.** M6 is where ATOM gets genuinely novel (per-record verifiable sync over FETCH could subsume `getRepo`/`getRecord`!). If that's the actual prize for Streamplace (e.g. browser clients verifying records without an AppView), we could pull parts of it earlier at the cost of relay parity later.

## 7. Risks

- **moq-lite churn**: kixelated iterates fast and the spec follows the code. Mitigation: isolate the transport behind `atmoq-atom` traits; pin crate/package versions per release. (This replaced the IETF-draft-churn risk; same shape, one repo instead of one WG.)
- **ATOM is -00 and unowned by atproto's authors**: Bluesky's sync direction (the new at-sync draft) evolved the same week ATOM was published. We are the integration point; expect to write the reconciliation ourselves (that's the fun part).
- **Validation correctness**: op-inversion subtleties (adjacent-node requirements, §4.1.2) are easy to get wrong; differential testing against indigo is our main defense.
- **Browser WebTransport reality**: still uneven across browsers/CDNs; the TS milestone should re-verify the landscape rather than trust today's assumptions.
