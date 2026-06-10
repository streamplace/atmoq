# ATOM draft-00 review notes

A close read of [draft-nandakumar-atproto-atom-00](https://www.ietf.org/archive/id/draft-nandakumar-atproto-atom-00.txt)
against the authoritative atproto specs
([at-repository-02](https://www.ietf.org/archive/id/draft-holmgren-at-repository-02.txt),
[at-synchronization-00](https://www.ietf.org/archive/id/draft-holmgren-at-synchronization-00.txt))
and MOQT draft-16/17. The broad design — firehose as MOQT tracks, groups as cache/replay
units, FETCH for gap recovery, relay-tier caching — is sound and worth building. The
items below are where the draft conflicts with atproto reality or with MOQT itself, and
what atmoq intends to do instead. Each is a candidate issue for the
[draft's tracker](https://github.com/snandaku/draft-nandakumar-atproto-atom).

## 1. Cursor mapping assumes dense sequence numbers (§5.1)

ATOM derives MOQT position arithmetically: `group = (seq - seq_base) / group_size`,
`object = seq % group_size`. But at-sync §4.3 explicitly permits arbitrary gaps between
consecutive sequence numbers ("Sequence semantics are flexible, and they may contain
arbitrary gaps"), and real relays produce gaps (filtered events, sequencer restarts that
must jump strictly upward). With gaps, division gives wrong positions, and "Objects 0–4
missing from Group 6" (§5.2.1) becomes indistinguishable from a legitimate seq gap —
the gap-detection mechanism would trigger spurious FETCHes network-wide.

**atmoq**: objects are packed densely into groups (object IDs 0..N-1 are positional,
not seq-derived); the authoritative cursor is the `at-seq` extension header (which §5.1
already mandates — good); cursor→(group,object) resolution uses a persisted index
maintained by the publisher, not arithmetic. MOQT-level gap detection then works as
designed because object IDs are dense by construction.

## 2. Namespace format is internally inconsistent (§3.1 vs §4.1 vs §4.2.1)

Three different shapes appear:

- §3.1 table: `at/firehose/pds.example.com`
- §4.1 diagram: `at/{pds-host}/firehose`
- §4.2.1: `at/firehose/{host}/{event-type}` with track name `{did|all}`

Also note MOQT namespaces are *tuples* of binary fields, not slash-strings; the draft
should specify the tuple encoding explicitly.

**atmoq**: namespace tuple `("at", "firehose", host)`, track name
`{event-type}/{all|did}` — pending what other implementations do. We'll pick one
canonical form, document it, and file the inconsistency upstream.

## 3. Per-event-type tracks break total ordering (§4.2.1, Table 2)

The atproto firehose is a single totally-ordered stream, and that matters semantically:
an `#account` takedown must gate later `#commit`s for that DID; `#identity` (key
rotation) affects signature verification of subsequent commits (at-sync §4.5 step 4).
ATOM splits events across four tracks with independent priorities and never specifies
cross-track ordering. A consumer that processes the high-priority identity/account
tracks "ahead of" commits will also sometimes process commits *before* the takedown that
should have gated them — priority delivery reorders both ways under congestion.

**atmoq**: all tracks share one sequence space; `at-seq` is mandatory everywhere; we
publish an additional combined-order `firehose` track as the canonical stream, with the
per-type tracks as a subscription/priority optimization. Consumers of split tracks MUST
merge on `at-seq` before applying state-dependent validation. (The priority table is
still useful — it's the *transport* ordering, never the *application* ordering.)

Open per-DID question: §4.2.1's `{did|all}` track naming implies a relay offers millions
of per-DID tracks. Announcement, cache-key, and state costs of that are unexamined in
the draft. Deferred; `all` only for MVP.

## 4. Repo-sync subgroup filtering doesn't exist in MOQT (§4.2.2, §4.4.2)

The selective-sync design ("SubgroupFilter: [0, 1]", "Posts only: ~15% bandwidth")
requires subscribing to a subset of subgroups within a track. MOQT (through -17) has no
subgroup filter in SUBSCRIBE or FETCH — subgroups are delivery/stream scheduling units,
not addressable subsets. As drafted, every subscriber gets every subgroup.

Also, mapping `Subgroup = Collection` is awkward: subgroup IDs are numeric and
per-group; collections are strings whose set changes per commit; nothing specifies the
numbering.

**atmoq (phase 2)**: keep `Group = commit` (that part is good — unicity of the MST
means a commit is a natural immutable cache unit). For selectivity, use per-collection
*tracks* (`at/repo/{host}/{did}` namespace, track per collection plus a `commits+mst`
track) or FETCH-driven retrieval of specific records + MST proof paths, rather than
subgroup filtering. Needs a design pass of its own; this is the most novel and most
underspecified part of ATOM. The "record + MST proof path" object format also needs a
concrete encoding (the existing CAR-slice format from at-repo §5 with dangling links is
the obvious candidate).

## 5. Blob track addressing truncates CIDs (§4.2.3)

`Group ID = first 8 bytes of multihash as uint64` invites engineered collisions
(attacker grinds a blob whose multihash prefix matches a popular blob → cache poisoning
at relays that key on group ID), and `Object ID = 0` makes a multi-GB video a single
MOQT object, defeating incremental delivery and relay caching granularity.

**atmoq (phase 2)**: address blobs by full CID — either track-per-blob
(`at/blobs/{host}` namespace, track name = CID string) or FETCH with the CID in the
track name — and chunk blob bytes across objects within a group (fixed chunk size,
`at-block-cid` extension header on object 0 for verification). Streaming verification of
chunked blobs (the multihash covers the whole blob) needs thought; possibly punt
verification to the consumer after reassembly, as HTTP blob fetch does today.

## 6. Event payload framing is underspecified (§4.3)

"Payload: CBOR-encoded event (compatible with AT-REPO)" doesn't say whether the payload
includes the at-sync header object (`{op, t}`) or just the message payload, whether
deterministic CBOR is required, or how the 5 MB WS frame limit maps.

**atmoq**: payload = the at-sync *payload object only*, deterministic CBOR, exactly
the bytes a WS consumer would see after the header object; `t` is conveyed by
`at-event-type`; `op=-1` error frames have no MOQT equivalent (errors are
SUBSCRIBE_ERROR / session close). Size limits inherited from at-sync (§4.4.2: 5 MB
message, 2 MB blocks, 200 ops). Duplicated fields (`at-seq` vs payload `seq`,
`at-repo-did` vs payload `repo`/`did`, `at-repo-rev` vs `rev`) MUST match; receivers
validate payload-side values (the extension headers are unauthenticated routing hints —
same trust model as the WS header).

## 7. Smaller items

- **§4.4.1 `StartGroup=N: resume from specific cursor position`** — conflates SUBSCRIBE
  (live-edge protocol) with replay; resumption is FETCH-then-SUBSCRIBE, which §5.3.2
  itself describes correctly. Also the filter names (LatestGroup/LatestObject/
  AbsoluteStart/AbsoluteRange) drift across MOQT draft versions; should cite -16's exact
  names.
- **§4.1 setup parameters** (`at-version`, `at-supported-events`, `at-relay-caps`):
  fine in principle; values/semantics of `relay-capabilities: 0x07` are never defined.
  We'll implement `at-version=1` and treat the rest as reserved until defined.
- **§2.1.2 / §2.2.1 framing of current-protocol limits** is somewhat stale vs
  at-synchronization-00 (e.g. gap detection via `prevData` chaining + op inversion is
  cheap and stateless-ish; "recovery requires fetching the complete repository" is the
  worst case, not the norm). Doesn't affect the design, but the motivation section
  oversells some pain points.
- **§5.2.4** says full re-sync validates "against a #sync event" — per at-sync §4.6 the
  consumer re-syncs against `getRepo` (or repo-sync track) output and the *current
  commit*; #sync messages are one trigger among several (desync via prevData mismatch
  being the common one).
- **§6/§7/§8 (Auth, Security, IANA) are TODO.** For atmoq: firehose data is
  manifestly public (at-arch §7), so MVP is unauthenticated reads, parity with WS
  firehose. The C4M common-access-token draft is referenced informatively and is the
  obvious shape for rate-limit tiers/trusted consumers later. The extension-header and
  setup-parameter ID space (0x4154xxxx) presumably needs IANA registration eventually.
- **Hop-by-hop semantics**: at-sync is explicit that `#account`/`#identity` are
  hop-by-hop and unauthenticated, and that intermediaries may override upstream status.
  ATOM's relay-aggregation section (§4.6) doesn't address how a MOQT relay (which is
  content-agnostic) interacts with that — an *atproto* relay is not a *MOQT* relay: it
  re-originates a new sequence space and applies policy. Worth a paragraph upstream;
  atmoq is an atproto relay that *uses* MOQT, and generic MOQT relays can sit
  between it and consumers as dumb caches.
