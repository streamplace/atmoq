# Relay behavior on invalid firehose data — a differential survey

_2026-07-19 (hydrant + zlay added 2026-07-21) · prep for the IETF atproto WG · harness: `tests/relay-conformance/`_

## The question

atproto's guidance is that infrastructure should be **tolerant of imperfectly
formatted data** to leave room for protocol evolution — but "tolerant" has
never been pinned down. A relay sits between thousands of PDSes and the rest of
the network; what should it do when a PDS emits something malformed? Drop the
event? Drop the connection? Pass it through unchanged? Reject it and force the
PDS to fix its encoder?

There are really **two different questions** hiding under "invalid data", and
conflating them causes most of the confusion:

1. **Malformed *encoding*** — bytes that violate CBOR or the DRISL determinism
   profile (unsorted map keys, float16, indefinite lengths, forbidden tags).
   Objectively wrong regardless of protocol version.
2. **Unknown *semantics*** — well-encoded data the relay doesn't recognize (a
   new message type, an unknown field on a known message). This is exactly what
   forward-compatibility is *for*.

Tolerance arguments almost always mean #2; strictness arguments almost always
mean #1. A relay can — and arguably should — be strict on #1 and tolerant on
#2. This survey measures where five implementations actually land.

## Method

`tests/relay-conformance/` builds a corpus of `subscribeRepos` frames each
malformed in exactly one way (one defect per frame → attributable behavior),
then runs the corpus through **each relay's real decode path** and records
accept / reject. The core corpus rides on otherwise-complete, signature-free
`#account` events, so a reject can only mean "the encoding or shape was
rejected" — no commit signature, MST, or CAR check to confound the result. Two
controls (a float64 value, a tag-42 CID) are valid DRISL and are *meant* to be
accepted; they catch a validator that is merely trigger-happy. The tag-42 control
passes on all five relays — but the **float64 control does not**: zlay's DAG-CBOR
decoder rejects all floats outright (see below), so even a "must-accept" control
now splits. Two further corpora of real signed `#commit`s and stateful sync-1.1
sequences (see the `#commit` and sync-1.1 sections below) probe the
repo/record/signature and at-synchronization layers.

Relays surveyed, at the commits checked out locally (atmoq/indigo/rsky on
2026-07-19, hydrant + zlay added 2026-07-21):

- **atmoq** (this repo) — Rust, DRISL-strict by design. Verdict via
  `Frame::parse`, its real ingest entry point.
- **indigo** `cmd/relay` (the "Sync 1.1" relay — current production Bluesky
  relay) — Go, whyrusleeping/cbor-gen. Verdict via the header + per-type body
  decode `HandleRepoStream` performs.
- **rsky-relay** (Blacksky) — Rust. Verdict via `SubscribeReposEvent::parse`,
  the validator's per-message entry point.
- **hydrant** — Rust, built on the `jacquard` atproto stack. Verdict via
  `ingest::stream::decode_frame` plus `ingest::validation::validate_commit` /
  `validate_sync` at its **default posture**: the relay always resolves the
  signing key, so signature verification *and* MST inversion run by default
  (`verify_mst || signing_key.is_some()`), with `verify_cids` off. The strictest
  default of the five.
- **zlay** — Zig, built on the `zat` atproto SDK. Verdict via `zat.cbor` frame
  decode (a strict whole-frame DAG-CBOR decoder) plus `Validator.validateCommit` /
  `validateSync` with the case's signing key pre-seeded into the key cache. Its
  posture is **fail-open**: on a cache miss *or any* signature/structure/MST
  failure the validator forwards the frame UNVALIDATED and re-resolves the key in
  the background — it never drops a commit on bad crypto. Only `#sync` structural
  checks and the `frame_worker` stale-rev check are hard drops.

**What a "reject" *does* differs per relay — itself a headline finding:**

| Relay | A decode-level reject causes… | Source |
|---|---|---|
| atmoq | drop **the frame**, connection stays up | `ingest.rs` (warn + continue) |
| indigo | drop **the whole upstream connection**, reconnect from cursor | `consumer.go` error → `slurper.go` redialer |
| rsky | drop **the event**, connection stays up | `manager.rs:211` (`continue`) |
| hydrant | CBOR error drops **the whole connection**; unknown op/type skips **one frame** | `firehose.rs` (`break Err` vs `continue`) |
| zlay | drop **the frame**, connection stays up | `subscriber.zig` / `frame_worker.zig` (`return`) |

The same malformed frame that costs rsky/atmoq/zlay a single event costs indigo
*and hydrant* the entire connection to that PDS until they reconnect — a much
larger blast radius, and a mild DoS foothold: one bad frame flaps the whole host.
Both reserve this for *encoding* errors: repo/MST/signature failures drop one
event and keep the socket, and (unlike rsky) both keep the connection open on an
unknown message type, skipping just that frame — as zlay does too.

## Results (empirical)

Verdict per relay. **reject** = the relay's decoder errored on this frame;
_accept_ = it decoded without error. Generated by
`node tests/relay-conformance/aggregate.mjs`.

| Case | Layer | atmoq | indigo | rsky | hydrant | zlay |
|---|---|:--:|:--:|:--:|:--:|:--:|
| `framing/single-object` (header, no payload) | framing | reject | reject | reject | reject | reject |
| `framing/trailing-bytes` | framing | reject | _accept_ | reject | _accept_ | reject |
| `framing/empty-message` | framing | reject | reject | reject | reject | reject |
| `cbor/truncated-payload` | cbor | reject | reject | reject | reject | reject |
| `cbor/reserved-ai-payload` (AI 28) | cbor | reject | reject | reject | reject | reject |
| `cbor/bare-break-payload` (0xff) | cbor | reject | reject | reject | reject | reject |
| `cbor/garbage-payload` | cbor | reject | reject | reject | reject | reject |
| `drisl/unordered-keys-payload` | drisl | reject | _accept_ | _accept_ | _accept_ | reject |
| `drisl/unordered-keys-header` | drisl | reject | _accept_ | _accept_ | _accept_ | reject |
| `drisl/duplicate-key-payload` | drisl | reject | _accept_ | reject | reject | reject |
| `drisl/nonminimal-int-payload` | drisl | reject | reject | _accept_ | _accept_ | reject |
| `drisl/nonminimal-len-payload` | drisl | reject | reject | _accept_ | reject | reject |
| `drisl/indefinite-map-payload` | drisl | reject | reject | reject | reject | reject |
| `drisl/indefinite-str-payload` | drisl | reject | reject | _accept_ | reject | reject |
| `drisl/float16-payload` † | drisl-float | reject | _accept_ | _accept_ | _accept_ | reject |
| `drisl/float32-payload` † | drisl-float | reject | _accept_ | _accept_ | _accept_ | reject |
| `drisl/float64-ok-payload` (**control**) | drisl-float | _accept_ | _accept_ | _accept_ | _accept_ | **reject** |
| `drisl/nan-payload` † | drisl-float | reject | _accept_ | _accept_ | _accept_ | reject |
| `drisl/infinity-payload` † | drisl-float | reject | _accept_ | _accept_ | _accept_ | reject |
| `drisl/undefined-payload` † | drisl-float | reject | _accept_ | _accept_ | _accept_ | reject |
| `drisl/simple-value-payload` † (simple 19) | drisl-float | reject | _accept_ | reject | reject | reject |
| `drisl/tag-0-payload` † | drisl-tag | reject | _accept_ | _accept_ | _accept_ | reject |
| `drisl/tag-2-bignum-payload` † | drisl-tag | reject | _accept_ | _accept_ | _accept_ | reject |
| `drisl/tag-42-ok-payload` (**control**) | drisl-tag | _accept_ | _accept_ | _accept_ | _accept_ | _accept_ |
| `drisl/tag-42-no-prefix-payload` † | drisl-tag | reject | reject | _accept_ | _accept_ | reject |
| `drisl/int-map-key-payload` | drisl-tag | reject | reject | reject | reject | reject |
| `drisl/invalid-utf8-payload` (did field) | drisl-tag | reject | _accept_ | reject | reject | reject |
| `sync/unknown-type` (`#futurething`) | at-sync | _accept_ | _accept_ | **reject** | _accept_ | _accept_ |
| `sync/unknown-field` | at-sync | _accept_ | _accept_ | _accept_ | _accept_ | _accept_ |
| `sync/missing-seq` | at-sync | _accept_ | _accept_ | reject | reject | _accept_ |
| `sync/wrong-type-seq` (seq as text) | at-sync | _accept_ | reject | reject | reject | _accept_ |
| `sync/op-1-no-t` | at-sync | reject | _accept_ | reject | reject | reject |

† The defect rides in an **unknown extra field** `x` (a float/tag has no
natural home in a typed `#account` field). indigo, rsky, and hydrant deserialize
into a typed struct and *skip* unknown fields without inspecting their interior
encoding, so they accept these — **not** because they bless float16, but because
they never look. atmoq **and zlay** DRISL-validate the whole frame, so they see
`x`. This is the crux: **float-width / tag / NaN enforcement only exists if you
validate the entire encoding, not just the fields you consume.** (hydrant uses
the same `serde_ipld_dagcbor` decoder as rsky's body, so it tracks rsky on the
skip-based cases — but decodes the *whole* frame with it, which is why it parts
ways with rsky on duplicate keys, indefinite strings, and simple values. zlay's
`zat.cbor` is a strict whole-frame DAG-CBOR decoder like atmoq's, so the two agree
on every DRISL case — except zlay rejects float64 too.)

Tally: **8 of 32** cases are rejected by all five (the hard-broken-CBOR floor:
truncated, reserved AI, bare break, garbage, indefinite map, header-only, empty,
integer map key). Only **2** are accepted by all five (the tag-42 control plus the
unknown-field forward-compat case) — the float64 control drops out because zlay
rejects it. The other **22 cases show at least two relays disagreeing.** Adding
relays four and five did not shrink the disagreement; it added a case (float64) on
which they now split. There is still no shared definition of "invalid" today.

## Reading the results

**The hard floor is universal.** Every relay rejects CBOR that cannot be
decoded at all — truncation, reserved additional-info, a bare break, indefinite
maps, non-text map keys. Nobody tolerates genuinely undecodable bytes. Good.

**DRISL determinism is where they scatter.** Each rule is enforced by a
different subset:

- *Map-key ordering:* atmoq **and zlay** (both strict whole-frame DAG-CBOR).
  indigo (cbor-gen), rsky, and hydrant all read keys in any order — hydrant too,
  because its dag-cbor decoder doesn't re-check ordering into a struct.
- *Duplicate keys:* atmoq, rsky, hydrant, **and zlay** reject; indigo silently
  takes the last value (`seq=999` won).
- *Minimal int / length encoding:* atmoq, indigo, **and zlay** reject; rsky does
  not enforce it. hydrant is in between only by accident — it accepts a
  non-minimal **int**, and its one non-minimal-**length** rejection is incidental
  (a `Did` format failure, not a canonical-encoding check).
- *Indefinite length:* everyone rejects an indefinite **map**. rsky's cbor4ii
  backend uniquely accepts an indefinite-length **string**; atmoq, indigo,
  hydrant, and zlay all reject it.
- *Floats / NaN / undefined / foreign tags:* atmoq **and zlay** (whole-frame; see
  the † note). indigo, rsky, and hydrant enforce nothing here for data they skip.
  **zlay goes further than everyone: it rejects float64 as well** (its `zat`
  DAG-CBOR decoder forbids all floats), so it fails the float64 control that the
  DRISL profile explicitly permits — a genuine disagreement about whether float64
  is legal atproto data. Where such a value lands in a *known* field, the
  per-field decoders react — a wrong-type `seq` is rejected by indigo, rsky, and
  hydrant but accepted by atmoq and zlay (which check encoding, not schema).
- *CID structure:* atmoq, indigo, **and zlay** reject a tag-42 whose bytes lack
  the 0x00 multibase prefix; rsky and hydrant skip it (it's under an unknown field).
- *UTF-8:* atmoq, rsky, hydrant, **and zlay** reject invalid UTF-8 in a string;
  indigo (Go strings are arbitrary bytes) keeps it verbatim.

**Semantic tolerance (`at-sync`) — the forward-compat layer:**

- *Unknown field* on a known message: accepted by all five. This is the one
  forward-compat case everyone gets right (though the struct decoders **drop** the
  field on re-serialize — they don't preserve it, so it won't survive a rebroadcast).
- *Unknown message type* (`#futurething`): atmoq, indigo, hydrant, and zlay pass
  it through (skip the frame, keep the connection — zlay even advances its cursor);
  **rsky drops it** (`ParseError::UnknownType`). A relay that hard-drops unknown
  event types is exactly the forward-compat hazard the WG worries about. rsky is
  the lone outlier across five relays — though note "tolerated" ≠ "propagated":
  the skip-based relays keep the connection but don't necessarily rebroadcast the
  unknown frame downstream (a rebroadcast question these decode harnesses don't
  measure).
- *Missing / mistyped required fields:* here "tolerance" is a bug, not a
  feature. atmoq **and zlay** accept a `#account` with no `seq` and a text-typed
  `seq` because they validate encoding, not schema (for atmoq, semantic validation
  is milestone M2). indigo fills a zero value for missing `seq` (then its
  downstream out-of-order check may drop it) and rejects the mistyped one; rsky
  and hydrant reject both. This is the gap between "valid DRISL" and "valid
  at-sync event".

## #commit frames — repo, records, and signatures

`#account` is signature- and CAR-free, so its defects are pure encoding/shape
questions. `#commit` is where the repo lives: the payload carries a CAR of
blocks (a signed commit object, the record blocks, MST nodes), and the
validation that matters happens *below* frame-decode. We built **real signed
commits** with `@atproto/repo` and a controlled keypair (so CIDs and signatures
are valid), then perturbed one thing each. The relay harnesses run each relay's
real commit-verification path and report the **relay-level verdict** (does it
drop the event?), mirroring enforce-vs-advisory gating — not merely whether a
verify function errored.

| Case | atmoq | indigo | rsky | hydrant | zlay |
|---|:--:|:--:|:--:|:--:|:--:|
| `commit/valid` (**control**) | accept | accept | accept | accept | accept |
| `commit/record-no-type` — record omits `$type` | accept | accept | accept | accept | accept |
| `commit/record-unknown-type` — unknown NSID | accept | accept | accept | accept | accept |
| `commit/record-not-map` — record is a CBOR list | accept | accept | accept | accept | accept |
| `commit/envelope-unordered` — outer map mis-sorted | **reject** | accept | accept | accept | **reject** |
| `commit/envelope-float16` — float16 in envelope | **reject** | accept | accept | accept | **reject** |
| `commit/too-big` — `tooBig` flag set | accept | **reject** | **reject** | accept | accept |
| `commit/cid-mismatch` — block ≠ its CID | accept | **reject** | **reject** | **reject** | accept¹ |
| `commit/missing-block` — op cites absent record CID | accept | accept | accept | **reject** | accept |
| `commit/bad-signature` — wrong signing key | accept | **reject** | accept | **reject** | accept¹ |

¹ zlay's `verifyCommitCar` *does* error on `cid-mismatch` and `bad-signature`, but
its fail-open policy **forwards the frame UNVALIDATED** rather than dropping it
(and re-resolves the key in the background). The event still propagates.

Six things stand out, and **still not one `#commit` case is rejected by all
five** — hydrant is the strictest column, zlay the most permissive:

1. **No relay validates record *contents*.** `record-no-type`,
   `record-unknown-type`, and `record-not-map` are accepted everywhere — even by
   hydrant, which is the *only* relay that decodes record bodies at all (as
   generic CBOR, `jacquard_common::Data`). So hydrant would reject a record that
   is *malformed CBOR*, but it still doesn't require a lexicon `$type` or even a
   map. Record-level semantics are left entirely to PDSes and AppViews. (So the
   reported "indigo rejects a record without `$type`" is **not** relay behavior —
   it's the PDS/lexicon layer.)
2. **Envelope DRISL is enforced by atmoq and zlay.** The two envelope-encoding
   defects (mis-sorted keys, float16) are caught by the two whole-frame validators
   — atmoq and zlay's `zat.cbor` — which decode the entire outer payload strictly.
   indigo's cbor-gen and rsky's/hydrant's dag-cbor decode into a struct without
   re-canonicalizing it, same as for `#account`.
3. **Repo/crypto integrity: indigo, rsky, and hydrant enforce it — atmoq and zlay
   don't.** `cid-mismatch` is rejected by indigo, rsky, and hydrant but forwarded
   by atmoq (opaque CAR, its M2) and by zlay (fail-open — its CAR verifier errors
   on the mismatch but publishes the frame anyway). hydrant catches it *without*
   CID hashing (`verify_cids` off) — it decodes the mismatched record block as CBOR
   and the corrupted bytes aren't valid. `bad-signature` is a genuine five-way
   split: indigo and hydrant **reject** (hard); atmoq accepts (no crypto); **rsky
   accepts under its default lenient mode** (published with a warning); and **zlay
   forwards it UNVALIDATED** (fail-open — a bad signature is treated as a possible
   key rotation, so the frame is published and the key re-resolved). Three of five
   relays let a wrong-key commit reach downstream consumers.
4. **`missing-block` is caught only by hydrant.** An op that references a record
   CID absent from the CAR is accepted by atmoq (doesn't read the CAR), rsky
   (record-presence is a TODO), and indigo (`repo.VerifyCommitMessage` *notices*
   — "could not find <cid>" — but the check is **advisory, logged not enforced**,
   and a first commit early-returns before it). hydrant alone hard-drops it: its
   per-op loop requires every op's block to be present in the CAR
   (`validation.rs`). The sharpest example of "a verify function errored" ≠ "the
   relay dropped it" — and the one relay that closes it.
5. **`tooBig` is a deprecated-flag reject only indigo and rsky honor.** hydrant
   and zlay (like atmoq) never inspect the `tooBig` flag, so they accept the frame.
   Which deprecated flags are *fatal* is itself unstandardized (see the `rebase`
   split in sync-1.1 below).
6. **zlay is fail-open; hydrant is fail-closed — same checks, opposite defaults.**
   zlay runs real signature verification, but on *any* failure (bad sig, bad CID,
   cache miss) it **forwards the frame unvalidated** and re-resolves the key in the
   background — it never drops a commit on crypto. hydrant runs the same class of
   checks and **drops** on failure. The only `#commit` cases zlay hard-rejects are
   the envelope-DRISL ones (caught at frame decode, before validation). A relay's
   fail-open-vs-fail-closed default is invisible in the lexicon but decides whether
   a forged commit reaches consumers.

## sync-1.1 compliance — `#sync`, prevData, and rev ordering

The [at-synchronization](https://www.ietf.org/archive/id/draft-holmgren-at-synchronization-00.txt)
("sync 1.1") rules are the ones that make the firehose *verifiable*: the
`#sync` event, and the §4.5 commit checks — `prevData`, rev ordering, the
retired `rebase` flag. Most of these fire only on a **second** commit, once the
relay holds prior repo state, so these cases carry a **sequence**: a valid setup
commit runs first, then the frame under test. rsky verdicts below are its
**default lenient** mode (production default); strict mode is noted where it
differs.

| Case | atmoq | indigo | rsky (default) | hydrant | zlay |
|---|:--:|:--:|:--:|:--:|:--:|
| `sync-event-valid` (**control**) | accept | accept | accept | accept | accept |
| `sync-event-bad-sig` — wrong-key `#sync` | accept | **reject** | accept¹ | **reject** | accept⁴ |
| `commit2-valid` (**control**) | accept | accept | accept | accept | accept |
| `commit2-missing-prevdata` | accept | **reject** | accept¹ | accept² | accept |
| `commit2-wrong-prevdata` | accept | accept | accept | **reject** | accept⁴ |
| `commit2-rev-rollback` | accept | **reject** | accept¹ | skip³ | skip³ |
| `commit-rebase-flag` | accept | accept | **reject** | accept | accept |

¹ rsky **strict** mode rejects these (bad signature, missing prevData, rev
rollback); its default lenient mode publishes them with a warning.
² hydrant treats a missing `prevData` as a **soft chain-break** (advisory) and
still emits the event — looser than indigo here.
³ hydrant's and zlay's stale-rev checks drop a rev-rollback as a **replay** (rev
not greater than the last seen), before/around validation — not forwarded.
⁴ zlay is **fail-open**: the `#sync` signature and the commit MST/prevData checks
run, but a failure forwards the frame UNVALIDATED (bad sig → key evicted +
re-resolve; prevData mismatch → advisory stat). The event still propagates.

Findings:

1. **`prevData` *correctness* is enforced by exactly one relay — hydrant.**
   `commit2-wrong-prevdata` — a commit whose `prevData` is present but points at
   the wrong root, so the MST inversion fails — is a **hard drop in hydrant**
   (`MST inversion root mismatch`), because hydrant always holds the signing key
   and therefore always runs the inversion. atmoq/indigo/rsky all accept it:
   indigo Warn-logs the mismatch but emits (`verify.go:142,153`); rsky's
   inverted-root check is advisory (`utils.rs:148-153` returns `true`); atmoq
   doesn't look; and zlay's inversion path is opt-in (`verify_commit_diff` off)
   while its `frame_worker` prevData check is advisory (a `chain_breaks` stat).
   **This is the flipped headline: the sync-1.1 property that lets a consumer
   verify an operation without fetching the repo is guaranteed by one relay out of
   five** — everywhere else, a commit with a lying `prevData` propagates. Whether
   that guarantee is a MUST or a hint is the open question.
2. **`prevData` *presence* is where the strict relays *disagree*.** indigo
   hard-rejects a missing `prevData` (`verify.go:137`); hydrant treats it as an
   advisory chain-break and forwards; rsky drops it only in strict mode. So the
   two "strict" relays land on opposite sides of the same case — indigo enforces
   presence but not correctness, hydrant enforces correctness but not presence.
3. **Rev ordering is enforced by indigo, hydrant, and zlay.** indigo drops a
   stale rev at `ingest.go:116`; hydrant's `StaleRev` check drops it as a replay;
   zlay's `frame_worker` stale-rev check (`frame_worker.zig:200`) drops it too.
   rsky computes the failure but its default lenient mode publishes it (strict
   drops). Rev-rollback is the one sync-1.1 check the fail-open zlay *does* enforce
   — because it lives in the frame pipeline, not the (fail-open) validator.
4. **rsky's default config enforces almost nothing at this layer.** In lenient
   default the only sync-1.1 hard drop is the deprecated `rebase` flag (an
   envelope check that isn't lenient-gated) — and `rebase` is a flag *hydrant and
   indigo don't drop on at all*. Bad `#sync` signatures, missing `prevData`, and
   rev rollbacks are all published with a warning. Production defaults matter as
   much as the code paths.
5. **`#sync` signatures are *checked* everywhere with a key, but only indigo and
   hydrant *drop* on failure.** All three of indigo, hydrant, and zlay verify the
   `#sync` commit-block signature; indigo and hydrant reject on failure, but
   **zlay forwards it unvalidated** (fail-open: a bad sig is treated as a possible
   key rotation → evict + re-resolve, publish anyway). rsky is lenient by default;
   atmoq does no crypto. So a wrong-key `#sync` reaches consumers via three of five
   relays.
6. **The deprecated flags are a mess.** `rebase` is a hard drop only in rsky;
   `tooBig` (in the `#commit` corpus) a hard drop only in indigo and rsky. indigo's
   `rebase` rejection additionally has a first-commit hole — a single commit takes
   the `prevRepo==nil` early return (`verify.go:132`) before the rebase check
   (`verify.go:158`), so indigo accepts `rebase=true` here. Five relays, five
   different sets of "fatal" deprecated flags.

## For the WG

1. **Separate the two questions in the spec.** State "be tolerant" as: reject
   malformed *encoding*, preserve/pass-through unknown *semantics*. The at-sync
   draft §4.5 covers semantics; the encoding contract (DRISL / deterministic
   CBOR) deserves an equally explicit normative "MUST reject" list. Today every
   relay improvises a different subset, and the subsets barely overlap.

2. **Specify the reject *consequence*, not just reject-or-not.** Dropping one
   event vs. dropping a whole PDS connection are very different failure modes
   for a shared relay. If one PDS's bad frame shouldn't degrade a relay's view
   of the network, the spec should say a malformed frame drops the *event*, not
   the *stream*. indigo's connection-drop is the outlier and arguably a DoS
   foothold.

3. **Unknown message types must be forwarded, not dropped.** rsky erroring on
   `#futurething` is the single most consequential forward-compat divergence here:
   it would blackhole a future event type. The other four tolerate it — though
   "tolerate" (keep the connection) is not the same as "propagate" (rebroadcast
   downstream), a distinction these decode harnesses don't measure. If the
   firehose is meant to be extensible, "pass through unknown `t` verbatim" should
   be a MUST, and unknown fields should be **preserved** on rebroadcast.

4. **Decide whether DRISL is normative or advisory — and enforce it uniformly.**
   Map-key ordering, duplicate keys, minimal encoding, and float width are all
   DRISL rules enforced fully by the two whole-frame validators (atmoq, zlay),
   partially by indigo, and barely by rsky. If DRISL is normative, a record that
   is "valid" from one relay's view is "invalid" from another's — the current
   middle ground is the worst outcome. **And pin down floats specifically:** the
   DRISL profile permits float64 while forbidding float16/32 — but zlay's decoder
   rejects float64 too (atproto records carry no floats, so `zat` forbids them
   outright), which means the two strict validators *disagree on the float64
   control itself*. Is float64 legal atproto wire data or not? Two conformant-looking
   relays already answer differently.

5. **Say explicitly that relays do not validate record contents — or that they
   must.** Today no relay decodes record bodies, so a record missing `$type`,
   carrying an unknown `$type`, or that isn't even a map propagates unchanged.
   That may be the intended app-agnostic design — but it's currently implicit,
   and it means "the firehose carries valid records" is not a guarantee any
   relay provides. Relatedly, the spec should distinguish *advisory* from
   *gating* checks: indigo's MST/record-presence verification errors on a
   missing block yet still forwards the event, so "the relay verifies X" can be
   true of the code and false of the behavior.

6. **Is `prevData` a promise or a hint?** sync 1.1 adds `prevData` so consumers
   can verify an operation's inversion without fetching the repo — but only
   **one of five** surveyed relays (hydrant) actually enforces that the
   `prevData`/inversion is *correct* (§ sync-1.1 above); the other four forward
   a lying `prevData`. And the two relays usually called "strict" split on the
   *presence* case too — indigo enforces presence, hydrant enforces correctness,
   neither enforces both. If verifiable-sync-without-fetch is a real goal, the
   spec should say a commit whose inversion doesn't reproduce `prevData` MUST be
   dropped, and relays should make that a gating check, not a warning. If it's
   only a hint, say so — because consumers are otherwise trusting a field four
   of five relays never checked. And since the default deployment config is what
   the network actually runs, the spec's "MUST" list should be what ships
   enabled, not what a strict flag turns on. hydrant's stricter posture, notably,
   is its *default* — it costs an always-on signing-key resolution + MST
   inversion per commit, the real price of that guarantee.

7. **Say whether a relay may fail *open*.** The starkest split isn't a check —
   it's the *default when a check fails*. hydrant drops a commit it can't validate
   (fail-closed); **zlay forwards it unvalidated and re-resolves the key in the
   background** (fail-open), so a wrong-key or wrong-CID commit still reaches
   consumers. Both are defensible engineering choices — fail-open keeps the
   firehose flowing during a key-rotation lag; fail-closed guarantees every
   forwarded commit was verified. But they are *opposite security postures*, and
   nothing in the lexicon says which a "relay" is. Three of five relays let a
   wrong-key commit through today. The spec should state whether a relay MUST drop
   an event that fails verification, or MAY forward it pending re-resolution.

## Reproducing / extending

See `tests/relay-conformance/README.md`. Adding a relay = write a small harness
that runs the shared `corpus.json` through that relay's decode entry point and
emits `results/<relay>.jsonl`; `aggregate.mjs` does the rest. The five existing
harnesses:

- atmoq: `rust/crates/atmoq/examples/relay_conformance.rs`
- indigo: `cmd/relay-conformance/main.go` (in the indigo checkout)
- rsky: `rsky-relay/examples/conformance.rs` (in the rsky checkout)
- hydrant: `examples/conformance.rs` + `src/conformance.rs` façade behind the
  `conformance` feature (in the hydrant checkout) — run with
  `cargo run --example conformance --features conformance -- <corpus.json>`
- zlay: `src/conformance_main.zig` + a `conformance` build target (in the zlay
  checkout). Zig 0.16 builds cleanly only in the vendored Docker toolchain, so it
  runs there: `docker build --target builder -t zlay-builder .`, then
  `docker run --rm -v <corpus-dir>:/corpus -w /corpus -e MODE=account -e
  CORPUS=corpus.json -e OUT=results/zlay.jsonl --entrypoint
  /build/zig-out/bin/conformance zlay-builder` (repeat with `MODE=commit`/`sync`).
  It seeds the signing key into the validator cache and drives the real
  `zat.cbor` decoder + `Validator.validateCommit`/`validateSync`.

**Next step — end-to-end injection.** These harnesses answer accept/reject at
the decoder precisely and cheaply. To confirm the *consequences* live (does
indigo's connection actually drop? is an accepted frame rebroadcast
byte-for-byte, or re-encoded and normalized? does an unknown field survive?),
point a relay at a **mutating upstream**: a WS server that proxies the vendored
dev-env PDS (`tests/e2e/dev-env`) so identity resolves, but injects/mutates
crafted frames on the firehose leg, and capture the relay's downstream. That
turns the inferred "what reject means" column into measured behavior.
