# relay-conformance: invalid-data behavior across atproto relays

A differential harness for the question *"what should an atproto relay do with
invalid data on the firehose?"* — built for the IETF atproto WG discussion on
relay tolerance vs. strictness.

It feeds each relay a corpus of `com.atproto.sync.subscribeRepos` frames that
are malformed in exactly **one** way and records what the relay does. One
defect per frame means every observed behavior is attributable to that defect.

## The corpus

`cases.mjs` builds each frame from hand-written CBOR (so we can emit encodings
no conformant encoder would — non-minimal ints, unsorted keys, wrong float
widths, forbidden tags, indefinite lengths). `emit.mjs` serializes the corpus
to `corpus.json` (`{id, layer, title, note, expect, hex}`). Layers, roughly in
increasing subtlety:

| Layer | What it probes | Examples |
|---|---|---|
| `framing` | the two-CBOR-object frame contract | one object only, trailing byte, empty message |
| `cbor` | CBOR well-formedness | truncated item, reserved additional-info, bare break |
| `drisl` | DRISL determinism (valid CBOR, non-canonical) | unsorted/duplicate keys, non-minimal int/len, indefinite length |
| `drisl-float` | DRISL float & simple-value rules | float16, float32, **float64 (valid — control)**, NaN, Infinity, undefined, simple(19) |
| `drisl-tag` | DRISL tag & key-type rules | tag 0, tag 2, **tag 42 (valid — control)**, CID without 0x00 prefix, int key, bad UTF-8 |
| `at-sync` | frame semantics (valid DRISL, odd shape) | **unknown type**, **unknown field**, missing/mistyped field, op:1 without t |

The DRISL cases ride on signature-free events (`#account`) so a reject can only
mean "the encoding was rejected" — no commit signature, MST, or CAR check to
confound the result. Two control cases (`float64-ok`, `tag-42-ok`) are valid
DRISL and MUST be accepted; they catch a validator that is merely trigger-happy.

The `at-sync` layer is where the WG's real tension lives: **unknown message
types and unknown fields are the forward-compatibility cases** — tolerance says
pass them through; strictness says the encoding is fine so of course pass them
through. Malformed *encoding* is a different question from unknown *semantics*.

## Per-relay harnesses

Each harness runs the corpus through that relay's **real decode path** and
writes `results/<relay>.jsonl` (`{id, outcome: accept|reject|skip, detail}`).
This isolates the decode verdict; what a "reject" then *does* (drop the frame,
drop the event, or drop the whole connection) is a per-relay policy documented
in `aggregate.mjs` and the findings doc.

- **atmoq** (`rust/crates/atmoq/examples/relay_conformance.rs`): runs each frame
  through `atmoq::frame::Frame::parse` — exactly what `ingest::subscribe_repos`
  calls per upstream message.
  ```
  cd ../../rust && cargo run --example relay_conformance -- \
    ../tests/relay-conformance/corpus.json > ../tests/relay-conformance/results/atmoq.jsonl
  ```
- **indigo** (`cmd/relay-conformance/` in the indigo checkout): header +
  per-type body decode via whyrusleeping/cbor-gen, as `HandleRepoStream` does.
- **rsky** (`examples/conformance.rs` in the rsky-relay crate): runs
  `SubscribeReposEvent::parse` — the validator's per-message entry point.
- **hydrant** (`examples/conformance.rs` in the hydrant checkout, behind the
  additive `conformance` feature): runs `ingest::stream::decode_frame` and
  `ingest::validation::validate_commit`/`validate_sync` at hydrant's **default
  posture** — the relay always resolves the signing key, so signature verify and
  MST inversion are both on (`verify_mst || signing_key.is_some()`), with
  `verify_cids` off (config default). A thin in-crate `pub mod conformance`
  façade exposes those crate-internal paths without widening any other API.
  ```
  cd ../../../hydrant && cargo run --example conformance --features conformance -- \
    ../atmoq/tests/relay-conformance/corpus.json \
    > ../atmoq/tests/relay-conformance/results/hydrant.jsonl
  ```
- **zlay** (`src/conformance_main.zig` + a `conformance` build target in the zlay
  checkout): a Zig 0.16 relay on the `zat` SDK. Runs `zat.cbor` frame decode plus
  `Validator.validateCommit`/`validateSync` with the signing key pre-seeded into
  the key cache. zlay's posture is **fail-open** (forwards what it can't validate).
  Zig 0.16 builds cleanly only in the vendored Docker toolchain, so the harness
  runs there (env vars carry the config since the 0.16 process/args API is in flux):
  ```
  cd ../../../zlay && docker build --target builder -t zlay-builder .
  docker run --rm -v $PWD/../atmoq/tests/relay-conformance:/corpus -w /corpus \
    -e MODE=account -e CORPUS=corpus.json -e OUT=results/zlay.jsonl \
    --entrypoint /build/zig-out/bin/conformance zlay-builder   # repeat: MODE=commit|sync
  ```

## Aggregate

```
node aggregate.mjs                            # #account corpus
node aggregate.mjs corpus-commit.json -commit # #commit corpus
node aggregate.mjs corpus-sync.json -sync     # sync-1.1 corpus
```

## The `#commit` corpus

`#account` is signature- and CAR-free, so its defects are pure encoding/shape
questions. `#commit` is the hard case: the payload carries a CAR of blocks — a
signed commit object, the record blocks, and MST nodes, all content-addressed —
and the interesting validation happens *below* frame-decode, in each relay's
commit/CAR/MST/signature verification.

`commit-gen/build-commits.mjs` builds **real, signed** commits with
`@atproto/repo` and a keypair it controls (so CIDs and signatures are valid),
then perturbs exactly one thing → `corpus-commit.json`. Each entry also carries
`signingKey` (a `did:key`) and `repoDid` so a harness can verify the signature
the way the relay would after resolving identity. Cases: a valid control, a
record missing `$type`, an unknown `$type`, a non-map record, envelope key-order
and float16 defects, `tooBig`, a CAR CID mismatch, a missing referenced block,
and a wrong-key signature.

```
cd commit-gen && npm install && node build-commits.mjs   # regenerate (new keypair each run)
```

Because the signing key is random per generation, generate **once** and point
all harnesses at the same `corpus-commit.json`. Each relay harness takes a
`--commit` flag to run this corpus through its commit-verification path.

A key subtlety these results expose: a relay's decision is **enforce-vs-advisory**,
not just "did a verify function error". indigo logs MST/record failures without
dropping the event; rsky's default lenient mode publishes signature failures.
hydrant sits at the strict end — it hard-drops on bad signature, a missing block,
and (uniquely) a wrong `prevData`, and it is the only relay that decodes record
blocks as CBOR at all. zlay sits at the opposite, **fail-open** end — it runs the
same crypto but *forwards* the frame unvalidated on any failure (bad sig, bad CID,
cache miss), re-resolving the key in the background; it never drops a commit on
crypto. The harnesses report the *relay-level* verdict (drop or not), not the raw
function result.

## The sync-1.1 corpus (stateful)

`commit-gen/build-sync.mjs` → `corpus-sync.json` probes the
[at-synchronization](https://www.ietf.org/archive/id/draft-holmgren-at-synchronization-00.txt)
rules: the `#sync` event, and the §4.5 commit checks that only fire on a
**second** commit once the relay holds prior repo state — `prevData` presence &
correctness, rev ordering, and the deprecated `rebase` flag. So each of these
cases carries a **sequence**:

```json
{ "id": "...", "frames": [ {"role":"setup","hex":"..."}, {"role":"test","hex":"..."} ] }
```

The harness feeds the `setup` commit first (to establish the prior `RepoState` /
MST the relay caches), then judges the `test` frame — the verdict is the
behavior on that last frame. Cases: valid `#sync`, wrong-key `#sync`, valid
second commit, and second commits that omit `prevData`, carry a wrong
`prevData`, roll back `rev`, or set `rebase`. Run:

```
node aggregate.mjs corpus-sync.json -sync
```

This is where enforce-vs-advisory bites hardest: a relay may *compute* that
`prevData` is wrong or an operation doesn't invert, yet still forward the event
(indigo logs it; rsky's lenient default publishes it). The corpus separates
`prevData` **presence** from **correctness** precisely to expose that gap — and
exactly one relay, **hydrant**, closes it: because it always holds the signing
key it always runs the MST inversion, so a present-but-wrong `prevData` is a hard
drop there and nowhere else. (hydrant is *looser* on `prevData` **presence**,
though: a second commit that omits `prevData` is a soft chain-break it forwards,
where indigo hard-rejects.)

## Deeper: end-to-end injection (future)

The decode harnesses answer *"accept or reject?"* precisely and cheaply. To
observe the full consequence end-to-end (does the connection actually drop? is
the frame republished byte-for-byte? does the relay crash?), point a relay at a
**mutating upstream** — a WS server that proxies the vendored dev-env PDS
(`../e2e/dev-env`) so identity resolves, but injects/mutates crafted frames on
the firehose leg — and capture the relay's downstream. Sketched in the findings
doc; the decode harnesses cover the accept/reject axis today.
