# 0001: MoQ transport stack — kixelated's moq (moq-lite)

Date: 2026-06-10. Decided by Eli.

## Decision

Build on [moq-dev/moq](https://github.com/moq-dev/moq) (kixelated's moq-lite +
hang stack, Rust + TypeScript), staying as close to his spec as practical.
Rationale: he's been helpful, the project is the most focused on shipping
working software fast, and it has the best Rust + TS + browser story — which is
the actual product requirement (PLAN.md §3.4).

## Consequences

- **We are not on IETF MOQT.** ATOM's wire details (PUBLISH_NAMESPACE /
  SUBSCRIBE_NAMESPACE, FETCH, extension headers, subgroup machinery) are
  draft-MOQT constructs; we adopt ATOM's *data-plane concepts* (track layout,
  group semantics, seq-carrying objects, priority tiers) translated onto
  moq-lite's model (broadcasts/tracks/groups/frames, announce-based discovery).
  `atmoq-atom` remains the translation layer either way, which keeps a
  future IETF-MOQT backend possible if the drafts and implementations converge.
- **No FETCH, and that's fine.** True atproto backfill comes from syncing back
  to the PDS fleet (at-sync §3 + §4.6 full-repo fetch), not from selectively
  re-fetching transport blocks. Gap recovery on the MoQ side is therefore:
  re-subscribe at the live edge, diff `at-seq`/`rev` per account, and re-sync
  desynchronized accounts from their PDS — the same path any at-sync consumer
  already needs. The legacy WS output keeps full cursor-replay semantics for
  drop-in indigo compatibility; whether the MoQ side also serves a replay
  window (e.g. via group history) is an open tuning question, not a
  correctness requirement.
- Since moq-lite has no per-object extension headers, `at-seq` / event-type /
  did metadata live in the payload framing we define (the at-sync payload
  already carries `seq`; event type needs a place — small envelope or
  per-type tracks; to be settled in the atmoq-atom design, guided by
  what's idiomatic in moq-lite/hang).
- Cite/track moq-lite's spec (moq.dev) rather than draft-ietf-moq-transport in
  implementation docs; pin crate/package versions per release (kixelated
  iterates fast — the spec-churn risk moves from the IETF WG to one repo).

## Update 2026-06-10: public relay fleets — Cloudflare first

Part of the motivation for this project: Cloudflare, Cisco, and other large
operators want MoQ to succeed and are currently running **public, unmetered MoQ
relays**. Riding that infrastructure (free global fan-out for atproto firehose
distribution) is a strategic goal. Eli: target **Cloudflare's relay first**.

What we know about the landscape (see kixelated's
[First MoQ CDN post](https://moq.dev/blog/first-cdn/) and
[Cloudflare's MoQ docs](https://developers.cloudflare.com/moq/)):

- Cloudflare's public relay: `relay.cloudflare.mediaoverquic.com`, running on
  their full anycast network (330+ cities), free technical preview. It speaks
  a *small subset of IETF draft-07* — and per kixelated (directly to Eli, and
  per his blog), his @moq client libraries interoperate with it. So
  "moq-lite-first" and "Cloudflare-first" are compatible choices.
- Known Cloudflare dialect gaps today: **no ANNOUNCE** (no discovery — track
  names must be constructed deterministically out-of-band) and **no auth**
  (anyone can publish under any broadcast name; squatting/poisoning is
  possible — atproto's signed events make garbage *detectable*, but per-relay
  abuse models need characterizing).
- kixelated also runs his own public moq-lite relay (moq.dev infra), and other
  fleets (Cisco etc.) exist with their own dialects.

Consequences:

1. **Per-relay compatibility suites** (PLAN.md §5): the same self-verifying
   diagnostic suite run against each public relay — Cloudflare first, moq.dev
   second, others as discovered — to empirically characterize how differently
   each one needs to be spoken: protocol version/subset, discovery (ANNOUNCE
   or not), auth/abuse model, object size limits, group retention/caching,
   ordering, late-join. Expect to maintain more than one dialect over time.
2. **Never depend on ANNOUNCE for correctness.** Track/broadcast names must be
   deterministically derivable (e.g. from relay host + DID + event type) so
   discovery-free relays like Cloudflare's work. This aligns with ATOM's
   deterministic namespace scheme anyway.
3. The transport abstraction in `atmoq-atom` must keep additional backends
   genuinely implementable — no single relay's types or discovery semantics
   may leak into the data-plane design. The diag suite is the acceptance test
   for each new backend/dialect.
