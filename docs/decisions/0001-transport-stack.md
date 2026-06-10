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
  `lastproto-atom` remains the translation layer either way, which keeps a
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
  per-type tracks; to be settled in the lastproto-atom design, guided by
  what's idiomatic in moq-lite/hang).
- Cite/track moq-lite's spec (moq.dev) rather than draft-ietf-moq-transport in
  implementation docs; pin crate/package versions per release (kixelated
  iterates fast — the spec-churn risk moves from the IETF WG to one repo).

## Update 2026-06-10: public relay fleets

Part of the motivation for this project: Cloudflare, Cisco, and other large
operators want MoQ to succeed and are currently running **public, unmetered MoQ
relays**. Riding that infrastructure (free global fan-out for atproto firehose
distribution) is a strategic goal, and it cuts across this decision:

- kixelated operates a public **moq-lite** relay (moq.dev) — usable with our
  primary stack immediately.
- The Cloudflare / Cisco fleets speak **IETF MOQT drafts**, not moq-lite.

This doesn't change the primary choice, but it upgrades two soft requirements
to hard ones:

1. The transport abstraction in `lastproto-atom` must keep a second (IETF MOQT)
   backend genuinely implementable — no moq-lite types or discovery semantics
   may leak into the data-plane design.
2. We need **end-to-end diagnostics that run over third-party public relays we
   don't operate** (see PLAN.md §5): publish synthetic firehose tracks through
   a public relay, subscribe from elsewhere, and verify delivery, ordering,
   group/caching behavior, and late-join semantics. These diagnostics double as
   the acceptance test for any second transport backend.
