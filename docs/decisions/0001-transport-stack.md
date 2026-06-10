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
