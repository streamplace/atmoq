# Discovery: stable relay URLs over squattable namespaces

Status: design sketch (2026-06-11, from discussion with Eli). Not implemented.

## Problem

Public MoQ relays with no auth (Cloudflare; cdn.moq.dev's `/anon`) make
namespaces IRC-channels-without-services: held by occupancy, lost the moment
the publisher blips (Cloudflare frees a claim ~16s after unclean death —
docs/diag). You can't advertise `moq://relay.example/atproto` as *the*
firehose URL when anyone can claim it during a gap.

## Design: authenticated pointer, rotating namespace

Run a small HTTPS origin (`https://example.network`) as the trust root.
The atmoq publisher:

1. generates a random (unguessable, 128-bit) broadcast namespace;
2. claims it on the MoQ relay and **passes announce probation**
   (`Publisher07::is_dead()` after the 500ms window) — only then
3. publishes a pointer document at
   `GET /.well-known/atmoq` (`Cache-Control: no-store`):

```json
{
  "url": "https://relay.cloudflare.mediaoverquic.com",
  "dialect": "ietf-07",
  "broadcast": "<current-random-id>",
  "track": "firehose",
  "startedAt": "2026-06-11T01:23:45Z"
}
```

On any publisher restart: new random namespace, updated pointer. Old
namespaces are abandoned, never reused.

Consumers resolve the pointer, then connect to the MoQ relay. **Pointer
resolution rules (the security-critical part):**

- re-fetch on *every* (re)connect — never reuse a pointer across sessions;
- re-fetch periodically while healthy (a squatter takeover of a stale
  namespace is NOT a transport failure: the session looks alive and serves
  forged frames; only pointer freshness + frame validation catch it);
- re-fetch on any anomaly (unparseable frames, seq regression).

Exposure window: only clients holding a pointer older than the last
publisher restart. A squatter cannot predict fresh namespaces. Post-M2,
forged `#commit`s also fail signature/op-inversion checks; `#account` /
`#identity` remain hop-by-hop trusted, with the HTTPS origin as their
effective trust anchor — same trust shape as a WS relay URL today.

### WebTransport 302 (native-only sugar)

The same origin can answer WebTransport CONNECTs with `302 Location:` the
current relay+namespace. Per the HTTP/3 draft, clients MUST NOT auto-follow
but MAY surface the redirect; atmoq's native client can follow it (may need
a small web-transport-quinn patch to expose status+Location). **Browsers
cannot**: the W3C API hides redirect targets (open issue
[w3c/webtransport#499](https://github.com/w3c/webtransport/issues/499)) —
browser consumers use the `.well-known` fetch, which they can always do
before opening WebTransport. Precedent for offering both: atproto handle
resolution (DNS TXT and `.well-known/atproto-did`).

### Authenticated namespaces where available

cdn.moq.dev supports `moq-token` JWTs: a signing root for an `atmoq/`
prefix makes squatting impossible there, reducing the pointer to pure
discovery. Worth requesting from kixelated. Cloudflare's preview has no
auth; the rotation scheme is the defense there.

### Long game: the pointer as an atproto record

Give the relay a DID and publish the pointer as a signed record
(`at://<did>/network.atmoq.relay/self`) and/or a DID-document service
endpoint, with `.well-known` kept for bootstrap. The firehose then
announces itself, verifiably, on the network it carries — and relay
migration becomes an atproto identity operation rather than a DNS one.

## Implementation sketch

- `atmoq gateway` (or `atmoq relay --advertise <addr>`): runs the relay
  with namespace rotation + serves the pointer endpoint (+ optional WT 302).
- Consumer side: `--moq-host https://example.network` detects a pointer
  document (or 302) and chases it; `subscribe_loop` re-resolves on every
  reconnect.
- e2e: harness gains a gateway leg; churn test asserts a consumer follows
  a pointer rotation across a publisher restart without manual help.
