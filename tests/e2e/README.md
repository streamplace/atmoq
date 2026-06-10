# e2e harness

A self-contained atproto network for differential relay testing, per
[PLAN.md §5](../../PLAN.md): PLC + PDS (vendored atcute dev-env) and the
**indigo relay as the oracle**, all in one container on localhost so the
DID-document URLs the PDS registers resolve from every process.

```
./test.sh
```

builds the image, boots the network, drives writes into the PDS over XRPC
(`harness/driver.mjs`), captures the relay's firehose to normalized JSONL
(`harness/capture.mjs`), and asserts the capture matches the driven writes
(`harness/verify.mjs`): identity/account events present, commit ops exact and
in order, seq and rev strictly increasing, sync-v1.1 invariants
(`prevData` present, `tooBig` false).

Ports (published by `test.sh`): 2582 PLC, 2583 PDS, 2470 relay
API + firehose (`ws://localhost:2470/xrpc/com.atproto.sync.subscribeRepos`).
Relay admin: basic auth `admin:admin`. PDS admin password: `admin-pass`.

When lastproto exists, the same driver runs once while *both* relays are
subscribed to the PDS, capture runs against indigo-WS / lastproto-WS /
lastproto-MoQ, and the three normalized captures must agree (modulo seq
numbering, which is per-relay).

The indigo commit is pinned via `INDIGO_COMMIT` in the Dockerfile
(`docker build --build-arg INDIGO_COMMIT=...` to override).
