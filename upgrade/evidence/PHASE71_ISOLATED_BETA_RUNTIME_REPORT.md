# Phase 7.1 Isolated Beta Runtime Report

## Decision

Phase 7.1 is `PASS` on branch `feat/fund-grade-data-layer-v2` at implementation
commit `5e013be6431944f199beb5bc26de7d9e537006d5`. Phase 7 remains `IN_PROGRESS`:
7.2 consumer canary and 7.3 evidence freeze/beta decision are not started.

V1 remained authoritative and unchanged. No V1 process was restarted, no venue
socket or provider ownership changed, and no production Redis/history/cursor
state was written by beta.

## Implemented

- Added explicit `query_v2` and `stream_v2` runtime roles in one immutable,
  non-root image with separate entrypoints, state, audit paths and bounded
  resources.
- Selected active/passive stream ownership per gateway shard. A Redis lease
  holds one owner and a monotonic sink-visible fencing epoch; passive replicas
  report `STANDBY` and reject work.
- Replaced static readiness with bounded concurrent dependency probes for
  identity, manifest revision, query store, durable source, cursor signer,
  authority manifest, shared Redis quota and gateway lease.
- Replaced the per-replica beta request quota with an atomic Redis Lua quota.
  Redis failure returns dependency-unavailable; it never bypasses quota.
- Removed blocking replay/durable I/O from the event loop and removed the global
  handoff lock. Replay registration and live fan-out now share a per-partition
  barrier, closing the replay-to-live race while preserving cross-partition
  concurrency.
- Added bounds for request/query bytes, compressed bodies, deadlines, HTTP/RPC
  concurrency, subscriber count, outbound buffer, warmup quota and replay size.
- Added separate private Redis and beta ingress networks. Routes bind only to
  loopback; beta joins no V1, execution or provider network.
- Added reproducible Compose, environment example, runbook, CI gates and a
  topology smoke that always removes disposable beta state.

## Verification

| Gate | Result |
|---|---|
| Phase 7.1 runtime tests | `PASS`, 11/11 |
| Phase 7 contract/security tests | `PASS`, 11/11 |
| Phase 5/6/7 gateway and contract regression | `PASS`, 44/44 |
| Full Python regression | `PASS`, 296 tests, 5 conditional skips |
| Rust fmt/clippy/tests | `PASS`, warnings denied, 11/11 |
| Buf format/lint/breaking | `PASS`, Phase 1 and Phase 7 baselines |
| V2 OpenAPI digest | `PASS`, unchanged `bea44d...bcea` |
| Non-root immutable image | `PASS`, `qdl:qdl`, 172,606,741 bytes |
| JWT-protected real topology query | `PASS`, HTTP 200 |
| Active/passive takeover | `PASS`, epoch `1 -> 2` |
| Replay/live barrier | `PASS`, zero missing/duplicate logical offset |
| Cross-partition concurrency | `PASS`, blocked partition did not block peer |
| V1 topology rollback | `PASS`, IDs/restarts/networks/mounts unchanged |
| V1 Redis isolation | `PASS`, zero beta keys before/after |
| Beta cleanup | `PASS`, zero beta container/network/volume remaining |
| SBOM/release manifest verification | `PASS` |

The conditional Python skips are existing integration variants covered by
separate Docker topology, Buf, Rust and contract gates. Synthetic events were
used only inside unit tests; no generated market event entered provider or
production evidence.

## Frozen Artifacts

- Image: `sha256:e640b0e7afe54790ebe0d6ee240e8d45ec86a09b247411dd7d3087cb261a6bfb`
- OpenAPI: `bea44d3920db52f5893eb773aa195ae7f4abd2684d5ca65d904e995934fabcea`
- Phase 7 descriptor: `16686bb63dd2633f5572dca98baef1d7e1c3d9aec249a259a9725a58ad1445ef`
- SBOM: `bcb637f089fc752e34a141a82efbee617e84bf9d28f3ff0eb70fea803fe8a08e`
- Release manifest: `1c55507c4493bf2cc7fc3112a134696274c6ff8b64c6557c7c7e532ce062bb4d`

Machine-readable results are in `phase7-readiness-matrix.json`,
`phase7-cursor-handoff.json`, `phase7-topology-rollback.json` and
`phase71-release-bundle/`.

## Remaining Gate

Phase 7.1 intentionally activates no real consumer data source. The empty beta
query backend is structurally ready, while instrument data remains
`DATA_NOT_READY`. Phase 7.2 must register the reference monitoring consumer,
bind the approved canonical catalog/query source, then run the disposable paper
alpha parity and recovery matrix. Phase 7.3 must run capacity/security evidence,
revoke credentials, clean state and record `BETA-GO` or `BETA-NO-GO`.

This report does not authorize execution-only dependency, source-authority
promotion, anonymous access, V1 retirement or production durability claims.
