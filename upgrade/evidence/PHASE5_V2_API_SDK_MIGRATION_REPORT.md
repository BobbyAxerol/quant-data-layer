# Phase 5 V2 API, SDK And Consumer Migration Report

## Conclusion

Phase 5 is complete and frozen in shadow mode. Quant Data Layer now has a
provider-neutral REST/gRPC V2 boundary, a Python SDK correctness boundary and
governed per-consumer migration without changing the running V1 authority,
legacy Redis contracts, venue subscriptions or production data.

There is no unresolved Phase 5 P0/P1 defect. Production HA transport, external
AuthN/AuthZ/TLS, secret-manager custody, OpenTelemetry operations and authority
cutover remain explicit Phase 6 activation gates rather than hidden Phase 5
debt. No consumer is `ACTIVE`; the two certified consumers remain `SHADOW`.

## Implemented

- Provider-neutral `/v2` instruments, snapshot, warmup, history, batch,
  feed-status, readiness and gap contracts with frozen OpenAPI and stable
  problem details.
- Canonical `instrument_uid` addressing for Binance and OKX. Provider identity,
  source role, authority and quality are response metadata, never URL routing.
- Strict server and SDK validation for source policy, entitlement, coverage,
  final bars, revisions, freshness, gaps and execution authority.
- gRPC server streaming with signed opaque cursors, durable replay, monotonic
  offsets, explicit `REPLAYING`, `LIVE`, `RATE_LIMITED` and recovery controls,
  bounded fanout and slow-consumer isolation.
- Cursor topology remains server-side. Removed public `stream` and
  `partition_key` inputs, reserved their Protobuf field numbers/names and
  preserved wire safety.
- Async/sync Python V2 query wrappers, atomic cursor persistence, explicit
  acknowledge, restart/reconnect/cursor-expiry recovery and a V1 facade.
- Audited `DataRequirement` manifests and PostgreSQL-backed migration states:
  `REGISTERED -> SHADOW -> ACCEPTED -> ACTIVE`, plus explicit rollback.
- Shadow reference alpha for OKX and execution-grade Trading System consumer
  for Binance, both using data-layer contracts without direct venue access.
- Aggregate V1/V2 usage and deprecation telemetry without strategy parameters
  or tick-level database writes.

Implementation commits:

- `78302e8` provider-neutral REST/query contracts.
- `9668e2e` resumable stream SDK.
- `9553377` shadow consumer and parity certification.
- `f234006` opaque topology, typed controls and policy hardening.

## Verification

| Gate | Result |
|---|---|
| Focused Phase 5 suite | 27/27 pass |
| Full Python/V1 regression | 251 run: 246 pass, 5 expected environment skips |
| OpenAPI | frozen snapshot matches generated schema; all 10 paths have typed success responses |
| Protobuf | Buf format, lint, baseline breaking and generated-code checks pass |
| Rust | format and Clippy `-D warnings` pass; 11/11 unit/parity tests pass |
| PostgreSQL | clean/existing/second apply pass; legacy row preserved; 20 QDL tables and 3 lease functions |
| Redis recovery | 3/3 pass; AOF restart/rebuild checksum identical; disposable DB size returns to zero |
| Dependency audit | no known vulnerabilities |
| API replica load | 8 replicas, 2,000 requests, concurrency 100, 317.45 req/s, p50 266.942 ms, p99 444.088 ms |
| Replica ownership | zero venue connection attempts and zero live-ingestion owners |
| Real Binance | read-only USD-M trade; price, quantity, trade ID and event-time canonical parity all true |
| Real OKX | 5 provider-authentic `BTC-USDT-SWAP` 1m rows; no cache/storage write |
| Runtime safety | running V1 health `ok`; authority unchanged; no production restart or write |

The first bounded load attempt exposed sync FastAPI handlers entering the thread
pool and failed the 500 ms p99 gate at about 677 ms. Query handlers were made
async, then the same gate passed. This failed attempt is retained here because
it explains the implementation decision and prevents benchmark cherry-picking.

Deterministic use cases cover partial batch, stale/gap/incomplete data,
unentitled source, reference fallback, execution fail-closed, final/revised
bars, duplicate suppression, process restart, transient reconnect, cursor
expiry, cursor-scope tampering, slow consumers, durable replay and V1/V2
canonical value parity.

Evidence:

- [`phase5-api-replica-load.json`](phase5-api-replica-load.json)
- [`phase5-real-provider-smoke.json`](phase5-real-provider-smoke.json)
- [`phase5-freeze.json`](phase5-freeze.json)

## Cleanup And Rollback

Disposable PostgreSQL, Redis containers, networks, temporary databases and
cursor files were removed by test cleanup traps. Rust compilation used a
container-local target directory. The isolated `data-layer:phase5-test` image is
removed after certification. User-owned `symbols.json` is not staged or changed
by Phase 5.

Rollback is per consumer: transition the manifest to `ROLLED_BACK`, keep V1 as
authority and retain V2 durable state for diagnosis. Since no consumer or feed
was promoted, phase closure requires no production rollback.
