# Phase 9.0-B Isolated V2 Beta Report

Decision: `PASS_ISOLATED_NO_AUTHORITY_CUTOVER`

## Scope

The existing V2 query/stream beta was re-certified from the Phase 9.0-A
correctness baseline. V1 remained source and public authority. The candidate
used an isolated Redis, durable spool, credentials, consumer group, loopback
ports and Compose project. No Rust canary or public-internet exposure occurred.

## Results

- Authentic slice: `BINANCE / USDM / PERPETUAL / BTCUSDT / BAR / 1m`.
- Canonical mismatches: `0`; generated events:
  `0`; duplicate open times:
  `0`.
- Continuous bridge watermark delta across the bounded observation window:
  `0`.
- Active/passive stream replay: `3` fast consumers,
  `1` isolated slow consumer, contiguous=
  `True`.
- Query normal: `30` requests, p99.9
  `72.211` ms, `0` errors.
- Query burst: `60` requests, p99.9
  `486.649` ms, `0` errors.
- Peak candidate RSS: `55658414.08` bytes; peak CPU:
  `33.36%` of one core.
- Security/adversarial, cursor, failover/fencing, dependency outage/recovery,
  rate limit, oversized request and cleanup gates all passed.

## Safety And Cleanup

Candidate revision: `1c881389b4ee21a153903505822c61512b176044`. Runtime user was
`10001:10001`, root was read-only, no source bind was mounted and bridge access
was restricted to beta-internal plus the existing V1 internal network.
Production V1 topology/OpenAPI remained unchanged. Candidate containers,
networks, volumes and candidate tags after cleanup: `0/0/0/0`; production beta
keys: `0`.

## Remaining Boundary

This result permits review of an isolated read-only V2 beta only. Phase 9.1
remains blocked on replicated production transport, OTel/alerts, workload
identity, external secrets, signature admission, independent DR, complete
consumer registration and explicit exact-slice authority approval.
