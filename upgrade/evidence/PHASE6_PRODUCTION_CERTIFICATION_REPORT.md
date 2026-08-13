# Phase 6 Production Certification Report

## Decision

Recorded on 2026-08-13 for branch `feat/fund-grade-data-layer-v2`.

| Decision layer | Result |
|---|---|
| Phase 6 implementation | `PASS` |
| Contract/domain/replay parity | `PASS` |
| Local shadow reliability certification | `PASS` |
| Real-provider read-only certification | `PASS` for the bounded scopes below |
| Production authority cutover | `NO-GO` |
| Existing V1 production authority | `UNCHANGED` |

The code and shadow path are ready to merge and deploy dark. They are not yet
authorized to replace V1 as production authority. Phase 6 deliberately does not
represent a local SQLite bridge, same-host chaos test, test signing key or
in-process telemetry buffer as replicated production infrastructure.

## Change Summary

- Added bounded correlation context, metric labels, SLO/error-budget evaluation
  and fail-closed canonical drop/completeness alerts.
- Added JWT identity validation, role/environment/venue-scoped RBAC, exact egress
  allowlists with private-IP blocking, payload/decompression bounds, secret
  redaction and hash-chained mutation audit records.
- Added deterministic failure simulation and capability-scoped venue
  certification. Test-generated market data remains confined to tests.
- Added process/broker/consumer/object-store/ownership/gap/reconnect recovery
  tests and independent Binance USD-M, OKX, DNSE and option-readiness gates.
- Added deterministic SPDX release bundles, immutable image references,
  checksums and a signing/verification rehearsal.
- Added dependency, license, secret, misconfiguration and container image gates.
  GitHub Actions are pinned by commit SHA.
- Moved the Python runtime image to fixed non-root UID/GID `10001` and upgraded
  final runtime `setuptools` to a non-vulnerable version. Added an idempotent,
  path-bounded host preflight for the `data/` and `logs/` bind mounts.
- Kept `/v1`, production Redis namespaces, running containers and source
  authority unchanged.

## Verification Results

| Gate | Result | Evidence |
|---|---|---|
| Full Python suite in final non-root image | `PASS` | 274 tests, 5 conditional integration skips |
| Phase 6 operations/security | `PASS` | 14/14 targeted tests |
| Recovery/multi-venue matrix | `PASS` | 43/43 history, quality, replay and Phase 6 tests |
| Rust 1.82 fmt/clippy/tests | `PASS` | 11 Rust tests; warnings denied |
| Protobuf/Buf | `PASS` | lint, frozen baseline breaking gate and generated-code diff |
| V1/OpenAPI compatibility | `PASS` | regenerated contracts produce no diff |
| Isolated Redis rebuild | `PASS` | seed/rebuild checksum equal; isolated DB cleaned |
| PostgreSQL migrations | `PASS` | clean, existing and idempotent migration paths; legacy rows retained |
| Python dependency audit | `PASS` | no known runtime vulnerabilities |
| Rust dependency/license audit | `PASS` | advisories, bans, sources and approved licenses |
| Container vulnerability scan | `PASS` | 0 unresolved HIGH/CRITICAL |
| Repository security scan | `PASS` | 0 secrets and 0 HIGH/CRITICAL misconfigurations |
| Signed release rehearsal | `PASS` | immutable digest, SPDX, checksums, RSA sign and verify |
| Existing production runtime | `UNCHANGED/HEALTHY` | V1 `health=ok`, restart count 0, OOM false |

The five conditionally skipped unit-discovery cases require optional app,
protobuf or isolated Redis dependencies. Their equivalent Docker/app/Buf/Redis
integration gates were run separately and passed.

## Capacity Evidence

The bounded local durable bridge test used 80 partitions and 5,000 events per
window:

| Window | Target | Achieved | p99.9 durable latency | Rejects |
|---|---:|---:|---:|---:|
| Normal 1 | 500 event/s | 500.20 event/s | 187.96 ms | 0 |
| Normal 2 | 500 event/s | 500.19 event/s | 183.78 ms | 0 |
| Burst | 1,500 event/s | 1,410.90 event/s | 314.26 ms | 0 |

Replay after restart matched, and traced memory did not grow between normal
windows. This certifies the local bridge implementation only. It is not evidence
for Kafka-compatible replication throughput or regional capacity.

## Real Provider Evidence

- Binance USD-M: authentic public trade frame reached the current service and
  exact event time, native trade ID, price and quantity canonical parity passed.
- OKX V5 JSON: real `BTC-USDT-SWAP` trade/mark/index candles, funding history and
  open-interest snapshot passed with explicit coverage metadata and zero writes.
- DNSE: real `VN30F1M` session on 2026-08-12 returned exactly 241 provider bars,
  with zero missing expected rows, outside-session rows or fabricated rows.
- No provider response was generated or seeded to pass a production-data gate.

## Chaos And Recovery Matrix

| Failure | Result | Boundary proven |
|---|---|---|
| Process restart | `PASS` | acknowledged durable offsets survive reopen |
| Transient durable sink failure | `PASS` | bounded retry occurs before acknowledgement |
| Slow consumer | `PASS` | isolated disconnect and replay from signed cursor |
| Redis loss/rebuild | `PASS` | deterministic isolated rebuild checksum |
| Projector/checkpoint replay | `PASS` | duplicate-safe cursor recovery |
| Object-store commit failure | `PASS` | head does not advance; orphan cleanup is explicit |
| Lease expiry/owner failover | `PASS` | old epoch is fenced |
| Gap/duplicate/out-of-order/malformed | `PASS` | quality blocks execution until recovery |
| OKX reconnect/maintenance | `PASS` | make-before-break; old-generation frames rejected |
| Regional DR | `BLOCKED` | requires independent replicated infrastructure |

## Section 41 Acceptance Matrix

`PASS` means code plus appropriate test evidence. `BLOCKED` means the contract is
implemented but production infrastructure/operator evidence is absent.

### Contracts And Identity

- `PASS`: generated Python/Rust Protobuf, Buf breaking gate, collision-safe
  venue/market/product identity, fixed-point/time/nullability semantics.
- `BLOCKED`: complete alias-history import and review for every production
  instrument, rather than the certified selected universe.

### Ingestion And Durability

- `PASS`: durable-before-projection contract, no-silent-drop backpressure,
  lease/fencing and reconnect/session recovery in local/shadow tests.
- `BLOCKED`: replicated broker producer idempotence, acknowledgements and
  replication are not deployed. SQLite is a bounded bridge, not the target log.

### Data Quality

- `PASS`: duplicate/out-of-order/gap/quarantine, source role/authority and
  execution eligibility are explicit and tested.
- `BLOCKED`: production reconciliation dashboards and alert routing are not
  active in an OpenTelemetry backend.

### Historical

- `PASS`: immutable Parquet objects, atomic manifest head, cursor-aligned
  materialization, explicit revisions and local rollback/recovery.
- `BLOCKED`: shared object store/Iceberg catalog, lifecycle policy, PITR and
  independently restored production snapshot have not been deployed/rehearsed.

### APIs And SDK

- `PASS`: V1 golden compatibility, provider-neutral V2 typed API, signed cursor,
  snapshot-plus-cursor handoff, persistence/recovery and reference consumers.
- `BLOCKED`: registration and cutover evidence for every critical production
  alpha/execution/research consumer.

### Operations

- `PASS`: liveness/readiness/data-readiness separation, bounded telemetry/SLO
  policy, local chaos/load evidence, Redis rebuild and recovery runbook.
- `BLOCKED`: active collector/dashboards/pages and regional DR rehearsal.

### Security And Governance

- `PASS`: fail-closed identity/RBAC library, egress/SSRF and payload controls,
  mutation audit, secret redaction, non-root image, clean scans and signed-bundle
  rehearsal.
- `BLOCKED`: production network policy, workload identity, Vault/KMS-backed
  rotation, registry signature admission and entitlement/retention approval.

### Migration

- `PASS`: Rust/provider fixture parity, V1 projector compatibility and bounded
  consumer rollback rehearsal in shadow.
- `BLOCKED`: dedicated production roles, full consumer cutover and owner-based
  legacy sunset. The combined V1 runtime remains intentionally authoritative.

## Adapter Definition Of Done

| Scope | Adapter conformance | Production authority |
|---|---|---|
| Binance USD-M selected TRADE | `PASS` | `BLOCKED` by shared infrastructure gates |
| OKX V5 JSON SWAP reference/history | `PASS` | `BLOCKED` by profile/credentials/cutover gates |
| DNSE `VN30F1M` BAR | `PASS` read-only | V1 remains authoritative |
| OKX SBE | `BLOCKED` | needs entitlement, pinned schema and JSON parity |
| Deribit-style option identity/book fixture | `CORE READY` | adapter/source activation not performed |

For the certified scopes, capability descriptors, identity, native precision,
timestamps/sequences, reconnect, rate behavior, malformed/gap behavior,
canonical schemas, quality state, source policy, rollback, bounded performance
and runbooks have evidence. Unsupported capabilities fail independently and do
not degrade certified core feeds.

## Production Blockers And Next Approval

1. Deploy a replicated Kafka-compatible canonical log and prove broker failover,
   replication and restore on the actual topology.
2. Deploy OTel collector, dashboards, alerts and paging; obtain SLO approval.
3. Deploy workload identity/RBAC/network policies, external secrets and rotation.
4. Publish a production-registry signed image and enforce signature admission.
5. Rehearse PITR and regional DR on independent infrastructure.
6. Register all critical consumers and cut over one bounded feed slice through
   `SHADOW -> CANARY -> PRIMARY`, with an operator-approved rollback.
7. Sunset broad Spot/combined V1 producers only after demand telemetry is zero.

Until those gates pass, the correct decision is to merge the Phase 6 code,
optionally deploy it dark/shadow, and keep V1 authoritative.

## Cleanup And Runtime Safety

All provider checks were read-only. Isolated Redis/PostgreSQL test resources and
temporary signing keys were removed after their checks. No production Redis key,
Parquet file, cursor, consumer group, volume or container was mutated. The final
local test image and scanner cache are removed after report freeze.
