# Pre-Phase 5 V2 Readiness Report

## Conclusion

Pre-Phase 5 is complete and frozen in **dark mode**. Phase 5 may now implement
provider-neutral REST/gRPC/SDK delivery over stable query, failure, entitlement
and handoff semantics. It must remain shadow until Phase 6 infrastructure,
security and authority decisions are approved.

No V2 endpoint was started. V1 remains authoritative and the running Data Layer
was not rebuilt or restarted.

## Cross-Phase Debt Closure Matrix

| Origin | Debt or gate | Disposition before Phase 5 |
|---|---|---|
| Phase 0 | Excess Spot + USD-M source topology | **CLOSED:** effective runtime is USD-M trade+kline only; Spot is disabled |
| Phase 0 | Ambiguous generic trade authority | **CLOSED for current runtime:** only USD-M producer is active; V2 uses canonical instrument identity |
| Phase 0 | Feed-agnostic queue drops | **SUPERSEDED for V2:** canonical lossless queue/backpressure is certified; V1 Pub/Sub remains explicitly best-effort and cannot provide cursor guarantees |
| Phase 0 | OKX cursor defect | **CLOSED:** exact-window pagination and real-provider coverage passed in Phase 4 |
| Phase 0 | Deprecated WebSocket API | **CLOSED in branch:** modern asyncio client plus real Binance frame passed; deployment remains coordinated |
| Phase 1 | Public V2 naming/semantics undecided | **CLOSED:** canonical requirement/error/coverage/recovery vocabulary frozen |
| Phase 1 | Buf compares only initial binary baseline | **CLOSED:** PR base-branch breaking check added while preserving immutable baseline |
| Phase 1 | Dark runtime roles/control tables | **EXPECTED:** Phase 5 uses dark roles; authority activation remains Phase 6 |
| Phase 2 | Python dependency advisories | **CLOSED:** final runtime audit 0/61; build tools removed from runtime |
| Phase 2/3 | SQLite spool is single-host/non-HA | **INTERFACE CLOSED, INFRA GATE:** handoff no longer depends on SQLite; Kafka-compatible HA remains Phase 6 evidence/approval |
| Phase 2 | OpenTelemetry/multi-node failover | **PHASE 6:** not required to define correct shadow API semantics |
| Phase 3 | Binance kline WS not live-certified | **CAPABILITY BOUNDARY:** closed-bar REST is certified; no false WS capability claim |
| Phase 3 | OKX VIP/deep-book not certified | **CAPABILITY BOUNDARY:** unavailable until entitlement and provider certification |
| Phase 4 | Snapshot/live boundary could be assembled ad hoc | **CLOSED:** exact snapshot cursor/watermark coordinator is mandatory |
| Phase 4 | HMAC key storage was in-memory-specific | **INTERFACE CLOSED:** rotation-aware provider added; production secret backend is Phase 6 |
| Phase 4 | Licensing/redistribution was implicit | **CLOSED semantically:** default-deny entitlement policy added; actual grants require business approval |
| Phase 4 | Object store/Iceberg production provisioning | **PHASE 6:** S3/Iceberg boundaries tested; no fake production authority claim |
| Phase 4 | Historical OKX OI | **NOT AVAILABLE:** remains truthful `SNAPSHOT_ONLY` pending a certified source |

## Implemented Readiness Boundaries

- `DataRequirement` and `BatchRequirement` enforce bounded inputs and strict
  execution-grade completeness/freshness/gap/authority behavior.
- One canonical error vocabulary is shared by future REST problem details, gRPC
  details and SDK exceptions. Old documentation names are aliases only.
- Entitlement policy separates source capability from legal/contractual access.
- Public handoff accepts only signed, scoped, expiring cursors. The unsigned
  logical cursor encoding is internal diagnostics only.
- Historical/live handoff depends on portable protocols and exact watermark
  equality, allowing SQLite today and an approved HA backbone later without API
  contract changes.
- Multi-stage image construction keeps Poetry and its dependency graph out of
  the final runtime and makes every runtime import an explicit project dependency.

## Verification

| Gate | Result |
|---|---|
| Phase 4.5 focused tests | 11/11 pass |
| Combined Phase 4 + 4.5 | 47/47 pass |
| Full Python/V1 regression | 224 run: 219 pass, 5 expected environment skips |
| Dependency audit | 61 runtime packages, 0 known findings |
| Final image content size | 163,007,213 bytes; 58.66% below running image |
| Redis restart/rebuild | 3/3 pass; checksum preserved; disposable DB cleaned |
| PostgreSQL migrations | clean/existing/second apply pass; legacy preserved; 16 tables, 3 lease functions |
| Buf contracts | format/lint/immutable-baseline breaking/codegen diff pass |
| Rust | fmt, Clippy `-D warnings`, 11/11 tests pass |
| Durable benchmark | 10,000 events; append 1,432.70/s; replay 8,266.96/s; p99 59.70 ms; 2.072x disk |
| Real provider | one Binance USD-M BTCUSDT trade frame; zero production writes |
| Running V1 | health, VN preload, Binance USD-M history HTTP 200; restart count 0 |
| Cleanup | no Phase 4.5 image/container/network; 520.3 MiB Cargo artifacts removed |

## Remaining Decisions, Not Phase 5 Implementation Defects

- Select and provision production HA durable transport when measured trigger and
  Phase 6 approval justify it.
- Provision production object store/Iceberg catalog and approve retention cost.
- Select production signing-secret backend and operational rotation owner.
- Record provider licensing grants before raw retention or redistribution.
- Approve authority promotion per venue/market/feed only after Phase 6 chaos,
  capacity, security and rollback certification.
- Historical OKX OI and VIP/deep-book remain unavailable until separately
  certified and entitled.

These gates block production authority claims, not implementation of shadow V2
endpoints in Phase 5. Phase 5 must expose their readiness/capability state and
fail closed rather than silently degrading guarantees.

## Rollback

No live rollback is required. Revert the Phase 4.5 commits to remove the dark
query/handoff contracts. The existing V1 process, Redis payloads, PostgreSQL,
Parquet, source subscriptions and consumer behavior remain authoritative.
