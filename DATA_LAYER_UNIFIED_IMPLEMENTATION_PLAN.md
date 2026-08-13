# Quant Data Layer Unified Implementation Plan

> **Status:** Proposed for user approval. No implementation or runtime cutover has started.
> **Working branch:** `feat/fund-grade-data-layer-v2`, created from `dev`.
> **Detailed architecture:** [Fund-grade architecture and migration guide](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md)
> **OKX V5 market-data specification:** [OKX Market Data V5 implementation guide](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md)
> **Compatibility boundary:** Existing `/v1`, SDK v1, Redis keys and Redis Pub/Sub remain supported until a governed per-consumer sunset.

## 1. How To Use This Tracker

This file is the tracked implementation journal for the fund-grade data-layer upgrade. The architecture guide owns detailed design and rationale; this file owns execution order, status, evidence, decisions and remaining debt.

Every phase must keep the following fields current:

- **Goal:** the measurable outcome, not merely files to create.
- **Guide index:** links to the detailed architecture sections that govern implementation.
- **To do:** approved scope for the phase.
- **Completed:** exact code, configuration, migration and operational work actually performed.
- **Verification:** commands, fixtures, data comparisons, latency/capacity results and cleanup evidence that actually ran.
- **Technical debt / decision gate:** only unresolved matters that require a user, infrastructure, cost or business-semantics decision. In-scope defects are fixed before phase closure, not relabeled as debt.
- **Rollback:** a tested path back to the last authoritative producer/read path.

Phase status is one of `PLANNED`, `IN_PROGRESS`, `BLOCKED`, `COMPLETE`. A phase is not `COMPLETE` while a required gate is untested or while test artifacts affect live data.

## 2. Program-Wide Rules And Invariants

These rules apply to all seven phases.

1. **No big-bang cutover.** Use strangler migration, shadow reads/writes, parity reports and per-feed authority flags as defined in [Sections 30-33](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#30-migration-strategy-no-big-bang-rewrite).
2. **V1 remains stable.** Existing alpha and Trading System consumers must not change merely because internal transport, schemas or implementation language changes. Protect `/v1`, SDK v1 and legacy Redis payloads with golden tests.
3. **One canonical contract across languages.** Python and Rust generate types from the same Protobuf/OpenAPI sources. External projects are behavioral references, not naming authorities for established data-layer fields.
4. **Correctness before throughput.** Validate exact decimal representation, venue/market/instrument identity, source/event/receive timestamps, sequence semantics, bar closure, provenance and quality flags before accepting performance results.
5. **No silent loss.** Trade and order-book delta queues must never use “drop oldest and continue healthy”. Backpressure, spool, reconnect, resnapshot and feed-state transitions must be explicit and observable.
6. **Demand controls cost, capability controls architecture.** Unused Spot feeds are disabled by configuration and zero-demand evidence, not deleted. A new consumer can re-enable them through a reviewed `DataRequirement` without code changes.
7. **Multi-venue by capability.** Core code must not route with growing `if venue == ...` branches. Market types include spot, equity, perpetual, dated futures, option and index/reference products. Future Deribit support must fit the existing instrument/event/order-book contracts.
8. **Rust is promoted by evidence.** Rust owns approved hot paths only after shadow parity, replay determinism, failure recovery and capacity gates pass. Python remains the control/query/history authority where it is the better fit.
9. **Tests are isolated and cleaned.** Use deterministic venue fixtures and disposable Compose project names, topics, Redis prefixes, PostgreSQL schemas and object-store buckets. Do not flush shared Redis, alter production parquet, or reuse live consumer groups. Remove test resources and report cleanup after each phase.
10. **Real-provider tests are bounded.** Read-only venue smoke tests use small symbol/feed sets, respect rate limits and never seed/bypass missing data. They supplement deterministic tests; they do not replace them.
11. **Evidence is concise and durable.** Store checksums, counts, latency percentiles, gap/duplicate results and compact report files. Do not paste unbounded logs into this plan.
12. **Commit discipline.** Commit one coherent, tested implementation slice at a time with the configured BobbyAxerol identity. Do not bundle unrelated `symbols.json`, local data, logs or caches. Open PRs into `dev`; promote to `main` only through release gates.
13. **New debt is governed.** Fix in-scope bugs during the phase. Stop and request direction only for a material architecture, infrastructure-cost, licensing, source-authority or public-contract decision.
14. **Provider guides refine, not fork, the platform.** OKX `P0-P4` work follows the seven-phase mapping in its guide. Public V2 remains provider-neutral; provider routes are authenticated diagnostics/control-plane only. Provider docs and changelog are re-verified for every touched endpoint/channel and the verification date is recorded.
15. **Running consumers are protected by default.** Development, fixtures, load tests and shadow producers use isolated process/container names, ports, Redis prefixes, consumer groups, schemas and output paths. No phase may restart, reconfigure, flush, prune, overwrite or redirect the running producer/consumer path unless an approved cutover step explicitly names the blast radius and rollback.
16. **Source changes require a coordinated release plan.** A new producer/source remains shadow until contract, domain parity, freshness, recovery and capacity gates pass. Authority changes use immutable artifacts and one versioned deployment manifest so every owner for the selected feed slice changes consistently; partial mixed ownership is prohibited. Consumer migration remains per declared manifest and does not require a big-bang V1 sunset.
17. **Testing covers behavior, not only availability.** Each slice runs applicable unit, contract/golden, deterministic replay, domain-oracle, integration, failure/reconnect, compatibility, resource/capacity and bounded real-provider checks. Reports state cases run, exact results, untested cases and cleanup evidence. A healthy HTTP response alone is never phase acceptance.
18. **Correctness, stability and scalability are release gates.** No optimization is promoted if it changes identity, units, timestamps, ordering, bar closure, source authority or legacy behavior without an approved versioned contract. No benchmark is accepted without zero unexplained loss/duplicate/gap and bounded CPU, memory, disk, queue and lag under measured load plus headroom.

## 3. Phase Summary

| Phase | Name | Primary outcome | Initial status |
|---:|---|---|---|
| 0 | Containment, inventory and measurable baseline | Freeze compatibility, stop unused cost and establish reproducible truth | `PLANNED` |
| 1 | Canonical contracts, identity and runtime boundaries | Stable venue-neutral domain plus separately scalable Python roles | `PLANNED` |
| 2 | Durability contract, bridge and Rust foundation | Replayable transport boundary and deterministic cross-language core without premature broker cutover | `PLANNED` |
| 3 | Scalable ingestion and compatibility projection | Demand-driven Rust hot path with legacy V1/Redis parity | `PLANNED` |
| 4 | Quality, history, replay and gap-free handoff | Certified data products from warmup through live recovery | `PLANNED` |
| 5 | V2 API/SDK and controlled consumer migration | Stable snapshot/cursor interface without breaking existing consumers | `PLANNED` |
| 6 | Production certification and multi-venue readiness | HA/security/SLO gates, controlled authority cutover and adapter scalability | `PLANNED` |

## 4. Phase 0 - Containment, Inventory And Measurable Baseline

**Status:** `PLANNED`

### Goal

Create a trustworthy, reproducible baseline before changing transport or schemas; freeze V1 behavior; identify every real consumer; and stop broad Spot streaming only when no active/declared consumer requires it.

### Guide Index

- [Phase 0 detailed guide index: Sections 2, 4, 24, 27, 30-31 and Epic E0](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-0)
- [OKX Phase 0 workstream: compatibility inventory, fixtures, profiles and known pagination defect](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md#okx-program-phase-0)

### To Do

- Inventory all REST routes, request/response examples, SDK public methods, Redis keys/channels, source/market semantics and known consumers in alpha, Trading System and diagnostics.
- Snapshot V1 OpenAPI and legacy Redis payloads as golden compatibility artifacts. Record owner, criticality, freshness, warmup and fallback requirements per consumer.
- Measure current host/runtime baseline: enabled feeds, shard count, messages/s, queue depth/drop delta, reconnects, Redis commands/memory/network, API latency, CPU/RSS, parquet coverage and provider REST pressure.
- Add a deterministic baseline corpus for Binance USD-M, Binance Spot, OKX and VN payloads, including malformed, duplicate, out-of-order, reconnect and market-closed cases.
- For OKX, freeze `/v1/crypto/ohlcv/okx/...`/SDK output, capture exact native `instId` and REST/WS profile fixtures, and characterize the current `after`/`before` defect before correcting it in an approved implementation slice.
- Separate `liveness`, service `readiness`, feed readiness and execution eligibility in reports without changing V1 response shape.
- Add explicit source/market feature flags and validate configuration at startup. Remove duplicate/hardcoded universe overrides from runtime ownership.
- Prove Spot demand from declaration plus telemetry. If demand is zero, set Spot ingestion disabled by default while preserving Spot REST wrappers, adapter code and a tested re-enable path.
- Establish test-resource namespaces and cleanup commands for broker topics, Redis, PostgreSQL and object storage before those dependencies are introduced.
- Record baseline budgets and acceptance thresholds used by later phases. Thresholds must reflect measured load plus agreed headroom, not arbitrary aspirational numbers.

### Verification And Exit Gate

- Golden `/v1`, SDK and Redis compatibility tests pass against the current implementation.
- Consumer inventory covers every observed and declared caller; unknown callers are listed and block destructive contract changes.
- Two bounded runtime windows show demand telemetry, feed freshness, queue/drop deltas, reconnect behavior and resource usage.
- Spot-off configuration starts without Spot WebSocket shards, leaves USD-M/VN demanded feeds healthy, reduces resource use measurably and can be rolled back with one configuration change.
- Existing unit suite, Docker integration suite and read-only sampled provider smoke pass; generated test state is cleaned.
- Baseline report and machine-readable artifacts are committed without credentials or raw unbounded logs.

### Completed

- Not started.

### Technical Debt / Decision Gate

- Confirm measured production headroom target after baseline results. No infrastructure purchase is assumed in this phase.
- Any consumer that depends on undeclared Spot Pub/Sub must be registered before Spot can remain disabled.

### Rollback

- Restore the previous feed-enable configuration and immutable image. No schema, topic or consumer cutover occurs in Phase 0.

## 5. Phase 1 - Canonical Contracts, Identity And Runtime Boundaries

**Status:** `PLANNED`

### Goal

Define one precise, venue-neutral data domain and split the combined process into independently scalable roles while preserving all V1 behavior.

### Guide Index

- [Phase 1 detailed guide index: Sections 8-10, 12, 20-23, P1 and Epics E1-E3](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-1)
- [OKX Phase 1 workstream: canonical identity, authoritative registry and capability profiles](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md#okx-program-phase-1)

### To Do

- Create versioned Protobuf packages and Buf configuration for common envelope, instrument, trade, BBO/quote, bar, order-book snapshot/delta, quality and feed-state events.
- Preserve price/quantity as exact decimal coefficient/scale or venue-native string; prohibit canonical binary float.
- Implement canonical `instrument_uid`, aliases, venue, source/provider, market/product type, contract expiry, strike, option type, multiplier, tick/lot metadata and trading calendar/session model.
- Define capability descriptors for REST history, trades, BBO, bars, L2 snapshots/deltas, sequence/checksum, resubscribe/resnapshot and source authority. Ensure options and dated contracts need no core schema redesign.
- Model OKX Spot, Swap, dated Futures, Options and Event contracts from `/public/instruments`; never derive a derivatives `instId` with string heuristics. Region/entity/tier availability is explicit capability metadata.
- Build PostgreSQL migrations for instrument master, aliases, source policy, subscription registry, config revisions, leases/fencing and job state. Do not store the tick stream in PostgreSQL.
- Extract deployable Python roles (`api`, `control`, `history`, compatibility facade) from the current combined lifespan. A role flag must have one owner and fail startup on contradictory ownership.
- Keep legacy imports and application entrypoints through explicit facades. API replicas must not create venue subscriptions.
- Write ADRs for contract representation, instrument identity, role boundaries, transport decision inputs and V1 compatibility ownership.

### Verification And Exit Gate

- Buf lint/generation and breaking checks pass; generated Python and Rust types encode/decode the same golden bytes.
- Exact decimal, timestamp, event-ID and identity collision suites pass for spot, perpetual, dated futures, VN derivative and option fixtures.
- Role-topology integration test proves scaling API replicas does not increase provider connections or duplicate publication.
- V1 OpenAPI/Redis/SDK golden artifacts are byte/semantic compatible with Phase 0.
- Schema migrations are forward-only, idempotent in clean and existing DB cases, and have backup/rollback evidence.
- Static dependency tests prevent API/control/history modules from importing and starting ingestion ownership accidentally.

### Completed

- Not started.

### Technical Debt / Decision Gate

- Contract naming or semantics that affect public V2 behavior require explicit approval before schema freeze. Pure implementation details do not.

### Rollback

- New roles and schemas remain dark. Existing combined runtime stays authoritative until a later per-feed cutover.

## 6. Phase 2 - Durability Contract, Bridge And Rust Foundation

**Status:** `PLANNED`

### Goal

Introduce a transport-neutral replay contract, a bounded durable bridge and a deterministic Rust data-plane foundation without making Rust or a Kafka-compatible broker authoritative prematurely.

### Guide Index

- [Phase 2 detailed guide index: Sections 6-7, 11, 28-29 and Epics E4-E5](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-2)
- [OKX Phase 2 workstream: durable fixtures, simulator and cross-language deterministic parity](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md#okx-program-phase-2)

### To Do

- Define transport-neutral `EventSink`, `EventSource`, cursor/checkpoint, event-ID, retry and replay contracts before selecting infrastructure. Application and public contracts must not expose Redis- or Kafka-specific identifiers.
- Implement a bounded bridge for the first isolated feed slice using a dedicated durable Redis Streams instance or local WAL/spool. It must have persistence, `noeviction`, strict memory/disk bounds, trimming, cursor-expiry behavior, monitoring and a tested cleanup/sunset path; the existing ephemeral `redis_marketdata` is forbidden for this role.
- Measure bridge throughput, replay horizon, consumer-group count, lag, memory/disk amplification and operational recovery. Provision a Kafka-compatible broker only when the promotion gate demonstrates a real need; keep producer/consumer contracts Kafka-compatible from the start.
- When the Kafka gate is approved, provision isolated raw, canonical, quality and DLQ/quarantine topics with explicit partition keys, retention, replication, quotas and ACLs. A single-node broker is a replay/durability step, not an HA claim.
- Implement idempotent publication, deterministic event IDs, retry classification, bounded local spool and feed-state transition when durable commit is unavailable.
- Create a Cargo workspace for contract types, decimal/time utilities, instrument identity, event IDs, adapter traits, broker client, telemetry and replay test tools.
- Implement cross-language golden codecs/checksums and a deterministic venue simulator reusable by Python and Rust.
- Include OKX REST envelopes and WS book snapshot/update/gap/keepalive/maintenance-reset/connection-generation frames in the same durable simulator; no separate OKX event backbone is introduced.
- Build a shadow raw-to-canonical pipeline for one small, demand-backed slice. The provisional slice is selected Binance USD-M trade symbols, not broad Spot.
- Keep Redis as latest-state/legacy projection only; prove it can be rebuilt from canonical events.
- Add CI gates for formatting, lint, tests, unsafe-code policy, dependency/license/security audits, Buf compatibility and reproducible container artifacts.

### Verification And Exit Gate

- Restart/recovery tests for the selected bridge lose no acknowledged canonical events and do not expose non-idempotent duplicate state. If Kafka is promoted in this phase, broker restart/failover tests are additionally mandatory.
- Same raw fixtures and config/normalizer revision produce identical canonical checksums across repeated replay and across Python/Rust reference implementations.
- Slow/down broker tests prove spool bounds, backpressure and `DEGRADED/BLOCKED` semantics; no silent queue drop is allowed.
- Redis flush/restart followed by replay rebuilds the same latest-state checksum and legacy projection fixture.
- Performance baseline reports p50/p95/p99/p99.9, throughput, CPU, RSS, allocation and disk/network amplification using semantically identical parsing.
- All ephemeral topics, groups, Redis prefixes and volumes created by tests are removed after evidence capture.

### Completed

- Not started.

### Technical Debt / Decision Gate

- Kafka promotion requires explicit approval after measured evidence shows at least one material trigger: multiple independent replay consumers, replay horizon beyond the bounded bridge, raw trade/book volume exceeding its safe budget, multi-node HA requirement, or unacceptable bridge lag/recovery time.
- Until that gate passes, the dedicated bridge/local spool is transitional infrastructure with a declared limit and sunset path, not the canonical long-term target.

### Rollback

- Stop shadow Rust/broker services and remove their isolated resources. Existing Python-to-Redis path remains authoritative.

## 7. Phase 3 - Scalable Ingestion And Compatibility Projection

**Status:** `PLANNED`

### Goal

Run high-throughput ingestion and canonical projection with explicit shard ownership, demand-driven subscriptions and V1-compatible outputs, initially in shadow and then per-feed authority slices.

### Guide Index

- [Phase 3 detailed guide index: Sections 12-14, 20, 23, 37, P2 and Epics E6-E8](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-3)
- [OKX Phase 3 workstream: async ingestion, public/business WS and order-book state machine](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md#okx-program-phase-3)

### To Do

- Implement Rust Binance USD-M adapter for demanded trade/BBO/bar feeds using capability-based interfaces, validated instrument discovery and exact native sequence/time/decimal extraction.
- Replace configured broad-universe ownership with declared baseline plus TTL demand leases; implement lease/fencing epochs so only one active owner publishes a shard.
- Keep Spot disabled when registry refcount is zero. Add conformance tests showing Spot can be enabled without changing code or namespaces.
- Implement bounded queues by feed class: lossless backpressure/spool for trade/book delta; explicit coalescing only for latest-state-safe projections.
- Add reconnect/resubscribe, rate-limit budget, jittered backoff, heartbeat, gap detection and REST snapshot/resync appropriate to each feed.
- Implement OKX endpoint-bucketed async REST and public/business WS supervisors. True book-sequence gaps invalidate executable state and require a fresh WS snapshot; REST `/books` must not be used as a fictitious delta bridge.
- Implement canonical Redis projector plus V1 compatibility projector with checkpointed idempotence, versioned keys and legacy payload snapshots.
- Shadow-compare Python and Rust on event count, IDs, price, quantity, side, timestamps, sequence, bar closure and quality flags before authority changes.
- Provide adapter extension fixtures for OKX and a synthetic Deribit-style option/order-book source to prove capability boundaries without claiming those venues production-ready.

### Verification And Exit Gate

- Adapter conformance suite passes normal, malformed, duplicate, out-of-order, gap, reconnect storm, delist and graceful shutdown cases.
- Two-owner fencing test proves stale owner cannot publish after lease loss.
- Burst and sustained load at measured universe size plus headroom produces zero silent loss, bounded memory and controlled broker/Redis lag.
- Python-versus-Rust shadow reports meet exact-field parity; every allowed divergence is versioned and approved.
- Legacy V1/Redis consumers observe no shape, namespace or source-authority regression.
- One low-risk feed slice can switch authority and roll back without restarting unrelated venue/feed shards.

### Completed

- Not started.

### Technical Debt / Decision Gate

- Venue-specific sequence/checksum limitations must be documented in capability metadata and certification, not hidden with generic assumptions.

### Rollback

- Per-feed authority flag returns publication to Python. Rust remains shadow; compatibility projector checkpoints permit deterministic recovery.

## 8. Phase 4 - Quality, History, Replay And Gap-Free Handoff

**Status:** `PLANNED`

### Goal

Produce auditable, replayable and revision-aware data from raw ingestion through historical warmup and live continuation, with no undetected gap or duplicate at the handoff.

### Guide Index

- [Phase 4 detailed guide index: Sections 13-16, 38 and Epics E7/E9](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-4)
- [OKX Phase 4 workstream: historical pagination, reference data coverage and handoff](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md#okx-program-phase-4)

### To Do

- Implement canonical validation levels, duplicate/out-of-order handling, sequence/gap ledger, clock-discipline metrics and failover/source-authority state machine.
- Preserve raw source payload and lineage for governed retention; quarantine malformed/unknown-instrument events instead of coercing invalid values.
- Add S3-compatible object storage and Iceberg/Parquet materialization with immutable data files, atomic snapshots, schema evolution and compaction governance.
- Migrate existing VN canonical 1m and derived intervals without fabrication; preserve session calendars, origin, revision and sparse-market semantics.
- Materialize crypto history/replay only where demand/cost evidence requires it; retain direct bounded REST wrappers for ordinary warmup.
- Implement snapshot plus durable cursor/watermark protocol, cursor persistence and reconnect replay so warmup transitions to live without gap or duplicate.
- Add historical/live reconciliation, bar revision behavior and replay determinism reports keyed by source/config/normalizer versions.
- For OKX, correct `after`/`before` traversal with exact window filtering/dedup/no-progress guards; expose explicit funding/mark/index/OI provenance and coverage. Current OI snapshots never imply pre-ingestion historical coverage.

### Verification And Exit Gate

- OHLCV oracle tests verify first/max/min/last/sum rules, market-session boundaries, DST/time zones, daily close and late/revised bars.
- Crash tests at file upload/metadata commit boundaries expose either old or new Iceberg snapshot, never partial state.
- Snapshot-cursor tests cover cold start, reconnect, cursor expiration, compaction boundary, late event and consumer restart.
- Historical/live overlap checks produce zero unexplained gaps/duplicates for certified feeds.
- Raw replay produces deterministic canonical checksums and traceable lineage to historical rows and Redis latest state.
- VN real-provider bounded checks distinguish market closed/sparse/late data from outage without synthetic seeding.

### Completed

- Not started.

### Technical Debt / Decision Gate

- Object-store/catalog deployment and retention cost require approval before production provisioning; local MinIO/catalog remains a test implementation only.
- Provider licensing constraints must be recorded before raw retention is enabled for a new source.

### Rollback

- Existing VN Parquet/read path remains available until shadow snapshot reconciliation passes. Historical authority switches per dataset, never globally.

## 9. Phase 5 - V2 API, SDK And Controlled Consumer Migration

**Status:** `PLANNED`

### Goal

Expose provider-neutral V2 snapshot/query/stream contracts and migrate consumers one `DataRequirement` at a time while V1 remains fully operational.

### Guide Index

- [Phase 5 detailed guide index: Sections 17-19, 24-25, 32, P2/P3 and Epics E10-E13](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-5)
- [OKX Phase 5 workstream: provider-neutral V2, internal diagnostics and controlled migration](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md#okx-program-phase-5)

### To Do

- Implement provider-neutral V2 REST snapshot/batch/history endpoints, typed errors, explicit quality/provenance and partial-batch semantics.
- Serve OKX through those provider-neutral contracts using canonical `instrument_uid`; keep capability/status/subscription reconcile routes internal rather than making alpha code depend on `/providers/okx` paths.
- Implement gRPC server-streaming with snapshot cursor, durable replay, backpressure status and consumer telemetry.
- Generate Python SDK V2 from canonical contracts; provide sync/async wrappers, cursor persistence, freshness/source validators and V1 compatibility facade.
- Make `DataRequirement` manifests the audited source for subscription, startup readiness, fallback, revision and warmup needs.
- Integrate one reference alpha-grade and one execution-grade consumer in shadow. Trading System must not need Kafka knowledge.
- Validate current alpha runtime and Trading System V1 behavior unchanged; migrate only explicitly selected consumers after parity evidence.
- Publish deprecation telemetry and owner notifications, but do not remove V1 or legacy Redis in this phase.

### Verification And Exit Gate

- OpenAPI/Buf breaking gates and generated-client tests pass in CI.
- End-to-end tests cover venue simulator -> durable log -> canonicalizer -> Redis/V1 and gRPC/V2 -> reference consumers.
- Consumer restart, slow consumer, cursor expiration, fallback activation and revised-bar cases produce documented deterministic behavior.
- V1 and V2 shadow responses match canonical values and differ only in declared metadata/versioning.
- Selected alpha and Trading System consumers pass warmup, latest, stream, reconnect and freshness tests without direct venue connections.
- Load tests prove API replicas scale independently and do not multiply ingestion connections.

### Completed

- Not started.

### Technical Debt / Decision Gate

- No consumer is forced to migrate without owner acceptance. Sunset dates are a separate governed decision based on telemetry.

### Rollback

- Move the selected consumer manifest back to V1/legacy projection. V2 and durable state remain available for diagnosis; no shared contract reversion is needed.

## 10. Phase 6 - Production Certification And Multi-Venue Readiness

**Status:** `PLANNED`

### Goal

Certify production reliability, security, resource efficiency and operational recovery; complete controlled authority cutover for approved feeds; and prove the architecture can add OKX, DNSE/VN and future Deribit options without core redesign.

### Guide Index

- [Phase 6 detailed guide index: Sections 25-29, P3, 34-35, 37-41 and Appendix B](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-6)
- [OKX Phase 6 workstream: profile-aware certification, P4 products and optional SBE/Rust promotion](upgrade/OKX_MARKET_DATA_V5_GUIDE_QUANT_DATA_LAYER.md#okx-program-phase-6)

### To Do

- Add OpenTelemetry traces/metrics/log correlation, role/feed dashboards, error budgets and actionable alerts for connection, lag, gaps, quality, projection, history and consumers.
- Enforce network zones, control-plane AuthN/AuthZ, secret handling, egress allowlists, SSRF/payload limits, audit logs and supply-chain/container policies.
- Run chaos matrix: process kill, broker failover, Redis loss/rebuild, projector checkpoint boundaries, DB/object-store outage, network partition, malformed frames, reconnect storm and slow consumers.
- Run sustained and burst performance/soak against measured production load plus headroom; verify CPU/RSS/disk/network and no monotonic memory/lag growth.
- Certify Binance USD-M first; certify OKX and Python VN adapters independently using the common conformance suite and explicit source-authority policies.
- Certify OKX JSON core feeds before any SBE promotion. Tier/profile products fail independently; SBE requires pinned schema/version, JSON shadow parity, unknown-schema fail-closed behavior and tested JSON rollback.
- Prove option readiness with instrument discovery, expiry/strike/call-put identity and order-book snapshot/delta/checksum fixtures representative of Deribit. Actual Deribit production activation remains a separate adapter certification, not a core rewrite.
- Cut over authority per venue/market/feed/hash range only after shadow parity and rollback rehearsal. Keep V1 compatibility projector until registered consumer sunset criteria are met.
- Remove obsolete combined-runtime producers and unused broad Spot runtime only after ownership, consumer and rollback gates pass; retain reusable adapter capability.
- Produce immutable artifacts, SBOM/provenance, release notes, runbooks and DR evidence; clean all test infrastructure and generated state.

### Verification And Exit Gate

- Every item in [Section 41 production acceptance checklist](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#41-production-acceptance-checklist) and Appendix B has evidence or an explicitly approved non-applicable rationale.
- Certified feeds meet correctness, freshness, gap, durability, replay, latency and recovery SLOs under normal, burst and failure tests.
- Security scans and AuthN/AuthZ/egress/secret-redaction tests meet policy with no unresolved critical/high issue.
- Capacity report demonstrates safe headroom on the current host or supplies a measured scale-out requirement before cutover.
- Rollback rehearsal restores the previous authoritative producer without data ambiguity.
- Test topics, consumer groups, schemas, Redis prefixes, buckets, volumes and containers are removed; production state remains untouched.

### Completed

- Not started.

### Technical Debt / Decision Gate

- Actual Deribit, additional options vendors or regional HA are separate production activations requiring credentials, licensing, capacity and source-semantics approval. The core architecture must already support them.
- V1/legacy Redis removal is not part of automatic Phase 6 closure; it requires zero-consumer telemetry and an approved sunset release.

### Rollback

- Authority flags roll back per feed/partition to the last certified producer. Durable cursors and canonical data remain available for reconciliation.

## 11. Approval Gate Before Implementation

Implementation begins only after the user approves this seven-phase decomposition and the two architecture-guide clarifications:

1. Use a demand-backed Binance USD-M slice instead of blindly starting with broad Binance Spot.
2. Treat options/Deribit as a first-class capability test now, while deferring actual venue activation until its own certification.

Upon approval, Phase 0 is implemented first. Later phases may refine measurable thresholds from Phase 0 evidence, but may not weaken compatibility, correctness, no-silent-loss or cleanup gates without explicit approval.
