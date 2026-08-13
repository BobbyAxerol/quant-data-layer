# Quant Data Layer Unified Implementation Plan

> **Status:** Phases 0-4 and Pre-Phase 5 readiness closure are complete on the feature branch in dark/shadow mode; no runtime cutover has started.
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
19. **Production data is provider-authentic.** Production and shadow ingestion
    paths may only publish bytes received from an approved real venue/provider or
    replay those previously durably captured bytes. They must never fabricate,
    seed, interpolate or silently substitute market events. Synthetic, generated
    and simulator payloads are restricted to isolated tests and are marked as
    test provenance. Bounded read-only provider smoke is mandatory before a feed
    implementation is frozen; fixtures remain the deterministic failure oracle,
    never evidence that a live source works.

## 3. Phase Summary

| Phase | Name | Primary outcome | Status |
|---:|---|---|---|
| 0 | Containment, inventory and measurable baseline | Freeze compatibility, stop unused cost and establish reproducible truth | `COMPLETE` |
| 1 | Canonical contracts, identity and runtime boundaries | Stable venue-neutral domain plus separately scalable Python roles | `COMPLETE (DARK)` |
| 2 | Durability contract, bridge and Rust foundation | Replayable transport boundary and deterministic cross-language core without premature broker cutover | `COMPLETE (DARK)` |
| 3 | Scalable ingestion and compatibility projection | Demand-driven Rust hot path with legacy V1/Redis parity | `COMPLETE (FROZEN SHADOW)` |
| 4 | Quality, history, replay and gap-free handoff | Certified data products from warmup through live recovery | `COMPLETE (FROZEN SHADOW)` |
| 4.5 | V2 readiness and debt closure | Freeze query semantics and remove correctness/security ambiguity before endpoint work | `COMPLETE (FROZEN DARK)` |
| 5 | V2 API/SDK and controlled consumer migration | Stable snapshot/cursor interface without breaking existing consumers | `PLANNED` |
| 6 | Production certification and multi-venue readiness | HA/security/SLO gates, controlled authority cutover and adapter scalability | `PLANNED` |

## 4. Phase 0 - Containment, Inventory And Measurable Baseline

**Status:** `COMPLETE`

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

- Added validated source ownership configuration. The new artifact defaults to Binance USD-M trade+kline; Spot, DNSE, vnstock and preload ownership remain independently configurable. The running service was not restarted.
- Frozen V1 OpenAPI, route/method/name inventory, SDK signatures and Redis payload shapes under [`contracts/v1`](contracts/v1).
- Added a read-only audit tool, bounded provider smoke and deterministic Binance/OKX/VN/malformed fixture corpus.
- Inventoried the full workspace, Trading System and active migrated alpha tree without reading generated logs/state/data.
- Captured two bounded runtime windows and a resource/topology baseline. Details and exact artifacts are in the [Phase 0 baseline report](upgrade/evidence/PHASE0_BASELINE_REPORT.md).
- Detected and fixed provider-scoped kline demand health correlation without changing V1 Redis payloads (`965275e`). Runtime verification remains part of the coordinated immutable-image deployment because Phase 0 did not restart the live process.
- Measured current source topology: 44 full Binance shards versus 16 USD-M-only shards, a projected 63.636% connection reduction. No active demand lease required Spot during observation.

### Technical Debt / Decision Gate

- Phase 1 must turn measured load into explicit SLO/headroom budgets. Baseline observed approximately 4.3k-4.5k Redis commands/s and 1.98-2.04 MB/s input during the two ten-second windows.
- The running image still has four Binance sources because no cutover was allowed. Deploying the USD-M-only default requires immutable-image V1/Redis shadow parity and coordinated recreation with the documented source-list rollback.
- Legacy `stream:trade:{symbol}` source authority must be frozen to USD-M (or explicitly versioned) before Spot producer removal. Active alphas had no direct-provider usage, but workspace-wide legacy/reference files remain and are tracked in inventory evidence.
- Existing queue code has 3,790,249 cumulative drops. Recent drops were zero in final windows, but Phases 2-3 must replace feed-agnostic drop/coalesce behavior before trade/book delta certification.
- OKX `after`/`before` pagination remains a characterized defect in the compatibility facade; it is corrected under the async adapter/history implementation with V1 golden protection, not silently in Phase 0.
- Existing `websockets.legacy` and `InvalidStatusCode` deprecation warnings must be removed during the scalable adapter implementation.

### Rollback

- Restore the previous feed-enable configuration and immutable image. No schema, topic or consumer cutover occurs in Phase 0.

## 5. Phase 1 - Canonical Contracts, Identity And Runtime Boundaries

**Status:** `COMPLETE (DARK / NO V1 CUTOVER)`

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

- Added canonical Protobuf packages, pinned Buf code generation and a frozen
  Phase 1 breaking baseline. Generated Python/Rust models share one contract;
  exact decimal, nanosecond timestamp, large native ID and deterministic golden
  binary tests pass.
- Added UUIDv5 canonical instrument identity, temporal aliases, metadata
  revisions, source/venue separation, session calendars and capability profiles.
  OKX Spot/Swap/Futures/Option/Event records are parsed from authoritative
  `/public/instruments` fields without symbol heuristics.
- Added forward-only PostgreSQL migrations for instrument/control metadata,
  source policies, subscriptions, revisions/audit, leases/fencing and jobs. Clean
  and legacy-seeded disposable databases pass second-apply idempotence with
  identical QDL schema hashes; no tick-event table was introduced.
- Added dark `api`, `control` and `history` entrypoints with fail-closed role
  ownership. Three API replicas instantiate no venue-loop owner. Existing
  `app.main:app` remains the sole V1 combined ingestion/projection authority.
- Added ADRs `0001`-`0005`, contract CI, Compose role profile and Phase 1 evidence.
- Verification: Buf format/lint/build/generate/breaking PASS; Python/Rust golden
  parity PASS; migration smoke PASS; full application regression `125 passed`;
  both environment-gated Redis integration tests separately PASS on disposable
  Redis; frozen V1 OpenAPI/Redis/SDK artifacts PASS.
- Evidence: [Phase 1 report](upgrade/evidence/PHASE1_IMPLEMENTATION_REPORT.md),
  [contract gate](upgrade/evidence/phase1-contract-gate.json), and
  [migration smoke](upgrade/evidence/phase1-migration-smoke.json). The unchanged
  live V1 path also passed [7/7 bounded read-only checks](upgrade/evidence/phase1-live-v1-smoke.json)
  with both running containers still at restart count zero.

### Technical Debt / Decision Gate

- Contract naming or semantics that affect public V2 behavior require explicit approval before schema freeze. Pure implementation details do not.
- Phase 1 schema bootstrap uses a checked-in binary breaking baseline. Once this
  branch lands on the protected base branch, add Git-ref breaking comparison as
  a second gate; do not replace the immutable initial baseline.
- Control tables and separated roles remain dark. Connecting them to authority,
  adding durable transport, or starting role services belongs to later approved
  phases and requires a coordinated immutable-image deployment.

### Rollback

- New roles and schemas remain dark. Existing combined runtime stays authoritative until a later per-feed cutover.

## 6. Phase 2 - Durability Contract, Bridge And Rust Foundation

**Status:** `COMPLETE (DARK / NO V1 CUTOVER)`

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

- Defined portable event sink/source, batch append, logical cursor/checkpoint,
  deterministic event-ID, retry and partition contracts without exposing a
  Redis/Kafka offset in application contracts.
- Implemented a bounded local SQLite WAL bridge for the selected BTCUSDT and
  ETHUSDT Binance USD-M trade shadow slice. It uses `synchronous=FULL`, atomic
  batches, raw-first durable acceptance, payload checksums, corruption checks,
  monotonic offsets, consumer TTL and strict logical/physical metadata bounds.
- Implemented restartable raw-to-canonical processing, idempotent compatibility
  projection and Redis flush/restart/replay rebuild. All Redis writes use an
  isolated `shadow:qdl:v2` namespace; legacy production publication is disabled.
- Added Python/Rust exact-decimal golden parity for Binance and OKX plus a
  deterministic OKX protocol simulator covering sequence gaps, stale connection
  generations, keepalive and maintenance reset.
- Added a Rust workspace and immutable replay tool with pinned compiler/base
  image digests, `unsafe` forbidden, deterministic backoff/rate-limit/fencing
  primitives and CI format/clippy/test/dependency policy gates.
- Measured 10,000 durable events over 10 partitions and 8 consumer groups:
  1,470.85 events/s append, 8,211.74 events/s replay, p99 62.28 ms per fsynced
  batch, 33,004 KiB max RSS and 2.072x disk amplification. All configured gates
  passed for the small shadow slice.
- Verification: full Python regression 146 run (143 pass, 3 environment-gated skips);
  Phase 2 focused tests 20 PASS; Rust 9 PASS; Buf compatibility PASS; isolated
  Redis recovery PASS; Rust dependency/license/advisory policy PASS; two
  immutable builds produced the same image ID.
- Read-only live smoke loaded BTCUSDT/ETHUSDT through unchanged V1, produced two
  raw and two canonical local records, and made zero production writes. Running
  Data Layer/Redis restart counts remained zero and all Phase 2 resources were
  cleaned.
- Evidence: [Phase 2 implementation report](upgrade/evidence/PHASE2_IMPLEMENTATION_REPORT.md),
  [verification summary](upgrade/evidence/phase2-verification.json),
  [performance report](upgrade/evidence/phase2-performance.json), and
  [live shadow smoke](upgrade/evidence/phase2-live-shadow-smoke.json).

### Technical Debt / Decision Gate

- Kafka promotion requires explicit approval after measured evidence shows at least one material trigger: multiple independent replay consumers, replay horizon beyond the bounded bridge, raw trade/book volume exceeding its safe budget, multi-node HA requirement, or unacceptable bridge lag/recovery time.
- Until that gate passes, the dedicated bridge/local spool is transitional infrastructure with a declared limit and sunset path, not the canonical long-term target.
- The bridge is a single-host, non-HA shadow mechanism and is not approved for
  broad-universe trades or order-book deltas. Phase 3 must produce sustained
  demand-backed parity/capacity evidence before any feed authority changes.
- Existing Python V1/build dependencies have advisory findings in the current
  image. Phase 2 added no Python dependency; a compatibility-tested dependency
  refresh remains mandatory before production promotion. Rust Phase 2
  dependencies pass advisory, license, ban and source policy checks.
- OpenTelemetry export and multi-node durable-broker failover are later
  certification work; Phase 2 supplies the stable interfaces, not an HA claim.

### Rollback

- Stop shadow Rust/broker services and remove their isolated resources. Existing Python-to-Redis path remains authoritative.

## 7. Phase 3 - Scalable Ingestion And Compatibility Projection

**Status:** `COMPLETE (FROZEN SHADOW)`

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

- Added deterministic demand registry, TTL leases, no-truncation shard planning,
  PostgreSQL lease ownership and monotonically increasing fencing epochs.
- Added feed-class queue policy: trade/book are lossless and block under
  pressure; BBO/bar may coalesce only the same pending latest-state key. Spot
  with zero declared demand creates zero shards but uses the same contract when
  enabled later.
- Added a provider-authentic Binance USD-M adapter with exchange-info
  validation, exact tick/step metadata, demanded-only WebSocket streams,
  reconnect/backoff and interruptible graceful shutdown. Added a bounded Rust
  Binance trade hot path that writes raw provider frames before canonical bytes
  to an fsynced shadow WAL.
- Added an OKX V5 async REST client with endpoint buckets and retry, separate
  public/business WebSocket supervisors with acknowledgement correlation,
  heartbeat/reconnect/resubscribe and an executable book state machine. A true
  `prevSeqId/seqId` gap clears the book and requires a fresh WebSocket snapshot;
  REST `/books` is explicitly forbidden as a delta-continuity bridge.
- Added exact canonical BBO, bar and book mappings; raw-first market events;
  canonical latest-state and frozen V1 bar compatibility projection; atomic
  Redis checkpoint/lease-epoch fencing; and a per-feed authority router that
  switches `SHADOW/CANONICAL/LEGACY` without process restart.
- Added adapter capability-boundary tests for an option/order-book source.
  Deribit-shaped data is explicitly test-synthetic and cannot be certified as a
  production source.
- Added reconnect-storm and stop-interruption tests for Binance and OKX so a
  requested shutdown does not wait for the heartbeat timeout.
- Added bounded real-provider, burst/restart, sustained-load and Rust live-frame
  parity evidence. Production/shadow implementations do not import fixtures or
  simulator modules.
- Froze the Phase 3 implementation in shadow mode. Evidence and immutable
  artifact identity are recorded in
  [`PHASE3_IMPLEMENTATION_REPORT.md`](upgrade/evidence/PHASE3_IMPLEMENTATION_REPORT.md)
  and [`phase3-freeze.json`](upgrade/evidence/phase3-freeze.json). Reopening this
  phase requires a reviewed contract/ADR change and new certification evidence.

### Verification

- Full Python/V1 regression: `177` tests run, `172` passed and `5` expected
  environment-gated skips.
- Phase 3 focused adapter/control/projection/provenance suite: `29/29` passed,
  including malformed input, reconnect storm, delist/inactive symbol,
  sequence gap and graceful shutdown.
- Rust `1.82.0`: format, Clippy with warnings denied and `11/11` tests passed;
  Python/Rust trade/BBO/bar golden bytes are exact.
- Buf format/lint/breaking/codegen-diff gates passed against the frozen Phase 1
  descriptor.
- PostgreSQL disposable integration passed exclusive owner, renew, release,
  expiry takeover and stale-epoch fencing.
- Redis disposable integration passed AOF restart, deterministic flush/rebuild,
  stale-epoch rejection and live authority switch/rollback across isolated
  targets with no process restart.
- Provider-authentic read-only smoke passed Binance USD-M trade/BBO/closed bar
  and OKX trade/book snapshot/deltas, with `20` durable records and zero
  production writes.
- Rust read-only smoke consumed `3` real Binance events; every canonical byte
  matched Python and was written to isolated fsynced WAL only.
- Burst/restart gate accepted and replayed `20,000/20,000` events across `80`
  partitions at `1,343.03 events/s`, with queue rejection `0` and peak traced
  Python memory `1,965,064` bytes.
- Sustained gate held `500.70 events/s` for `5,000` events across `80`
  partitions; durable p95/p99 were `157.09/163.60 ms`, queue rejection `0`,
  records survived restart and peak traced memory was `335,239` bytes.
- Running `data_layer_service` and `redis_marketdata` stayed on their existing
  images with restart count `0`; `/v1/health` remained `ok`. No production
  Redis, PostgreSQL, Parquet, route, namespace or authority flag changed.
- Cleanup left no Phase 3 test container/network, removed the disposable Python
  test image and `709 MB` Cargo target cache, and retained only the frozen Rust
  evidence image. Existing logs, volumes and running services were untouched.

### Technical Debt / Decision Gate

- Phase 3 is certified only as a dark/shadow implementation. SQLite remains a
  single-host transitional spool, not an HA broad-universe authority. The
  Kafka-compatible promotion trigger and production authority cutover remain
  governed Phase 6 decisions.
- Binance individual trade and BBO WebSockets plus closed-bar REST were
  provider-certified from this host. The Binance USD-M kline WebSocket emitted
  no frame during bounded probes, so its parser has deterministic golden parity
  but is not falsely marked live-certified.
- OKX public `books` is certified with `seqId/prevSeqId` continuity. The current
  V5 `checksum` value is deprecated/fixed and is not represented as a valid CRC
  capability. VIP/deep-book channels remain separately uncertified.
- Historical completeness, raw retention governance, quarantine, source
  failover and gap-free warmup/live cursor handoff are Phase 4 scope. No
  generated bar, REST order-book bridge or synthetic source is used to conceal
  those pending capabilities.
- Production promotion remains deliberately unperformed under Rules 15-16.
  The authority mechanism is integration-certified, but V1 stays authoritative
  until the Phase 6 release/cutover gate is explicitly approved.

### Rollback

- No live rollback was needed because no authority changed. For an isolated
  shadow deployment, stop only its shard owner, return its per-feed authority
  to `LEGACY`, verify the fencing epoch, and delete only its namespaced shadow
  checkpoints/spool after checksum capture. Existing Python/V1 publication
  continues uninterrupted.

## 8. Phase 4 - Quality, History, Replay And Gap-Free Handoff

**Status:** `COMPLETE (FROZEN SHADOW)`

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

- Phase 4 verification matrix frozen before implementation: canonical quality
  and source authority; immutable/revision-aware history; deterministic replay
  and signed cursor handoff; OKX pagination/reference coverage; VN sparse
  session migration; bounded real-provider checks; crash/restart/cleanup gates.
- Production V1, Redis, PostgreSQL and existing VN Parquet remain read-only for
  this phase. All new catalog/object-store resources use isolated shadow paths;
  no object-store or historical authority cutover is approved here.
- History shadow slice implemented: exact-decimal revision-aware bars,
  session/DST-aware OHLCV aggregation, immutable ZSTD Parquet snapshots,
  conditional atomic catalog heads, S3-compatible and PyIceberg boundaries,
  plus crash/concurrent-writer tests. The real VN migration canonicalizes the
  mixed legacy UTC/VN-naive 1m file, deduplicates only exact OHLCV duplicates
  with full source lineage, fails closed on conflicting revisions, and derives
  all larger intervals from canonical 1m without fabricating bars.
- Real read-only VN evidence: 28,196 legacy rows -> 27,955 canonical 1m rows;
  241 exact duplicate groups, zero conflicting revisions, zero fabricated rows;
  all seven interval snapshots round-trip from isolated shadow storage. See
  `upgrade/evidence/phase4-vn-shadow-migration.json`.
- Gap-free handoff/replay slice implemented with HMAC-signed, rotation-aware,
  consumer/stream/partition/snapshot-scoped cursors; snapshot watermarks,
  durable consumer checkpoints and offset-contiguity checks. Cursor tampering,
  wrong scope, expiry, compaction loss and unexplained gaps fail closed; late
  arrivals appended after the watermark remain replayable by durable offset.
- Deterministic raw replay now reports raw/canonical/lineage checksums keyed by
  source revision, normalizer version and config revision. History/live overlap
  reconciliation supports explicit higher revisions and checks only the open
  times supplied by a session calendar, preserving legitimate sparse feeds.
- OKX V5 shadow history/reference client implemented without changing the V1
  route: documented `after=oldest_ts` backward traversal, inclusive exact-window
  filtering, overlap dedup, confirmed-candle revision preference, no-progress
  failure, page/record budgets and explicit coverage. Trade, mark and index
  candles remain distinct contracts; funding retains formula/method/raw lineage;
  OI is explicitly `SNAPSHOT_ONLY` and never claims historical coverage.
- Bounded real OKX public-API evidence passed with 30 trade, 30 mark and 30 index
  1m bars, six funding records and one OI snapshot; all requested historical
  windows reported full coverage, with zero production writes. See
  `upgrade/evidence/phase4-okx-real-history.json`.
- Calendar quality policy now distinguishes closed sessions/holidays, legitimate
  sparse feeds, late bars and missing continuous bars. A real read-only DNSE
  probe for completed date 2026-08-12 returned exactly 241 VN30F1M bars over
  provider bar sessions 09:00-11:29, 13:00-14:29 and close 14:45, with zero
  missing/out-of-session/fabricated rows. The 08:45 market pre-open is retained
  as market-calendar context and is not fabricated as provider OHLCV. See
  `upgrade/evidence/phase4-dnse-provider-coverage.json`.
- Historical catalog governance now enforces additive-compatible schemas,
  records compaction operations, identifies uncommitted orphan files and
  requires exact dataset confirmation before deletion. S3 CAS uses the ETag
  from the same read (preventing a concurrent-head race) and paginates listings;
  dedicated race/immutability tests pass.
- Full-suite certification found and fixed a handwritten-package collision with
  generated protobuf namespace `qdl.quality.v1`; the implementation now lives
  under `qdl.data_quality`, leaving the stable generated contract untouched.
  Durable replay performance over 10,000 events passed all gates: 1,547.52
  append events/s, 8,538.58 replay events/s, 60.39 ms append p99 and 2.07x
  disk amplification. See `upgrade/evidence/phase4-replay-performance.json`.
- Final verification passed 36/36 focused Phase 4 tests and the full Python/V1
  regression (213 run, five expected environment skips). PostgreSQL migrations
  passed on a clean database, an existing database and a second idempotent
  apply while preserving legacy data (16 `qdl_*` tables and three lease
  functions). Rust fmt/Clippy `-D warnings`/11 tests, Buf format/lint/breaking/
  generated-code diff, and Redis AOF restart/rebuild checks also passed.
- Production compatibility remained read-only: the running V1 container kept
  restart count zero and health, VN preload and Binance USD-M OHLCV endpoints
  returned HTTP 200. No production Redis, PostgreSQL, Parquet or authority
  state was modified. The implementation report and machine-readable freeze
  are `upgrade/evidence/PHASE4_IMPLEMENTATION_REPORT.md` and
  `upgrade/evidence/phase4-freeze.json`.

### Technical Debt / Decision Gate

- Object-store/catalog deployment and retention cost require approval before production provisioning; local MinIO/catalog remains a test implementation only.
- Provider licensing constraints must be recorded before raw retention is enabled for a new source.
- Production HMAC key custody/rotation, externally exposed handoff endpoints and
  per-dataset authority promotion remain Phase 5/6 gates; test keys and local
  catalog boundaries are not production credentials or a cutover claim.
- OKX open interest remains a truthful point-in-time snapshot. Historical OI
  coverage requires a separately certified provider capability and must never
  be inferred from the current endpoint.

### Rollback

- Existing VN Parquet/read path remains available until shadow snapshot reconciliation passes. Historical authority switches per dataset, never globally.

## 8A. Phase 4.5 - V2 Readiness And Debt Closure

**Status:** `COMPLETE (FROZEN DARK)`

### Goal

Close cross-phase correctness, security and compatibility ambiguity before any
public V2 route or gRPC service is implemented. Freeze provider-neutral query
semantics over transport/storage interfaces so Phase 5 adds delivery surfaces,
not new domain behavior.

### Guide Index

- [Pre-Phase 5 readiness guide](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#implementation-phase-4-5)
- [Stable API, SDK and consumer semantics: Sections 17-19](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#17-stable-api-design)
- [Failure semantics: Section 38](upgrade/quant-data-layer-fund-grade-upgrade-architecture.md#38-failure-semantics-exposed-to-consumers)

### To Do

- Reconcile the conflicting Section 17/38 error names into one stable internal
  taxonomy and freeze `DataRequirement`, consumer grade, completeness/partial,
  freshness, bar revision, source-policy and recovery semantics.
- Define bounded provider-neutral query/result contracts before HTTP/gRPC
  serialization. An execution-grade all-instruments requirement must fail
  closed on partial, stale, gapped, non-authoritative or unentitled data.
- Replace handoff's concrete SQLite dependency with durable transport/catalog
  protocols. Bind immutable historical snapshot cursor end to the captured live
  watermark before issuing a signed token; mismatch must fail closed and retry.
- Introduce a rotation-aware signing-key provider boundary. Unsigned logical
  cursor tokens remain internal only and must never be accepted on a public V2
  boundary.
- Add source entitlement/licensing policy with default-deny external
  redistribution and raw-history access. Capability and provenance cannot imply
  entitlement.
- Close safe legacy compatibility debt needed by certification (notably the
  deprecated WebSocket client/exception API) without changing V1 routes, SDK or
  Redis payloads.
- Resolve stale debt notes from Phases 0-4 as `CLOSED`, `SUPERSEDED` or an
  explicit Phase 6 infrastructure/authority decision. Do not hide an in-scope
  defect as a decision gate.

### Verification And Exit Gate

- Pure-domain tests cover invalid/bounded requirements, strict versus partial
  batches, all consumer grades, freshness/gap/authority decisions and licensing
  denial without provider calls.
- Handoff tests cover snapshot/watermark match, mismatch, concurrent head move,
  key rotation, unknown/retired key, tamper, wrong scope, expiration, compaction,
  restart and transport substitution without SQLite-specific API assumptions.
- Existing Phase 1-4 focused suites, full Python/V1 regression, Buf breaking and
  generated-code gates, Rust fmt/Clippy/tests, Redis rebuild and PostgreSQL
  migration smoke pass.
- A Python dependency/advisory report is recorded. Unfixed exploitable runtime
  findings block Phase 5; accepted non-runtime/tooling findings require an owner
  and expiry.
- Read-only V1 health, VN preload and Binance USD-M history smoke return the
  frozen behavior while the running service restart count remains unchanged.
- Test containers, images, databases, Redis state, object-store paths and build
  caches are isolated and removed. User-owned `symbols.json` remains untouched.

### Completed

- Cross-phase audit identified four endpoint blockers: inconsistent failure
  names, absent query/requirement domain contracts, SQLite-coupled handoff and
  in-memory-only signing/entitlement boundaries. Provider certification and HA
  authority gates were separated from endpoint semantics instead of being
  pulled prematurely into Phase 5.
- Added provider-neutral query contracts for one canonical error vocabulary,
  bounded `DataRequirement`/batch semantics, consumer grades, coverage,
  freshness/gap/recovery and bar-revision policies. Execution-grade requests
  cannot relax full coverage, authoritative source, gap or freshness gates.
- Added source licensing/entitlement contracts with explicit purpose and data
  product. Missing, expired, raw-history and external-redistribution grants
  fail closed independently of provider capability.
- Refactored handoff to portable durable-store, catalog and signing-key-provider
  protocols. `SnapshotHandoffCoordinator` issues a signed grant only when the
  immutable snapshot cursor end exactly equals the captured durable watermark;
  live events after that capture remain replayable. Static keys are test/local
  only, rotation overlap works and retired/unknown/tampered/unsigned cursors
  fail closed.
- Reconciled Section 17/38 error names and added legacy alias mapping without
  creating extra public values. CI now checks Buf against both the immutable
  Phase 1 baseline and the pull-request base branch.
- Replaced deprecated legacy WebSocket APIs with `websockets.asyncio` and
  verified one real Binance USD-M trade frame read-only. V1 route, payload,
  Redis and reconnect semantics remain unchanged.
- Closed Python runtime dependency debt: multi-stage image excludes Poetry and
  build dependencies; patched runtime dependencies audit at 0 findings across
  61 packages. Full regression exposed and fixed undeclared DNSE `msgpack`
  ownership. The tested image content size was 163,007,213 bytes versus
  394,348,787 bytes for the running image (58.66% smaller).
- Certification passed 11/11 Phase 4.5 tests, 47/47 combined Phase 4/4.5 tests
  and 224 full Python tests with five expected environment skips. Redis rebuild
  3/3, PostgreSQL clean/existing/idempotent migration, Buf gates and Rust
  fmt/Clippy/11 tests passed. The 10,000-event durability benchmark passed at
  1,432.70 append/s, 8,266.96 replay/s, 59.70 ms p99 and 2.072x disk
  amplification.
- Read-only production smoke returned HTTP 200 for health, VN preload and
  Binance USD-M history; the running V1 restart count stayed zero. No endpoint,
  producer, storage or source authority was activated. Test image, containers,
  networks, temporary reports and 520.3 MiB Cargo artifacts were removed.
- Cross-phase closure matrix, implementation details and machine-readable
  evidence are in `upgrade/evidence/PHASE45_V2_READINESS_REPORT.md` and
  `upgrade/evidence/phase45-freeze.json`.

### Technical Debt / Decision Gate

- Kafka-compatible HA promotion, production object-store/Iceberg provisioning,
  production secret backend, raw-data licensing approval and per-feed authority
  cutover remain Phase 6 decisions. Phase 4.5 must provide stable interfaces and
  fail-closed readiness for them; it must not fake infrastructure approval.
- OKX historical OI and VIP/deep-book capability remain unavailable until a
  separately licensed/certified provider source exists. Public contracts expose
  this honestly as capability/coverage state.

### Rollback

- Phase 4.5 adds dark pure-domain contracts and compatibility-safe internals.
  Revert its commits if required; V1 remains authoritative and no runtime,
  storage, source or consumer manifest is changed.

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
