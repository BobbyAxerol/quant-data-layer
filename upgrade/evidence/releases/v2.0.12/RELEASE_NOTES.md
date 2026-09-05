# Quant Data Layer v2.0.12

## Certified Scope

Real-provider, no-order certification passed all five release gates for the
declared Binance USD-M and OKX Swap manifests. Rust remains canonical authority;
Python exposes stable V2 query/stream and SDK contracts. V1 is fallback only
where explicitly allowed. This package is ready for a release PR from dev to
main; this file alone does not assert GitHub publication.

- 299/299 V2 consumer products: monitoring4, Trading-System paper adapter60,
  Binance alpha SDK125, OKX alpha SDK110.
- 234 durable products and65 bounded on-demand reference products.
- BTC, ETH, SOL, DOGE and BNB across both venues;140 physical final-BAR
  bindings,10 execution books, declared venue-specific intervals/metrics.
- TRADE, QUOTE, final BAR, BOOK_SNAPSHOT/BOOK_DELTA, MARK_INDEX_PRICE,
  funding, open interest, contract metadata, and the declared Binance
  long/short, taker-flow and basis products. Unsupported OKX metrics are not
  substituted with Binance data.
- Zero blocked crypto routes, zero active fallback at certification, no
  resource-budget violations. Seven allowed fallback routes passed the
  V2 -> V1 -> V2 drill;292 disallowed fallback routes remained fail-closed.

## Corrections

- Keep stable history under bounded count/byte/disk retention instead of
  deleting sparse BARs solely by commit age. All140 retained BAR windows were
  verified complete with no holes, duplicates or sequence-gap flags.
- Repair genuine provider BARs using the active writer generation; never
  fence normal ingestion with an unrelated repair generation.
- Separate local canonical cache reads from provider REST admission quotas.
- Retry one prematurely closed read-only HTTP connection within its original
  deadline. Do not retry execution, authorization or provider-throttling errors.
- Align certification with typed live-session semantics, bounded reference
  deadlines and immutable BAR overlap at close boundaries. Separate hot
  closing batches from history; retain strict source and receive freshness.
- Update an inactive optional QUIC dependency with no change to the selected
  Rust production dependency graph. Final scanned image has zero fixable
  HIGH/CRITICAL findings in the recorded scan.

## Evidence

Full C2: opening883.031s (including identity quota pacing), observation300.100s,
closing13.401s. Opening duration is not endpoint latency. Zero order actions,
zero direct-provider connections from test clients, and client/cursor cleanup
completed. All44 protected services were unchanged.

Source regression, contract, SDK, Rust and CI evidence is linked from the
[main implementation journal](../../../../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md).
Exact scope, source/harness hashes and runtime digests are in
[scope-evidence.json](scope-evidence.json); the machine-readable result is
[certificate.json](certificate.json).

## Runtime And Rollback

- Python8 roles: `qdl-v2-python:2.0.12-35a7cd8`,
  `sha256:1c1392bf636dc40c67cc73a2e5ea5e8d17f4e53ca4ecb8c62ac387be4262045a`.
- Rust core/ingestion: `qdl-v2-rust:2.0.12-d536098`,
  `sha256:36a822c0ef61fb122dbf8fa12221cff27ad6a863976424be1407cd345f4dce65`.
- Runtime revision: `phasec36-reference-l2-r14`; no new topology.
- Immediate rollback retains query image `cca6355c` and other Python-role
  image `46a04c1e`, with exact existing runtime/state/TLS mounts.
- V1, Kafka offsets/topology, Redis/SQLite generations, Trading System and
  alpha/order paths remain protected. No offset reset or data deletion.

## Boundaries

This is a single-host, real-provider data-plane certificate for the listed
consumer manifests, not broker-order execution, independent HA/DR or every
possible future product. Four VN routes remain explicitly V1-primary pending
their separate market-hours certification. Per-consumer manifest identity,
entitlement, freshness and risk policy still apply. No trading alpha was
started by this release closure.
