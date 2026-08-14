# Phase 7.2 Consumer Canary Report

## Decision

`PASS` for Phase 7.2. Phase 7 remains `IN_PROGRESS` because the capacity,
adversarial and final beta decision gates belong to Phase 7.3.

## Implemented

- Added one strict canonical source catalog for Binance USD-M BTCUSDT final 1m
  bars. Instrument UID/ID, native symbol, source policy, lineage and V1 route are
  immutable and fail closed on unknown fields.
- Added a bounded read-only V1 bridge. It rejects direct venue hosts, filters
  in-progress bars, preserves native decimal text, canonicalizes through the
  existing Binance REST normalizer and uses authenticated internal ingest.
- Bound V2 query and active/passive stream roles to one isolated durable spool.
  Snapshot and stream cursors now derive from the same durable watermark and
  are signed per consumer.
- Added monitoring and disposable paper-alpha manifests. Both use SDK major 2,
  retain V1 rollback, and set `execution_dependency: FORBIDDEN`.
- Added deterministic revision-aware paper state, atomic local checkpoint state,
  credential rotation, gateway failover, restart/resume and fresh-rebuild parity.
- Added real bar-gap detection to the canary query backend. `BLOCK` policies now
  reject stale data and missing closed-bar intervals explicitly.
- Bound gRPC subscribe replay to the server's bounded replay window, separately
  from the authenticated live outbound-buffer quota. If backlog still exceeds
  that window, the gateway now returns cursor expiry and requires a fresh
  snapshot instead of silently switching to LIVE with an incomplete replay.

## Real Canary Result

- Source: canonical V1 API backed by real Binance provider data; no generated or
  simulated market event was admitted to evidence.
- Monitoring started first and received logical offset `118`.
- Paper alpha checkpointed `119`, then resumed after gateway epoch `1 -> 2` and
  checkpointed `120` on the next real closed 1m bar.
- V1/V2 identity, decimal, timestamp, source, authority, count and finality
  mismatch count: `0`.
- Restarted state hash exactly matched a fresh snapshot reconstruction:
  `ccfe11f93d13cc76624ffb43bee16c090e5133192416b6b9dce6b360a4a292f7`.
- After stopping V2 query, V1 fallback returned HTTP `200`.
- V1 container topology/restart counts/mounts/networks were unchanged;
  production mutation count and remaining production beta keys were both `0`.

## Verification

- Focused Phase 7.0-7.2 suite: 31 tests passed.
- Full Python regression: 305 tests passed; 5 existing conditional integration
  skips remain covered by their dedicated Docker/provider gates.
- Rust: fmt passed, clippy passed with warnings denied, 11 tests passed.
- Buf: format, lint and breaking checks passed against both Phase 1 and frozen
  Phase 7 baselines.
- OpenAPI snapshot remained byte-identical at
  `bea44d3920db52f5893eb773aa195ae7f4abd2684d5ca65d904e995934fabcea`.
- Tested image:
  `sha256:7c8a28af18a560f6ca227268895c6db2ebeb1a3b2800da65175ebe8ebe185677`.

## Scope Boundaries

- No VN feed was activated, so VN session open/close boundary testing was not
  applicable. VN rolling-future canary remains blocked until expiry and revision
  ownership is explicit.
- The bridge is a bounded Phase 7 compatibility component; it is not acceptable
  as the authority substrate for Phase 8/9.
- Capacity/burst, malformed traffic matrix, final credential revocation bundle
  and `BETA-GO`/`BETA-NO-GO` remain Phase 7.3.
