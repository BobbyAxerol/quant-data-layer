# Phase 4 Implementation Report

## Conclusion

Phase 4 is complete and frozen in **shadow mode**. It provides canonical data
quality state, immutable revision-aware history, deterministic replay and a
signed snapshot/cursor handoff without changing the running V1 authority.

Provider data used for acceptance is real and read-only. Generated data appears
only in isolated deterministic tests and is never presented as provider proof.
The running V1 service, Redis, PostgreSQL and VN Parquet paths were not restarted,
rewritten or promoted during this phase.

## Implemented

- Raw-first validation and quarantine with explicit duplicate, out-of-order,
  sequence-gap, clock and source-authority state.
- Exact-decimal, revision-aware OHLCV records and session/DST-aware aggregation.
- Immutable ZSTD Parquet data files, atomic compare-and-swap catalog heads,
  additive schema checks, compaction records and confirmation-gated orphan
  cleanup, with S3-compatible and PyIceberg integration boundaries.
- VN canonical 1m migration and derived 5m/10m/15m/30m/1h/4h materialization
  from canonical 1m, preserving sparse sessions and complete source lineage.
- HMAC-signed, key-rotation-aware handoff tokens scoped to consumer, stream,
  partition, snapshot and watermark; durable checkpoints and contiguous replay.
- Deterministic raw/canonical/lineage checksums and revision-aware
  historical/live reconciliation.
- Exact-window OKX V5 trade, mark and index candle history; funding provenance;
  truthful snapshot-only open-interest coverage.
- Calendar-aware quality classification for market-closed, sparse-no-event,
  late/stale and genuine missing-data conditions.

## Verification

| Gate | Result |
|---|---|
| Focused Phase 4 suite | 36/36 pass |
| Full Python/V1 regression | 213 run: 208 pass, 5 expected environment skips |
| PostgreSQL migration | clean/existing/second apply pass; legacy preserved; 16 tables, 3 lease functions |
| Rust workspace | fmt, Clippy `-D warnings`, 11/11 tests pass |
| Canonical contracts | Buf format/lint/breaking/codegen diff pass |
| Redis recovery | 3/3 AOF restart/rebuild checks pass; checksum stable; disposable DB empty after cleanup |
| Durable replay benchmark | 10,000 events; 1,547.52 append/s; 8,538.58 replay/s; p99 60.39 ms; 2.072x disk amplification |
| Real OKX history | 30 trade, 30 mark, 30 index 1m bars; 6 funding records; 1 OI snapshot; zero production writes |
| Real DNSE coverage | 241/241 VN30F1M bars on 2026-08-12; zero gaps, out-of-session or fabricated rows |
| Existing VN migration | 28,196 source rows; 27,955 canonical rows; 241 exact duplicate groups; zero conflicting revisions/fabrication |
| Running V1 compatibility | health, VN preload and Binance USD-M OHLCV HTTP 200; restart count remained zero |
| Cleanup | no Phase 4 test container/network; 1.28 GB test image and 520.3 MiB Cargo artifacts removed |

Evidence:

- [`phase4-vn-shadow-migration.json`](phase4-vn-shadow-migration.json)
- [`phase4-okx-real-history.json`](phase4-okx-real-history.json)
- [`phase4-dnse-provider-coverage.json`](phase4-dnse-provider-coverage.json)
- [`phase4-replay-performance.json`](phase4-replay-performance.json)
- [`phase4-freeze.json`](phase4-freeze.json)

## Provider And Storage Boundaries

- S3-compatible and PyIceberg boundaries are implemented and integration-tested,
  but no production object store/catalog has been provisioned or made authoritative.
- OKX OI is `SNAPSHOT_ONLY`; this phase does not claim pre-ingestion OI history.
- DNSE public OHLCV begins at 09:00 for the certified date. The 08:45 pre-open
  calendar period is not fabricated into candles.
- Raw retention requires provider licensing and retention approval per source.
- Production HMAC secret custody, endpoint exposure and per-consumer migration
  belong to Phase 5/6; test keys are not production credentials.

## Freeze And Rollback

The Phase 4 code baseline ends at `46669f4`. Contract or semantic changes require
an ADR, refreshed parity evidence and a new freeze manifest.

No live rollback is necessary because `LEGACY_V1` remains authoritative. A
shadow rollback stops the selected shadow materializer/replay worker, preserves
its checksums for audit and removes only isolated shadow resources. Existing V1
routes, Redis keys/PubSub, PostgreSQL data and VN Parquet paths remain unchanged.
