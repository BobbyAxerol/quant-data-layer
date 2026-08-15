# Phase 8.2 Reference Shadow And Cross-Venue Conformance

Date: 2026-08-15

Status: **PASS**

Authority: `RUST_SHADOW`; public/V1/legacy writes: **0**.

## Implemented

- Added atomic exact-frame callbacks to the existing Binance USD-M and OKX V5
  supervisors. One callback receives the exact post-decompression bytes and the
  parsed object from that same receive operation; existing `on_frame` consumers
  remain source-compatible.
- Appended wire-compatible `raw_capture_id` field 33 to the V2 canonical
  envelope. Exact-frame shadow contexts also carry the 32-byte raw-frame hash.
  Python and Rust fail closed when session metadata is present without either
  identity.
- Added deterministic capture IDs and a validated raw-envelope/context binding.
  Canonical `raw_payload_hash` references the exact provider frame in shadow
  mode; legacy canonicalization keeps its prior payload-hash behavior.
- Added shared Python/Rust canonical paths for authentic DNSE bars and a
  fixture-only Deribit option book. Missing DNSE trade count is represented by
  `QUALITY_FLAG_FIELD_MISSING`; it is not silently treated as a real zero.
- Corrected the Binance USD-M capability record to match the demanded
  individual trade stream (`trade_id`). The existing aggregate-trade fixture
  parser remains a compatibility alias.
- Added a thin DNSE acquisition helper. It runs inside the credential-owning
  workload and emits only checksummed SDK-delivery rows; no credential or secret
  crosses the boundary.

## Authentic Provider Evidence

The bounded certification observed, without publishing:

| Source | Observed | Retained | Boundary |
|---|---:|---:|---|
| Binance USD-M `BTCUSDT` trade | 1,855 | 128 | exact WS bytes after decompression |
| OKX `BTC-USDT-SWAP` trade | 510 | 128 | exact WS bytes after decompression |
| DNSE `VN30F1M` 1m, 2026-08-14 | 241 | 241 | checksummed SDK-delivery rows, complete session |
| Deribit option book | fixture only | 1 | deterministic test namespace only |

The Binance/OKX window ran concurrently for 189.03 seconds. The retained
authentic bundle is 47 KiB compressed and is bound by SHA-256
`0912db1d39ddf1ec27414bccc55e096467305534aa581f9d217b179b7a95ff46`.
No test-provenance record exists in the real-capture namespace.

## Parity And Capacity

- 498 unique fixtures were replayed 200 times: **99,600 events**.
- Python versus Rust deterministic Protobuf bytes: **0 mismatches**.
- Event/capture identity, decimal, timestamp, sequence, session/generation,
  quality flags and canonical payload hash: **0 unexplained mismatches**.
- Three clean Rust process runs produced the same aggregate SHA-256 and record
  hashes: **0 restart mismatches**.
- Python measured 10,468 events/s, p99 0.230 ms and p99.9 0.415 ms.
- Debug-profile Rust replay measured at least 7,056 events/s. The immutable
  release-profile artifact and final capacity rerun belong to 8.3; this result
  passes the Phase 8.2 floor and is not presented as production sizing.
- Peak certification-process RSS was approximately 131.7 MiB.

## Failure And Recovery Coverage

Phase 8.2 reuses rather than duplicates the destructive substrate cases already
frozen in 8.0/8.1:

- broker restart, replica volume loss, min-ISR failure, restore and Redis rebuild:
  `phase8-broker-failover.json`;
- mTLS/ACL fail-closed behavior: `phase8-broker-security.json`;
- stale generation, reconnect timeout, duplicate/gap/session reset, lossless
  spool, coalescing and spool-full disconnect: `phase8-rust-session-chaos.json`;
- malformed/missing fields, exact callback atomicity, synthetic provenance
  isolation and evidence checksums: `tests/test_fund_phase82_conformance.py`.

## Verification

- Buf format/lint and breaking checks against Phase 1 and Phase 7 baselines:
  PASS.
- Python targeted regression: 31 tests PASS before capture; evidence-specific
  tests PASS after capture.
- Rust workspace format, Clippy `-D warnings` and all workspace tests: PASS.
- Real provider certification and three-process replay: PASS.

Machine-readable evidence:

- `phase8-cross-venue-conformance.json`
- `phase8-python-rust-parity.json`
- `phase8-real-provider-shadow.json`
- `phase8-capacity.json`
- `phase8-soak.json`
- `captures/phase8-real-provider-frames.json.gz`

## Operational Note And Remaining Gate

The host `.env` DNSE key returned `OA-401`, while the running credential-owning
Data Layer workload successfully acquired 241 rows. Secret rotation should
reconcile the host/operator source with the workload secret; no secret was
copied into evidence. This does not change market-data parity, but it is an
operator configuration warning.

Phase 8.3 remains required for the release-profile immutable image, SBOM,
provenance/signature, persistent authority fencing rehearsal, exact Python
rollback manifest and disposable-artifact cleanup. Rust remains shadow-only.
