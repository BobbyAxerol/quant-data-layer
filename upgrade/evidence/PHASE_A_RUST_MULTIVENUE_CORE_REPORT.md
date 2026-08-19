# Phase A Rust Multi-Venue Realtime Core Report

Date: 2026-08-19

## Decision

- Status: `COMPLETE / ISOLATED CERTIFIED / CUTOVER NOT AUTHORIZED`
- Runtime authority: existing V1 remains authoritative
- Candidate authority used by certification: `RUST_SHADOW`
- Production/public/legacy writes: `0`
- Covered first-class products: Binance USD-M perpetual, Binance Spot, OKX
  SWAP perpetual, OKX Spot, VN equities and VN derivatives.

## Delivered Architecture

- Added one provider-neutral `qdl-realtime-core` Rust crate. It validates the
  versioned raw provider envelope, resolves an approved binding, enforces
  source role and quantity units, canonicalizes, deduplicates across reconnect,
  applies sequence policy and creates canonical/quarantine durable records.
- Added native Rust Binance/OKX acquisition with public/business socket
  separation where required, heartbeat/reconnect/backoff and fenced raw Kafka
  publication. Acquisition adapters never write Redis or public payloads.
- Added a Kafka consume-transform-produce transaction that atomically commits
  canonical/quarantine outputs with the next raw consumer offsets. A crash
  cannot acknowledge raw input without its corresponding output.
- Added a Python TLS/idempotent raw publisher for vendor-SDK/low-rate edges.
  DNSE/vnstock remain Python acquisition adapters, but canonical semantics and
  durable publication decisions belong to Rust.
- Added Binance latest-closed REST BAR acquisition. Binance TRADE/BBO remain
  native Rust; BAR uses the low-rate Python history edge and then the same Rust
  core. OKX TRADE/BBO/BAR remain native Rust.

## Domain Corrections

- Added explicit `BASE_ASSET`, `QUOTE_ASSET`, `CONTRACT` and `SHARE` quantity
  units. BAR preserves native, base, quote and contract volume independently.
- Added native versus derived trade identity. DNSE trades without a native
  trade ID use exact raw-capture identity; absent aggressor side/source time is
  explicit in quality flags rather than fabricated.
- Removed hardcoded `SOURCE_ROLE_PRIMARY`; vnstock fallback remains secondary
  and provider switches retain provenance.
- Binance Spot BBO may omit provider event time. The core uses receive time and
  emits `SOURCE_TIME_MISSING` instead of rejecting or inventing a timestamp.
- DNSE legacy `stream:vn:*` naming no longer determines V2 semantics. Native
  DNSE trade callbacks are canonical TRADE; QUOTE remains capability-gated.

## Provider And Durability Evidence

- Real provider shadow: 26 raw durable ACKs and 26 read-committed canonical
  records, zero quarantine, across six product scopes. Binance/OKX crypto data
  came from live WebSocket, Binance BAR from real latest-closed REST rows, VN
  equity from a recent durable DNSE SDK-delivery snapshot and VN derivative BAR
  from provider-derived canonical Parquet replay. Generated market events: 0.
- Transactional failure matrix: 8 raw records produced 6 canonical records,
  one duplicate suppression and one intentional sequence-gap quarantine. The
  transaction committed with one Kafka replica stopped, then full ISR restored;
  read-committed visibility and counts remained exact.
- Capacity: 100,000 raw envelopes produced 100,000 canonical records with zero
  duplicate/quarantine at approximately 154,139 events/s; p50 4.6 microseconds
  and p99 10.8 microseconds on this host.
- All isolated containers, networks and volumes were removed. Scoped cleanup
  also removed 2.8 GiB of Cargo target artifacts and the 2.91 GB disposable
  Rust builder image. V1 topology was unchanged by inspection and health
  remained HTTP 200 before and after every certification.

Machine evidence:

- `phase-a-real-provider-core.json`
- `phase-a-transactional-core.json`
- `phase-a-realtime-core-capacity.json`
- `contracts/golden/phase2/manifest-v2-stable-multivenue.json`

## Verification

- Python: 433 passed, 5 conditional integration skips.
- Rust: format and Clippy with warnings denied passed; 50 workspace tests plus
  doc tests passed.
- Contracts: Buf format/lint and breaking checks passed against Phase 1 and
  Phase 7 beta baselines; generated bindings are current.
- V2 OpenAPI semantic diff: 10 operations, 42 schemas, zero hard break.
- Python/Rust exact-byte corpus: 19 Binance/OKX/VN cases passed.

## Defects Found And Closed

1. Canonical quantities were unitless across products.
2. Source role was hardcoded primary.
3. DNSE missing trade identity/side could not be represented honestly.
4. The old Binance USD-M stream path was outdated after provider migration.
5. Binance Kline WebSocket emitted no frame on legacy, public-combined or
   public-raw probes; BAR was moved to provider REST latest-closed acquisition,
   not generated or silently omitted.
6. Binance Spot BBO omitted source timestamp and was initially quarantined.
7. Confluent Python headers required a list rather than a tuple.
8. Isolated Kafka network blocked venue DNS; only acquisition containers now
   receive explicit egress during certification.
9. Multi-row provider frames could partially stage valid rows before a later row
   failed. The core now rolls back ordering, dedup and partition state and emits
   exactly one quarantine for the whole raw frame.

## Remaining Boundary

No in-scope Phase A defect remains. Phase B owns the stable Python
projector/query/stream deployment, registered consumer migration, immutable
`2.0.0` release and operator-approved per-slice cutover. This report does not
make Rust production-authoritative and does not change V1 runtime ownership.
