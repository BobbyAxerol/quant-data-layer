# DR05 Export, G4 TRADE/MARK and Build Lock Handoff

Status: HANDOFF_READY / G4_BOUNDED_OBSERVATION / NOT_PHASE_D_CERTIFIED.
Documentation and read-only measurement only; no rollout, mutation, push or merge.

## DR05 public export

`PUBLIC_EXPORT=/home/bobby/.local/state/qdl-v2/dr05-observer-20261008/public-export.json`

SHA256 `9cf857a223cd9f370452c4121c0d14704df87f2843a368536898054e0ceef31a`,
1359 bytes. Checked exact top-level core/bindings and exact equality to
`isolated_config()` public allowlist. Exactly BINANCE ETHUSDT and OKX
ETH-USDT-SWAP, PRIMARY PERPETUAL BOOK with L2 parameters. No transport, credentials
or production authority. This file does not expire; newly generated TLS/JWT does.
Use a NEW work directory and current DR05 init/up/ready/reset-host guide. This
turn supplied the requested file rather than starting a duplicate disposable
stack. No watcher is running on Claude's behalf. Do not interpret this as DR05 PASS.
Existing `upgrade/evidence/p184-dr05-dl-fault.md` and `scripts/p184_dr05_reset.py`
remain authoritative for clean broker-preserving resets; TS clones must stop as
required before reset. Ports6391/5433 remain reserved by that packet.

## G4 simultaneous real observations

Window UTC: 2026-10-09T08:19:24.396767+00:00 to 2026-10-09T08:21:24.288447+00:00.
BTC and ETH, both venues, TRADE and MARK_INDEX_PRICE. 120.000s,
2689 observations, zero decode/Kafka errors or missing/negative timestamp pairs.
Quantiles nearest-rank, ms. At most one emitted event per product/feed per100ms:
this is a bounded time-throttled sample, NOT every-event or uniform-reservoir
statistics. Six partitions assigned at their current end; read_committed,
no group join, auto-store/commit disabled, no broker writes. Existing Query image
executes temporary Python observer; no service/image/config changes.

| Venue/symbol/feed | n | source -> visible p50 | p95 | p99 | max | ingest -> visible p95 |
|---|---:|---:|---:|---:|---:|---:|
| BINANCE/BTCUSDT/mark_index_price | 120 | 123.628 | 150.156 | 161.474 | 211.744 | 73.226 |
| BINANCE/BTCUSDT/trade | 371 | 131.515 | 188.369 | 224.310 | 236.418 | 161.551 |
| BINANCE/ETHUSDT/mark_index_price | 120 | 128.756 | 181.198 | 209.775 | 211.934 | 96.785 |
| BINANCE/ETHUSDT/trade | 307 | 126.992 | 190.452 | 211.739 | 239.268 | 160.767 |
| OKX/BTC-USDT-SWAP/mark_index_price | 614 | 385.625 | 985.058 | 1343.653 | 1707.399 | 913.008 |
| OKX/BTC-USDT-SWAP/trade | 267 | 82.607 | 103.174 | 112.896 | 120.435 | 72.996 |
| OKX/ETH-USDT-SWAP/mark_index_price | 619 | 332.750 | 927.811 | 1309.909 | 1885.003 | 865.238 |
| OKX/ETH-USDT-SWAP/trade | 271 | 80.675 | 105.900 | 119.849 | 138.854 | 74.511 |

Boundary: source envelope time -> reader observes the exact canonical
partition/offset under read_committed. This is an UPPER BOUND on visibility,
including observer fetch/decode/scheduling; not a broker commit clock. Source
clock synchronisation was not independently calibrated. MARK component cadence,
lineage and quiet eligibility are NOT certified by envelope timing alone.
Core `rust/qdl-realtime-core/src/lib.rs:1760` sets TRADE published_at_ns from
raw.received_at_ns; it is NOT a trustworthy commit timestamp. Raw timings remain
in evidence; do not subtract unrelated quantiles to allocate delay.

No sampled canonical tail of5-11s in THIS window. This cannot attribute a
historical/different-window TS tail or prove every event is fast. For attribution,
join TS SDK/apply/ACK/cursor trace on exact identity/generation/partition/offset
within this UTC interval. No TS trace rearm or restart was performed. G4 all-feed
phase-D baseline and stage100 remain open; no inverse or other-symbol extrapolation.

Observed writer already changed by another operator to162ea147668f, started
2026-10-09T07:31:40Z, restarts0/OOMfalse. Do not compare with old5849d716 trace as
if same runtime. Query51450612, core trio658a9570; no changes made by this turn.

Evidence root `/home/bobby/.local/state/qdl-v2/g4-trade-mark-20261009`.
- `summary.json` SHA256 `d603e34bd6a800c305a3c9142f58a4a989698786ec773cf1f97ba5afc65b0e30`.
- `observer.py` SHA256 `66ce6625d664641eac2fc5aa50d778c654ad09529c7d17d17b79d79609389624`.
- `capture/observations.jsonl` SHA256 `7998bb5b35107ef75a134f4e61e5e2292e3caec6536b3d9bb50d558f8607f51e`.
- `capture/receipt.json` SHA256 `59531419e7533599f8d166f75743ed2515200d97499e7908289e6074cf437246`.

Command: python3 -B $ROOT/observer.py --launch --mode canonical --seconds120
--tail-records0 --max-records10000 --sample-ms100 --max-output-bytes16777216
--output $ROOT/capture (CLI uses a space between flag and value).
Worker DONE and host process exit0; no remaining observer session. Bounded
metadata retained, no payload values or credentials. No extra TS subscription.

## Build lock audit

No `uv sync` found in tracked DL Dockerfiles, workflow YAML or shell build paths.
- Python Dockerfile14-20 copies pyproject.toml + poetry.lock and uses
  Poetry2.3.4 install --only main. Dockerfile25-29 runs
  verify_runtime_dependencies.py: locked main versions and installed RECORD
  hashes; reused dependency images must be digest-pinned. This is not the same
  unlocked-uv application-resolution bug.
- Rust runtime/core Dockerfiles use cargo build --locked and Cargo.lock.
- Residual reproducibility risk DOES exist: Dockerfile20 upgrades
  setuptools>=78.1.1 outside the main lock; Poetry's bootstrap dependencies and
  apt-get upgrade/install resolve against current indexes. The lock verifier
  only checks locked main entries and RECORD integrity, not a complete immutable
  OS/toolchain closure or wheel-archive hashes. Do not promise byte-identical
  builds from application SHA alone. Consolidated-build guide already records
  this limitation; fix requires reviewed dependency/toolchain pinning, not adding
  a meaningless uv flag or silently reusing an unverified mutable image.
No dependency/image change made in this audit. Before final build, freeze builder
and runtime package inventory/artifacts and record resulting immutable digests;
any changed dependency requires affected tests and packaged verification.

## Verification and cleanup

Export validated by actual isolated_config without generating material.
Host test attempt:36 discovered,2 import errors (missing pandas/redis); not PASS.
Same source in existing image77682eea, --rm/networknone/read-onlysource,
512MiB/.5CPU/pids128/tmpfs64MiB:
`python -B -m unittest tests.test_runtime_dependency_image
 tests.test_kn_dependency_boundary tests.test_p184_dr05_dl_fault`:
38PASS0FAIL0ERROR0SKIP in62.692s. Includes offline material/key tests only,
not a new Kafka/TS acceptance. Test container qdl-g4-handoff-unit-20261009 absent
after exit. No builds/volumes/image creation; active/rollback retained, no prune.
Canonical DL /home/bobby/data_layer main5bf5cd5, publishedv2.2.2 per ledger;
feature fix/p184-dl-closure. v2.2.3 HOLD; no release or runtime certification.
