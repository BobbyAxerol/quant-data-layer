# IDR-062 Active Capture And Capacity Envelope

Status: MEASUREMENT_COMPLETE; whole-stack 5-vCPU gate FAIL on current runtime; rc2 full-stack NOT_MEASURED.

Owner scope: read-only production inventory and isolated replay. No runtime change, build, push or merge. Candidate application source `43bbcf0209f49e9cc1af6802af8f8a207a53ee3c`; harness is unchanged from `5a261e0`.

## Actual BOOK Roster

Runtime core `/runtime/core.json` (host bind `~/.local/state/qdl-v2/releases/v2.2.1-0e00c81-recovery/rust_core.json`) declares **17 unique BOOK bindings**, not 51 across three core replicas. Ingestor configs independently confirm 7 Binance and 10 OKX subscriptions. These are configured/running acquisition subscriptions, not a claim that every tick of every product is currently eligible.

| Venue | Native symbol | Core config line | Materialization timer |
| --- | --- | ---: | ---: |
| BINANCE | BNBUSDT | 1193 | 1000 ms |
| BINANCE | BTCUSDT_261225 | 1348 | 30000 ms |
| BINANCE | BTCUSDT | 1624 | 1000 ms |
| BINANCE | DOGEUSDT | 2158 | 1000 ms |
| BINANCE | ETHUSDT_261225 | 2511 | 30000 ms |
| BINANCE | ETHUSDT | 2787 | 1000 ms |
| BINANCE | SOLUSDT | 5175 | 1000 ms |
| OKX | BTC-USD-261225 | 6338 | 30000 ms |
| OKX | BTC-USD-270326 | 6361 | 30000 ms |
| OKX | ETH-USD-261225 | 6384 | 30000 ms |
| OKX | ETH-USD-270326 | 6407 | 30000 ms |
| OKX | BNB-USDT-SWAP | 7493 | 1000 ms |
| OKX | BTC-USDT-SWAP | 7925 | 1000 ms |
| OKX | DOGE-USDT-SWAP | 8501 | 1000 ms |
| OKX | ETH-USDT-SWAP | 9113 | 1000 ms |
| OKX | SOL-USDT-SWAP | 11633 | 1000 ms |
| OKX | BTC-USD-SWAP | 12731 | 1000 ms |

The two ingestor `/runtime/config.json` evidence lists are `ingestor_binance_usdm-book-lines.json` (first BOOK line 46) and `ingestor_okx_swap-book-lines.json` (first line 33). Catalog declarations alone are not used as the count. Source default at `rust/qdl-realtime-core/src/lib.rs:68` is snapshot_refresh_seconds x 1000: **six research futures use 30 s, not 1 s**; eleven perpetual bindings explicitly use 1000 ms. Candidate top-change floor is 100 ms at `rust/qdl-realtime-core/src/lib.rs:402`, applied at line 1434. Bootstrap/generation changes bypass steady-state timing; 170/s is not a hard bound during reset storms.

## Paired Higher-Activity Capture

Command: `python3 -B scripts/measure_l2_top_materialization.py run --work /home/bobby/.local/state/qdl-v2/release-readiness-20261007/active-pair --seconds 120`.
One public capture, identical provider bytes, config, resource limits, replay timing, broker and consumer. Only core image changes. Baseline `658a9570...54023`; candidate `69e3b2b2...713d`. Capture implementation: `scripts/phase114_l2_real_provider_capture.py:578`. No production Kafka replay/write.

| Venue | Window s / frames | Top changes / minute | Snapshot count old/new | msg/s old/new | protobuf bytes/s old/new |
| --- | ---: | ---: | ---: | ---: | ---: |
| BINANCE/ETHUSDT | 119.896 / 1179 | 270 / 135.12 | 118/304 | 0.984/2.536 | 7767.0/19995.4 |
| OKX/ETH-USDT-SWAP | 120.100 / 1202 | 312 / 155.87 | 121/335 | 1.007/2.789 | 8234.2/22775.4 |

Preregistered high-activity criterion: >=1.5 x old 44/41 changes/min on BOTH venues. PASS: about 3.07x/3.80x. This is a measured active window, not proof of the daily maximum.

| Venue | Delay basis | Baseline p50/p95/max ms (n) | rc2 p50/p95/max ms (n) |
| --- | --- | --- | --- |
| BINANCE/ETHUSDT | next_verified_snapshot_delay | 408/918/924 (269) | 0/0/0 (270) |
| BINANCE/ETHUSDT | matched_delay | 204/818/924 (99) | 0/0/0 (270) |
| OKX/ETH-USDT-SWAP | next_verified_snapshot_delay | 500/900/900 (312) | 0/0/0 (312) |
| OKX/ETH-USDT-SWAP | matched_delay | 200/800/900 (110) | 0/0/0 (312) |

Delay is **provider event time at top change -> source time of materialized snapshot**, not wall-clock delivery. Zero means emitted for the same source event, not zero network/commit latency. Baseline Binance: 170 superseded tops + 1 tail; OKX: 202 superseded. Candidate: zero superseded/tails; exact delta payload chains match on both venues. No p99 invented.

| Core resource | Baseline | rc2 |
| --- | ---: | ---: |
| CPU seconds | 9.81 | 10.51 |
| vCPU / 120 s | 0.08175 | 0.08758333333333333 |
| RSS high-water KiB | 21844 | 23612 |
| Resource samples | 199 | 198 |

CPU is process CPU; cgroup CPU includes sampling exec overhead and is retained separately. RSS is sampled process high-water, not whole-stack RSS.

## Steady-State Upper Envelope

17 bindings x 10 snapshots/s = **170 snapshot/s**, 70 Binance + 100 OKX. Observed mean ETH encoded sizes: 7886.10 B Binance and 8165.17 B OKX. Extrapolated payload rate: **1,368,544 B/s (1.369 MB/s)**. This byte rate uses ETH means, NOT a mathematical worst-case size across all symbols, variable decimal strings, Kafka headers/TLS or replication. Snapshot amplification measured on active pair: 639/239 = 2.6736; all BOOK messages: 3014/2614 = 1.1530.

Illustrative message-cost model (NOT measured capacity): assume every binding also emits 10 deltas/s. Baseline timer snapshots = 11 + 6/30 = 11.2/s; total 181.2/s. Candidate ceiling = 170 deltas + 170 snapshots = 340/s; multiplier R=1.8764. Query request rate stays fixed; snapshots are not 10 additional Query calls/s. Stream cost also depends on subscriptions/fan-out. No buffer/quotas changed.

| Role group | Current measured mean vCPU | Illustrative vCPU at R | Assumption |
| --- | ---: | ---: | --- |
| core | 1.1895 | 2.2319 | all group CPU scales with total BOOK message ratio; non-BOOK/base costs not separated |
| projector | 0.2978 | 0.5587 | all group CPU scales with total BOOK message ratio; non-BOOK/base costs not separated |
| query | 0.2950 | 0.2950 | fixed HTTP request rate, not measured candidate CPU |
| stream | 0.2211 | 0.4149 | all group CPU scales with total BOOK message ratio; non-BOOK/base costs not separated |

Current full-stack mean **5.9618 vCPU**, p95 6.2197, max 6.3726; n=60 intervals over 307.156 s, 17 running roles including three Kafka brokers and both Redis roles. Measurement completed BEFORE isolated replay started. The 5-vCPU gate is **FAIL for this current workload**; headroom is -0.9618 vCPU. Candidate whole-stack maximum remains **NOT_MEASURED**. Even the illustrative model holding Kafka/ingestors/edge/Redis fixed is 7.459 vCPU; not a safe capacity ceiling, since broker/Redis costs can also rise. Do not use the two-binding core result to certify <=5 vCPU.

## Evidence And Cleanup

External root: `/home/bobby/.local/state/qdl-v2/release-readiness-20261007`. Paired receipt SHA256 `381760f55659bb5b98879526d7100e118f3c1cbb82e9b05674d5b28579105aa4`. Capture SHA256 `0a116c39866bbeb325b3833a21e0d865f1c599262176f0b8aa6e3135b8394a8d`; deleted after replay/hashing. All isolated containers/network and generated TLS keys removed by unchanged harness. No image built or removed. Cleanup free bytes 102468628480 -> 100905811968; concurrent host use means this is NOT reclaimed-space accounting. Active+rc2+rollback images retained. Full resource result: `resources-summary.json`; original 61 samples: `resource-samples.jsonl`.
