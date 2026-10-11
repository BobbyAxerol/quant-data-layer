# DL Release Readiness - rc2 / Deployed KN, 2026-10-07

**Decision: HOLD / NOT READY to claim the complete rc2 release production-qualified.** This is the requested read-only report, not a release certificate or permission to deploy. The active-capture source improvement is verified, but current full-stack CPU exceeds the existing gate; consumer cache/stream observations still need attribution and DR07 acceptance. No interface replacement is currently required by these findings.

Owner boundary: DL release depends on TS **DR01-DR08 (including DR07) + broker delta**, not completion of TS Paper features. No E03/E04 rerun, new TS phase, orders, runtime changes or new consumer/quota. Existing Phase C safety/resource/coherence gates remain mandatory.

## Evidence And Scope

- Worktree `/home/bobby/.worktrees/data-layer-p184-closure`, branch `fix/p184-dl-closure`. Capacity slice `a7587b2`; application rc2 source `43bbcf0209f49e9cc1af6802af8f8a207a53ee3c`.
- Canonical `/home/bobby/data_layer`, `main@5bf5cd540498b82959342c7cfb2ebeaac3007426`, tag ancestry `v2.2.2-3-g5bf5cd5`. Canonical owner changes untouched. Only this active feature worktree retained.
- rc2 Rust `sha256:69e3b2b214331a7ebb6a110dd8159c899bbbd6439aaf87293c8c68d145e3713d`; Python `sha256:77682eea5054ae9ddbd858323b083286669a55eeb4bc7bc065b04bae8854e32f`. Neither is deployed. These images already existed; no build.
- Current TS writer `market_data_service_p184w`, image `sha256:9150385e2bb3596793b6ee209dbc99696c3ed07439d6f4199356f417a97b6105`; its qdl-sdk package is **2.0.7**, not candidate 2.1.0. Its binding reader version and qdl-sdk version are separate contracts.
- External evidence root `/home/bobby/.local/state/qdl-v2/release-readiness-20261007`. All tables below identify measured versus estimated/inherited quantities. Each PASS is scoped to its row, not blanket execution certification.

## Verdict Matrix

| Item | Verdict | Basis / remaining condition |
| --- | --- | --- |
| IDR-062 active paired correctness | PASS | Identical capture/config and delta chains; same-event snapshots n=270 Binance,312 OKX. |
| Current whole stack <=5 vCPU | FAIL | 5.9618 mean,6.2197 p95,6.3726 max;60 intervals/307.156 s before isolated replay. |
| rc2 whole-stack CPU and coherence | NOT_MEASURED | Offline core only; post-core 120 samples/venue still required. |
| All12 app roles /5 infrastructure roles remain running | PASS | Same container IDs/images/restarts before/after. Existing BAR-edge restart count3 retained. |
| Current TS60 Query usability | FAIL for 100%-usable claim | 119/120 usable; one exact-view TRADE freshness refusal, not an unexplained SDK success. |
| Public contract preservation | PASS for reviewed candidate | Existing endpoints unchanged; SDK2.1.0 response tolerance; V3 r22 additive policy binding, caller compatibility gate retained. |
| Continuous Stream delivery | FAIL for uninterrupted claim | Active Stream overflow counter +4 in60 metric samples over approx10 min; no new decode/reader errors. |
| Exact source-event -> Redis SET/ACK latency / alpha cache | NOT_MEASURED | Possible observer lag; bounded GET availability upper bounds below, no alpha cache process measured. |
| All four latency quantities including delivery lag | NOT_MEASURED in full | Source event age available; publication timestamp absent from served cache views, no fabricated commit time. |
| DR01-DR08 /broker delta handoff | NOT_MEASURED by this audit | Owner/Claude must attach current receipts; TS Paper feature construction is not a dependency. |

## A. Latency, Milliseconds

Read path: actual TS adapter/identity/policy -> HTTPS Query -> SDK validation. 120 serial reads, one per second, one inflight,60 products on each replica. TLS aliases `query_v2_1`/`query_v2_2`, both actual KN Query replicas. No additional subscriptions; no bypass of freshness or TLS. Source reference: `scripts/benchmark_kn_ts_consumer.py:102` through line135; only its snapshot read logic was reused, NOT the isolated bridge/writer phase. External commands: `python3 -B request-launch-tls.py` (inside writer via `docker exec -i ... python -B -`).

| Feed | Request->validated usable p50/p95/max ms (n successes) | Durable-view event age at SDK return p50/p95/max ms (n) | Refused/total |
| --- | --- | --- | ---: |
| BAR | 20.68/45.57/47.49 (n=20) | 18669.75/52430.53/52690.49 (n=20) | 0/20 |
| BOOK_DELTA | 18.17/56.21/123.14 (n=20) | 401.62/533.01/579.98 (n=20) | 0/20 |
| BOOK_SNAPSHOT | 29.49/50.72/51.39 (n=20) | 1061.58/1356.35/1368.47 (n=20) | 0/20 |
| MARK_INDEX_PRICE | 34.01/62.29/64.68 (n=20) | 644.33/1433.34/1464.64 (n=20) | 0/20 |
| QUOTE | 12.27/91.69/93.56 (n=20) | 328.71/473.29/582.06 (n=20) | 0/20 |
| TRADE | 13.66/46.89/46.89 (n=19) | 464.79/2828.97/2828.97 (n=19) | 1/20 |

These per-feed samples are pooled across venues/replicas; per-binding rows are below. n=19/20 is small: p95 is a nearest-rank sample statistic (n=19 p95 equals maximum), **no p99**. Initial clock includes route lookup, SDK request/server queue, decode/validation; pacing wait before starting the request is not counted. No concurrency queue was imposed.

Durable-view event age means client receipt wall-clock minus the **source event timestamp of the returned canonical/materialized view**, BAR uses close time. It is not age since Kafka commit, and not request duration. A 52-second-old final1m BAR is not a 52-second HTTP response. MARK combined timestamp is not a substitute for each component eligibility/quiet proof.

Refusal retained: Query replica1, OKX BNB-USDT-SWAP TRADE; request79.78 ms, exact-view freshness4262 ms, event STALE, session LIVE/liveness59 ms, no gap, complete=true, execution_eligible=false, LAST_EVENT_STALE. This is correct blocking at3,000 ms; no simultaneous provider TRADE capture, so quiet-market versus delayed pipeline attribution is NOT_MEASURED. Second replica positive read does not erase this refusal.

### Delivery Lag And Actual Cache Boundary

Delivery lag (source event -> canonical publication, matching `scripts/kn_native_slice_probe.py:387`) is **NOT_MEASURED for QUOTE/TRADE/MARK/BOOK/BAR** in this read-only run: served TS cache does not preserve `published_at_ns`. No new Stream identity/subscription was opened to recover it. `received_at_ns` is provider ingest, NOT Kafka commit/publication. Commit-to-consumer is likewise NOT_MEASURED. Aggregate Stream dispatch histogram is not a per-feed replacement.

Existing `scripts/measure_kn_ts_cache_delivery.py:66` was run passively first180s: Redis closed the PUBSUB connection and it produced no summary. Preserved failure. A bounded60s copy retained partial/error output without changing runtime;37,723 notifications,50 groups, no missing-key/watermark error, but notification-derived ages approximately20-44 s were much larger than later direct GET ages; these were not simultaneous. The difference is not conclusively attributable to the observer alone. It computes age of the queued notification while permitting a newer cached watermark, so those numbers are **observer-inclusive upper bounds, not a TS write-delay certificate**. BAR absent in that notification window. No claim that changing an observer fixes a runtime issue.

A separate1Hz pipelined GET observer then read all60 actual TS cache keys for30 rounds:1,800 reads, zero missing keys, no writes. Boundary: event/BAR-close -> external observation of present TS cache; excludes alpha caches and does not timestamp actual SET ACK. Values below include repeated/current views and stored quality is NOT revalidated into execution eligibility. This is useful consumer-visible age, not exact event-application timing.

| Feed | Current cache age p50/p95/max ms (n) | New watermark after initial round: first-observed availability upper bound ms (n) | Verdict |
| --- | --- | --- | --- |
| BAR | 15663.58/60184.83/61343.88 (n=300) | 2208.46/2208.46/2208.46 (n=10) | Measured age; exact apply latency NOT_MEASURED |
| BOOK_DELTA | 7042.15/19358.58/22825.17 (n=300) | 7850.59/20401.21/22825.17 (n=189) | Measured age; exact apply latency NOT_MEASURED |
| BOOK_SNAPSHOT | 1339.19/2043.93/2436.88 (n=300) | 1324.19/2003.98/2436.88 (n=267) | Measured age; exact apply latency NOT_MEASURED |
| MARK_INDEX_PRICE | 1983.34/3147.93/3343.88 (n=300) | 1830.17/2331.13/3208.46 (n=205) | Measured age; exact apply latency NOT_MEASURED |
| QUOTE | 863.15/1301.52/1648.88 (n=300) | 886.98/1299.88/1648.88 (n=288) | Measured age; exact apply latency NOT_MEASURED |
| TRADE | 1023.59/8603.54/15411.21 (n=300) | 920.45/2679.88/4521.93 (n=229) | Measured age; exact apply latency NOT_MEASURED |

**Open attribution:** BOOK_DELTA cache reached22,825 ms despite Query BOOK_DELTA max580 ms in a different, nearby window. Snapshot cache max2437 ms exceeds a2,000 ms package-use age threshold at that observation. MARK age alone does not decide quiet/component eligibility. These observations cannot certify uninterrupted execution or blame a specific pipeline segment without paired watermark tracing. Hand them to DR07; do not silently waive them or trigger a new DL rewrite. One stored value per feed being present is not readiness.

Measurement harness errors preserved: first120 reads used TLS-incompatible `query_kn_*` aliases and all failed ConnectError before application response; two-request alias check succeeded with canonical TLS names; final120-read run uses correct aliases. This was an audit setup error, not120 production service failures. Initial quota lookup omitted the `:identity` prefix and returned null; corrected using `qdl/security/data_plane.py:190` and actual key namespace before issuing requests. No quota headroom was inferred from the incorrect lookup.

## B. Runtime Health And Resources

Command/method: bounded docker inspect, cgroup cpu.stat/memory.current at5s intervals using **only** `kn_production_watch.sample()`; never its main/auto-stop path.61 samples/60 deltas over307.156s. CPU columns are measured mean/p95/max vCPU; throttle is delta nr_throttled/nr_periods, not CPU time lost. RSS uses one later `docker top -eo pid,rss,comm` sum of role processes (n=1; shared pages can double-count), not tini PID RSS. Cgroup peak includes page cache and is not RSS.

| Role | Image digest prefix | Restarts / OOM | CPU mean/p95/max (n=60) | throttle periods % | RSS MiB (n=1) | cgroup peak/cap MiB |
| --- | --- | --- | --- | ---: | ---: | ---: |
| ingestor_binance_usdm | `0f6876e16e51` | 0 / False | 0.161/0.194/0.232 | 0.521 | 41.4 | 39.4/256.0 |
| query_kn_1 | `514506122111` | 0 / False | 0.161/0.176/0.385 | 0.035 | 693.2 | 714.1/1536.0 |
| query_kn_2 | `514506122111` | 0 / False | 0.134/0.151/0.156 | 0.000 | 606.6 | 612.8/1536.0 |
| market_projector_1 | `a696cfba6ab3` | 0 / False | 0.141/0.150/0.186 | 0.000 | 145.3 | 143.9/512.0 |
| market_projector_2 | `a696cfba6ab3` | 0 / False | 0.156/0.172/0.214 | 0.000 | 158.2 | 157.5/512.0 |
| stream_kn_1 | `1f13408fe946` | 0 / False | 0.146/0.173/0.266 | 0.130 | 393.4 | 391.6/512.0 |
| stream_kn_2 | `1f13408fe946` | 0 / False | 0.076/0.079/0.086 | 0.000 | 333.9 | 331.0/512.0 |
| ingestor_okx_swap | `0f6876e16e51` | 0 / False | 0.145/0.174/0.196 | 0.195 | 39.0 | 43.4/256.0 |
| kafka3 | `9516fb7634ba` | 0 / False | 1.053/1.093/1.115 | 0.554 | 899.4 | 1535.9/1536.0 |
| kafka2 | `9516fb7634ba` | 0 / False | 1.026/1.065/1.076 | 0.684 | 860.2 | 2048.0/2048.0 |
| kafka1 | `9516fb7634ba` | 0 / False | 0.903/0.962/0.982 | 2.345 | 1061.1 | 1536.0/1536.0 |
| rust_core | `658a9570c5fc` | 0 / False | 0.599/0.639/0.670 | 0.065 | 129.6 | 128.3/256.0 |
| rust_core_2 | `658a9570c5fc` | 0 / False | 0.419/0.447/0.510 | 0.000 | 162.7 | 160.4/256.0 |
| rust_core_3 | `658a9570c5fc` | 0 / False | 0.172/0.185/0.204 | 0.033 | 130.1 | 127.7/256.0 |
| binance_bar_edge | `f7351c3bda08` | 3 / False | 0.568/0.630/0.639 | 2.851 | 184.1 | 183.9/512.0 |
| market_cache | `dfa18828cbc0` | 0 / False | 0.081/0.088/0.090 | 0.000 | 1153.3 | 1154.5/9216.0 |
| stable_redis | `dfa18828cbc0` | 0 / False | 0.021/0.026/0.029 | 0.000 | 8.7 | 8.9/160.0 |

Exact full image digests/config retention set are in the [Phase C packet](p184-dl-phase-c-packet.md) and hashed inventory. No restart increment or OOM increment during observation. Projector status has no new conflict/rewind/cache reconnect and owned partitions remain split even/odd. Historical projector2 abort count1 is not zero-history proof. Cache has about1.15GiB charged against9GiB cap; near-cap Kafka charged memory includes file cache, so cannot infer imminent heap OOM from memory.current alone.

Logs bounded to10 min/1,000 lines per role: no WARN/ERROR severity for core/Stream/projectors; projector string `stale:0` caused keyword matches and is NOT an error. BAR edge has INFLIGHT_CAPACITY deferrals and known Binance3d history discontinuity errors. Binance3d stays explicitly excluded by owner, not secretly repaired or counted as complete. No output from several docker logs paths is not proof of no errors; stable_redis docker logs timed out. Current TS log had a MARK/INDEX reconnect, not enough evidence for cause. Process health alone is insufficient.

### Kafka Partition Lag (records, one snapshot per group)

Command: existing broker `kafka-consumer-groups.sh --bootstrap-server kafka1:9092 --command-config /etc/kafka/secrets/admin.properties --describe --group <group>`. No offset commit/reset/group membership change. Groups sampled sequentially; not an atomic cluster snapshot.

| Group | Topic | Partition:lag |
| --- | --- | --- |
| kn-projector-v3-a | md.canonical.v2 | 0:34, 1:27, 2:15, 3:38, 4:54, 5:28 |
| kn-projector-v3-b | md.bars.v2 | 0:1, 1:1, 2:1, 3:1, 4:1, 5:1 |
| kn-projector-v3-b | md.latest.v2 | 0:41, 1:40, 2:45, 3:61, 4:25, 5:48 |
| qdl-v2-realtime-core-v2 | md.raw.realtime.v2 | 0:40, 1:43, 2:4, 3:20, 4:27, 5:14 |
| qdl-v2-book-resync-ingestor_okx_swap-okx-public-001 | md.quarantine.stable.v1 | 0:76, 1:1, 2:1, 3:1, 4:1, 5:1 |
| qdl-v2-book-resync-ingestor_binance_usdm-binance-public-book-001 | md.quarantine.stable.v1 | 0:76, 1:1, 2:1, 3:1, 4:1, 5:1 |

No under-replicated partitions returned by read-only kafka-topics --describe --under-replicated-partitions (exit0). Lag trend/maximum recovery during a fault is NOT_MEASURED in this read-only audit; one bounded lag sample is not a recovery test. Keep six10s samples during proposed rollout as packet requires.

## C. TS60 r22 Capability And Observed Reads

Verdict: PASS for declared roster/route preservation; FAIL for claiming every observation execution-usable.

Actual mounted schema `qdl.v2.consumer-route-binding.v2`, revision22, manifest10;60 products. Ten linear perpetual instruments, six feeds each; BAR interval1m. Explicit V2_PRIMARY and fail-closed fallback; no silent V1 price substitution. No inverse/spot/futures/options inferred from this60-product claim, though17 acquisition BOOK bindings include other products. TS paper manifest has61 requirements (entitlement roster is not identical to sealed60-product usage); sandbox has64, requiring its own identity checks.

| Venue / native product | Feed | Interval | Binding max freshness ms | Current replica1/2 usable | Request max ms (2 observations) |
| --- | --- | --- | ---: | --- | ---: |
| BINANCE / BNBUSDT | BAR | 1m | 180000 | yes/yes | 22.86 |
| BINANCE / BNBUSDT | BOOK_DELTA | - | 2000 | yes/yes | 27.52 |
| BINANCE / BNBUSDT | BOOK_SNAPSHOT | - | 60000 | yes/yes | 29.15 |
| BINANCE / BNBUSDT | MARK_INDEX_PRICE | - | 2000 | yes/yes | 62.29 |
| BINANCE / BNBUSDT | QUOTE | - | 2000 | yes/yes | 12.27 |
| BINANCE / BNBUSDT | TRADE | - | 3000 | yes/yes | 14.07 |
| BINANCE / BTCUSDT | BAR | 1m | 180000 | yes/yes | 18.50 |
| BINANCE / BTCUSDT | BOOK_DELTA | - | 2000 | yes/yes | 123.14 |
| BINANCE / BTCUSDT | BOOK_SNAPSHOT | - | 60000 | yes/yes | 38.93 |
| BINANCE / BTCUSDT | MARK_INDEX_PRICE | - | 2000 | yes/yes | 34.61 |
| BINANCE / BTCUSDT | QUOTE | - | 2000 | yes/yes | 12.03 |
| BINANCE / BTCUSDT | TRADE | - | 3000 | yes/yes | 46.89 |
| BINANCE / DOGEUSDT | BAR | 1m | 180000 | yes/yes | 45.57 |
| BINANCE / DOGEUSDT | BOOK_DELTA | - | 2000 | yes/yes | 19.41 |
| BINANCE / DOGEUSDT | BOOK_SNAPSHOT | - | 60000 | yes/yes | 38.13 |
| BINANCE / DOGEUSDT | MARK_INDEX_PRICE | - | 2000 | yes/yes | 34.47 |
| BINANCE / DOGEUSDT | QUOTE | - | 2000 | yes/yes | 14.18 |
| BINANCE / DOGEUSDT | TRADE | - | 3000 | yes/yes | 13.16 |
| BINANCE / ETHUSDT | BAR | 1m | 180000 | yes/yes | 20.68 |
| BINANCE / ETHUSDT | BOOK_DELTA | - | 2000 | yes/yes | 56.21 |
| BINANCE / ETHUSDT | BOOK_SNAPSHOT | - | 60000 | yes/yes | 28.56 |
| BINANCE / ETHUSDT | MARK_INDEX_PRICE | - | 2000 | yes/yes | 64.68 |
| BINANCE / ETHUSDT | QUOTE | - | 2000 | yes/yes | 12.78 |
| BINANCE / ETHUSDT | TRADE | - | 3000 | yes/yes | 22.94 |
| BINANCE / SOLUSDT | BAR | 1m | 180000 | yes/yes | 17.72 |
| BINANCE / SOLUSDT | BOOK_DELTA | - | 2000 | yes/yes | 31.74 |
| BINANCE / SOLUSDT | BOOK_SNAPSHOT | - | 60000 | yes/yes | 27.61 |
| BINANCE / SOLUSDT | MARK_INDEX_PRICE | - | 2000 | yes/yes | 32.78 |
| BINANCE / SOLUSDT | QUOTE | - | 2000 | yes/yes | 39.71 |
| BINANCE / SOLUSDT | TRADE | - | 3000 | yes/yes | 12.20 |
| OKX / BNB-USDT-SWAP | BAR | 1m | 180000 | yes/yes | 27.97 |
| OKX / BNB-USDT-SWAP | BOOK_DELTA | - | 2000 | yes/yes | 13.52 |
| OKX / BNB-USDT-SWAP | BOOK_SNAPSHOT | - | 60000 | yes/yes | 40.69 |
| OKX / BNB-USDT-SWAP | MARK_INDEX_PRICE | - | 2000 | yes/yes | 34.17 |
| OKX / BNB-USDT-SWAP | QUOTE | - | 2000 | yes/yes | 12.42 |
| OKX / BNB-USDT-SWAP | TRADE | - | 3000 | refused/yes | 79.78 |
| OKX / BTC-USDT-SWAP | BAR | 1m | 180000 | yes/yes | 42.97 |
| OKX / BTC-USDT-SWAP | BOOK_DELTA | - | 2000 | yes/yes | 18.17 |
| OKX / BTC-USDT-SWAP | BOOK_SNAPSHOT | - | 60000 | yes/yes | 31.19 |
| OKX / BTC-USDT-SWAP | MARK_INDEX_PRICE | - | 2000 | yes/yes | 34.05 |
| OKX / BTC-USDT-SWAP | QUOTE | - | 2000 | yes/yes | 12.56 |
| OKX / BTC-USDT-SWAP | TRADE | - | 3000 | yes/yes | 11.91 |
| OKX / DOGE-USDT-SWAP | BAR | 1m | 180000 | yes/yes | 21.69 |
| OKX / DOGE-USDT-SWAP | BOOK_DELTA | - | 2000 | yes/yes | 18.49 |
| OKX / DOGE-USDT-SWAP | BOOK_SNAPSHOT | - | 60000 | yes/yes | 51.39 |
| OKX / DOGE-USDT-SWAP | MARK_INDEX_PRICE | - | 2000 | yes/yes | 33.09 |
| OKX / DOGE-USDT-SWAP | QUOTE | - | 2000 | yes/yes | 11.69 |
| OKX / DOGE-USDT-SWAP | TRADE | - | 3000 | yes/yes | 22.79 |
| OKX / ETH-USDT-SWAP | BAR | 1m | 180000 | yes/yes | 47.49 |
| OKX / ETH-USDT-SWAP | BOOK_DELTA | - | 2000 | yes/yes | 25.25 |
| OKX / ETH-USDT-SWAP | BOOK_SNAPSHOT | - | 60000 | yes/yes | 35.37 |
| OKX / ETH-USDT-SWAP | MARK_INDEX_PRICE | - | 2000 | yes/yes | 34.01 |
| OKX / ETH-USDT-SWAP | QUOTE | - | 2000 | yes/yes | 11.29 |
| OKX / ETH-USDT-SWAP | TRADE | - | 3000 | yes/yes | 20.79 |
| OKX / SOL-USDT-SWAP | BAR | 1m | 180000 | yes/yes | 18.55 |
| OKX / SOL-USDT-SWAP | BOOK_DELTA | - | 2000 | yes/yes | 16.07 |
| OKX / SOL-USDT-SWAP | BOOK_SNAPSHOT | - | 60000 | yes/yes | 31.63 |
| OKX / SOL-USDT-SWAP | MARK_INDEX_PRICE | - | 2000 | yes/yes | 44.48 |
| OKX / SOL-USDT-SWAP | QUOTE | - | 2000 | yes/yes | 93.56 |
| OKX / SOL-USDT-SWAP | TRADE | - | 3000 | yes/yes | 14.52 |

Quality contract: QUOTE ON_CHANGE/OBSERVE does not invent ticks; session/generation/gap checks remain. TRADE session-live and executable LAST price are distinct; stale LAST cannot price an order. MARK/INDEX requires same-instrument component lineage, component/session proof and native timestamps. BOOK requires sequence integrity, generation and matching execution intent; a60s generic binding cap does not override2s package/top-coherence gate. BAR is final-bar validity/close recency, not tick SLA. Binding policy V3 seals these distinctions; it does not widen entitlement.

D1 deferred: Binance quiet BOOK heartbeat/materialization can still legitimately expire. D3 separate: ingestor, BAR edge, Redis, Kafka/host share failure domains; no independent HA or zero-downtime claim. Backup through Kafka-native Stream still shares Kafka/host. These limitations are in the runbook/packet, not solved by green Query reads.

## D. Endpoints, Schemas And Change Policy

Endpoint source: `qdl/api_v2/router.py:738` through1199 (mounted under `/v2`); gRPC `contracts/proto/qdl/query/v2/query.proto:190`. Paths are resource selectors with feed/interval/requirements, not separate routes per symbol.

| Surface | Consumer purpose | Contract |
| --- | --- | --- |
| GET /v2/instruments; /v2/instruments/{identity} | TS/alpha metadata/native instrument resolution | V2 typed instrument schema |
| GET /v2/market-data/{instrument_uid}/snapshot | latest TRADE/QUOTE/MARK/BOOK/BAR | V2 SnapshotResponse; typed quality/authority |
| GET /v2/market-data/{instrument_uid}/warmup; /history | alpha bounded final BAR history | V2 WarmupResponse; interval + limit |
| POST /v2/market-data/warmup:batch | universe1d / multi-symbol equal requested limit | V2 BatchResponse, require_all semantics; no invented pre-listing history |
| POST /v2/market-data/reference:batch | funding, OI, long/short, taker and other admitted references | V2 ReferenceBatchResponse; venue-specific capability/units |
| GET /v2/feeds/{instrument_uid}/status | feed quality/liveness | V2 FeedStatusResponse |
| POST /v2/system/readiness:check; GET /v2/system/readiness | scoped readiness / service summary | V2, not an order-admission guarantee |
| GET /v2/data-quality/gaps | bounded retained-window diagnostics | V2 GapListResponse; incomplete/partial is not full-history PASS |
| MarketDataStreamService.Subscribe / Replay | event stream, cursor recovery | protobuf qdl.query.v2; cursor v3 consumer/realm/generation binding |
| MarketDataStreamService.GetSnapshot / GetFeedStatus | stream read/backup status | same scoped typed contract, not independent HA |

Public endpoint/schema presence is source-reviewed here; this audit directly exercised snapshot reads, not all warmup/reference/diagnostic APIs. Inherit prior scoped packet evidence; do not label uncalled APIs live-PASS on this window. Alpha universe is batch1d, not automatic subscription entitlement for all universe members.

Candidate qdl-sdk2.1.0 is locally packaged/tested (CERTIFICATION_LEDGER.md:2114); actual TS writer qdl-sdk2.0.7. Binding V1/V2 readers retained, V3r22 additive policies staged, not mounted. Release r22 universal hash `b4e43688ef714e6cef28c0bf2e565b0fbd7b5c70703c9271e1189a9c5bf99afe`; actual V2 binding hash `60118db19bfca5d6d39aec83c1f0b6a0b4068cfe085d4a786e554792e99ebd5d`. Candidate V3 file hash and rollback file are in packet. SDK version, release revision and JWT manifest revision are not interchangeable.

**Required endpoint replacement identified now: none.** Intended evolution is additive optional response fields/new products under versioned manifests; existing request semantics remain strict. New venue/product/FX/collateral capability can require explicit new schema/version/entitlement, not a guarantee that every possible future feature fits today's schema. Removal/change of a required field, unit, cursor meaning or freshness policy requires explicit versioned migration; no unreviewed breaking interface change is authorized. Adding an alpha uses an existing allowed manifest or a versioned config/identity activation with compatibility preflight, not rebuilding the DL core merely to add a consumer name.

## E. Capacity, Quota And Headroom

Verdict: PASS for observed request quota headroom; FAIL for <=5vCPU; NOT_MEASURED for added-consumer concurrency capacity.

Actual manifests from Query mounts and shared Redis minute counters (`...:identity:quota:minute:<sha256-id-prefix>:<minute>`). Table uses completed minute before probe and a later completed minute, not partially elapsed-minute extrapolation. Query+Stream request accounting shares identity budget; stream counts are separate. Limits are not per-container freebies.

| Consumer | JWT/manifest rev | req/min | prior used / remaining | later used / remaining | batch / warmup / streams |
| --- | ---: | ---: | --- | --- | --- |
| monitoring.multivenue.stable | 2 | 180 | 0 / 180 | 0 / 180 | 50 / 2000 / 20 |
| alpha.binance.paper.stable | 14 | 2400 | 0 / 2400 | 0 / 2400 | 50 / 10000 / 60 |
| alpha.okx.paper.stable | 14 | 2400 | 0 / 2400 | 0 / 2400 | 50 / 10000 / 60 |
| alpha.vn.paper.stable | 1 | 180 | 0 / 180 | 0 / 180 | 50 / 2000 / 20 |
| trading-system.paper.stable | 10 | 1500 | 961 / 539 | 885 / 615 | 50 / 2000 / 50 |
| qdl.crypto.reference-l2.stable | 6 | 120 | 0 / 120 | 0 / 120 | 100 / 10000 / 24 |
| alpha.binance.live.stable | 1 | 2400 | 0 / 2400 | 0 / 2400 | 50 / 10000 / 60 |
| alpha.binance.sandbox.stable | 1 | 2400 | 0 / 2400 | 0 / 2400 | 50 / 10000 / 60 |
| alpha.okx.live.stable | 1 | 2400 | 0 / 2400 | 0 / 2400 | 50 / 10000 / 60 |
| alpha.okx.sandbox.stable | 1 | 2400 | 0 / 2400 | 0 / 2400 | 50 / 10000 / 60 |
| trading-system.live.stable | 1 | 1500 | 0 / 1500 | 0 / 1500 | 50 / 2000 / 50 |
| trading-system.sandbox.stable | 2 | 1500 | 0 / 1500 | 0 / 1500 | 50 / 2000 / 50 |

50 active subscriptions observed on Stream1, zero on Stream2; role limit384 subscriptions +32 replay RPCs each. Slots remaining334/384 at those instants are technical slot counts, **not a promise of334 more consumers**. Per-identity active occupancy is not exported in these sampled metrics; do not infer all50 belong to one identity. If a60-stream alpha identity uses2-5 subscriptions per process, theoretical per-replica identity quota supports12-30 such processes before other requirements; not a load certificate. The prior50-logical-alpha+TS60 acceptance in candidate budget is historical evidence, not a rerun or current5-vCPU PASS.

Current container CPU ceilings sum 16.50 vCPU. They are burst ceilings, not observed demand and not a shared hard cap. Sum of per-role sampled cgroup peaks 9.10 GiB is conservative/non-simultaneous; sum caps 20.91 GiB. CPU budget headroom against5 is **negative0.962 vCPU**. No extra consumer admission capacity can be certified under that budget now. [Capacity envelope](p184-idr062-capacity-envelope.md) distinguishes measured active pair,170snapshot/s scenario and downstream estimates; no resource cap changed.

## F. Stability And Recovery

Verdict: PASS for unchanged process/restart inventory during observation; FAIL for an uninterrupted-stream claim; rc2 fault recovery NOT_MEASURED.

| Role | Current process start UTC | Current recorded restart count |
| --- | --- | ---: |
| ingestor_binance_usdm | 2026-10-02T12:24:58.369037654Z | 0 |
| query_kn_1 | 2026-10-02T11:13:49.101943789Z | 0 |
| query_kn_2 | 2026-10-02T11:12:13.488673234Z | 0 |
| market_projector_1 | 2026-10-01T12:16:20.78036445Z | 0 |
| market_projector_2 | 2026-10-01T12:15:13.766866129Z | 0 |
| stream_kn_1 | 2026-10-01T12:10:50.691735492Z | 0 |
| stream_kn_2 | 2026-10-01T12:07:24.322915897Z | 0 |
| ingestor_okx_swap | 2026-10-01T05:51:24.840836395Z | 0 |
| kafka3 | 2026-10-01T03:58:43.963843285Z | 0 |
| kafka2 | 2026-10-01T03:57:34.401411155Z | 0 |
| kafka1 | 2026-10-01T03:56:13.899839854Z | 0 |
| rust_core | 2026-09-30T17:46:56.312111099Z | 0 |
| rust_core_2 | 2026-09-30T17:46:23.058376424Z | 0 |
| rust_core_3 | 2026-09-30T17:45:38.534537186Z | 0 |
| binance_bar_edge | 2026-10-01T04:03:57.875233715Z | 3 |
| market_cache | 2026-09-27T05:29:58.000340326Z | 0 |
| stable_redis | 2026-09-16T06:38:55.588316335Z | 0 |

These start timestamps are process uptime, not duration of error-free eligible data. Stream1 metrics60 samples/approx10min:449,679 delivered, overflow+4, expired/refused/reader/decode/duplicate deltas0. Stream2 delivered0,overflow delta0. Historical counters remain in evidence; an idle replica is not exercised failover.

BAR edge18s restart is **inherited historical measurement** at main plan line43467, not current rc2 fault testing or a bound for all roles. Recovery under broker failure, restart/rollback rc2, rolling continuity and alpha consumer cache latency: NOT_MEASURED here, no runtime fault injected. Paired replay used an isolated broker and source capture, not production HA.

## G. Handoff To TS

**TS can rely on now:** existing versioned Query/gRPC interfaces and r22 product identities; strict fail-closed typed quality; preserved source timestamps/units/cursor isolation; observed119 usable snapshot reads with measured latency; native top-change fix correctness on the active ETH pair. Continue coding against these contracts without waiting for unrelated Paper construction.

**TS must not rely on yet:** rc2 already deployed; all60 feeds always eligible; <=5vCPU capacity; continuous Stream/TS cache freshness during this window; inverse entitlement inferred from linearTS60; daily-universe history being complete before listing; quiet heartbeat supplying a new price; independent HA/zero downtime; exact E2E-to-alpha/Redis-ACK or canonical delivery lag that was not measured.

**Release boundary:** attach DR01-DR08 including attribution/closure in DR07 and broker delta. Existing CPU gate fails before Phase C, so hold and present the measured mismatch for owner decision, not silently raise budget. Post-core TS coherence remains after the three Rust cores, not a precondition that old core must pass. No TS Paper feature building added to the release dependency.

## Cleanup, Reproducibility And Limitations

All commands were reads except private evidence files and temporary isolated replay resources. No production container ID/image/config/offset/manifest/credential change; no order. Capture hashed then deleted; exact isolated containers/network/TLS keys cleaned. Bounded diagnostic cache payload hashed then deleted; no raw cursor-bearing payload committed. Two failed pytest invocations (host and existing candidate lack pytest) were recorded; no package install/build. The one disposable test container used --rm. Source tests inherited unchanged; receipt consistency assertions passed.

Docker inventory after:92 images28.19GB; BuildKit227 entries16.83GB (1.089GB reclaimable), no new builds/images. Active+rc2+explicit rollback images retained; broad prune intentionally not done under read-only task. Pair disk free102468628480 ->100905811968 bytes includes concurrent host use, not claimed reclaimed bytes. No test process is left running.

External compact evidence hashes (not raw credentials/log dumps):

| File | SHA256 |
| --- | --- |
| `active-pair/paired-receipt.json` | `381760f55659bb5b98879526d7100e118f3c1cbb82e9b05674d5b28579105aa4` |
| `active-pair/cleanup.json` | `a2fa60030476010b8a6453d9c6f2ac6e3a8cd849fa16d2e284ce33ebb901e4e0` |
| `resources-summary.json` | `06ad61ff7b202733830640c934be0473f168c86d40bbd71fbd4645ba441b0dca` |
| `resource-samples.jsonl` | `5313cab1e7fc5abc1f533f33b1dc74c75ca8a9ee20642d712b3d297dabf2603e` |
| `inventory-before.json` | `68dee779e1eb4f1471c02af33da78e926be80979749f4eedb2ac9bda9a2eaae9` |
| `inventory-after.json` | `cf1ffe56f1eebd38b1e5acdce9da3f050a5e022107447909089378e94f9b69b2` |
| `requests-tls.json` | `86bbd396369b424559340e960d97071bd19753ac4c3203ad98684e46f3b6a608` |
| `requests.json` | `860d529b7bf5535a2c8cfbead981ee22dcab1435f5c39a2a4a89a6b0304162a0` |
| `request-alias-check.json` | `dd6dc81b86efe566d24552fed6e976f7c26f8495ec1830ed11d24e8a1ee86354` |
| `cache-observation-bounded.json` | `96c63c629a23bb82afe257b900b1a04b03b9165fe62489c51ac93d8670bc9689` |
| `cache-observation.stderr` | `8f7426a70db8c2bfaa2203fabc6664cdb42092600b72808c4c412fe6d65a0861` |
| `cache-poll.json` | `1ad12f4d5ec633b3b83ca8be5246fee523f599d304f13f3a2a884d1c210ca04d` |
| `quota-before-corrected.json` | `de195058533dc2deb810f2f140f314e149814325c4d599b6b30ad2458378964c` |
| `quota-after.json` | `1e1c75b1b5b80c6ba2bf76701ef3fa4347028aea352fd31cc5ceb3649ea63c69` |
| `kafka-lag.json` | `a0a8443c3e77daec409753398b5cf628af8618dc3ef0cfa8fdf0ba074c0e6849` |
| `logs-summary.json` | `341904e43f147d274ef92e4bd471a7b82ebee856b10c871b2cb666e726255d0f` |
| `logs-examples.json` | `e7974240928a9d35ebad6bb93fb7533ee6740662a3c4a971db31e45992abfe3e` |
| `stream-metrics.json` | `a31429b948e3c1aab322c32d14c052779c09084be8ff8418ca538868b5051071` |
| `stream-limits.json` | `a4083513ee1366853ba0612ec5c135401b6c77886d2815235dc033d4ccf1a07d` |
| `process-rss.json` | `4c60eadebe7b2eb23cc7696381b2968580384c6a4e5999d239c3a2a41cc75ff2` |
| `ts-binding.json` | `aa85d6cf41011da8f05fac335e4b642a7d9f0a3157a660b464fb19f827172159` |
| `binding-projector-status.json` | `92971c3061422b41b29ef1861fa078ed155a3d2b0d38dcf66dcd579b1725b6b5` |

TLS alias negative diagnostic SHA256 `1fb11d834c348a68604ab2289d5656af62ac2ad81f5b961c81526935b79c2d96`: explicit CERTIFICATE_VERIFY_FAILED / hostname mismatch for query_kn_1, not provider unavailability.
