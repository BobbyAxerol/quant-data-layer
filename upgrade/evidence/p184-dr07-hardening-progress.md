# P18.4 DR07 Hardening Progress - 2026-10-07

Status: IN_PROGRESS / PHASE_C_HOLD. This is not a release certificate.

## Scope And Findings

- Existing DR07 only: CPU attribution, truthful consumer measurement, BOOK reset/recovery, TRADE attribution. No Paper features or Data Layer interface changes.
- Correctness defect reproduced in TS source: a BOOK reset followed by deltas in the same batch could yield the new-generation deltas before the verified Query anchor. Narrow TS adapter fix is under test/review; not deployed.
- Source inspection: 64 BOOK deltas perform 64 MGET and 64 CAS EVAL calls sequentially. This is a performance candidate, not yet proof of the historical Stream overflows or 22.825s cache age.
- Thread CPU attribution over31.801s: TS Python1.507vCPU; Kafka network-named threads2.069vCPU combined. These are thread names, not function stacks; TS is outside the DL5vCPU denominator. No CPU configuration change or claim that the5vCPU gate now passes. A bounded Kafka2 perf sample was denied by perf_event_paranoid=4/missing CAP_PERFMON; no security setting changed. Thread attribution cannot substitute for function-level profiling.

## Observer Correction

PUBSUB drain no longer awaits a GET per notification. Bounded batched MGET samples the latest notification per channel. Matched availability requires the same source observation; superseded cache views cannot supply delivery latency. MARK compares component capture/time/price/generation rather than inventing a scalar offset. Authority/context changes cannot match. Execution and last-value paths are reported separately.

25 stdlib tests PASS: `python3 -B tests/test_kn_ts_cache_delivery_observer.py`. No pytest claim (not installed in host/candidate). Source snapshot used for the second live window precedes the final authority-context negative and sampling-label adjustment; those final diagnostic-only edits are unit-tested, not a new runtime benchmark.

Second window:60.060s,36659notifications,80cache-path groups,106MGET batches. MGET p50/p95/p99/max26.298/189.489/265.391/294.093ms. Status INCOMPLETE:2notifications exceeded the256KiB observer bound. No missing-MARK-watermark error remains. First diagnostic window is retained (502MARK watermark errors,7oversize messages), not replaced or called a production defect.

All numbers below are time-sampled **current cache event age**, not request latency, Redis SET ACK, execution eligibility or distinct source-event latency. MARK event age alone does not evaluate component/session eligibility. Samples are notification-driven, not a complete availability census. The read-only observer adds its own process/subscriber overhead; this is not a capacity workload.

| Venue/instrument/feed/path | n | p50 ms | p95 ms | max ms | Matched availability samples |
|---|---:|---:|---:|---:|---:|
| BINANCE/BNBUSDT/BAR/bar | 1 | 1137.854 | None | 1137.854 | 1 |
| BINANCE/BNBUSDT/BOOK_DELTA/execution | 106 | 270.303 | 784.167 | 938.862 | 68 |
| BINANCE/BNBUSDT/BOOK_SNAPSHOT/execution | 56 | 626.325 | 1174.698 | 1300.583 | 56 |
| BINANCE/BNBUSDT/MARK_INDEX_PRICE/execution | 55 | 1256.764 | 1827.012 | 2169.425 | 55 |
| BINANCE/BNBUSDT/QUOTE/execution | 105 | 311.073 | 614.464 | 792.698 | 76 |
| BINANCE/BNBUSDT/TRADE/execution | 63 | 462.534 | 1030.002 | 1101.675 | 56 |
| BINANCE/BTCUSDT/BAR/bar | 1 | 618.591 | None | 618.591 | 1 |
| BINANCE/BTCUSDT/BOOK_DELTA/execution | 106 | 283.235 | 1122.862 | 1469.907 | 76 |
| BINANCE/BTCUSDT/BOOK_SNAPSHOT/execution | 58 | 612.024 | 1101.152 | 1268.167 | 58 |
| BINANCE/BTCUSDT/MARK_INDEX_PRICE/execution | 53 | 1329.087 | 1716.708 | 2021.862 | 53 |
| BINANCE/BTCUSDT/QUOTE/execution | 106 | 213.282 | 418.123 | 500.583 | 68 |
| BINANCE/BTCUSDT/TRADE/execution | 104 | 336.024 | 707.773 | 917.166 | 79 |
| BINANCE/DOGEUSDT/BAR/bar | 1 | 1137.854 | None | 1137.854 | 1 |
| BINANCE/DOGEUSDT/BOOK_DELTA/execution | 106 | 278.203 | 716.182 | 766.583 | 74 |
| BINANCE/DOGEUSDT/BOOK_SNAPSHOT/execution | 57 | 619.708 | 1119.511 | 1382.583 | 57 |
| BINANCE/DOGEUSDT/MARK_INDEX_PRICE/execution | 55 | 1256.764 | 1956.167 | 2206.511 | 55 |
| BINANCE/DOGEUSDT/QUOTE/execution | 106 | 221.087 | 437.464 | 548.055 | 75 |
| BINANCE/DOGEUSDT/TRADE/execution | 71 | 488.26 | 1091.862 | 1292.708 | 66 |
| BINANCE/ETHUSDT/BAR/bar | 1 | 618.591 | None | 618.591 | 1 |
| BINANCE/ETHUSDT/BOOK_DELTA/execution | 106 | 240.712 | 918.637 | 1285.907 | 72 |
| BINANCE/ETHUSDT/BOOK_SNAPSHOT/execution | 58 | 550.336 | 982.167 | 1230.862 | 58 |
| BINANCE/ETHUSDT/MARK_INDEX_PRICE/execution | 54 | 1237.692 | 1896.29 | 2021.862 | 54 |
| BINANCE/ETHUSDT/QUOTE/execution | 106 | 265.235 | 523.166 | 792.698 | 70 |
| BINANCE/ETHUSDT/TRADE/execution | 104 | 288.708 | 681.595 | 772.235 | 75 |
| BINANCE/SOLUSDT/BAR/bar | 1 | 618.591 | None | 618.591 | 1 |
| BINANCE/SOLUSDT/BOOK_DELTA/execution | 106 | 220.087 | 594.583 | 822.637 | 85 |
| BINANCE/SOLUSDT/BOOK_SNAPSHOT/execution | 57 | 484.055 | 1078.425 | 1145.992 | 57 |
| BINANCE/SOLUSDT/MARK_INDEX_PRICE/execution | 54 | 1298.732 | 1934.339 | 2386.675 | 54 |
| BINANCE/SOLUSDT/QUOTE/execution | 106 | 205.317 | 451.691 | 653.698 | 68 |
| BINANCE/SOLUSDT/TRADE/execution | 93 | 358.073 | 699.698 | 1089.583 | 80 |
| OKX/BNB-USDT-SWAP/BAR/bar | 1 | 1717.953 | None | 1717.953 | 1 |
| OKX/BNB-USDT-SWAP/BOOK_DELTA/execution | 106 | 290.294 | 735.583 | 919.862 | 80 |
| OKX/BNB-USDT-SWAP/BOOK_SNAPSHOT/execution | 59 | 566.907 | 1119.862 | 1235.583 | 59 |
| OKX/BNB-USDT-SWAP/MARK_INDEX_PRICE/execution | 54 | 1175.258 | 1875.583 | 2323.698 | 54 |
| OKX/BNB-USDT-SWAP/QUOTE/execution | 103 | 324.675 | 634.639 | 859.637 | 70 |
| OKX/BNB-USDT-SWAP/TRADE/execution | 17 | 685.008 | None | 8760.912 | 15 |
| OKX/BTC-USDT-SWAP/BAR/bar | 1 | 1717.953 | None | 1717.953 | 1 |
| OKX/BTC-USDT-SWAP/BOOK_DELTA/execution | 106 | 293.406 | 994.992 | 1281.182 | 67 |
| OKX/BTC-USDT-SWAP/BOOK_SNAPSHOT/execution | 59 | 568.971 | 1005.764 | 1215.862 | 59 |
| OKX/BTC-USDT-SWAP/MARK_INDEX_PRICE/execution | 53 | 995.406 | 1391.907 | 1472.708 | 53 |
| OKX/BTC-USDT-SWAP/QUOTE/execution | 106 | 244.235 | 440.654 | 597.123 | 62 |
| OKX/BTC-USDT-SWAP/TRADE/execution | 101 | 354.277 | 669.83 | 847.698 | 81 |
| OKX/DOGE-USDT-SWAP/BAR/bar | 1 | 1137.854 | None | 1137.854 | 1 |
| OKX/DOGE-USDT-SWAP/BOOK_DELTA/execution | 106 | 293.286 | 736.583 | 820.862 | 76 |
| OKX/DOGE-USDT-SWAP/BOOK_SNAPSHOT/execution | 60 | 604.431 | 999.992 | 1286.182 | 60 |
| OKX/DOGE-USDT-SWAP/MARK_INDEX_PRICE/execution | 55 | 1201.949 | 2056.123 | 2142.235 | 55 |
| OKX/DOGE-USDT-SWAP/QUOTE/execution | 106 | 312.503 | 518.691 | 680.698 | 79 |
| OKX/DOGE-USDT-SWAP/TRADE/execution | 40 | 471.645 | 838.425 | 1208.583 | 40 |
| OKX/ETH-USDT-SWAP/BAR/bar | 1 | 1137.854 | None | 1137.854 | 1 |
| OKX/ETH-USDT-SWAP/BOOK_DELTA/execution | 106 | 290.286 | 983.182 | 1264.907 | 75 |
| OKX/ETH-USDT-SWAP/BOOK_SNAPSHOT/execution | 60 | 596.992 | 1053.167 | 1433.583 | 59 |
| OKX/ETH-USDT-SWAP/MARK_INDEX_PRICE/execution | 54 | 962.455 | 1461.166 | 1711.764 | 54 |
| OKX/ETH-USDT-SWAP/QUOTE/execution | 106 | 236.258 | 448.166 | 647.698 | 67 |
| OKX/ETH-USDT-SWAP/TRADE/execution | 96 | 308.306 | 671.862 | 882.583 | 77 |
| OKX/SOL-USDT-SWAP/BAR/bar | 1 | 1137.854 | None | 1137.854 | 1 |
| OKX/SOL-USDT-SWAP/BOOK_DELTA/execution | 106 | 293.732 | 732.583 | 916.862 | 67 |
| OKX/SOL-USDT-SWAP/BOOK_SNAPSHOT/execution | 60 | 639.235 | 1063.907 | 1206.764 | 59 |
| OKX/SOL-USDT-SWAP/MARK_INDEX_PRICE/execution | 54 | 1123.425 | 2042.008 | 2642.595 | 54 |
| OKX/SOL-USDT-SWAP/QUOTE/execution | 106 | 224.406 | 451.654 | 552.583 | 71 |
| OKX/SOL-USDT-SWAP/TRADE/execution | 83 | 418.152 | 916.235 | 1158.583 | 75 |

## Contemporaneous BNB Trade Attribution

75.189s,278cache samples per venue;719Binance raw trade events and33OKX trade events, no capture errors. Public frames hashed in memory, not retained. Read-only existing TS cache, no Query identity quota or stream allocation consumed. No canonical commit timestamp measured.

| Venue | Cache samples >3000ms | Same public trade ID + source time | Public trade newer than stale cache | Unattributed |
|---|---:|---:|---:|---:|
| OKX | 141 | 132 | 8 | 1 |
| BINANCE | 10 | 10 | 0 | 0 |

These are contemporaneous observations, not a guarantee that a separate provider socket sees every production event. They do not explain the old4262ms refusal retroactively. Across all behind-cache observations, time since the independent public receiver saw the newer event: OKX n15,p50/p95/max156.090/340.280/340.280ms; Binance n64,83.633/940.982/1784.090ms. This is neither production-ingress lag nor a paired commit/apply latency. A newly arrived provider event with stale preceding trade does not itself prove an internal outage.

First capture attempt retained: Binance used the wrong cache product and aggregate channel while production uses USD_M/raw trade. It supplies no Binance attribution. Corrected from acquisition config (`config/v2/stable-acquisition-bindings.yaml`, native channel `bnbusdt@trade`) before the second window. OKX observations from that first attempt are not mixed into the second denominator.

## Gates Still Open

| Gate | Current verdict |
|---|---|
| Whole DL stack <=5vCPU | Previous measured FAIL; not remeasured after a runtime optimization |
| Four Stream overflows | Attribution/fix still open; no buffer increase |
| Historical BOOK_DELTA22.825s | Root cause not proved; reset ordering source defect fixed separately |
| Redis ACK / canonical commit / alpha apply | NOT_MEASURED |
| Consumer usable after candidate rollout | NOT_MEASURED; Phase C still HOLD |

## Evidence And Runtime Safety

External root: `/home/bobby/.local/state/qdl-v2/dr07-hardening-20261007/`. Bounded metrics and public trade identifiers only, no credentials/provider raw payloads.

- `thread-profile.json` sha256 `b73b2d50b48e56d07e20e6efa500ad6108cb5c6b5737a5e9ab583e04b67710e7`
- `observer-candidate.json` sha256 `a67d3605f1a42eed03dffa2f15d11e96bd357b9f97a6407cd46d43797dfcf177`
- `observer-pair-candidate.json` sha256 `afb872d039b3df345acc3de21231a2fd4ff82c5e318ec2ec320c48e9584a1552`
- `trade-attribution.json` sha256 `48df88ec69a01d712f4cb54732b52efd066d16aed62bbb16102350f7e4ae9a5e`
- `trade-attribution-v2.json` sha256 `2826d7791f050f0a48858351adc3db828dd8f0aebc0e6dc8c2a6306e416c9f8c`
- `trade-attribution-v2-summary.json` sha256 `85e39a900326133b8aa2608b8e9ca2769405a758c49756106d7e36a0b213647f`

No runtime recreation, cap/config/manifest/quota/offset or order mutations. Existing rc2 and active/rollback images retained, no image build or broad prune. Disposable TS reset tests are documented in the TS journal; DL observations leave no container/network/image. Canonical `/home/bobby/data_layer` main5bf5cd5 unchanged; working branch fix/p184-dl-closure; no push/merge. Active role/config/digest and rollback map remains the [Phase C packet](p184-dl-phase-c-packet.md).
