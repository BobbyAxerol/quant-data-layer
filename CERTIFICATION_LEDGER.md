# Certification Ledger

**Mục đích: không chạy lại thứ đã chứng nhận.** Trước khi test bất cứ gì, đọc file
này. Chỉ chạy lại khi **commit / image / config** ở cột "Pinned at" đã đổi.

Ledger phủ cả ba repo. Cập nhật cùng transaction với commit làm nó đổi (G4).

Cập nhật lần cuối: **2026-08-22 15:00 UTC**, data layer `d58f594`+.

---

## 1. Đã chứng nhận — KHÔNG chạy lại

### 1.1 Suite offline

| Repo | Kết quả | Pinned at | Lệnh |
|---|---|---|---|
| Data Layer | **700 pass / 6 skip** | `833f4ce` | `docker run --rm -v "$PWD:/workspace" -w /workspace 41c135dcf450 python3 -B -m unittest discover -s tests` |
| execution_alpha | **80 test / 3 error** | `4ed0c81` | `docker run --rm -v "$PWD:/w" -w /w execution-alpha-runtime-numba:0.2.0-v2-c11 sh -c "PYTHONPATH=/w/runtime/app python -m unittest discover -s runtime/tests"` |
| Trading System | **732 pass / 0 fail** | `dd75441` | — |

3 error phía alpha là **baseline, không phải regression**:
`test_capital_model_sizing`, `test_combine_bracket_runtime`,
`test_fib_bracket_runtime` — `unittest.loader._FailedTest`, thiếu `sys.path`
riêng từng alpha. Đã đo trước khi làm và sau khi làm, y hệt. **Đừng điều tra lại.**

### 1.2 Bar đóng, đánh sàn thật, 0 lệch OHLC

| Venue | Interval đã chứng nhận | Pinned at |
|---|---|---|
| BINANCE USDM | `15m` `1h` `1d` `1w` | C.31 / C.36 |
| OKX SWAP | `1h` `1d` `2d` `3d` `1w` | C.31 / C.36 |

OKX native mapping đã xác minh: `1h→1H`, `1d→1Dutc`, `2d→2Dutc`, `3d→3Dutc`,
`1w→1Wutc`. Cách đều đúng: 3,6M / 86,4M / 172,8M / 259,2M / 604,8M ms.

### 1.3 Pass-through trên image đang deploy (C.36)

Image `sha256:75ab6244a798b8eff53db11e539c348219e479f12b52fa2950e74fe3790c15da`,
chạy **từ trong event loop đang chạy** — đúng hình dạng production.

| Instrument | Interval | Nến | Cách đều (ms) | |
|---|---|---:|---:|---|
| BINANCE USDM BTC-USDT | 15m | 5 | 900 000 | PASS |
| BINANCE USDM ETH-USDT | 15m | 5 | 900 000 | PASS |
| OKX SWAP ETH-USDT | 1h | 5 | 3 600 000 | PASS |
| BINANCE USDM ETH-USDT | 1d | 3 | 86 400 000 | PASS |

Mọi dòng: `authoritative=False`, `execution_eligible=False`,
`source_role=REFERENCE`, `flags=['PROVIDER_PASS_THROUGH']`,
`cursor=PASS_THROUGH_NO_REPLAY`, `bar_lifecycle=FINAL`. Fetch 0,07–0,11s.

**Kiểm âm cũng đã pass:**
- purpose `INTERNAL_EXECUTION` → từ chối (`entitlement or licensing policy denied`)
- requirement `1m` (có binding) → **không** route sang pass-through
- request không auth tới `query_v2_1` đang chạy → `401 UNAUTHENTICATED`

Script: `scratchpad/certify_pass_through.py` (session-scoped, không nằm trong repo).

### 1.4 Runtime sau rollout C.36

| | Trước | Sau |
|---|---:|---:|
| Role V2 chạy | 15 | 15 |
| Container stopped | 15 | 15 |
| `market_data_service` restarts | 0 | 0 |

`DATA_STALE` trên BINANCE TRADE: **có từ 07:50 UTC**, 34 lần trước 14:00,
recreate lúc 14:01 → **không phải do rollout**. Burst `DEPENDENCY_UNAVAILABLE`
2 phút khi stream restart, hồi lúc 14:03. Cache trade sống: 80 ms tuổi khi đo.
**Đừng điều tra lại như lỗi mới.**

### 1.5 Test hồi quy đã verify đỏ với code cũ

Không cần chứng minh lại giá trị của chúng:

| Test | Đỏ với code cũ |
|---|---|
| `test_pass_through_event_loop.py` | 5/7 (4 error, 1 fail) |
| `test_pass_through_grants_bound_instruments.py` | 4/6 |
| `test_module_import_order.py` | 6/10 entry point |
| `test_capabilities_no_false_advertisement` (C.30) | 4/5 |

---

## 2. CHƯA chứng nhận — đừng nói là đã

| Thứ | Trạng thái | Chặn ở đâu |
|---|---|---|
| Alpha tiêu thụ interval khác 1m | **Chưa** | Không có workload identity ALPHA (§3) |
| VN mọi feed | **Chưa** | Reachability + giờ giao dịch (C.37) |
| Binance/OKX **Spot** trên 1m | Chưa | Không có demand, binding đã tắt (C.32) |
| Bar realtime trên 1m | Chưa và **không định làm** | Cần streaming binding, không có |
| Feed phái sinh (funding/OI/basis) | Chưa bắt đầu | Program riêng, Section 20 |
| 4 trường tick/step lệch (C.27) | Chưa sửa | Cần rollout được duyệt |
| Tắt Spot có hiệu lực runtime | Chưa | Cần bundle refresh (C.32) |

---

## 3. Blocker duy nhất của Phase B

Pass-through **đã bật và đã chứng nhận**, nhưng **không consumer nào chạm tới được**:

1. Grant mang `INTERNAL_ALPHA` + `INTERNAL_RESEARCH`, không bao giờ `INTERNAL_EXECUTION`.
2. Bundle chỉ có **một** consumer identity: `trading-system`, purpose `INTERNAL_EXECUTION`.
3. ⇒ Identity duy nhất auth được lại đúng purpose phải bị từ chối.
4. Mọi alpha chạy `DATA_LAYER_CONSUMER_MODE: V1`, không giữ identity V2 (C.15).

**Không được lách bằng cách thêm `INTERNAL_ALPHA` vào manifest trading system.**

Cần: cert mTLS ký bởi stable CA + keypair RS256 đăng ký trong
`QDL_DATA_JWT_KEYS_JSON`. Manifest đã sẵn sàng ở revision 3
(`alpha-binance-paper`: Binance USD-M BTC/ETH 15m; `alpha-okx-paper`: OKX Swap
BTC/ETH 1h).

**CA private key không có trên host** (`cert-material/ca.crt` có, `ca.key` không).
Nhưng đó **không** phải blocker thật — xem §4: script sinh TLS tạo CA mới mỗi lần
chạy, nên chỉ cần thêm principal vào danh sách.

---

## 4. ĐÃ XOAY — PKI mTLS (C.38)

**Xong 2026-08-22 ~14:50 UTC.** Vấn đề hết hạn 48 giờ đã đóng.

| | Trước | Sau |
|---|---|---|
| CA hết hạn | Aug 22 17:39 2026 | **Nov 20 14:43 2026** |
| Hạn cert | `-days 2` hard-code 2 chỗ | `QDL_PHASE8_CERT_DAYS`, mặc định **90** |
| Consumer identity | 1 (`trading-system`, EXECUTION) | **2** (+ `alpha-binance`, **ALPHA**) |
| JWT key id đăng ký | 1 | **2** |

Material mới: `cert-material-rotate-20260822T144323Z`,
`bundle/identities-rotate-20260822T144323Z`,
`bundle/stable.env.rotate-20260822T144323Z`.
Mọi secret đối chiếu digest, giống hệt.

**Đã xoay và xác minh khoẻ:** kafka1/2/3 (healthy), stable_redis, rust_core_3
(generation 3, đang xử lý), 4 ingestor, binance_bar_edge, query ×2, stream ×2,
`market_data_service` (**0 lỗi TLS** sau khi trỏ lại identity mới).

**Hai bài học đã trả giá — đừng lặp lại:**

1. **Không roll được xoay CA.** Recreate kafka1 một mình → `PKIX path validation
   failed: Path does not chain with any of the trust anchors`, vì kafka2/3 còn CA
   cũ. Mọi peer xác thực lẫn nhau phải đổi **cùng lúc**.
2. **Consumer nằm trong phạm vi xoay.** `market_data_service` mount identity theo
   đường host; không trỏ lại thì `CERTIFICATE_VERIFY_FAILED` dù server đã xoay.

### 4.1 Đang hỏng — `projector_v2`

`stable_redis` **không nằm trên mesh mTLS và không cần recreate**. Tôi đưa nhầm
nó vào danh sách; nó ephemeral (`--appendonly no`, tmpfs) nên mất cache identity:

```
ProjectionCacheMismatch: stable Redis cache identity is missing for a non-empty spool
```

Guard này **đúng** và đã gặp 2026-08-20. Hệ quả đã đo, không đoán: projector là
thứ nạp spool, nên spool **đứng yên** — newest accepted 14:47:24 trong khi đồng hồ
14:57:44. `market_data_service` báo `DATA_STALE` chính vì lý do này.

Spool lúc đóng băng: 127.584 record, 18 partition, **1440 nến 1m mỗi BAR
partition** = đúng cửa sổ 24 giờ.

**Cách chữa: runbook có sẵn** `scripts/rebuild_v2_stable_projection_cache.py`.
Giá phải trả: warmup 1m tụt từ 1440 nến xuống ~15, đầy lại 1 nến/phút → 24 giờ.
**Pass-through không ảnh hưởng** (lấy từ sàn), nên 15m/1h/1d giữ nguyên độ sâu.

---

## 5. Ghi chú lịch sử — PKI hết hạn (đã đóng ở §4)

Phát hiện 2026-08-22 14:23 UTC khi truy tìm CA key.

```
CA                        notBefore=Aug 20 17:39:23 2026 GMT
                          notAfter =Aug 22 17:39:23 2026 GMT
```

**Còn ~3,26 giờ tính từ lúc đo.** Mọi cert đều 48 giờ và cùng hạn:

| Cert | notAfter |
|---|---|
| `ca.crt` | Aug 22 17:39:23 |
| `kafka1/2/3.crt` | Aug 22 17:39:23 |
| `phase8-producer/core/consumer.crt` | Aug 22 17:39:24 |
| `stable-query.crt`, `stable-stream.crt` | Aug 22 17:39:25 |
| `stable-trading-system.crt` | Aug 22 17:39:25 |

Nguồn: `scripts/phase80_generate_tls.sh` dùng `-days 2` cho **cả CA lẫn mọi leaf**
(dòng 16 và 33).

**Khi hết hạn:** mọi kết nối mTLS trong deployment V2 đứt — query, stream,
ingestor, Kafka broker, và `market_data_service`. Trading System sẽ **rơi về V1**
theo đúng thiết kế `_select_v2`, nên không mất giao dịch, nhưng **V2 tắt hoàn toàn**.

### Điều này gỡ luôn blocker ở §3

`phase80_generate_tls.sh` **tự sinh CA mới** (`openssl req -x509 -new`, dòng 15–19),
không cần CA key có sẵn. Thêm một identity ALPHA chỉ là thêm principal vào danh
sách client ở dòng 52.

**Xoay PKI là bắt buộc trong ~3 giờ dù có làm gì đi nữa.** Làm một lần và thêm
principal alpha trong cùng lượt = đóng Phase B với **blast radius bằng không so
với việc phải xoay**.

### Đường lan truyền cert (đã truy)

```
cert-material/            <- phase80_generate_tls.sh ghi vào đây
  -> bundle/identities/*  <- copy theo principal
  -> stable_tls_init      <- copy vào volume stable_tls
  -> mọi role mount :ro
```

Xoay = sinh lại cert-material → làm mới `bundle/identities/*` → chạy lại
`stable_tls_init` → **recreate mọi role, gồm cả kafka1/2/3** (chúng dùng keystore
riêng từ cert-material). Đây là blast radius toàn deployment và cần bạn duyệt.

---

## 6. C39.1 - Closure contract and reproducible ALPHA bundle

Pinned at: data layer 96d0d19.

- Added the governed final-closure gates C39.1-C39.5 to the main plan.
- Fixed candidate-bundle regeneration so stable-alpha-binance mTLS and RS256
  identities, both public JWT keys and exact alpha identity paths are generated
  from source rather than existing only in manually staged runtime material.
- Targeted direct tests: 2/2 passed.
- Complete test_phaseb_stable_deployment module: 20/20 passed.
- Disposable real PKI contract: 15/15 leaf certificates chained to the
  generated CA, QDL_PHASE8_CERT_DAYS=3 bounded the observed validity, ALPHA/JWT
  artifacts existed, ca.key was removed and temporary state was cleaned.
- Recovery runbook dry-run passed and reported only the exact isolated stable
  cache scope. No apply, FLUSHDB, cache deletion, service restart or V1 mutation
  occurred.
- Projector_v2 remains exited because the rotated ephemeral stable Redis lost
  its cache identity. C39.2 is not certified.
- Full current Python suite: 701 passed, 6 intentional skips, 0 failed.
- Pinned Rust 1.82 CI gate: fmt passed, clippy -D warnings passed, 74 tests
  passed, 0 failed. The disposable container and target state were removed.
- External PKI hardening is not certified: current generator uses one fresh CA
  per run, CA and leaf share one lifetime, and Kafka reads a shared
  certification cert-material bind mount. This is acceptable only for the
  current controlled candidate, not as offline-CA/external-secret production
  evidence.

---

## 7. C39.2 - Governed cache rebuild attempt and artifact-skew failure

- Owner-approved apply used confirmation token
  `REBUILD_QDL_V2_STABLE_PROJECTION_CACHE` and the rotated environment
  `stable.env.rotate-20260822T144323Z`.
- Exact mutation: five cache users stopped; three isolated canonical-cache
  SQLite paths removed; only `stable_redis` flushed; projector group reset to
  the 900-second `md.canonical.v2` window; stream then projector started.
- Result: **FAILED CLOSED**. Redis began repopulating, but projector rejected
  Binance USD-M ETHUSDT and OKX Swap ETH-USDT-SWAP TRADE/QUOTE canonical events
  as outside its image-local stable catalog. BTC events passed the same
  non-committing Kafka probe.
- Root cause: Rust ingestor runtime uses the current 22-binding catalog while
  immutable Python image revision `4f411e8a216a` embeds the older catalog.
- Safety result: the runbook never started query roles during the failed gate;
  the waiting process was stopped, query roles were then restored to pre-state,
  projector remains fail-closed, and V1 plus Trading System were untouched.
- C39.2 is open until a same-revision immutable Python edge image and catalog
  digest preflight are tested and the governed rebuild passes all lag/readiness
  gates.
- The rebuild tool now hashes the image-local catalog in an isolated no-network
  container and rejects drift before any stop/delete/flush. Targeted tests:
  13 passed, 0 failed.
- Focused rebuild/deployment/refresh tests: 46 passed, 0 failed. Corrected
  full-suite container (read-only source plus tmpfs log path): 703 passed, six
  intentional skips, 0 failed in 26.944 seconds. Earlier system-Python and
  read-only-log loader errors were harness errors and were rerun correctly.
- The first apply also exposed loss of the active pass-through Compose override
  because the runbook used only the base file. Override provenance is now an
  explicit staged-env input used by every Compose call; invalid or ambiguous
  paths fail before mutation. Focused suite after the fix: 47 passed, 0 failed.
- Corrected image: `sha256:bd5a8b44974c...`, revision `0df4360`, non-root,
  catalog revision 3/22 bindings/8 instruments and SHA-256 `a148e892b642...`.
  Baked-image focused tests passed 46/46.
- Second governed rebuild: PASS; 218706 replay records, six partitions, final
  lag 16, observed accepted bound 69/250 and 71 Redis keys at completion.
- Post-state: query replicas/active stream/projector READY; passive stream live
  and STANDBY by design; later lag 37; five role logs had zero bounded TLS,
  catalog, collision, quarantine, traceback or error matches. Redis/SQLite cache
  identity matched. Disposable backup audit: 114542 events, 18 partitions, zero
  open gaps, zero quarantine. V1 remained healthy and untouched. C39.2 PASS.

## 8. C39.3 - Multi-symbol contract slice

- Harness now binds `trading-system.paper.stable` revision 2 and covers BTC/ETH
  for Binance USD-M and OKX Swap with per-instrument durable cursors.
- First real-provider run passed 4/4 replica parity and exact N+1 stream resume.
  The stricter gate correctly distinguishes stale historical warmup members from
  the mandatory LIVE/execution-eligible latest closed BAR.
- Found and fixed one contract defect: materialized BAR history now reports
  `data_as_of_ns` at the final BAR close boundary, matching pass-through history,
  while preserving venue source timestamps on individual items.
- Correctness checks now include exact decimals, OHLCV, one-minute boundaries,
  ordering/gaps, finality, authority, coverage, latest quality, cursor/watermark
  and bounded latency measurements.
- Tests at the source revision before immutable build: 4 targeted PASS; 82 focused
  PASS with one intentional skip; 707 full Python PASS with six intentional
  skips. Runtime remains on `0df4360` pending immutable rebuild and explicitly
  scoped edge-role recreate.

## 9. C39.3 - SDK generation reset and provider status-frame correction

- Shared SDK correction: a fresh warmup now establishes a new durable cursor
  generation on first ACK, while restored-state and all later ACKs remain
  strictly monotonic. Retry before first ACK stays on the fresh server cursor.
- SDK module: 17 passed, 0 failed. Full Python discovery: 708 passed, 6
  environment-dependent skips, 0 failed in 27.004 seconds.
- Rust correction filters only observed Binance `e=trade,p=0,q=0,X=NA,st=1`
  status records, counts them as filtered, keeps normal trades canonical and
  keeps every other non-positive/malformed trade quarantined.
- Rust 1.82 gate: fmt PASS; workspace clippy `-D warnings` PASS; 75 tests PASS.
  A first workspace harness lacked OpenSSL development packages; the corrected
  run used the repository CI package set and passed.
- Runtime mutation: none. V1, Trading System, Kafka offsets, Redis/SQLite state,
  stable roles and authority are unchanged. Immutable rebuild/recreate, signed
  cursor-generation semantics and demanded-slice health remain explicit gates.

## 10. C39.3/C39.5 - Immutable artifacts and bundle dry-run

- Python image `sha256:6bc8ac77e9d...` and Rust image
  `sha256:685aaa68f7c7...` are pinned to `a3b068a`, run non-root and have not
  replaced any runtime role. Baked Python tests: 708 passed, 6 skipped.
- Stable refresh dry-run only: acquisition revision 4 -> 5; only the two Spot
  ingestor configs are removed; ten core/authority/active-ingestor artifacts
  change; env and identities remain preserved.
- `stable-crypto-bar-edge.json` is the single stranded checkpoint because it
  pins acquisition revision 4. Apply requires an exact checkpoint backup/move,
  real-provider bootstrap, role recreate and rollback packet. No apply occurred.


## C39.4 Generation-Bound Cursor Source Evidence

- Date: 2026-08-23 UTC.
- Decision: owner-approved signed stable cursor generation boundary.
- Stable token schema: `qdl.handoff-cursor.v2`, HMAC signed, generation bound
  to `SQLiteDurableSpool.cache_id`, opaque to all consumers.
- Compatibility: unbound codecs preserve v1; stable v1 and cross-generation
  tokens fail as `CURSOR_EXPIRED`; V1 data APIs and public V2 fields do not
  change.
- Focused evidence: 62 passed, one skipped.
- Full current-tree evidence: 706 passed, six skipped, zero failed.
- Same-image HEAD baseline: 702 passed, six skipped, zero failed.
- Runtime mutation: none. Production certification: pending immutable paired
  rollout and demanded-consumer acceptance.


### C39.4 Immutable Artifact Evidence

Exact source `62202b2d11e2607c6211f6cc1764d18969160c6d` was built as
Python image
`sha256:d4a97938fd6da1b226d5a6db2f51a42047c6aab811c511bdad3541d2c6a2016d`
and Rust image
`sha256:66988ae4254a149447b2a4e5ff6008aa864a4071796934421f5d92dd0248bd76`.
Both passed network-off executable/import probes. Read-only live preflight
matched SQLite and Redis cache identities at
`ae7554250ad548e7818559c140728ed4`; runtime remained unchanged.


### C39.4 Non-1m Alpha Contract Finding

The first execution-disabled alpha container was removed after a fail-closed
15m warmup. No execution effect occurred. Real 1m Binance/OKX acquisition and
all eight demanded Trading System slices remained healthy. The defect is the
registered 15m/1h alpha freshness bound of 180 seconds, shorter than the bar
interval itself. The bounded repair raises only those two interval policies,
bumps manifest revisions and adds a regression gate; production acceptance is
still open until the repaired smoke passes.


### C39.4 Non-1m Manifest Source Repair

Binance 15m and OKX 1h ALPHA freshness now cover one complete interval plus
180 seconds, and both manifests are revision 4. Targeted tests passed 35/35;
the full network-off suite passed 707 with six intentional skips. Two preceding
collection failures were test tmpfs permission errors and produced no runtime
mutation. The repaired artifact is not yet deployed at this ledger point.


### C39.4 Pass-Through No-Replay Cursor Finding

The repaired Python edge image was rolled through only the six approved Python
roles. The repeated execution-disabled alpha smoke then failed closed on the
15m provider pass-through because the cursor issuer tried to sign a canonical
replay token for an interval with no materialized binding. The response already
carried the intentional `PASS_THROUGH_NO_REPLAY` sentinel and watermark zero.
The in-scope correction must preserve that explicit non-replayable contract,
fail inconsistent sentinel states, retain signed generation-bound cursors for
materialized paths and repeat the complete bounded alpha smoke. Kafka, Redis,
SQLite, Rust, V1 and execution state remain outside this repair.


### C39.4 No-Replay Cursor Source Result

The query contract now preserves the explicit non-replayable cursor only for a
`FRESH_SNAPSHOT` response at watermark zero and rejects inconsistent states. No
durable cursor or authority is fabricated. Focused tests passed 83 with one
intentional skip; full network-off discovery passed 709 with six intentional
skips. Runtime deployment and the repeated alpha smoke remain pending.
