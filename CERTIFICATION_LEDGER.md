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


### C39.4/C39.5 Final Binance/OKX Acceptance

Final Python source `31c8ca5` and image `sha256:13fdb777a71f...` passed the
709/6 network-off suite and the real execution-disabled alpha acceptance.
Binance BTC/ETH 1m and 15m warmups returned 500 FULL rows; both TRADE sessions
reached replay/live and ACKed. Trading System reports 8/8 demanded Binance/OKX
slices READY, three Kafka lag samples were 15/10/22, cache identities match,
quarantine is zero, and a 30-second stable-state window grew by zero bytes.
Execution DB/Redis counts are unchanged. The exact rollback checkpoint SHA is
`98aef817697e...`; the redacted final packet SHA is `352a1d5f345e...`. Binance
and OKX crypto scope passes. DNSE remains `PARTIAL_EXTERNAL`; global Rust
authority promotion is outside this packet.


### C39.4/C39.5 Scoped Cleanup

Removed the two disposable C39 test images and nine exact temporary paths. No
broad prune or volume deletion occurred; final/rollback images, V1, stable
state and the checkpoint backup remain. Final packet SHA is
`352a1d5f345e3253620bc38d54ef387c2961650c6a3557325f2ae2cdc908c9bb`.

---

## 11. V2 stable stack host-reboot recovery and restart policy (2026-09-16)

Pinned at: data layer `d4c9763`, images unchanged (`qdl-v2-python:2.0.14-ccd0c43`
`sha256:3e062a3ba38d…`, `qdl-v2-rust:2.0.12-3f1c50e` `sha256:407a67131ca6…`).

- The 2026-09-15 04:31Z reboot stopped all of `qdl_v2_stable_candidate`
  (`restart: "no"` on every service). Recovery started the **existing**
  containers (never `compose up`, whose base file for eleven roles lives in a
  removed worktree) and ran the governed cache rebuild in the runbook's exact
  scope. PASS: cache 912 MB, Redis 548 keys, projector lag 104/6 partitions,
  restart count 0 on every role.
- 17 runtime containers are now `unless-stopped` at runtime and in
  `docker-compose.v2-stable.yml`; `stable_admin`/`stable_state_init`/
  `stable_tls_init` stay `"no"`. `tests/test_phaseb_stable_deployment.py`
  pins both sets, 28/28 pass in `qdl-v2-python:2.0.14-ccd0c43`.
- **Not certified: unattended reboot.** `stable_redis` is still ephemeral by
  design, so the projectors will restart into `ProjectionCacheMismatch` after
  the next reboot until someone runs the governed rebuild. Do not read
  "restart policy fixed" as "survives reboot".
- Trading System `market_data_service` consumes V2 again: `V2_PRIMARY`,
  60 demanded slices, 0 unhealthy, `v1_fallback_count=0`.

---

## 12. V2 stable boot recovery unit and crash rehearsal (2026-09-16)

Pinned at: data layer `4433497` + this journal; images unchanged.

- `qdl-v2-stable-boot-recovery.service` enabled; `scripts/v2_stable_boot_recovery.py`
  rehearsed end-to-end on the running stack (`--simulate-crash`): PASS in 17 min 44 s,
  receipt `boot-recovery-20260916T065640Z.json` sha256 `6af808c74b3adefd…`.
  **Certified: unattended recovery from the post-boot state.** Not certified: an
  actual host reboot (owner chose not to reboot); the unit's `After=docker.service`
  ordering is asserted by systemd, not observed.
- Do not re-run the rehearsal to "check": it deletes the spool and costs a
  15-20 min replay plus the 1m warmup dip. Re-run only if the runbook, the tool
  or the projector image changes.
- Serving measurement of the same day: Data Layer projection cache 1.2 s median
  behind the venue for trades, prices within 0.42-1.25 bps of the public ticker.

---

## 13. Consumer-side endpoint measurement (2026-09-16)

- Request latency of the V2 query endpoints measured from the Trading System
  identity **still matches the certified snapshot** (snapshot TRADE/QUOTE
  6.8-8.1 ms p50). Do not re-measure to confirm.
- **Not certified, open:** Binance USD-M `MARK_INDEX_PRICE` unavailable
  (40/40); OKX mark/quote intermittently exceed the sealed 2,000 ms freshness
  bound; **Binance 1m final bars are short on volume and trade count versus
  the venue kline while OKX 1m bars are exact**. Details and receipts in
  `dl-v2-consumer-endpoint-measurement-20260916`.

---

## 14. Binance final-BAR settlement (2026-09-16)

Pinned at: data layer `5130f6f`, image `qdl-v2-python:2.0.15-5130f6f`
(`sha256:b3f908cb17cf…`), `binance_bar_edge` only.

- **Certified:** Binance and OKX 1m final bars now equal the venue's own REST
  kline, **20/20 exact** across BTCUSDT, ETHUSDT, BTC-USDT-SWAP and
  ETH-USDT-SWAP on open/high/low/close/volume/base volume/trade count. Before
  the fix Binance was 0/10.
- Cause: Binance kline replicas disagree about a freshly closed bar for about
  five seconds; the edge read once at close + 0.10 s and never revised.
- Fix: read the same bar until two consecutive reads agree **and** the bar is
  at least 6 s old, then publish exactly that venue row; fail closed otherwise.
- Cost: the final 1m bar now lands 6.7-8.2 s after close instead of about 2 s.
  The published "close-to-final-BAR availability p50 2.151 s" figure no longer
  describes Binance and must be re-measured before it is quoted again.
- OKX is unchanged (single read; its `confirm=1` candles are final on arrival).

---

## 15. RUSTSEC-2026-0285 (2026-09-16)

- `rustls` pinned `=0.23.45` (was `=0.23.43`). Rust gate green in pinned
  `rust:1.82-slim`: fmt, clippy `-D warnings`, `cargo test --workspace --locked`
  **81 passed**, `cargo-deny 0.20.2` advisories/bans/licenses/sources ok.
- **Open:** the deployed `qdl-v2-rust:2.0.12-3f1c50e` still contains the
  affected `rustls 0.23.43`. Source is fixed; the runtime is not.

---

## 16. v2.0.15 published; rollback image correction (2026-09-16)

- Release **v2.0.15** published from `main` `653fdb6`, tag `a4c5ec3`, CI
  `35078347388` green.
- The bar-edge rollback named in that certificate
  (`sha256:1c1392bf…`, `qdl-v2-python:2.0.12-35a7cd8`) was deleted by my
  cleanup and is unrecoverable. The packet now pins the retained
  `qdl-v2-python:2.0.14-ccd0c43`, verified to carry the pre-fix bar edge.
- **Retention rule:** before deleting an image, check rollout packets and
  release certificates, not only running containers.

---

## 17. Endpoint closure (2026-09-16)

- Gateway `READY`, `stale_or_bad_services` empty. `market_data` `READY` with 60
  demanded V2 slices, 0 unhealthy, 0 V1 fallback, after recreating
  `market_data_service` from its packet (image unchanged) to rebuild two dead
  gRPC stream sessions.
- V2 endpoint latency re-measured at 20 iterations per hop: all inside the
  certified band. OHLCV 20/20 exact. Do not re-measure without a reason.
- `ingestor_binance_spot` and `ingestor_okx_spot` retired: their sealed config
  files do not exist, no consumer demands spot, dead since 2026-09-03.
- **Open:** the Rust runtime still carries `rustls 0.23.43`.
  `Dockerfile.qdl-rust-runtime` is committed, the rebuild is proven
  (`qdl-v2-rust:2.0.15-c5a5be0`, `sha256:5d1d7f02b904...`, same ten binaries and
  non-root user as the deployed image) and the packet
  `rust-rustls-2.0.15-c5a5be0-20260916T0950Z` is prepared. The five-role
  recreate needs owner approval.

---

## 18. Rust runtime patched and evidence ownership fixed (2026-09-16)

- All five Rust roles run `qdl-v2-rust:2.0.15-c5a5be0` (`rustls 0.23.45`);
  RUSTSEC-2026-0285 closed in runtime. Canonical topic advanced 132,736 records
  through the rollout, projector lag 155/500.
- P18 evidence files normalised to the evidence owner; every declared receipt
  verifies by SHA-256 and the E01 matrix verifier is **PASS with zero
  findings**.

---

## 19. Freshness measurement, 451 samples per slice (2026-09-16)

- QUOTE meets the 2,000 ms gate on both venues: p95 782-785 ms, max 1366 ms,
  **0 of 902 samples** over 1500 ms. The earlier rejection count does not
  survive a larger sample. Do not re-measure without a reason.
- **Open:** OKX `MARK_INDEX_PRICE` p99 1947 ms, max 2110 ms, 26 of 421 samples
  over 1500 ms. Ingest is 35-132 ms; the age is the wait for a newer paired
  record. Cause is pairing `mark-price` with once-per-second `index-tickers`.
  Proposed fix: publish on either component's update and keep the per-component
  2,000 ms policy. Needs owner approval; the bound must not be widened.

---

## 20. OKX mark/index: no code change (2026-09-16)

- The reducer already emits the pair on either component's update. The envelope
  deliberately stamps the **oldest** component's confirmation time; changing it
  would let a stale index look fresh. Withdrawn.
- OKX publishes index every 613 ms median (max gap 1,078 ms) and mark every
  231 ms. The pair's age floor is the index's publication rate.
- The consumed path is the reference batch, measured at 217 ms (Binance) and
  935 ms (OKX), both execution eligible: **the gate is met where it matters.**
  The durable-projection tail is a venue-bound characteristic, not a defect.


---

## 21. OKX index freshness: entry 20 corrected (2026-09-16)

- **Entry 20 is wrong on two points.** The reference-batch path *is* breaching
  the sealed 2,000 ms gate in production, and OKX's index publication rate is
  *not* the floor. Measured, not inferred.
- **Production, one hour of `market_data_service` logs:** 118 `MARK_INDEX_PRICE`
  slice disconnects, 117 OKX; `index_price` age p50 2255 ms, max 2973 ms, while
  `mark_price` at the same instants is p50 36 ms. Service state `DEGRADED`,
  reconnect count 294 on DOGE-USDT-SWAP. The consumer reports the Data Layer's
  own typed `DATA_STALE` problem; it applies no rule of its own.
- **Cause, measured against OKX from this host with no Data Layer in the path:**
  `/api/v5/market/index-tickers` returns rows stamped p50 901 ms and **max
  2511 ms** behind the clock, while `/api/v5/public/mark-price` returns 31 ms.
  The REST index endpoint serves a stale cached row. The 935 ms figure in
  entry 20 was a median that hid this tail.
- **The venue is fine.** `wss://ws.okx.com:8443/ws/v5/public` `index-tickers`,
  470 frames in 60 s: `ts` lag p50 81 ms, **max 354 ms**, push interval p50
  256 ms. The "613 ms publication rate" in entry 20 was the REST row's change
  rate, not the venue's index rate.
- **Fix, not applied:** serve the reference batch's OKX `INDEX` component from
  the `index-tickers` binding the deployment already subscribes to
  (`qdl/runtime/stable_deployment.py:195`), REST only as fallback; the
  per-component 2,000 ms policy stays sealed. Owner approval and reference-batch
  re-certification required; `v2.0.15` is released.
- **Pinned at:** `market_data_service` running image of 2026-09-16, OKX public
  REST and WS measured 2026-09-16. Re-measure only if OKX changes the endpoint.

---

## 22. V2 staleness is one CPU quota, not one feed (2026-09-16)

- **Entry 21 found a real defect but not the main one.** Measuring all feeds
  instead of mark/index: TRADE p50 **356 s** on Binance and **390 s** on OKX,
  BOOK_SNAPSHOT rejected `DATA_STALE` on every probed instrument, Binance
  MARK_INDEX_PRICE `DATA_NOT_READY`, BAR 1m 130 s. QUOTE alone is healthy at
  p50 403-573 ms. Consumer: 27 of 60 slices unhealthy, `DEGRADED`.
- **Projector lag** on `stable-projector-v1`: ~145,000 total against a
  documented gate of 500 total / 250 per partition; partition 4 is 6.1 minutes
  behind. Production 945 msg/s, consumption 768 msg/s, deficit 178 msg/s.
- **Root cause:** `stream_v2_active` runs at **104% of its 0.75-CPU quota**
  while the identical `stream_v2_passive` sits at 0.17%, because
  `qdl/runtime/stable_ingest.py:383` treats the two ingest URLs as ordered
  failover rather than load sharing. All three projectors post through the one
  throttled process and wait at 43% of their own quota. Host has 16 cores.
- **Fix not applied:** `docker update --cpus=2.5` on that one container, live,
  no recreate, no cache identity change, reversible with `--cpus=0.75`. The
  command was refused by the agent session's shared-resource permission policy.
  Owner action required. Both-endpoint load sharing is deliberately deferred:
  the two stream processes share one WAL SQLite database, which allows one
  writer at a time.
- **Pinned at:** stack `qdl_v2_stable_candidate` as running 2026-09-16 14:05Z;
  every container's `NanoCpus`/`Memory` captured for rollback. Re-measure lag
  after any quota change; the backlog must drain, not merely stop growing.

---

## 23. Entry 22 corrected: the throttle is a symptom, the spool lock is the cause (2026-09-16)

- **Withdrawn from earlier reasoning:** the "60x re-parse per subscription"
  claim (fan-out matches `(stream, partition_key)` first, `gateway.py:276`;
  the parse runs ~once per event, ~0.1% CPU) and the header-at-append fix built
  on it. The CPU quota raise is not step one either: the process is at
  33.1% loop + 36.3% across 55 worker threads, and the worker share is SQLite.
- **Cause:** every delivered record calls `advance_token` ->
  `handoff.issue()` -> `spool.high_watermark()` under the spool `RLock`
  (`sqlite_spool.py:116`), the lock `append_many` holds through its fsync. At
  ~945 records/s the delivery path serialises against ingest inside the one
  writer-lease process; replay on reconnect (322 per 10 min) pays the same per
  unmatched record (`grpc_service.py:254`).
- **Measured now:** consumer `DEGRADED`, 23/60 unhealthy, 22 execution-ready;
  projector lag 202,163 (p4 96,772); active loop p95 59.4 ms vs passive
  2.3 ms; ingest decode 12.6 us/event (1.2% CPU); 522 B per canonical record;
  Kafka 97.85 GB at the 24 h broker default with no recorded reason.
- **Plan:** DL-V2 R1 in the unified plan, anchor
  `dl-v2-r1-delivery-lock-20260916`. Nothing applied; awaiting owner approval.
- **Pinned at:** stack as running 2026-09-16 15:15Z. Re-measure only after an
  R1 step lands.

---

## 24. DL-V2 R1 landed; the single-writer ceiling is now the binding limit (2026-09-16)

- **Code:** R1.1 took the durable read off the live delivery path, R1.2
  collapsed replay token advances, R1.3 named the stale reason, R1.8 made a
  latest-state feed keep the newest record in a bounded buffer, R1.9 signs a
  known cursor on the loop instead of paying a thread hop. R1.4 on the Trading
  System side keeps a valid snapshot view instead of tearing the slice down.
  62 tests across two repositories; the data layer suite is 1,455.
- **Measured gain:** consumption 767 -> 1,239 records/s. TRADE freshness
  355,952 ms -> 926 ms, BOOK_SNAPSHOT from rejected to 942-1,205 ms, OKX
  MARK_INDEX from rejected to 575-731 ms, projector lag 161,732 -> 322 with the
  500/250 gate passing at the time.
- **Ceiling:** one writer by `ActivePassiveGatewayLease` plus one interpreter
  lock per process caps ingest near one core, about 1,240 records/s. Evening
  load reached 2,749 raw records/s against 950 in the afternoon, the backlog
  regrew to 2.6 M and the consumer fell to 27/60 ready. The same deficit
  existed before any change (945 produced vs 767 consumed).
- **Ruled out with numbers:** duplication (4/17/9 against millions), ingestor
  reconnect storms (39 renewals in 20 min, exactly the 30 s cadence), CPU
  starvation (projectors 53-63% of quota, stream 43% of a 2.00 quota), decode
  cost (12.6 us/event, 1.2% of a core).
- **Open, owner decision:** the governed offset reset to restore service now
  (attempted, refused by this session's shared-resource policy, nothing was
  reset); and whether to shard the gateway lease so both stream processes
  ingest disjoint partitions, which changes the single-writer invariant.
- **Open, next correction:** recreating a stream container stalls every
  projector on a dead pooled connection plus a 409 from the no-longer-active
  peer. Each rollout in this phase needed a projector restart.
- **CPU and retention:** ceilings are now per service with the measurement on
  each, declared total 12.25 -> 14.35 on a 16-core host, eight services reduced.
  Canonical Kafka retention 24 h -> 6 h with the reason recorded; raw stays 24 h
  because it cannot be refetched and canonical is derived from it.
- **Pinned at:** `qdl-v2-python:2.0.16-e87ef8d` on both stream processes and
  `2.0.16-190217b` on both query readers; projectors, ingestors, rust cores,
  kafka and redis unchanged. Rollback images in
  `~/.local/state/qdl-v2/dlv2-r1-190217b-20260916T163120Z/README.md`.

---

## 25. v2.0.16: the delivery path, not capacity (2026-09-17)

- **Release:** `v2.0.16`, certificate `upgrade/evidence/releases/v2.0.16/`,
  predecessor `v2.0.15` (`be1b94fe...c761ffe`). Seven python roles recreated on
  `qdl-v2-python:2.0.16-df4b8aa`
  (`sha256:3c1af2c74d5f2d9981d3ae9c5b098a0ba2f918f49735d5f7bbc3b87a637ede26`),
  built from `df4b8aa8f24e9b6f7dfec5da9c5121cd0fd07b98`, `restarts=0`.
  Rollback `qdl-v2-python:2.0.15-5130f6f`
  (`sha256:b3f908cb17cf9363afb9258ef2ae08e4cdfbc0cb26d01b5bd371559ad82b6bea`),
  packet `~/.local/state/qdl-v2/dlv2-r1-190217b-20260916T163120Z`.
- **Unchanged and pinned:** `binance_bar_edge` on `2.0.15-5130f6f`; the five Rust
  roles on `qdl-v2-rust:2.0.15-c5a5be0`
  (`sha256:5d1d7f02b904dc37611febbfea6930a6cf69448544e4d1b065d8624b5528f0d1`),
  which already closes RUSTSEC-2026-0285 in runtime; V1 fallback
  `qdl-v1-fallback:v1.2.4-2b0dcf7`
  (`sha256:dbfb57844977513ae7ec0a4782e04da0213028a789753c6b991f26043b615d65`);
  consumer `tradingsystem-image:v1.2.5-75df46e`.
- **Post-recovery measurement, 2026-09-17T04:11:44Z**, eight iterations through
  the real data plane: `QUOTE` p50 437-700 ms, `TRADE` p50 804-1,711 ms,
  `BOOK_SNAPSHOT` p50 1,186-1,361 ms, OKX `MARK_INDEX_PRICE` p50 785-892 ms,
  all against a 2,000 ms policy that `TRADE` was missing by 355,952 ms before.
  OHLCV 20/20 exact, all `FINAL`. Request latency p50: snapshots 6.5-7.2 ms,
  book 76.6 ms, 1m warmup 119 ms, batched warmup 649 ms.
- **31-minute window, 30 samples:** consumer `V2_PRIMARY` with `fb=0` on every
  sample, 60 slices demanded, `READY` on 26 of 30, worst sample 3 of 60 slices
  transiently unhealthy, execution-ready slices p50 45.
- **The backlog question, answered with numbers.** 24 consumer-group snapshots
  over 356 s: produced 132,880 records, consumed 132,894 — the projector
  consumed 14 *more* than were produced, both at 373 rec/s. There is no
  backlog. Lag oscillates 138-1,273 records and the spikes wander across
  partitions 2-5, which is a queue breathing, not a stuck partition.
- **The gate was the wrong gate.** `MAX_ACCEPTED_LAG = 500` /
  `MAX_ACCEPTED_PARTITION_LAG = 250` in
  `scripts/rebuild_v2_stable_projection_cache.py:31` is the **convergence** gate
  of the cache-rebuild runbook: three consecutive acceptable samples prove a
  replay drained. At 373 rec/s, 500 records is **1.3 seconds of work**, so an
  instantaneous sample of a healthy queue crosses it — 5 of 30 window samples
  did, while the consumer stayed ready and never fell back. Reusing it as a
  steady-state health gate was my error, not a defect in the runbook. Steady
  state is the produced-versus-consumed rate plus lag in seconds of work:
  p50 1.02 s, p95 1.58 s, max 2.19 s.
  **Do not loosen the runbook constant** — it is correct for convergence and the
  17m44s boot-recovery rehearsal (entry 12) depends on it.
- **Disk, measured after the retention change.** Across the three brokers:
  canonical `55.6 -> 17.0 GB`, raw `40.1 -> 50.7 GB` (still 24 h, and the
  realtime volume is higher than when the before figure was taken), total
  `95.7 -> 67.7 GB`. Host filesystem 52%. Verified live:
  `retention.ms=21600000` is a `DYNAMIC_TOPIC_CONFIG` on `md.canonical.v2`,
  raw inherits the 24 h static broker config.
- **Entry 24 corrected:** the declared CPU total after the R1 ceilings is
  **13.35**, not 14.35, summed over the 17 roles this stack runs on a 16-core
  host (12.25 before). Eight services were reduced, five raised: kafka1/2/3
  `0.75 -> 1.00` and both stream processes `0.75 -> 2.00`.
- **Open, next slice:** express steady-state projector health in seconds of work
  rather than an instantaneous record count, and stop quoting the runbook
  constant as a health gate.
- **Still open, untouched:** Binance `MARK_INDEX_PRICE` answers
  `DATA_NOT_READY`; 1 of 16 OKX `MARK_INDEX_PRICE` samples rejected on
  `EVENT_AGE` (entry 21 cause unchanged); recreating a stream container still
  stalls the projectors on a dead pooled connection plus a 409 from the
  no-longer-active peer.
- **Governed recovery:** the backlog of 3,002,343 was cleared by a consumer-group
  offset reset to latest on `stable-projector-v1` (stop projectors, reset,
  start), authorised by the owner for a pre-production stack. Canonical records
  between the old committed offset and latest were not projected; raw still
  holds them and canonical retention is 6 h.

---

## 26. Post-v2.0.16 resource rebalance: -63% p95, -38 GB disk (2026-09-17)

- **The CPU was misallocated, not short.** `cpu.stat` 60-second deltas: the two
  stream processes held 4.00 cores at **0.0%** throttle while `query_v2_2` was
  denied **15.51 CPU-seconds every 60 seconds** at a 0.50 ceiling, `query_v2_1`
  28.5%, `rust_core_2` 24.9%, kafka2/3 15.8%/12.1%. That denial was the p95 tail.
- **Applied live with `docker update --cpus`, zero containers recreated**, and
  mirrored into `docker-compose.v2-stable.yml`. stream ×2 `2.00 -> 1.25`,
  query ×2 `0.50 -> 1.00`, rust_core_2 `0.75 -> 1.00`, kafka2/3 `1.00 -> 1.25`,
  binance_bar_edge `0.35 -> 0.75`, stable_redis `0.25 -> 0.50`. Declared total
  `13.35 -> 14.25`; measured use stays about 4.4 of 16 cores.
- **Measured gain**, same benchmark, 20 iterations, 33 minutes apart: request
  latency p95 summed over 29 V2 endpoints `6,767 -> 2,527 ms`, **-63%**. QUOTE
  p95 `81-101 -> 9.6-28 ms`, TRADE p95 `87-105 -> 10-40 ms`, BOOK_SNAPSHOT p95
  `210-289 -> 84-110 ms`, batched warmup p95 `1,589 -> 418 ms`, instrument
  lookup p95 `71.5 -> 5.2 ms`. Throttle after: query 2.0%/4.5%, rust_core_2
  2.0%, kafka2/3 1.2%/1.1%. **All three `EVENT_AGE` rejections disappeared.**
  Event age unchanged or better, OHLCV still 20/20 exact, consumer back to
  `V2_PRIMARY` 60/60 ready, 0 unhealthy, 0 fallback.
- **Raw tick retention 24 h -> 8 h** (owner decision): dynamic topic config
  `retention.ms=28800000` on `md.raw.realtime.v2`, no restart. Raw per broker
  `16.92 -> 5.13 GiB`, Kafka per broker `23.56 -> 11.76 GiB`, host filesystem
  `52% -> 39%`, free `141 -> 178 GB`. **Warmup contract untouched:** the SQLite
  spool keeps 24 h, canonical keeps 6 h.
- **Images pruned** by digest with an `until=24h` filter: `22.22 -> 16.31 GB`,
  2.07 GB reclaimed. Container logs truncated in place `439 -> 160 MB`.
- **Log rotation declared but not yet active.** `x-logging` (`max-size 50m`,
  `max-file 3`) is wired into the kafka, python and rust anchors and into
  `stable_redis`, covering all 17 running roles; it applies at the next
  recreate. Deliberately not activated today by restarting a stack certified an
  hour earlier.
- **A regression I caused and reverted.** Lowering the stream ceiling to 1.25
  was wrong. The benchmark measures the snapshot path through the query readers;
  the consumer's slice health measures the streaming subscription through the
  gateway. The consumer's unhealthy-slice mean went `0.27 -> 2.0` (max `3 -> 7`),
  every one a `QUOTE` slice, which is a `LATEST_STATE` feed on that path, while
  `stream_v2_active` throttle went `0.0% -> 4.5%`. `v1_fallback_count` and
  `v2_error_count` stayed 0 the whole time, so nothing failed, but the margin
  narrowed. Both stream processes are back at `cpus: 2.00`; declared total
  `15.75`. The query gain came from query/kafka/rust getting more, not from
  stream getting less. **Revert confirmed over 12 samples:** mean back to
  `0.33` against the `0.27` baseline, max `2`, nine of twelve samples clean,
  and the residue is single thin-symbol `MARK_INDEX_PRICE` slices rather than
  the `QUOTE` cluster.
- **Second pass on the two that did not respond:** `binance_bar_edge`
  `0.50 -> 0.75` halved it to 9.4% and the `exhausted retries` warning has not
  recurred; `stable_redis` `0.25 -> 0.50` took it to 0.0%.
- **Corrects an earlier guess of mine:** projector partition assignment is
  **even**, two partitions each. The CPU spread across the three projectors is
  traffic per partition key, not a rebalancing fault.
- **Withdrawn:** the suggestion to drop Kafka `ReplicationFactor` 3 -> 2. After
  the 8 h retention cut the prize is about 11 GiB, not worth trading
  `min.insync.replicas=2` for.
- **Pinned at:** `qdl-v2-python:2.0.16-df4b8aa`, `qdl-v2-rust:2.0.15-c5a5be0`,
  `qdl-v2-python:2.0.15-5130f6f` on the bar edge. No image changed.

---

## 27. Age measured stage by stage; the projector was not the dominant term (2026-09-17)

- **The serving path is free.** Across fourteen realtime endpoints the data
  layer's reported freshness and the consumer's independently computed durable
  age agree within **3-4 ms**. The age is baked in before anything serves it.
- **Stage delays**, lag over that stage's own rate: `rust_core` raw->canonical
  **0.89 s**, projector canonical->spool **0.46 s**, query spool->consumer
  0.003 s. **`rust_core` is the dominant term.**
  **Corrected in entry 28:** the 0.89 s reading was taken minutes after the
  projector recreate while the system was still settling. A clean baseline an
  hour later put the same stage at **0.34 s**, so it is about 1.5x the
  projector, not 4x. The ordering holds; the multiple does not.
- **R1.22:** `QDL_STABLE_PROJECTOR_BATCH_WAIT_SECONDS` `0.10 -> 0.02`. Projector
  queue delay **0.46 -> 0.22 s, -52%, at a load 27% higher** (493 vs 387 rec/s);
  lag 177 -> 108. The fivefold rise in drains cost nothing: projector throttle
  went *down* 4.0% -> 1.7% and 4.3% -> 1.8%, and `stream_v2_active`, which takes
  every round trip, throttles 0.0%.
- **Attributed honestly:** that change alone moved consumer-visible age
  12,205 -> 11,488 ms, **-6%, inside the noise** of two 20-iteration samples an
  hour apart. It is kept for the measured queue improvement at lower CPU cost,
  not for the age number.
- **Whole session, 05:04 -> 06:53:** event age p50 summed over fourteen
  endpoints **12,943 -> 10,460 ms, -19%**, improving on 13 of 14; request p50
  `3,269 -> 1,353 ms`; request p95 `6,767 -> 2,265 ms`; rejections 5/78 -> 2/80,
  the two remaining being the known Binance `MARK_INDEX_PRICE`; OHLCV 20/20.
  **Limit of this figure:** it bundles the CPU rebalance, the batch wait and the
  broker recreates across two hours of differing market conditions. The
  direction is consistent on 13 of 14 endpoints; the attribution is not
  separable beyond the isolated -6% above.
- **R1.18 landed:** `parse_canonical_progress` and `steady_state_lag_seconds`
  with 19 new tests; the convergence gate and its existing tests are untouched,
  and both readers now carry docstrings saying what they are and are not for.
  The new measure returns None rather than a number for a stalled projector, a
  group reset, a mid-interval rebalance or a non-positive interval.
- **Log rotation live on 6 of 17 roles** - three projectors, three brokers,
  about 95% of log volume. **First measured broker restart on this stack:** each
  recreate left a projector backlog of 3,458-20,200 that drained in about three
  minutes, ISR returned to 3 on all 154 partitions, and the consumer fell to a
  worst of 34/60 ready before returning to 60/60 about five minutes after the
  last broker. `v1_fallback_count` and `v2_error_count` stayed 0: it degraded,
  it never failed over.
- **Trap caught before it fired.** The compose override chain recorded in the
  container labels is **incomplete** - it omits the R1 stream and query image
  overrides, so `up -d` against it resolves stream and query to
  `sha256:8bd10da6...` (2.0.12) and moves the bar edge. Every recreate went
  through a packet pinning all 17 roles to their running digest; 0 of 17
  drifted, before or after.
- **Disk:** `150 -> 96 GB`, `52% -> 34%`, free 194 GB. Build cache
  `13.52 -> 4.85 GB`, six orphaned volumes removed.
- **Suite:** 1,514 tests, 0 failures, 7 skipped, 405 s in
  `qdl-v2-python:2.0.16-df4b8aa`.
- **Pinned at:** no image changed. All seven python roles
  `qdl-v2-python:2.0.16-df4b8aa`, bar edge `2.0.15-5130f6f`, rust
  `qdl-v2-rust:2.0.15-c5a5be0`, brokers `apache/kafka@sha256:9516fb76`.
  Rollback packet `~/.local/state/qdl-v2/dlv2-r122-projector-age-20260917T062751Z/`.
- **Next lever, already measured:** `rust_core` at 0.89 s is not CPU-starved
  (2.0% throttle, 0.10-0.45 cores), so the cost is a batching or flush interval
  in the Rust core. Not opened in this session rather than left half-done.

---

## 28. Config-generation drift, exposed by restarting what had never restarted (2026-09-17)

- **Root cause, one for both incidents:** containers that had run for weeks were
  holding configuration generations that no longer exist on disk. Nothing here
  is a new defect; it is old drift becoming visible the first time each process
  was asked to read its configuration again. **16 of 17 roles are now proven
  restartable**, one at a time, with observed recovery.
- **Corrects entry 27:** the `rust_core` stage is **0.34 s**, not 0.89 s. The
  higher figure was sampled minutes after the projector recreate while the
  system was still settling. It is about 1.5x the projector, not 4x.
- **Withdrawn:** `rust_core` `batch_size` 256 -> 64. A single-core A/B showed
  `0.38 -> 0.20 s` (-47%) with both controls worse, which looked decisive and
  **did not reproduce**: with all three cores changed, twelve samples over five
  minutes gave **0.41 s against a 0.34 s baseline**. Reverted. Unproven, and it
  costs four times the Kafka transactions. The method lesson: in 90-second
  windows the variance of this signal exceeds the effect, and controls moving
  the other way inside one short window prove nothing.
- **Incident, 22 minutes, caused by this session.** Recreating
  `binance_bar_edge` for log rotation exposed a checkpoint at
  `catalog_revision 7 / acquisition_revision 14` that no configuration on this
  host still produces (image 8/16, packets 9/17). Recovered onto a fresh state
  path, old checkpoint preserved and backed up. **The bootstrap cost was mine to
  foresee and I did not:** I verified the 10,000-row cap per binding and did not
  multiply by 140 bindings. About a million records in five minutes, projector
  backlog **601,622**, `TRADE` age **250-310 s**, every other feed rejected on
  freshness, consumer down to 24 of 60. **`v1_fallback_count` and
  `v2_error_count` stayed 0 throughout: it degraded, it never failed over.**
- **The throttle watchdog written that morning paid for itself immediately.**
  The projectors were throttled **67-92%** at 0.50/0.75 while `docker stats`
  showed 50-75% and looked like headroom. Raised live to 2.00: drain
  `114 -> 900 rec/s`, backlog cleared in 15 minutes. They settled at 0.9 of a
  core with 0.0% throttle, so they were returned to **1.00**, not to 0.50/0.75.
- **Open defect, not guessed at: no component publishes OKX realtime bars.** The
  acquisition catalog marks OKX `bar-1m` `RUST_NATIVE`; the bar edge's realtime
  loop takes `PYTHON_REST` only and correctly skips them; the rust core config
  has 197 bindings and **zero bar bindings**; `ingestor_okx_swap` carries no BAR
  feed. Measured: Binance `bar-1m` 27 s old, OKX `bar-1m` 1000 s and ageing.
  They worked until today only because the old bar edge was still running the
  orphaned revision 14 catalog. Three options are written up in the plan; all
  need an owner decision, and option 1 touches the image v2.0.16 was certified
  on.
- **Log rotation:** active on 14 of 17 roles. `stream_v2_active/passive`
  excluded (recreate stalls the projectors, for 1-2 MB of log) and
  `stable_redis` excluded (recreate destroys the projection cache identity by
  design).
- **VN/DNSE not testable from this host:** `openapi.dnse.com.vn` refuses TCP 443
  from container and host, with and without proxy, while `api.dnse.com.vn` and
  `services.entrade.com.vn` answer. Credentials and FPT/VN30F1M bindings are
  present; the endpoint is unreachable.
- **State:** `QUOTE` 381-436 ms, `TRADE` 319-839 ms, `BOOK_SNAPSHOT`
  767-1265 ms, OKX `MARK_INDEX_PRICE` 590-753 ms, Binance `BAR1m` 27 s,
  projector lag ~180, disk 34%, **no image changed anywhere in this session**.
  Consumer tops out near 51-55 of 60 until the OKX bar owner is decided.

## 29. R1.25: three defects, and two answers that close work instead of opening it (2026-09-17)

**Pinned at:** `dev` `ff34867`, `qdl-v2-python:2.0.17-436171f`
(`sha256:97d2f593f0a6…`), `qdl-v2-rust:2.0.17-d9adee3`
(`sha256:4aa578fc0be7…`), 13 of 17 roles recreated from
`dlv2-r125-rollout-20260917T125950Z`.

- **OKX native bars never reached the canonical stream, and the reason was a
  cross-lane comparison.** 1,067 quarantine records, every one
  `StaleGeneration` "connection generation is stale", while raw carried all 70
  candle channels and provisional candles were filtered correctly. The ingestor
  runs one socket per feed class, each with its own generation counter: OKX
  stood at BOOK 53,986, QUOTE 82, TRADE 70, MARK_INDEX 37 and **BAR 23**. The
  ordering fence compared those as bare integers before it looked at the
  session. The system proved it itself - the bar lane reconnected at 11:29Z from
  generation 22 to 23 and the brand-new session was fenced identically. Repaired
  by scoping the fence to one lane; an unparsable session id keeps the original
  comparison, so nothing an unknown producer sends becomes newly acceptable.
  After the repair and a live ingestor reconnect: OKX `bar-1m` **p50 1.19 s,
  p95 1.67 s**, all five instruments, zero candle quarantines.
- **The spool failed every write closed while inside its own retention policy.**
  2,252,414,976 + 966,902,232 + 1,900,544 bytes against a 3 GiB bound: **7,720
  bytes of headroom**, two hours of `503` to every projector. The WAL frames
  were already checkpointed - PASSIVE recycles a WAL but never shrinks the file
  - and one TRUNCATE reclaimed all 922 MB **in 0.08 s with no reader blocking
  it**. The bound was also tighter than `max_records` permits: 183 partitions ×
  a 10,064-record window = 1,841,712 rows, and the live cache held 1,301,097
  rows in 1,114 MB of payload inside a 2,312 MB file. Bound now 6 GiB, derived;
  WAL reclaimed at its own `journal_size_limit`. Live: 2,378 MB of 6,144 MB,
  WAL 64 MB.
- **The backlog could not drain, and the disk was not why.** Consumption had
  converged on production at **759 events/s** with projectors at 0.49 of a 2.00
  ceiling, no throttling, and the spool's own volume benchmarking **40,280
  rows/s** at `synchronous=FULL`. The projector asked Kafka for one record at a
  time and paid a thread hop for each. Batched: **1,029 events/s**. Honest
  limit: the thread hop was real but not dominant, and the remaining ceiling is
  the projector's single event-loop thread. Recorded, not guessed at.
- **Binance mark price and native klines are a venue condition on this host, not
  our configuration.** Binance USD-M answers `{"result":null}` to a subscribe
  for `@markPrice@1s` and `@kline_1m` and then sends **nothing** - 80 s, zero
  frames - on the same socket where `btcusdt@trade` delivered 353 frames in
  12 s. Four subscription shapes tried for mark price, all zero. This is the
  condition `production_catalog.py` already recorded for klines; it holds for
  mark price too. Neither can be repaired from the WebSocket path.
- **Binance's seven-second bars are evidence, not caution.** A closed BTCUSDT 1m
  kline was still changing **5.04 s** after its close boundary, ETHUSDT
  **3.96 s**. The bar edge's 6 s settlement plus two confirmations is exactly
  sized for that. Cutting it would publish bars the venue then revises.
- **The BAR gate was wrong and is now measured.** `max_freshness_ms = 180000` is
  a tolerance, and the canonical envelope timestamps a bar by its *open* time,
  so "age of the newest record" can never read below 60 s for a 1m bar. The
  instrument is publish time minus **close** time, final bars only.
- **Still open:** `stable_redis` records a compose chain from a deleted
  worktree; repairing it needs a redis recreate plus the projection cache
  rebuild runbook, which is not something to run while a backlog is draining.
  With six partitions, three replicas and range assignment, a drained replica
  cannot take work from a loaded one - measured mid-drain at projector-1 idle on
  43 and 26 records while projector-3 carried 1.29M.

## 30. Entry 29 corrected: the OKX fence was losing to the bar edge, not to another connection (2026-09-17)

**Pinned at:** `dev` `1acf87a`, `qdl-v2-rust:2.0.17-1acf87a`, three cores and two
ingestors recreated from `dlv2-r125-rollout-20260917T125950Z`.

- **What entry 29 got wrong.** It named cross-lane connection generations as the
  cause. That repair is real and its tests hold, but it was not the cause: after
  it shipped, three of five OKX instruments published and ETH and SOL kept being
  quarantined on live frames carrying the single business session's generation
  25, while neither the raw nor the canonical stream contained any OKX
  generation above 25 at all. The configurations of the failing and the working
  instruments were identical.
- **How the real cause was found.** Not by a third hypothesis. The rejection was
  made self-describing - it now logs the tracked session and generation it
  compared against - and answered on the first occurrence:
  `tracked_session qdl-v2-stable-okx-rest-r1-g1789653808007759187`,
  `tracked_generation 1789653808007759187`.
- **The cause.** The bar edge publishes REST bootstrap and repair rows carrying a
  **nanosecond timestamp** where a connection generation belongs. One such row on
  a bar partition fences every native candle behind it for the life of the core
  process: 25 never exceeds 1.79e18. The timestamp decodes to **14:03:28Z**, the
  minute the bar edge was recreated in this session's rollout and bootstrapped
  its history; quarantines began at 14:20. The 09:16 outage has the same shape -
  the bar edge was given a new state path at 07:53.
- **Why the first repair let it through.** When it could not parse an identity it
  kept the bare comparison and called that conservative. It is the opposite: an
  unparsable identity is a different producer whose numbering means something
  else, so comparing the two is meaningless rather than careful. Only two
  positively identified identities in the same lane may fence each other.
- **Why the suite never caught it.** The test helpers built session identities
  (`s1`, `session-1`) the running system never emits, so the tests exercised a
  comparison production never makes. They now use the production shape; two
  tests went red on the correct behaviour and were fixed rather than the code.
- **Proof.** After rollout: 10 of 10 `bar-1m` endpoints publishing, 8 closed bars
  each over eight minutes, OKX from `okx-business-001-26` and Binance from the
  bar edge on the same partitions, **zero** stale-generation rejections across
  all three cores, consumer unhealthy slices 40 → 8 of 60.
- **The repair that proved it.** Republishing 1,074 missing bars through the
  documented repair tool re-poisoned every OKX bar partition within minutes.
  That is the clearest statement of the defect available: the sanctioned repair
  path and the native path could not coexist. 1,106 rows are now back and the
  gap is zero.

## 31. Two open items closed and one priced honestly (2026-09-17)

- **The sink retry is proven, not just tested.** `stream_v2_active` was
  recreated on its own at 15:50:19Z with the projectors untouched. All three
  kept their original start time and restart count 0, logged no reconnect
  failure, and the group stayed at lag 279 with bar partitions 33 s fresh on
  both venues. Every R1 stream rollout before this one needed a manual projector
  restart; this is the first that did not.
- **`stable_redis` was deferred for the wrong stated reason, and the real one is
  worse.** The reason given was that a backlog was draining. That had already
  stopped being true. The actual cost, read from the runbook rather than
  remembered: `rebuild_v2_stable_projection_cache.py` accepts exactly one
  compose override (`scripts/rebuild_v2_stable_projection_cache.py:57-71`) while
  the running chain has thirteen and the env file declares none, so it would
  recreate every service from the bare compose file and discard every image pin
  including this release; and it deletes the durable spool outright
  (`:49-53`) - 2.3 GB holding seven days of bar history and the 1,106 rows
  repaired an hour earlier - replaying only 900 s afterwards. The correct
  sequence is to consolidate the thirteen overrides into one file first. That is
  a maintenance window, not a release step, and consolidating them is worth doing
  properly because this chain is the origin of the whole config-generation drift
  class in entry 28.

## 32. A warmup optimisation, measured three ways, and reverted (2026-09-17)

The owner compared this session's numbers against v2.0.16 and asked why they had
become seconds. Two of the three quantities involved were different quantities,
and one regression was real.

- **Same quantity, same benchmark, run twice.** Snapshot reads 6.4-7.5 ms then,
  6.7-9.2 ms now. Instrument lookup 4.1 then, 4.3-5.2 now. `BOOK_SNAPSHOT`
  73-78 ms then, **44-50 ms** now. Durable event age: QUOTE 437-700 then,
  577-780 now; TRADE 804-1,711 then, 728-1,575 now. Nothing there moved by more
  than noise, and one endpoint class halved.
- **The real regression: BAR warmup, 117-156 ms → 364-428 ms.** Not caused by
  this release. `stable_source.history` reads the entire retained window for any
  BAR requirement, because a history repair legitimately appends older bars after
  live ones and the market tail cannot be taken from the append tail. At 04:11Z
  those windows were far from full and OKX bars did not exist at all; they are
  full now, partly because this session repaired 1,106 bars into them.
- **Where the time goes, measured rather than reasoned.** `read_tail` on one 1m
  partition: 1 row 0.0 ms, 1,000 rows 17.9 ms, **10,064 rows 199.8 ms** - the
  cost is materialising a Cursor, a decoded header mapping and a dataclass per
  row, almost all discarded. Decode and sort inside `_records` cost 41.4 ms more.
- **Three variants, and the numbers chose.** Decoding each payload twice, 41.4 ms.
  Carrying the decoded envelope to avoid the second decode - the first idea -
  **52.1 ms, worse**, because ten thousand live protobuf objects cost more than
  the decode they save. Carrying only the open time as an integer, **26.5 ms**.
  All three returned identical rows.
- **The attempt that broke it.** Reading the window as plain rows and resolving
  only the survivors through `find_events` passed 137 tests and then failed every
  BAR warmup in production with `warmup batch item failed inside the bounded
  executor`. Reverted within minutes; warmup verified working again at
  390-475 ms. The suite did not catch it, which is the finding worth keeping: the
  bounded-read contract is pinned by a test double and a spy, and neither
  exercises the executor the real query path runs inside.
- **What the real fix needs.** The spool must know a bar's market time without
  decoding it - a column and an index on the durable write path, with a
  migration. That is a designed slice, not something to improvise while a
  release is waiting, and today is the second time that lesson was paid for.

## 33. The repository's own gate had been red for six runs (2026-09-17)

The release head was pushed and CI came back `failure`. So had the five pushes
before it. Every one failed at the same place: `contract-tests` step 10,
`Test Rust generated contracts`.

- **The gate is three clauses, and I had been running one.**
  `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --locked
  -- -D warnings && cargo test --workspace --locked`. I ran `cargo test`, got
  43 + 36 green, and wrote `rust_gate` into the certificate as if that were the
  gate. `fmt` is the *first* clause, so nothing after it ever ran on CI.
- **What broke it was the fix for the outage.** Replacing synthetic session ids
  (`s1`, `session-1`) with production-shaped ones
  (`qdl-test-lane-001-1-1700000000000000000`) pushed six `tracker.observe` calls
  past rustfmt's width limit. Green locally, red on CI, from `1acf87a` to
  `f104d7c`.
- **Why it went unseen for six runs.** I was watching production - quarantine
  counts, projector lag, spool headroom - and production was the thing at risk.
  CI was a tab I never opened, and the certificate said the Rust gate had run.
- **How it was read.** The job-log endpoint needs admin rights on the repo and
  returned 403, but `/actions/runs/<id>/jobs` gives every step's conclusion
  without them - enough to name the failing step. Reproduced locally in the same
  `rust:1.82` image, which printed the exact rustfmt diff.
- **Closed.** `cargo fmt --all` (`053ea9b`, whitespace only), then the full
  three-clause command in that image: **FMT OK**, **CLIPPY OK** with
  `-D warnings` and zero warnings, **165 tests passed, 1 ignored** across the
  workspace. The certificate's `rust_gate` now records the command, the
  toolchain, all three results and this correction.
- **The rule.** A gate is the command CI runs, not the part of it that is
  convenient to run by hand. Reporting a subset under the gate's name is the
  same class of error as entry 32's test doubles: the check passed, and it was
  not checking the thing.
