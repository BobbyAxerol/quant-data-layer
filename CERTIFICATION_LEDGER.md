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

## 34. The certificate's own provenance had drifted from the images it certified (2026-09-17)

Clearing build artifacts after the tag was pushed, every digest in
`certificate.json` was mapped back to a local image id. Two of the four did not
point where their tag said.

- **`active_rust_image_digest`** read `sha256:4aa578fc`, which is
  `qdl-v2-rust:2.0.17-d9adee3` - the *first* lane fix, the one whose
  `_ => true` fallback was the defect. The tag beside it,
  `qdl-v2-rust:2.0.17-1acf87a`, was correct.
- **`active_python_image_digest`** read `sha256:97d2f593`, which is
  `qdl-v2-python:2.0.17-436171f`, an image built before the warmup change was
  reverted.
- **`image_source_commit`** read `ff34867`, the commit the certificate was first
  drafted at, and not a commit either running image was built from.
- **What actually runs**, read from the containers: five rust roles on
  `sha256:b05d4446`, eight python roles on `sha256:42fe008a`. The running stack
  was right the whole time; the paper describing it was not.

The cause is plain: the digests were captured by hand when the certificate was
drafted, and two rebuilds later - the revert and the second lane fix - nobody
went back for them. A rollback driven by that block would have deployed the
build that caused the outage.

Fixed by taking each digest from `docker image inspect` together with the
image's own `org.opencontainers.image.revision` label, so tag, digest and commit
check each other, and by recording the previous values inside the certificate
rather than quietly replacing them.

**The tag was already published when this was found.** The corrected certificate
is on `main`; the asset attached to the GitHub release still carries the stale
digests, and replacing it needs the release deleted and the tag re-pushed, which
is the owner's call, not mine.

Same class as entries 32 and 33: the check ran, and it was not checking the
thing. Here the check was a human copy of a value that later moved.

**Closed the same day.** The owner deleted the release; the tag was deleted on
the remote and re-cut at `658b6e8`, the corrected head of `main`. The asset now
published under `v2.0.17` is byte-identical to the certificate in the repository
(`sha256:0eaf55d3f06c3fcd0a68ce3b0518868efedbdc31842380cf888efef3529d3583`,
19,408 bytes) and names `sha256:42fe008a` and `sha256:b05d4446` - the two images
the containers are running. The superseded builds `2.0.17-436171f` and
`2.0.17-d9adee3` were then removed by digest; their identity survives in
`digest_correction.previous_values` and their source survives in Git.

One practical note for the next time evidence has to be read back from a
release: the browser download URL returns 404 through this host's proxy, because
it redirects to `objects.githubusercontent.com`. The asset endpoint with
`--noproxy '*'` and `Accept: application/octet-stream` returns it.

## 35. The routed Binance lane worked; its book lane stopped (2026-09-18)

R1.27 Phase 2. Applied 06:11:13Z, rolled back 06:21:04Z, one role each way.

- **The thing it was built for worked.** Binance mark/index went from **zero**
  canonical partitions to **five** within a minute, newest record one second
  old. For five months that plane did not exist, because the ingestor opened
  the base Binance decommissioned on 2026-04-23 and `/market` channels are
  acknowledged and then silent.
- **A feed that had nothing to do with the fix stopped.** Binance `book` froze
  at the recreate: nine partitions **425 seconds** stale while `quote`, `trade`
  and `mark_index_price` sat at 0-1 seconds and OKX was fresh throughout. Book
  feeds Risk's L2 gate, so the rollback was not a judgement call.
- **The rollback identified the cause and restored the system in one command.**
  Book returned to **0 seconds** on the pinned image, which rules out the
  restart: the previous binary resyncs its book and this one does not.
- **Five hypotheses were closed before rolling back**, so the retry does not
  re-walk them: the REST snapshot endpoint answered **HTTP 200** from the
  ingestor's own network; all four lanes were `LIVE` with fresh transport; the
  cores quarantined nothing; the ingestor logged no error in ten minutes; and
  `@depth@100ms` classifies to `/public` with the lanes opened as the feed table
  says. The frames reach the socket and do not become canonical, and **why is
  still unknown**.
- **One acceptance item was wrong before the work started.** I wrote that the
  alpha cache would read Binance mark/index from the canonical plane. It cannot:
  `adapters/market_data/data_layer_v2.py:752` routes `MARK_INDEX_PRICE` to the
  reference batch unconditionally, in the consumer, so no data-layer change can
  move it. The routed lane is the prerequisite for that switch, not the switch.

**The finding worth keeping.** Phase 1 was green three ways - fmt, clippy under
`-D warnings`, 177 Rust and 1,579 Python tests - and none of it covered the one
feed whose bootstrap is stateful. Neither suite takes a Binance BOOK lane from
subscription through the REST snapshot to a verified book. Entry 32 said the
same thing about a warmup: the check ran, and it was not checking the thing.
Twice now a unit-green change has been proved wrong only by production.

So the order is fixed before the next attempt: an offline harness that fails
when the book bridge does not complete, then a diagnostic that makes a book
which never verifies say so - today it is silent in both processes, exactly the
condition entry 30 paid for - and only then a second recreate.

## 36. The lane-aware book fence is in production, and the counter that would have found it (2026-09-18)

R1.27 Phase 2b. `qdl-v2-rust:2.0.17-e9cb4b7` (`sha256:432f4b62e567`) on
`rust_core`, `rust_core_2` and `rust_core_3`, recreated one at a time at
07:41:52Z, 07:45:41Z and 07:48:58Z, each verified before the next.

- **All five gate items held.** Running with `restart 0` and no error line;
  progress resuming within seconds; quarantines flat at 4/4/0; every Binance and
  OKX `book`, `quote` and `trade` partition newest **0 s**; consumer
  `execution_ready` 34 → 36 → 38 → 36, never below where it started, with
  `v1_fallback` and `v2_error` at 0 throughout.
- **Backward compatibility was measured, not argued.** Zero
  `IGNORED_STALE_GENERATION`, zero `l2_frame_refused` and zero
  `l2_session_began` on all three cores over twenty minutes. The running
  ingestors change their session id on every reconnect and never their lane, so
  the new decision path is not reached and today's behaviour is unchanged.
- **`filtered_by_outcome` paid for itself on its first use.** A core restart now
  says `BUFFERED_AWAITING_BOOTSTRAP`, `REJECTED_AWAITING_SNAPSHOT`,
  `BOOTSTRAP_APPLIED`, `KEEPALIVE` - and each froze once the books had their
  snapshots. Before 2a that was one `filtered` total, which is exactly why a
  book dying silently and a book bootstrapping normally were indistinguishable
  on the morning of 2026-09-18.

**Two readings that would be misread without saying so.** Quarantine counters
are per process: a fresh core starts at zero, so 4/4/0 cannot be compared with
the old processes' 2/9/5. And Binance `mark_index_price` sits at 5,632 s and
climbing - those five partitions were created by the failed 2c attempt and have
had no producer since the rollback; nothing reads them, and they age until 2c
lands.

**What this entry is not.** It is not evidence that the book fence fix works,
because nothing has yet changed a lane identity in production. It is evidence
that the fix is inert when it should be inert, which is the only thing 2b can
prove and the reason it is a separate phase from 2c.

## 37. The book fence is proved working, and one book still did not come back (2026-09-18)

R1.27 Phase 2c on the fixed cores. Applied 08:04:55Z, rolled back 08:13:52Z.

- **The 2a fix fired and was readable.** Nine `l2_session_began` events, one per
  Binance book binding, each naming `previous_generation 96` against
  `frame_generation 1` on a provably different lane. **Zero
  `IGNORED_STALE_GENERATION`, zero `l2_frame_refused`** on all three cores. The
  same moment on 2026-09-18 at 06:11Z produced no log line and a dead feed;
  entry 30's rule, applied to a second fence, worked.
- **The routed lane delivered its purpose.** Binance `mark_index_price` reached
  five canonical partitions at **0 s**, against zero partitions before the first
  attempt. Eight of nine books stayed at 0-1 s; quote and trade never moved.
- **One book did not recover.** `ethusdt-261225`, a dated quarterly, ran at a
  0.24 s median gap for the forty minutes before the roll - 5,996 rows - then
  published **four rows and stopped**. Snapshots kept being applied and the
  bridge never completed: buffered 498 → 2,157, bootstrap 7 → 24, quarantines
  4 → 17, looping about every thirty seconds. The rollback returned it to 0 s
  within three minutes, which identifies the cause as this path.
- **Two gate items failed.** `execution_ready` 37 → 32 at T+2 and 33 at T+10,
  measured where the gate says to measure. And the book item passed only in
  letter: "newest age 0 s" was true of the feed while one of its partitions was
  dead.

**The finding worth keeping is about the instrument, not the bug.** A feed-level
number hid a dead partition, exactly as a bare `filtered` count hid a dead feed
two days ago. Each time the fix was to make the measure one level finer, and
each time the next failure was one level finer still. `filtered_by_outcome` is
per core; it needs to be per binding. The acceptance item needs to read every
partition, not the newest. This is entry 33 again in a new place: a subset
reported under the whole gate's name.

**What is no longer in doubt.** The lane rule, `begin_session`, and the logging
all work in production under the exact condition that defeated them. What
remains is narrower and offline-reproducible: after `begin_session` clears the
bootstrap buffer, one book fails to bridge its snapshot to its deltas while
eight others succeed.

## 38. The stalled book was an off-by-one against Binance's own procedure (2026-09-18)

R1.27 Phase 2d, source only. Two 2c attempts were diagnosed from aggregate
counters and both diagnoses were guesses. The third diagnosis is not: the raw
topic keeps eight hours (R1.20), so the window was still there and **8,184 raw
frames** were captured - the book that stalled, a healthy peer on the same lane,
both lanes, deltas and REST anchors.

- **The stream was never broken.** `pu` chaining on the routed lane is 2,047 of
  2,047 for the stalled book and 4,135 of 4,135 for the peer, zero breaks.
  Measuring continuity by `U` instead reports ~100% gapped on *both* lanes,
  including the one where the book was healthy - the measure was wrong, not the
  data. Holes in `U` are normal in USD-M and are why the venue added `pu`.
- **The bridge is where it failed.** Of eighteen REST anchors in the window, the
  shipped rule could bridge **2** for the stalled book and 11 for the peer; the
  venue's documented rule bridges **8** and 14.
- **Quoted from Binance, "How to manage a local order book correctly":** drop
  any event where `u` **<** `lastUpdateId`; the first processed event should
  have `U` **<=** `lastUpdateId` **AND** `u` **>=** `lastUpdateId`. Our
  `continuity` compared against `lastUpdateId + 1` on both sides and dropped
  `u <= lastUpdateId`, so the event ending exactly on the anchor - the one the
  venue names as the bridge - was discarded as a duplicate and the next event
  read as a gap. An observed anchor shows it directly: `Y = 11588170543546`
  with the following event's `u` equal to `Y`.
- **Why it waited five months to appear.** A book already `Ready` is never
  re-bootstrapped; the 30 s refresh returns `Keepalive` and this rule is never
  reached. Phase 2c forced every book to re-bootstrap at once, and the slowest
  of the nine needed the exact rule to find a bridge.

**Corrected** to "no hole between the anchor and this event": drop when
`u < lastUpdateId`, apply when `U <= lastUpdateId + 1`. That admits the venue's
bridge and a perfectly contiguous successor, and nothing else. Only Binance uses
`RangeBridgeThenPrevious`; OKX is untouched. Three of the captured frames are
committed as a fixture and replayed by
`a_captured_binance_anchor_bridges_on_the_event_that_ends_on_it`; restoring the
off-by-one turns it red, so the production stall now lives in a unit test.

**Two measures changed, because the measure failed twice before the code did.**
`qdl_realtime_core_l2_status_changed` emits one line per book per status change
with both statuses, the generation, `last_sequence`, `snapshot_sequence` and the
pending bootstrap depth - a per-core counter cannot tell one book looping from
every book bootstrapping. And `scripts/verify_stable_feed_partitions.py`
enumerates every partition of every feed and fails on the worst rather than the
newest; run against production it reports 188 partitions and fails on exactly
the five Binance mark/index partitions the rolled-back 2c left without a
producer.

**The pattern worth keeping.** Entry 35 was a bare `filtered` counter hiding a
dead feed. Entry 37 was a feed-level "newest age" hiding a dead partition. Both
times the fix was to make the measure one level finer and both times the next
failure hid one level below it. This time the measure was inverted instead:
enumerate and fail on the worst. A number that answers "is anything fresh" can
always be satisfied by the healthy majority; only one that answers "is anything
stale" cannot.

Gate: fmt ok, clippy `-D warnings` ok with zero warnings, `cargo test
--workspace --locked` **184 passed, 1 ignored**; Python suite **1,579 OK,
7 skipped**. Nothing was built, recreated or deployed.

---

## 39. Binance USD-M is on the venue's routed base URLs, in production (2026-09-18)

**Pinned at** image `qdl-v2-rust:2.0.17-ee7f1b3`
(`sha256:a863f7e11c158545f98a4a5f708bc481da013cca393aa2ad1ba2ef9748dc11b9`,
revision label `ee7f1b3`); runtime config
`/home/bobby/.local/state/qdl-v2/dlv2-r127-2c-retry-20260918T090834Z/bundle/runtime`,
which differs from the sealed bundle in exactly two files -
`ingestor-binance-usdm.json` and `stable-acquisition-bindings.yaml` - both
carrying only the routed pair `wss://fstream.binance.com/public/ws` and
`wss://fstream.binance.com/market/ws`. Roles recreated: `rust_core` 09:11:34Z,
`rust_core_2` 09:15:00Z, `rust_core_3` 09:15:55Z, `ingestor_binance_usdm`
09:22:05Z. Nothing else in the 17-role stack was touched.

**Certified.** `shadow-certified` is the wrong word here and so is
`production-ready`: this is **production-authoritative**. The stack it runs in
is `RUST_PRIMARY` and serves the live consumer, which read it throughout with
`v1_fallback_count=0` and `v2_error_count=0`.

**What it fixes, as one measurement.** `scripts/verify_stable_feed_partitions.py`
before and after the ingestor roll:

```
09:20Z  binance mark_index_price  n=5  stale=5  worst_age=4014s   exit=1
09:24Z  binance mark_index_price  n=5  stale=0  worst_age=   3s
        188 partitions, 0 stale, 0 empty                          exit=0
```

Those five partitions had no producer because `markPrice@1s` is a `/market`
stream and the ingestor was still dialling the base Binance decommissioned on
2026-04-23. The data layer had **zero** canonical Binance mark/index events
before this roll and now has them at a measured 1000 ms cadence.

**Soaked.** The full eight-item acceptance passed at T+2 (09:24Z), T+10 (09:32Z)
and T+30 (09:52Z): four routed lanes LIVE at `gen=2`; 188 partitions with 0
stale and 0 empty at all three points; `l2_frame_refused=0` on all three cores;
all 9 Binance books `READY` with `pending=0` by name, not by counter;
`execution_ready_v2_slices` 31 -> 35 -> 36. The `QUIET` slices at T+2 were the
consumer re-establishing after the session generation moved 1 -> 2 and were gone
by T+10.

**Why the third attempt worked where two failed.** Not luck and not a retry. The
cores carried the corrected bridge rule (entry 38) before the ingestor produced
frames under new lane names, and the acceptance was the inverted one: enumerate
every partition and fail on the worst. Entry 37's attempt passed its own gate
while a partition was dead; this gate could not have.

**Four quantities, per routed feed, steady state**, from
`scripts/report_feed_latency_quantities.py` (committed with this change; median
per partition over a 300 s window, 200 events each, 24 Binance partitions, none
quiet):

| feed | 1 venue->recv | 2 recv->pub | 3 pub->durable | 4 venue->durable | p95 total | event period |
|---|---|---|---|---|---|---|
| book (9) | 26 ms | 0 ms | 396-578 ms | 424-608 ms | 0.79-1.22 s | 102-256 ms |
| quote (5) | 27 ms | 0 ms | 427-485 ms | 454-512 ms | 0.72-0.81 s | 55-81 ms |
| trade (5) | 27-28 ms | 0 ms | 496-737 ms | 523-765 ms | 0.58-0.99 s | bursty |
| mark_index_price (5) | **72-74 ms** | 0 ms | 437-606 ms | 509-685 ms | 0.85-1.57 s | **1000 ms** |

The `/market` edge is ~46 ms further away than `/public`; that is the route, not
our code. `markPrice@1s` arrives at a measured 1000 ms period, the documented
cadence. The dominant hop on every feed is `published -> durable`, the
projector's commit, already measured in R1.22 - routing did not change it.

The same run against OKX, for contrast and because the owner asked for every
endpoint rather than the one being changed:

| feed | 1 venue->recv | 2 recv->pub | 3 pub->durable | 4 venue->durable | p95 total |
|---|---|---|---|---|---|
| book (9) | 28-31 ms | 0 ms | 436-564 ms | 466-594 ms | 0.86-1.21 s |
| quote (5) | 28 ms | 0 ms | 382-577 ms | 411-605 ms | 0.68-0.97 s |
| trade (5) | 29 ms | 0 ms | 493-713 ms | 521-742 ms | 0.68-1.77 s |
| mark_index_price (5) | 45-90 ms | **145 ms** | 449-550 ms | **735-1013 ms** | 1.27-2.11 s |

OKX `MARK_INDEX_PRICE` is the one feed in the stack with a non-zero
`received -> published` hop: 145 ms of normalisation, which is where its total
exceeds Binance's despite a comparable wire and commit. Nothing in R1.27 touched
it and nothing here explains it; it is recorded so it is not rediscovered.

**Open, and not caused by this roll.** The consumer reports three to five
Binance and three to four OKX `QUOTE` slices `STALE` at `age_seconds` 19-51
while the spool's own quote partitions read 0 s. Symmetric across both venues,
present in the 09:20Z measurement taken before the ingestor moved, and OKX was
not touched by this program. It is a consumer-side `QUOTE` delivery gap and it
needs its own investigation.

**Rollback** was prepared, proven twice in entries 35 and 37, and not used:
`--env-file rollback.env` on the same override pins core `432f4b62` and ingestor
`b05d4446` against the sealed bundle.

---

## 40. Binance native 1m bars: admitted by measurement, gated in source, not rolled (2026-09-18)

**Pinned at** commit of this entry; `qdl-v2-rust` not rebuilt, no role recreated,
`config/v2/stable-acquisition-bindings.yaml` revision 16 to 17 in the repository
only. The running stack still publishes Binance 1m over REST.

**Certified.** `tested locally` and nothing above it. The admission evidence is
live and from the venue; the lane it admits has never run here.

**The admission evidence** - `scripts/certify_binance_native_bar_admission.py`,
against `wss://fstream.binance.com/market/ws` with a SUBSCRIBE, the same control
shape the ingestor uses:

```
15 of 15 final klines, five symbols, three closes each
arrival after close: min 0.043 s, median 0.253 s, max 1.144 s
REST identical to the WS final bar: 1/15 at +0 s, 6/15 at +2 s,
                                    8/15 at +4 s, 15/15 at +6 s
```

The websocket bar is what the venue settles on; REST takes up to six seconds to
agree with it. The 6 s settlement guard is the cost of reading bars over REST,
not a correctness requirement. `production_catalog.py` had kept Binance BAR on
REST behind a comment asking for exactly this evidence - evidence that could not
exist while the ingestor dialled the base Binance decommissioned on 2026-04-23.

**One provider event identity per closed bar.** A closed kline now keys on
`open_time:close_time`, identical to the REST path, so the REST row and the
native row for one bar are one event. A provisional kline keeps its frame-scoped
key. OKX had this by design (`canonicalize_okx_bar`); Binance did not, and the
cutover needs it because both producers are briefly live. Rust and Python were
changed together and the two Binance bar goldens regenerated; the other 17 did
not move.

**One kind, two provider shapes.** The bar edge bootstraps warmup history over
REST for every enabled BAR demand regardless of mode, and the core refuses two
bindings sharing a `source_id`, so a native binding still receives REST-shaped
frames. Found by the C40 live-parity corpus failing with "Binance kline frame
requires k object". The dispatch now sends `k` to the kline path and `row` to the
REST path, in both languages.

**Gate.** `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets
--locked -- -D warnings`, `cargo test --workspace --locked`: clean, clean,
**186 passed / 0 failed / 1 ignored** (184 before). Python suite **1,588 OK,
7 skipped** (1,579 before), in `qdl-v2-python:2.0.17-5c01cb6`. Two new Rust
tests and nine new Python tests, including one that is red if the frame-scoped
sequence is restored.

**Reconciliation baseline**, before any cutover:
`scripts/reconcile_native_bars_against_rest.py` compared 25 canonical bars across
the five symbols against REST for the same `open_time` and found **0
disagreements**. It publishes nothing and records no revision; see below.

**Why it is not rolled.** Two things, neither of which is a code defect.

1. **The consumer contract for a revised bar is unsettled**, which the plan
   itself names as a precondition of Phase 3 rather than a step inside it.
   `adapters/market_data/data_layer_v2.py:407-420` treats `FINAL` and `REVISED`
   identically and carries neither `revision` nor `supersedes_event_id`, so what
   a strategy does with a bar it already acted on is undefined. That is why the
   reconciliation shipped here reads and reports rather than publishes.
2. **The bar edge's checkpoint pins `acquisition_revision`.** `_restore_state`
   raises `stable BAR checkpoint acquisition_revision differs from runtime
   authority` on a mismatch; the running edge is on revision 14 from its own
   packet and this change makes the repository 17. Recreating it therefore needs
   a checkpoint migration that no step of Phase 3 describes - R1.24's "a
   regeneration is a migration", one layer down.

**And one correction.** Phase 3 item 3 claimed the bar edge "filters by mode for
OKX but not for Binance". It has been venue-neutral since `302eb21`
(2026-08-25): measured against the shipped plan, 70 Binance BAR bindings polled
and 0 OKX. The claim was copied into the plan from a docstring on 2026-09-18
without reading `stable_bar_edge.py` - the failure rule E1 exists to prevent,
committed while writing that plan. The rule now has a name,
`recurring_rest_bar_bindings`, and a test on both venues.

---

## 41. Binance USD-M 1m bars are native, in production (2026-09-18)

**Pinned at** `qdl-v2-rust:2.0.18-3ecf0ac`
(`sha256:eec6388421845ef570cb3878145d0c5e0398fc44cc2a662805298295cfb0976a`) on
`rust_core`, `rust_core_2`, `rust_core_3` and `ingestor_binance_usdm`;
`qdl-v2-python:2.0.18-<channel fix>`
(`sha256:afb053ee14fc663c01e8871619d5043cfc449db2b5d215c1cce09ba75dba6898`) on
`binance_bar_edge`. Runtime config
`/home/bobby/.local/state/qdl-v2/dlv2-r128-native-bar-20260918T120904Z/runtime`.
Rolled 12:17:11Z to 12:26:22Z. Eleven other roles untouched.

**Certified** `production-authoritative`: the stack is `RUST_PRIMARY` and served
the live consumer throughout with `v1_fallback_count` and `v2_error_count` at 0.

**What changed, measured.** Close-to-canonical for Binance USD-M 1m:

```
before (REST edge)          6,630 ms
after  (native /market)     1,107 - 4,852 ms, median ~1,500 ms
every bar: origin=VENUE_NATIVE, lifecycle=FINAL
```

**No gap anywhere.** `scripts/verify_stable_feed_partitions.py` at T+4:
**188 partitions, 0 stale, 0 empty**. That is the point of the order used, and
it was not the order this plan originally specified.

**The order, and why.** The cores route a raw envelope to a binding by its
`native_channel`, and a binding has one channel, so the REST and native
producers cannot both be routable at once. `strict_subscription_scope` is true,
so a mismatched frame is quarantined rather than ending the consume loop
(`qdl-realtime-core.rs:326`). That made the cheap failure the REST one: cores
first, ingestor immediately after, bar edge last. The alternative - bar edge
first, as the plan said - would have left two to four minutes with no 1m
producer in V2 at all.

**Two regenerations refused, both the R1.24 trap.** Generating the ingestor
config from the current catalog **removes the five Binance MARK_INDEX
bindings**; generating the core configs removes **ten** of them, five per venue.
Those are the bindings R1.27 brought back to life the same morning. Both were
applied as targeted transforms instead: the ingestor config gained five BAR
bindings and kept everything else, the core configs changed five bindings'
`native_channel` and `provider_kind` and nothing else, verified by diffing the
result against the running config before it was applied. The R1.24 migration
that moved MARK_INDEX to the reference path is still unfinished, and every
regeneration will keep trying to finish it.

**One defect found in production and fixed in the same window.** After the core
roll, the bar edge was still stamping its REST warmup rows `rest-klines/1m`
while the cores had moved to `btcusdt@kline_1m`. Every one was quarantined as
FencingRejected - 1,447 to 3,486 per core - and the 1m warmup repair was
publishing into nothing. The canonicaliser already accepted both provider shapes
under one kind; the capture channel had to follow the acquisition mode too. Once
that shipped the quarantine counters stopped moving.

**Checkpoint migration.** `scripts/migrate_stable_bar_edge_checkpoint.py` carried
the bar edge across acquisition revision 16 to 17 with all 140 watermarks, the
connection generation and the canonical cache identity, keeping the previous file
at `.pre-r17`. The edge restored and did not re-bootstrap its watermarks; it did
run its ordinary bounded history repair, which is the pre-existing gap between
`warmup_rows=10000` and the spool's retention, not an effect of the migration.

**Reconciliation**, after the cutover: 20 canonical bars across the five symbols
compared against Binance REST for the same `open_time`, **0 disagreements**.

**`PROVISIONAL_BAR` is live**: 8,271 to 11,326 per core in the first twenty
minutes. A filtered provisional kline now says why it was filtered.

**Rollback**, unused: `--env-file rollback.env` on the same override pins the
previous rust and python digests and the sealed R1.27 bundle; the checkpoint
restores from `.pre-r17`.

---

## 42. The CPU ceiling incident: throttling was admission control (2026-09-18)

**Pinned at** commit `5676656` (the instruments), running cores on
`qdl-v2-rust:2.0.19-003b5f9`, projectors on `qdl-v2-python:2.0.17-5c01cb6`
with ceilings raised to 2.0 by `docker update`; compose at the proven baseline.

**Certified** `incident`. Nothing here is a pass.

**What happened.** Every role's CPU ceiling was raised because every role
showed throttling: kafka2 13.2% of periods, ingestor_okx_swap 5.8% for 5,945 s,
query_v2_1 5.4% for 1,372 s. Throttling fell as intended. Venue-to-durable
latency, unmeasured between the two rounds of raises, went from **475 ms to
17,942 ms** at p50, the cores fell behind the raw topic by 13-21 s, sixteen
partitions went stale and the consumer dropped from 50 to 31 ready slices.
`v1_fallback_count` and `v2_error_count` stayed 0 throughout; no endpoint
stopped and no consumer fell back.

**Why.** On a 16-vcore host at load 12, the tight ceilings were the only
admission control the stack had. Raising fourteen of them at once let every
tier burst together; the producers were raised further than the consumers,
which manufactured a backlog; and the first revert came while that backlog was
still draining, which put the cores into a lag spiral at 0.50 CPU. The data
layer's actual draw reached **6.1 vcore against 3.5 for everything else on the
host** - portal, trading system, alphas combined.

**Recovery, partial.** Cores and projectors were given 2.0 and left there. The
cores are healthy (`raw_age` min 6-29 ms). One quote partition,
`binance-usdm-solusdt-quote`, is still **42 minutes behind and losing ground**;
the projectors are not CPU-bound and log nothing, and the cause is unknown. The
projector spans that would name it were committed in `5676656` and not
deployed before the tuning began - the order this revision's own text
prescribed and its author did not follow.

**Also.** Five preflight containers had been left running without `--rm`,
spinning on a missing env var for three to seven hours each. Two were removed
in R1.28 and the removal was reported as complete; three
(`goofy_heisenberg`, `nervous_moore`, `exciting_kare`) were still running and
were removed while writing this entry. Four images with no container remain:
2.9 GB, listed for deletion in the R1.29 guide, Phase 0d.

**Recorded rather than edited out** because the failure is the method, and
the guide that follows it in the plan is built from these numbers.

---

## 43. R1.29 Phase 0 to Phase 3: the instruments landed, every tuning candidate was rejected (2026-09-18)

**Pinned at** commit of this entry. Running images, each digest from
`docker image inspect`:

| role | image | digest |
|---|---|---|
| `rust_core`, `rust_core_2`, `rust_core_3` | `qdl-v2-rust:2.0.19-003b5f9` | `sha256:b7b9d153f0ed…` |
| `ingestor_binance_usdm` | `qdl-v2-rust:2.0.18-3ecf0ac` | `sha256:eec638842184…` |
| `ingestor_okx_swap` | `qdl-v2-rust:2.0.17-1acf87a` | `sha256:b05d44467942…` |
| `projector_v2`, `_2`, `_3` | `qdl-v2-python:2.0.19-211bf14` | `sha256:1d99cc83f8e0…` |
| `stream_v2_active`, `_passive` | `qdl-v2-python:2.0.19-40629b7` | `sha256:656e7e043dea…` |
| `query_v2_1`, `_2` | `qdl-v2-python:2.0.17-5c01cb6` | `sha256:42fe008a1b9f…` |
| `binance_bar_edge` | `qdl-v2-python:2.0.18-5ea5915` | `sha256:afb053ee14fc…` |

The projectors are one commit behind the streams. `40629b7` differs only in
`serve_stable_stream`, so their behaviour is identical and R3 left them alone.

**Certified.** The instruments are `production-authoritative`. The tuning is
`measured and rejected` - nothing from Phase 2 is in the running stack.

**Closing state**, 20:17Z: **188 partitions, 0 stale, 0 empty**;
`v1_fallback_count` and `v2_error_count` **0**; consumer 49-56 ready and 30-37
execution-ready across the session; data layer draw **4.64 vcore against a 5.0
budget**; compose ceiling sum **20.75, the baseline, unchanged**; **no unnamed
container**; nine `qdl-v2-*` images, every one of them referenced except the
ingestor's one-step rollback.

**What landed.** Three roles that had never logged now log. The projector and
the stream had no handler at all - `stable_bar_edge.main` calls
`logging.basicConfig` and nothing on their paths ever did - so the projector's
`"generation failed; reconnecting"` and the gateway's subscription counters were
written into a root logger with nothing attached, for as long as those roles
have existed. uvicorn took the handler back off the stream after `basicConfig`
put it on, which the first C5 roll proved and `log_config=None` fixed.

**What that instrument then answered, in one session:**

* The 42-minute `solusdt-quote` stall was consumer lag on the canonical topic,
  18 and 36 minutes on two projectors, `durable_append` about 1 s. It drained
  on its own at twice real time and **nothing was changed to make it**.
* `rust_core` at a 0.50 ceiling is disproven: `raw_age` min 8 ms to **5,755 ms**
  in twelve minutes, pinned at 51% of its ceiling. Paid for under R4 by
  `rust_core_2`, which drew 0.09 of a 1.00 ceiling at the same instant - the
  three cores stopped being equally loaded across R1.24, R1.27 and R1.28.
* The QUOTE livelock is `rejected_at_push` **254,834** against
  `aged_out_at_read` **956**, and there is not one `slow_consumer` line: the
  gateway never ejected a consumer, so those reconnects are the consumer's own.
  The guide's own hypothesis was the other way round.

**Every tuning candidate was rejected on its own criterion**, which is the
point of writing the criterion first. C3 cut kafka2's throttling from 16.9% to
4.2% and still failed - p50 fell on two feeds of four while draw went 5.08 to
5.63 vcore and load rose. C2 raised the `raw_age` it was meant to lower, and
the commit improvement that looked like its doing appeared equally on the two
cores it had not touched.

**Three corrections of this executor's own reports**, recorded rather than
edited out:

1. A drain check read column 7 (`p95`) where the criterion names column
   `4 total`, so two windows were called failures and were not.
2. The incident entry said the compose file was back at baseline. It was not:
   the revert was a `git checkout` of a file whose raises had been committed,
   and the ceiling sum was 26.00 until 18:20Z.
3. R1.28 reported Binance weekly bars at `origin=RECON`. `BarOrigin` is
   `AGGREGATED 2, BACKFILLED 3, RECONCILED 4`; the value was 3, which is
   `BACKFILLED` and is correct for a bootstrapped weekly bar. There was never
   an anomaly.

**Gate.** Python suite **1,598 OK, 7 skipped**, in `qdl-v2-python:2.0.17-5c01cb6`.
No Rust source changed after `003b5f9`, whose three-clause gate was green at
186 passed.

**Not done, and why.** Phase 3 item 1 - the thirteen Binance intervals - is
gated on Phase 2 holding twenty-four hours and waits. Phase 4's push and release
were refused by this environment's publication control and need the owner.
R1.30 is opened for the MARK_INDEX regeneration trap, which R1.28 survived only
because someone diffed by hand.

---

## 44. R1.31: a lossy-bundle refusal, a hung projector, and R1.30's gate answered (2026-09-19)

**Pinned at.** `dev` at `766d5ae`; `qdl-v2-rust:2.0.19-003b5f9`
(`sha256:b7b9d153f0ed…`), `qdl-v2-python:2.0.20-e7fd0c9`
(`sha256:a8419c182180…`). Catalog revision **8**, acquisition revision **17**,
routing revision **18** - none moved.

**Roles and digests as certified:**

| role | image |
|---|---|
| `rust_core` ×3, `ingestor_okx_swap` | `qdl-v2-rust:2.0.19-003b5f9` |
| `ingestor_binance_usdm` | `qdl-v2-rust:2.0.18-3ecf0ac` |
| `projector_v2` ×3, `query_v2` ×2, `binance_bar_edge` | `qdl-v2-python:2.0.20-e7fd0c9` |
| `stream_v2_active`, `stream_v2_passive` | `qdl-v2-python:2.0.19-40629b7` |

`ingestor_binance_usdm` and the two stream roles are at functional parity with
the release commit: the streams have **zero** files changed in `qdl/stream/`
since their image, and the ingestor's one file is `qdl-realtime-core.rs`, which
is not the binary it runs (`docker inspect` shows
`/usr/local/bin/qdl-native-raw-ingestor`).

**No Rust image was built.** `git diff 003b5f9..HEAD -- rust/ Cargo.* generated/rust`
is empty, so the deployed 2.0.19 image already is HEAD's Rust; `ingestor_okx_swap`
was moved onto it rather than onto anything new.

### Certified

**A lossy regeneration is now refused, not caught by eye.**
`scripts/assert_runtime_bundle_is_not_lossy.py`, 15 tests. Against the live
bundle it reads **629 bindings in five files** and refuses all forty MARK_INDEX
identities. A first cut keyed only on `source_id` read 576 in three and missed
both ingestors entirely - which is the count-neutral half of the R1.30 trap. It
earned itself the same hour: the unapplied `ingestor-okx-swap.json` in the r128
packet holds **24** identities against the running **94**.

**The VN plane is pinned to V1 by test, not by memory.** All four
`vn_primary_v2` requirements already routed `V1_PRIMARY`/`NONE`; three further
fences hold - `BAR_EDGE_RUNTIMES`, the `stable-vn` Compose profile, and no DNSE
binding on any ingestor. 10 tests. Owner decision, 2026-09-19.

**A projector that cannot close its broker no longer stops projecting.**
`projector_v2` and `projector_v2_3` were `Up`, silent and idle from 05:07 and
05:09, both stopping immediately after `attempt=5` with `retry_max_seconds` at
5.0. The only await in the recovery path was an unbounded
`asyncio.to_thread(broker.close)`. Kafka had rebalanced all six
`md.canonical.v2` partitions onto the one survivor, lag to **47,258**. Bounded
at 10 s, 6 tests driving the real supervisor with a blocking broker. After the
roll: three consumers, two partitions each, lag **318**.

**Seven of thirteen Binance intervals admitted on the routed lane**: 3m, 5m,
15m, 30m, 1h, 2h, 6h, each PASS on 5 of 5 symbols, final bar **0.079-0.527 s**
after close against 7,820-22,473 ms on REST. The lane is proven for all
thirteen - subscription accepted, 63-64 provisional frames each in 45 s, and
every long interval's `t`/`T` matching `canonical_interval_ms` with `1w`
anchored to Monday. Six captures run unattended to their own boundaries at
0.03 vcore.

The script could not have been run per interval before this: its budget was
`60 * (BAR_COUNT + 1)`, one minute per bar, so every interval other than 1m gave
up before its first boundary and printed nothing. 1m is unchanged at 160 s, so
entry 40's evidence still stands.

**R1.30's gate is answered, and the answer is no.** R1.24 left one unknown and
R1.30 restated it: can the reference path serve `MARK_INDEX_PRICE`? It serves
it and cannot meet the bound the manifest grants. Measured through
`OkxRestClient` itself, on a 90-100 ms round trip:

| | |
|---|---|
| manifest | `max_freshness_ms: 2000`, `EXECUTION`, ten requirements |
| OKX REST `index-tickers` `ts` | **764 - 1,414 ms** stale at the venue |
| consumer observes | 2,088 - 2,501 ms, p50 2,212 |
| canonical binding | **286 - 418 ms** end to end |

Both terms the consumer divides are stamped at fetch, so no cache, lane or
bucket on this side closes it.

### Health at certification

17/17 roles Up, **4.86 vcore** of the 5.0 budget, `quarantines` 0,
`scope_quarantines` 0, `raw_age_ms` mean 138. **188/188 partitions inside their
own declared `stale_after_ms`** - read from a `--rm` side container with the
state volume mounted read-only, which is the only way this should have been read
all session. Consumer errors per minute against the pre-session window:
`DataLayerError` **-96%**, `SilentSliceError` -47%,
`StaleExecutionReferenceError` -32%.

**Gate.** Python suite **1,628 tests, 7 skipped, 0 real failures** in
`qdl-v2-python:2.0.20-e7fd0c9`. The four reported errors were all
`RotatingFileHandler` opening `/app/logs/app.log` under a mount the container's
uid could not write; the same four modules run 14 tests and pass with it
writable. No Rust source changed after `003b5f9`.

**Cleanup.** Build cache 15.57 → 6.46 GB, disk 113 → 107 G. Deleted by digest:
`qdl-v2-rust:2.0.17-ee7f1b3`, `qdl-v2-python:2.0.20-6bd9e74` (a duplicate
`2.0.20` tag), two anonymous 0 B volumes. Kept deliberately: one rollback digest
per role, `rust:<none>` (the builder base pinned by
`Dockerfile.qdl-rust-runtime`), the reuse image set, and the volumes
`stable_authority_db`, `qdl-cargo-home`, `qdl_c40_authority_admin_packets`. No
probe container survives; `docker ps -a` shows none unnamed.

### Five corrections of this executor's own reports

1. **A probe restarted a production role.** Every endpoint reading ran
   `docker exec` inside `query_v2_1` with `cache_size=-64000` and a full-table
   `GROUP BY`. That role has a 512 MiB limit and serves at 120 MiB; at 06:03 it
   exited 0 and Docker restarted it.
2. **"The spool is 2.3 s behind"** - four times, reversed four times. The probe
   read the clock *after* its own 1.2 s query. Corrected: 286-418 ms, which is
   `canonical_age` 261 plus `durable_append` 66.
3. **"33 of 188 partitions are stale"** was a draining backlog, not a state.
4. **"The 750 ms TTL moved the tail by 580 ms"** - it cannot have.
   `received_at_ns` and `observed_at_ns` are both stamped at fetch and travel on
   the cached result, so a cache hit reports the same number. The drop was venue
   variance. The TTL is kept for a different reason: the cache age is an
   *unreported* addition to the age of an execution-grade input.
5. **"Four packets are unmounted evidence"** - four of the eleven are live bind
   mounts carrying the running configuration; the check used
   `--filter volume=`, which does not match bind mounts.

### Not done, and why

**Item 4 is not implemented.** Its fix is to declare the ten `MARK_INDEX_PRICE`
bindings in the source catalog so the spool can serve them - which **reverses
the direction R1.24 chose**, on an execution-grade risk input. The latency
consequence of R1.24 is measured here; R1.24's own reasoning is not re-examined.
That is an owner decision, not a release-day edit, and committing a revision-9
catalog the runtime is not on would manufacture exactly the drift entries 39-43
spend their pages chasing.

**Six intervals are in flight, not certified.** Their boundaries fall at 08:00Z,
12:00Z, 00:00Z and 2026-09-21.

**Nothing was pushed, merged, tagged or released.** The owner reserved that
("nếu ổn thì tôi sẽ duyệt") and has not given it. Twenty commits wait on `dev`.

## 45. R1.31 release gate: the suite is green, one regression was mine, 6 vcore (2026-09-19)

**Pinned at.** `dev` at the commit carrying this entry.
`qdl-v2-python:2.0.20-95d9595`
(`sha256:9039236e7a8e570f2364b470b33386ab702bc1dde5ae9d5e7d90a4dda531e8f0`),
`qdl-v2-rust:2.0.19-003b5f9` (`sha256:b7b9d153f0ed…`),
`qdl-v2-rust:2.0.18-3ecf0ac` (`sha256:eec638842184…`). Catalog revision **9**,
source policy **1**, authority **1**.

| role | image |
|---|---|
| `projector_v2` ×3, `query_v2` ×2, `stream_v2_active`, `stream_v2_passive`, `binance_bar_edge` | `qdl-v2-python:2.0.20-95d9595` |
| `rust_core` ×3, `ingestor_okx_swap` | `qdl-v2-rust:2.0.19-003b5f9` |
| `ingestor_binance_usdm` | `qdl-v2-rust:2.0.18-3ecf0ac` |

**No image was built for this entry.** The two changes are test-only
(`5f85410`) and a new read-only script (`b7d62e7`).

### Certified

**Suite green: 1675 tests, 0 failures, 7 skipped.** The three failures at the
gate were caused by this session's own headroom change (64 → 2064) and were
stale literals pinning `10_064`. The substantive one,
`test_public_bar_warmup_scans_physical_tail_before_market_selection`, was
reparametrised on the headroom rather than re-pinned, and then **mutation
tested**: forcing the bar scan back to the public window fails it with
`PARTIAL != FULL`, so it still discriminates instead of passing vacuously.

**History is correct for past dates, checked against the venue, not against
ourselves.** 14/14 Binance intervals, every one `FULL`, compared field by field
with Binance REST at both ends of the window:

| iv | 1m | 5m | 15m | 1h | 4h | 6h | 12h | 1d | 3d | 1w |
|---|---|---|---|---|---|---|---|---|---|---|
| time | 431ms | 444ms | 323ms | 395ms | 265ms | 269ms | 107ms | 98ms | 100ms | **58ms** |
| back | 0.1d | 0.4d | 1.3d | 5.0d | 20.0d | 30.1d | 60.4d | 120.4d | 362.4d | **845.4d** |

**Delivery to a consumer, measured from where the consumer stands.** Stream
delivery is the venue's stamp to arrival; the subscribe gate is 40 attempts per
feed with the trading system's own certificate, **200 subscribes, 0 refused**:

| feed | p50 | p95 | max | budget | margin |
|---|---|---|---|---|---|
| TRADE | 627.7ms | 1029.8ms | 1242.1ms | 3000ms | 1758ms |
| QUOTE | 643.2ms | 1171.2ms | 1188.8ms | 2000ms | 811ms |
| MARK_INDEX_PRICE | 648.7ms | 1285.9ms | **1647.6ms** | 2000ms | **352ms** |
| BOOK_SNAPSHOT | 625.7ms | 1497.7ms | 1652.7ms | 60000ms | wide |
| BOOK_DELTA | 747.1ms | 1249.5ms | 1562.3ms | 2000ms | 438ms |

`MARK_INDEX_PRICE` has the thinnest margin in the system at 352 ms and is the
one feed that a pipeline hiccup can push past its `BLOCK` policy. That is a
property of a 2,000 ms budget against a ~650 ms path, not a defect, and it is
named here so the next reader does not rediscover it as an incident.

**Endpoint inventory.** 216 catalog bindings: **206 live, 0 over their own
budget, 10 with no event stored.** The ten are the four DNSE bindings - owner
decision, served by V1, and live V2 refuses them with `required data is not
available` - and six spot bindings that **no ingestor produces and no consumer
manifest requests**. The 53 ingestor subscriptions are 24 `binance-usdm` and 29
`okx`, none spot, so the six are dead catalog weight rather than a gap.

**CPU budget 5 → 6 vcore, spent by measurement.** The stack was at 4.70 of 5.0
(94%). The seventeen per-container limits already sum to 17.0 vcore on a
16-vcore host, so the raise went to the four cgroups actually losing time:
`kafka2` 14.4% of periods throttled (9,612 s) and `kafka3` 5.2% (2,480 s) to
1.75; `rust_core_2` 8.9% and `stable_redis` 1.8% to 0.75. `kafka1` at 1.4% and
the projectors and gateways at ≈0% were left alone. Applied with
`docker update --cpus`, **no container recreated** - which is what keeps
`stable_redis` away from `ProjectionCacheMismatch`. Now **5.66 of 6.0**; the
headroom was absorbed at once, which is the evidence the brokers were starved.
Memory was checked separately because a `cpus` limit throttles and cannot OOM:
worst headroom is `kafka2` at **50.2% of 2 GiB**, so there is no OOM exposure.

### Health at certification

17/17 roles `Up`, 14 reporting `healthy`; the three `rust_core` replicas carry
no healthcheck, which needs a Rust change and is recorded with B3.

### Cleanup

Four superseded python builds deleted **by digest**: `2.0.20-4baada5`,
`2.0.20-d661428`, `2.0.20-7f8dac2`, `2.0.18-5ea5915`. Images 33 → 29,
14.82 → 14.04 GB - layer sharing, not the nominal 3.5 GB. Unused build cache
994.8 MB → **0**, 6.60 → 5.61 GB. **No volume removed**: all three dangling ones
are the three kept deliberately (`qdl-cargo-home`,
`qdl_c40_authority_admin_packets`, `stable_authority_db`). **No container
removed**: the only two stopped are the stack's own one-shot inits, and every
probe this session ran `--rm`. Kept as the one rollback per role -
`2.0.20-e7fd0c9` (projector/query/bar-edge), `2.0.19-40629b7` (stream),
`2.0.17-1acf87a` (OKX ingestor, and referenced by three files in the active
chain). `tradingsystem-image:v1.2.4-8ef859a` was left alone as another repo's
rollback target.

### Four corrections of this executor's own reports

1. **A regression I caused, in the B1 roll.** At 08:21:55Z `ingestor_okx_swap`
   was recreated with a chain carrying `r125-rollout.override.yml` (old digest
   pin) but **not** `okx-ingestor-image.override.yml`, so it fell from
   `2.0.19-003b5f9` to `2.0.17-1acf87a` - the digest that override names as its
   own rollback - undoing R1.31 item 1 and leaving the OKX producer nine `rust/`
   files behind the three cores it feeds. Not a live outage: 206 bindings live
   and none over budget throughout. Repaired by appending the missing override
   and recreating that one role; the rendered diff was **exactly one line** and
   the B1 healthcheck survived the merge.
2. **"Sixteen bindings are over budget."** No. The first cut of the liveness
   report compared the spool's `committed_at_ns` against `stale_after_ms`. The
   runtime reads a `PROVIDER_CONFIRMATION` binding - all ten
   `MARK_INDEX_PRICE` ones - from `received_at_ns`
   (`stable_source.py:483-492`), which is the whole point of R1.24. Against the
   runtime's own rule: **206 live, 0 over budget**.
3. **"TRADE stream p50 is 1530 ms, a regression."** It was measured six minutes
   after the projector cold start at 08:29:26, while the restart backlog
   drained. Steady state is **619 ms**. The spikes are confined to
   08:29:41-08:30:21 and 08:35:16-08:35:59 and nothing exceeds 3 s after 08:36.
4. **"MARK_INDEX_PRICE is refused on EVENT_AGE."** Same window. Steady state is
   **0 refusals in 200 subscribes**, and the instrument is Binance USD-M, so the
   OKX `index-tickers` finding never applied to it.

### Not done, and why

**`stable_redis` remains a single point of failure and was not touched beyond
its cpu ceiling.** Recreating it is the documented way to freeze the spool.

**One optimisation is measured and deferred.** Each projector posts every
canonical batch to both gateways and the non-holder answers 409: **6,080
rejected POSTs per projector per 30 minutes**, ~36,000 an hour across three.
Removing it is projector code on the write path and belongs with B2/B3.

**Nothing was pushed, merged, tagged or released.** The owner reserved that
explicitly ("trước khi tôi duyệt release"). Commits wait on `dev`.
