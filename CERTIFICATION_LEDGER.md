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
