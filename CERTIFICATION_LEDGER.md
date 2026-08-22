# Certification Ledger

**Mục đích: không chạy lại thứ đã chứng nhận.** Trước khi test bất cứ gì, đọc file
này. Chỉ chạy lại khi **commit / image / config** ở cột "Pinned at" đã đổi.

Ledger phủ cả ba repo. Cập nhật cùng transaction với commit làm nó đổi (G4).

Cập nhật lần cuối: **2026-08-22**, data layer `833f4ce`.

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

**Chặn ở CA private key**: `cert-material/ca.crt` có, `ca.key` **không có** ở
`~/.local/state/qdl-v2` lẫn trong repo.
