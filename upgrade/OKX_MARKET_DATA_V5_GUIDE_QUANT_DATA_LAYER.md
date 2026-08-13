# OKX API v5 Market Data Integration Guide cho `quant-data-layer`

> **Trạng thái:** Implementation specification / agent guide
> **Mục tiêu hệ thống:** [`BobbyAxerol/quant-data-layer`](https://github.com/BobbyAxerol/quant-data-layer)
> **Nguồn chuẩn:** [OKX API v5](https://www.okx.com/docs-v5/en/) và [OKX API changelog](https://www.okx.com/docs-v5/log_en/)
> **Ngày đối chiếu:** 2026-08-13
> **Phạm vi:** Market Data, Public Data, Status, WebSocket JSON, order-book state, lịch sử, normalization, Redis/REST contract, khả năng mở rộng SBE
> **Ngôn ngữ triển khai ưu tiên:** Python async; Rust/SBE là phase tối ưu riêng, không làm thay đổi contract phía consumer.
> **Program tracker:** [`DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md`](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md)
> **Kiến trúc nền:** [`quant-data-layer-fund-grade-upgrade-architecture.md`](quant-data-layer-fund-grade-upgrade-architecture.md)

---

<details>
<summary><strong>Mục lục cấp cao</strong></summary>

| Phần | Nội dung |
|---:|---|
| 0–3 | Quy tắc đọc, baseline repository, capability và host/profile |
| 4–6 | Quy ước OKX v5, rate limit và canonical event envelope |
| 7 | Toàn bộ Market Data REST, endpoint-by-endpoint |
| 8 | Toàn bộ Public Data/Status REST, endpoint-by-endpoint |
| 9–10 | WebSocket protocol, public/business service và từng channel |
| 11–12 | Order-book state machine và SBE |
| 13–15 | Instrument registry, units, pagination và reconciliation |
| 16–18 | Module layout, internal REST và Redis contracts |
| 19–25 | Reliability, recovery, health, persistence, observability, security và tests |
| 26–27 | Roadmap và quy trình dành cho implementation agent |
| 28–30 | Endpoint inventory, bar matrix và end-to-end flows |
| 31–33 | Changelog watchlist, Definition of Done và references |

</details>

## 0. Cách đọc tài liệu này

Tài liệu này là **đặc tả triển khai**, không chỉ là danh sách endpoint. Agent triển khai phải tuân theo các từ khóa chuẩn sau:

- **MUST / PHẢI**: yêu cầu bắt buộc để dữ liệu đúng hoặc hệ thống an toàn.
- **MUST NOT / KHÔNG ĐƯỢC**: hành vi bị cấm.
- **SHOULD / NÊN**: khuyến nghị mạnh, chỉ bỏ qua khi có lý do kiến trúc được ghi lại.
- **MAY / CÓ THỂ**: lựa chọn tùy nhu cầu.
- **PROFILE-DEPENDENT**: endpoint, hostname, field hoặc quyền truy cập phụ thuộc pháp nhân/khu vực/tier của tài khoản OKX; phải feature-gate và kiểm tra capability.

### 0.1 Quyền sở hữu tài liệu và thứ tự ưu tiên

- [`DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md`](../DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md) sở hữu thứ tự bảy phase, trạng thái, evidence, rollback và technical debt.
- [`quant-data-layer-fund-grade-upgrade-architecture.md`](quant-data-layer-fund-grade-upgrade-architecture.md) sở hữu contract canonical đa venue, durable backbone, V1/V2 boundary, Python/Rust boundary và target architecture dài hạn.
- Tài liệu này sở hữu semantics/provider profile của **OKX public market data**: REST/WS, cursor, rate limit, instrument lifecycle, sequence, units, capability và fixture bắt buộc.
- Guide OKX của Trading System sở hữu private execution/account/order integration. `data_layer` không đưa API key giao dịch hoặc private order flow vào adapter market data này.
- Roadmap `P0-P4` ở Phần 26 là workstream nội bộ của OKX. Nó phải được thực hiện theo mapping vào bảy phase chương trình tại [Phần 26.1](#okx-program-phase-map), không phải một release plan song song.

Nếu có xung đột, contract và migration invariant của kiến trúc nền được ưu tiên; chi tiết wire semantics của OKX trong tài liệu này được ưu tiên đối với adapter OKX. Mọi thay đổi public V2 cần cập nhật cả hai tài liệu và tracker trước khi code.

Mục tiêu cuối cùng là:

1. `data_layer` mở và quản lý kết nối OKX tập trung.
2. Alpha, trading, portfolio, risk và execution **không kết nối trực tiếp OKX**.
3. Live data đi qua Redis Pub/Sub hoặc transport nội bộ tương đương.
4. REST nội bộ dùng cho warmup, backfill, latest-state recovery, diagnostics và capability inspection.
5. Mọi payload downstream có contract versioned, có source timestamp, receive timestamp, market/instrument identity và unit rõ ràng.
6. OKX có thể là source chính hoặc source fallback tùy risk policy, nhưng **source role không được suy diễn từ việc endpoint đang hoạt động**.

---

## 1. Baseline hiện tại của repository và vấn đề phải sửa

Repository hiện tại đã định nghĩa `data_layer` là system-of-record gateway cho market data; downstream dùng Redis Pub/Sub cho live stream và REST/SDK cho warmup, recovery, diagnostics. Đây là hướng đúng và phải được giữ nguyên.

Adapter OKX hiện tại nằm tại:

```text
app/providers/okx/rest.py
```

Nó mới thực hiện một wrapper đồng bộ cho:

```http
GET /api/v5/market/candles
```

và đang có các giới hạn sau:

1. Chỉ hỗ trợ candle REST, chưa có ticker, trade, depth, instruments, mark/index/funding/open-interest.
2. Dùng `requests.get` đồng bộ trong một service chủ yếu async.
3. Hard-code `https://www.okx.com`.
4. Symbol normalization dạng `BTCUSDT -> BTC-USDT` chỉ đúng cho một phần spot; không đủ cho `SWAP`, `FUTURES`, `OPTION`, `EVENTS`, X-Perp hay instrument được rename.
5. Không có instrument registry authoritative.
6. Không có endpoint-specific rate limiter.
7. Không có stale-response guard dù OKX cảnh báo các market-data service có cache độc lập và request sau có thể trả dữ liệu cũ hơn request trước.
8. Không có WebSocket public/business supervisor.
9. Không có order-book state machine.
10. Không có typed canonical event envelope.
11. `start_time -> after` và `end_time -> before` đang dễ tạo window sai.

### 1.1 Lỗi pagination cần sửa ngay

Trong OKX API v5:

- `after=<cursor>`: lấy bản ghi **cũ hơn** cursor.
- `before=<cursor>`: lấy bản ghi **mới hơn** cursor.

Do đó code hiện tại:

```python
if end_time is not None:
    params["before"] = end_time
if start_time is not None:
    params["after"] = start_time
```

**không tương đương** với bộ lọc chuẩn `start <= ts <= end`.

Quy tắc đúng:

- Backfill từ hiện tại về quá khứ: request page đầu không cursor; page tiếp theo dùng `after=<oldest_ts_or_id_of_previous_page>`.
- Sau khi gom đủ dữ liệu, filter chính xác theo `[start_ms, end_ms]` ở client.
- Dùng `before` cho top-up/forward navigation khi thật sự cần dữ liệu mới hơn cursor; không map máy móc từ `end_time`.
- Mọi paginator phải deduplicate vì boundary/caching/retry có thể tạo overlap.

### 1.2 Chính sách compatibility

Không phá endpoint hiện có ngay. Thay vào đó:

- Giữ `fetch_candles(...)` như compatibility facade trong một release window.
- Bên trong facade, chuyển sang `OkxRestClient` async/typed mới.
- Thêm response contract `v2` cho endpoint/provider-aware.
- Chỉ deprecate contract cũ sau khi consumer inventory xác nhận không còn phụ thuộc.

---

## 2. Phạm vi endpoint và phân lớp capability

Không phải mọi endpoint xuất hiện trong một SDK đều khả dụng cho mọi pháp nhân OKX. Hệ thống PHẢI phân endpoint thành bốn lớp.

### 2.1 Lớp A — Core execution/reference market data

Đây là baseline phải triển khai trước:

- Instruments registry.
- Ticker và BBO.
- Trades gộp và từng trade.
- Order book snapshot/incremental.
- Candles current/history.
- Mark price, index price.
- Funding rate, open interest.
- Price limit.
- System/status.

### 2.2 Lớp B — Derivatives/options enrichment

Triển khai sau core, nhưng contract phải được thiết kế từ đầu:

- Delivery/exercise history.
- Estimated settlement/delivery price.
- Position tiers.
- Security/insurance fund.
- Index components.
- Option summary, option trades, tick bands.
- Liquidation samples và ADL warnings.

### 2.3 Lớp C — Region/account/tier-dependent

Feature-gate và capability probe:

- 24h platform volume.
- Exchange rate.
- Economic calendar.
- Historical bulk market-data download.
- Một số option/event-contract endpoint.
- SBE channels.
- Deep tick-by-tick books yêu cầu VIP tier.

### 2.4 Lớp D — Không thuộc baseline hoặc đã deprecated/offline

- Private account/trading endpoint dù path có chữ `public`.
- Rubik/statistical analytics nếu chưa có consumer rõ ràng.
- Block-trading market data nếu hệ thống chưa ingest block/RFQ domain.
- `open-oracle`: đã offline; KHÔNG ĐƯỢC gọi.
- `books-elp`: đang được thay thế bởi `books-rpi`; chỉ giữ decoder compatibility có thời hạn, không dùng làm target mới.
- `books-lite`: không implement chỉ vì SDK có constant; cần xác nhận trong docs của entity đang dùng.

---

## 3. Hostname, region profile và môi trường

### 3.1 Không hard-code entity host

OKX có tài liệu và endpoint host khác nhau theo pháp nhân/khu vực. Agent PHẢI dùng cấu hình:

```env
OKX_ENABLED=true
OKX_REGION_PROFILE=global
OKX_REST_BASE_URL=https://www.okx.com
OKX_WS_PUBLIC_URL=wss://ws.okx.com:8443/ws/v5/public
OKX_WS_BUSINESS_URL=wss://ws.okx.com:8443/ws/v5/business
OKX_DEMO=false
```

Global có thể dùng domain OpenAPI khác khi được OKX hỗ trợ, ví dụ:

```env
OKX_REST_BASE_URL=https://openapi.okx.com
```

Nhưng lựa chọn domain phải là config/deployment decision, không rải hard-code trong client.

Ví dụ profile EEA có thể là:

```env
OKX_REGION_PROFILE=eea
OKX_REST_BASE_URL=https://eea.okx.com
OKX_WS_PUBLIC_URL=wss://wseea.okx.com:8443/ws/v5/public
OKX_WS_BUSINESS_URL=wss://wseea.okx.com:8443/ws/v5/business
```

### 3.2 Demo trading

Demo profile dùng hostname WebSocket demo tương ứng và REST request cần:

```http
x-simulated-trading: 1
```

Không suy diễn demo URL bằng string replacement. Khai báo đầy đủ:

```env
OKX_DEMO=true
OKX_REST_BASE_URL=...
OKX_WS_PUBLIC_URL=...
OKX_WS_BUSINESS_URL=...
```

### 3.3 Capability manifest

Mỗi deployment nên tạo manifest runtime:

```yaml
provider: okx
profile: global
verified_at: 2026-08-13T00:00:00Z
rest_base_url: https://www.okx.com
ws:
  public: true
  business: true
  sbe: false
capabilities:
  market.tickers: true
  market.books: true
  market.books_rpi: true
  market.books_full: true
  market.candles: true
  market.history_candles: true
  market.trades: true
  market.history_trades: true
  public.instruments: true
  public.option_summary: probe
  public.tick_bands: probe
  public.economic_calendar: false
  public.market_data_history: probe
  sbe.bbo_tbt: false
  sbe.trades: false
  sbe.books_l2_tbt: false
```

`probe` có nghĩa là deployment startup/diagnostics kiểm tra endpoint bằng request an toàn và ghi kết quả; không coi SDK constant là bằng chứng availability.

---

## 4. Quy ước chung của OKX API v5

### 4.1 REST response envelope

Phần lớn REST response có dạng:

```json
{
  "code": "0",
  "msg": "",
  "data": []
}
```

Client PHẢI kiểm tra cả hai lớp:

1. HTTP status.
2. Business `code` trong JSON.

HTTP `200` không đảm bảo thành công. Success condition chuẩn:

```python
response.status_code == 200 and payload.get("code") == "0"
```

Mọi error object nội bộ nên giữ:

```json
{
  "provider": "okx",
  "transport": "rest",
  "endpoint": "/api/v5/market/candles",
  "http_status": 200,
  "code": "50011",
  "message": "...",
  "retryable": true,
  "request_id": null,
  "params_redacted": {}
}
```

### 4.2 Kiểu dữ liệu số

OKX trả hầu hết price, size, volume, rate dưới dạng string.

PHẢI:

- Preserve raw string trong provider model hoặc parse bằng `Decimal`.
- Serialize canonical decimal dưới dạng string.
- Không dùng binary float cho price/size/rate.
- Không tự biến `""` thành `0`; `""` thường có nghĩa là không áp dụng/chưa có dữ liệu.

Ví dụ:

```python
from decimal import Decimal

price = Decimal(row[0])
size = Decimal(row[1])
```

### 4.3 Timestamp

- Timestamp market data thường là Unix epoch milliseconds dưới dạng string.
- Canonical model dùng `int` milliseconds.
- Có thể thêm ISO-8601 UTC cho diagnostics, nhưng `source_ts_ms` là field authoritative.
- Phân biệt:
  - `source_ts_ms`: timestamp từ OKX.
  - `received_ts_ms`: lúc adapter nhận frame/response.
  - `normalized_ts_ms`: lúc parse xong.
  - `published_ts_ms`: lúc publish Redis.

### 4.4 Market-data cache không monotonic giữa REST calls

OKX có nhiều market-data service với cache độc lập; request sau có thể trả snapshot có `ts` nhỏ hơn request trước.

Vì vậy latest-state writer PHẢI:

```text
accept(new) khi:
  new.source_ts > current.source_ts
hoặc:
  new.source_ts == current.source_ts và new.received_ts > current.received_ts
```

Đối với cùng timestamp nhưng payload cập nhật hợp lệ, quy tắc per-stream có thể khác; phải định nghĩa rõ. Không được overwrite latest cache bằng snapshot cũ chỉ vì HTTP request vừa thành công.

### 4.5 Instrument type

Canonical enum phải giữ đúng OKX:

```text
SPOT
MARGIN
SWAP
FUTURES
OPTION
EVENTS
```

Không gộp `SWAP` và `FUTURES` thành một market trong provider layer. Downstream có thể group thành derivatives bằng field derived.

### 4.6 Instrument identity

Ví dụ:

```text
BTC-USDT                 SPOT
BTC-USDT-SWAP            linear SWAP
BTC-USD-SWAP             inverse/coin-margined SWAP
BTC-USD-260925            FUTURES
BTC-USD-260925-100000-C   OPTION
```

Canonical key đề xuất:

```text
okx:{inst_type_lower}:{instId}
```

Ví dụ:

```text
okx:spot:BTC-USDT
okx:swap:BTC-USDT-SWAP
okx:futures:BTC-USD-260925
okx:option:BTC-USD-260925-100000-C
```

### 4.7 `instFamily` thay cho `uly`

- Với WebSocket derivatives/options, dùng `instFamily`.
- Không phát triển code mới dựa trên `uly` nếu endpoint đã hỗ trợ `instFamily`.
- Khi cả hai xuất hiện, contract nội bộ ưu tiên `instFamily`.

### 4.8 Event contracts

Đối với `EVENTS`:

- Market Data module có thể chỉ trả dữ liệu phía YES.
- NO side là dữ liệu derived, không phải raw feed từ endpoint.
- Volume có thể mang unit contract.
- Derived NO price phải có `is_derived=true`, `derivation_method` và không được giả làm raw OKX tick.

---

## 5. Rate limiting, retry và concurrency

### 5.1 Rate limit theo endpoint bucket

Không dùng một limiter chung cố định cho toàn OKX. Tạo bucket theo:

```text
transport + endpoint/channel + rule dimension
```

Ví dụ:

```text
rest:/api/v5/market/tickers:ip
rest:/api/v5/market/books:ip
rest:/api/v5/public/instruments:ip+instType
rest:/api/v5/public/mark-price:ip+instId
ws:connection:operation
```

### 5.2 Token bucket an toàn

Cấu hình limiter nên giữ headroom 5–15% thay vì chạy sát trần. Ví dụ endpoint `40 requests / 2 seconds`:

```yaml
capacity: 36
period_seconds: 2
```

Backfill batch phải có global concurrency cap để không tạo burst đồng thời ở nhiều bucket.

### 5.3 Retry matrix

| Lỗi | Retry | Quy tắc |
|---|---:|---|
| DNS/connect timeout | Có | exponential backoff + jitter |
| Read timeout | Có | retry có giới hạn; request idempotent |
| HTTP 429 | Có | tôn trọng header nếu có; giảm rate |
| OKX `50011` | Có | rate-limit backoff, metric riêng |
| HTTP 5xx | Có | capped exponential backoff |
| JSON parse/schema error | Có điều kiện | 1 retry, sau đó quarantine payload |
| Invalid parameter | Không | lỗi code/config |
| Instrument không tồn tại | Không mù quáng | refresh registry rồi đánh giá lại |
| Tier/channel denied `64003` | Không | disable capability, alert |
| Channel conflict `64004` | Không | sửa subscription planner |

Suggested defaults:

```env
OKX_REST_CONNECT_TIMEOUT_S=3
OKX_REST_READ_TIMEOUT_S=10
OKX_REST_MAX_ATTEMPTS=4
OKX_REST_BACKOFF_BASE_S=0.25
OKX_REST_BACKOFF_MAX_S=8
OKX_REST_MAX_CONCURRENCY=16
```

### 5.4 Circuit breaker

Mỗi capability có breaker riêng. Không để lỗi option endpoint làm ngắt ticker/WS core.

State:

```text
closed -> open -> half_open -> closed
```

Readiness chỉ fail khi capability bắt buộc của deployment fail; optional capability chỉ làm `degraded`.

---

## 6. Canonical event envelope cho `data_layer`

Mọi live event mới nên dùng envelope versioned:

```json
{
  "schema_version": 1,
  "event_id": "okx:public:trades:BTC-USDT:1730000000000:123456",
  "provider": "okx",
  "provider_profile": "global",
  "transport": "ws-json",
  "connection_id": "a4d3ae55",
  "stream": "trade",
  "tick_type": "trade_agg",
  "market": "SPOT",
  "instrument_id": "BTC-USDT",
  "instrument_key": "okx:spot:BTC-USDT",
  "instrument_family": null,
  "source_ts_ms": 1730000000000,
  "received_ts_ms": 1730000000012,
  "normalized_ts_ms": 1730000000013,
  "published_ts_ms": 1730000000014,
  "sequence_id": 817263,
  "is_snapshot": false,
  "is_replay": false,
  "is_derived": false,
  "source_role": "reference",
  "payload": {},
  "raw": null
}
```

### 6.1 Tick taxonomy chuẩn

| `tick_type` | Nguồn OKX | Ý nghĩa |
|---|---|---|
| `ticker` | REST/WS `tickers` | snapshot last/BBO/24h |
| `bbo` | `bbo-tbt` | best bid/ask snapshot |
| `trade_agg` | WS `trades` | một message có thể gộp nhiều matches |
| `trade_atomic` | WS `trades-all` | một trade/fill mỗi update |
| `book_snapshot` | WS books action snapshot | state đầy đủ ban đầu |
| `book_delta` | WS books action update | delta incremental |
| `book_rpi_snapshot` | `books-rpi` snapshot | consolidated organic + RPI |
| `book_rpi_delta` | `books-rpi` update | delta consolidated |
| `candle_update` | candle `confirm=0` | candle chưa đóng |
| `candle_close` | candle `confirm=1` | candle hoàn tất |
| `mark_price` | mark-price | mark price |
| `index_price` | index-tickers | index price |
| `funding_rate` | funding-rate | funding current/next |
| `open_interest` | open-interest | OI snapshot/change |
| `price_limit` | price-limit | buy/sell limit band |
| `instrument_update` | instruments WS | listing/state/spec change |
| `settlement_estimate` | estimated-price | estimated delivery/exercise |
| `liquidation_sample` | liquidation-orders | sample liquidation, không phải tổng thị trường |
| `adl_warning` | adl-warning | ADL warning event |
| `maintenance_status` | status | maintenance/service status |
| `economic_event` | optional calendar | macro event |
| `sbe_trade` | SBE | binary trade |
| `sbe_bbo` | SBE | binary BBO |
| `sbe_book_delta` | SBE | binary book delta |

### 6.2 Raw payload policy

- Production default: `raw=null` để giảm memory/Redis bandwidth.
- Debug/capture mode: raw payload có TTL ngắn hoặc ghi ra object storage/quarantine, không nhúng mọi raw frame vào Redis public stream.
- Khi schema parse fail, lưu payload redacted kèm hash và connection metadata.

---
## 7. REST Market Data — đặc tả endpoint-by-endpoint

> Nhóm `/api/v5/market/*` dưới đây là public market data. Không cần API key ở baseline. Tuy nhiên, client vẫn PHẢI kiểm tra `code == "0"`; HTTP `200` không đồng nghĩa payload thành công.

### 7.0 Contract chung cho REST market data

Mỗi method provider nên trả một object typed thay vì trả thẳng dictionary của OKX:

```python
@dataclass(frozen=True, slots=True)
class OkxRestPage(Generic[T]):
    items: tuple[T, ...]
    provider_code: str
    provider_message: str
    request_started_ns: int
    response_received_ns: int
    endpoint: str
    query: Mapping[str, str]
    raw_hash: str | None
```

Quy tắc bắt buộc:

- Query parameter rỗng KHÔNG ĐƯỢC gửi dưới dạng `""` trừ khi docs yêu cầu rõ.
- Mọi timestamp query là chuỗi Unix milliseconds.
- Mọi numeric response phải parse bằng `Decimal` hoặc giữ raw string.
- Mỗi request gắn `request_id`, endpoint bucket và profile.
- Validate `code`, `msg`, kiểu `data`; schema mismatch phải vào quarantine và metric.
- Retry chỉ cho lỗi transient; không retry vô hạn với lỗi validation/capability.
- Không dùng thứ tự response của ticker/snapshot để kết luận event-time monotonic.

---

### 7.1 `GET /api/v5/market/tickers`

**Mục đích:** lấy snapshot ticker của toàn bộ instrument thuộc một `instType`, có thể thu hẹp bằng `instFamily`.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 20 request / 2 giây / IP |
| Freshness | Snapshot cache; không bảo đảm monotonic giữa hai request |
| Dùng cho | Bootstrap latest ticker, health check theo market type, universe scan |
| Không dùng cho | Reconstruct tick-by-tick, tính latency execution, order-book state |

#### Request

```http
GET /api/v5/market/tickers?instType=SWAP
GET /api/v5/market/tickers?instType=OPTION&instFamily=BTC-USD
```

| Parameter | Required | Giá trị / quy tắc |
|---|---:|---|
| `instType` | Có | `SPOT`, `SWAP`, `FUTURES`, `OPTION`, `EVENTS` |
| `instFamily` | Không | Áp dụng cho `FUTURES`, `SWAP`, `OPTION`; NÊN gửi khi consumer chỉ cần một family |

#### Response fields cần parse

| Field | Canonical field | Semantics |
|---|---|---|
| `instType` | `market` | Loại instrument |
| `instId` | `instrument_id` | ID nguyên bản OKX |
| `last` | `last_price` | Giá giao dịch gần nhất |
| `lastSz` | `last_size_raw` | Size của giao dịch gần nhất; unit theo instrument |
| `askPx`, `askSz` | `ask_price`, `ask_size_raw` | Best ask snapshot |
| `bidPx`, `bidSz` | `bid_price`, `bid_size_raw` | Best bid snapshot |
| `open24h` | `open_24h` | Giá 24 giờ trước theo cửa sổ rolling |
| `high24h`, `low24h` | `high_24h`, `low_24h` | High/low 24 giờ |
| `volCcy24h` | `volume_ccy_24h_raw` | Semantics khác theo instrument |
| `vol24h` | `volume_24h_raw` | Semantics khác theo instrument |
| `sodUtc0`, `sodUtc8` | `open_utc0`, `open_utc8` | Giá mở ngày theo timezone tương ứng |
| `ts` | `source_ts_ms` | Thời điểm tạo ticker |

#### Unit chính xác

- `SPOT`/`MARGIN`: `vol24h` là lượng **base currency**; `volCcy24h` là lượng **quote currency**.
- Derivatives: `vol24h` là số **contracts**; `volCcy24h` là volume theo currency được OKX mô tả cho contract, thường là base currency.
- KHÔNG ĐƯỢC map cả hai về một field `volume` không có unit.

#### Edge cases

- Trong pre-open/call auction, best ask có thể thấp hơn best bid. Không reject payload chỉ vì book crossed.
- String rỗng có thể xuất hiện khi instrument chưa có trade/BBO.
- Một ticker có `ts` nhỏ hơn snapshot đã nhận trước đó do cache độc lập. Latest-store phải dùng policy `(source_ts, received_ts)`; không overwrite blindly.

#### Normalized event

```json
{
  "tick_type": "ticker",
  "market": "SWAP",
  "instrument_id": "BTC-USDT-SWAP",
  "source_ts_ms": 1730000000000,
  "payload": {
    "last_price": "68420.1",
    "last_size_raw": "12",
    "bid_price": "68420.0",
    "bid_size_raw": "91",
    "ask_price": "68420.1",
    "ask_size_raw": "48",
    "volume_24h_raw": "1200345",
    "volume_24h_unit": "contract",
    "volume_ccy_24h_raw": "35021.7",
    "volume_ccy_24h_unit": "BTC"
  }
}
```

#### Provider method

```python
async def fetch_tickers(
    self,
    *,
    inst_type: OkxInstrumentType,
    inst_family: str | None = None,
) -> OkxRestPage[OkxTicker]: ...
```

---

### 7.2 `GET /api/v5/market/ticker`

**Mục đích:** lấy ticker snapshot cho đúng một `instId`.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 20 request / 2 giây / IP |
| Required | `instId` |
| Dùng cho | Targeted warmup, REST fallback, diagnostics |

```http
GET /api/v5/market/ticker?instId=BTC-USDT
```

Response fields và unit giống mục `tickers`.

**Implementation rule:** Không poll endpoint này cho hàng nghìn instrument. Live ticker phải dùng WS; bootstrap theo universe nên batch bằng `/tickers` theo `instType`/`instFamily`.

```python
async def fetch_ticker(self, *, inst_id: str) -> OkxTicker: ...
```

---

### 7.3 `GET /api/v5/market/books`

**Mục đích:** lấy order-book snapshot thông thường.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 40 request / 2 giây / IP |
| Server cache | Khoảng 50 ms |
| Required | `instId` |
| Optional | `sz`, default `1`, tối đa `400` levels mỗi side |
| Dùng cho | REST snapshot/diagnostics/warmup UI |
| Không dùng cho | Seed sequence state của incremental WS |

```http
GET /api/v5/market/books?instId=BTC-USDT-SWAP&sz=400
```

#### Response shape

```json
{
  "code": "0",
  "msg": "",
  "data": [{
    "asks": [["68421.0", "42", "0", "7"]],
    "bids": [["68420.9", "31", "0", "5"]],
    "ts": "1730000000000",
    "seqId": 123456789
  }]
}
```

Mỗi level:

```text
[price, quantity, deprecated_field, order_count]
```

- Index `0`: price.
- Index `1`: aggregate quantity.
- Index `2`: hiện giữ giá trị `"0"`; giữ parser positional nhưng không dùng business logic.
- Index `3`: số order được aggregate tại level.

#### Unit

- `SPOT`/`MARGIN`: quantity là base currency.
- Derivatives: quantity là contracts.

#### Quy tắc normalize

```python
@dataclass(frozen=True, slots=True)
class BookLevel:
    price: Decimal
    quantity_raw: Decimal
    order_count: int
    quantity_unit: Literal["base", "contract"]
```

- Sort asks tăng dần, bids giảm dần sau khi parse để bảo vệ consumer trước schema/order anomalies.
- Validate price > 0, quantity >= 0, order_count >= 0.
- Không reject crossed book trong pre-open.
- Gắn `snapshot_source="rest"` và `sequence_bridge=false`.

**Cảnh báo quan trọng:** REST `seqId` không tạo một bridge được tài liệu đảm bảo tới snapshot/delta WS đã nhận trên một connection. Khi build WS book, PHẢI chờ `action=snapshot` từ chính subscription đó.

---

### 7.4 `GET /api/v5/market/books-rpi`

**Mục đích:** lấy consolidated depth gồm organic liquidity và RPI liquidity.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 20 request / 2 giây / IP |
| Server refresh | Khoảng 200 ms |
| `sz` | Default `1`, tối đa `400` |
| Migration | Target mới; thay thế tên `books-elp` |

```http
GET /api/v5/market/books-rpi?instId=BTC-USDT&sz=400
```

Mỗi level:

```text
[price, totalQty, nonRpiQty, orderCount]
```

Canonical fields:

```json
{
  "price": "68421.0",
  "total_quantity_raw": "12.5",
  "non_rpi_quantity_raw": "10.0",
  "rpi_quantity_raw": "2.5",
  "order_count": 4
}
```

Công thức:

```text
rpiQty = max(totalQty - nonRpiQty, 0)
```

#### Semantics cần giữ

- Feed public là consolidated view; khả năng một taker thực tế execute RPI liquidity phụ thuộc quyền/taker setting, không thể suy ra chỉ từ feed.
- Không biến `totalQty` thành guaranteed executable size cho execution simulator.
- Lưu cả `totalQty` và `nonRpiQty`; derived `rpiQty` phải đánh dấu `is_derived=true`.
- WS `books-rpi` không dùng checksum; sequencing bằng `seqId`/`prevSeqId`.
- Không phát triển mới trên `books-elp`; chỉ giữ compatibility decoder tới khi migration hoàn tất.

---

### 7.5 `GET /api/v5/market/books-full`

**Mục đích:** snapshot full order book sâu hơn REST books thông thường.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 10 request / 2 giây / IP |
| Refresh | Xấp xỉ 1 giây |
| `sz` | Default `1`, tối đa `5000` levels mỗi side |
| Use case | Research, diagnostics, periodic deep snapshot, cold archive |

```http
GET /api/v5/market/books-full?instId=BTC-USDT&sz=5000
```

Level shape:

```text
[price, quantity, orderCount]
```

Không giả định shape 4 phần tử giống `/books`.

**Operational rule:** Đây là payload lớn; PHẢI có semaphore riêng, timeout riêng, compression HTTP và giới hạn concurrency. Không poll ở cadence cao. Không dùng để repair từng WS gap; repair WS bằng resubscribe/snapshot, còn full REST chỉ là diagnostic/reference.

---

### 7.6 `GET /api/v5/market/candles`

**Mục đích:** latest candlesticks; tối đa 1.440 data points gần nhất theo bar.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 40 request / 2 giây / IP |
| Required | `instId` |
| `limit` | Default `100`, tối đa `300` |
| Pagination | `after` cũ hơn; `before` mới hơn |
| Optional | `bar`, `adjust` ở profile/instrument hỗ trợ |

```http
GET /api/v5/market/candles?instId=BTC-USDT&bar=1m&limit=300
```

#### Supported bars baseline

| Nhóm | `bar` values |
|---|---|
| Intraday | `1m`, `3m`, `5m`, `15m`, `30m`, `1H`, `2H`, `4H` |
| Calendar UTC+8 default | `6H`, `12H`, `1D`, `2D`, `3D`, `1W`, `1M`, `3M` |
| Calendar UTC+0 | `6Hutc`, `12Hutc`, `1Dutc`, `2Dutc`, `3Dutc`, `1Wutc`, `1Mutc`, `3Mutc` |

Agent KHÔNG ĐƯỢC lowercase toàn bộ bar: `1M` là month, `1m` là minute.

#### Row schema

```text
[ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
```

| Index | Canonical | Semantics |
|---:|---|---|
| 0 | `open_ts_ms` | Start timestamp của candle |
| 1..4 | `open`, `high`, `low`, `close` | OHLC |
| 5 | `volume_raw` | Spot: base; derivatives: contracts |
| 6 | `volume_ccy_raw` | Spot: quote; derivatives: base currency |
| 7 | `volume_quote_raw` | Quote-currency volume |
| 8 | `confirm` | `0` ongoing, `1` completed |

#### Candle state rules

- `(instId, bar, ts)` là primary key.
- `confirm=0`: upsert/revise được.
- `confirm=1`: publish `candle_close`; data warehouse có thể freeze, nhưng vẫn nên giữ correction pathway có audit nếu provider phát correction.
- Không phát `candle_close` nhiều lần cho cùng version nếu payload không đổi.
- Sort ascending trước khi publish/backfill dù OKX thường trả newest-first.
- Validate `low <= min(open,close) <= max(open,close) <= high`; anomaly không nên silently drop — quarantine và metric.

#### Pagination đúng

```python
async def backfill_candles(inst_id, bar, start_ms, end_ms):
    cursor_after = None
    seen = set()
    out = []

    while True:
        page = await get_candles(
            instId=inst_id,
            bar=bar,
            after=cursor_after,
            limit="300",
        )
        if not page:
            break

        for row in page:
            ts = int(row[0])
            key = (inst_id, bar, ts)
            if key not in seen and start_ms <= ts <= end_ms:
                seen.add(key)
                out.append(row)

        oldest_ts = min(int(row[0]) for row in page)
        if oldest_ts <= start_ms or str(oldest_ts) == cursor_after:
            break
        cursor_after = str(oldest_ts)

    return sorted(out, key=lambda row: int(row[0]))
```

Trong production cần thêm page guard, no-progress guard, retry budget và exact boundary dedup.

#### `adjust`

Một số equity perpetual/profile hỗ trợ adjustment. Không expose generic boolean. Dùng enum rõ:

```python
adjust: Literal["forward"] | None
```

Capability phải được kiểm tra theo profile; response phải gắn `price_adjustment` để tránh trộn adjusted/unadjusted series.

---

### 7.7 `GET /api/v5/market/history-candles`

**Mục đích:** candlestick lịch sử từ các năm gần đây.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 20 request / 2 giây / IP |
| `limit` | Tối đa `300` |
| Pagination | `after` cũ hơn; `before` mới hơn |
| `1s` | Chỉ dữ liệu khoảng 3 tháng gần nhất; không áp dụng cho `OPTION` |

```http
GET /api/v5/market/history-candles?instId=BTC-USDT-SWAP&bar=1m&limit=300
```

Row schema và volume semantics giống `/candles`.

#### Route selection

```text
requested window entirely inside latest-1440 coverage
    -> /market/candles
otherwise
    -> /market/history-candles
optionally top-up newest edge with /market/candles
```

Client KHÔNG ĐƯỢC assume `/history-candles` có toàn bộ lịch sử từ listing. Lưu coverage metadata:

```json
{
  "requested_start_ms": 0,
  "requested_end_ms": 0,
  "observed_min_ts_ms": 0,
  "observed_max_ts_ms": 0,
  "complete_left": false,
  "complete_right": true,
  "provider_retention_note": "recent years"
}
```

---

### 7.8 `GET /api/v5/market/trades`

**Mục đích:** recent public trades, tối đa `500` records.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 100 request / 2 giây / IP |
| Required | `instId` |
| `limit` | Tối đa `500` |
| Use case | Recent trade warmup, diagnostics, REST recovery nhỏ |

```http
GET /api/v5/market/trades?instId=BTC-USDT&limit=500
```

Response fields:

| OKX | Canonical | Ghi chú |
|---|---|---|
| `instId` | `instrument_id` | Raw OKX ID |
| `tradeId` | `trade_id` | String; không cast int nếu không cần |
| `px` | `price` | Decimal |
| `sz` | `quantity_raw` | Spot base; derivatives contracts |
| `side` | `taker_side` | `buy`/`sell`, là phía taker |
| `source` | `trade_source` | `0` normal; `1` RPI/ELP source |
| `ts` | `source_ts_ms` | Trade timestamp |

**Không gắn maker side trực tiếp:**

```text
taker_side=buy  => maker side có thể suy ra sell
```

nhưng derived field phải đánh dấu rõ; raw side luôn là taker side.

**Dedup key:** `(provider, instId, tradeId)` cho REST trade record, có fallback composite khi profile trả ID bất thường.

---

### 7.9 `GET /api/v5/market/history-trades`

**Mục đích:** trade history khoảng 3 tháng gần nhất.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 20 request / 2 giây / IP |
| Required | `instId` |
| `limit` | Tối đa `100` |
| `type` | `1` cursor theo `tradeId` (default); `2` cursor theo timestamp |
| Pagination | `after` cũ hơn; `before` mới hơn |

```http
GET /api/v5/market/history-trades?instId=BTC-USDT&type=1&after=123456&limit=100
```

#### Cursor rules

- `type=1`: cursor là `tradeId`; hỗ trợ `after` và `before` theo tài liệu.
- `type=2`: cursor là millisecond timestamp; `before` không được dùng theo contract hiện hành.
- Không trộn cursor type giữa các page.
- Persist checkpoint gồm cả `type`, cursor và last observed timestamp.

```json
{
  "endpoint": "/api/v5/market/history-trades",
  "instId": "BTC-USDT",
  "cursor_type": "tradeId",
  "after": "123456",
  "oldest_source_ts_ms": 1730000000000
}
```

#### Backfill strategy

- Với archival exact trade ID: ưu tiên `type=1`.
- Với time-window discovery: có thể dùng `type=2`, nhưng filter/dedup theo event fields.
- Dừng khi oldest timestamp vượt qua `start_ms`, no-progress hoặc hết data.
- Do retention hữu hạn, trả `coverage_status=partial` thay vì giả vờ đủ.

---

### 7.10 Endpoint market-data tùy profile / không thuộc core

Các endpoint sau chỉ triển khai khi capability probe trên đúng hostname/entity xác nhận:

| Endpoint | Mục đích | Baseline policy |
|---|---|---|
| `GET /api/v5/market/platform-24-volume` | Platform rolling 24h volume (`volCny`, `volUsd`, `ts`) | Optional; rate limit thấp, không poll thường xuyên |
| `GET /api/v5/market/exchange-rate` | USD/CNY reference, dạng average theo window của OKX | Optional; không dùng làm FX execution price |
| `GET /api/v5/market/option/instrument-family-trades` | Option trades theo family | Options profile only |
| `GET /api/v5/public/option-trades` | Public option trades | Options profile only |
| Block ticker/trade endpoints | Block/RFQ market domain | Tách schema/stream; không trộn lit-market trades |
| Bulk historical market-data endpoint | Download/metadata lịch sử | Feature-gate, exact docs/version required |

#### Endpoint bị cấm/deprecated

- `GET /api/v5/market/open-oracle`: offline; KHÔNG gọi.
- `books-elp`: tên legacy đang sunset; target mới là `books-rpi`.
- `books-lite`: SDK constant không phải bằng chứng endpoint khả dụng; chỉ triển khai sau docs/profile smoke test.

---

## 8. REST Public Data — đặc tả endpoint-by-endpoint

### 8.1 `GET /api/v5/public/instruments` — registry authoritative

Đây là endpoint quan trọng nhất cho symbol/specification. Agent PHẢI bootstrap registry từ endpoint này trước khi mở live stream cho derivatives/options/events.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 20 request / 2 giây / IP + instrument type |
| Required | `instType` |
| Conditional | `seriesId` cho `EVENTS`; `instFamily` cho `OPTION` |
| Optional | `instId`, `instFamily` tùy type |

```http
GET /api/v5/public/instruments?instType=SPOT
GET /api/v5/public/instruments?instType=SWAP
GET /api/v5/public/instruments?instType=OPTION&instFamily=BTC-USD
GET /api/v5/public/instruments?instType=EVENTS&seriesId=BTC-ABOVE-DAILY
```

#### Không suy diễn ID derivatives

Ví dụ hình thức ID thường gặp:

```text
SPOT      BTC-USDT
SWAP      BTC-USDT-SWAP
FUTURES   BTC-USDT-260925
OPTION    BTC-USD-260925-70000-C
EVENTS    profile-defined; lấy trực tiếp registry
```

Đây chỉ là mô tả hình thức, KHÔNG phải generator contract. `instId` hợp lệ phải đến từ registry.

#### Field groups cần lưu

**Identity và grouping**

- `instType`, `instId`, `seriesId`.
- `uly`, `instFamily`, `groupId`.
- `baseCcy`, `quoteCcy`, `settleCcy`.
- `instCategory` và các category field profile-specific.

**Contract specification**

- `ctVal`, `ctMult`, `ctValCcy`, `ctType`.
- `optType`, `stk`.
- `tickSz`, `lotSz`, `minSz`.
- Các max order-size field hiện hành.
- `lever`, rule/limit-related fields.

**Lifecycle**

- `listTime`, `contTdSwTime`, `preMktSwTime`, `expTime`.
- `openType`, `state`, `ruleType`.
- `futureSettlement`.
- `alias` chỉ compatibility; không dùng làm maturity source mới.

**Routing/compatibility**

- `tradeQuoteCcyList`.
- `instIdCode` cho SBE.
- `rpiMinLevel`, `rpiMinPxBand` nếu profile trả.
- `upcChg` hoặc field thông báo upcoming parameter changes.
- `initPxLmtPct`, `floatPxLmtPct`, `maxPxLmtPct` và field mới khác phải preserved trong raw/spec extension.

#### State và rule type

Registry phải hỗ trợ ít nhất:

```text
state:
  live
  suspend
  rebase
  post_only
  preopen
  test
  settling
  expired     # có thể xuất hiện trong WS lifecycle/profile

ruleType:
  normal
  pre_market
  rebase_contract
  xperp       # profile/product-dependent
```

Không hard-fail khi OKX thêm enum mới. Parse theo chiến lược:

```python
known_state: OkxInstrumentState | None
raw_state: str
is_unknown_state: bool
```

Unknown enum khiến instrument `not_ready_for_trading`, nhưng vẫn được ingest/quarantine và alert.

#### Tick size

- `tickSz` là tick size thông thường.
- Với `OPTION`/`EVENTS`, docs có thể trả minimum tick trong các bands; exact valid tick tại một price có thể cần endpoint tick-bands.
- Order validation/execution không được chỉ dùng `tickSz` minimum nếu product áp dụng price bands.

#### Lifecycle sync

1. REST full bootstrap cho từng `instType`/family.
2. Commit registry snapshot atomically.
3. Subscribe WS `instruments` để nhận thay đổi.
4. Periodic REST reconcile để phát hiện missed WS event.
5. Không xóa ngay một instrument chỉ vì nó biến mất khỏi REST response; chuyển lifecycle qua `inactive/expired` dựa trên diff + WS + grace window.

#### Canonical instrument model

```python
@dataclass(frozen=True, slots=True)
class InstrumentSpec:
    provider: Literal["okx"]
    profile: str
    inst_type: str
    inst_id: str
    series_id: str | None
    instrument_family: str | None
    underlying: str | None
    base_ccy: str | None
    quote_ccy: str | None
    settle_ccy: str | None
    contract_value: Decimal | None
    contract_multiplier: Decimal | None
    contract_value_ccy: str | None
    contract_type: str | None
    tick_size: Decimal | None
    lot_size: Decimal | None
    min_size: Decimal | None
    list_time_ms: int | None
    expiry_time_ms: int | None
    state_raw: str
    rule_type_raw: str | None
    inst_id_code: int | None
    raw_extra: Mapping[str, Any]
```

---

### 8.2 `GET /api/v5/public/estimated-price`

**Mục đích:** estimated delivery/exercise/settlement price gần thời điểm expiry/delivery.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 10 request / 2 giây / IP |
| Required | `instId` |
| Product | `FUTURES`, `OPTION` và product settlement profile hỗ trợ |

```http
GET /api/v5/public/estimated-price?instId=BTC-USD-260925
```

Response core:

- `instType`
- `instId`
- `settlePx`
- `ts`

**Semantics:** data thường chỉ meaningful trong cửa sổ gần delivery/exercise. `settlePx=""` không phải parse error; normalize `None` với `availability_reason="outside_estimation_window"` khi có thể xác định.

Không dùng estimated price thay thế mark/index price trong valuation thông thường.

---

### 8.3 `GET /api/v5/public/delivery-exercise-history`

**Mục đích:** delivery records của Futures và exercise records của Options trong khoảng retention gần nhất, hiện khoảng 3 tháng.

| Thuộc tính | Giá trị |
|---|---|
| Rate limit | 40 request / 2 giây / IP + (`instType`, `instFamily`) |
| Required | `instType`, `instFamily` |
| Product | `FUTURES`, `OPTION` |
| Pagination | `after` cũ hơn; `before` mới hơn; `limit` tối đa `100` |

```http
GET /api/v5/public/delivery-exercise-history?instType=OPTION&instFamily=BTC-USD
```

Response thường group theo settlement timestamp, với `details` chứa instrument và delivery/exercise price. Canonical storage nên tách:

```text
settlement_batch
  provider
  inst_type
  inst_family
  settlement_ts

settlement_detail
  instrument_id
  settlement_price
  tag/type fields
```

Primary key đề xuất:

```text
(provider, instType, instFamily, settlement_ts, instId)
```

---

### 8.4 `GET /api/v5/public/funding-rate`

**Mục đích:** current/predicted funding information cho perpetual/X-Perp product.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 10 request / 2 giây / IP + instrument ID |
| Required | `instId` |

```http
GET /api/v5/public/funding-rate?instId=BTC-USDT-SWAP
```

Fields cần lưu đầy đủ:

| Field | Ý nghĩa |
|---|---|
| `instType`, `instId` | Identity |
| `method` | Funding calculation method |
| `formulaType` | Formula variant |
| `fundingRate` | Predicted/current upcoming settlement rate |
| `fundingTime` | Settlement time liên quan |
| `nextFundingTime` | Next scheduled time |
| `minFundingRate`, `maxFundingRate` | Bounds |
| `interestRate` | Interest component |
| `impactValue` | Depth-weighted quote amount |
| `settState` | `processing`/`settled` |
| `settFundingRate` | Rate đang/đã settlement theo state |
| `premium` | Premium index component |
| `ts` | Data timestamp |

#### Không hard-code funding 8 giờ

```python
interval_ms = int(nextFundingTime) - int(fundingTime)
```

OKX có thể điều chỉnh cadence xuống 6h/4h/2h/1h cho một số contract. Mọi annualization/forecast phải dùng actual interval.

#### Sign convention

Giữ raw sign và mô tả canonical:

```text
positive rate -> long pays short at settlement
negative rate -> short pays long
```

Không tính realized cashflow nếu chưa có position notional, settlement time và contract spec.

---

### 8.5 `GET /api/v5/public/funding-rate-history`

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 10 request / 2 giây / IP + instrument ID |
| Required | `instId` |
| Retention | Khoảng 3 tháng |
| Pagination | `before` mới hơn; `after` cũ hơn |
| `limit` | Default/tối đa `400` theo docs hiện hành |

```http
GET /api/v5/public/funding-rate-history?instId=BTC-USDT-SWAP&limit=400
```

Fields core:

- `instType`, `instId`.
- `fundingRate`: predicted value associated with record.
- `realizedRate`: realized/settled value khi endpoint/profile trả.
- `fundingTime`.
- Method/formula fields nếu có.

Primary key:

```text
(provider, instId, fundingTime)
```

Upsert vì predicted record có thể được bổ sung realized rate sau settlement.

---

### 8.6 `GET /api/v5/public/open-interest`

**Mục đích:** snapshot open interest.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 20 request / 2 giây / IP + instrument ID |
| Required | `instType` |
| Optional | `instFamily`, `instId` |
| Products | `SWAP`, `FUTURES`, `OPTION`, `EVENTS` theo profile |

```http
GET /api/v5/public/open-interest?instType=SWAP&instId=BTC-USDT-SWAP
```

Fields:

| Field | Canonical |
|---|---|
| `oi` | `open_interest_contracts_raw` |
| `oiCcy` | `open_interest_ccy_raw` |
| `oiUsd` | `open_interest_usd_raw` |
| `ts` | `source_ts_ms` |

Không map tất cả về `open_interest` duy nhất. Preserve ba representation và unit. Với options family aggregation, xác định rõ record là instrument hay aggregate theo field identity thực tế.

#### Historical coverage policy

Endpoint này là **snapshot hiện tại**, không được giả định là historical OI API tương đương Binance. Baseline phải:

- bootstrap snapshot hiện tại rồi duy trì lịch sử từ durable canonical ingestion theo retention đã khai báo;
- trả `coverage_start`, `coverage_end`, `coverage_status` và provenance cho query lịch sử;
- trả `CAPABILITY_UNSUPPORTED` hoặc `PARTIAL_COVERAGE` cho khoảng trước watermark lưu trữ, không fabricate/backfill bằng aggregate khác;
- không dùng family/market aggregate thay cho instrument history nếu identity hoặc unit không tương đương;
- chỉ bổ sung nguồn lịch sử khác sau khi source authority, licensing và reconciliation policy được phê duyệt.

---

### 8.7 `GET /api/v5/public/price-limit`

**Mục đích:** buy/sell price limit hiện hành.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 20 request / 2 giây / IP + instrument ID |
| Required | `instId` |

```http
GET /api/v5/public/price-limit?instId=BTC-USDT-SWAP
```

Fields:

- `instType`, `instId`.
- `buyLmt`: highest buy limit.
- `sellLmt`: lowest sell limit.
- `enabled`: limit có hiệu lực hay không.
- `ts`.

Khi `enabled=false`, `buyLmt`/`sellLmt` có thể là empty string. Normalize thành `None`, không thành `0`.

---

### 8.8 `GET /api/v5/public/time`

**Mục đích:** API server time.

| Thuộc tính | Giá trị |
|---|---|
| Rate limit | 10 request / 2 giây / IP |
| Response | `ts` Unix milliseconds |

```http
GET /api/v5/public/time
```

Dùng để đo clock offset, không sửa system clock trong process:

```text
t0_local_monotonic
request
server_ts
response
t1_local_monotonic
estimated_rtt = t1 - t0
estimated_offset ≈ server_ts - midpoint_wall_clock
```

Expose metrics:

```text
okx_clock_offset_ms
okx_clock_rtt_ms
```

---

### 8.9 `GET /api/v5/public/mark-price`

**Mục đích:** mark price snapshot.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 10 request / 2 giây / IP + instrument ID |
| Required | `instType` |
| Optional | `instFamily`, `instId` |
| Types | `MARGIN`, `SWAP`, `FUTURES`, `OPTION`, `EVENTS` theo profile |

```http
GET /api/v5/public/mark-price?instType=SWAP&instId=BTC-USDT-SWAP
```

Fields: `instType`, `instId`, `markPx`, `ts`.

Canonical `price_type="mark"`. Không merge vào last price; valuation/risk phải chọn price type rõ ràng.

---

### 8.10 `GET /api/v5/public/position-tiers`

**Mục đích:** risk/position tiers, max leverage và maintenance-margin information.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 10 request / 2 giây / IP |
| Important params | `instType`, `tdMode`, cùng `instFamily`/`instId`/`ccy` tùy product |
| Use case | Risk enrichment, pre-trade validation snapshot |

```http
GET /api/v5/public/position-tiers?tdMode=cross&instType=SWAP&instFamily=BTC-USDT
```

Không đưa endpoint này vào hot live loop. Cache theo TTL và version bằng hash payload. Các field tier như min/max position, maintenance-margin ratio, initial-margin ratio, max leverage phải giữ Decimal/raw, không làm tròn.

Vì request/response shape khác theo `MARGIN`, derivatives và account mode, implement typed union:

```python
PositionTier = MarginPositionTier | DerivativePositionTier | UnknownPositionTier
```

---

### 8.11 `GET /api/v5/public/underlying`

**Mục đích:** danh sách underlying/family được hỗ trợ cho derivatives/options.

| Thuộc tính | Giá trị |
|---|---|
| Rate limit | 20 request / 2 giây / IP |
| Required | `instType` |
| Types | `SWAP`, `FUTURES`, `OPTION` |

```http
GET /api/v5/public/underlying?instType=FUTURES
```

Response có thể là nested arrays. Không ép thành object giả; normalize thành:

```python
@dataclass(frozen=True)
class UnderlyingFamily:
    inst_type: str
    underlying: str
    instrument_family: str | None
```

Dùng để discover family, nhưng registry `/instruments` vẫn là source-of-truth cho instrument cụ thể.

---

### 8.12 `GET /api/v5/public/insurance-fund`

**Tên docs:** security fund; HTTP path là `insurance-fund`.

| Thuộc tính | Giá trị |
|---|---|
| Rate limit | 10 request / 2 giây / IP |
| Params | `instType`, cùng `instFamily`/`uly`/`ccy` tùy product/profile |
| Use case | Risk monitoring, research |

```http
GET /api/v5/public/insurance-fund?instType=SWAP&instFamily=BTC-USD
```

Core response:

- `total` theo USD.
- `instType`, `instFamily`.
- `details[]`: `balance`, `amt`, `ccy`, `type`, `ts`.

Supported meaningful `type` hiện hành:

```text
liquidation_balance_deposit
bankruptcy_loss
```

Các type/field `adl`, `platform_revenue`, `maxBal`, `maxBalTs`, `decRate` liên quan đã deprecated/empty trong docs hiện hành; parser MAY giữ raw compatibility nhưng business logic KHÔNG được phụ thuộc.

---

### 8.13 `GET /api/v5/market/index-tickers`

Dù thuộc mục Public Data trong docs, path nằm dưới `/market`.

| Thuộc tính | Giá trị |
|---|---|
| Rate limit | 20 request / 2 giây / IP |
| Required | Ít nhất một trong `quoteCcy` hoặc `instId` theo contract/profile |
| Use case | Index bootstrap/reference |

```http
GET /api/v5/market/index-tickers?instId=BTC-USDT
```

Fields:

- `instId` — index ID, không phải tradable instrument ID.
- `idxPx`.
- `high24h`, `low24h`, `open24h`.
- `sodUtc0`, `sodUtc8`.
- `ts`.

Canonical identity:

```text
index_key = okx:index:{instId}
price_type = index
```

---

### 8.14 Index candlesticks

#### Latest

```http
GET /api/v5/market/index-candles
```

- Rate limit: 20 request / 2 giây / IP.
- Latest tối đa 1.440 entries.
- Request: `instId`, `bar`, `after`, `before`, `limit`.
- `limit` default/max theo endpoint hiện hành; adapter nên clamp theo docs profile, baseline `100`.

#### History

```http
GET /api/v5/market/history-index-candles
```

- Rate limit: 10 request / 2 giây / IP.
- Recent years.
- Pagination `after` cũ hơn, `before` mới hơn.

Row:

```text
[ts, open, high, low, close, confirm]
```

Không có volume. Canonical schema phải dùng `volume=null`, không `0`.

---

### 8.15 Mark-price candlesticks

#### Latest

```http
GET /api/v5/market/mark-price-candles
```

- Rate limit: 20 request / 2 giây / IP.
- Latest tối đa 1.440 entries.

#### History

```http
GET /api/v5/market/history-mark-price-candles
```

- Rate limit: 20 request / 2 giây / IP.
- Recent years.

Row:

```text
[ts, open, high, low, close, confirm]
```

Canonical fields phải gắn `price_type="mark"`; không trộn với trade candles trong cùng table nếu table không có dimension `price_type`.

---

### 8.16 `GET /api/v5/market/index-components`

**Mục đích:** constituent composition của một OKX index.

| Thuộc tính | Giá trị |
|---|---|
| Rate limit | 20 request / 2 giây / IP |
| Required | `index` |

```http
GET /api/v5/market/index-components?index=BTC-USD
```

Response:

- `index`, `last`, `ts`.
- `components[]`:
  - `exch`
  - `symbol`
  - `symPx`
  - `wgt`
  - `cnvPx`

`cnvPx` có thể khác `symPx` do quote conversion, multiplier adjustment hoặc smoothing. Không recompute index bằng `symPx * wgt` rồi coi mismatch là provider error.

Store versioned snapshot keyed by `(index, ts)` và constituent keyed by `(index, ts, exch, symbol)`.

---

### 8.17 Instrument tick bands, option summary/trades và endpoint mở rộng

Một số profile/docs hiện hành cung cấp:

- `GET /api/v5/public/instrument-tick-bands`.
- Option market data summary.
- Option trades theo family.
- Contract/coin conversion helper.
- Historical bulk market-data metadata/download.
- Economic calendar REST.

Policy:

1. Định nghĩa capability flag riêng từng endpoint.
2. Probe production hostname lúc deploy bằng request hợp lệ nhỏ nhất.
3. Pin request/response fixture theo docs của entity.
4. Không mở public internal route trước khi schema ổn định.
5. Economic calendar có thể yêu cầu auth/VIP và production-only; tách khỏi anonymous public client.
6. Tick bands là bắt buộc cho exact order validation của product áp dụng bands; minimum `tickSz` không đủ.

---

### 8.18 `GET /api/v5/system/status`

**Mục đích:** planned/unplanned maintenance/service status.

| Thuộc tính | Giá trị |
|---|---|
| Auth | Không |
| Rate limit | 1 request / 5 giây / IP |
| Optional filter | `state` |

```http
GET /api/v5/system/status
GET /api/v5/system/status?state=scheduled
```

Fields cần preserve:

- `title` nếu profile trả.
- `state`.
- `begin`, `end`, `preOpenBegin`.
- `href`/service detail nếu có.
- `serviceType`, `system`, `maintType`.
- `env`.

Status là signal vận hành, không phải bằng chứng market stream chắc chắn down/up. Readiness kết hợp status với live heartbeat và REST probe.

---

### 8.19 Endpoint có chữ `public` nhưng vẫn private

`GET /api/v5/public/interest-rate-loan-quota` yêu cầu authentication trong contract hiện hành. Không đặt method này vào `AnonymousOkxPublicClient`; nếu cần, đặt trong account/risk authenticated client và tách secret boundary.

---
## 9. WebSocket JSON — transport contract và channel map

### 9.1 Tách connection pool theo URL class

Baseline có hai pool public market data:

```text
Public pool:
  /ws/v5/public
  tickers, trades, books, instruments, mark/index/funding/OI, limits, liquidation, status

Business pool:
  /ws/v5/business
  candles, trades-all, mark-price candles, index candles, optional authenticated business channels
```

Không gửi channel public vào business hoặc ngược lại. Error `64002` phải được classify là routing/config error, không retry cùng URL.

```python
class OkxWsService(str, Enum):
    PUBLIC = "public"
    BUSINESS = "business"
```

Mỗi desired subscription phải tự khai báo service:

```python
@dataclass(frozen=True, slots=True)
class SubscriptionSpec:
    service: OkxWsService
    channel: str
    inst_id: str | None = None
    inst_type: str | None = None
    inst_family: str | None = None
    series_id: str | None = None
```

---

### 9.2 Connection, operation và heartbeat limits

Client PHẢI enforce ở phía mình:

| Limit | Contract |
|---|---|
| Connection requests | Tối đa 3 request kết nối / giây / IP |
| WS operations | Tổng `subscribe` + `unsubscribe` + `login` tối đa 480 / connection / giờ |
| Idle | Không có subscription hoặc không có data > khoảng 30 giây có thể bị disconnect |
| Subscription payload | Tổng args/channels trong request không vượt 64 KB |
| Request `id` | Tối đa 32 ký tự, case-sensitive alphanumeric |

Config đề xuất:

```env
OKX_WS_PING_IDLE_S=20
OKX_WS_PONG_TIMEOUT_S=10
OKX_WS_OPS_PER_SECOND=2
OKX_WS_OPS_PER_HOUR_SOFT_LIMIT=430
OKX_WS_MAX_ARGS_BYTES=60000
OKX_WS_RECONNECT_MIN_S=0.5
OKX_WS_RECONNECT_MAX_S=30
OKX_WS_CONNECTS_PER_SECOND=2
```

Dùng soft limit thấp hơn provider limit để dành headroom cho recovery.

#### Heartbeat

OKX application heartbeat là text frame:

```text
client -> "ping"
server -> "pong"
```

Không gửi JSON `{"op":"ping"}` trừ khi endpoint docs profile nói khác.

State:

```text
message received -> reset idle timer
idle >= N (<30s) -> send "ping"
pong before timeout -> healthy
no pong -> close connection, reconnect, resubscribe
```

System TCP ping/pong có thể dùng thêm, nhưng không thay application heartbeat.

---

### 9.3 Subscription frame

```json
{
  "id": "subA001",
  "op": "subscribe",
  "args": [
    {"channel": "tickers", "instId": "BTC-USDT"},
    {"channel": "trades", "instId": "BTC-USDT"}
  ]
}
```

Unsubscribe dùng cùng exact arg:

```json
{
  "id": "unsubA001",
  "op": "unsubscribe",
  "args": [
    {"channel": "trades", "instId": "BTC-USDT"}
  ]
}
```

#### Ack correlation

```json
{
  "id": "subA001",
  "event": "subscribe",
  "arg": {"channel": "tickers", "instId": "BTC-USDT"},
  "connId": "a4d3ae55"
}
```

Một request nhiều args có thể tạo ack theo arg. Subscription chỉ chuyển `PENDING -> ACTIVE` khi ack tương ứng đã nhận.

```text
DESIRED
  -> PENDING_SEND
  -> SENT
  -> ACTIVE
  -> UNSUB_PENDING
  -> REMOVED

failure:
  -> REJECTED_CAPABILITY
  -> RETRYABLE_ERROR
  -> DEAD_LETTER
```

---

### 9.4 Event frames phải xử lý

#### Subscribe / unsubscribe

- Correlate bằng `id` và normalized `arg`.
- Record `connId`.
- Unknown ack không được silently ignore; metric `orphan_ack_total`.

#### Error

```json
{
  "id": "subA001",
  "event": "error",
  "code": "60012",
  "msg": "Invalid request...",
  "connId": "a4d3ae55"
}
```

Classify:

| Code/class | Hành vi |
|---|---|
| `60012` invalid request | Không retry nguyên payload; quarantine config/schema |
| `64002` wrong WS service | Route correction; fail deployment check |
| `64003` fee tier denied | Disable capability; fallback channel |
| `64004` incompatible book subscriptions | Re-plan topology; unsubscribe conflicting channel |
| Rate/too-many request | Backoff; reduce batch/op rate |
| Unknown | Bounded retry + alert |

#### Notice `64008`

```json
{
  "event": "notice",
  "code": "64008",
  "msg": "The connection will soon be closed for a service upgrade. Please reconnect.",
  "connId": "a4d3ae55"
}
```

PHẢI thực hiện make-before-break:

1. Đánh dấu connection `DRAINING`.
2. Mở replacement connection trong connect-rate budget.
3. Subscribe desired set.
4. Chờ ack và, với books, chờ snapshot mới.
5. Atomically switch active connection generation.
6. Đóng connection cũ.

Không chờ server tự ngắt rồi mới reconnect.

---

### 9.5 Data frame routing

Một frame data điển hình:

```json
{
  "arg": {
    "channel": "trades",
    "instId": "BTC-USDT"
  },
  "data": [
    {}
  ]
}
```

Order-book incremental có thêm `action`:

```json
{
  "arg": {"channel": "books", "instId": "BTC-USDT"},
  "action": "update",
  "data": [{}]
}
```

Router key:

```text
(service, channel, instId?, instType?, instFamily?, seriesId?)
```

Không route chỉ bằng `channel`; instrument-level subscriptions cần identity đầy đủ.

---

### 9.6 Connection sharding

Sharding dimensions:

1. Service: public/business.
2. Channel cost: normal vs deep book/tick-by-tick.
3. Market family: spot, swap/futures, options/events.
4. Subscription count and payload bytes.
5. Expected message rate.
6. Access tier/login requirement.

Đề xuất:

```text
public-general-spot-N
public-general-derivatives-N
public-books-100ms-N
public-books-tbt-N
public-instruments-N
business-candles-N
business-trades-all-N
business-reference-candles-N
```

Với 50/400-level channels, giữ dưới 30 deep-book subscriptions/connection như khuyến nghị của OKX; production target nên thấp hơn khi symbols rất active.

---

## 10. WebSocket channel-by-channel

### 10.1 `tickers` — `/ws/v5/public`

```json
{"channel":"tickers","instId":"BTC-USDT"}
```

- Push nhanh nhất khoảng 100 ms khi trade hoặc BBO thay đổi.
- Fields giống REST ticker.
- Không bảo đảm mỗi trade tạo một ticker update.
- Pre-open có thể crossed BBO.

Canonical tick: `ticker`.

Latest-store conflict policy:

```python
if incoming.source_ts_ms > current.source_ts_ms:
    replace()
elif incoming.source_ts_ms == current.source_ts_ms:
    # OKX có thể cập nhật cùng timestamp; frame nhận sau thắng.
    replace_by_received_order()
else:
    record_late_event_without_overwriting_latest()
```

---

### 10.2 Candlesticks — `/ws/v5/business`

Subscription:

```json
{"channel":"candle1m","instId":"BTC-USDT"}
```

Push nhanh nhất khoảng 1 giây.

#### Channel names

| Time basis | Channels baseline |
|---|---|
| Standard | `candle3M`, `candle1M`, `candle1W`, `candle1D`, `candle2D`, `candle3D`, `candle5D`, `candle12H`, `candle6H`, `candle4H`, `candle2H`, `candle1H`, `candle30m`, `candle15m`, `candle5m`, `candle3m`, `candle1m`, `candle1s` khi product hỗ trợ |
| UTC calendar | `candle3Mutc`, `candle1Mutc`, `candle1Wutc`, `candle1Dutc`, `candle2Dutc`, `candle3Dutc`, `candle5Dutc`, `candle12Hutc`, `candle6Hutc` |

Không generate channel bằng `.lower()`.

Data row:

```text
[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
```

Rules giống REST candle. `confirm=0` phát `candle_update`; transition sang `confirm=1` phát `candle_close` đúng một lần theo event version.

#### Reconnect behavior

WS candle không bảo đảm replay mọi revision trong thời gian disconnect. Sau reconnect:

1. Subscribe và nhận update hiện tại.
2. REST top-up từ `last_persisted_open_ts - one_bar` tới now.
3. Upsert bằng key `(instId, bar, ts)`.
4. Re-emit only when canonical payload version changed.

---

### 10.3 `trades` — `/ws/v5/public`

```json
{"channel":"trades","instId":"BTC-USDT"}
```

**Semantics cực kỳ quan trọng:** một update có thể aggregate nhiều matches. OKX phát một message theo tổ hợp taker order + filled price + source; `count` cho biết số trade IDs được gộp.

Fields:

| Field | Semantics |
|---|---|
| `instId` | Instrument |
| `tradeId` | ID cuối/cao nhất của group theo contract |
| `px` | Filled price chung của group |
| `sz` | Aggregate size |
| `side` | Taker side |
| `ts` | Trade timestamp |
| `count` | Số matches aggregate |
| `source` | `0` normal, `1` RPI/ELP source |
| `seqId` | Publish sequence; có thể lặp, không dùng một mình làm trade primary key |

Ví dụ `tradeId=123`, `count=3` biểu diễn group IDs `123`, `122`, `121` theo semantics docs. Tuy nhiên:

- Không tự explode thành ba sizes bằng cách chia đều; không biết size từng match.
- Có thể tạo `aggregated_trade_id_range`, nhưng raw aggregate vẫn là event authoritative.
- `trade_agg` không tương đương tape atomic.

Dedup key đề xuất:

```text
(instId, seqId, tradeId, px, source, ts, side, sz, count)
```

Không chỉ `(instId, seqId)` vì `seqId` có thể lặp.

---

### 10.4 `trades-all` — `/ws/v5/business`

```json
{"channel":"trades-all","instId":"BTC-USDT"}
```

- Mỗi update chứa đúng một trade.
- Dùng khi backtest/live analytics cần atomic trade tape.
- Fields core: `instId`, `tradeId`, `px`, `sz`, `side`, `source`, `ts`.

Canonical tick: `trade_atomic`.

Dedup:

```text
(provider, instId, tradeId)
```

Nếu OKX/profile có ID reuse anomaly, retain payload hash và alert; không silently collapse khác price/size.

#### Chọn `trades` hay `trades-all`

| Consumer | Channel |
|---|---|
| Dashboard, rolling price/volume nhẹ | `trades` |
| Microstructure, exact trade count, replay | `trades-all` |
| Alpha chỉ cần last trade | `trades` hoặc ticker |
| Market impact calibration | `trades-all` |
| Low-bandwidth fallback | `trades` |

Có thể ingest cả hai nhưng PHẢI phát sang tick types/streams riêng, không double-count volume.

---

### 10.5 Order-book channels — `/ws/v5/public`

#### Channel matrix

| Channel | Depth | Push model | Fastest cadence | Access | State model |
|---|---:|---|---:|---|---|
| `bbo-tbt` | 1 | Snapshot on change | 10 ms | Public JSON baseline | Replace-only |
| `books5` | 5 | Snapshot on change | 100 ms | Public | Replace-only |
| `books` | 400 | Initial snapshot + increments | 100 ms | Public | Stateful |
| `books-rpi` | 400 | Initial snapshot + increments | 100 ms | Public/profile | Stateful, RPI shape |
| `books50-l2-tbt` | 50 | Snapshot + increments | 10 ms | VIP4+ | Stateful |
| `books-l2-tbt` | 400 | Snapshot + increments | 10 ms | VIP4+ | Stateful |
| `books-elp` | 400 | Legacy ELP-only | 100 ms | Deprecated | Do not target |

Normal `books`, `books5`, `bbo-tbt`, `books50-l2-tbt`, `books-l2-tbt` không trả RPI/ELP orders. `books-rpi` là consolidated view.

#### Fixed publish order trên cùng connection/symbol

```text
bbo-tbt
  -> books-l2-tbt
  -> books50-l2-tbt
  -> books
  -> books-elp
  -> books-rpi
  -> books5
```

Không dùng fixed order này để merge channels thành một sequence chung; mỗi channel vẫn có state riêng.

#### Incompatible subscriptions

Cùng `instId` trên cùng connection:

- Không subscribe đồng thời `books-l2-tbt` và `books50-l2-tbt`/`books`.
- Vi phạm có thể trả `64004`.
- Tier không đủ trả `64003`.

Planner phải resolve trước khi gửi:

```python
if wants_books_l2_tbt:
    move_books_or_books50_for_same_inst_to_another_connection_or_disable()
```

#### Snapshot-only channels

`bbo-tbt` và `books5` được xử lý replace-only:

```python
state.asks = parsed_asks
state.bids = parsed_bids
state.seq_id = msg.seqId
state.source_ts = msg.ts
publish_snapshot()
```

Không đòi `prevSeqId`.

#### Stateful channels

`books`, `books-rpi`, `books50-l2-tbt`, `books-l2-tbt`:

- `action=snapshot`: replace entire state.
- `action=update`: apply deltas.
- `quantity=0`: delete level.
- `quantity>0`: insert/update level.
- Validate `prevSeqId` continuity theo mục 11.

#### Level shape

Normal:

```text
[price, quantity, "0", orderCount]
```

RPI:

```text
[price, totalQty, nonRpiQty, orderCount]
```

#### Checksum

Field `checksum` có thể còn xuất hiện nhưng đã deprecated và fixed `0`. PHẢI ignore hoàn toàn. Integrity dùng `seqId/prevSeqId`.

---

### 10.6 `instruments` — `/ws/v5/public`

```json
{"channel":"instruments","instType":"SWAP"}
```

Conditional args cho options/events profile có thể gồm `instFamily`/`seriesId`.

**Không phải full bootstrap.** Channel này gửi incremental changes khi:

- Listing/delivery/exercise/suspension/state change.
- Trading parameters như tick size/min size/max market size thay đổi.
- `expTime` hoặc `listTime` thay đổi.
- Upcoming change được áp dụng.

Workflow bắt buộc:

```text
REST /public/instruments full snapshot
  -> atomic registry commit
  -> WS instruments subscribe
  -> apply versioned patches
  -> periodic REST reconcile
```

WS event có thể báo instrument expired/delisted mà REST list sau đó không còn record; tombstone phải được giữ.

---

### 10.7 `open-interest` — `/ws/v5/public`

```json
{"channel":"open-interest","instId":"BTC-USDT-SWAP"}
```

- Push khoảng 3 giây khi có update theo docs hiện hành.
- Fields `oi`, `oiCcy`, `oiUsd`, `ts`.
- Tick type `open_interest`.
- Latest-state only consumers có thể coalesce; archive consumers giữ event-time sequence.

Do OI có thể không đổi trong thời gian dài, absence of update không tự động là unhealthy; health dựa connection + heartbeat + channel-specific freshness SLA.

---

### 10.8 `funding-rate` — `/ws/v5/public`

```json
{"channel":"funding-rate","instId":"BTC-USDT-SWAP"}
```

Fields tương đương current funding REST, gồm method/formula, current/predicted rate, funding times, bounds, interest, impact, settlement state/rate, premium, `ts`.

Rules:

- Không assume cadence cố định.
- Upsert latest by `(instId, fundingTime)`.
- Khi `settState`/`settFundingRate` thay đổi, publish event dù `fundingRate` không đổi.
- Không drop empty deprecated `nextFundingRate` field as parser failure.

---

### 10.9 `price-limit` — `/ws/v5/public`

```json
{"channel":"price-limit","instId":"BTC-USDT-SWAP"}
```

- Push khoảng 200 ms khi limit thay đổi.
- Không push khi không thay đổi.
- Fields `buyLmt`, `sellLmt`, `enabled`, `ts`.

Latest state freshness phải hiểu event-driven; không đặt SLA “phải có message mỗi giây”.

---

### 10.10 `estimated-price` — `/ws/v5/public`

Ví dụ subscription theo profile:

```json
{
  "channel":"estimated-price",
  "instType":"FUTURES",
  "instFamily":"BTC-USDT"
}
```

- Push gần delivery/exercise/settlement, cadence nhanh khoảng 200 ms khi active.
- Identity có thể subscription theo `instType` + `instFamily`, payload có `instId`.
- Không coi silence ngoài settlement window là disconnect.

---

### 10.11 `mark-price` — `/ws/v5/public`

```json
{"channel":"mark-price","instId":"BTC-USDT-SWAP"}
```

- Push khoảng 200 ms khi thay đổi.
- Có heartbeat/update định kỳ khoảng 10 giây theo docs hiện hành.
- Fields `markPx`, `ts` cùng identity.

Nếu hai messages có cùng `ts`, message nhận sau thắng cho latest-state. Archive vẫn giữ receive sequence để audit.

---

### 10.12 `index-tickers` — `/ws/v5/public`

```json
{"channel":"index-tickers","instId":"BTC-USDT"}
```

- Push khoảng 100 ms khi index thay đổi.
- Nếu không đổi, provider có thể push định kỳ khoảng một phút.
- Fields `idxPx`, 24h stats, `sodUtc0`, `sodUtc8`, `ts`.

Identity namespace phải là index, không tradable instrument.

---

### 10.13 Mark-price candlesticks — `/ws/v5/business`

```json
{"channel":"mark-price-candle1m","instId":"BTC-USDT-SWAP"}
```

Push nhanh nhất khoảng 1 giây.

Row:

```text
[ts, open, high, low, close, confirm]
```

Không volume. Channel family gồm standard và UTC variants theo docs/profile, ví dụ:

```text
mark-price-candle1m
mark-price-candle1H
mark-price-candle1D
mark-price-candle1Dutc
mark-price-candle1Mutc
```

Không hard-code một list vĩnh viễn; khai báo supported set versioned và reject unknown config before subscribe.

---

### 10.14 Index candlesticks — `/ws/v5/business`

```json
{"channel":"index-candle1m","instId":"BTC-USDT"}
```

- Push nhanh nhất khoảng 1 giây.
- Row `[ts,o,h,l,c,confirm]`.
- `instId` là index ID.
- Tick type có thể `index_candle_update`/`index_candle_close` hoặc reuse `candle_*` với `price_type=index`; ưu tiên dimension `price_type` để schema thống nhất.

---

### 10.15 `liquidation-orders` — `/ws/v5/public`

Ví dụ:

```json
{"channel":"liquidation-orders","instType":"SWAP"}
```

Subscription có thể thu hẹp bằng family/instrument theo profile.

#### Semantics

- Đây là **recent liquidation order samples**, không phải tổng volume liquidation của toàn thị trường.
- Các record trong một push không nhất thiết chronological.
- `side`/position side semantics phải parse theo exact response fields; không đoán từ dấu quantity.
- Spot/margin quantity thường base currency; derivatives quantity contracts.

Canonical tick: `liquidation_sample`, không đặt tên `liquidation_total`.

Dedup composite:

```text
(instId, ts, side, posSide, bkPx, sz, raw_hash)
```

Không dùng `ts` một mình.

---

### 10.16 `adl-warning` — `/ws/v5/public`

```json
{
  "channel":"adl-warning",
  "instType":"FUTURES",
  "instFamily":"BTC-USDT"
}
```

- Push khi state là `warning` hoặc `adl`, cadence tối đa khoảng một lần/giây khi có condition.
- Không có normal/healthy push liên tục.
- Fields meaningful: `instType`, `instFamily`, `state`, `bal`, `ts`.
- Nhiều field legacy ADL/security fund đã deprecated và có thể trả `""`; parser phải tolerate.

Alerting rule:

```text
state=warning -> severity warning
state=adl     -> severity critical
silence       -> unknown/no active event, không suy ra healthy tuyệt đối
```

---

### 10.17 `status` — `/ws/v5/public`

```json
{"channel":"status"}
```

- Nhận status change mới nhất và các thay đổi tiếp theo.
- Fields tương tự REST system status.
- Dùng để chủ động giảm traffic/reconnect around maintenance.
- Không tự động stop toàn data-layer chỉ vì một serviceType khác market-data bị maintenance.

Map status theo affected capability/service.

---

### 10.18 Economic calendar — optional `/ws/v5/business`

Một số profile cung cấp economic-calendar channel, có thể yêu cầu login/VIP. Đây không phải anonymous core market-data.

Tách thành:

```text
OkxBusinessAuthenticatedWs
capability = economic_calendar
production_only/profile_dependent = true
```

Không cho thiếu credential của channel optional làm fail public ticker/order-book readiness.

---

## 11. Order-book state machine chuẩn production

### 11.1 State model

```python
@dataclass(slots=True)
class OrderBookState:
    instrument_id: str
    channel: str
    generation: int
    status: Literal[
        "awaiting_snapshot",
        "ready",
        "stale",
        "resyncing",
        "closed",
    ]
    bids: SortedPriceMap
    asks: SortedPriceMap
    last_seq_id: int | None
    last_source_ts_ms: int | None
    last_received_ns: int | None
    update_count: int
```

State is scoped by:

```text
(provider_profile, connection_generation, channel, instId)
```

Không share one book state giữa `books` và `books-rpi`.

---

### 11.2 Snapshot

Expected:

```text
action = snapshot
prevSeqId = -1      # stateful channels
seqId >= 0
```

Algorithm:

```python
def apply_snapshot(state, message):
    bids = parse_and_validate(message.bids)
    asks = parse_and_validate(message.asks)

    state.bids.replace_all(bids)
    state.asks.replace_all(asks)
    state.last_seq_id = message.seq_id
    state.last_source_ts_ms = message.ts_ms
    state.status = "ready"
    state.generation += 1

    publish_full_snapshot(state)
```

Do not apply pending deltas received before snapshot unless the protocol guarantees buffering and sequence validation. Baseline safer: discard pre-snapshot deltas, count anomaly, wait/resubscribe.

---

### 11.3 Incremental update

Normal continuity:

```text
incoming.prevSeqId == state.last_seq_id
```

Apply each level:

```python
def apply_levels(side, levels):
    for level in levels:
        price = Decimal(level.price)
        qty = Decimal(level.quantity)
        if qty == 0:
            side.delete(price)
        else:
            side.upsert(price, level)
```

Then:

```python
state.last_seq_id = incoming.seq_id
state.last_source_ts_ms = incoming.ts_ms
publish_delta_and_optional_top_n()
```

Atomicity: bids/asks update và `last_seq_id` phải commit trong một critical section; consumer không được thấy half-applied frame.

---

### 11.4 Empty keepalive update

OKX có thể gửi:

```json
{
  "asks": [],
  "bids": [],
  "prevSeqId": 100,
  "seqId": 100
}
```

Khi `prevSeqId == seqId == last_seq_id`:

- Không mutate book.
- Update liveness timestamps.
- Không publish fake depth change trừ diagnostics stream.

---

### 11.5 Sequence reset/maintenance exception

Thông thường `seqId > prevSeqId`, nhưng maintenance/reset có thể tạo sequence thấp hơn. Accept khi continuity vẫn chain đúng:

```text
incoming.prevSeqId == state.last_seq_id
incoming.seqId may be lower than incoming.prevSeqId
```

Sau đó set `last_seq_id = incoming.seqId` và future message chain từ giá trị mới.

Không viết validation `seqId must always increase`.

---

### 11.6 Gap detection

Gap khi:

```text
state.ready
and incoming.prevSeqId != state.last_seq_id
and not valid_empty_keepalive
```

Recovery:

```text
1. Mark state STALE immediately.
2. Stop publishing the state as valid executable book.
3. Emit book_gap event/metric with expected and observed sequence.
4. Unsubscribe/resubscribe or replace connection.
5. Discard old-generation updates.
6. Wait for a fresh WS snapshot.
7. Publish recovery snapshot with new generation.
8. Mark READY.
```

Không vá gap bằng REST `/books` vì không có sequence bridge được đảm bảo.

---

### 11.7 Crossed/locked book validation

- `best_bid > best_ask` có thể hợp lệ trong pre-open/call auction.
- `best_bid == best_ask` có thể là locked state ngắn hạn.
- Validator phải consult instrument `state/openType` và auction period.
- Trong continuous live normal market, persistent cross vượt threshold là anomaly, không nhất thiết parser failure.

Metrics:

```text
okx_book_crossed_total{inst_id,state}
okx_book_crossed_duration_ms{inst_id}
```

---

### 11.8 RPI state

RPI level stores:

```python
@dataclass(frozen=True, slots=True)
class RpiBookLevel:
    price: Decimal
    total_quantity_raw: Decimal
    non_rpi_quantity_raw: Decimal
    rpi_quantity_raw: Decimal
    order_count: int
```

Validation:

```text
totalQty >= 0
nonRpiQty >= 0
nonRpiQty <= totalQty + decimal_tolerance
rpiQty = max(totalQty - nonRpiQty, 0)
```

Qty zero deletion should use `totalQty == 0`. Preserve non-RPI field even if total zero for raw audit.

---

### 11.9 Data structures

Python baseline:

- `dict[Decimal, Level]` plus cached sorted top-N is simple nhưng sorting mỗi update tốn CPU.
- Production high-rate: balanced sorted map, `sortedcontainers.SortedDict`, hoặc Rust native book core.
- Do not use binary float keys.

Recommended boundary:

```python
class BookCore(Protocol):
    def replace(self, bids, asks) -> None: ...
    def apply(self, bid_deltas, ask_deltas) -> None: ...
    def top(self, depth: int) -> BookView: ...
```

Python và Rust implementations phải pass cùng golden test vectors.

---

### 11.10 Publishing policy

Không publish full 400 levels sau mỗi delta cho mọi consumer.

Tách:

```text
raw delta stream        -> exact replay/state builders
book top-N stream       -> alpha/UI consumers
latest top-N Redis key  -> low-latency snapshot
periodic full snapshot  -> recovery/archive
```

Ví dụ:

```text
stream:book_delta:okx_swap:books:BTC-USDT-SWAP
stream:book_top:okx_swap:books:BTC-USDT-SWAP:20
latest:book_top:okx_swap:books:BTC-USDT-SWAP:20
```

Mỗi event gồm `book_generation`, `seq_id`, `prev_seq_id`, `is_stale`.

---

## 12. SBE market data — phase tối ưu có kiểm soát

OKX đã triển khai SBE market data cho một số channel/tier. Đây không phải drop-in replacement của JSON.

### 12.1 Capability hiện hành cần coi là profile/tier-dependent

- SBE `bbo-tbt`: từ thay đổi 2026, có thể khả dụng cho mọi fee tier nhưng yêu cầu login.
- SBE `trades` và `books-l2-tbt`: có thể yêu cầu VIP4+.
- Access denial phải map về capability, không làm fail JSON fallback.
- SBE sử dụng `instIdCode`; code production và demo có thể khác, và có thể nullable cho instrument chưa hỗ trợ.

### 12.2 Không hard-code URL/schema từ memory

Tại build/deploy:

1. Đọc exact SBE section của docs cho entity/profile.
2. Pin URL, template/schema version và XML/IR artifact checksum.
3. Generate decoder hoặc dùng library đã version-lock.
4. Validate template ID/schema ID/version trên mỗi message.
5. Unknown template/version -> fail closed cho SBE stream, fallback JSON; không decode best-effort.

### 12.3 Architecture

```text
OKX SBE socket
  -> framed binary reader
  -> schema/version validator
  -> SBE decoder (Rust preferred)
  -> canonical event mapper
  -> same Redis/internal contracts as JSON
```

Transport-specific fields:

```json
{
  "transport": "ws-sbe",
  "sbe_schema_id": 1,
  "sbe_template_id": 100,
  "sbe_version": 3,
  "instrument_id_code": 12345,
  "instrument_id": "BTC-USDT"
}
```

### 12.4 Registry dependency

Build bi-directional mapping:

```text
(profile, environment, instIdCode) -> instId
(profile, environment, instId) -> instIdCode
```

Invalidation khi WS instruments/REST registry thay đổi. Không copy code từ production sang demo.

### 12.5 Rollout

```text
Phase 1: JSON authoritative, SBE shadow decode
Phase 2: compare canonical outputs/latency/gaps
Phase 3: SBE primary for selected channels, JSON hot fallback
Phase 4: wider rollout after error budget proves stable
```

Metrics compare:

```text
sbe_json_price_mismatch_total
sbe_json_sequence_gap_total
sbe_decode_latency_us
sbe_schema_unknown_total
sbe_instrument_code_miss_total
```

---
## 13. Instrument identity, symbol resolution và lifecycle

### 13.1 Canonical key

Không dùng một chuỗi `symbol` mơ hồ làm identity xuyên hệ thống. Dùng:

```text
provider         = okx
provider_profile = global | eea | tr | ...
market           = SPOT | MARGIN | SWAP | FUTURES | OPTION | EVENTS
instrument_id    = exact OKX instId
instrument_key   = okx:{market_lower}:{instrument_id}
```

Ví dụ:

```text
okx:spot:BTC-USDT
okx:swap:BTC-USDT-SWAP
okx:futures:BTC-USDT-260925
okx:option:BTC-USD-260925-70000-C
```

`BTCUSDT`, `BTC-USDT` và `BTC-USDT-SWAP` không được coi là cùng identity.

---

### 13.2 External alias resolution

Current facade nhận `BTCUSDT`. Compatibility resolver có thể hỗ trợ alias, nhưng output phải là một resolution object:

```python
@dataclass(frozen=True, slots=True)
class InstrumentResolution:
    requested_symbol: str
    resolved_inst_id: str
    inst_type: str
    resolution: Literal[
        "exact_inst_id",
        "registered_alias",
        "legacy_spot_concat",
    ]
    registry_version: str
```

Rules:

1. Exact `instId` match thắng.
2. Explicit mapping `(symbol, market)` thắng legacy heuristic.
3. Legacy concat chỉ cho spot allowlist và phải log deprecation.
4. Với derivatives/options/events, heuristic concat bị cấm.
5. Ambiguous input phải trả `409/422`, không chọn ngẫu nhiên.

Ví dụ request internal mới:

```http
GET /v2/market/ohlcv?provider=okx&market=SPOT&instrument_id=BTC-USDT&bar=1m&limit=300
```

Facade cũ:

```http
GET /v1/crypto/ohlcv/okx/BTCUSDT?interval=1m&limit=300
```

được translate rõ sang `market=SPOT`, không tự áp dụng cho SWAP.

---

### 13.3 Registry versioning

Mỗi full registry snapshot có:

```json
{
  "provider": "okx",
  "profile": "global",
  "snapshot_id": "sha256:...",
  "fetched_at_ms": 1730000000000,
  "server_time_offset_ms": 12,
  "instrument_count": 1234,
  "source": "rest"
}
```

Mỗi instrument mutation có:

```json
{
  "instrument_id": "BTC-USDT-SWAP",
  "registry_version_before": "sha256:a",
  "registry_version_after": "sha256:b",
  "changed_fields": ["tickSz", "minSz"],
  "effective_time_ms": 1730000100000,
  "source": "ws"
}
```

Không mutate object shared in-place. Dùng immutable spec + atomic pointer/version swap.

---

### 13.4 Upcoming parameter changes

Khi `upcChg` báo thay đổi sắp tới:

- Preserve `param`, `newValue` và effective timestamp nếu có.
- Phát `instrument_upcoming_change` event.
- Không áp dụng `newValue` trước effective time.
- Sau effective time, chờ/verify actual REST/WS registry update.
- Alert nếu actual value không chuyển trong tolerance window.

Điều này đặc biệt quan trọng với `tickSz`, `minSz`, `lotSz`, `maxMktSz`.

---

### 13.5 Alias/maturity

`alias` đã deprecated cho việc diễn đạt expiry. Dùng:

- `expTime` cho exact expiration timestamp.
- `listTime`, `contTdSwTime`, `preMktSwTime` cho lifecycle.
- `instId`/`instFamily` để identity/group.

Không parse `this_week`, `next_week`, `quarter` từ alias rồi dùng làm contract key.

---

### 13.6 Event contracts

- Market-data module có thể chỉ trả YES side.
- NO side là derived, không phải provider event.
- Nếu derive binary probability-like complement, giữ exact product rules, fee/price convention và tick constraints; không mặc định `NO = 1 - YES` cho mọi product/profile nếu docs chưa xác nhận.
- Derived event phải có `is_derived=true`, `derived_from` và không reuse raw `instId`.

---

## 14. Numeric precision, units và notional conversion

### 14.1 Không dùng float

Cấm:

```python
price = float(raw[0])
```

Dùng:

```python
price = Decimal(raw[0])
```

Hoặc fixed-point integer sau khi biết scale từ instrument spec.

JSON serialization downstream nên giữ decimal dưới dạng string:

```json
{"price":"68420.10","quantity_raw":"12"}
```

Không serialize `Decimal` thành IEEE-754 number.

---

### 14.2 Empty strings

OKX dùng `""` cho unavailable/not applicable/deprecated fields.

Normalization:

```python
def decimal_or_none(value: str | None) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(value)
```

Không convert empty thành zero. `None` và `0` có ý nghĩa khác nhau.

---

### 14.3 Quantity unit matrix

| Data | Spot/Margin | Derivatives |
|---|---|---|
| Trade `sz` | Base currency | Contracts |
| Book level quantity | Base currency | Contracts |
| Candle `vol` | Base currency | Contracts |
| Candle `volCcy` | Quote currency | Base currency |
| Candle `volCcyQuote` | Quote currency | Quote currency |
| Ticker `vol24h` | Base currency | Contracts |
| Ticker `volCcy24h` | Quote currency | Currency semantics theo contract/docs |
| Liquidation size | Base currency | Contracts |
| OI `oi` | N/A | Contracts |

Mọi canonical quantity có:

```json
{
  "quantity_raw": "12",
  "quantity_unit": "contract",
  "quantity_ccy": null
}
```

---

### 14.4 Contract conversion

Không có một công thức universal cho mọi `ctType`.

Input bắt buộc:

- `ctVal`.
- `ctMult` nếu meaningful.
- `ctValCcy`.
- `ctType` (`linear`, `inverse` hoặc product-specific).
- `settleCcy`.
- Price type dùng để convert.

Conceptual examples, chỉ áp dụng sau metadata validation:

```text
linear base exposure ≈ contracts * ctVal * ctMult
linear quote notional ≈ base exposure * price

inverse quote notional ≈ contracts * ctVal * ctMult
inverse base exposure ≈ quote notional / price
```

Implementation nên trả provenance:

```python
@dataclass(frozen=True)
class ConvertedQuantity:
    raw_contracts: Decimal
    base_quantity: Decimal | None
    quote_notional: Decimal | None
    conversion_price: Decimal | None
    conversion_price_type: str | None
    formula_version: str
    exact: bool
```

Nếu thiếu spec/price, preserve raw contracts; không fabricate conversion.

---

### 14.5 Price tick validation

```python
(price - band_origin) % tick_size == 0
```

Nhưng với tick bands, exact tick depends on price band. Use registry/tick-band service:

```python
valid_tick = tick_band_resolver.tick_for(inst_id, price, event_time)
```

Lưu version/effective time của tick-band; historical validation phải dùng spec tại event time, không current spec.

---

### 14.6 Timestamp taxonomy

Mỗi event nên có tối thiểu:

```text
source_ts_ms       provider event/generation time
received_ts_ns     local monotonic/wall receive point
normalized_ts_ns   parsing completed
published_ts_ns    internal publish completed
```

Derived:

```text
network_plus_provider_lag_ms = received_wall_ms - source_ts_ms
normalization_latency_us     = normalized_ts_ns - received_ts_ns
publish_latency_us           = published_ts_ns - normalized_ts_ns
```

Không dùng `time.time()` nhiều lần trong hot path khi `time.time_ns()`/monotonic pair phù hợp hơn.

---

## 15. Historical ingestion, pagination và reconciliation

### 15.1 Generic paginator contract

```python
class OkxCursorDirection(str, Enum):
    OLDER = "after"
    NEWER = "before"
```

```python
@dataclass(frozen=True)
class HistoricalQuery:
    start_ms: int | None
    end_ms: int | None
    limit: int
    direction: OkxCursorDirection
    max_pages: int
```

A paginator must implement:

- Boundary filter.
- Dedup.
- No-progress detection.
- Max pages/records/time budget.
- Rate-limit bucket.
- Retry budget.
- Coverage metadata.
- Ascending canonical output unless endpoint contract explicitly asks otherwise.

---

### 15.2 No-progress guard

```python
new_cursor = cursor_from_oldest(page)
if new_cursor == previous_cursor:
    raise PaginationStalled(...)
```

Cũng dừng nếu toàn bộ keys page đã seen lặp lại liên tiếp.

---

### 15.3 Candle reconciliation

Sources:

```text
WS current candle
REST latest candles
REST history candles
stored historical rows
```

Priority không đơn giản “WS luôn thắng” hoặc “REST luôn thắng”. Use version:

```text
same candle key:
  greater confirm wins over lower confirm when payload coherent
  same confirm: later provider/receive version wins
  conflicting confirmed candles: store correction/audit + alert
```

Suggested record:

```json
{
  "provider":"okx",
  "inst_id":"BTC-USDT",
  "bar":"1m",
  "open_ts_ms":1730000000000,
  "o":"...",
  "h":"...",
  "l":"...",
  "c":"...",
  "confirm":1,
  "revision":3,
  "source_transport":"rest-history",
  "observed_at_ms":1730000065000
}
```

---

### 15.4 Trade reconciliation

- REST recent/history trade and WS atomic trade can overlap.
- Dedup by `(instId, tradeId)` where exact atomic semantics align.
- Do not dedup WS aggregate `trades` against atomic `trades-all` as if same row.
- Store aggregate stream separately or annotate source granularity.

```text
trade_granularity = aggregate | atomic
```

---

### 15.5 Snapshot regression guard

REST ticker/mark/index/book snapshot after request B may be older than state after request A.

```python
def should_update_latest(current, incoming):
    if incoming.source_ts_ms > current.source_ts_ms:
        return True
    if incoming.source_ts_ms < current.source_ts_ms:
        return False
    return incoming.received_ts_ns > current.received_ts_ns
```

Still archive late snapshot when diagnostics/research needs it; do not overwrite latest.

Metric:

```text
okx_rest_snapshot_regression_total{endpoint,inst_id}
```

---

### 15.6 Backfill scheduling

Use priority queues:

```text
P0: reconnect top-up for execution/reference active instruments
P1: startup warmup for active alpha universe
P2: scheduled recent reconciliation
P3: deep historical archive
P4: optional research universe
```

Separate token buckets so P3 cannot starve P0/P1.

---

### 15.7 Data completeness contract

Every historical response to internal consumers should include:

```json
{
  "coverage": {
    "requested_start_ms": 1720000000000,
    "requested_end_ms": 1730000000000,
    "observed_start_ms": 1721000000000,
    "observed_end_ms": 1730000000000,
    "complete_start": false,
    "complete_end": true,
    "provider_retention": "recent years",
    "pages": 42,
    "deduplicated_rows": 17
  }
}
```

Không trả partial data mà không báo partial.

---

## 16. Target code structure cho `quant-data-layer`

### 16.1 Module layout

```text
app/
  providers/
    okx/
      __init__.py
      config.py
      capabilities.py
      constants.py
      errors.py
      models.py
      symbols.py
      units.py
      rate_limit.py
      rest_client.py
      market_rest.py
      public_rest.py
      parsers/
        common.py
        ticker.py
        trades.py
        candles.py
        books.py
        instruments.py
        public_data.py
        status.py
      ws/
        protocol.py
        subscription.py
        connection.py
        supervisor.py
        public.py
        business.py
        router.py
        heartbeat.py
      order_book/
        models.py
        state.py
        engine.py
        recovery.py
      sbe/
        capability.py
        decoder.py
        registry.py
      recovery.py
      provider.py
  stream/
    okx_feed_builder.py
    okx_publishers.py
  schemas/
    market_data_v2.py
    okx_market_data.py
  api/
    routes_okx.py
  sdk/
    ...

tests/
  providers/
    okx/
      fixtures/
      test_rest_*.py
      test_ws_*.py
      test_order_book.py
      test_symbols.py
      test_units.py
      test_recovery.py
      test_capabilities.py
```

Do not keep growing a single `rest.py`.

---

### 16.2 Responsibility boundaries

#### `rest_client.py`

- HTTP transport.
- Base URL/profile headers.
- timeout/retry/rate limit.
- response envelope/error validation.
- No endpoint-specific normalization.

#### `market_rest.py` / `public_rest.py`

- Endpoint path and query models.
- Clamp endpoint-specific limits.
- Return parsed typed response.

#### `parsers/*`

- Raw string/array -> typed provider model.
- Strict positional length validation.
- Empty/unknown enum handling.
- No Redis/network calls.

#### `provider.py`

- High-level facade used by routes/feed builder.
- Registry resolution.
- Historical pagination.
- Capability policy.

#### `ws/supervisor.py`

- Desired subscription set.
- Connection sharding.
- reconnect/notice/heartbeat.
- ack/error state.

#### `order_book/engine.py`

- Pure state machine.
- No WebSocket I/O.
- Deterministic testability.

---

### 16.3 Async REST transport

Replace sync `requests` in async lifecycle with shared `httpx.AsyncClient` or equivalent:

```python
class OkxRestClient:
    def __init__(self, settings, limiter, metrics):
        self._client = httpx.AsyncClient(
            base_url=settings.rest_base_url,
            timeout=httpx.Timeout(
                connect=settings.connect_timeout_s,
                read=settings.read_timeout_s,
                write=settings.write_timeout_s,
                pool=settings.pool_timeout_s,
            ),
            limits=httpx.Limits(
                max_connections=settings.max_connections,
                max_keepalive_connections=settings.max_keepalive_connections,
            ),
            http2=True,
            headers=self._base_headers(settings),
        )
```

- One client per process/profile, not per request.
- Close in FastAPI lifespan.
- Do not hold global module client that leaks in tests.

---

### 16.4 Response envelope validation

```python
async def get(self, path: str, params: Mapping[str, str], bucket: str):
    await self._limiter.acquire(bucket)
    response = await self._client.get(path, params=params)
    response.raise_for_status()

    payload = orjson.loads(response.content)
    if not isinstance(payload, dict):
        raise OkxSchemaError("root is not object")
    if payload.get("code") != "0":
        raise classify_okx_error(payload, response.status_code)
    data = payload.get("data")
    if not isinstance(data, list):
        raise OkxSchemaError("data is not array")
    return data
```

Do not log full response at INFO; sample/hash/redact.

---

### 16.5 Compatibility facade cho `fetch_candles`

Giữ signature trong migration nhưng sửa semantics:

```python
async def fetch_candles_compat(
    symbol: str,
    interval: str = "1m",
    limit: int = 500,
    start_time: int | None = None,
    end_time: int | None = None,
    market: str = "SPOT",
) -> dict:
    resolution = registry.resolve(symbol, market=market)
    rows = await okx_provider.fetch_candle_window(
        inst_id=resolution.resolved_inst_id,
        bar=normalize_bar(interval),
        start_ms=start_time,
        end_ms=end_time,
        limit=limit,
    )
    return legacy_response_adapter(rows, resolution)
```

`limit > 300` phải paginate ở facade mới, không silently clamp toàn request và làm consumer tưởng nhận đủ.

Legacy response thêm:

```json
{
  "partial": false,
  "requested_limit": 500,
  "returned_count": 500,
  "pages": 2,
  "deprecated_contract": true
}
```

---

### 16.6 Settings

```python
class OkxSettings(BaseSettings):
    enabled: bool = False
    region_profile: str = "global"
    rest_base_url: AnyHttpUrl
    ws_public_url: AnyUrl
    ws_business_url: AnyUrl
    demo: bool = False

    rest_connect_timeout_s: float = 3.0
    rest_read_timeout_s: float = 10.0
    rest_max_connections: int = 64
    rest_max_keepalive_connections: int = 32
    rest_max_concurrency: int = 32

    ws_ping_idle_s: float = 20.0
    ws_pong_timeout_s: float = 10.0
    ws_reconnect_min_s: float = 0.5
    ws_reconnect_max_s: float = 30.0
    ws_max_args_bytes: int = 60_000
    ws_deep_book_channels_per_connection: int = 24

    enabled_inst_types: set[str] = {"SPOT", "SWAP"}
    capability_probe: bool = True
    raw_payload_enabled: bool = False
    raw_payload_ttl_s: int = 300
```

Validate `ws_ping_idle_s < 30`.

---

### 16.7 Capability manifest implementation

```python
@dataclass(frozen=True)
class OkxCapability:
    name: str
    enabled_by_config: bool
    verified: bool
    available: bool
    auth_required: bool
    tier_required: str | None
    profile: str
    verified_at_ms: int | None
    failure_code: str | None
    failure_message: str | None
```

Probe rules:

- Core REST: small valid request.
- WS: subscribe one low-volume instrument/channel, verify ack, unsubscribe.
- Deep book/SBE: only when configured credentials/tier available.
- Cache probe result with expiry; refresh on deployment and provider capability error.

---

## 17. Internal REST API contract

### 17.1 Versioned endpoint strategy

Giữ `/v1` compatibility. Public consumer contract mới PHẢI provider-neutral và dùng canonical `instrument_uid`; provider là provenance/source policy, không phải namespace mà alpha phải hard-code.

Stable consumer routes:

```text
GET  /v2/market-data/{instrument_uid}/snapshot
GET  /v2/market-data/{instrument_uid}/warmup
POST /v2/market-data/warmup:batch
GET  /v2/instruments/{instrument_uid}
GET  /v2/instruments
```

Provider operations remain internal/control-plane diagnostics and are not the stable alpha-facing surface:

```text
GET  /internal/v2/providers/okx/capabilities
GET  /internal/v2/providers/okx/status
GET  /internal/v2/providers/okx/instruments/{inst_id}
POST /internal/v2/providers/okx/subscriptions/reconcile
POST /internal/v2/providers/okx/backfill
```

The V1 compatibility facade may keep `/v1/crypto/ohlcv/okx/...` and legacy rows unchanged while resolving them into canonical identity internally.

Do not expose raw provider pass-through query indiscriminately. Validate enum/limits to protect provider budget.

---

### 17.2 Candle query

```http
GET /v2/market-data/OKX.SWAP.PERPETUAL.BTC-USDT/warmup?feed=bar&bar=1m&start_ms=...&end_ms=...&limit=1000&price_type=trade
```

Response:

```json
{
  "schema_version": 2,
  "provider": "okx",
  "profile": "global",
  "market": "SWAP",
  "instrument_id": "BTC-USDT-SWAP",
  "bar": "1m",
  "price_type": "trade",
  "items": [],
  "coverage": {},
  "source_endpoints": [
    "/api/v5/market/history-candles",
    "/api/v5/market/candles"
  ]
}
```

---

### 17.3 Latest order book

```http
GET /v2/market-data/OKX.SWAP.PERPETUAL.BTC-USDT/snapshot?feed=order_book&channel=books&depth=20
```

Response must include:

```json
{
  "ready": true,
  "stale": false,
  "channel": "books",
  "generation": 4,
  "seq_id": 123456,
  "source_ts_ms": 1730000000000,
  "received_ts_ms": 1730000000012,
  "bids": [],
  "asks": [],
  "quantity_unit": "contract"
}
```

Return `503` or explicit `ready=false` when state awaiting snapshot/stale; do not return last book as if live without stale flag.

---

### 17.4 Capability endpoint

```json
{
  "provider": "okx",
  "profile": "global",
  "environment": "production",
  "rest": {
    "market.candles": {"available": true},
    "market.books_rpi": {"available": true}
  },
  "ws": {
    "public.books": {"available": true},
    "public.books_l2_tbt": {
      "available": false,
      "reason": "fee_tier",
      "provider_code": "64003"
    }
  }
}
```

---

### 17.5 Internal authorization and quotas

Dù OKX public, internal warmup/deep-book endpoints có thể tiêu tốn shared provider quota. Add:

- Service identity/internal auth.
- Per-consumer quotas.
- Max symbols/window/pages.
- Batch limits.
- Audit logs cho manual subscription/reconcile.

Alpha không được dùng internal API làm unbounded provider proxy.

---

## 18. Redis channel/key contracts

### 18.1 Naming principles

- Include provider and market namespace.
- Preserve exact `instId` hoặc canonical escaped form.
- Include channel/granularity where semantics differ.
- Keep legacy aliases only during migration.

Suggested channels:

```text
stream:trade:okx_spot:BTC-USDT
stream:trade:okx_swap:BTC-USDT-SWAP
stream:trade_all:okx_spot:BTC-USDT
stream:ticker:okx_spot:BTC-USDT
stream:bbo:okx_swap:BTC-USDT-SWAP
stream:book:okx_swap:books:BTC-USDT-SWAP
stream:book:okx_spot:books-rpi:BTC-USDT
stream:kline:okx_swap:1m:BTC-USDT-SWAP
stream:mark_price:okx_swap:BTC-USDT-SWAP
stream:index_price:okx:BTC-USDT
stream:funding_rate:okx_swap:BTC-USDT-SWAP
stream:open_interest:okx_swap:BTC-USDT-SWAP
stream:price_limit:okx_swap:BTC-USDT-SWAP
stream:instrument:okx:SWAP
stream:liquidation:okx:SWAP
stream:adl_warning:okx:FUTURES:BTC-USDT
stream:status:okx
```

Latest keys:

```text
latest:ticker:okx_spot:BTC-USDT
latest:trade:okx_spot:BTC-USDT
latest:trade_all:okx_spot:BTC-USDT
latest:book:okx_swap:books:BTC-USDT-SWAP
latest:kline:okx_swap:1m:BTC-USDT-SWAP
latest:mark_price:okx_swap:BTC-USDT-SWAP
latest:index_price:okx:BTC-USDT
latest:funding_rate:okx_swap:BTC-USDT-SWAP
latest:open_interest:okx_swap:BTC-USDT-SWAP
registry:instrument:okx:BTC-USDT-SWAP
registry:instruments:okx:SWAP
health:provider:okx
```

---

### 18.2 Pub/Sub versus durable streams

Current system uses Redis Pub/Sub, which drops messages for disconnected/slow consumers. Target architecture places a Kafka-compatible durable log before Redis projection:

```text
Kafka-compatible log   canonical accepted-event durability and replay
Redis latest keys      rebuildable latest-state cache
Redis Pub/Sub          V1 low-latency compatibility fan-out
Redis Streams          bounded transitional bridge only, if explicitly approved
Object storage         governed raw/history archive
```

Minimum target design:

- Commit accepted raw/canonical event to durable log or bounded durable local spool before reporting publication success.
- Project the same event ID to Redis latest state and V1 Pub/Sub.
- V2 consumer reconnects from durable cursor; V1 consumer recovers via warmup/latest-state contract.
- Redis Streams, when used during migration, must declare max length, cursor expiry, memory budget and sunset criterion. It is not the long-term source of truth.

Do not claim REST can reconstruct missed order-book deltas.

---

### 18.3 Atomic publish

For latest + stream metadata:

```text
MULTI / pipeline
  SET latest:key payload PX ttl
  XADD recovery:stream ...
  PUBLISH stream:channel payload
EXEC
```

Strict atomicity with Pub/Sub and Stream has caveats; define ordering. A robust pattern:

1. Generate event ID.
2. Write durable/latest state.
3. Publish event referencing same ID.
4. Consumer can fetch event/latest if notification arrives first/duplicate.

---

### 18.4 TTL policy

| Key | TTL |
|---|---|
| Live ticker/trade | Dynamic by market SLA, e.g. seconds |
| Book | Short; invalidated immediately on gap |
| Candle current | Several bars |
| Mark/index/funding/OI | Channel-specific; not one global TTL |
| Instrument registry | Long/no TTL with version/reconcile timestamp |
| Status | Until superseded, with observed time |

A key expiring means “not fresh/available”, not price zero.

---

### 18.5 Backward compatibility

Current generic keys like `stream:trade:{symbol}` should not silently point to OKX when existing consumers assume Binance. Use explicit provider namespace. A migration alias must declare source:

```json
{
  "source":"okx",
  "source_role":"reference",
  "legacy_alias":true
}
```

No execution consumer should switch source solely due alias fallback.

---
## 19. Rate limiting, retries và error taxonomy

### 19.1 Endpoint-specific buckets

Không dùng một global semaphore duy nhất. Rate limiter key:

```text
(profile, endpoint, rate_limit_rule_dimensions)
```

Ví dụ:

```text
okx:global:/market/tickers:ip
okx:global:/public/instruments:ip+SPOT
okx:global:/public/funding-rate:BTC-USDT-SWAP
```

Minimum bucket table:

| Endpoint | Limit | Bucket rule |
|---|---:|---|
| `/market/tickers` | 20 / 2s | IP |
| `/market/ticker` | 20 / 2s | IP |
| `/market/books` | 40 / 2s | IP |
| `/market/books-rpi` | 20 / 2s | IP |
| `/market/books-full` | 10 / 2s | IP |
| `/market/candles` | 40 / 2s | IP |
| `/market/history-candles` | 20 / 2s | IP |
| `/market/trades` | 100 / 2s | IP |
| `/market/history-trades` | 20 / 2s | IP |
| `/public/instruments` | 20 / 2s | IP + instType |
| `/public/estimated-price` | 10 / 2s | IP |
| `/public/delivery-exercise-history` | 40 / 2s | IP + type/family |
| `/public/funding-rate` | 10 / 2s | IP + instId |
| `/public/funding-rate-history` | 10 / 2s | IP + instId |
| `/public/open-interest` | 20 / 2s | IP + instId/context |
| `/public/price-limit` | 20 / 2s | IP + instId |
| `/public/time` | 10 / 2s | IP |
| `/public/mark-price` | 10 / 2s | IP + instId |
| `/public/position-tiers` | 10 / 2s | IP |
| `/public/underlying` | 20 / 2s | IP |
| `/public/insurance-fund` | 10 / 2s | IP |
| `/market/index-tickers` | 20 / 2s | IP |
| `/market/index-candles` | 20 / 2s | IP |
| `/market/history-index-candles` | 10 / 2s | IP |
| `/market/mark-price-candles` | 20 / 2s | IP |
| `/market/history-mark-price-candles` | 20 / 2s | IP |
| `/market/index-components` | 20 / 2s | IP |
| `/system/status` | 1 / 5s | IP |

Rate limits are provider contract and may change. Keep them in versioned config generated/reviewed from docs; do not scatter numeric literals.

---

### 19.2 Token bucket with safety margin

```python
provider_limit = 20
window_s = 2
configured_rate = provider_limit * 0.85 / window_s
configured_burst = max(1, floor(provider_limit * 0.8))
```

For high-priority recovery, use reservation, not violating provider rate:

```text
80% normal pool
20% reserved P0 recovery
unused reservation may be borrowed after threshold
```

---

### 19.3 Retry matrix

| Failure | Retry? | Policy |
|---|---:|---|
| DNS/connect timeout | Có | Exponential backoff + jitter, bounded |
| Read timeout | Có | Endpoint/idempotent GET, bounded |
| HTTP 429 / code `50011` | Có | Respect/backoff, reduce limiter |
| HTTP 5xx | Có | Bounded, circuit breaker |
| `code != 0` invalid params | Không | Fix request, quarantine |
| Capability/tier error | Không cùng config | Disable/fallback |
| JSON decode/schema mismatch | Limited | Retry once if truncation suspected; then alert |
| Empty valid `data` | Không mặc định | Domain-valid; handle endpoint semantics |
| Stale snapshot regression | Không retry storm | Ignore latest overwrite; metric |

Backoff:

```python
sleep = min(cap, base * 2**attempt) * random.uniform(0.5, 1.5)
```

Do not retry all instruments in sync; jitter by instrument hash.

---

### 19.4 Circuit breaker scope

One breaker per capability family:

```text
rest_market_snapshot
rest_historical_candles
rest_instruments
ws_public_general
ws_public_books
ws_business_candles
ws_business_trades_all
sbe_market_data
```

A failing optional option endpoint must not open breaker for spot ticker.

Breaker transitions:

```text
CLOSED
  -> OPEN after threshold in rolling window
  -> HALF_OPEN after cooldown
  -> CLOSED on probes
  -> OPEN on failed probe
```

Readiness must encode required capabilities per deployment.

---

### 19.5 Error classes

```python
class OkxError(Exception): ...
class OkxTransportError(OkxError): ...
class OkxHttpError(OkxError): ...
class OkxApiError(OkxError): ...
class OkxRateLimitError(OkxApiError): ...
class OkxInvalidRequestError(OkxApiError): ...
class OkxCapabilityError(OkxApiError): ...
class OkxSchemaError(OkxError): ...
class OkxSequenceGapError(OkxError): ...
class OkxStaleDataError(OkxError): ...
class OkxPaginationStalledError(OkxError): ...
```

Each error carries:

```text
provider_code
http_status
endpoint/channel
profile
request_id/connection_id
retryable
capability
safe_context
```

Do not include secrets/signatures.

---

## 20. Startup, recovery, readiness và source role

### 20.1 Provider startup sequence

```text
1. Load + validate OKX settings/profile.
2. Initialize shared async REST client and rate limiters.
3. Fetch /public/time and record clock offset.
4. Fetch /system/status.
5. Capability probes for enabled endpoint/channel set.
6. Bootstrap /public/instruments for configured instTypes/families.
7. Build immutable registry and symbol aliases.
8. Warm latest ticker/mark/index/funding/OI where required.
9. Start public/business WS supervisors.
10. Subscribe instruments lifecycle channel.
11. Subscribe general channels.
12. Subscribe books; wait fresh snapshots.
13. REST top-up candles/trades where required.
14. Publish provider health and mark required feed groups ready.
```

Do not mark whole provider ready before book snapshot/required channel ack.

---

### 20.2 Per-feed readiness

```json
{
  "provider":"okx",
  "overall":"degraded",
  "feeds": {
    "spot.ticker": {"status":"ready"},
    "spot.trades": {"status":"ready"},
    "spot.books": {"status":"recovering"},
    "swap.funding": {"status":"ready"},
    "options.summary": {"status":"disabled"}
  }
}
```

Statuses:

```text
disabled
starting
warming
subscribing
awaiting_snapshot
ready
degraded
stale
recovering
failed
```

---

### 20.3 Reconnect sequence

General non-book channel:

```text
connection lost
  -> mark connection unhealthy
  -> retain last state with stale clock running
  -> reconnect with jitter
  -> resubscribe desired set
  -> wait acknowledgements
  -> REST top-up if channel needs continuity
  -> mark ready
```

Book:

```text
connection lost/gap
  -> invalidate book immediately
  -> reconnect/resubscribe
  -> wait new WS snapshot
  -> new generation
  -> ready
```

---

### 20.4 Source-role policy

The repository already treats OKX fallback as reference-only unless a separate risk policy authorizes it. Preserve explicit fields:

```text
source_role = authoritative | reference | fallback | shadow
execution_eligible = true | false
risk_policy_id = ...
```

Example:

```json
{
  "provider":"okx",
  "source_role":"reference",
  "execution_eligible":false,
  "reason":"deployment_policy"
}
```

Availability does not promote source role.

---

### 20.5 Multi-source failover

Failover requirements:

1. Instrument mapping verified.
2. Market/product equivalent, not merely same base/quote text.
3. Price type equivalent.
4. Freshness within source-specific SLA.
5. Unit/contract size compatible.
6. Risk explicitly authorizes provider role.
7. Consumer sees source change event.

```json
{
  "event":"market_data_source_change",
  "instrument_key":"internal:btc-usdt-spot",
  "from":"binance",
  "to":"okx",
  "effective_ts_ms":1730000000000,
  "policy_id":"md-failover-v3",
  "execution_eligible":false
}
```

Never hide provider switch behind generic `price` key.

---

### 20.6 Shutdown

```text
1. Mark provider draining/not ready.
2. Stop accepting new manual subscriptions/backfills.
3. Cancel scheduled history tasks.
4. Send bounded unsubscribes where useful, but do not exceed op limit.
5. Close WS connections.
6. Flush metrics/checkpoints.
7. Close REST client.
```

Do not block shutdown indefinitely waiting for unsubscribe acks.

---

## 21. Mô hình freshness và health

### 21.1 Channel-specific SLA

No single `STREAM_STALE_SECONDS` fits all channels.

Example configurable classes:

| Feed | Warning | Stale/blocking semantics |
|---|---:|---|
| `bbo-tbt` active market | Hundreds of ms to seconds | Strict for execution-grade paths |
| `books` active market | Seconds | Gap invalidates immediately regardless age |
| `trades` | Market-liquidity dependent | Silence alone not always outage |
| Ticker | Seconds | Depends active market/session |
| Candle `1m` | > expected updates | Confirm timing/bar boundary aware |
| Mark price | > heartbeat multiple | Expected periodic update |
| OI | Several publish intervals | Low-frequency okay |
| Funding | Minutes/next funding context | Event schedule aware |
| Price limit | Event-driven | Silence normal |
| Estimated price | Settlement-window aware | Silence normal outside window |
| ADL warning | Event-driven | Silence normal |
| Instruments | Event-driven + periodic REST reconcile | Silence normal |

---

### 21.2 Health dimensions

```text
transport_health
subscription_health
message_freshness
sequence_integrity
parser_health
registry_health
publish_health
source_role_eligibility
```

Overall cannot be derived only from “socket connected”.

---

### 21.3 Latest-state validation for consumer

Every latest payload exposes:

```json
{
  "freshness": {
    "age_ms": 12,
    "sla_ms": 1000,
    "is_fresh": true
  },
  "quality": {
    "sequence_valid": true,
    "registry_resolved": true,
    "unit_resolved": true,
    "is_snapshot_regression": false
  },
  "source": {
    "provider":"okx",
    "role":"reference",
    "execution_eligible":false
  }
}
```

---

## 22. Persistence, ordering và schema evolution

### 22.1 Raw, provider-typed và canonical layers

```text
Layer 0 raw frame/response       optional short-lived/capture
Layer 1 OKX typed model          exact provider semantics
Layer 2 canonical event          shared downstream contract
Layer 3 derived analytics        notional, spread, imbalance, NO side, etc.
```

Do not parse directly raw -> generic dict; provider-typed layer prevents semantic loss.

---

### 22.2 Ordering

Different channels/connections have no global total order.

Store:

- `source_ts`.
- `received_ts`.
- `connection_id/generation`.
- `seqId` where provided.
- local `ingest_sequence` per process/partition.

A canonical event ID must not imply global chronology.

---

### 22.3 Dedup table

| Tick type | Dedup key |
|---|---|
| Atomic trade | `(provider, instId, tradeId)` |
| Aggregate trade | Composite with `seqId`, `tradeId`, price/source/time/size/count |
| Candle | `(provider, instId, priceType, bar, openTs)` + revision |
| Ticker | Latest conflict by source/receive time; archive hash |
| Mark/index price | Identity + timestamp + receive version |
| Funding | `(instId, fundingTime)` + revision |
| OI | `(instId, ts)` or payload hash if same ts updates |
| Book delta | `(generation, channel, instId, seqId, payload_hash)` |
| Instrument update | `(instId, registry_version_after)` |
| Liquidation | Composite/raw hash |
| Status | Provider event identity/hash |

---

### 22.4 Schema versioning

```json
{
  "schema_name":"market_data.trade",
  "schema_version":2,
  "provider_schema_version":"okx-v5-2026-08-13"
}
```

Compatibility rules:

- Additive optional fields: minor version or same compatible schema version per project policy.
- Field semantic/unit change: major schema version.
- Do not rename/remove without dual-publish/migration window.
- Raw provider field additions should not break parser if core fields valid; preserve unknown fields in `raw_extra` when configured.

---

### 22.5 Storage partitioning

Examples:

```text
trades/provider=okx/market=SPOT/date=YYYY-MM-DD/hour=HH/
candles/provider=okx/market=SWAP/bar=1m/date=YYYY-MM-DD/
books/provider=okx/channel=books/market=SWAP/date=.../
instruments/provider=okx/snapshot_date=.../
```

Do not partition by high-cardinality `instId` alone if it creates tiny files; use compaction strategy.

---

## 23. Observability

### 23.1 REST metrics

```text
okx_rest_requests_total{endpoint,status_class,provider_code}
okx_rest_latency_seconds{endpoint}
okx_rest_rate_limit_wait_seconds{endpoint}
okx_rest_retries_total{endpoint,reason}
okx_rest_payload_bytes{endpoint}
okx_rest_snapshot_regression_total{endpoint,inst_id}
okx_rest_schema_error_total{endpoint}
okx_rest_pagination_pages{endpoint}
okx_rest_pagination_stall_total{endpoint}
```

Avoid `inst_id` label on all high-volume metrics if cardinality too high; use sampled per-instrument diagnostics.

---

### 23.2 WebSocket metrics

```text
okx_ws_connections{service,state,shard}
okx_ws_reconnects_total{service,reason}
okx_ws_last_message_age_seconds{service,shard}
okx_ws_ping_total{service}
okx_ws_pong_timeout_total{service}
okx_ws_ops_total{service,op}
okx_ws_ops_hourly_remaining{service,conn_id}
okx_ws_subscription_desired{service,channel}
okx_ws_subscription_active{service,channel}
okx_ws_subscription_errors_total{service,channel,code}
okx_ws_notice_total{service,code}
okx_ws_payload_bytes{service,channel}
okx_ws_parse_latency_seconds{channel}
```

---

### 23.3 Order book metrics

```text
okx_book_ready{channel,market}
okx_book_sequence_gap_total{channel,market}
okx_book_resync_total{channel,reason}
okx_book_resync_duration_seconds{channel}
okx_book_update_levels{channel,side}
okx_book_depth_levels{channel,side}
okx_book_crossed_total{channel,market,state}
okx_book_publish_latency_seconds{channel}
okx_book_generation{channel,inst_id_sampled}
```

Alert immediately on repeated gaps across many symbols on one connection — likely connection/parser issue.

---

### 23.4 Registry and parser metrics

```text
okx_registry_instrument_count{inst_type,state}
okx_registry_reconcile_diff_total{inst_type,change_type}
okx_registry_unknown_enum_total{field,value}
okx_registry_upcoming_change_total{param}
okx_parser_unknown_field_total{model,field}
okx_parser_invalid_row_length_total{model}
okx_parser_empty_required_field_total{model,field}
okx_sbe_instrument_code_miss_total
```

Unknown field metrics should be sampled/rate-limited to avoid cardinality explosion.

---

### 23.5 Redis/publish metrics

```text
okx_publish_total{tick_type,result}
okx_publish_latency_seconds{tick_type}
okx_publish_payload_bytes{tick_type}
okx_latest_write_total{tick_type,result}
okx_recovery_stream_append_total{tick_type,result}
okx_events_dropped_total{stage,reason}
okx_backpressure_queue_depth{pipeline}
```

No silent drop. Every bounded queue drop has metric and policy.

---

### 23.6 Structured logs

Minimum context:

```json
{
  "provider":"okx",
  "profile":"global",
  "component":"ws_supervisor",
  "service":"public",
  "connection_id":"a4d3ae55",
  "connection_generation":12,
  "channel":"books",
  "instrument_id":"BTC-USDT-SWAP",
  "event":"sequence_gap",
  "expected_prev_seq_id":100,
  "observed_prev_seq_id":105,
  "observed_seq_id":106
}
```

Do not log every market event at INFO.

---

## 24. Kiểm soát bảo mật và vận hành

### 24.1 Public data still needs egress control

- Allowlist configured OKX hosts.
- TLS verification mandatory.
- Do not accept arbitrary URL from request query.
- Resolve DNS normally but monitor unexpected endpoint/certificate changes.
- Secrets absent from anonymous public client.

---

### 24.2 Authenticated optional channels

When SBE/economic calendar requires login:

- Load key/secret/passphrase from secret manager.
- Separate authenticated connection class.
- Never include signature/prehash in logs.
- API key read permission only where possible; no trade/withdraw permission for data-layer market feed.
- Pin environment/profile to prevent demo/prod credential mix.

---

### 24.3 Internal controls

- Manual subscription endpoints require operator role.
- Batch history requests have quotas and max ranges.
- Capability probes cannot be triggered unbounded by public callers.
- Raw payload capture is disabled by default and TTL/size-limited.

---

## 25. Test strategy và mandatory fixtures

### 25.1 Unit tests: REST envelope

Test:

- `code="0"` valid.
- HTTP 200 + nonzero `code` raises typed API error.
- HTTP errors.
- Invalid JSON.
- Missing/non-list `data`.
- Empty data valid.
- Decimal/empty parsing.
- Unknown fields/enums.

---

### 25.2 Endpoint parser fixtures

Fixture cho từng endpoint/channel core:

```text
ticker_spot.json
ticker_swap.json
books_rest.json
books_rpi_rest.json
books_full_rest.json
candle_trade.json
trade_rest.json
history_trade.json
instruments_spot.json
instruments_swap.json
instruments_option.json
instruments_events.json
funding_rate.json
funding_history.json
open_interest.json
price_limit_disabled.json
mark_price.json
index_ticker.json
index_candle.json
mark_candle.json
index_components.json
insurance_fund_deprecated_fields.json
status.json
```

Use sanitized captured payloads plus synthetic edge cases.

---

### 25.3 Pagination tests

- `after` moves older.
- `before` moves newer.
- Boundary overlap dedup.
- Response newest-first converted ascending.
- Cursor no-progress.
- Empty page.
- Request limit > endpoint max triggers pagination, not silent truncation.
- History retention returns partial coverage.
- `history-trades type=2` does not send unsupported `before`.

---

### 25.4 Candle tests

- `1m` versus `1M` case sensitivity.
- Standard versus `utc` bars.
- `confirm 0 -> 0 -> 1` revisions.
- No duplicate close event.
- Volume units spot/derivatives.
- 1s unavailable for option.
- Forward-adjustment metadata and volume treatment.
- No-trade bar behavior.

---

### 25.5 Trade tests

- Taker side preserved.
- `source=0/1` preserved.
- Aggregate `count>1` not exploded into fake atomic sizes.
- `seqId` duplicate accepted with distinct trade composite.
- `trades-all` atomic dedup.
- No double count when both channels enabled.

---

### 25.6 Order-book golden tests

1. Initial snapshot.
2. Insert new level.
3. Update quantity.
4. Delete quantity zero.
5. Multi-level/both-side atomic update.
6. Empty keepalive `seq==prev==last`.
7. Normal gap.
8. Valid sequence reset where `seq < prev` but chain matches.
9. Message before snapshot.
10. Stale old connection generation.
11. RPI total/non-RPI derivation.
12. Invalid non-RPI > total.
13. Snapshot-only `books5`/`bbo-tbt` replacement.
14. Crossed book in pre-open accepted.
15. Persistent cross in live normal flagged.
16. Checksum nonzero/zero ignored according to deprecation policy.
17. Resync waits for WS snapshot; REST cannot mark ready.

Golden vectors must run against Python and Rust book cores.

---

### 25.7 WS protocol tests

- Ping literal/pong.
- Idle timer reset on any message.
- Ack per arg.
- Orphan ack.
- Error `60012`, `64002`, `64003`, `64004`.
- Notice `64008` make-before-break.
- 64KB batching.
- 480/hour soft guard.
- 3 connection/s guard.
- Public/business routing.
- Reconnect jitter.
- Desired subscription reconciliation.
- Less-than-configured deep books per connection.

---

### 25.8 Registry tests

- Exact/legacy alias resolution.
- Derivative ambiguity rejected.
- New listing.
- Suspension/preopen/rebase/settling/expired.
- Instrument disappears from REST: tombstone/grace logic.
- Tick/min/max size updates.
- `upcChg` effective-time handling.
- Unknown enum fail-safe.
- `alias` ignored as authoritative maturity.
- Production/demo `instIdCode` separation.

---

### 25.9 Integration/smoke tests

Against configured non-critical environment/profile:

```text
REST time
REST status
REST instruments SPOT
REST ticker BTC-USDT
REST candles BTC-USDT 1m
WS public tickers
WS public trades
WS public books snapshot
WS business candle1m
WS business trades-all
```

Deep books/SBE smoke only when tier/credentials configured.

Smoke tests must not assume a specific instrument exists forever; pick from registry by criteria, with BTC-USDT as preferred fallback.

---

### 25.10 Chaos tests

- Kill socket mid-frame.
- Delay Redis.
- Drop one incremental book frame.
- Duplicate frames.
- Reorder non-sequenced frames.
- REST cache regression.
- Provider 50011 burst.
- DNS failure.
- Capability disappears.
- Notice-driven reconnect.
- Parser receives new unknown field/enum.

Acceptance: no invalid book marked ready, no silent loss without metric, bounded recovery.

---
## 26. Implementation roadmap theo ưu tiên

### P0 — Sửa correctness của adapter hiện tại

Deliverables:

1. Fix pagination semantics `after`/`before`.
2. Add exact window filter + dedup + pagination khi `limit > 300`.
3. Replace hard-coded base URL bằng settings/profile.
4. Replace sync `requests` in async path bằng shared async client.
5. Parse numeric/timestamp/candle confirm typed.
6. Add provider envelope error classes.
7. Preserve existing `/v1/crypto/ohlcv/okx/...` compatibility.
8. Tests cho start/end/limit/bar case sensitivity.

Acceptance:

- Request `limit=500` returns up to 500 through pages, not silently 300.
- `[start,end]` exact filter correct.
- No direct string heuristic for derivatives.
- Existing consumer contract still works.

---

### P1 — Registry + core snapshot/live feeds

Deliverables:

- `/public/instruments` bootstrap/reconcile.
- REST ticker, trades, candles, time/status.
- WS public ticker/trades/instruments.
- WS business candles/trades-all.
- Canonical event envelope v1/v2.
- Provider/market namespaced Redis keys.
- Health/capabilities.

Acceptance:

- Spot and swap universe resolved from registry.
- No direct OKX connection from alpha/trading containers.
- Restart recovers latest state then live subscription.
- `trades` and `trades-all` cannot be confused/double-counted.

---

### P2 — Order books production-grade

Deliverables:

- REST books/books-rpi/books-full.
- WS `bbo-tbt`, `books5`, `books`, `books-rpi`.
- Pure deterministic book engine.
- Sequence/gap/reset/keepalive logic.
- Book generation/stale semantics.
- Durable bounded recovery stream/top-N publishing.

Acceptance:

- Any gap invalidates state.
- Fresh WS snapshot required to become ready.
- Checksum ignored.
- Crossed pre-open book accepted.
- Golden tests pass Python backend.

---

### P3 — Derivatives/public reference suite

Deliverables:

- Mark/index price and candles.
- Funding current/history.
- Open interest.
- Price limit.
- Estimated settlement.
- Delivery/exercise history.
- Underlying, tiers, security fund, index components.
- Liquidation samples and ADL warning.

Acceptance:

- Funding interval derived, not hard-coded.
- Units/contracts converted only with registry metadata.
- Risk consumers distinguish last/mark/index.
- ADL deprecated fields do not break parser.

---

### P4 — Advanced/tier/profile products

Deliverables:

- 10 ms books where tier allows.
- Options tick bands/summary/trades.
- Event contract domain.
- Bulk historical pipeline.
- Economic calendar authenticated channel.
- Rust order-book core.
- SBE shadow then primary rollout.

Acceptance:

- Capability-gated and profile-tested.
- JSON fallback remains available.
- SBE unknown schema fails closed.
- Python/Rust canonical output parity.

---

<a id="okx-program-phase-map"></a>

### 26.1 Mapping OKX P0-P4 vào bảy phase chương trình

Mapping này là thứ tự bắt buộc để OKX không tạo một kiến trúc riêng bên cạnh platform chung.

| Program phase | OKX scope | Exit evidence riêng cho OKX |
|---:|---|---|
| 0 | Inventory V1, capture fixtures/profile, freeze compatibility, characterize pagination defect và provider budget | V1 golden artifacts, capability/profile record, bounded live baseline, pagination fixture failing for the known reason |
| 1 | Canonical identity/contracts, authoritative instrument registry, region/entity capability manifest, async role boundaries | Spot/swap/futures/options/event identity tests; no derivative symbol heuristic; API replicas open no OKX connection |
| 2 | Raw/provider/canonical fixtures on durable backbone; deterministic simulator; cross-language decimal/time/event-ID parity | Replay checksum and Python/Rust codec parity on REST/WS/order-book frames |
| 3 | P0/P1 live adapter plus P2 order-book machine: async REST, public/business WS, demand leases, sequence/gap/reset/keepalive, V1 projector | Shadow parity, reconnect/gap/lease fencing tests, zero silent loss, exact V1 compatibility |
| 4 | P3 history/reference suite, pagination/reconciliation, coverage contracts, raw lineage and gap-free warmup-to-live | Exact `[start,end]`, no-progress/dedup, OI partial coverage, funding/mark/index provenance and snapshot-cursor tests |
| 5 | Provider-neutral V2/SDK and controlled consumer manifests; provider diagnostics remain internal | OpenAPI/SDK tests, V1/V2 value parity, alpha/Trading System shadow consumer recovery |
| 6 | P4 capability-gated products, chaos/load/security certification; optional Rust order-book and SBE shadow promotion | Profile/tier matrix, JSON fallback, SBE fail-closed/parity, production SLO and rollback evidence |

<a id="okx-program-phase-0"></a>

#### OKX workstream for program Phase 0

- Capture current `/v1/crypto/ohlcv/okx/...` rows and SDK behavior before correction.
- Add deterministic fixtures for cursor overlap, cache regression, malformed envelope, profile host and exact native `instId`.
- Record public/business WS limits, REST endpoint buckets and regional capability assumptions with verification date.
- Characterize the current `start_time/end_time` bug without silently changing consumer behavior before the implementation slice is approved.

<a id="okx-program-phase-1"></a>

#### OKX workstream for program Phase 1

- Generate canonical contracts for ticker, trade, bar, BBO/book, funding, OI, mark/index, instrument and feed state.
- Bootstrap/reconcile `/public/instruments`; preserve exact `instId`, `instFamily`, expiry, strike, option type, event contract fields, contract multiplier and tick/lot rules.
- Implement region/entity profile plus capability manifest. Unsupported/tier-gated endpoint is an explicit capability result, not startup success guessed from one call.
- Keep provider-specific routes internal; expose canonical identity through the platform V2 contract only.

<a id="okx-program-phase-2"></a>

#### OKX workstream for program Phase 2

- Put captured REST/WS/provider typed frames into the deterministic simulator and durable raw/canonical test path.
- Include book snapshot/update/gap/keepalive/maintenance-reset and connection-generation fixtures.
- Prove exact decimal, source/receive timestamp, event ID and instrument identity parity across generated Python/Rust types before implementing a Rust OKX hot path.

<a id="okx-program-phase-3"></a>

#### OKX workstream for program Phase 3

- Replace synchronous candle calls with endpoint-bucketed async transport, correct pagination facade and typed errors.
- Implement public/business WS supervisors, demand-backed shards, heartbeat, reconnect, subscription ack correlation and provider profile fencing.
- Implement the deterministic book state machine from Phần 11. A true sequence gap invalidates executable state and requires a fresh WS snapshot; REST book data never bridges missing deltas.
- Implement P0/P1 and required P2 feeds in shadow, project canonical events to V1, then promote only certified feed slices.

<a id="okx-program-phase-4"></a>

#### OKX workstream for program Phase 4

- Implement exact historical windows, overlap dedup, no-progress guards, latest/history reconciliation and source timestamp regression guards.
- Add funding, mark/index, price-limit and OI coverage with exact units/provenance. OI history starts from governed ingestion unless an approved authoritative historical source exists.
- Certify gap-free warmup-to-live handoff using durable cursor/watermark; report partial/unsupported coverage instead of fabricating data.

<a id="okx-program-phase-5"></a>

#### OKX workstream for program Phase 5

- Serve OKX through provider-neutral snapshot/warmup/batch/stream contracts and generated SDK V2.
- Keep capability/status/subscription reconciliation under authenticated internal control-plane routes.
- Migrate only declared consumers; preserve `/v1/crypto/ohlcv/okx/...`, SDK V1 and legacy Redis shape/source semantics until their governed sunset.

<a id="okx-program-phase-6"></a>

#### OKX workstream for program Phase 6

- Run provider-profile, rate-limit, reconnect storm, malformed frame, maintenance notice, delist, order-book gap, Redis rebuild, durable replay and sustained-load certification.
- Certify P4 products individually. Tier/profile endpoint unavailability must not degrade unrelated core feeds.
- Promote Rust book core or SBE only after JSON shadow parity, schema/version pinning, unknown-schema fail-closed behavior and tested JSON rollback.
- Store compact evidence and clean every fixture topic/group/key/container after certification.

---

## 27. Agent implementation workflow

### 27.1 Before writing code

Agent PHẢI:

1. Read this guide.
2. Read current `app/providers/okx/rest.py` and related API/service/schema tests.
3. Read current OKX docs section for every endpoint/channel being touched.
4. Read OKX changelog entries after the guide’s verification date.
5. Confirm deployment `OKX_REGION_PROFILE` and hostname.
6. Determine whether capability is core, optional, auth/tier-dependent or deprecated.
7. State affected canonical schema/Redis/internal API versions.

Do not begin by adding dozens of untyped methods to existing `rest.py`.

---

### 27.2 Endpoint implementation template

For every REST endpoint, PR must include:

```text
[ ] path constant
[ ] request model + validation
[ ] rate-limit bucket config
[ ] response provider model
[ ] parser with Decimal/empty/unknown handling
[ ] canonical mapper
[ ] unit semantics
[ ] freshness/latest policy
[ ] retry/error classification
[ ] fixture + tests
[ ] capability manifest entry
[ ] internal route only if consumer needs it
[ ] docs link/changelog note in code comment or provider registry
```

For every WS channel:

```text
[ ] exact public/business/SBE service
[ ] subscription arg model
[ ] ack/error routing
[ ] push parser
[ ] tick type
[ ] dedup/ordering model
[ ] reconnect/top-up behavior
[ ] freshness SLA
[ ] Redis channel/latest key
[ ] fixture + protocol/reconnect test
[ ] capability/tier fallback
```

---

### 27.3 Decision tree: choose REST or WS

```text
Need continuous low-latency changes?
  yes -> WS
  no  -> REST

Need initial full instrument universe?
  -> REST instruments, then WS incremental

Need initial incremental book state?
  -> subscribe WS and wait WS snapshot

Need recent candles/history?
  -> REST, then WS for current updates

Need exact atomic tape?
  -> WS trades-all

Need low-bandwidth aggregate tape?
  -> WS trades

Need current single ticker during diagnostics?
  -> REST ticker

Need ongoing ticker universe?
  -> WS tickers per subscribed instrument; REST tickers for bootstrap
```

---

### 27.4 Parser rules

Agent MUST NOT:

- Cast all numeric strings to float.
- Treat empty string as zero.
- Assume all array rows have same shape across endpoints.
- Drop unknown fields before schema drift can be observed.
- Use index `2` of normal book row for business logic.
- Verify deprecated checksum.
- Assume timestamp order equals arrival order.
- Assume all `side` fields mean maker side.
- Assume volume unit from field name alone.

Agent MUST:

- Validate row length.
- Preserve raw enum/field extension.
- Attach instrument spec version.
- Record source/receive timestamps.
- Quarantine malformed payload with bounded raw capture.

---

### 27.5 Review red flags

Reject PR when it contains:

```python
float(payload["px"])
```

```python
assert seq_id > prev_seq_id
```

```python
if checksum != calculate_checksum(...):
```

```python
params["after"] = start_time
params["before"] = end_time
```

```python
inst_id = symbol.replace("USDT", "-USDT")
```

for derivatives/general symbol resolution.

Reject also:

- Hard-coded `www.okx.com` inside endpoint methods.
- New direct provider connection inside alpha/trading service.
- `trades` published as atomic fills.
- REST book marked as recovered WS incremental state.
- Deep/SBE channel enabled without capability guard.
- Generic Redis key that hides provider/market.

---

### 27.6 Minimal code-review evidence

A PR should show:

- Docs section and verification date.
- Fixture from valid response shape.
- Rate-limit rule.
- Unit semantics.
- Failure/recovery path.
- Backward compatibility impact.
- Test results, including edge case.
- Metrics added.

---

## 28. Endpoint inventory matrix

### 28.1 Market Data REST

| Capability | HTTP endpoint | Priority | Canonical output |
|---|---|---:|---|
| All tickers | `GET /api/v5/market/tickers` | P1 | `ticker[]` |
| Single ticker | `GET /api/v5/market/ticker` | P1 | `ticker` |
| Standard book | `GET /api/v5/market/books` | P2 | `book_snapshot` |
| RPI book | `GET /api/v5/market/books-rpi` | P2 | `book_rpi_snapshot` |
| Full book | `GET /api/v5/market/books-full` | P2/P3 | deep `book_snapshot` |
| Latest candles | `GET /api/v5/market/candles` | P0 | trade candle |
| Historical candles | `GET /api/v5/market/history-candles` | P0 | trade candle history |
| Recent trades | `GET /api/v5/market/trades` | P1 | recent trade records |
| Historical trades | `GET /api/v5/market/history-trades` | P1/P3 | trade history |
| Platform 24h volume | `GET /api/v5/market/platform-24-volume` | Optional | platform aggregate |
| Exchange rate | `GET /api/v5/market/exchange-rate` | Optional | reference FX |
| Index tickers | `GET /api/v5/market/index-tickers` | P3 | `index_price` |
| Index candles | `GET /api/v5/market/index-candles` | P3 | index candle |
| Index candle history | `GET /api/v5/market/history-index-candles` | P3 | index history |
| Mark candles | `GET /api/v5/market/mark-price-candles` | P3 | mark candle |
| Mark candle history | `GET /api/v5/market/history-mark-price-candles` | P3 | mark history |
| Index components | `GET /api/v5/market/index-components` | P3 | index composition |
| Option family trades | profile endpoint | P4 | option trade domain |
| Open oracle | legacy/offline | Forbidden | none |

---

### 28.2 Public Data REST

| Capability | HTTP endpoint | Priority | Notes |
|---|---|---:|---|
| Instruments | `GET /api/v5/public/instruments` | P1 | Authoritative registry |
| Estimated delivery/exercise | `GET /api/v5/public/estimated-price` | P3 | Settlement window |
| Delivery/exercise history | `GET /api/v5/public/delivery-exercise-history` | P3 | Retention limited |
| Funding current | `GET /api/v5/public/funding-rate` | P3 | Interval dynamic |
| Funding history | `GET /api/v5/public/funding-rate-history` | P3 | Up to ~3 months |
| Open interest | `GET /api/v5/public/open-interest` | P3 | Preserve contracts/ccy/USD |
| Price limit | `GET /api/v5/public/price-limit` | P3 | Empty when disabled |
| System time | `GET /api/v5/public/time` | P1 | Clock offset |
| Mark price | `GET /api/v5/public/mark-price` | P3 | Price type mark |
| Position tiers | `GET /api/v5/public/position-tiers` | P3 | Risk enrichment |
| Interest rate/loan quota | `GET /api/v5/public/interest-rate-loan-quota` | Separate private | Requires auth; not anonymous client |
| Underlying | `GET /api/v5/public/underlying` | P3 | Family discovery |
| Security fund | `GET /api/v5/public/insurance-fund` | P3 | Deprecated fields tolerated |
| Instrument tick bands | profile/current endpoint | P4 | Exact option/event tick validation |
| Option summary/trades | profile endpoints | P4 | Capability-gated |
| Historical bulk market data | profile endpoint | P4 | Separate archival workflow |
| Economic calendar | profile endpoint | Optional | May require auth/VIP |

---

### 28.3 Status REST

| Capability | Endpoint | Priority |
|---|---|---:|
| Maintenance/status | `GET /api/v5/system/status` | P1 |

---

### 28.4 Market Data WS

| Channel | Service | Priority | Tick type/state |
|---|---|---:|---|
| `tickers` | public | P1 | `ticker` |
| `candle*` | business | P1 | `candle_update/close` |
| `trades` | public | P1 | `trade_agg` |
| `trades-all` | business | P1 | `trade_atomic` |
| `bbo-tbt` | public | P2 | BBO replace snapshot |
| `books5` | public | P2 | 5-level replace snapshot |
| `books` | public | P2 | Stateful 400 depth |
| `books-rpi` | public | P2 | Stateful consolidated RPI |
| `books50-l2-tbt` | public | P4 | Stateful 10 ms, VIP4+ |
| `books-l2-tbt` | public | P4 | Stateful 10 ms, VIP4+ |
| `books-elp` | public | Legacy | Deprecated; migrate RPI |

---

### 28.5 Public Data WS

| Channel | Service | Priority | Output |
|---|---|---:|---|
| `instruments` | public | P1 | Registry changes |
| `open-interest` | public | P3 | OI |
| `funding-rate` | public | P3 | Funding |
| `price-limit` | public | P3 | Limits |
| `estimated-price` | public | P3 | Settlement estimate |
| `mark-price` | public | P3 | Mark price |
| `index-tickers` | public | P3 | Index price |
| `mark-price-candle*` | business | P3 | Mark candles |
| `index-candle*` | business | P3 | Index candles |
| `liquidation-orders` | public | P3 | Liquidation sample |
| `adl-warning` | public | P3 | Warning/ADL only |
| `status` | public | P1 | Maintenance status |
| Economic calendar | business/auth | Optional | Macro event |

---

## 29. Bar/channel compatibility matrix

### 29.1 Trade candle REST

```text
Latest /market/candles:
  1m 3m 5m 15m 30m 1H 2H 4H
  6H 12H 1D 2D 3D 1W 1M 3M
  6Hutc 12Hutc 1Dutc 2Dutc 3Dutc 1Wutc 1Mutc 3Mutc

History /market/history-candles:
  includes 1s plus the above
  1s retention about 3 months
  1s unsupported for OPTION
```

### 29.2 Trade candle WS

Channel prefix `candle`; docs may expose a broader channel set than REST, including product/profile-dependent `5D` and UTC calendar variants. Maintain an allowlist generated from current docs, not from REST bar list.

### 29.3 Mark/index candles

Mark/index REST limits are typically `100` per request, unlike trade candles max `300`. Their supported calendar bars are not necessarily identical to trade candles. Define separate enums:

```python
TradeCandleBar
IndexCandleBar
MarkPriceCandleBar
```

Do not reuse one enum unless it represents the intersection and the caller explicitly accepts restrictions.

---

## 30. End-to-end usage flows

### 30.1 Spot alpha: warmup + live candles/trades

```text
Alpha starts
  -> GET data_layer /health
  -> GET OKX capability/source-role state
  -> request 500 trade candles through internal /v2
  -> data_layer paginates OKX REST and returns ascending bars
  -> alpha loads latest trade/ticker state
  -> alpha subscribes Redis candle/trade stream
  -> alpha validates source=fallback/reference according to policy
  -> current candle revisions applied until confirm=1
```

Alpha does not create OKX WebSocket.

---

### 30.2 Swap risk service

```text
Registry spec
  + mark price
  + index price
  + funding rate/times
  + open interest
  + position tiers
  -> normalized risk snapshot
```

Fields must retain `price_type`, contracts and conversion provenance.

---

### 30.3 Order-book consumer restart

```text
Consumer restarts
  -> GET latest book from data_layer
  -> require ready=true, stale=false, generation=N
  -> subscribe book Pub/Sub/Stream
  -> discard event with generation < N
  -> apply deltas with sequence check if consumer maintains state
  -> on notification gap, refetch latest valid state or recovery stream
```

If data-layer itself has a provider sequence gap, it invalidates book and waits a new WS snapshot.

---

### 30.4 Options service

```text
Discover underlying/families
  -> fetch instruments per family
  -> fetch tick bands
  -> subscribe instruments changes
  -> ingest option trades/summary/mark/OI
  -> delivery/exercise history reconciliation
```

No option `instId` generation from strike/date strings without registry validation.

---

### 30.5 Provider service-upgrade notice

```text
connection A receives notice 64008
  -> supervisor marks A draining
  -> opens B
  -> resubscribes general channels
  -> book subscriptions wait snapshots on B
  -> active generation switches B
  -> A closes
```

Business candle/trades-all supervisor must also support this notice.

---

## 31. Các thay đổi hiện hành đã được phản ánh trong guide

Tại ngày đối chiếu, implementation PHẢI xử lý các thay đổi sau:

1. **Checksum order book đã bị deprecate:** field `checksum` vẫn tồn tại nhưng luôn bằng `0`; dùng `seqId/prevSeqId` để kiểm tra continuity.
2. **Thông báo nâng cấp Business WS:** mã `64008` áp dụng cả cho `/business`, không chỉ public/private.
3. **Global REST domain:** `openapi.okx.com` đã khả dụng và được khuyến nghị cho Global REST; vẫn phải giữ regional host dạng cấu hình.
4. **Migration RPI:** `books-rpi` thay thế `books-elp`; tên ELP legacy có lịch sunset ngày 2026-10-31 ở profile/changelog áp dụng.
5. **SBE:** được ra mắt ngày 2025-11-06; yêu cầu truy cập đã thay đổi trong 2026, gồm VIP4 cho SBE trades/deep book và yêu cầu login cho SBE BBO.
6. **Security fund/ADL:** không còn push ADL ở trạng thái `normal`; nhiều field/type đã deprecate hoặc trả rỗng.
7. **Instrument tiếp tục thay đổi:** state, rule type, category, upcoming change, X-Perp và event products buộc parser phải forward-compatible.

### 31.1 Quy trình rà soát changelog

Trước mỗi release:

```text
1. Đọc OKX API changelog kể từ ngày pin gần nhất.
2. Lọc các thay đổi thuộc Market Data, Public Data, Status, WS, SBE và instruments.
3. Cập nhật registry capability/rate-limit/bar/channel.
4. Thêm hoặc sửa fixture.
5. Chạy contract tests.
6. Ghi ngày xác minh mới và source hash vào guide/provider metadata.
```

Không được coi danh sách method của một SDK cũ là API contract hiện hành.

---

## 32. Definition of Done — điều kiện hoàn thành

Tích hợp OKX market data chỉ được coi là production-ready khi:

- [ ] Đã sửa lỗi pagination candle hiện tại.
- [ ] Base URL được cấu hình theo profile.
- [ ] Có REST client async dùng connection pool.
- [ ] Instrument registry là nguồn authoritative và có version.
- [ ] Unit của spot và derivatives được mô tả tường minh.
- [ ] Giá trị số không dùng `float` cho contract chính xác.
- [ ] REST snapshot cũ hơn không thể ghi đè state mới hơn.
- [ ] Public WS và Business WS được tách supervisor/connection.
- [ ] Heartbeat, giới hạn operation và payload được enforce.
- [ ] Proactive reconnect theo `64008` hoạt động cho cả public và business.
- [ ] `trades` và `trades-all` có contract khác nhau.
- [ ] Stateful books vượt qua test snapshot/delta/gap/reset.
- [ ] Không dùng checksum để kiểm tra continuity.
- [ ] Giữ đúng semantics của `books-rpi`.
- [ ] Channel phụ thuộc tier/profile fail gracefully.
- [ ] Schema candle/mark/index/funding/OI được tách riêng khi semantics khác nhau.
- [ ] Redis key/channel chứa provider và market identity.
- [ ] Consumer legacy có compatibility path được kiểm soát.
- [ ] Source role và fallback eligibility được khai báo tường minh.
- [ ] Metrics/alerts bao phủ gap, reconnect, schema drift và provider rate limit.
- [ ] Toàn bộ unit/integration/chaos test suite chạy đạt.
- [ ] Checklist agent/code review không còn red flag.
- [ ] Changelog đã được rà soát tại ngày release.

---

## 33. Nguồn tham chiếu chính

- OKX API v5 documentation: <https://www.okx.com/docs-v5/en/>
- OKX API v5 changelog: <https://www.okx.com/docs-v5/log_en/>
- Phải đối chiếu thêm regional/entity docs tương ứng với profile đã cấu hình.
- Repository mục tiêu: <https://github.com/BobbyAxerol/quant-data-layer>
- Baseline adapter OKX hiện tại: `app/providers/okx/rest.py`
- Contract service nội bộ hiện tại: `DATA_LAYER_SERVICE_ACCESS_GUIDE.md`

> **Lưu ý pin phiên bản:** API docs là tài liệu sống. Guide này được đối chiếu với docs/changelog khả dụng vào **2026-08-13**. Mọi implementation sau ngày đó phải rà soát lại changelog và cập nhật metadata xác minh.
