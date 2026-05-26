# Data Layer Service Integration Guide (Production)

This document provides technical details on how to integrate production services (like Alpha strategies or Paper trading engines) with the `data_layer` infrastructure.

## Standard

Other services should connect to `data_layer` using this rule:

- use Redis Pub/Sub for live streaming consumption
- use REST API for warmup, latest-state recovery, diagnostics, and manual triggers
- use the official SDK client where possible: `app.sdk.DataLayerClient`
- do not connect directly to Binance, DNSE, vnstock, or other providers if the service is already inside `bobby_network`

The `data_layer` is the system-of-record gateway for market-data distribution inside this stack.

## 1. Connection Architecture

Production services should **never** connect directly to external exchanges (Binance, DNSE, etc.) if they are running within the `bobby_network`. Instead, they must use the `data_layer` as a unified gateway.

### Network Configuration
All services must be on the same Docker network:
```yaml
networks:
  - bobby_network
```

### Hostnames & Ports
| Service | Internal Hostname | Port | Purpose |
|---------|-------------------|------|---------|
| **Data Layer API** | `data_layer` | 8100 | Warmup data, health checks, manual triggers |
| **Redis** | `redis_service` | 6379 | Real-time Pub/Sub streams, current state cache |

### Service Startup Expectations

The `data_layer` service starts these runtime responsibilities during FastAPI lifespan:

- Binance live trade stream for execution and papertrade consumers
- Binance `1m` kline stream for candle-based and alpha consumers
- DNSE live VN stream as primary VN market source
- vnstock polling as VN fallback source
- preload watchdog for VN historical warmup refresh

Downstream services should assume:

- live price channels may come online before preload refresh finishes
- preload data may be available slightly after service boot
- VN live quote keys may expire after market close, while last-snapshot keys remain available

### Official SDK Client

Use this client from downstream Python services when the service mounts or vendors the `data_layer` package:

```python
from app.sdk import DataLayerClient

client = DataLayerClient(
    base_url="http://data_layer:8100",
    redis_host="redis_service",
    redis_port=6379,
    redis_db=2,
)

health = client.health()
btc_trade = client.latest_trade("binance", "BTCUSDT")
btc_kline = client.latest_kline("binance", "BTCUSDT", interval="1m")
fpt_warmup = client.warmup_ohlcv("vn_stock", "FPT", interval="5m", limit=500)
fallback = client.fallback_status("BTCUSDT", interval="1m")
```

SDK responsibilities:

- `health()`
- `stream_health()`
- `latest_trade(provider, symbol)`
- `latest_kline(provider, symbol, interval)`
- `latest_vn_quote(symbol, allow_last_snapshot=True)`
- `warmup_ohlcv(market, symbol, interval, limit, provider=None)`
- `fallback_status(symbol, interval)`
- `fallback_reference(symbol, interval, force=False)`
- `stream_trades(symbols)`
- `stream_klines(symbols, interval)`
- `stream_vn_quotes(symbols)`
- `validate_freshness(payload, max_age_seconds)`
- `validate_source(payload, allowed_sources)`

If a service cannot import the SDK, it must still follow the exact REST and Redis contracts documented below.

### Required Startup And Recovery Sequences

Alpha startup sequence:

1. Call `client.health()` and require `status=ok`.
2. Load the symbol universe from your alpha/trading-system config; do not create ad-hoc direct provider streams.
3. For crypto candle alpha, call `client.warmup_ohlcv("crypto", symbol, interval=..., limit=..., provider="binance")`.
4. For VN alpha, call `client.warmup_ohlcv("vn_stock", symbol, interval=..., limit=...)`.
5. Fetch latest live state with `latest_trade()`, `latest_kline()`, or `latest_vn_quote()`.
6. Validate `source` and freshness before enabling trading decisions.
7. Subscribe to Redis streams through `stream_trades()`, `stream_klines()`, or `stream_vn_quotes()`.

Paper/live execution startup sequence:

1. Call `health()` and `stream_health()`.
2. Load authoritative trading symbols from trading-system config.
3. For every active symbol, recover the latest state from REST before subscribing to Redis.
4. Reject or pause trading if the required live source is missing, stale, or only a non-authoritative fallback.
5. Start execution only after risk/portfolio services confirm the data source contract they need.

Recovery after restart:

1. Reconnect to REST first.
2. Reload latest state for all active symbols.
3. Rebuild local in-memory bars/marks from warmup plus latest-state endpoints.
4. Reattach Redis subscriptions.
5. Compare the first live message timestamp with the recovered timestamp; if there is a gap, run a small REST warmup/top-up before making the next decision.

Stale-data behavior:

- Binance live trading should treat stale Binance trade/kline data as blocking for execution-grade decisions.
- OKX fallback is reference-only unless a separate risk policy explicitly allows fallback-driven action.
- VN `/v1/vn/quote/{symbol}` is live TTL state; if it is missing after market close, use `/v1/vn/quote-last/{symbol}` only for inspection/recovery, not proof of a live tradable market.
- VN preload may be sparse by real provider data, but it must not be fabricated by downstream services.

---

## 2. Real-time Data Streams (Redis Pub/Sub)

The `data_layer` multiplexes WebSocket connections and publishes updates to Redis. This allows multiple consumers to receive the same data without multiplying external connections.

### Subscription Channels
- **Binance Trade Price**: `stream:trade:{symbol}`
  - Purpose: execution and papertrade latest live price
  - Example: `stream:trade:BTCUSDT`
- **Binance Klines**: `stream:kline:{interval}:{symbol}`
  - Purpose: candle-based alpha, resampling, indicator logic
  - Example: `stream:kline:1m:BTCUSDT`
- **VN Stock Quotes**: `stream:vn:{symbol}`
  - Example: `stream:vn:SSI`

### Redis Key Semantics

These are the current key contracts used by `data_layer`:

- `trade:price:{symbol}`
  - latest Binance trade price snapshot
- `kline:{interval}:{symbol}`
  - latest Binance kline snapshot for that interval
- `vn:quote:{symbol}`
  - latest live VN quote with short TTL
- `vn:quote:last:{symbol}`
  - latest known VN snapshot, available even if market is closed

Use the Redis channels for streaming and the Redis keys or REST endpoints for latest-state recovery.

### Integration Example (Python)
```python
import redis
import orjson

r = redis.Redis(host='redis_service', port=6379, db=2)
pubsub = r.pubsub()
pubsub.subscribe("stream:trade:BTCUSDT")

for message in pubsub.listen():
    if message['type'] == 'message':
        # Data is serialized with orjson (bytes)
        data = orjson.loads(message['data'])
        print(f"Received trade price for {data['symbol']}: {data['price']}")
```

### Recommended Consumer Patterns

- Execution and papertrade:
  - subscribe to `stream:trade:{symbol}`
  - use `GET /v1/binance/price/{symbol}` to recover latest state after reconnect
- Candle-based alpha:
  - subscribe to `stream:kline:{interval}:{symbol}`
  - use `GET /v1/binance/kline/{symbol}?interval=...` for latest candle state
  - use preload endpoints for historical warmup
- VN live consumers:
  - subscribe to `stream:vn:{symbol}`
  - use `GET /v1/vn/quote/{symbol}` only when live TTL state is expected
  - use `GET /v1/vn/quote-last/{symbol}` for last known snapshot, especially after market close

SDK equivalents:

```python
trade_pubsub = client.stream_trades(["BTCUSDT", "ETHUSDT"])
kline_pubsub = client.stream_klines("BTCUSDT", interval="1m")
vn_pubsub = client.stream_vn_quotes(["FPT", "HPG"])
```

---

## 3. Warmup & Historical Data (REST API)

When a service starts, it usually needs "warmup" data (e.g., the last 100-1000 candles).

### Pattern A: Get Latest State
Use this to get the latest live state if the service missed a few updates during boot.
- `GET http://data_layer:8100/v1/binance/price/{symbol}`
- `GET http://data_layer:8100/v1/binance/kline/{symbol}?interval=1m`
- `GET http://data_layer:8100/v1/vn/quote/{symbol}`
- `GET http://data_layer:8100/v1/vn/quote-last/{symbol}`

VN quote semantics:
- `/v1/vn/quote/{symbol}` is live-only and depends on the short TTL cache.
- `/v1/vn/quote-last/{symbol}` returns the latest known snapshot and is suitable for after-hours inspection, diagnostics, and non-live consumers.

### Pattern A.1: Health And Status

Use these endpoints for service-level checks:

- `GET http://data_layer:8100/v1/health`
- `GET http://data_layer:8100/v1/preload/status`

`/v1/preload/status` returns preload inventory and timestamp metadata.
Current response fields include local-market timezone context for VN preload data:

- `timezone_local`
- `first_local`
- `last_local`
- `first_utc`
- `last_utc`

Other services should treat preload timestamps as VN market-local data unless they explicitly consume the UTC fields.

### Pattern B: Historical Warmup (VN Market)
Use this to fetch VN warm-up candles from the latest available candle backwards.
- `GET http://data_layer:8100/v1/preload/{symbol}?limit=1000`

Recommended warmup boot flow:

1. call `/v1/health`
2. call `/v1/preload/{symbol}?limit=...` for VN warmup if needed
3. call latest-state endpoint for the live feed you will consume
4. subscribe to the correct Redis channel

VN preload standard:

- preload is a warm-up interface, not a generic historical query interface
- callers should request a `limit` of candles
- the response is the latest available `N` candles in ascending time order
- callers should not send `start_date` or `end_date`
- if a service needs arbitrary historical slicing, that should be implemented as a different endpoint or a separate historical service

### Recommended Usage Split
- Execution and papertrade should use `stream:trade:{symbol}` or `GET /v1/binance/price/{symbol}` for latest live price.
- Alpha and candle-based strategies should use `stream:kline:{interval}:{symbol}` plus preload parquet warmup.
- Preload is interval history for warmup and signal generation, not the primary live execution feed.
- VN services that require after-hours visibility should read `/v1/vn/quote-last/{symbol}` rather than assuming `/v1/vn/quote/{symbol}` is always present.

### Warmup Contract For Other Services

If another service implements VN warm-up logic against `data_layer`, use this exact contract:

- input:
  - `symbol`
  - `limit`
- output:
  - latest warm-up candles from newest backwards, returned sorted ascending by `time`
- business meaning:
  - "give me the most recent lookback window I need before live trading starts"

This is the standard expected by `data_layer`.
Do not design new consumers around date-window warm-up requests for VN preload.

### Pattern C: Historical Proxy (Binance)
Acts as a rate-limited proxy to Binance API for historical klines.
- `GET http://data_layer:8100/v1/binance/klines/{symbol}?interval=1m&limit=1000`

### Pattern D: Crypto Provider Warmup And Fallback

Crypto history should be requested through `data_layer`, not directly from provider APIs:

- Binance:
  - `GET http://data_layer:8100/v1/crypto/ohlcv/binance/BTCUSDT?interval=15m&limit=500&market=spot`
- OKX:
  - `GET http://data_layer:8100/v1/crypto/ohlcv/okx/BTCUSDT?interval=15m&limit=300`

Fallback semantics:

- `GET http://data_layer:8100/v1/fallback/crypto/status/BTCUSDT?interval=1m`
- `GET http://data_layer:8100/v1/fallback/crypto/reference/BTCUSDT?interval=1m&force=false`

OKX fallback response is explicitly non-authoritative:

- `provider=okx`
- `venue=OKX`
- `reference_for=BINANCE`
- `authoritative=false`
- `fallback_reason=<reason>`

Execution/risk services must not use OKX fallback as Binance fill price unless a separate policy explicitly allows it. For live Binance trading, fallback should normally block trading, alert, or provide conservative reference context.

---

## 4. Parse Standard For Other Services

Other services should match the normalized payload contracts used in:

- [feed_parsers.py](/root/bobby/data_layer/app/stream/feed_parsers.py)

This matters because downstream services should consume normalized data fields, not provider-native raw payload shapes, unless they explicitly need raw exchange fields.

### Binance Kline Parse Standard

Expected normalized fields:

- `symbol`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `timestamp`
- `is_closed`
- `raw`

If a downstream service parses Binance kline payloads itself, it should follow the same semantic mapping as `data_layer`:

- `symbol` from exchange symbol
- OHLCV from the kline body
- `timestamp` from kline open time
- `is_closed` from candle close flag
- preserve `raw` when full exchange payload is still needed

### VN Quote Parse Standard

Expected normalized fields for VN quote-like live data:

- `symbol`
- `price`
- `quantity` when available
- `open`
- `high`
- `low`
- `source`
- `timestamp`
- `raw` if the service keeps provider-native payload

### Binance Trade Parse Standard

Expected normalized fields for live execution price:

- `symbol`
- `price`
- `quantity`
- `trade_id`
- `event_time`
- `trade_time`
- `side`
- `source`
- `raw`

### Implementation Rule For Agents And Services

If you are implementing another service that connects to `data_layer`, follow these rules:

1. Subscribe to normalized channels first.
2. Build business logic on normalized fields like `price`, `close`, `timestamp`, and `is_closed`.
3. Use `raw` only for provider-specific edge cases.
4. Do not assume all feeds share the same shape.
5. Keep your local parser compatible with `data_layer` if you need fallback parsing.

### Recommended Local Consumer Shapes

Execution or papertrade service:

- consume `stream:trade:{symbol}`
- expect fields:
  - `symbol`
  - `price`
  - `quantity`
  - `event_time`
  - `trade_time`
  - `side`

Candle alpha service:

- consume `stream:kline:{interval}:{symbol}`
- expect provider-shaped Binance kline payload or normalize it locally into:
  - `symbol`
  - `open`
  - `high`
  - `low`
  - `close`
  - `volume`
  - `timestamp`
  - `is_closed`

VN live service:

- consume `stream:vn:{symbol}`
- expect quote-style fields:
  - `symbol`
  - `price`
  - `source`
  - `timestamp`

If another service wants strict consistency, it should mirror the `data_layer` parser logic instead of inventing a new field contract.

---

## 5. Performance & Reliability Notes

1. **Serialization**: The `data_layer` uses `orjson` for all Redis payloads to ensure high throughput and low latency. Ensure your clients also use `orjson` or handle the bytes correctly.
2. **Backpressure**: Real-time streams are Pub/Sub. If your service lags, it will miss messages. Use the REST API to "fill the gaps" if a disconnect is detected.
3. **Internal Routing**: Always use the service names (`data_layer`, `redis_service`) instead of hardcoded IPs. This ensures compatibility across different environments (dev/prod).
4. **Error Handling**: Implement exponential backoff for REST calls and automatic reconnection for Redis Pub/Sub.
5. **Live vs Snapshot**: Treat live TTL keys and channels as execution-grade current state. Treat `quote-last` and preload data as recovery or inspection state, not as proof that the market is live.
6. **Timezone Discipline**: VN preload and trading-session logic are VN-market-time based (`UTC+7`). If your service uses UTC internally, convert explicitly at the boundary.
7. **Boot Sequence**: Do not assume preload, live stream, and fallback stream become ready at exactly the same time. Your service should retry warmup and then attach to the live stream.
8. **No Direct Provider Connections**: New services inside `bobby_network` should not import provider SDKs or connect to Binance/DNSE/OKX/vnstock directly. `data_source_checker` includes a contract-enforcement audit to flag obvious direct-provider usage outside allowed internal provider modules.
9. **Strict Diagnostics Flags**: By default, health and diagnostics report sparse/no-recent-trade provider behavior as warnings, not outages. Use `PRELOAD_STRICT_FRESHNESS=true`, `VN_STRICT_LIVE_STREAMS=true`, or `STREAM_STRICT_FEED_HEALTH=true` when you intentionally want diagnostics/health to fail on these warnings.

---

## 6. Deployment Checklist

- [ ] Add `data_layer` and `redis_service` to your service's `environment` variables.
- [ ] Ensure `networks` include `bobby_network`.
- [ ] Use `orjson` for maximum performance.
- [ ] Prefer `app.sdk.DataLayerClient` over hand-written REST/Redis wrappers.
- [ ] Check `http://data_layer:8100/v1/health` during startup to verify connection.
- [ ] Choose the correct data contract:
  - `stream:trade:{symbol}` for live execution price
  - `stream:kline:{interval}:{symbol}` for candle logic
  - `/v1/preload/{symbol}` for VN warmup history
  - `/v1/vn/quote-last/{symbol}` for last known VN snapshot
- [ ] If reconnect happens, reload latest state from REST before resuming stream-only logic.

---

## 7. Migration From Legacy Consumers

This section is the rulebook for migrating old alpha folders, old paper execution services, and `trading_system` market-data inputs.

### Migration Goal

Old consumers may currently do one or more of these:

- open their own Binance WebSocket or REST clients
- open their own DNSE/vnstock clients
- read Redis keys directly without source/freshness validation
- call provider-specific preload/history utilities from inside alpha code
- resample or patch missing data with local ad-hoc logic

The target architecture is:

- `data_layer` owns all external market-data provider connections.
- alpha and trading services consume `data_layer` through the SDK, REST, and Redis contracts only.
- execution/risk logic validates freshness/source before it acts.
- alpha-specific custom logic stays inside the alpha, but data access is standardized.

### Old To New Mapping

| Legacy behavior | New behavior |
|-----------------|--------------|
| Alpha opens Binance WebSocket for trades | `client.stream_trades(symbols)` or Redis `stream:trade:{symbol}` |
| Alpha opens Binance WebSocket for klines | `client.stream_klines(symbols, interval="1m")`; resample locally if the alpha needs higher live intervals |
| Alpha calls Binance REST klines directly | `client.warmup_ohlcv("crypto", symbol, interval, limit, provider="binance")` |
| Alpha calls OKX directly as backup | `client.fallback_status()` and `client.fallback_reference()`; OKX is reference-only by default |
| Alpha opens DNSE/vnstock live quote connection | `client.stream_vn_quotes(symbols)` or Redis `stream:vn:{symbol}` |
| Alpha calls vnstock/DNSE history directly for VN warmup | `client.warmup_ohlcv("vn_stock", symbol, interval, limit)` |
| Paper/live executor reads exchange price directly | `client.latest_trade("binance", symbol)` plus `stream_trades()` |
| Service trusts any Redis key if present | validate `source` and `timestamp` with `validate_source()` / `validate_freshness()` |
| Service treats `/v1/vn/quote-last` as live | only use it for recovery/inspection unless live TTL state is confirmed |

### Recommended Alpha Migration Steps

1. Add `data_layer` network access.
   - Docker network: `bobby_network`.
   - Environment:
     - `DATA_LAYER_URL=http://data_layer:8100`
     - `REDIS_HOST=redis_service`
     - `REDIS_PORT=6379`
     - `REDIS_DB=2`

2. Mount or vendor the SDK.
   - Preferred import:

```python
from app.sdk import DataLayerClient
```

3. Replace local provider clients.
   - Remove direct Binance/DNSE/OKX/vnstock imports from alpha runtime code.
   - Keep strategy signal code unchanged where possible.
   - Replace only the data access boundary first.

4. Use warmup before live stream.
   - Crypto:

```python
warmup = client.warmup_ohlcv(
    "crypto",
    "BTCUSDT",
    interval="5m",
    limit=500,
    provider="binance",
)
```

   - VN:

```python
warmup = client.warmup_ohlcv(
    "vn_stock",
    "FPT",
    interval="15m",
    limit=500,
)
```

5. Attach live stream.
   - Crypto execution price:

```python
pubsub = client.stream_trades(["BTCUSDT", "ETHUSDT"])
```

   - Crypto live candles:

```python
pubsub = client.stream_klines(["BTCUSDT", "ETHUSDT"], interval="1m")
```

   - VN live quote:

```python
pubsub = client.stream_vn_quotes(["FPT", "HPG"])
```

6. Validate before producing a trade signal.
   - Binance live execution should require fresh Binance data.
   - VN live trading should require fresh `vn:quote:{symbol}` when market is open.
   - `quote-last` and preload data can recover state but should not prove the market is tradable.

7. Resample locally only from trusted data.
   - Crypto live Redis currently exposes `kline:1m:{symbol}`.
   - If an alpha trades on `5m`, `15m`, `30m`, or `1h`, it should warm up with REST and then resample incoming `1m` live bars locally.
   - Do not open a separate Binance kline stream from the alpha just to get another interval.

### Recommended `trading_system` Migration Steps

1. Keep `trading_system` execution/risk/accounting logic separate from market-data transport.
2. Replace market-data adapters that call providers directly with a `data_layer` adapter.
3. For Binance paper/sandbox/live:
   - mark price / execution reference:
     - live stream: `stream:trade:{symbol}`
     - recovery: `GET /v1/binance/price/{symbol}`
   - candle context:
     - warmup: `GET /v1/crypto/ohlcv/binance/{symbol}?interval=...`
     - live: `stream:kline:1m:{symbol}` then local resample if required
4. For DNSE/VN paper/live:
   - warmup: `GET /v1/preload/{symbol}?interval=...`
   - live quote: `stream:vn:{symbol}` or `GET /v1/vn/quote/{symbol}`
   - after-hours/restart inspection: `GET /v1/vn/quote-last/{symbol}`
5. Risk should reject execution-grade decisions when:
   - required Binance data is stale or missing
   - only OKX fallback/reference is available and policy does not explicitly allow it
   - VN market is open but the alpha requires live data and only `quote-last` exists
6. Reconciliation/recovery should load latest state from REST before accepting new stream events.

### Compatibility Period

Legacy Redis keys remain supported during migration:

- `trade:price:{symbol}`
- `kline:1m:{symbol}`
- `vn:quote:{symbol}`
- `vn:quote:last:{symbol}`

Legacy Redis channels remain supported:

- `stream:trade:{symbol}`
- `stream:kline:1m:{symbol}`
- `stream:vn:{symbol}`

Rules during compatibility:

- direct Redis reads are allowed only when wrapped with source/freshness checks.
- new services should prefer `DataLayerClient`.
- direct external provider connections from alpha/trading services are not allowed unless documented as a temporary exception.
- versioned/provider-aware keys can be added later in parallel; do not remove the current keys until all old consumers are migrated.

### Temporary Exceptions

The only acceptable exceptions are:

- internal `data_layer` provider modules
- diagnostics and tests
- short-lived migration scripts with a deletion date or explicit owner
- external research notebooks outside service runtime

Anything running as an alpha, executor, risk service, portfolio service, or trading-system bridge inside `bobby_network` should not connect directly to external market-data providers.

### Migration Acceptance Checklist

- [ ] No direct provider import or WebSocket creation in alpha/trading runtime code.
- [ ] Startup calls `health()` before warmup/subscription.
- [ ] Warmup uses `warmup_ohlcv()`.
- [ ] Live data uses `stream_trades()`, `stream_klines()`, or `stream_vn_quotes()`.
- [ ] Restart recovery reloads latest state from REST before acting on new stream events.
- [ ] Execution-grade paths validate source and freshness.
- [ ] OKX fallback is treated as non-authoritative unless risk policy says otherwise.
- [ ] VN `quote-last` is not treated as live tradable data.
- [ ] `data_source_checker` passes, or remaining warnings are documented provider/sparse-market warnings.
