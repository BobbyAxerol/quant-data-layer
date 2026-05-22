# Data Layer Service Integration Guide (Production)

This document provides technical details on how to integrate production services (like Alpha strategies or Paper trading engines) with the `data_layer` infrastructure.

## Standard

Other services should connect to `data_layer` using this rule:

- use Redis Pub/Sub for live streaming consumption
- use REST API for warmup, latest-state recovery, diagnostics, and manual triggers
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

---

## 6. Deployment Checklist

- [ ] Add `data_layer` and `redis_service` to your service's `environment` variables.
- [ ] Ensure `networks` include `bobby_network`.
- [ ] Use `orjson` for maximum performance.
- [ ] Check `http://data_layer:8100/v1/health` during startup to verify connection.
- [ ] Choose the correct data contract:
  - `stream:trade:{symbol}` for live execution price
  - `stream:kline:{interval}:{symbol}` for candle logic
  - `/v1/preload/{symbol}` for VN warmup history
  - `/v1/vn/quote-last/{symbol}` for last known VN snapshot
- [ ] If reconnect happens, reload latest state from REST before resuming stream-only logic.
