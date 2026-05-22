# data_layer

A centralized, high-performance market data gateway service that aggregates and distributes real-time financial market data from multiple sources — **Binance** (crypto) and **Vietnamese stock market** (via DNSE WebSocket + vnstock fallback) — to downstream trading services via **Redis Pub/Sub** and a **REST API**.

## Features

- **Real-time streaming** — WebSocket multiplexer for Binance (spot & futures trade + kline) and DNSE (VN stock live quotes)
- **Automatic failover** — DNSE as primary VN source, vnstock REST poller as secondary fallback
- **Redis Pub/Sub distribution** — single upstream connection shared across many downstream consumers
- **Historical warmup (VN)** — Parquet-backed preload service for 1-minute OHLCV candle warmup
- **Preload watchdog** — auto-refreshes VN candle data during market hours, sleeps until next open otherwise
- ⚡ **High-performance serialization** — `orjson` throughout for minimal latency
- **Alpha strategy example** — moving-average crossover strategy included as a reference implementation
- 🐳 **Docker-first** — full Docker Compose stack with networking, volumes, and log management

## Architecture

```
                    ┌─────────────────────────────────────────┐
                    │            data_layer service            │
                    │                                          │
  Binance WS ──────►│  Async WS Multiplexer                   │
  (trade + kline)   │  (spot & futures)                       │
                    │            │                             │
  DNSE WS ─────────►│  DNSE Stream Manager  ──────────────────►  Redis Pub/Sub
  (VN primary)      │  (primary VN source)                    │  stream:trade:{symbol}
                    │            │                             │  stream:kline:{interval}:{symbol}
  vnstock REST ─────►│  VN Poller            ──────────────────►  stream:vn:{symbol}
  (VN fallback)     │  (fallback if DNSE stale)               │
                    │                                          │
                    │  Preload Watchdog                        │  REST API
                    │  (Parquet 1m OHLCV)  ──────────────────►  GET /v1/preload/{symbol}
                    └─────────────────────────────────────────┘
```

Downstream services (alpha strategies, paper trading engines, execution services) connect to:
- **Redis Pub/Sub** for real-time streaming
- **REST API** (`http://data_layer:8100`) for warmup, latest-state recovery, and health checks

## Tech Stack

| Component | Technology |
|-----------|------------|
| Framework | [FastAPI](https://fastapi.tiangolo.com/) + [Uvicorn](https://www.uvicorn.org/) |
| Cache & Streaming | [Redis](https://redis.io/) 5+ with [hiredis](https://github.com/redis/hiredis) |
| Data Processing | [Pandas](https://pandas.pydata.org/) + [PyArrow](https://arrow.apache.org/) (Parquet) |
| Serialization | [orjson](https://github.com/ijl/orjson) |
| VN Market Data | [vnstock](https://github.com/thinh-vu/vnstock) + DNSE WebSocket |
| Package Manager | [Poetry](https://python-poetry.org/) |
| Runtime | Python 3.10+ |
| Container | Docker + Docker Compose |

## Project Structure

```
data_layer/
├── app/
│   ├── main.py               # FastAPI app, lifespan, all REST endpoints
│   ├── config.py             # Environment config loader
│   ├── logging_config.py     # Structured logging setup
│   ├── alpha/
│   │   ├── strategy.py       # DataLayerClient + MovingAverageCrossAlpha example
│   │   └── run_alpha.py      # Alpha service entrypoint
│   ├── cache/
│   │   └── redis_cache.py    # Redis abstraction layer
│   ├── database/
│   │   ├── preload.py        # Parquet-based VN candle preload logic
│   │   └── dnse_fallback.py  # DNSE REST fallback for historical data
│   ├── diagnostics/          # Data source health check scripts
│   ├── openapi_sdk/          # Generated DNSE OpenAPI client (Python + JS)
│   └── stream/
│       ├── async_live_feed.py   # Async Binance WS multiplexer
│       ├── binance_ws.py        # Binance WebSocket client
│       ├── dnse_ws.py           # DNSE WebSocket client & stream manager
│       ├── feed_builder.py      # Stream task builder
│       ├── feed_parsers.py      # Normalized payload parsers
│       ├── price_manager.py     # Price aggregation logic
│       └── vnstock_poller.py    # vnstock REST polling fallback
├── tests/
│   ├── test_alpha_strategy.py
│   └── test_data_layer_client.py
├── data/                     # Parquet preload storage (gitignored)
├── logs/                     # Application logs (gitignored)
├── symbols.json              # Binance symbols configuration
├── symbols_vn.yaml           # VN stock symbols configuration
├── docker-compose.yml        # Full service stack
├── Dockerfile
├── pyproject.toml
└── DATA_LAYER_SERVICE_ACCESS_GUIDE.md  # Integration guide for downstream services
```

## Getting Started

### Prerequisites

- Docker & Docker Compose
- A running Redis instance accessible as `redis_service` on `bobby_network`
- (Optional) DNSE API credentials for VN stock live data
- (Optional) vnstock API key

### 1. Clone & Configure

```bash
git clone <your-repo-url>
cd data_layer
cp .env.example .env
```

Edit `.env` and fill in your credentials:

```bash
# Required for VN stock live data (DNSE)
DNSE_API_KEY=your_dnse_api_key
DNSE_API_SECRET_KEY=your_dnse_secret

# Optional: vnstock API key
VNSTOCK_API_KEY=your_vnstock_key
```

### 2. Create Docker Networks (if not already present)

```bash
docker network create bobby_network
docker network create executor_network
```

### 3. Build & Run

```bash
docker compose up -d data_layer
```

The service will be available at `http://localhost:8100`.

### 4. Verify

```bash
curl http://localhost:8100/v1/health
```

Expected response:
```json
{
  "status": "ok",
  "redis": true,
  "binance_trade_stream": true,
  "binance_kline_stream": true
}
```

## API Reference

### Health

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/v1/health` | Service health + Redis status |

### Binance

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/v1/binance/price/{symbol}` | Latest cached trade price |
| `GET` | `/v1/binance/kline/{symbol}?interval=1m` | Latest cached kline |
| `GET` | `/v1/binance/klines/{symbol}?interval=1m&limit=500` | Historical klines proxy |

### VN Stock

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/v1/vn/quote/{symbol}` | Live quote (short TTL) |
| `GET` | `/v1/vn/quote-last/{symbol}` | Last known snapshot (survives market close) |
| `GET` | `/v1/vn/board` | Full VN price board snapshot |

### Preload (VN Historical Warmup)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/v1/preload/status` | Preload inventory & timestamp metadata |
| `GET` | `/v1/preload/{symbol}?limit=1000` | Latest N warmup candles (ascending time) |
| `POST` | `/v1/preload/run` | Trigger full preload for all symbols |
| `POST` | `/v1/preload/append/{symbol}` | Append delta for a single symbol |

## Redis Stream Channels

Downstream services subscribe to these channels:

| Channel | Purpose |
|---------|---------|
| `stream:trade:{symbol}` | Binance live trade price (execution/papertrade) |
| `stream:kline:{interval}:{symbol}` | Binance kline/candle data |
| `stream:vn:{symbol}` | VN stock live quote |

All payloads are serialized with `orjson`. Clients should use `orjson.loads(message['data'])`.

### Quick Subscribe Example (Python)

```python
import redis
import orjson

r = redis.Redis(host='redis_service', port=6379, db=2)
pubsub = r.pubsub()
pubsub.subscribe("stream:trade:BTCUSDT")

for message in pubsub.listen():
    if message['type'] == 'message':
        data = orjson.loads(message['data'])
        print(f"Price: {data['price']}")
```

## Recommended Boot Sequence for Downstream Services

1. Check `GET /v1/health` — verify `data_layer` is reachable
2. Fetch `GET /v1/preload/{symbol}?limit=N` — warm up with historical candles (VN only)
3. Call the latest-state endpoint for your feed to recover any missed updates
4. Subscribe to the appropriate Redis channel for live streaming

> See [DATA_LAYER_SERVICE_ACCESS_GUIDE.md](./DATA_LAYER_SERVICE_ACCESS_GUIDE.md) for the full integration contract.

## Normalized Payload Contracts

All feeds emit normalized payloads regardless of provider. See [`app/stream/feed_parsers.py`](./app/stream/feed_parsers.py) for the canonical field definitions.

| Feed | Key Fields |
|------|-----------|
| Binance Trade | `symbol`, `price`, `quantity`, `trade_id`, `event_time`, `trade_time`, `side`, `source` |
| Binance Kline | `symbol`, `open`, `high`, `low`, `close`, `volume`, `timestamp`, `is_closed` |
| VN Quote | `symbol`, `price`, `quantity`, `open`, `high`, `low`, `source`, `timestamp` |

## Configuration

All configuration is via environment variables. See [`.env.example`](./.env.example) for the full list.

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `redis_service` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_DB` | `2` | Redis database index |
| `DNSE_API_KEY` | — | DNSE OpenAPI key |
| `DNSE_API_SECRET_KEY` | — | DNSE OpenAPI secret |
| `VNSTOCK_API_KEY` | — | vnstock API key |
| `VNSTOCK_SOURCE` | `KBS` | vnstock data source |
| `VNSTOCK_POLL_INTERVAL` | `3.0` | Polling interval (seconds) |
| `PRELOAD_DIR` | `/app/data/preload/1m` | Parquet storage path |
| `PRELOAD_MONTHS` | `6` | Months of history to preload |
| `BINANCE_SYMBOLS_FILE` | `/app/symbols.json` | Binance symbols config |
| `API_HOST` | `0.0.0.0` | FastAPI bind host |
| `API_PORT` | `8100` | FastAPI bind port |

## Development

### Install Dependencies (local, without Docker)

```bash
pip install poetry
poetry install
```

### Run Locally

```bash
# Set up environment
cp .env.example .env
# Edit .env to point REDIS_HOST to your local Redis instance

uvicorn app.main:app --host 0.0.0.0 --port 8100 --reload
```

### Run Tests

```bash
# Via Docker Compose
docker compose run --rm test_runner

# Or locally
python -m unittest discover tests
```

### Run Alpha Strategy Example

```bash
docker compose up -d alpha_service
```

### Run Data Source Diagnostics

```bash
docker compose run --rm data_source_checker
```

## Docker Services

| Service | Description |
|---------|-------------|
| `data_layer` | Main FastAPI data gateway (port 8100) |
| `alpha_service` | Moving-average crossover alpha strategy example |
| `test_runner` | Unit test runner |
| `data_source_checker` | Data source health diagnostics |

## VN Market Schedule

The preload watchdog respects VN stock exchange hours (`UTC+7`):

- **Morning session**: 09:00 – 11:30
- **Afternoon session**: 13:00 – 14:30
- **Weekends**: skipped

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -m 'Add your feature'`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License — see the [LICENSE](./LICENSE) file for details.
