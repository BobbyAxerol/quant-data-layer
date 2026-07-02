import os
from dotenv import load_dotenv

load_dotenv()

# ── DNSE OpenAPI ────────────────────────────────
DNSE_API_KEY = os.getenv("DNSE_API_KEY", "")
DNSE_API_SECRET_KEY = os.getenv("DNSE_API_SECRET_KEY", "")
DNSE_REST_BASE = os.getenv("DNSE_REST_BASE", "https://openapi.dnse.com.vn")
DNSE_WS_BASE = os.getenv("DNSE_WS_BASE", "wss://ws-openapi.dnse.com.vn")

# ── vnstock ─────────────────────────────────────
VNSTOCK_API_KEY = os.getenv("VNSTOCK_API_KEY", "")
VNSTOCK_SOURCE = os.getenv("VNSTOCK_SOURCE", "KBS")
VNSTOCK_POLL_INTERVAL = float(os.getenv("VNSTOCK_POLL_INTERVAL", 3.0))

# ── Redis ───────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 2))

# ── Preload ─────────────────────────────────────
PRELOAD_DIR = os.getenv("PRELOAD_DIR", "/app/data/preload/1m")
PRELOAD_MONTHS = int(os.getenv("PRELOAD_MONTHS", 6))
PRELOAD_CHUNK_DAYS = int(os.getenv("PRELOAD_CHUNK_DAYS", 7))
PRELOAD_DELAY = float(os.getenv("PRELOAD_DELAY", 1.0))
PRELOAD_MAX_RETRIES = int(os.getenv("PRELOAD_MAX_RETRIES", 3))
PRELOAD_DAILY_RUN_TIME = os.getenv("PRELOAD_DAILY_RUN_TIME", "16:00")
PRELOAD_API_FRESH_TOPUP = os.getenv("PRELOAD_API_FRESH_TOPUP", "true").lower() in {"1", "true", "yes", "on"}
PRELOAD_API_TOPUP_MAX_LAG_MINUTES = int(os.getenv("PRELOAD_API_TOPUP_MAX_LAG_MINUTES", "5"))

# ── Binance WS ──────────────────────────────────
BINANCE_WS_BATCH_SIZE = int(os.getenv("BINANCE_WS_BATCH_SIZE", 100))
BINANCE_WS_QUEUE_MAXSIZE = int(os.getenv("BINANCE_WS_QUEUE_MAXSIZE", 10000))
BINANCE_WS_MAX_CONNS_PER_SOURCE = int(os.getenv("BINANCE_WS_MAX_CONNS_PER_SOURCE", 0))
BINANCE_SYMBOLS_FILE = os.getenv("BINANCE_SYMBOLS_FILE", "/app/symbols.json")
BINANCE_SPOT_SYMBOLS_FILE = os.getenv("BINANCE_SPOT_SYMBOLS_FILE", "/app/symbols_spot.json")
STREAM_STALE_SECONDS = float(os.getenv("STREAM_STALE_SECONDS", "180"))
STREAM_STRICT_FEED_HEALTH = os.getenv("STREAM_STRICT_FEED_HEALTH", "false").lower() in {"1", "true", "yes", "on"}

# ── Provider fallback ───────────────────────────
OKX_FALLBACK_ENABLED = os.getenv("OKX_FALLBACK_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
OKX_FALLBACK_STALE_SECONDS = float(os.getenv("OKX_FALLBACK_STALE_SECONDS", "180"))
OKX_FALLBACK_PRIORITY_SYMBOLS = [
    s.strip().upper()
    for s in os.getenv("OKX_FALLBACK_PRIORITY_SYMBOLS", "BTCUSDT,ETHUSDT").split(",")
    if s.strip()
]

# ── FastAPI ─────────────────────────────────────
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", 8100))
