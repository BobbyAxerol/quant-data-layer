import logging
import time
from typing import Any, Dict, Optional

import orjson
import pandas as pd

from app.sdk.client import DataLayerClient as BaseDataLayerClient, DataLayerClientError

logger = logging.getLogger(__name__)


class DataLayerClient(BaseDataLayerClient):
    """Backward-compatible alpha helper built on the official SDK client."""

    def __init__(
        self,
        base_url: str,
        redis_host: str = "redis_service",
        redis_port: int = 6379,
        redis_db: int = 2,
    ):
        super().__init__(base_url, redis_host=redis_host, redis_port=redis_port, redis_db=redis_db)
        logger.info("DataLayerClient connected to %s, redis=%s:%s db=%s", self.base_url, redis_host, redis_port, redis_db)

    def fetch_preload(
        self,
        symbol: str,
        interval: str = "1m",
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        payload = self.warmup_ohlcv("vn_stock", symbol, interval=interval, limit=limit or 1000)

        data = payload.get("data", [])
        df = pd.DataFrame(data)
        if df.empty:
            logger.warning("Preload endpoint returned empty dataset for %s", symbol.upper())
            return df

        df["time"] = pd.to_datetime(df["time"])
        return df

    def get_cached_quote(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self.redis_get(f"vn:quote:{symbol.upper()}")

    def subscribe_vn_quotes(self, symbol: str):
        return self.stream_vn_quotes(symbol)


class MovingAverageCrossAlpha:
    def __init__(
        self,
        client: DataLayerClient,
        symbol: str,
        high_tf: str = "60min",
        fast: int = 8,
        slow: int = 20,
    ):
        self.client = client
        self.symbol = symbol.upper()
        self.high_tf = high_tf
        self.fast = fast
        self.slow = slow
        self.warmup_df: Optional[pd.DataFrame] = None
        self.signal_df: Optional[pd.DataFrame] = None

    def prepare_warmup(self, limit: int = 1000, max_retries: int = 10, retry_delay: int = 30):
        """Prepare warmup data, retrying if preload data not available yet."""
        for attempt in range(max_retries):
            try:
                logger.info("%s attempt %d/%d: Fetching preload data (limit=%d)...",
                          self.symbol, attempt + 1, max_retries, limit)
                preload_df = self.client.fetch_preload(self.symbol, limit=limit)
                if preload_df.empty:
                    logger.warning("%s attempt %d/%d: Preload returned empty DataFrame, will retry in %ds",
                                 self.symbol, attempt + 1, max_retries, retry_delay)
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    else:
                        raise ValueError(f"No preload data available for {self.symbol} after {max_retries} attempts")

                # Detailed data inspection logging
                logger.info("%s attempt %d/%d: SUCCESS - Preload data loaded!", self.symbol, attempt + 1, max_retries)
                logger.info("%s DataFrame shape: %s", self.symbol, preload_df.shape)
                logger.info("%s DataFrame columns: %s", self.symbol, list(preload_df.columns))
                logger.info("%s DataFrame dtypes:\n%s", self.symbol, preload_df.dtypes.to_string())
                logger.info("%s DataFrame describe:\n%s", self.symbol, preload_df.describe().to_string())
                logger.info("%s DataFrame head(5):\n%s", self.symbol, preload_df.head(5).to_string())
                logger.info("%s DataFrame tail(5):\n%s", self.symbol, preload_df.tail(5).to_string())

                # Check for null/NaN values
                null_counts = preload_df.isnull().sum()
                logger.info("%s Null value counts:\n%s", self.symbol, null_counts.to_string())
                total_nulls = null_counts.sum()
                logger.info("%s Total null values: %d", self.symbol, total_nulls)

                # Check data quality
                if 'time' in preload_df.columns:
                    logger.info("%s Time range: %s to %s", self.symbol,
                              preload_df['time'].min(), preload_df['time'].max())
                    logger.info("%s Time duplicates: %d", self.symbol, preload_df['time'].duplicated().sum())

                if 'volume' in preload_df.columns:
                    zero_volume = (preload_df['volume'] == 0).sum()
                    logger.info("%s Zero volume bars: %d", self.symbol, zero_volume)

                self.warmup_df = preload_df
                high_tf_df = self._resample_high_tf(preload_df)
                self.signal_df = self._calculate_signals(high_tf_df)

                logger.info("%s High-TF resampled: %s bars on %s timeframe", self.symbol, len(high_tf_df), self.high_tf)
                logger.info("%s Signal DataFrame shape: %s", self.symbol, self.signal_df.shape)
                logger.info("%s Latest signal: %s", self.symbol, self.latest_signal())

                return

            except DataLayerClientError as e:
                if " 404 " in str(e):
                    logger.warning("%s attempt %d/%d: Preload data not found (404), will retry in %ds",
                                 self.symbol, attempt + 1, max_retries, retry_delay)
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    else:
                        raise ValueError(f"Preload data not found for {self.symbol} after {max_retries} attempts")
                else:
                    raise

    def _resample_high_tf(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.set_index("time").sort_index()
        ohlc = df["close"].resample(self.high_tf).ohlc()
        volume = df["volume"].resample(self.high_tf).sum().rename("volume")
        high_tf_df = ohlc.join(volume)
        high_tf_df = high_tf_df.dropna()
        return high_tf_df

    def _calculate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["fast_sma"] = df["close"].rolling(self.fast, min_periods=1).mean()
        df["slow_sma"] = df["close"].rolling(self.slow, min_periods=1).mean()
        df["direction"] = 0
        df.loc[df["fast_sma"] > df["slow_sma"], "direction"] = 1
        df.loc[df["fast_sma"] < df["slow_sma"], "direction"] = -1
        df["signal"] = df["direction"].diff().fillna(0).astype(int)
        return df

    def latest_signal(self) -> Optional[pd.Series]:
        if self.signal_df is None or self.signal_df.empty:
            return None
        return self.signal_df.iloc[-1]

    def handle_quote(self, quote: Dict[str, Any]):
        if not quote:
            logger.warning("%s Received empty quote payload", self.symbol)
            return

        price = quote.get("last") or quote.get("close") or quote.get("price")
        if price is None:
            logger.warning("%s Quote payload missing price field: %s", self.symbol, quote)
            return

        # Log detailed quote information
        logger.info("%s WEBSOCKET QUOTE RECEIVED: price=%.3f, full_payload=%s",
                   self.symbol, price, quote)

        signal = self.latest_signal()
        if signal is not None:
            logger.info("%s SIGNAL ANALYSIS: live_price=%.3f, fast_sma=%.3f, slow_sma=%.3f, direction=%d, signal=%d",
                       self.symbol, price, signal.get("fast_sma", float("nan")),
                       signal.get("slow_sma", float("nan")), signal.get("direction", 0),
                       signal.get("signal", 0))

            # Check for signal changes
            if signal.get("signal", 0) == 1:
                logger.info("🚀 %s LONG SIGNAL: Fast MA crossed above Slow MA at price %.3f", self.symbol, price)
            elif signal.get("signal", 0) == -1:
                logger.info("📉 %s SHORT SIGNAL: Fast MA crossed below Slow MA at price %.3f", self.symbol, price)
        else:
            logger.warning("%s No signal data available for analysis", self.symbol)

    def run(self):
        try:
            self.prepare_warmup()
            pubsub = self.client.subscribe_vn_quotes(self.symbol)
            logger.info("Alpha listening for live Redis quotes on stream:vn:%s", self.symbol)

            for message in pubsub.listen():
                if not message or message.get("type") != "message":
                    continue

                payload = message["data"]
                if isinstance(payload, bytes):
                    payload = payload.decode("utf-8", errors="ignore")

                try:
                    quote = orjson.loads(payload)
                except Exception:
                    logger.exception("Unable to parse Redis quote payload")
                    continue

                try:
                    self.handle_quote(quote)
                except Exception:
                    logger.exception("Error handling quote")
                    continue

        except Exception as e:
            logger.error("Alpha service encountered fatal error: %s", e, exc_info=True)
            raise
