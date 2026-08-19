import logging
import re
from typing import Dict, List

logger = logging.getLogger(__name__)

# Allowed symbol pattern (simple): uppercase letters, numbers
SYMBOL_RE = re.compile(r'^[A-Za-z0-9_]+$')

def _sanitize_symbol(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    return s

def validate_symbols(symbols: List[str]) -> List[str]:
    out = []
    for s in symbols:
        ss = _sanitize_symbol(s)
        if not ss:
            continue
        if SYMBOL_RE.match(ss):
            out.append(ss)
        else:
            logger.debug(f"[feed_builder] Invalid symbol skipped: {s}")
    return out

def build_urls(symbols_by_source: Dict[str, List[str]], interval: str = "1m", batch_size: int = 50) -> Dict[str, List[str]]:
    """
    Build WS URLs grouped by source.
    symbols_by_source: {"binance_spot": ["BTCUSDT", ...], "binance_futures": [...]}
    """
    urls = {}

    for source, syms in symbols_by_source.items():
        clean = validate_symbols(syms)
        if not clean:
            logger.info(f"[feed_builder] No valid symbols for {source}, skipping.")
            urls[source] = []
            continue

        if source in {"binance_futures", "binance_futures_kline"}:
            base = "wss://fstream.binance.com/public/stream?streams="
            lower_syms = [s.lower() for s in clean]
            src_urls = []
            for i in range(0, len(lower_syms), batch_size):
                batch = lower_syms[i:i + batch_size]
                stream = "/".join(f"{s}@kline_{interval}" for s in batch)
                src_urls.append(base + stream)
            urls[source] = src_urls

        elif source in {"binance_spot", "binance_spot_kline"}:
            base = "wss://stream.binance.com:9443/stream?streams="
            lower_syms = [s.lower() for s in clean]
            src_urls = []
            for i in range(0, len(lower_syms), batch_size):
                batch = lower_syms[i:i + batch_size]
                stream = "/".join(f"{s}@kline_{interval}" for s in batch)
                src_urls.append(base + stream)
            urls[source] = src_urls

        elif source == "binance_futures_trade":
            base = "wss://fstream.binance.com/public/stream?streams="
            lower_syms = [s.lower() for s in clean]
            src_urls = []
            for i in range(0, len(lower_syms), batch_size):
                batch = lower_syms[i:i + batch_size]
                stream = "/".join(f"{s}@trade" for s in batch)
                src_urls.append(base + stream)
            urls[source] = src_urls

        elif source == "binance_spot_trade":
            base = "wss://stream.binance.com:9443/stream?streams="
            lower_syms = [s.lower() for s in clean]
            src_urls = []
            for i in range(0, len(lower_syms), batch_size):
                batch = lower_syms[i:i + batch_size]
                stream = "/".join(f"{s}@trade" for s in batch)
                src_urls.append(base + stream)
            urls[source] = src_urls
            
        else:
            logger.info(f"[feed_builder] Source {source} has no WS builder implemented - skipped.")
            urls[source] = []

    return urls
