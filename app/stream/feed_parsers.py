import logging

logger = logging.getLogger(__name__)

def parse_binance(msg: dict):
    """Normalize Binance websocket format."""
    try:
        # data_layer normally outputs the raw 'k' dictionary for klines.
        # But for unified processing, we can extract common fields.
        if "k" in msg:
            k = msg["k"]
            return {
                "symbol": str(msg.get("s", "")).upper(),
                "open": float(k.get("o", 0)),
                "high": float(k.get("h", 0)),
                "low": float(k.get("l", 0)),
                "close": float(k.get("c", 0)),
                "volume": float(k.get("v", 0)),
                "timestamp": int(k.get("t", 0)),
                "is_closed": bool(k.get("x", False)),
                "raw": msg # Keep raw payload for legacy downstream consumers
            }
        # If it's already the raw kline without 'k' wrapping
        elif "o" in msg and "c" in msg:
            return {
                "symbol": str(msg.get("s", "")).upper(),
                "open": float(msg.get("o", 0)),
                "high": float(msg.get("h", 0)),
                "low": float(msg.get("l", 0)),
                "close": float(msg.get("c", 0)),
                "volume": float(msg.get("v", 0)),
                "timestamp": int(msg.get("t", 0)),
                "is_closed": bool(msg.get("x", False)),
                "raw": {"s": str(msg.get("s", "")).upper(), "k": msg}
            }
        return msg # Fallback
    except Exception as e:
        logger.debug(f"Binance parse error: {e}")
        return None

def parse_dnse(msg: dict):
    """Normalize DNSE quote format (if passed through unified WS)."""
    try:
        return {
            "symbol": str(msg.get("symbol", "")).upper(),
            "open": float(msg.get("open", 0)),
            "high": float(msg.get("high", 0)),
            "low": float(msg.get("low", 0)),
            "close": float(msg.get("price", 0)),
            "volume": float(msg.get("quantity", 0)),
            "timestamp": int(msg.get("timestamp", 0) * 1000) if "timestamp" in msg else 0,
            "is_closed": False,
            "raw": msg
        }
    except Exception as e:
        logger.debug(f"DNSE parse error: {e}")
        return None

PARSERS = {
    "binance": parse_binance,
    "binance_spot": parse_binance,
    "binance_futures": parse_binance,
    "dnse": parse_dnse,
}
