from __future__ import annotations

import json
import time

import requests


BASE = "http://127.0.0.1:8100"


def summarize(name: str, endpoint: str) -> dict:
    try:
        response = requests.get(BASE + endpoint, timeout=25)
        item = {"name": name, "status": response.status_code}
        try:
            data = response.json()
        except Exception:
            item["body"] = response.text[:200]
            return item

        if not isinstance(data, dict):
            item["json_type"] = type(data).__name__
            return item

        for key in [
            "status",
            "symbol",
            "interval",
            "count",
            "provider",
            "venue",
            "reference_for",
            "authoritative",
            "fallback_reason",
            "fallback_activated",
            "is_live",
        ]:
            if key in data:
                item[key] = data[key]

        if name == "health":
            item["stream_status"] = data.get("binance_stream", {}).get("status")
            item["dnse_status"] = data.get("dnse_stream", {}).get("status")
        if name == "fallback_status":
            item["trade_reason"] = data.get("trade", {}).get("reason")
            item["kline_reason"] = data.get("kline", {}).get("reason")
        if name == "fallback_force":
            item["okx_data_count"] = len((data.get("data") or {}).get("data") or [])
        return item
    except Exception as exc:
        return {"name": name, "error": str(exc)}


def main():
    time.sleep(8)
    endpoints = [
        ("health", "/v1/health"),
        ("fallback_status", "/v1/fallback/crypto/status/BTCUSDT?interval=1m"),
        ("fallback_force", "/v1/fallback/crypto/reference/BTCUSDT?interval=1m&limit=1&force=true"),
        ("binance_trade", "/v1/binance/price/BTCUSDT"),
        ("binance_kline", "/v1/binance/kline/BTCUSDT?interval=1m"),
        ("binance_history", "/v1/crypto/ohlcv/binance/BTCUSDT?interval=15m&limit=1&market=spot"),
        ("okx_history", "/v1/crypto/ohlcv/okx/BTCUSDT?interval=15m&limit=1"),
        ("vn_quote_live", "/v1/vn/quote/FPT"),
        ("vn_quote_last", "/v1/vn/quote-last/FPT"),
        ("vn_board", "/v1/vn/board"),
    ]
    for interval in ["1m", "5m", "10m", "15m", "30m", "1h", "4h"]:
        endpoints.append((f"preload_FPT_{interval}", f"/v1/preload/FPT?interval={interval}&limit=3&fresh=false"))
    endpoints.extend(
        [
            ("preload_invalid_interval", "/v1/preload/FPT?interval=2m&limit=3&fresh=false"),
            ("preload_unknown_symbol", "/v1/preload/NOTREAL?interval=1m&limit=3&fresh=false"),
        ]
    )

    print(json.dumps([summarize(name, endpoint) for name, endpoint in endpoints], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

