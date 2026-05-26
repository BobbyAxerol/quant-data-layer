from __future__ import annotations


def latest_response(symbol: str, data: dict, *, is_live: bool | None = None) -> dict:
    payload = {"symbol": symbol.upper(), "data": data}
    if is_live is not None:
        payload["is_live"] = is_live
    return payload

