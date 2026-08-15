#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import json

from websockets.asyncio.client import connect


async def main() -> None:
    async with connect(
        "wss://fstream.binance.com/ws/btcusdt@trade",
        open_timeout=15,
        close_timeout=5,
        ping_interval=20,
    ) as websocket:
        raw = await asyncio.wait_for(websocket.recv(), timeout=15)
        payload = json.loads(raw)
        if payload.get("e") != "trade" or payload.get("s") != "BTCUSDT":
            raise RuntimeError("unexpected Binance USD-M trade frame")
        print(json.dumps({
            "event": payload["e"],
            "production_writes": 0,
            "provenance": "REAL_BINANCE_USDM_WEBSOCKET",
            "status": "PASS",
            "symbol": payload["s"],
        }, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
