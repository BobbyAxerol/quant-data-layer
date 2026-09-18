#!/usr/bin/env python3
"""Does Binance USD-M deliver final 1m klines on the routed /market lane, and are they right?

Read-only against the venue. It subscribes, listens, reads REST and prints; it
writes nothing and touches no runtime.

Why it exists. `qdl/runtime/production_catalog.py` keeps every generated Binance
BAR demand on `PYTHON_REST` with a comment that says the platform has never
proven "final kline delivery after a valid WS ACK", and that a native BAR lane
may be re-enabled "after fresh final-bar admission evidence". That comment was
written while the ingestor was still dialling the base Binance decommissioned on
2026-04-23, so the evidence could not have existed. R1.27 routed the lanes; this
script is the evidence the comment asks for, and the gate for R1.28 Phase 3.

It answers three questions at once, because they are the three Phase 3 depends on:

  1 admission   does `x=true` arrive at all, for every symbol, every boundary?
  2 latency     how long after the bar's own close time does it arrive?
  3 truth       does the WS final bar equal what REST returns for the SAME
                open_time, and if not, when does REST converge onto it?

Question 3 decides whether REST stays authoritative or becomes reconciliation.
The comparison identity is always symbol + interval + explicit `startTime`,
never "the last row", so a late REST replica cannot be mistaken for a revision.

It dials `/market/ws` and sends SUBSCRIBE - the same control shape the ingestor
uses - rather than a combined `/stream?streams=` URL, because a probe that
proves a different URL shape proves nothing about production.

    docker run --rm --network none ... is NOT possible: this needs the internet.
    docker run --rm --entrypoint python qdl-v2-python:<tag> - < this_file
    BAR_COUNT=3 SYMBOLS=BTCUSDT,ETHUSDT ... python3 -B this_file
"""

from __future__ import annotations

import asyncio
import json
import os
import statistics
import sys
import time
import urllib.request

import websockets

# The routed control URL. `/market` carries kline, aggTrade, markPrice and
# ticker since 2026-03-06; the unrouted base was decommissioned 2026-04-23.
MARKET_WS = os.environ.get("MARKET_WS", "wss://fstream.binance.com/market/ws")
REST_KLINES = "https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&startTime={start}&limit=1"

SYMBOLS = [s.strip().upper() for s in os.environ.get(
    "SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,DOGEUSDT").split(",") if s.strip()]
INTERVAL = os.environ.get("INTERVAL", "1m")
BAR_COUNT = int(os.environ.get("BAR_COUNT", "3"))
# When to ask REST for the same open_time, in seconds after the bar's close.
REST_DELAYS = [float(v) for v in os.environ.get("REST_DELAYS", "0,2,4,6,10").split(",")]

# The nine fields both sides carry for the same bar. `close_time` is excluded:
# it is derived from the interval and is identical by construction.
FIELDS = ["open", "high", "low", "close", "volume",
          "quote_volume", "trades", "taker_base", "taker_quote"]


def rest_bar(symbol: str, open_ms: int) -> dict[str, str] | None:
    """The venue's own row for exactly this open_time, or None if it has none."""
    url = REST_KLINES.format(symbol=symbol, interval=INTERVAL, start=open_ms)
    with urllib.request.urlopen(url, timeout=8) as response:
        rows = json.load(response)
    if not rows or int(rows[0][0]) != open_ms:
        return None
    row = rows[0]
    return dict(zip(FIELDS, [row[1], row[2], row[3], row[4], row[5],
                             row[7], str(row[8]), row[9], row[10]]))


def ws_bar(kline: dict) -> dict[str, str]:
    return {
        "open": kline["o"], "high": kline["h"], "low": kline["l"],
        "close": kline["c"], "volume": kline["v"], "quote_volume": kline["q"],
        "trades": str(kline["n"]), "taker_base": kline["V"], "taker_quote": kline["Q"],
    }


async def main() -> int:
    streams = [f"{s.lower()}@kline_{INTERVAL}" for s in SYMBOLS]
    # One boundary per bar, plus the slowest REST delay, plus a margin for the
    # first partial minute.
    budget = 60 * (BAR_COUNT + 1) + max(REST_DELAYS) + 30
    print(f"  url        {MARKET_WS}")
    print(f"  subscribe  {len(streams)} streams, {INTERVAL}, {BAR_COUNT} closed bars each")
    print(f"  budget     {budget:.0f}s\n")

    finals: dict[str, list[dict]] = {symbol: [] for symbol in SYMBOLS}
    provisional: dict[str, int] = {symbol: 0 for symbol in SYMBOLS}
    ack = None
    loop = asyncio.get_running_loop()
    deadline = loop.time() + budget

    async with websockets.connect(MARKET_WS, open_timeout=15, ping_interval=20) as socket:
        await socket.send(json.dumps({"method": "SUBSCRIBE", "params": streams, "id": 1}))
        pending: list[asyncio.Task] = []
        while loop.time() < deadline:
            if all(len(v) >= BAR_COUNT for v in finals.values()):
                break
            try:
                raw = await asyncio.wait_for(socket.recv(), timeout=deadline - loop.time())
            except (asyncio.TimeoutError, TimeoutError):
                break
            message = json.loads(raw)
            if "id" in message or "error" in message:
                ack = message
                continue
            if message.get("e") != "kline":
                continue
            symbol = message["s"].upper()
            kline = message["k"]
            if symbol not in finals:
                continue
            if not kline.get("x"):
                provisional[symbol] += 1
                continue
            if len(finals[symbol]) >= BAR_COUNT:
                continue
            arrived = time.time()
            record = {
                "open_ms": int(kline["t"]),
                "close_s": int(kline["T"]) / 1000.0,
                "arrival_after_close_s": arrived - int(kline["T"]) / 1000.0,
                "ws": ws_bar(kline),
                "rest": {},
            }
            finals[symbol].append(record)
            pending.append(asyncio.create_task(reconcile(symbol, record)))
        for task in pending:
            try:
                await asyncio.wait_for(task, timeout=max(REST_DELAYS) + 20)
            except (asyncio.TimeoutError, TimeoutError):
                pass

    print(f"  SUBSCRIBE ack: {json.dumps(ack)}\n")
    return report(finals, provisional)


async def reconcile(symbol: str, record: dict) -> None:
    """Ask REST for this exact open_time at each delay after its close."""
    loop = asyncio.get_running_loop()
    for delay in REST_DELAYS:
        wait = (record["close_s"] + delay) - time.time()
        if wait > 0:
            await asyncio.sleep(wait)
        try:
            row = await loop.run_in_executor(None, rest_bar, symbol, record["open_ms"])
        except Exception as exc:                       # noqa: BLE001 - reported, not raised
            record["rest"][delay] = {"error": f"{type(exc).__name__}: {exc}"}
            continue
        if row is None:
            record["rest"][delay] = {"missing": True}
            continue
        record["rest"][delay] = {
            "differs": [f for f in FIELDS if row[f] != record["ws"][f]],
            "row": row,
        }


def report(finals: dict[str, list[dict]], provisional: dict[str, int]) -> int:
    missing = [s for s, v in finals.items() if not v]
    print("  --- 1/2. admission and latency ---")
    print(f"  {'symbol':10} {'finals':>7} {'provisional':>12} {'arrival after close (s)':>26}")
    all_latencies: list[float] = []
    for symbol in finals:
        bars = finals[symbol]
        if not bars:
            print(f"  {symbol:10} {0:>7} {provisional[symbol]:>12}   NO FINAL BAR   <== FAIL")
            continue
        lat = [b["arrival_after_close_s"] for b in bars]
        all_latencies.extend(lat)
        print(f"  {symbol:10} {len(bars):>7} {provisional[symbol]:>12}"
              f"   min={min(lat):.3f} med={statistics.median(lat):.3f} max={max(lat):.3f}")
    if all_latencies:
        print(f"\n  across all symbols: n={len(all_latencies)} "
              f"min={min(all_latencies):.3f}s median={statistics.median(all_latencies):.3f}s "
              f"max={max(all_latencies):.3f}s")

    print("\n  --- 3. does REST agree with the WS final bar, for the same open_time? ---")
    agreed_at: dict[float, int] = {d: 0 for d in REST_DELAYS}
    total = 0
    disagreements: list[str] = []
    for symbol, bars in finals.items():
        for bar in bars:
            total += 1
            first_agreement = None
            for delay in REST_DELAYS:
                outcome = bar["rest"].get(delay)
                if not outcome or "differs" not in outcome:
                    continue
                if not outcome["differs"]:
                    agreed_at[delay] += 1
                    if first_agreement is None:
                        first_agreement = delay
            if first_agreement is None:
                fields = set()
                for delay in REST_DELAYS:
                    outcome = bar["rest"].get(delay) or {}
                    fields.update(outcome.get("differs") or [])
                    if "error" in outcome or "missing" in outcome:
                        fields.add(next(iter(outcome)))
                disagreements.append(
                    f"  {symbol} open_ms={bar['open_ms']} never agreed; fields: {sorted(fields)}")
            elif first_agreement > 0:
                disagreements.append(
                    f"  {symbol} open_ms={bar['open_ms']} REST caught up at +{first_agreement:.0f}s")
    for delay in REST_DELAYS:
        share = f"{agreed_at[delay]}/{total}" if total else "0/0"
        print(f"  REST at close+{delay:>4.0f}s identical to the WS final bar: {share}")
    if disagreements:
        print("\n  where REST lagged the WS bar:")
        for line in disagreements:
            print(line)

    print()
    if missing:
        print(f"  ADMISSION: FAIL - no final bar for {', '.join(missing)}")
        return 1
    print(f"  ADMISSION: PASS - every one of {len(finals)} symbols delivered "
          f"{BAR_COUNT} final bars on the routed /market lane")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
