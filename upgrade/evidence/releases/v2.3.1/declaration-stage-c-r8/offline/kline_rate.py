"""Public Binance USDM kline_1m rate for the 96 stage-C symbols (read-only, public market WS, no DL/runtime).

Counts every kline frame (provisional and final) for 120 s over one connection with SUBSCRIBE, which is what the
Rust ingestor would publish to md.raw.realtime.v2 per binding (the core drops provisional rows after decode).
"""
import asyncio, json, os, sys, time, collections
import websockets

URL = "wss://fstream.binance.com/market/ws"
syms = json.load(open(sys.argv[1]))
secs = float(sys.argv[2])

async def main():
    kw = {}
    proxy = os.environ.get("HTTPS_PROXY")
    async with websockets.connect(URL, max_size=2**22, proxy=proxy) as ws:
        params = [f"{s.lower()}@kline_1m" for s in syms]
        for i in range(0, len(params), 50):
            await ws.send(json.dumps({"method": "SUBSCRIBE", "params": params[i:i+50], "id": i + 1}))
        per = collections.Counter(); final = collections.Counter(); start = None; acks = 0
        end = time.monotonic() + secs + 10
        while time.monotonic() < end:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
            except asyncio.TimeoutError:
                continue
            msg = json.loads(raw)
            if "result" in msg:
                acks += 1
                if acks * 50 >= len(params) and start is None:
                    start = time.monotonic(); end = start + secs
                continue
            if start is None or msg.get("e") != "kline":
                continue
            per[msg["s"]] += 1
            if msg["k"]["x"]:
                final[msg["s"]] += 1
        dur = time.monotonic() - start
        total = sum(per.values())
        rates = sorted(v / dur for v in per.values())
        print(json.dumps({"url": URL, "symbols": len(syms), "symbols_with_frames": len(per), "seconds": round(dur, 1),
                          "frames": total, "frames_per_s": round(total / dur, 1),
                          "per_binding_mean": round(total / dur / len(syms), 3),
                          "per_binding_p50": round(rates[len(rates)//2], 3), "per_binding_max": round(rates[-1], 3),
                          "final_frames": sum(final.values())}))

asyncio.run(main())
