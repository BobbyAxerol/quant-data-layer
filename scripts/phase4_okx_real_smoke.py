from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time
from pathlib import Path

from qdl.adapters.okx import OkxHistoricalClient
from qdl.adapters.okx.client import OkxRestClient


def checksum(rows: tuple[object, ...]) -> str:
    payload = json.dumps(
        [row.__dict__ for row in rows], sort_keys=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def summary(result) -> dict:
    return {
        "rows": len(result.records),
        "checksum": checksum(result.records),
        "coverage": result.coverage.__dict__,
        "coverage_status": result.coverage.status,
    }


async def run(output: Path) -> dict:
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - 30 * 60 * 1000
    client = OkxHistoricalClient(OkxRestClient(timeout_seconds=10))
    trade = await client.candles(
        inst_id="BTC-USDT-SWAP", bar="1m", start_ms=start_ms, end_ms=end_ms,
        max_records=60, max_pages=2,
    )
    mark = await client.candles(
        inst_id="BTC-USDT-SWAP", bar="1m", start_ms=start_ms, end_ms=end_ms,
        price_type="MARK", max_records=60, max_pages=2,
    )
    index = await client.candles(
        inst_id="BTC-USDT", bar="1m", start_ms=start_ms, end_ms=end_ms,
        price_type="INDEX", max_records=60, max_pages=2,
    )
    funding = await client.funding_history(
        inst_id="BTC-USDT-SWAP", start_ms=end_ms - 2 * 24 * 3600 * 1000,
        end_ms=end_ms, max_records=20, max_pages=2,
    )
    open_interest = await client.open_interest_snapshot(
        inst_type="SWAP", inst_id="BTC-USDT-SWAP"
    )
    if not trade.records or not mark.records or not index.records or not funding.records:
        raise RuntimeError("OKX real smoke returned an empty required history dataset")
    if len(open_interest) != 1 or open_interest[0].coverage != "SNAPSHOT_ONLY":
        raise RuntimeError("OKX OI smoke did not preserve snapshot-only coverage")
    report = {
        "schema": "qdl.phase4.okx-real-history.v1",
        "status": "PASS",
        "provenance": "REAL_OKX_V5_PUBLIC_API",
        "production_writes": 0,
        "requested_at_ms": end_ms,
        "trade_candles": summary(trade),
        "mark_candles": summary(mark),
        "index_candles": summary(index),
        "funding_history": summary(funding),
        "open_interest": {
            "rows": len(open_interest),
            "checksum": checksum(open_interest),
            "coverage": open_interest[0].coverage,
            "observed_ts_ms": open_interest[0].observed_ts_ms,
        },
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = asyncio.run(run(args.output))
    print(json.dumps({
        "status": report["status"],
        "trade_rows": report["trade_candles"]["rows"],
        "funding_rows": report["funding_history"]["rows"],
        "oi_rows": report["open_interest"]["rows"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
