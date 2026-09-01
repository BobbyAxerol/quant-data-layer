#!/usr/bin/env python3
"""Resolve separate Binance USD-M and OKX Swap top-volume universes.

The command defaults to read-only.  ``--apply`` writes only the supplied
host-visible universe state directory; it never changes a V2 runtime catalog,
Kafka, Redis, SQLite, service, authority record or consumer route.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import sys
import time
from typing import Any, Mapping

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.universe import (
    TopVolumeUniversePolicy,
    UniverseAuditStore,
    resolve_binance_usdm_top_volume,
    resolve_okx_swap_top_volume,
)


BINANCE_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_TICKERS = "https://fapi.binance.com/fapi/v1/ticker/24hr"
OKX_INSTRUMENTS = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
OKX_TICKERS = "https://www.okx.com/api/v5/market/tickers?instType=SWAP"


def _get_json(url: str, *, timeout_seconds: float, max_attempts: int) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url, timeout=timeout_seconds)
            if response.status_code == 429 or response.status_code >= 500:
                raise requests.HTTPError(f"retryable HTTP {response.status_code}", response=response)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as error:
            last_error = error
            if attempt == max_attempts:
                break
            time.sleep(min(2.0, 0.25 * (2 ** (attempt - 1))))
    raise RuntimeError(f"provider request failed after {max_attempts} attempts: {url}: {last_error}")


def _okx_data(value: Any, endpoint: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, Mapping) or str(value.get("code")) != "0":
        raise RuntimeError(f"OKX response is not successful: {endpoint}")
    rows = value.get("data")
    if not isinstance(rows, list) or not all(isinstance(item, Mapping) for item in rows):
        raise RuntimeError(f"OKX response has no data list: {endpoint}")
    return rows


def run(*, top_n: int, timeout_seconds: float, max_attempts: int):
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="qdl-universe") as executor:
        binance_info, binance_tickers, okx_instruments, okx_tickers = tuple(
            future.result()
            for future in (
                executor.submit(_get_json, BINANCE_EXCHANGE_INFO, timeout_seconds=timeout_seconds, max_attempts=max_attempts),
                executor.submit(_get_json, BINANCE_TICKERS, timeout_seconds=timeout_seconds, max_attempts=max_attempts),
                executor.submit(_get_json, OKX_INSTRUMENTS, timeout_seconds=timeout_seconds, max_attempts=max_attempts),
                executor.submit(_get_json, OKX_TICKERS, timeout_seconds=timeout_seconds, max_attempts=max_attempts),
            )
        )
    if not isinstance(binance_info, Mapping) or not isinstance(binance_tickers, list):
        raise RuntimeError("Binance provider payload shape is invalid")
    binance = resolve_binance_usdm_top_volume(
        exchange_info=binance_info,
        tickers=binance_tickers,
        policy=TopVolumeUniversePolicy("BINANCE", "USDM", size=top_n),
    )
    okx = resolve_okx_swap_top_volume(
        instruments=_okx_data(okx_instruments, "instruments"),
        tickers=_okx_data(okx_tickers, "tickers"),
        policy=TopVolumeUniversePolicy("OKX", "SWAP", size=top_n),
    )
    return binance, okx


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--top-n", type=int, default=350)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--max-audit-files", type=int, default=90)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    if not 1 <= args.top_n <= 1_000:
        parser.error("--top-n must be within [1, 1000]")
    if not 1.0 <= args.timeout_seconds <= 60.0:
        parser.error("--timeout-seconds must be within [1, 60]")
    if not 1 <= args.max_attempts <= 5:
        parser.error("--max-attempts must be within [1, 5]")
    binance, okx = run(
        top_n=args.top_n,
        timeout_seconds=args.timeout_seconds,
        max_attempts=args.max_attempts,
    )
    if args.apply:
        store = UniverseAuditStore(args.state_dir, max_audit_files=args.max_audit_files)
        output = [store.publish(binance), store.publish(okx)]
    else:
        output = [
            {
                "universe_id": item.policy.universe_id,
                "selection_sha256": item.selection_sha256,
                "member_count": len(item.members),
                "eligible_symbol_count": len(item.eligible_symbols),
            }
            for item in (binance, okx)
        ]
    print(json.dumps({"status": "APPLIED" if args.apply else "DRY_RUN", "universes": output}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
