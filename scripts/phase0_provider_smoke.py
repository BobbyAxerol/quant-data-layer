#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.phase0_audit import _http_json, _write_json  # noqa: E402


CHECKS = (
    ("service_health", "/v1/health", True),
    ("binance_usdm_latest_trade", "/v1/binance/price/BTCUSDT?market=usdm", True),
    ("binance_usdm_latest_kline", "/v1/binance/kline/BTCUSDT?interval=1m", True),
    ("binance_history_two_bars", "/v1/crypto/ohlcv/binance/BTCUSDT?interval=15m&limit=2&market=usdm", True),
    ("okx_history_two_bars", "/v1/crypto/ohlcv/okx/BTCUSDT?interval=15m&limit=2", True),
    ("vn_preload_two_bars", "/v1/preload/VN30F1M?interval=1m&limit=2&fresh=false", True),
    ("vn_last_quote", "/v1/vn/quote-last/VN30F1M", True),
)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bounded read-only Phase 0 provider smoke")
    parser.add_argument("--base-url", default="http://127.0.0.1:8100")
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    checks = []
    for name, path, required in CHECKS:
        result = _http_json(args.base_url, path)
        checks.append({"name": name, "path": path, "required": required, **result})

    failures = [item["name"] for item in checks if item["required"] and not item.get("ok")]
    report = {
        "schema_version": 1,
        "mode": "bounded_read_only",
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "base_url": args.base_url,
        "checks": checks,
        "summary": {
            "total": len(checks),
            "passed": len(checks) - len(failures),
            "failed": len(failures),
            "failures": failures,
        },
    }
    _write_json(args.output.resolve(), report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
