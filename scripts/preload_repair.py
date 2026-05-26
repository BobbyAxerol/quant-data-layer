from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import UTC, datetime

from app.database.preload import (
    load_vn_symbols,
    preload_needs_topup,
    update_symbol,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("preload_repair")


def repair_symbols(
    symbols: list[str],
    max_lag_minutes: int,
    dry_run: bool = False,
    missing_months: int = 2,
    symbol_delay: float = 2.0,
) -> dict:
    reports = []
    for idx, symbol in enumerate(symbols, start=1):
        symbol = symbol.upper().strip()
        decision = preload_needs_topup(symbol, max_lag_minutes=max_lag_minutes)
        should_repair = decision["reason"] == "canonical_missing" or decision["needs_topup"]
        row = {
            "symbol": symbol,
            "index": idx,
            "needs_repair": should_repair,
            "before": {k: str(v) for k, v in decision.items()},
        }
        if should_repair and not dry_run:
            logger.info("[%s/%s] repairing %s reason=%s", idx, len(symbols), symbol, decision["reason"])
            try:
                if decision["reason"] == "canonical_missing":
                    # Bootstrap a recent window first so diagnostics and live
                    # consumers get fresh warmup quickly. The daily preload job
                    # can continue extending history toward the full FIFO window.
                    update_symbol(symbol, months=max(1, int(missing_months)))
                else:
                    update_symbol(symbol)
                after = preload_needs_topup(symbol, max_lag_minutes=max_lag_minutes)
                row["after"] = {k: str(v) for k, v in after.items()}
                row["status"] = "ok" if not after["needs_topup"] and after["reason"] != "canonical_missing" else "still_stale"
            except Exception as exc:
                logger.exception("[%s] repair failed", symbol)
                row["status"] = "error"
                row["error"] = str(exc)
        elif should_repair:
            row["status"] = "dry_run"
        else:
            row["status"] = "skipped_fresh"
        reports.append(row)
        if symbol_delay > 0 and idx < len(symbols):
            time.sleep(symbol_delay)

    return {
        "started_at": datetime.now(UTC).isoformat(),
        "symbols_checked": len(symbols),
        "repaired_or_needed": sum(1 for row in reports if row["needs_repair"]),
        "status_counts": {
            status: sum(1 for row in reports if row["status"] == status)
            for status in sorted({row["status"] for row in reports})
        },
        "reports": reports,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair missing/stale VN preload parquet files.")
    parser.add_argument("--symbols", default="", help="Comma-separated symbol list. Defaults to symbols_vn.yaml.")
    parser.add_argument("--max-lag-minutes", type=int, default=20)
    parser.add_argument("--missing-months", type=int, default=2, help="Bootstrap window for symbols with no canonical parquet.")
    parser.add_argument("--symbol-delay", type=float, default=2.0, help="Delay between symbols to respect provider limits.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()] if args.symbols else load_vn_symbols()
    report = repair_symbols(
        symbols,
        max_lag_minutes=args.max_lag_minutes,
        dry_run=args.dry_run,
        missing_months=args.missing_months,
        symbol_delay=args.symbol_delay,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 1 if report["status_counts"].get("error") or report["status_counts"].get("still_stale") else 0


if __name__ == "__main__":
    raise SystemExit(main())
