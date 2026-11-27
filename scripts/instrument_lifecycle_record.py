#!/usr/bin/env python3
"""DL v2.4.4 (DL-BL-2026-11-26-K): record venue listing captures into the instrument lifecycle ledger.

Purpose: the only writer of the append-only, hash-chained ledger that Query serves at
``GET /v2/instruments/{identity}/lifecycle`` (``qdl/runtime/instrument_lifecycle.py`` holds every rule). Feed it the
venue-listing captures the declaration gate already takes (``venue_listings`` of a declaration,
``scripts/catalog_venue_listing_gate.py``), at each render and from any later read-only listing capture.

Boundary: reads a catalogue and capture files, writes only the named ledger file, atomically (temporary file +
rename) and only after proving the result is an append-only extension of the old file. No network, no runtime.

  python3 -B scripts/instrument_lifecycle_record.py init --ledger L.jsonl --ledger-id qdl-v2-stable
  python3 -B scripts/instrument_lifecycle_record.py record --ledger L.jsonl --catalog stable-source-bindings.yaml \
      --listing OKX:SWAP:okx-instruments-swap.json --endpoint 'GET https://www.okx.com/api/v5/public/instruments?instType=SWAP' \
      --observed-utc 2026-11-19T14:03:37Z --catalog-revision 18 --source-revision declaration-r10
  python3 -B scripts/instrument_lifecycle_record.py verify --ledger L.jsonl [--previous OLD.jsonl]

Exit 0 on success, 1 on a typed refusal (printed as JSON), 2 on usage errors.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from qdl.runtime import instrument_lifecycle as lc  # noqa: E402


def _write_atomic(path: Path, data: bytes) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_bytes(data)
    os.replace(temporary, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init")
    init.add_argument("--ledger", type=Path, required=True)
    init.add_argument("--ledger-id", required=True)
    record = commands.add_parser("record")
    record.add_argument("--ledger", type=Path, required=True)
    record.add_argument("--catalog", type=Path, required=True)
    record.add_argument("--listing", required=True, help="VENUE:MARKET:capture.json")
    record.add_argument("--endpoint", required=True)
    record.add_argument("--observed-utc", required=True)
    record.add_argument("--catalog-revision", type=int)
    record.add_argument("--source-revision", required=True)
    record.add_argument("--max-absent-fraction", type=float, default=0.5)
    verify = commands.add_parser("verify")
    verify.add_argument("--ledger", type=Path, required=True)
    verify.add_argument("--previous", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "init":
            if args.ledger.exists():
                raise lc.LifecycleLedgerError(f"{args.ledger} exists; a ledger is never re-initialised")
            _write_atomic(args.ledger, lc.render_ledger(lc.header(args.ledger_id), []))
            result = {"status": "INITIALISED", "ledger": str(args.ledger)}
        elif args.command == "record":
            old = args.ledger.read_bytes()
            head, events = lc.parse_ledger(old)
            venue, market, capture_path = args.listing.split(":", 2)
            capture = lc.load_listing_capture(venue, market, Path(capture_path).read_bytes(),
                                              endpoint=args.endpoint, observed_utc=args.observed_utc)
            instruments = json.loads(args.catalog.read_text(encoding="utf-8"))["instruments"]
            appended, report = lc.record_capture(head, events, instruments, capture,
                catalog_revision=args.catalog_revision, source_revision=args.source_revision,
                max_absent_fraction=args.max_absent_fraction)
            new = lc.render_ledger(head, [*events, *appended])
            lc.verify_append_only(old, new)
            if appended:
                _write_atomic(args.ledger, new)
            result = {"status": "RECORDED", **report,
                      "ledger": lc.InstrumentLifecycleIndex(new).ledger_view()}
        else:
            new = args.ledger.read_bytes()
            if args.previous is not None:
                lc.verify_append_only(args.previous.read_bytes(), new)
            result = {"status": "VERIFIED", "ledger": lc.InstrumentLifecycleIndex(new).ledger_view()}
    except lc.LifecycleCaptureRefused as error:
        print(json.dumps({"status": "REFUSED", "code": error.code, "detail": str(error)}, sort_keys=True))
        return 1
    except lc.LifecycleLedgerError as error:
        print(json.dumps({"status": "REFUSED", "code": "LIFECYCLE_LEDGER_INVALID", "detail": str(error)},
                         sort_keys=True))
        return 1
    print(json.dumps(result, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
