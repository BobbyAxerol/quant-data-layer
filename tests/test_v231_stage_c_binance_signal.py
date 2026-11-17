"""DL v2.3.1 stage C: Binance USDM signal universes in scripts/dl_declaration.py (offline, no runtime).

Base = the deployed stage-B r7 repo form (declaration-stage-b-r7/repo). Checks: Binance USDM intervals are
declarable for SIGNAL consumers; a signal consumer's first Binance rows take its own OKX BAR row as template; the
1m base is the certified native kline lane, 1h/12h become DERIVED_FINAL with a Binance-anchored parent; verbatim
exchangeInfo rows are appended for listed symbols only (FETUSDT, LUNA2USDT); the runtime universe mount carries
the retained universes too; the alpha manifest bump is additive with the accept-set floor.
"""
from __future__ import annotations

from collections import Counter
from pathlib import Path
import tempfile
import unittest

import yaml

from scripts import dl_declaration as tool

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "upgrade/evidence/releases/v2.3.0/declaration-stage-b-r7/repo"
CAPTURE = ROOT / "tests/fixtures/v231_stage_c/binance-usdm-exchangeinfo-fet-luna2.json"
SYMBOLS = ["SOLUSDT", "1000PEPEUSDT", "ADAUSDT", "FETUSDT", "LUNA2USDT"]


def _declaration(directory: Path, *, symbols=SYMBOLS, capture_symbols=("FETUSDT", "LUNA2USDT")) -> Path:
    document = {
        "schema": "qdl.v2.declaration.v1", "declaration_revision": 8, "environment": "paper", "accept_set": True,
        "ingestor": {"lane_pins": True},
        "capacity": {"baseline_dl_vcpu": 6.21, "ceiling_dl_vcpu": 6.5, "venue_priority": ["OKX", "BINANCE"],
                     "rates_per_binding": {"binance_usdm_bar": 0.416}},
        "instrument_captures": [{"venue": "BINANCE", "market": "USDM", "path": str(CAPTURE),
                                 "captured_at_utc": "2026-11-17T06:20:00Z",
                                 "endpoint": "GET https://fapi.binance.com/fapi/v1/exchangeInfo",
                                 "symbols": list(capture_symbols)}],
        "base": {"repo": str(BASE)},
        "consumers": [{"consumer_id": "alpha.okx.paper.stable", "class": "SIGNAL",
                       "manifest": "consumers/stable/alpha-okx-paper.yaml",
                       "universes": [{"universe_id": "binance-usdm-crossec-test", "venue": "BINANCE",
                                      "market": "USDM", "symbols": list(symbols),
                                      "bars": {"native": ["1m"], "derived": ["1h", "12h"]},
                                      "grant_intervals": ["1m", "1h", "12h"], "reference": ["FUNDING_RATE"]}]}],
    }
    path = directory / "declaration.yaml"
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return path


class StageCBinanceSignalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.out = Path(cls.tmp.name) / "out"
        cls.report = tool.render(_declaration(Path(cls.tmp.name)), cls.out)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_rendered_without_blocked_rows(self):
        self.assertEqual(self.report["status"], "RENDERED_NOT_APPLIED")
        self.assertEqual(self.report["blocked"], [])

    def test_new_instruments_only_from_the_listed_capture_rows(self):
        catalog = self.report["summary"]["catalog"]
        self.assertEqual(sorted(catalog["new_instruments"]), ["FETUSDT", "LUNA2USDT"])
        capture = (self.out / "repo/config/v2/captures/binance-usdm-exchangeinfo.filtered.json").read_text()
        base = (BASE / "config/v2/captures/binance-usdm-exchangeinfo.filtered.json").read_text()
        self.assertTrue(capture.startswith(base.rstrip("]}\n")[:200]))
        provenance = yaml.safe_load((self.out / "repo/config/v2/captures/provenance.json").read_text())
        usdm = next(e for e in provenance["captures"] if e["venue"] == "BINANCE" and e["market"] == "USDM")
        self.assertEqual(usdm["appended_captures"][-1]["symbols_added"], 2)

    def test_native_1m_and_binance_anchored_derived_1h_12h(self):
        acquisition = self.report["summary"]["acquisition"]
        # SOLUSDT already has native 1m and REST 1h/12h (alpha.binance); the four others are new.
        self.assertEqual(acquisition["new_rows_by_mode"], {"DERIVED_FINAL": 8, "RUST_NATIVE": 4})
        runtime = yaml.safe_load((self.out / "repo/config/v2/stable-acquisition-bindings.yaml").read_text())
        catalog = yaml.safe_load((self.out / "repo/config/v2/stable-source-bindings.yaml").read_text())
        source = {row["binding_id"]: row for row in catalog["bindings"]}
        derived = [row for row in runtime["bindings"] if row["mode"] == "DERIVED_FINAL"
                   and row["runtime"] == "BINANCE"]
        self.assertEqual(Counter(source[row["binding_id"]]["interval"] for row in derived), {"1h": 4, "12h": 4})
        # The largest finer materialised parent: 1h from 1m, 12h from the derived 1h (Binance UTC anchors).
        parents = {source[row["binding_id"]]["interval"]: row["derived_from"]["interval"] for row in derived}
        self.assertEqual(parents, {"1h": "1m", "12h": "1h"})
        native = [row for row in runtime["bindings"] if row["provider_kind"] == "binance_usdm_bar"
                  and source[row["binding_id"]]["interval"] == "1m"]
        self.assertTrue(any(row["native_channel"] == "1000pepeusdt@kline_1m" for row in native))

    def test_alpha_manifest_bump_is_additive_with_accept_set(self):
        manifest = self.report["summary"]["manifests"]["alpha.okx.paper.stable"]
        self.assertEqual(manifest["revision"], [16, 17])
        self.assertEqual(manifest["min_token_revision"], 15)
        self.assertEqual(manifest["additive_problems"], [])
        self.assertEqual(self.report["consumer_environment"], {})

    def test_runtime_universes_carry_the_retained_set(self):
        names = {path.name for path in (self.out / "runtime/universes").glob("*.json")}
        self.assertTrue({"binance-usdm-crossec-test.json", "okx-swap-covered-25.json",
                         "okx-swap-crossec-94.json"} <= names)

    def test_gate_passes(self):
        result = tool.gate(self.out)
        failing = [c for c in result["checks"] if c["status"] != "PASS"]
        self.assertEqual(failing, [])

    def test_a_listed_symbol_missing_from_the_capture_refuses(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = _declaration(Path(tmp), capture_symbols=("FETUSDT", "NOPEUSDT"))
            with self.assertRaisesRegex(tool.DeclarationRefused, "NOPEUSDT is not in the exchangeInfo response"):
                tool.render(path, None, dry_run=True)


if __name__ == "__main__":
    unittest.main()
