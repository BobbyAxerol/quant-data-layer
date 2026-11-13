"""DL v2.3.0 declarable final-BAR source (demand row ``bar_source``: NATIVE | REST_FINAL).

The lane of an OKX SWAP final BAR is declared, not inferred: ``bar_source`` on the BAR demand row wins, and
without it the bar-family rule of ``qdl.runtime.universal_demand`` decides (v2.2.6 stage A: Trading System
QUOTE/MARK pricing alone keeps a symbol on the REST final lane). The release-route demand reader accepts the
same field. Existing demand files carry no ``bar_source`` and render exactly as before.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import yaml

from qdl.adapters.binance_spot import parse_spot_exchange_info
from qdl.consumer.release import StableReleaseRoutePlan
from qdl.runtime.production_catalog import (
    ProductionCatalogBuilder,
    ProductionDemandManifest,
    load_binance_exchange_info,
    load_okx_instruments,
)
from qdl.runtime.universal_demand import resolve_bar_source

ROOT = Path(__file__).resolve().parents[1]
CAPTURES = ROOT / "config/v2/captures"


def _row(symbol: str, feed: str, interval: str | None = None, **extra) -> dict:
    row = {"venue": "OKX", "market": "SWAP", "product_type": "PERPETUAL", "native_symbol": symbol, "feed": feed,
           "interval": interval, "source_policy_id": "crypto_primary_v2"}
    if feed == "MARK_INDEX_PRICE":
        row.update(source_policy_id="crypto_liquid_v2", max_freshness_ms=2000, require_live=True,
                   index_native_symbol=symbol[: -len("-SWAP")])
    row.update(extra)
    return row


def _demand(consumers: list[tuple[str, str, list[dict]]]) -> dict:
    return {"schema": "qdl.v2.production-demand.v1", "revision": 1, "consumers": [
        {"consumer_id": cid, "consumer_grade": grade, "requirements": rows} for cid, grade, rows in consumers]}


def _load(document: dict) -> ProductionDemandManifest:
    with tempfile.TemporaryDirectory() as raw:
        path = Path(raw) / "demand.yaml"
        path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
        return ProductionDemandManifest.load_many([path])


def _modes(document: dict) -> dict[str, str]:
    bundle = ProductionCatalogBuilder(catalog_revision=1, source_policy_revision=1, authority_revision=1).build(
        demand=_load(document),
        binance_usdm=load_binance_exchange_info(CAPTURES / "binance-usdm-exchangeinfo.filtered.json"),
        binance_spot=parse_spot_exchange_info(
            json.loads((CAPTURES / "binance-spot-exchangeinfo.filtered.json").read_text()), valid_from_ns=0),
        okx_rows=(*load_okx_instruments(CAPTURES / "okx-instruments-swap.filtered.json"),
                  *load_okx_instruments(CAPTURES / "okx-instruments-spot.filtered.json")),
    )
    return {row["binding_id"]: row["mode"] for row in bundle.acquisition_plan["bindings"]}


class BarSourceTests(unittest.TestCase):
    def test_rule_without_a_declaration_is_unchanged(self):
        self.assertEqual(resolve_bar_source(None, bar_family=True), "NATIVE")
        self.assertEqual(resolve_bar_source(None, bar_family=False), "REST_FINAL")
        self.assertEqual(resolve_bar_source("NATIVE", bar_family=False), "NATIVE")
        self.assertEqual(resolve_bar_source("REST_FINAL", bar_family=True), "REST_FINAL")
        with self.assertRaises(ValueError):
            resolve_bar_source("WS", bar_family=True)

    def test_declared_source_decides_the_lane(self):
        pricing = [_row("APT-USDT-SWAP", "QUOTE"), _row("APT-USDT-SWAP", "MARK_INDEX_PRICE"),
                   _row("ARB-USDT-SWAP", "QUOTE")]
        default = _modes(_demand([("ts", "EXECUTION", pricing),
                                  ("alpha", "ALPHA", [_row("APT-USDT-SWAP", "BAR", "1m")])]))
        self.assertEqual(default["okx-swap-apt-usdt-swap-bar-1m"], "PYTHON_REST")  # pricing-only symbol
        native = _modes(_demand([("ts", "EXECUTION", pricing), ("alpha", "ALPHA", [
            _row("APT-USDT-SWAP", "BAR", "1m", bar_source="NATIVE")])]))
        self.assertEqual(native["okx-swap-apt-usdt-swap-bar-1m"], "RUST_NATIVE")
        family = [_row("ARB-USDT-SWAP", "TRADE")]
        rest = _modes(_demand([("alpha", "ALPHA", family + [
            _row("ARB-USDT-SWAP", "BAR", "1m", bar_source="REST_FINAL")])]))
        self.assertEqual(rest["okx-swap-arb-usdt-swap-bar-1m"], "PYTHON_REST")  # declared wins over the family

    def test_conflicting_and_misplaced_declarations_are_refused(self):
        with self.assertRaisesRegex(ValueError, "conflicting bar sources"):
            _load(_demand([("a", "ALPHA", [_row("APT-USDT-SWAP", "BAR", "1m", bar_source="NATIVE")]),
                           ("b", "ALPHA", [_row("APT-USDT-SWAP", "BAR", "1m", bar_source="REST_FINAL")])]))
        merged = _load(_demand([("a", "ALPHA", [_row("APT-USDT-SWAP", "BAR", "1m")]),
                                ("b", "ALPHA", [_row("APT-USDT-SWAP", "BAR", "1m", bar_source="NATIVE")])]))
        self.assertEqual([item.bar_source for item in merged.demands], ["NATIVE"])
        with self.assertRaisesRegex(ValueError, "requirement fields are invalid"):
            _load(_demand([("a", "ALPHA", [_row("APT-USDT-SWAP", "QUOTE", bar_source="NATIVE")])]))
        binance = {"venue": "BINANCE", "market": "USDM", "product_type": "PERPETUAL", "native_symbol": "BTCUSDT",
                   "feed": "BAR", "interval": "1m", "source_policy_id": "crypto_primary_v2", "bar_source": "NATIVE"}
        with self.assertRaisesRegex(ValueError, "native final-bar lane is certified"):
            _load(_demand([("a", "ALPHA", [binance])]))

    def test_release_demand_reader_accepts_the_field(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "demand.yaml"
            path.write_text(yaml.safe_dump(_demand([("a", "ALPHA", [
                _row("APT-USDT-SWAP", "BAR", "1m", bar_source="NATIVE")])])), encoding="utf-8")
            self.assertEqual(len(StableReleaseRoutePlan._load_crypto_demand(path)), 1)
            path.write_text(yaml.safe_dump(_demand([("a", "ALPHA", [
                _row("APT-USDT-SWAP", "BAR", "1m", bar_source="WS")])])), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "bar_source is invalid"):
                StableReleaseRoutePlan._load_crypto_demand(path)


if __name__ == "__main__":
    unittest.main()
