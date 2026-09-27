"""D48: the daily-bar market-cap universe (top <=300 on Binance and OKX) and its change log."""
from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location("kn_universe_top300", ROOT / "scripts/kn_universe_top300.py")
U = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = U
_SPEC.loader.exec_module(U)

DAY = 86_400_000
NOW = 1_800_000_000_000


def binance(*bases, status="TRADING", listed=NOW - 400 * DAY, extra=()):
    rows = [{"symbol": f"{base}USDT", "baseAsset": base, "quoteAsset": "USDT", "contractType": "PERPETUAL",
             "status": status, "onboardDate": listed} for base in bases]
    return {"symbols": rows + list(extra)}


def okx(*bases, state="live", listed=NOW - 400 * DAY):
    return {"data": [{"instId": f"{base}-USDT-SWAP", "instFamily": f"{base}-USDT", "ctType": "linear",
                      "settleCcy": "USDT", "state": state, "listTime": str(listed)} for base in bases]}


def coins(*pairs):
    return [{"id": symbol.lower() + str(index), "symbol": symbol.lower(), "market_cap": cap}
            for index, (symbol, cap) in enumerate(pairs)]


class SelectionRuleTests(unittest.TestCase):
    def test_only_bases_trading_on_both_venues_with_a_market_cap_are_ranked(self):
        result = U.select(
            binance=binance("BTC", "ETH", "SOL", "USDC", extra=[{"symbol": "1000PEPEUSDT", "baseAsset": "1000PEPE",
                "quoteAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING", "onboardDate": NOW - 400 * DAY}]),
            okx=okx("BTC", "ETH", "USDC", "PEPE", "NEW") | {},
            coins=coins(("BTC", 100), ("ETH", 90), ("USDC", 80), ("PEPE", 5)),
            incumbents=[], now_ms=NOW)
        self.assertEqual([row["base"] for row in result["members"]], ["BTC", "ETH", "PEPE"])
        self.assertEqual(result["members"][2]["binance_symbol"], "1000PEPEUSDT")
        self.assertEqual(result["reasons"]["SOL"], "NOT_LIVE_OKX")
        self.assertEqual(result["reasons"]["USDC"], "EXCLUDED_ASSET")
        self.assertEqual(result["reasons"]["NEW"], "NOT_TRADING_BINANCE")

    def test_settling_new_and_capless_bases_are_excluded_with_reasons(self):
        payload = binance("BTC", "ETH")
        payload["symbols"].append({"symbol": "OLDUSDT", "baseAsset": "OLD", "quoteAsset": "USDT",
                                   "contractType": "PERPETUAL", "status": "SETTLING", "onboardDate": NOW - 900 * DAY})
        result = U.select(binance=payload | {}, okx=okx("BTC", "ETH", "OLD"),
                          coins=coins(("BTC", 100)), incumbents=["OLD"], now_ms=NOW)
        self.assertEqual([row["base"] for row in result["members"]], ["BTC"])
        self.assertEqual(result["reasons"]["OLD"], "NOT_TRADING_BINANCE")
        self.assertEqual(result["reasons"]["ETH"], "NO_MARKET_CAP")
        young = U.select(binance=binance("BTC", listed=NOW - 5 * DAY), okx=okx("BTC"),
                         coins=coins(("BTC", 1)), incumbents=[], now_ms=NOW)
        self.assertEqual((young["members"], young["reasons"]["BTC"]), ([], "TOO_NEW"))

    def test_a_shared_symbol_takes_the_largest_coin_and_is_flagged(self):
        result = U.select(binance=binance("ONE"), okx=okx("ONE"),
                          coins=coins(("ONE", 50), ("ONE", 3)), incumbents=[], now_ms=NOW)
        self.assertEqual(result["members"][0]["market_cap_usd"], 50)
        self.assertTrue(result["members"][0]["ambiguous_market_cap_symbol"])

    def test_the_cap_and_the_incumbent_band(self):
        bases = [f"C{index:03d}" for index in range(340)]
        caps = [(base, 10_000 - index) for index, base in enumerate(bases)]
        first = U.select(binance=binance(*bases), okx=okx(*bases), coins=coins(*caps), incumbents=[], now_ms=NOW)
        self.assertEqual(len(first["members"]), U.CAP)
        self.assertEqual(first["reasons"]["C300"], "CAP_FULL")
        # An incumbent ranked 320 (inside the 330 band) stays; one ranked 335 leaves.
        incumbents = bases[:299] + ["C319"]
        second = U.select(binance=binance(*bases), okx=okx(*bases), coins=coins(*caps),
                          incumbents=incumbents, now_ms=NOW)
        members = {row["base"] for row in second["members"]}
        self.assertIn("C319", members)
        self.assertEqual(len(members), U.CAP)
        third = U.select(binance=binance(*bases), okx=okx(*bases), coins=coins(*caps),
                         incumbents=bases[:299] + ["C334"], now_ms=NOW)
        self.assertNotIn("C334", {row["base"] for row in third["members"]})
        self.assertEqual(third["reasons"]["C334"], "RANK_BELOW_BAND")


class ChangeLogTests(unittest.TestCase):
    def test_the_log_replays_to_point_in_time_membership(self):
        rows1 = U.select(binance=binance("BTC", "ETH", "SOL"), okx=okx("BTC", "ETH", "SOL"),
                         coins=coins(("BTC", 3), ("ETH", 2), ("SOL", 1)), incumbents=[], now_ms=NOW)
        first = U.change_record(revision=1, effective_date="2026-09-26", before={}, after=rows1["members"],
                                reasons=rows1["reasons"], sources={"x": "1"})
        self.assertEqual([item["reason"] for item in first["added"]], ["SIGNED"] * 3)
        rows2 = U.select(binance=binance("BTC", "ETH", "AVAX"), okx=okx("BTC", "ETH", "AVAX"),
                         coins=coins(("BTC", 3), ("ETH", 2), ("AVAX", 1)),
                         incumbents=["BTC", "ETH", "SOL"], now_ms=NOW)
        before = {row["base"]: row for row in rows1["members"]}
        second = U.change_record(revision=2, effective_date="2026-10-03", before=before, after=rows2["members"],
                                 reasons=rows2["reasons"], sources={"x": "2"})
        self.assertEqual(second["removed"], [{"base": "SOL", "reason": "NOT_TRADING_BINANCE"}])
        self.assertEqual(second["added"], [{"base": "AVAX", "market_cap_rank": 3, "reason": "ENTERED_BY_RANK"}])
        log = [first, second]
        self.assertEqual(U.members_as_of(log, "2026-09-25"), set())
        self.assertEqual(U.members_as_of(log, "2026-10-01"), {"BTC", "ETH", "SOL"})
        self.assertEqual(U.members_as_of(log, "2026-10-03"), {"BTC", "ETH", "AVAX"})

    def test_native_identity_change_is_logged_without_faking_base_membership_change(self):
        old = {"base": "PEPE", "binance_symbol": "1000PEPEUSDT", "okx_inst_id": "PEPE-USDT-SWAP"}
        new = dict(old, binance_symbol="PEPEUSDT")
        record = U.change_record(revision=2, effective_date="2026-09-27", before={"PEPE": old},
                                 after=[new], reasons={}, sources={})
        self.assertEqual((record["added"], record["removed"]), ([], []))
        self.assertEqual(record["identity_changes"][0]["after"]["binance_symbol"], "PEPEUSDT")

    def test_an_unchanged_membership_writes_nothing(self):
        rows = U.select(binance=binance("BTC"), okx=okx("BTC"), coins=coins(("BTC", 1)), incumbents=["BTC"],
                        now_ms=NOW)
        record = U.change_record(revision=2, effective_date="2026-10-01", before={"BTC": rows["members"][0]},
                                 after=rows["members"], reasons=rows["reasons"], sources={})
        self.assertEqual((record["added"], record["removed"]), ([], []))


class DemandOwnershipTests(unittest.TestCase):
    def demand(self, rows=()):
        return {"revision": 1, "consumers": [
            {"consumer_id": spec["consumer_id"], "requirements": list(rows) if venue == "BINANCE" else []}
            for venue, spec in U.UNIVERSE_CONSUMERS.items()]}

    def row(self, symbol="PRIVATEUSDT", interval="1h", **extra):
        return dict(venue="BINANCE", market="USDM", product_type="PERPETUAL",
                    native_symbol=symbol, feed="BAR", interval=interval,
                    source_policy_id="crypto_primary_v2", **extra)

    def members(self):
        return [{"binance_symbol": "BTCUSDT", "okx_inst_id": "BTC-USDT-SWAP"}]

    def test_checked_in_ownership_only_claims_exact_current_daily_rows(self):
        import json
        import yaml
        owned = json.loads(U.OWNERSHIP.read_text())["consumers"]
        demand = yaml.safe_load(U.DEMAND.read_text())
        universe = json.loads(U.UNIVERSE.read_text())
        self.assertEqual(sum(len(v) for v in owned.values()), 500)
        again, summary, ownership = U.sync_demand(demand, universe["members"], owned=owned)
        self.assertEqual(again, demand)
        self.assertEqual(ownership, owned)
        self.assertTrue(all(s["added"] == s["removed"] == 0 for s in summary.values()))

    def test_independent_bars_all_intervals_and_other_policies_survive(self):
        rows = [self.row(interval=i) for i in ("1h", "1d", "5m")]
        rows.append(dict(self.row(), source_policy_id="independent"))
        result, summary, owned = U.sync_demand(self.demand(rows), self.members())
        self.assertTrue(all(row in result["consumers"][0]["requirements"] for row in rows))
        self.assertEqual(summary["BINANCE"]["removed"], 0)
        self.assertEqual(len(owned["alpha.binance.paper.stable"]), 1)

    def test_both_venues_exact_owned_departures_and_idempotent_rerun(self):
        first, _, owned = U.sync_demand(self.demand(), self.members())
        again, summary, again_owned = U.sync_demand(first, self.members(), owned=owned)
        self.assertEqual(first, again)
        self.assertEqual(owned, again_owned)
        self.assertTrue(all(v["added"] == v["removed"] == 0 for v in summary.values()))
        gone, summary, gone_owned = U.sync_demand(first, [], owned=owned)
        self.assertTrue(all(not c["requirements"] for c in gone["consumers"]))
        self.assertTrue(all(v["removed"] == 1 for v in summary.values()))
        self.assertTrue(all(not v for v in gone_owned.values()))

    def test_preexisting_equivalent_row_is_borrowed_never_adopted(self):
        row = self.row("BTCUSDT", "1d")
        first, _, owned = U.sync_demand(self.demand([row]), self.members())
        self.assertFalse(owned["alpha.binance.paper.stable"])
        gone, _, _ = U.sync_demand(first, [], owned=owned)
        self.assertIn(row, gone["consumers"][0]["requirements"])

    def test_edited_owned_row_loses_ownership_and_is_preserved(self):
        first, _, owned = U.sync_demand(self.demand(), self.members())
        first["consumers"][0]["requirements"][0]["max_freshness_ms"] = 123
        gone, _, after = U.sync_demand(first, [], owned=owned)
        self.assertEqual(gone["consumers"][0]["requirements"][0]["max_freshness_ms"], 123)
        self.assertFalse(after["alpha.binance.paper.stable"])

    def test_missing_ledger_fails_safe_and_execution_demand_releases_ownership(self):
        first, _, owned = U.sync_demand(self.demand(), self.members())
        retained, summary, _ = U.sync_demand(first, [])
        self.assertEqual(first, retained)
        self.assertTrue(all(v["removed"] == 0 for v in summary.values()))
        trade = dict(self.row("BTCUSDT"), feed="TRADE", interval=None)
        first["consumers"][0]["requirements"].append(trade)
        retained, _, after = U.sync_demand(first, [], owned=owned)
        self.assertEqual(len(retained["consumers"][0]["requirements"]), 2)
        self.assertFalse(after["alpha.binance.paper.stable"])


if __name__ == "__main__":
    unittest.main()
