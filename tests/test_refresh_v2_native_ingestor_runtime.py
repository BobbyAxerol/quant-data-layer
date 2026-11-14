from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import (
    StableAcquisitionPlan,
    stable_authority_record,
    write_stable_runtime_bundle,
)
from scripts.refresh_v2_native_ingestor_runtime import (
    REALTIME_FEEDS,
    TARGETS,
    _validate_transition,
    refresh,
)


ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
ACQUISITION = ROOT / "config/v2/stable-acquisition-bindings.yaml"
RUST_IMAGE = "sha256:" + "a" * 64
REMOVED_BOOKS = {
    "binance-usdm": {"SOLUSDT", "DOGEUSDT", "BNBUSDT"},
    "okx-swap": {"SOL-USDT-SWAP", "DOGE-USDT-SWAP", "BNB-USDT-SWAP"},
}


class NativeIngestorRuntimeRefreshTests(unittest.TestCase):
    def _authority(self) -> dict[str, object]:
        return stable_authority_record(
            rust_image_digest=RUST_IMAGE,
            capability_manifest=ROOT / "config/v2/stable-capabilities.yaml",
            contract=ROOT / "contracts/proto/qdl/marketdata/v2/market_data.proto",
            partition_plan=ACQUISITION.read_bytes(),
            effective_at_ns=1,
            mode="RUST_PRIMARY",
            revision=1,
            slice_id="qdl-v2-native-ingestor-refresh-test",
            approved_by="test",
        )

    def _runtime(self, root: Path) -> Path:
        runtime = root / "runtime"
        catalog = StableSourceCatalog.load(CATALOG)
        acquisition = StableAcquisitionPlan.load(ACQUISITION, catalog=catalog)
        write_stable_runtime_bundle(
            runtime,
            catalog=catalog,
            acquisition=acquisition,
            authority=self._authority(),
        )
        for lane, file_name in TARGETS.items():
            path = runtime / file_name
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["config_revision"] = 14
            payload["bindings"] = [
                item
                for item in payload["bindings"]
                if item["feed"] != "BAR"
                and not (
                    item["feed"] == "BOOK"
                    and item["native_symbol"] in REMOVED_BOOKS[lane]
                )
            ]
            for item in payload["bindings"]:
                item["instrument_catalog_revision"] = catalog.catalog_revision - 1
            path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return runtime

    def _refresh(self, root: Path, *, apply: bool) -> dict:
        runtime = self._runtime(root)
        return refresh(
            runtime_dir=runtime,
            output_dir=root / "state" / "native-ingestor-refresh" if apply else None,
            apply=apply,
            state_root=root / "state",
        )

    def _expected_realtime_count(self, lane: str) -> int:
        catalog = StableSourceCatalog.load(CATALOG)
        acquisition = StableAcquisitionPlan.load(ACQUISITION, catalog=catalog)
        generated = acquisition.native_ingestor_configs(
            catalog=catalog,
            authority=self._authority(),
        )[lane]["bindings"]
        return sum(item["feed"] in REALTIME_FEEDS for item in generated)

    def test_dry_run_adds_only_the_three_declared_l2_books_per_venue(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            original = {
                name: (runtime / name).read_bytes()
                for name in TARGETS.values()
            }

            result = refresh(
                runtime_dir=runtime,
                output_dir=None,
                apply=False,
                state_root=root / "state",
            )

            self.assertEqual(result["status"], "DRY_RUN")
            self.assertEqual(result["production_mutations"], 0)
            self.assertTrue(result["authority_bytes_preserved"])
            self.assertEqual(
                result["files"]["ingestor-binance-usdm.json"]["added_book_symbols"],
                ["BNBUSDT", "DOGEUSDT", "SOLUSDT"],
            )
            self.assertEqual(
                result["files"]["ingestor-okx-swap.json"]["added_book_symbols"],
                ["BNB-USDT-SWAP", "DOGE-USDT-SWAP", "SOL-USDT-SWAP"],
            )
            self.assertEqual(
                {
                    name: (runtime / name).read_bytes()
                    for name in TARGETS.values()
                },
                original,
            )

    def test_apply_preserves_authority_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            authority_before = (runtime / "authority.json").read_bytes()
            before_modes = {
                file_name: (runtime / file_name).stat().st_mode & 0o777
                for file_name in TARGETS.values()
            }
            result = refresh(
                runtime_dir=runtime,
                output_dir=root / "state" / "native-ingestor-refresh",
                apply=True,
                state_root=root / "state",
            )

            self.assertEqual(result["status"], "APPLIED")
            self.assertEqual(result["production_mutations"], 2)
            self.assertEqual((runtime / "authority.json").read_bytes(), authority_before)
            for file_name in TARGETS.values():
                backup = root / "state" / "native-ingestor-refresh" / "rollback" / file_name
                self.assertTrue(backup.is_file())
                self.assertEqual(
                    backup.stat().st_mode & 0o777,
                    before_modes[file_name],
                )
                self.assertEqual(
                    result["files"][file_name]["before_mode"],
                    oct(before_modes[file_name]),
                )
                payload = json.loads((runtime / file_name).read_text(encoding="utf-8"))
                lane = next(name for name, value in TARGETS.items() if value == file_name)
                self.assertEqual(
                    len(payload["bindings"]),
                    self._expected_realtime_count(lane),
                )
                self.assertFalse(any(item["feed"] == "BAR" for item in payload["bindings"]))
                mark_index = [
                    item for item in payload["bindings"] if item["feed"] == "MARK_INDEX"
                ]
                self.assertEqual(
                    len({
                        (item["subscription_id"], item["native_symbol"], item["native_channel"])
                        for item in mark_index
                    }),
                    len(mark_index),
                )

            repeated = refresh(
                runtime_dir=runtime,
                output_dir=None,
                apply=False,
                state_root=root / "state",
            )
            self.assertEqual(repeated["production_mutations"], 0)
            self.assertTrue(all(not item["changed"] for item in repeated["files"].values()))

    def test_refuses_trade_or_quote_contract_drift(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            path = runtime / "ingestor-binance-usdm.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            binding = next(item for item in payload["bindings"] if item["feed"] == "TRADE")
            binding["native_channel"] = "unexpected-channel"
            path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "TRADE binding contract"):
                refresh(
                    runtime_dir=runtime,
                    output_dir=None,
                    apply=False,
                    state_root=root / "state",
                )

    def test_refuses_non_binding_runtime_drift(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            path = runtime / "ingestor-okx-swap.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["heartbeat_seconds"] = 1
            path.write_text(json.dumps(payload), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "non-binding runtime field"):
                refresh(
                    runtime_dir=runtime,
                    output_dir=None,
                    apply=False,
                    state_root=root / "state",
                )

    def test_additions_keep_retained_bindings_and_operator_fields_verbatim(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            path = runtime / "ingestor-okx-swap.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            payload["book_resync"] = {"consumer_group_prefix": "p", "quarantine_stream": "q"}
            bar = dict(next(item for item in payload["bindings"] if item["feed"] == "QUOTE"))
            bar.update(feed="BAR", native_channel="candle1m", subscription_id="okx-test-bar-1m")
            payload["bindings"].append(bar)
            quote = next(item for item in payload["bindings"] if item["feed"] == "QUOTE")
            payload["bindings"].remove(quote)
            path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            retained = [item for item in payload["bindings"]]

            refresh(runtime_dir=runtime, output_dir=root / "state" / "r", apply=True, state_root=root / "state")

            after = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(after["book_resync"], payload["book_resync"])
            self.assertEqual(after["bindings"][: len(retained)], retained)
            added = after["bindings"][len(retained):]
            catalog = StableSourceCatalog.load(CATALOG)
            self.assertIn(quote["subscription_id"], {item["subscription_id"] for item in added})
            self.assertTrue(all(item["instrument_catalog_revision"] == catalog.catalog_revision for item in added))
            self.assertTrue(all(
                item["instrument_catalog_revision"] == catalog.catalog_revision - 1 for item in retained
                if item["feed"] != "BAR"
            ))
            self.assertIn(bar, after["bindings"])
            self.assertGreater(after["config_revision"], payload["config_revision"])

    def test_file_without_additions_is_left_byte_identical(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            first = refresh(runtime_dir=runtime, output_dir=root / "state" / "a", apply=True, state_root=root / "state")
            self.assertEqual(first["production_mutations"], 2)
            before = {name: (runtime / name).read_bytes() for name in TARGETS.values()}
            again = refresh(runtime_dir=runtime, output_dir=None, apply=False, state_root=root / "state")
            for name in TARGETS.values():
                self.assertFalse(again["files"][name]["changed"])
                self.assertEqual((runtime / name).read_bytes(), before[name])

    def test_refuses_removing_an_existing_subscription(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            path = runtime / "ingestor-okx-swap.json"
            payload = json.loads(path.read_text(encoding="utf-8"))
            extra = dict(next(item for item in payload["bindings"] if item["feed"] == "QUOTE"))
            extra["subscription_id"] = "okx-retired-quote"
            payload["bindings"].append(extra)
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "removes an existing QUOTE subscription"):
                refresh(runtime_dir=runtime, output_dir=None, apply=False, state_root=root / "state")

    def test_refuses_okx_lane_renumbering(self):
        def binding(feed: str, channel: str, number: int) -> dict:
            return {"feed": feed, "native_channel": channel, "native_symbol": f"S{number}-USDT-SWAP",
                    "subscription_id": f"okx-{feed.lower()}-{number}", "instrument_catalog_revision": 1}

        base = {"config_revision": 1, "max_subscriptions_per_connection": 2,
                "bindings": [binding("BOOK", "books", 1), binding("BOOK", "books", 2),
                             binding("QUOTE", "bbo-tbt", 3)]}
        grow_quote = dict(base, config_revision=2,
                          bindings=base["bindings"] + [binding("QUOTE", "bbo-tbt", 4), binding("QUOTE", "bbo-tbt", 5)])
        successor, change = _validate_transition(active=base, expected=grow_quote, target=TARGETS["okx-swap"])
        self.assertEqual(change["okx_lanes_after"], ["okx-public-001", "okx-public-002", "okx-public-003"])
        grow_book = dict(base, config_revision=2, bindings=base["bindings"] + [binding("BOOK", "books", 6)])
        with self.assertRaisesRegex(ValueError, "renames an existing OKX lane"):
            _validate_transition(active=base, expected=grow_book, target=TARGETS["okx-swap"])

    def test_apply_requires_a_new_state_root_output(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            runtime = self._runtime(root)
            with self.assertRaisesRegex(ValueError, "new private QDL state path"):
                refresh(
                    runtime_dir=runtime,
                    output_dir=root / "outside-state",
                    apply=True,
                    state_root=root / "state",
                )


if __name__ == "__main__":
    unittest.main()
