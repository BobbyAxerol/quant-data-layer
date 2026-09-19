"""R1.31: the refusal R1.30 asked for, so a regeneration cannot quietly drop a feed.

R1.28 applied a regenerated bundle only after diffing it against the running one
by hand. It caught the MARK_INDEX loss, and it is not a control: it worked because
someone looked. These tests pin the control that replaces looking.

The shape that matters is the fourth test. The Binance ingestor's real failure had
a *net count of zero* - five `@markPrice@1s` bindings out, five `@kline_1m` in - so
any check that compares totals passes it. The guard compares identities.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "assert_runtime_bundle_is_not_lossy.py"

_spec = importlib.util.spec_from_file_location("assert_runtime_bundle_is_not_lossy", SCRIPT)
assert _spec and _spec.loader
guard = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = guard
_spec.loader.exec_module(guard)


def _binding(source_id: str, channel: str, provider_kind: str) -> dict[str, object]:
    return {
        "source_id": source_id,
        "native_channel": channel,
        "provider_kind": provider_kind,
        "instrument_catalog_revision": 8,
        "venue": "BINANCE",
        "market": "USDM",
    }


def _write_bundle(root: Path, files: dict[str, dict]) -> Path:
    runtime = root / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    for name, document in files.items():
        (runtime / name).write_text(json.dumps(document, indent=2))
    return root


class LossyBundleGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    def _run(self, running: dict, proposed: dict, *extra: str) -> int:
        a = _write_bundle(self.tmp / "running", running)
        b = _write_bundle(self.tmp / "proposed", proposed)
        return guard.main(["--running", str(a), "--proposed", str(b), *extra])

    def test_a_bundle_that_only_adds_bindings_is_accepted(self) -> None:
        running = {"ingestor.json": {"bindings": [_binding("a", "x", "k")]}}
        proposed = {
            "ingestor.json": {
                "bindings": [_binding("a", "x", "k"), _binding("b", "y", "k")]
            }
        }
        self.assertEqual(self._run(running, proposed), 0)

    def test_an_identical_bundle_is_accepted(self) -> None:
        both = {"ingestor.json": {"bindings": [_binding("a", "x", "k")]}}
        self.assertEqual(self._run(both, both), 0)

    def test_bindings_nested_under_core_are_read(self) -> None:
        running = {"core.json": {"core": {"bindings": [_binding("a", "x", "k")]}}}
        proposed = {"core.json": {"core": {"bindings": []}}}
        self.assertEqual(self._run(running, proposed), 1)

    def test_a_removal_hidden_behind_an_equal_number_of_additions_is_refused(self) -> None:
        """The real R1.30 shape: five markPrice out, five kline in, net zero."""
        mark = [
            _binding(f"binance-usdm-{s}-mark_index_price-primary-v2", f"{s}@markPrice@1s", "binance_usdm_mark")
            for s in ("btcusdt", "ethusdt", "solusdt", "dogeusdt", "bnbusdt")
        ]
        kline = [
            _binding(f"binance-usdm-{s}-bar-1m-primary-v2", f"{s}@kline_1m", "binance_usdm_bar")
            for s in ("btcusdt", "ethusdt", "solusdt", "dogeusdt", "bnbusdt")
        ]
        keep = [_binding("binance-usdm-btcusdt-trade-primary-v2", "btcusdt@aggTrade", "binance_usdm_trade")]
        running = {"ingestor-binance-usdm.json": {"bindings": keep + mark}}
        proposed = {"ingestor-binance-usdm.json": {"bindings": keep + kline}}

        running_total = sum(len(v["bindings"]) for v in running.values())
        proposed_total = sum(len(v["bindings"]) for v in proposed.values())
        self.assertEqual(running_total, proposed_total, "the shape under test must be count-neutral")

        self.assertEqual(self._run(running, proposed), 1)

    def test_an_intended_removal_named_on_the_command_line_is_accepted(self) -> None:
        running = {"ingestor.json": {"bindings": [_binding("a", "x", "k"), _binding("b", "y", "k")]}}
        proposed = {"ingestor.json": {"bindings": [_binding("a", "x", "k")]}}
        self.assertEqual(self._run(running, proposed), 1)
        self.assertEqual(self._run(running, proposed, "--allow-removal", "b"), 0)

    def test_an_intended_removal_may_come_from_a_file(self) -> None:
        running = {"ingestor.json": {"bindings": [_binding("a", "x", "k"), _binding("b", "y", "k")]}}
        proposed = {"ingestor.json": {"bindings": [_binding("a", "x", "k")]}}
        allow = self.tmp / "allow.txt"
        allow.write_text("# de-scoped by owner\nb\n")
        self.assertEqual(self._run(running, proposed, "--allow-removal-file", str(allow)), 0)

    def test_a_binding_that_moves_to_a_sibling_file_is_a_move_not_a_loss(self) -> None:
        """Resharding across cores must not read as a lost feed."""
        running = {
            "core.json": {"core": {"bindings": [_binding("a", "x", "k"), _binding("b", "y", "k")]}},
            "core-002.json": {"core": {"bindings": []}},
        }
        proposed = {
            "core.json": {"core": {"bindings": [_binding("a", "x", "k")]}},
            "core-002.json": {"core": {"bindings": [_binding("b", "y", "k")]}},
        }
        self.assertEqual(self._run(running, proposed), 0)

    def test_a_proposed_bundle_missing_a_whole_file_is_refused(self) -> None:
        running = {
            "core.json": {"core": {"bindings": [_binding("a", "x", "k")]}},
            "ingestor.json": {"bindings": [_binding("b", "y", "k")]},
        }
        proposed = {"core.json": {"core": {"bindings": [_binding("a", "x", "k")]}}}
        self.assertEqual(self._run(running, proposed), 1)

    def test_a_running_bundle_with_no_bindings_is_an_input_error_not_a_pass(self) -> None:
        running = {"core.json": {"core": {}}}
        proposed = {"core.json": {"core": {"bindings": [_binding("a", "x", "k")]}}}
        self.assertEqual(self._run(running, proposed), 2)

    def test_a_missing_directory_is_an_input_error(self) -> None:
        a = _write_bundle(self.tmp / "running", {"core.json": {"bindings": [_binding("a", "x", "k")]}})
        self.assertEqual(
            guard.main(["--running", str(a), "--proposed", str(self.tmp / "nope")]), 2
        )

    def test_identities_are_indexed_from_every_known_binding_location(self) -> None:
        document = {
            "bindings": [_binding("top", "x", "k")],
            "core": {"bindings": [_binding("nested", "y", "k")]},
        }
        self.assertEqual(set(guard.binding_identities(document)), {"top", "nested"})

    def test_an_ingestor_binding_is_identified_by_its_subscription_id(self) -> None:
        """The gap the live bundle found: a first cut read only `source_id`.

        An ingestor binding has no `source_id` at all - it carries
        `subscription_id` for the same thing - so half of the R1.30 trap, and
        precisely the count-neutral half, went past the check unseen.
        """
        entry = {
            "subscription_id": "binance-usdm-btcusdt-mark_index_price-primary-v2",
            "native_channel": "btcusdt@markPrice@1s",
            "feed": "MARK_INDEX_PRICE",
        }
        self.assertIsNotNone(guard.binding_identity(entry))
        self.assertEqual(
            set(guard.binding_identities({"bindings": [entry]})),
            {"binance-usdm-btcusdt-mark_index_price-primary-v2"},
        )

    def test_the_ingestor_half_of_the_mark_index_trap_is_refused(self) -> None:
        """Same count-neutral shape as the core half, in the ingestor's own dialect."""
        def sub(source_id: str, channel: str) -> dict[str, object]:
            return {"subscription_id": source_id, "native_channel": channel, "feed": "X"}

        mark = [
            sub(f"binance-usdm-{s}-mark_index_price-primary-v2", f"{s}@markPrice@1s")
            for s in ("btcusdt", "ethusdt", "solusdt", "dogeusdt", "bnbusdt")
        ]
        kline = [
            sub(f"binance-usdm-{s}-bar-1m-primary-v2", f"{s}@kline_1m")
            for s in ("btcusdt", "ethusdt", "solusdt", "dogeusdt", "bnbusdt")
        ]
        running = {"ingestor-binance-usdm.json": {"bindings": mark}}
        proposed = {"ingestor-binance-usdm.json": {"bindings": kline}}
        self.assertEqual(len(mark), len(kline), "the shape under test must be count-neutral")
        self.assertEqual(self._run(running, proposed), 1)

    def test_a_source_id_is_preferred_when_a_binding_carries_both(self) -> None:
        entry = {"source_id": "canonical", "subscription_id": "other"}
        self.assertEqual(guard.binding_identity(entry), "canonical")

    def test_an_entry_without_any_identity_is_ignored_rather_than_crashing(self) -> None:
        document = {"bindings": [{"native_channel": "x"}, _binding("a", "x", "k"), "junk"]}
        self.assertEqual(set(guard.binding_identities(document)), {"a"})


if __name__ == "__main__":
    unittest.main()
