from __future__ import annotations

import json
import unittest
from decimal import Decimal
from pathlib import Path

from qdl.l2.conformance import (
    BookConfig,
    BookIdentity,
    BookLevelInput,
    ChecksumEvidence,
    ChecksumPolicy,
    L2BookReference,
    SequencePolicy,
    SnapshotOrigin,
    _parse_exact_decimal,
    canonical_decimal_text,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_PATH = ROOT / "tests/fixtures/phase104/l2_book_state_machine.json"


def _identity(value: dict[str, str]) -> BookIdentity:
    return BookIdentity(**value)


def _levels(values: list[dict[str, object]]) -> list[BookLevelInput]:
    return [BookLevelInput(**value) for value in values]


def _rows(levels: tuple[object, ...]) -> list[list[str]]:
    return [
        [canonical_decimal_text(level.price), canonical_decimal_text(level.quantity)]
        for level in levels
    ]


class Phase104L2BookConformanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture = json.loads(FIXTURE_PATH.read_text())

    def test_shared_protocol_fixture_is_fail_closed_and_matches_rust_oracle(self) -> None:
        self.assertEqual(self.fixture["schema_version"], 1)
        self.assertEqual(self.fixture["provenance"], "TEST_ONLY_SYNTHETIC_PROTOCOL_FIXTURE")
        for case in self.fixture["cases"]:
            with self.subTest(case=case["name"]):
                config = case["config"]
                core = L2BookReference(
                    BookConfig(
                        identity=_identity(config["key"]),
                        sequence_policy=SequencePolicy(config["sequence_policy"]),
                        checksum_policy=ChecksumPolicy(config["checksum_policy"]),
                        view_depth_per_side=config["view_depth_per_side"],
                    )
                )
                for action in case["actions"]:
                    expected = action["expect"]
                    outcome = self._apply(core, action, config["key"])
                    self.assertEqual(outcome, expected["outcome"], action["kind"])
                    self.assertEqual(core.status.value, expected["status"], action["kind"])
                    self.assertEqual(core.generation, expected["generation"], action["kind"])
                    self.assertEqual(core.last_sequence, expected["last_sequence"], action["kind"])
                    view = core.view()
                    self.assertEqual(view is not None, expected["ready"], action["kind"])
                    if view is not None:
                        self.assertEqual(_rows(view.bids), expected.get("bids", []))
                        self.assertEqual(_rows(view.asks), expected.get("asks", []))
                        self.assertEqual(view.truncated, expected.get("truncated", False))

    def test_exact_decimal_exponent_and_whole_text_share_one_price_key(self) -> None:
        self.assertEqual(canonical_decimal_text(Decimal("1000")), "1000")
        self.assertEqual(canonical_decimal_text(Decimal("1e3")), "1000")
        with self.assertRaisesRegex(ValueError, "invalid"):
            _parse_exact_decimal("1_0")

    @staticmethod
    def _apply(
        core: L2BookReference, action: dict[str, object], default_identity: dict[str, str]
    ) -> str:
        kind = action["kind"]
        if kind == "snapshot":
            return core.apply_snapshot(
                identity=_identity(action.get("identity", default_identity)),
                generation=action["generation"],
                sequence_end=action["sequence_end"],
                checksum=ChecksumEvidence(action.get("checksum", "NOT_PROVIDED")),
                origin=SnapshotOrigin(action.get("origin", "WEBSOCKET")),
                levels=_levels(action.get("levels", [])),
            )
        if kind == "delta":
            return core.apply_delta(
                identity=_identity(action.get("identity", default_identity)),
                generation=action["generation"],
                sequence_start=action.get("sequence_start"),
                previous_sequence=action.get("previous_sequence"),
                sequence_end=action["sequence_end"],
                checksum=ChecksumEvidence(action.get("checksum", "NOT_PROVIDED")),
                levels=_levels(action.get("levels", [])),
            )
        if kind == "request_resync":
            return core.request_resync(action["generation"])
        if kind == "disconnect":
            return core.disconnect()
        self.fail(f"unknown fixture action: {kind}")


if __name__ == "__main__":
    unittest.main()
