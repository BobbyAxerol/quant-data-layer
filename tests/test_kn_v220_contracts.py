"""KN-1 K1.3 shared contracts: Python side of the golden vectors.

``contracts/golden/kn_v220/*.json`` is the cross-language oracle: the Rust
tests in ``rust/qdl-contracts`` read the same files. Expected outcomes were
specified by hand; token bytes and digests were produced once and reviewed.
Any change that alters them is a contract change, not a refactor.
"""

from __future__ import annotations

import json
from pathlib import Path
import unittest

from qdl.projection.state_contract import (
    BarState,
    LogicalProductKey,
    SourceCoordinate,
    bar_revision_decision,
    cache_read_state,
    latest_apply_decision,
)
from qdl.query.contracts import DataRequirement
from qdl.replay.cursor_v3 import (
    CursorInvalid,
    CursorV3Claims,
    CursorV3Expectation,
    CursorV3Expired,
    SignedCursorV3Codec,
    requirement_digest,
)

GOLDEN = Path(__file__).resolve().parents[1] / "contracts/golden/kn_v220"


def _load(name: str) -> dict:
    return json.loads((GOLDEN / name).read_text(encoding="utf-8"))


class CursorV3GoldenTests(unittest.TestCase):
    def setUp(self) -> None:
        self.doc = _load("cursor_v3.json")
        keys = {key: value.encode() for key, value in self.doc["keys"].items()}
        self.codec = SignedCursorV3Codec(keys, active_key_id=self.doc["active_key_id"])

    def test_canonical_body_and_token_bytes(self):
        claims = CursorV3Claims(**self.doc["canonical"]["claims"])
        self.assertEqual(claims.body().decode(), self.doc["canonical"]["body"])
        self.assertEqual(self.codec.encode(claims), self.doc["canonical"]["token"])

    def test_every_verification_case(self):
        for case in self.doc["cases"]:
            with self.subTest(case["name"]):
                try:
                    claims = self.codec.verify(
                        case["token"], consumer_id=case["consumer_id"],
                        environment=case["environment"],
                        requirement_digest_value=case["requirement_digest"],
                        expected=CursorV3Expectation(**case["expectation"]),
                        now_ns=case["now_ns"],
                    )
                    outcome = ("OK", None)
                    # u64/ns claims survive exactly (no float round trip).
                    self.assertEqual(claims.body(), CursorV3Claims(**{
                        k: v for k, v in json.loads(claims.body()).items() if k != "schema"}).body())
                except CursorV3Expired as error:
                    outcome = ("EXPIRED", error.reason)
                except CursorInvalid as error:
                    outcome = ("INVALID", error.reason)
                self.assertEqual(outcome, (case["outcome"], case["reason"]))

    def test_expired_maps_to_sdk_recovery_and_invalid_does_not(self):
        from qdl.transport.contracts import CursorExpired

        self.assertTrue(issubclass(CursorV3Expired, CursorExpired))
        self.assertFalse(issubclass(CursorInvalid, CursorExpired))

    def test_precision_of_the_largest_offset(self):
        claims = CursorV3Claims(**self.doc["canonical"]["claims"])
        self.assertEqual(claims.source_offset, 9_223_372_036_854_775_806)
        self.assertIn(b'"source_offset":9223372036854775806', claims.body())

    def test_only_the_active_key_signs(self):
        claims = dict(self.doc["canonical"]["claims"], key_id="kn1-test-k1")
        with self.assertRaises(ValueError):
            self.codec.encode(CursorV3Claims(**claims))


class RequirementDigestGoldenTests(unittest.TestCase):
    def test_vectors(self):
        for vector in _load("requirement_digest.json")["vectors"]:
            with self.subTest(vector["name"]):
                requirement = DataRequirement.from_mapping(dict(vector["requirement"]))
                self.assertEqual(requirement_digest(requirement), vector["digest"])

    def test_same_feed_on_two_venues_never_shares_a_digest(self):
        vectors = {v["name"]: v["digest"] for v in _load("requirement_digest.json")["vectors"]}
        self.assertNotEqual(vectors["binance_usdm_btc_trade"], vectors["okx_swap_btc_trade"])
        self.assertEqual(vectors["okx_swap_btc_bar_1m_warm500"], vectors["okx_swap_btc_bar_1m_warm5000"])


class StateContractGoldenTests(unittest.TestCase):
    def setUp(self) -> None:
        self.doc = _load("state_contract.json")

    def test_logical_product_keys(self):
        for vector in self.doc["logical_product_keys"]:
            with self.subTest(vector["name"]):
                key = LogicalProductKey.for_product(**vector["product"])
                self.assertEqual(key.encode(), vector["key"])
                self.assertEqual(LogicalProductKey.parse(vector["key"]), key)
        for value in self.doc["logical_product_keys_invalid"]:
            with self.subTest(value), self.assertRaises(ValueError):
                LogicalProductKey.parse(value)

    def test_book_snapshot_and_delta_are_distinct_products(self):
        keys = {v["name"]: v["key"] for v in self.doc["logical_product_keys"]}
        self.assertNotEqual(keys["okx_swap_btc_book_snapshot"], keys["okx_swap_btc_book_delta"])

    def test_latest_apply(self):
        for case in self.doc["latest_apply"]:
            with self.subTest(case["name"]):
                current = None if case["current"] is None else SourceCoordinate(**case["current"])
                self.assertEqual(
                    latest_apply_decision(current, SourceCoordinate(**case["incoming"])).value,
                    case["decision"])

    def test_bar_revision(self):
        def state(value):
            if value is None:
                return None
            return BarState(value["is_final"], value["revision"], value["content_sha256"],
                            SourceCoordinate(**value["source"]))

        for case in self.doc["bar_revision"]:
            with self.subTest(case["name"]):
                self.assertEqual(
                    bar_revision_decision(state(case["current"]), state(case["incoming"])).value,
                    case["decision"])

    def test_cache_read_state(self):
        for case in self.doc["cache_read_state"]:
            with self.subTest(case["name"]):
                self.assertEqual(
                    cache_read_state(case["ready_generation"], case["entry_generation"]).value,
                    case["state"])


if __name__ == "__main__":
    unittest.main()
