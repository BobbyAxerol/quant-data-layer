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
    ChangelogCoordinate,
    LogicalProductKey,
    SourceCoordinate,
    bar_revision_decision,
    cache_read_state,
    latest_apply_decision,
)
from qdl.query.contracts import DataRequirement
from qdl.query.v2 import query_pb2
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

    def test_malformed_claims_are_refused_with_the_shared_reason(self):
        # F4: ``re.match`` with ``$`` accepted a trailing newline; claims are
        # now full-matched, and a bool is never an integer.
        self.assertGreaterEqual(len(self.doc["claims_invalid"]), 4)
        for case in self.doc["claims_invalid"]:
            with self.subTest(case["name"]):
                with self.assertRaises(CursorInvalid) as raised:
                    CursorV3Claims(**case["claims"])
                self.assertEqual(raised.exception.reason, case["reason"])

    def test_array_or_object_schema_is_invalid_never_a_type_error(self):
        cases = {case["name"]: case for case in self.doc["cases"]}
        for name in ("schema_array", "schema_object"):
            case = cases[name]
            with self.subTest(name), self.assertRaises(CursorInvalid) as raised:
                self.codec.verify(
                    case["token"], consumer_id=case["consumer_id"],
                    environment=case["environment"],
                    requirement_digest_value=case["requirement_digest"],
                    expected=CursorV3Expectation(**case["expectation"]),
                    now_ns=case["now_ns"],
                )
            self.assertEqual(raised.exception.reason, "SCHEMA")

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


def _enum(wrapper, prefix: str, value) -> int:
    """A golden enum is a name, or a raw wire number for unknown values."""

    return value if isinstance(value, int) else wrapper.Value(prefix + value)


def _requirement_proto(value: dict) -> query_pb2.DataRequirement:
    proto = query_pb2.DataRequirement(
        instrument_uid=value["instrument_uid"], interval=value["interval"],
        source_policy_id=value["source_policy_id"], warmup_limit=value["warmup_limit"],
        max_freshness_ms=value["max_freshness_ms"],
        require_full_coverage=value["require_full_coverage"],
        require_final_bars=value["require_final_bars"],
        feed_type=_enum(query_pb2.FeedType, "FEED_TYPE_", value["feed"]),
        grade=_enum(query_pb2.ConsumerGrade, "CONSUMER_GRADE_", value["consumer_grade"]),
        stale_policy_type=_enum(query_pb2.StalePolicy, "STALE_POLICY_", value["stale_policy"]),
        gap_policy_type=_enum(query_pb2.GapPolicy, "GAP_POLICY_", value["gap_policy"]),
        recovery_policy=_enum(query_pb2.RecoveryPolicy, "RECOVERY_POLICY_", value["recovery"]),
        revision_policy=_enum(
            query_pb2.BarRevisionPolicy, "BAR_REVISION_POLICY_", value["bar_revision_policy"]),
        event_recency_policy=_enum(
            query_pb2.StalePolicy, "STALE_POLICY_", value["event_recency_policy"] or "UNSPECIFIED"),
        max_session_liveness_ms=value["max_session_liveness_ms"],
    )
    warmup = value["warmup"]
    if warmup is not None:
        spec = query_pb2.WarmupSpecification(
            interval_source_policy=_enum(
                query_pb2.IntervalSourcePolicy, "INTERVAL_SOURCE_POLICY_",
                warmup["interval_source_policy"]),
            max_cache_age_ms=warmup["max_cache_age_ms"], deadline_ms=warmup["deadline_ms"])
        if warmup["horizon"] == "rows":
            spec.rows = warmup["rows"]
        elif warmup["horizon"] == "time_range":
            spec.time_range.start_time_ns = warmup["start_time_ns"]
            spec.time_range.end_time_ns = warmup["end_time_ns"]
        proto.warmup.CopyFrom(spec)
    return proto


class RequirementValidationGoldenTests(unittest.TestCase):
    """F1: the Rust gateway validates with ``qdl_contracts::requirement``
    against the same file; the Python server path is the oracle."""

    def test_every_case_matches_the_python_server_path(self):
        from qdl.stream.grpc_service import requirement_from_proto

        doc = _load("requirement_validation.json")
        self.assertGreaterEqual(len(doc["cases"]), 40)
        for case in doc["cases"]:
            with self.subTest(case["name"]):
                proto = _requirement_proto(case["requirement"])
                if case["rule"] is None:
                    requirement_from_proto(proto)
                else:
                    # The rule that fires first, identified by its message, so
                    # the rejection order is pinned, not only the refusal.
                    with self.assertRaises(ValueError) as raised:
                        requirement_from_proto(proto)
                    self.assertTrue(
                        str(raised.exception).startswith(doc["rule_messages"][case["rule"]]),
                        str(raised.exception))

    def test_every_rule_is_exercised(self):
        doc = _load("requirement_validation.json")
        exercised = {case["rule"] for case in doc["cases"] if case["rule"]}
        self.assertEqual(exercised, set(doc["rules"]))


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

    def test_malformed_state_records_are_refused(self):
        # F4: bool/float/str where an integer is due, empty topics, a hash
        # with a trailing newline - all refused, as in the Rust decoders.
        for value in self.doc["source_coordinates_invalid"]:
            with self.subTest(source=value), self.assertRaises(ValueError):
                SourceCoordinate(**value)
        for value in self.doc["changelog_coordinates_invalid"]:
            with self.subTest(changelog=value), self.assertRaises(ValueError):
                ChangelogCoordinate(**value)
        for value in self.doc["changelog_coordinates_valid"]:
            ChangelogCoordinate(**value)
        for value in self.doc["bar_states_invalid"]:
            with self.subTest(bar=value), self.assertRaises(ValueError):
                source = value["source"]
                BarState(value["is_final"], value["revision"], value["content_sha256"],
                         SourceCoordinate(**source))

    def test_trailing_newline_never_passes_a_key_field(self):
        with self.assertRaises(ValueError):
            LogicalProductKey.parse("lpk1|paper|OKX|SWAP|x|TRADE|-\n")
        with self.assertRaises(ValueError):
            LogicalProductKey.for_product(
                environment="paper", venue="OKX", market="SWAP", instrument_uid="x\n",
                feed="TRADE", interval=None)

    def test_cache_read_state(self):
        for case in self.doc["cache_read_state"]:
            with self.subTest(case["name"]):
                self.assertEqual(
                    cache_read_state(case["ready_generation"], case["entry_generation"]).value,
                    case["state"])


if __name__ == "__main__":
    unittest.main()
