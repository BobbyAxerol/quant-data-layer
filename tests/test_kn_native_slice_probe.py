"""KN-1 review F3: the native slice probe passes only on an explicit predicate.

``scripts/kn_native_slice_probe.py run`` used to exit 0 whatever it observed.
These tests pin ``slice_verdict`` (one failing condition at a time) and the
process exit code.
"""

from __future__ import annotations

import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location("kn_native_slice_probe", ROOT / "scripts/kn_native_slice_probe.py")
assert _SPEC is not None and _SPEC.loader is not None
PROBE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = PROBE
_SPEC.loader.exec_module(PROBE)


def _product(name: str) -> dict:
    return {"product": name, "records": 10, "decode_errors": 0, "token_errors": 0,
            "offsets_strictly_increasing": True, "resume_exactly_next_record": True,
            "digest_python_equals_proto_path": True, "controls": ["REPLAYING", "LIVE"]}


PRODUCTS = ["okx", "binance"]


def _passing() -> dict:
    negatives = [{"case": name, "expected": status, "observed": status, "pass": True}
                 for name, status in PROBE.NEGATIVE_CASES.items()]
    return {"products": [_product(name) for name in PRODUCTS], "negatives": negatives,
            "negatives_pass": len(negatives), "negatives_total": len(negatives)}


class SliceVerdictTests(unittest.TestCase):
    def test_a_complete_result_passes(self):
        self.assertEqual(PROBE.slice_verdict(_passing(), expected_products=PRODUCTS), [])

    def test_each_failing_condition_is_reported(self):
        product_mutations = {
            "no records": ("records", 0),
            "decode_errors": ("decode_errors", 1),
            "token_errors": ("token_errors", 2),
            "not strictly increasing": ("offsets_strictly_increasing", False),
            "exactly the next record": ("resume_exactly_next_record", False),
            "digest differs": ("digest_python_equals_proto_path", False),
            "REPLAYING then LIVE": ("controls", ["LIVE", "REPLAYING"]),
        }
        for expected, (field, value) in product_mutations.items():
            with self.subTest(field=field):
                result = _passing()
                result["products"][1][field] = value
                failures = PROBE.slice_verdict(result, expected_products=PRODUCTS)
                self.assertEqual(len(failures), 1, failures)
                self.assertIn(expected, failures[0])
                self.assertTrue(failures[0].startswith("binance:"))

    def test_missing_controls_or_fields_fail_closed(self):
        result = _passing()
        result["products"][0]["controls"] = ["REPLAYING"]
        del result["products"][1]["token_errors"]
        failures = PROBE.slice_verdict(result, expected_products=PRODUCTS)
        self.assertEqual(len(failures), 2, failures)

    def test_a_missing_product_fails(self):
        result = _passing()
        result["products"].pop()
        self.assertIn("products: missing ['binance']", PROBE.slice_verdict(result, expected_products=PRODUCTS))
        self.assertTrue(PROBE.slice_verdict({"products": [], "negatives": _passing()["negatives"]},
                                            expected_products=[]))

    def test_duplicate_or_unexpected_products_fail(self):
        # Astra R2 counterexample: two copies of one product for two expected.
        result = _passing()
        result["products"] = [_product("okx"), _product("okx")]
        failures = PROBE.slice_verdict(result, expected_products=PRODUCTS)
        self.assertIn("products: duplicated ['okx']", failures)
        self.assertIn("products: missing ['binance']", failures)
        result = _passing()
        result["products"].append(_product("other"))
        self.assertIn("products: unexpected ['other']", PROBE.slice_verdict(result, expected_products=PRODUCTS))

    def test_a_wrong_or_missing_negative_fails(self):
        result = _passing()
        result["negatives"][3]["observed"] = "OK"
        failures = PROBE.slice_verdict(result, expected_products=PRODUCTS)
        name = result["negatives"][3]["case"]
        self.assertEqual(failures, [f"negative {name}: expected UNAUTHENTICATED, observed OK"])
        result = _passing()
        dropped = result["negatives"].pop()["case"]
        self.assertEqual(PROBE.slice_verdict(result, expected_products=PRODUCTS),
                         [f"negatives: missing ['{dropped}']"])

    def test_empty_negative_objects_fail(self):
        # Astra R2 counterexample: 24 empty objects compared None == None.
        result = _passing()
        result["negatives"] = [{} for _ in range(PROBE.EXPECTED_NEGATIVES)]
        failures = PROBE.slice_verdict(result, expected_products=PRODUCTS)
        self.assertTrue(any(f.startswith("negatives: missing") for f in failures), failures)
        # An entry without observed/expected fields fails too.
        result = _passing()
        del result["negatives"][0]["observed"]
        self.assertEqual(len(PROBE.slice_verdict(result, expected_products=PRODUCTS)), 1)

    def test_repeated_negative_case_fails(self):
        # Astra R2 counterexample: the same case 24 times.
        result = _passing()
        result["negatives"] = [dict(result["negatives"][0]) for _ in range(PROBE.EXPECTED_NEGATIVES)]
        failures = PROBE.slice_verdict(result, expected_products=PRODUCTS)
        self.assertIn(f"negatives: duplicated ['{result['negatives'][0]['case']}']", failures)
        self.assertTrue(any(f.startswith("negatives: missing") for f in failures))

    def test_an_expected_status_not_in_the_authoritative_table_fails(self):
        result = _passing()
        result["negatives"][0]["expected"] = result["negatives"][0]["observed"] = "OK"
        self.assertEqual(len(PROBE.slice_verdict(result, expected_products=PRODUCTS)), 1)

    def test_malformed_sections_fail_closed(self):
        self.assertTrue(PROBE.slice_verdict({}, expected_products=PRODUCTS))
        result = _passing()
        result["negatives"] = None
        self.assertIn("negatives: missing or not a list", PROBE.slice_verdict(result, expected_products=PRODUCTS))
        result = _passing()
        result["products"][0]["records"] = True
        self.assertEqual(len(PROBE.slice_verdict(result, expected_products=PRODUCTS)), 1)


class SliceExitCodeTests(unittest.TestCase):
    def _run(self, result: dict) -> tuple[int, dict]:
        with tempfile.TemporaryDirectory() as directory:
            probes = Path(directory) / "probes.json"
            probes.write_text(json.dumps([{"physical_key": name} for name in PRODUCTS]), encoding="utf-8")
            out = Path(directory) / "result.json"

            async def fake_main_async(args):
                return copy.deepcopy(result)

            argv = ["run", "--target", "t", "--profile", "p", "--bundle", "b", "--cursor-keys", "k",
                    "--probes", str(probes), "--topic-id", "x", "--route-generation", "r",
                    "--quota-redis-url", "redis://unused", "--quota-prefix", "q", "--source-mode", "capture",
                    "--out", str(out)]
            with mock.patch.object(PROBE, "main_async", fake_main_async), \
                    contextlib.redirect_stderr(io.StringIO()):
                code = PROBE.main(argv)
            return code, json.loads(out.read_text(encoding="utf-8"))

    def test_pass_exits_zero_and_records_the_verdict(self):
        code, written = self._run(_passing())
        self.assertEqual(code, 0)
        self.assertEqual(written["verdict"], {"pass": True, "failures": []})

    def test_astra_counterexamples_exit_non_zero(self):
        empty = _passing()
        empty["negatives"] = [{} for _ in range(PROBE.EXPECTED_NEGATIVES)]
        repeated = _passing()
        repeated["negatives"] = [dict(repeated["negatives"][0]) for _ in range(PROBE.EXPECTED_NEGATIVES)]
        duplicate_product = _passing()
        duplicate_product["products"] = [_product("okx"), _product("okx")]
        for name, result in (("empty", empty), ("repeated", repeated), ("duplicate_product", duplicate_product)):
            with self.subTest(name):
                code, written = self._run(result)
                self.assertEqual(code, 1)
                self.assertFalse(written["verdict"]["pass"])

    def test_any_failure_exits_non_zero(self):
        result = _passing()
        result["products"][0]["token_errors"] = 1
        code, written = self._run(result)
        self.assertEqual(code, 1)
        self.assertFalse(written["verdict"]["pass"])
        self.assertEqual(len(written["verdict"]["failures"]), 1)


if __name__ == "__main__":
    unittest.main()


def _item(offset, policy="LOSSLESS", key=None, signature=("a",), filtered="no"):
    return {"offset": offset, "partition": 0, "policy": policy, "key": key, "signature": signature,
            "filtered": filtered}


class MatrixJudgeTests(unittest.TestCase):
    """KN-2 K2.5: per-subscription exactness against the Kafka oracle."""

    def test_an_exact_lossless_delivery_is_clean(self):
        counts = PROBE.judge_subscription([_item(3), _item(5), _item(9)], [3, 5, 9])
        self.assertEqual(set(counts.values()), {0})

    def test_every_lossless_defect_is_counted(self):
        expected = [_item(3), _item(5), _item(9)]
        self.assertEqual(PROBE.judge_subscription(expected, [3, 9])["missing_lossless"], 1)
        self.assertEqual(PROBE.judge_subscription(expected, [3, 5, 5, 9])["duplicates"], 1)
        self.assertEqual(PROBE.judge_subscription(expected, [5, 3, 9])["out_of_order"], 1)
        self.assertEqual(PROBE.judge_subscription(expected, [3, 5, 7, 9])["unexpected"], 1)

    def test_a_coalesced_record_must_be_superseded_by_a_delivered_one(self):
        latest = [_item(1, "LATEST_STATE"), _item(2, "LATEST_STATE"), _item(3, "LATEST_STATE")]
        self.assertEqual(PROBE.judge_subscription(latest, [3])["unsuperseded_drops"], 0)
        # The newest record itself was dropped: nothing supersedes it.
        self.assertEqual(PROBE.judge_subscription(latest, [1, 2])["unsuperseded_drops"], 1)
        # A quality transition is a different signature: dropping the last
        # record of the old state is a defect.
        transition = [_item(1, "LATEST_STATE", signature=("a",)), _item(2, "LATEST_STATE", signature=("b",))]
        self.assertEqual(PROBE.judge_subscription(transition, [2])["unsuperseded_drops"], 1)
        # In-progress bars supersede only within one open time.
        bars = [_item(1, "LIFECYCLE_COALESCE", key=60), _item(2, "LOSSLESS", key=120)]
        self.assertEqual(PROBE.judge_subscription(bars, [2])["unsuperseded_drops"], 1)

    def test_age_filtered_records_must_not_be_delivered(self):
        expected = [_item(1, filtered="must"), _item(2, filtered="either"), _item(3)]
        counts = PROBE.judge_subscription(expected, [1, 3])
        self.assertEqual(counts["delivered_filtered"], 1)
        self.assertEqual(counts["missing_lossless"], 0)
        self.assertEqual(PROBE.judge_subscription(expected, [3])["missing_lossless"], 0)


def _matrix_passing(ids):
    subscriptions = [{"id": name, "reached_live": True, "errors": [], "failovers": [],
                      "duplicates": 0, "out_of_order": 0, "unexpected": 0, "missing_lossless": 0,
                      "unsuperseded_drops": 0, "delivered_filtered": 0, "token_errors": 0, "cross_mix": 0}
                     for name in ids]
    subscriptions[0]["failovers"] = [{"at_ms": 1.0, "resumed": True, "rto_ms": 5.0}]
    subscriptions[0]["errors"] = [{"code": "DEPENDENCY_UNAVAILABLE", "detail": "replica gone"}]
    negatives = [{"case": name, "expected": status, "observed": status}
                 for name, status in PROBE.NEGATIVE_CASES.items()]
    rpcs = [{"rpc": "Replay", "consumer_id": "c", "pass": True, "detail": "ok"}]
    return {"subscriptions": subscriptions, "negatives": negatives, "rpcs": rpcs, "failover_expected": 1}


class MatrixVerdictTests(unittest.TestCase):
    IDS = ["c|k1|TRADE|-", "c|k2|QUOTE|-", "d|k3|BAR|1m"]

    def test_a_complete_matrix_passes(self):
        self.assertEqual(PROBE.matrix_verdict(_matrix_passing(self.IDS), expected_ids=self.IDS), [])

    def test_coverage_defects_fail(self):
        result = _matrix_passing(self.IDS)
        result["subscriptions"].pop()
        self.assertTrue(any("missing" in item for item in PROBE.matrix_verdict(result, expected_ids=self.IDS)))
        result = _matrix_passing(self.IDS)
        result["subscriptions"].append(dict(result["subscriptions"][1]))
        self.assertTrue(any("duplicated" in item for item in PROBE.matrix_verdict(result, expected_ids=self.IDS)))
        result = _matrix_passing(self.IDS)
        result["subscriptions"] = [{} for _ in self.IDS]
        self.assertTrue(PROBE.matrix_verdict(result, expected_ids=self.IDS))
        self.assertTrue(PROBE.matrix_verdict(_matrix_passing(self.IDS), expected_ids=[]))

    def test_exactness_errors_and_failover_defects_fail(self):
        for field in ("duplicates", "missing_lossless", "unsuperseded_drops", "token_errors", "cross_mix"):
            with self.subTest(field):
                result = _matrix_passing(self.IDS)
                result["subscriptions"][2][field] = 1
                self.assertEqual(len(PROBE.matrix_verdict(result, expected_ids=self.IDS)), 1)
        result = _matrix_passing(self.IDS)
        result["subscriptions"][1]["errors"] = [{"code": "CURSOR_EXPIRED", "detail": "false expiry"}]
        self.assertEqual(len(PROBE.matrix_verdict(result, expected_ids=self.IDS)), 1)
        result = _matrix_passing(self.IDS)
        result["subscriptions"][0]["failovers"][0]["resumed"] = False
        self.assertEqual(len(PROBE.matrix_verdict(result, expected_ids=self.IDS)), 1)
        result = _matrix_passing(self.IDS)
        result["subscriptions"][0]["failovers"] = []
        self.assertIn("failover: no subscription failed over (replica A was not killed?)",
                      PROBE.matrix_verdict(result, expected_ids=self.IDS))
        result = _matrix_passing(self.IDS)
        result["subscriptions"][1]["reached_live"] = False
        self.assertEqual(len(PROBE.matrix_verdict(result, expected_ids=self.IDS)), 1)

    def test_negatives_and_rpcs_are_exact(self):
        result = _matrix_passing(self.IDS)
        result["negatives"].pop()
        self.assertTrue(PROBE.matrix_verdict(result, expected_ids=self.IDS))
        result = _matrix_passing(self.IDS)
        result["rpcs"] = []
        self.assertIn("rpcs: Replay/GetSnapshot/GetFeedStatus not checked",
                      PROBE.matrix_verdict(result, expected_ids=self.IDS))
        result = _matrix_passing(self.IDS)
        result["rpcs"][0]["pass"] = False
        self.assertEqual(len(PROBE.matrix_verdict(result, expected_ids=self.IDS)), 1)


class ExpectedDeliveryTests(unittest.TestCase):
    def test_product_identity_and_the_strict_freshness_predicate(self):
        from types import SimpleNamespace

        from qdl.marketdata.v2 import market_data_pb2

        now = 10_000_000_000_000
        def envelope(kind, age_ms, interval="1m"):
            value = market_data_pb2.EventEnvelope(source_event_time_ns=now - age_ms * 1_000_000)
            if kind == "bar":
                value.bar.interval = interval
                value.bar.close_time_ns = now - age_ms * 1_000_000
                value.bar.lifecycle = 2
            else:
                value.trade.native_trade_id = "t"
            return value.SerializeToString()

        records = [(0, 1, envelope("trade", 10)), (0, 2, envelope("bar", 10)),
                   (0, 3, envelope("bar", 10, "5m")), (0, 4, envelope("bar", 600_000)), (0, 5, b"")]
        strict = SimpleNamespace(max_freshness_ms=180_000,
                                 effective_event_recency_policy=SimpleNamespace(value="BLOCK"))
        row = {"feed": "BAR", "interval": "1m", "requirement": strict}
        items = PROBE.expected_delivery(row, records, 0, now)
        self.assertEqual([(item["offset"], item["filtered"]) for item in items], [(2, "no"), (4, "must")])
        # A record fresh at the start but past the bound by the end: either.
        aging = PROBE.expected_delivery(row, records, 0, now, now + 200_000 * 1_000_000)
        self.assertEqual([item["filtered"] for item in aging], ["either", "must"])
        observe = SimpleNamespace(max_freshness_ms=180_000,
                                  effective_event_recency_policy=SimpleNamespace(value="OBSERVE"))
        items = PROBE.expected_delivery({**row, "requirement": observe}, records, 0, now)
        self.assertEqual([item["filtered"] for item in items], ["no", "no"])


class CommitToClientTests(unittest.TestCase):
    def test_latency_pairs_by_coordinate_and_splits_catchup_from_live(self):
        # The same event id at offsets 7 and 9: a skipped copy cannot shift
        # the pairing, because a delivery is matched by its coordinate.
        commits = {(0, 7): 10, (0, 9): 1000, (1, 3): 400}
        received = [
            (0, 9, 1005, 500),   # committed after LIVE (500): live path
            (1, 3, 600, 500),    # committed before LIVE, delivered after: catch-up
            (0, 7, 20, None),    # during replay: neither
            (2, 1, 700, 500),    # no commit logged (history phase): ignored
        ]
        live, catchup = PROBE.commit_to_client_ms(received, commits)
        self.assertEqual(live, [5 / 1e6])
        self.assertEqual(catchup, [200 / 1e6])


class MatrixSubsetTests(unittest.TestCase):
    def test_a_client_process_without_checks_is_judged_on_its_streams_only(self):
        ids = ["c|k1|TRADE|-"]
        result = _matrix_passing(ids)
        result["negatives"], result["rpcs"], result["checks"] = [], [], 0
        self.assertEqual(PROBE.matrix_verdict(result, expected_ids=ids), [])
        result["checks"] = 1
        self.assertTrue(PROBE.matrix_verdict(result, expected_ids=ids))


class ContiguousOracleTests(unittest.TestCase):
    """Astra KN-2 R1 F5: the oracle must not copy the reducer's old rule."""

    def _states(self, *signatures):
        return [_item(index + 1, "LATEST_STATE", signature=(value,)) for index, value in enumerate(signatures)]

    def test_a_b_a_b_reduced_to_the_last_two_is_a_defect(self):
        expected = self._states("a", "b", "a", "b")
        counts = PROBE.judge_subscription(expected, [3, 4])
        self.assertEqual(counts["unsuperseded_drops"], 2)
        self.assertEqual(PROBE.judge_subscription(expected, [1, 2, 3, 4])["unsuperseded_drops"], 0)

    def test_a_b_a_keeps_every_transition(self):
        expected = self._states("a", "b", "a")
        self.assertEqual(PROBE.judge_subscription(expected, [2, 3])["unsuperseded_drops"], 1)
        self.assertEqual(PROBE.judge_subscription(expected, [1, 2, 3])["unsuperseded_drops"], 0)

    def test_same_state_runs_collapse_to_their_last_record(self):
        expected = self._states("a", "a", "a", "b", "b", "a")
        self.assertEqual(PROBE.judge_subscription(expected, [3, 5, 6])["unsuperseded_drops"], 0)
        # Dropping a run's last record loses the transition out of it.
        self.assertEqual(PROBE.judge_subscription(expected, [2, 5, 6])["unsuperseded_drops"], 1)

    def test_a_record_rejected_at_push_does_not_separate_runs(self):
        expected = [_item(1, "LATEST_STATE", signature=("a",)),
                    _item(2, "LATEST_STATE", signature=("b",), filtered="either"),
                    _item(3, "LATEST_STATE", signature=("a",))]
        self.assertEqual(PROBE.judge_subscription(expected, [3])["unsuperseded_drops"], 0)
        # Delivered after all, it is a real transition between the runs.
        self.assertEqual(PROBE.judge_subscription(expected, [2, 3])["unsuperseded_drops"], 1)


class CoverageTests(unittest.TestCase):
    def test_live_is_not_event_delivery(self):
        self.assertEqual(PROBE.coverage_class([_item(1)], [1]), "event_positive")
        self.assertEqual(PROBE.coverage_class([_item(1)], []), "expected_but_missing")
        self.assertEqual(PROBE.coverage_class([_item(1, filtered="must")], []), "expected_filtered")
        self.assertEqual(PROBE.coverage_class([], []), "no_sample")
        summary = PROBE.coverage_summary([
            {"feed": "QUOTE", "coverage": "event_positive", "reached_live": True},
            {"feed": "QUOTE", "coverage": "no_sample", "reached_live": True},
            {"feed": "BAR", "coverage": "expected_filtered", "reached_live": True},
        ])
        self.assertEqual(summary["totals"], {"admitted": 3, "live": 3, "event_positive": 1,
                                             "no_sample": 1, "expected_filtered": 1})
        self.assertEqual(summary["by_feed"]["QUOTE"], {"event_positive": 1, "no_sample": 1})


class CaptureWindowTests(unittest.TestCase):
    @staticmethod
    def _source(pattern):
        # newest first: (event_id, payload=product label, accepted)
        return [(bytes([index]), label.encode(), 1_000 - index) for index, label in enumerate(pattern)]

    @staticmethod
    def _product(payload):
        feed = payload.decode()
        return (feed, None)

    def test_the_window_extends_until_rare_products_have_samples(self):
        pattern = ["BOOK_DELTA"] * 9 + ["BOOK_SNAPSHOT"]
        source = self._source(pattern * 5)
        window, counts = PROBE.capture_window(
            iter(source), demand=5, products={("BOOK_DELTA", None), ("BOOK_SNAPSHOT", None)},
            min_per_product=2, product_of=self._product)
        self.assertEqual(counts, {"BOOK_DELTA|-": 18, "BOOK_SNAPSHOT|-": 2})
        self.assertEqual(len(window), 20)
        # Contiguous and oldest first: exactly the newest 20, reversed.
        self.assertEqual(window, list(reversed(source[:20])))

    def test_the_window_stops_at_the_demand_or_the_source_end(self):
        source = self._source(["TRADE"] * 50)
        window, _ = PROBE.capture_window(iter(source), demand=7, products={("TRADE", None)},
                                         min_per_product=1, product_of=self._product)
        self.assertEqual(len(window), 7)
        window, counts = PROBE.capture_window(iter(source[:3]), demand=7, products={("MARK", None)},
                                              min_per_product=1, product_of=self._product)
        self.assertEqual((len(window), counts), (3, {"MARK|-": 0}))


class TailBatchTests(unittest.TestCase):
    def test_each_record_once_only_captured_keys_and_bounded_memory(self):
        seen = {}
        rows = [("md.canonical.v2", "k1", b"e1", b"p1", 10), ("md.canonical.v2", "other", b"e2", b"p2", 11),
                ("md.projector.public.v2", "k1", b"e3", b"p3", 12), ("md.canonical.v2", "k1", b"e4", b"p4", 13)]
        self.assertEqual(PROBE.tail_batch(rows, keys={"k1"}, seen=seen, horizon_ns=0),
                         [("k1", b"p1"), ("k1", b"p4")])
        # The overlapping next poll returns nothing twice.
        self.assertEqual(PROBE.tail_batch(rows, keys={"k1"}, seen=seen, horizon_ns=0), [])
        PROBE.tail_batch([], keys={"k1"}, seen=seen, horizon_ns=12)
        self.assertEqual(set(seen), {b"e4"})


class ReadViewVerdictTests(unittest.TestCase):
    """KN-4 D29: GetSnapshot/GetFeedStatus answer once the Query read view is attached."""

    def test_without_the_read_view_only_typed_not_ready_passes(self):
        verdict = PROBE.read_view_verdict
        self.assertTrue(verdict("GetSnapshot", False, code="FAILED_PRECONDITION", details="DATA_NOT_READY:x")[0])
        self.assertFalse(verdict("GetSnapshot", False, answer=mock.Mock(snapshot_id="s"), cursor_ok=True)[0])
        self.assertFalse(verdict("GetFeedStatus", False, code="UNAVAILABLE", details="DEPENDENCY_UNAVAILABLE:x")[0])

    def test_with_the_read_view_data_or_typed_refusals_pass(self):
        verdict = PROBE.read_view_verdict
        self.assertTrue(verdict("GetSnapshot", True, answer=mock.Mock(snapshot_id="qdl-v2-a", events=[1]),
                                cursor_ok=True)[0])
        self.assertFalse(verdict("GetSnapshot", True, answer=mock.Mock(snapshot_id="qdl-v2-a", events=[]),
                                 cursor_ok=False)[0], "a cursor the stream refuses fails")
        self.assertTrue(verdict("GetFeedStatus", True, answer=mock.Mock(state="LIVE", policy_id="p"))[0])
        self.assertFalse(verdict("GetFeedStatus", True, answer=mock.Mock(state="", policy_id="p"))[0])
        self.assertTrue(verdict("GetSnapshot", True, code="FAILED_PRECONDITION", details="DATA_STALE:old")[0])
        self.assertTrue(verdict("GetFeedStatus", True, code="RESOURCE_EXHAUSTED", details="RATE_LIMITED:lane")[0])
        self.assertTrue(verdict("GetFeedStatus", True, code="PERMISSION_DENIED", details="scope")[0])
        for code, details in (("UNAVAILABLE", "DEPENDENCY_UNAVAILABLE:down"), ("INTERNAL", "boom"),
                              ("FAILED_PRECONDITION", "no code here"), ("INVALID_ARGUMENT", "INVALID_ARGUMENT:x")):
            self.assertFalse(verdict("GetSnapshot", True, code=code, details=details)[0], code)


@unittest.skipUnless(__import__("os").environ.get("QDL_KN_TEST_KAFKA"), "SKIPPED LOUDLY: QDL_KN_TEST_KAFKA is not set")
class WindowOracleKafkaTests(unittest.TestCase):
    """KN-4 D41: the handoff oracle reads a fixed window, committed only."""

    def test_the_window_oracle_holds_exactly_the_committed_records_inside_the_boundary(self):
        import os
        import time
        import uuid

        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient, NewTopic

        bootstrap = os.environ["QDL_KN_TEST_KAFKA"]
        topic = f"kn4-oracle-{uuid.uuid4().hex[:8]}"
        admin = AdminClient({"bootstrap.servers": bootstrap})
        for future in admin.create_topics([NewTopic(topic, 2, 1)]).values():
            future.result(20)
        try:
            producer = Producer({"bootstrap.servers": bootstrap, "transactional.id": f"kn4-o-{uuid.uuid4().hex}"})
            producer.init_transactions(20)
            now = int(time.time() * 1000)

            def batch(rows, *, abort=False):
                producer.begin_transaction()
                for partition, key, stamp in rows:
                    producer.produce(topic, key=key, value=b"v-" + key, partition=partition, timestamp=stamp)
                producer.flush(20)
                (producer.abort_transaction if abort else producer.commit_transaction)(20)

            batch([(0, b"early", now - 60_000)])                   # before the window
            batch([(0, b"inside-a", now), (1, b"inside-b", now)])
            batch([(1, b"aborted", now)], abort=True)
            ends = PROBE.canonical_end_offsets(bootstrap, topic)
            batch([(0, b"after-boundary", now)])                   # after the boundary
            records = PROBE.kafka_oracle_window(bootstrap, topic, since_ms=now - 1_000, ends=ends)
            self.assertEqual(sorted(records), ["inside-a", "inside-b"])
            self.assertEqual(records["inside-a"], [(0, 2, b"v-inside-a")])
            self.assertEqual(records["inside-b"], [(1, 0, b"v-inside-b")])
        finally:
            for future in admin.delete_topics([topic]).values():
                future.result(20)
