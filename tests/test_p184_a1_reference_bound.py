"""A1 (owner 2026-10-18): the driver's reference verifier and the DEPLOYED Query apply ONE freshness bound.

Final stage-100 p184-final-campaign-20261018T074624Z: at 08:00:13.96Z Query served an OKX SOL-USDT-SWAP funding series
8 h + 13.8 s old under its 60 s settlement publication allowance (qdl/query/service.py, stage 100g) and the driver's
verifier (qdl/certification/reference_l2_acceptance.py reference_quality), which had none, refused it.
Fix 1ab4a638 imported Query's constant into the verifier; the verifier runs in the driver's CLIENT container, whose
profile image qdl-v2-python:2.2.2-d6d2637 predates the constant, so the import failed and every client read failed
(092255Z, 146,701 ImportError). This version (option (a)): the host resolves the allowance from the deployed Query
image (resolve_query_funding_allowance, fail closed) and passes it in the inner configuration; the verifier imports no
Query symbol and fails closed while unconfigured. The tests drive the REAL Query service and the verifier with the
same series age (8 h + 14 s / + 60 s accepted, + 61 s refused, on both sides) and run the mounted verifier inside the
profile client image with --network none.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
import tempfile
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace

from qdl.certification import reference_l2_acceptance as verifier
from qdl.certification.reference_l2_acceptance import (
    _MILLISECOND_NS,
    build_reference_l2_acceptance_scope,
    configure_query_funding_allowance_ms,
    reference_freshness_allowance_ms,
    reference_freshness_bound_ms,
    reference_quality,
)
from qdl.domain.instrument import InstrumentRegistry
from qdl.query import AccessPurpose, ConsumerGrade, InstrumentQuery, MemoryMarketDataBackend, V2QueryService
from qdl.query import service as query_service
from qdl.query.reference import ReferenceBatchRequirement, ReferenceDataRequirement
from qdl.reference.batch import ReferenceBatch
from qdl.reference.contracts import ReferenceProduct
from qdl.query import FeedType
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from tests.test_phase113_reference_v2 import FixtureReferenceAdapter, grants, record

ROOT = Path(__file__).resolve().parents[1]
NOW_NS = 1_800_000_000_000_000_000
FUNDING_8H_MS = 28_800_000
AGES_MS = {"8h+14s": (FUNDING_8H_MS + 14_000, True), "8h+60s": (FUNDING_8H_MS + 60_000, True),
           "8h+61s": (FUNDING_8H_MS + 61_000, False)}


def okx_funding_product():
    catalog = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")
    acquisition = StableAcquisitionPlan.load(ROOT / "config/v2/stable-acquisition-bindings.yaml", catalog=catalog)
    scope = build_reference_l2_acceptance_scope(ROOT / "consumers/stable/reference-l2-stable.yaml", catalog=catalog,
                                                acquisition=acquisition, now_ns=NOW_NS)
    return next(item for item in scope.references
                if item.requirement.feed is FeedType.FUNDING_RATE and item.venue == "OKX"
                and item.sdk_requirement.max_freshness_ms == FUNDING_8H_MS)


def verifier_item(product, observed_at_ns: int):
    field = SimpleNamespace(name="funding_rate", unit="DIMENSIONLESS_RATE",
                            value=SimpleNamespace(source_text="0", coefficient="0", scale=0))
    observation = SimpleNamespace(instrument_uid=product.instrument_uid, product=product.sdk_requirement.product,
                                  observed_at_ns=observed_at_ns, fields=[field])
    data = SimpleNamespace(instrument_uid=product.instrument_uid, product=product.sdk_requirement.product,
                           received_at_ns=NOW_NS, observations=[observation])
    return SimpleNamespace(instrument_uid=product.instrument_uid, product=product.sdk_requirement.product,
                           status="OK", problem=None, data=data)


def verifier_accepts(product, age_ms: int) -> bool:
    try:
        reference_quality(product, verifier_item(product, NOW_NS - age_ms * _MILLISECOND_NS), observed_at_ns=NOW_NS)
    except ValueError as error:
        if "governed freshness bound" not in str(error):
            raise
        return False
    return True


def query_accepts(age_ms: int) -> bool:
    okx = record(venue="OKX", market="SWAP", symbol="SOL-USDT-SWAP", base="SOL")
    registry = InstrumentRegistry()
    registry.register(okx, [])
    adapter = FixtureReferenceAdapter(observed_at_ns=NOW_NS - age_ms * 1_000_000)
    service = V2QueryService(
        instruments=InstrumentQuery(registry), backend=MemoryMarketDataBackend(), entitlements=grants(),
        reference_batch=ReferenceBatch({("OKX", "SWAP"): adapter}, clock_ns=lambda: NOW_NS),
        reference_source_id=lambda item: f"{item.identity.venue}_DIRECT", clock_ns=lambda: NOW_NS)
    requirement = ReferenceDataRequirement(
        instrument_uid=okx.instrument_uid, product=ReferenceProduct.FUNDING_RATE, consumer_grade=ConsumerGrade.ALPHA,
        source_policy_id="reference_primary_v2", start_time_ns=NOW_NS - 86_400_000_000_000, end_time_ns=NOW_NS,
        limit=10, max_freshness_ms=FUNDING_8H_MS)
    result = asyncio.run(service.reference_data_batch_async(
        ReferenceBatchRequirement("phase113-alpha", (requirement,), require_all=False),
        purpose=AccessPurpose.INTERNAL_ALPHA))
    problem = result.results[0].problem
    if problem is not None and problem.code.value != "DATA_STALE":
        raise AssertionError(f"unexpected Query problem {problem.code.value}")
    return problem is None


def configure_from_this_query():
    """The host-resolved value, in-process: the worktree Query here IS the code under test (its service.py equals the
    deployed one, `git diff 31a5155 HEAD -- qdl/` empty at 1ab4a638)."""
    configure_query_funding_allowance_ms(query_service._FUNDING_SETTLEMENT_PUBLICATION_GRACE_MS)


class A1ReferenceBoundParityTests(unittest.TestCase):
    def setUp(self):
        self.product = okx_funding_product()
        configure_from_this_query()
        self.addCleanup(configure_query_funding_allowance_ms, None)

    def test_allowance_is_querys_constant_for_funding_history_only(self):
        request = self.product.sdk_requirement
        self.assertIsNotNone(request.start_time_ns)
        self.assertEqual(reference_freshness_allowance_ms(request),
                         query_service._FUNDING_SETTLEMENT_PUBLICATION_GRACE_MS)
        self.assertEqual(reference_freshness_bound_ms(request), FUNDING_8H_MS + 60_000)
        snapshot = request.model_copy(update={"start_time_ns": None, "end_time_ns": None})
        self.assertEqual(reference_freshness_allowance_ms(snapshot), 0)
        self.assertEqual(reference_freshness_bound_ms(snapshot), FUNDING_8H_MS)
        other = SimpleNamespace(product=SimpleNamespace(value="OPEN_INTEREST"), start_time_ns=1, max_freshness_ms=5)
        self.assertEqual(reference_freshness_bound_ms(other), 5)
        self.assertIsNone(reference_freshness_bound_ms(SimpleNamespace(product=None, max_freshness_ms=None)))

    def test_verifier_and_query_agree_across_the_settlement_allowance(self):
        for name, (age_ms, expected) in AGES_MS.items():
            with self.subTest(age=name):
                server, client = query_accepts(age_ms), verifier_accepts(self.product, age_ms)
                self.assertEqual(server, expected, "Query side")
                self.assertEqual(client, expected, "verifier side")
                self.assertEqual(server, client)

    def test_the_074624Z_refusal_is_now_accepted_and_the_old_rule_would_refuse_it(self):
        age_ms = FUNDING_8H_MS + 13_838  # g6 unsafe example 28,813,838 ms at 08:00:13.96Z
        self.assertTrue(verifier_accepts(self.product, age_ms))
        self.assertTrue(age_ms > self.product.sdk_requirement.max_freshness_ms)  # the pre-A1 verifier rule refused


class A1DriverTimingTests(unittest.TestCase):
    def setUp(self):
        from scripts import phase3_consumer_load_acceptance as driver
        self.driver = driver
        self.product = okx_funding_product()
        configure_from_this_query()
        self.addCleanup(configure_query_funding_allowance_ms, None)

    def response(self, newest_ns: int, *, cache_hit: bool):
        item = verifier_item(self.product, newest_ns)
        item.data.cache_hit, item.data.coalesced, item.data.received_at_ns = cache_hit, False, newest_ns + 5
        return SimpleNamespace(results=[item])

    def test_timing_records_cache_hit_received_at_and_funding_time_with_crossing(self):
        boundary = (NOW_NS // (8 * 3_600_000_000_000)) * 8 * 3_600_000_000_000
        sent = boundary + 13_960_000_000
        previous = boundary - 8 * 3_600_000_000_000
        timing = self.driver.reference_timing([self.product], self.response(previous, cache_hit=True),
                                              sent_ns=sent, now_ns=sent + 119_000_000)
        (entry,) = timing
        self.assertEqual(entry["key"], f"FUNDING_RATE|OKX|{self.product.native_symbol}")
        self.assertTrue(entry["cache_hit"])
        self.assertEqual(entry["received_at_ns"], previous + 5)
        self.assertEqual(entry["newest_observed_at_ns"], previous)
        self.assertTrue(entry["settlement_crossing"])
        self.assertTrue(entry["allowance_used"])
        self.assertEqual(entry["effective_bound_ms"], FUNDING_8H_MS + 60_000)
        settled = self.driver.reference_timing([self.product], self.response(boundary, cache_hit=False),
                                               sent_ns=sent, now_ns=sent)
        self.assertFalse(settled[0]["settlement_crossing"])
        self.assertFalse(settled[0]["allowance_used"])

    def test_recorder_and_merge_keep_exact_counts_and_bounded_examples(self):
        boundary = (NOW_NS // (8 * 3_600_000_000_000)) * 8 * 3_600_000_000_000
        crossing = self.driver.reference_timing([self.product], self.response(boundary - 8 * 3_600_000_000_000,
                                                                              cache_hit=True),
                                                sent_ns=boundary + 10_000_000_000, now_ns=boundary + 10_100_000_000)
        normal = self.driver.reference_timing([self.product], self.response(boundary, cache_hit=False),
                                              sent_ns=boundary + 20_000_000_000, now_ns=boundary + 20_100_000_000)
        blocks = []
        for timings in ((crossing, normal), (normal,)):
            recorder = SimpleNamespace(reference=self.driver._ReferenceTimingRecorder())
            for timing in timings:
                self.driver._REFERENCE_TIMING.set(timing)
                self.driver._reference_timing_record(recorder, "STEADY", accepted=True, error_code=None)
            blocks.append(recorder.reference.evidence())
        merged = self.driver.merge_reference_timing(blocks)
        row = merged["rows"][f"FUNDING_RATE|OKX|{self.product.native_symbol}"]
        self.assertEqual((row["n"], row["accepted"], row["refused"], row["cache_hits"], row["settlement_crossings"],
                          row["allowance_used"]), (3, 3, 0, 1, 1, 1))
        self.assertEqual(row["effective_bound_ms"], FUNDING_8H_MS + 60_000)
        self.assertEqual(merged["examples_seen"], 1)
        self.assertEqual(len(merged["examples"]), 1)
        self.assertTrue(merged["report_only"])

    def test_driver_mounts_the_worktree_verifier_into_its_client_container(self):
        import tempfile
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "runtime").mkdir()
            files = {}
            for name in ("ca.crt", "client.crt", "client.key", "private.key"):
                files[name] = root / name
                files[name].write_text(name)
            profile = self.driver.validate_profile({
                "image": "qdl-v2-python:test", "network": "qdl_test", "runtime_dir": str(root / "runtime"),
                "queries": ["https://query_v2_1:8200", "https://query_v2_2:8200"],
                "stream_targets": ["stream_v2_active:8210", "stream_v2_passive:8210"],
                "query_containers": ["qdl_v2_stable_candidate-query_v2_1-1", "qdl_v2_stable_candidate-query_v2_2-1"],
                "identities": [{"id": "alpha.binance.paper.stable",
                                "tls": {"ca_file": str(files["ca.crt"]), "cert_file": str(files["client.crt"]),
                                        "key_file": str(files["client.key"])},
                                "jwt": {"private_key_file": str(files["private.key"]), "key_id": "k"}}]})
            command, inner = self.driver.docker_command(profile, name="qdl-phase3-load-unit",
                                                        image_id="sha256:" + "a" * 64, mode="target", sessions=5,
                                                        duration_seconds=60, query_funding_allowance_ms=60_000)
        expected = (f"type=bind,src={ROOT / 'qdl/certification/reference_l2_acceptance.py'},"
                    "dst=/app/qdl/certification/reference_l2_acceptance.py,readonly")
        self.assertIn(expected, command)
        self.assertEqual(command[command.index(expected) - 1], "--mount")
        self.assertEqual(inner["query_funding_allowance_ms"], 60_000)
        config = json.loads(next(v for v in command if v.startswith("QDL_PHASE3_LOAD_CONFIG="))
                            .split("=", 1)[1])
        self.assertEqual(config["query_funding_allowance_ms"], 60_000)


def fake_profile(root: Path, driver):
    (root / "runtime").mkdir()
    files = {}
    for name in ("ca.crt", "client.crt", "client.key", "private.key"):
        files[name] = root / name
        files[name].write_text(name)
    return driver.validate_profile({
        "image": "qdl-v2-python:test", "network": "qdl_test", "runtime_dir": str(root / "runtime"),
        "queries": ["https://query_v2_1:8200", "https://query_v2_2:8200"],
        "stream_targets": ["stream_v2_active:8210", "stream_v2_passive:8210"],
        "query_containers": ["qdl_v2_stable_candidate-query_v2_1-1", "qdl_v2_stable_candidate-query_v2_2-1"],
        "sdk_from_source": True,
        "identities": [{"id": "alpha.binance.paper.stable",
                        "tls": {"ca_file": str(files["ca.crt"]), "cert_file": str(files["client.crt"]),
                                "key_file": str(files["client.key"])},
                        "jwt": {"private_key_file": str(files["private.key"]), "key_id": "k"}}]})


class A1FailClosedTests(unittest.TestCase):
    """Option (a): no allowance -> no read is judged permissively and the client refuses to start."""

    def setUp(self):
        from scripts import phase3_consumer_load_acceptance as driver
        self.driver = driver
        self.product = okx_funding_product()
        configure_query_funding_allowance_ms(None)
        self.addCleanup(configure_query_funding_allowance_ms, None)

    def test_the_verifier_imports_no_query_symbol(self):
        source = (ROOT / "qdl/certification/reference_l2_acceptance.py").read_text()
        self.assertNotIn("from qdl.query.service", source)
        self.assertNotIn("import qdl.query.service", source)

    def test_unconfigured_funding_history_bound_fails_closed_and_other_products_are_unaffected(self):
        request = self.product.sdk_requirement
        with self.assertRaisesRegex(ValueError, "not configured"):
            reference_freshness_bound_ms(request)
        with self.assertRaisesRegex(ValueError, "not configured"):
            reference_quality(self.product, verifier_item(self.product, NOW_NS - 1_000 * _MILLISECOND_NS),
                              observed_at_ns=NOW_NS)
        snapshot = request.model_copy(update={"start_time_ns": None, "end_time_ns": None})
        self.assertEqual(reference_freshness_bound_ms(snapshot), FUNDING_8H_MS)

    def test_configure_rejects_non_int_negative_and_bool(self):
        for bad in (-1, 1.5, "60000", True, 3_600_001):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                configure_query_funding_allowance_ms(bad)
        configure_query_funding_allowance_ms(0)
        self.assertEqual(verifier.configured_query_funding_allowance_ms(), 0)
        self.assertEqual(reference_freshness_allowance_ms(self.product.sdk_requirement), 0)

    def test_inner_config_requires_and_configures_the_allowance(self):
        from unittest import mock
        base = {"mode": "target", "logical_sessions": 5, "duration_seconds": 60, "catalog": "c", "acquisition": "a",
                "queries": [], "stream_targets": [], "identities": [], "budget": {}, "final": False}
        with mock.patch.dict(self.driver.os.environ, {"QDL_PHASE3_LOAD_CONFIG": json.dumps(base)}):
            with self.assertRaisesRegex(ValueError, "fields are invalid"):
                self.driver._inside_config()
        self.assertIsNone(verifier.configured_query_funding_allowance_ms())
        for bad in (None, "60000", -5):
            with mock.patch.dict(self.driver.os.environ,
                                 {"QDL_PHASE3_LOAD_CONFIG": json.dumps({**base, "query_funding_allowance_ms": bad})}):
                with self.subTest(bad=bad), self.assertRaises(ValueError):
                    self.driver._inside_config()
        with mock.patch.dict(self.driver.os.environ,
                             {"QDL_PHASE3_LOAD_CONFIG": json.dumps({**base, "query_funding_allowance_ms": 60_000})}):
            self.driver._inside_config()
        self.assertEqual(verifier.configured_query_funding_allowance_ms(), 60_000)

    def test_docker_command_refuses_without_the_allowance(self):
        with tempfile.TemporaryDirectory() as temporary:
            profile = fake_profile(Path(temporary), self.driver)
            for bad in (None, -1, "60000"):
                with self.subTest(bad=bad), self.assertRaises(ValueError):
                    self.driver.docker_command(profile, name="n", image_id="sha256:" + "a" * 64, mode="target",
                                               sessions=5, duration_seconds=60, query_funding_allowance_ms=bad)

    def test_host_resolution_is_fail_closed(self):
        resolve = self.driver.resolve_query_funding_allowance
        good = {"allowance_ms": 60_000, "applied_in_reference_problem": True}
        images = {"q1": "sha256:" + "1" * 64, "q2": "sha256:" + "1" * 64}
        value = resolve(["q1", "q2"], image_of=images.get, probe=lambda image: good)
        self.assertEqual(value["allowance_ms"], 60_000)
        self.assertEqual(set(value["query_images"]), {"q1", "q2"})
        cases = {
            "no container": ([], images.get, lambda image: good),
            "constant absent (2.2.2)": (["q1"], images.get,
                                        lambda image: {"allowance_ms": None, "applied_in_reference_problem": False}),
            "constant not applied": (["q1"], images.get,
                                     lambda image: {"allowance_ms": 60_000, "applied_in_reference_problem": False}),
            "non-int": (["q1"], images.get, lambda image: {"allowance_ms": "60000",
                                                           "applied_in_reference_problem": True}),
            "replicas disagree": (["q1", "q2"], {"q1": "sha256:" + "1" * 64, "q2": "sha256:" + "2" * 64}.get,
                                  lambda image: {"allowance_ms": 60_000 if image.endswith("1") else 30_000,
                                                 "applied_in_reference_problem": True}),
        }
        for name, (containers, image_of, probe) in cases.items():
            with self.subTest(case=name), self.assertRaises(RuntimeError):
                resolve(containers, image_of=image_of, probe=probe)

    def test_the_probe_reads_the_constant_inside_an_image(self):
        probe = self.driver._QUERY_ALLOWANCE_PROBE
        self.assertIn("_FUNDING_SETTLEMENT_PUBLICATION_GRACE_MS", probe)
        self.assertNotIn("open(", probe)


if __name__ == "__main__":
    unittest.main()
