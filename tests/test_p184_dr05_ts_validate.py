"""TEST_ONLY synthetic protobuf controls, executed against imports in TS image."""

import base64
import copy
import importlib.util
import json
import os
import sys
import unittest

from qdl.common.v1 import common_pb2
from qdl.marketdata.v2 import market_data_pb2

if os.path.exists("/helper.py"):
    spec = importlib.util.spec_from_file_location("helper", "/helper.py")
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
else:
    from scripts import p184_dr05_ts_validate as helper

# The helper validates through Trading System application code (adapters.*,
# services.*). Inside the TS image it resolves from PYTHONPATH=/app. From a DL
# checkout, point QDL_TRADING_SYSTEM_SOURCE_ROOT at a TS checkout; the root is
# appended so DL packages keep precedence. A DL-only checkout (DL CI) skips the
# TS-dependent tests explicitly; an explicitly configured root that lacks the
# TS code is an error, never a skip.
TS_SOURCE_ROOT_ENV = "QDL_TRADING_SYSTEM_SOURCE_ROOT"
TS_SOURCE_ROOT = os.environ.get(TS_SOURCE_ROOT_ENV)
TS_PROBE_MODULE = "adapters.market_data.data_layer_client"
if TS_SOURCE_ROOT:
    TS_SOURCE_ROOT = os.path.abspath(TS_SOURCE_ROOT)
    if not os.path.isfile(os.path.join(TS_SOURCE_ROOT, *TS_PROBE_MODULE.split(".")) + ".py"):
        raise RuntimeError(f"{TS_SOURCE_ROOT_ENV}={TS_SOURCE_ROOT} has no {TS_PROBE_MODULE}")
    if TS_SOURCE_ROOT not in sys.path:
        sys.path.append(TS_SOURCE_ROOT)


def _ts_source_available():
    # Only an absent top-level TS package means "DL-only checkout". Any other
    # import failure inside TS code propagates so it cannot hide as a skip.
    try:
        return importlib.util.find_spec(TS_PROBE_MODULE) is not None
    except ModuleNotFoundError as error:
        if TS_SOURCE_ROOT or error.name != TS_PROBE_MODULE.split(".")[0]:
            raise
        return False


TS_SOURCE_AVAILABLE = _ts_source_available()
if TS_SOURCE_ROOT and not TS_SOURCE_AVAILABLE:
    raise RuntimeError(f"{TS_PROBE_MODULE} not importable from {TS_SOURCE_ROOT_ENV}={TS_SOURCE_ROOT}")
REQUIRES_TS = unittest.skipUnless(
    TS_SOURCE_AVAILABLE,
    f"Trading System source ({TS_PROBE_MODULE}) not importable: run inside the TS image "
    f"or set {TS_SOURCE_ROOT_ENV} to a TS checkout",
)


def helper_subprocess_env():
    env = dict(os.environ)
    if TS_SOURCE_ROOT:
        env["PYTHONPATH"] = os.pathsep.join(
            part for part in (env.get("PYTHONPATH"), TS_SOURCE_ROOT) if part)
    return env


NOW = 1_800_000_000_000_000_000
UID = "11111111-1111-5111-8111-111111111111"


def fixture(venue="BINANCE", generation=1):
    symbol = "ETHUSDT" if venue == "BINANCE" else "ETH-USDT-SWAP"
    instrument = dict(instrument_uid=UID, instrument_id=f"{venue}:PERPETUAL:{symbol}",
                      venue=venue, market="USDM" if venue == "BINANCE" else "SWAP",
                      product_type="PERPETUAL", canonical_symbol="ETH-USDT",
                      metadata_revision=7, asset_class="CRYPTO", native_symbol=symbol, status="ACTIVE")
    event = market_data_pb2.EventEnvelope(
        schema_name="TEST_ONLY.canonical", schema_major=2, instrument_uid=UID,
        instrument_id=instrument["instrument_id"], instrument_revision=7,
        venue=venue, provider="TEST_ONLY.provider", source_id="TEST_ONLY.source",
        source_role=common_pb2.SOURCE_ROLE_PRIMARY, authority_revision=3,
        normalizer_version="TEST_ONLY.normalizer", adapter_version="TEST_ONLY.adapter",
        config_revision=5, source_event_time_ns=NOW, received_at_ns=NOW,
        correlation_id="TEST_ONLY.capture",
    )
    event.book_snapshot.native_sequence = "42"
    event.book_snapshot.book_generation = generation
    event.book_snapshot.sequence_verified = True
    event.book_snapshot.depth = 1
    for side, price in ((common_pb2.BOOK_SIDE_BID, 100), (common_pb2.BOOK_SIDE_ASK, 101)):
        level = event.book_snapshot.levels.add(side=side, quantity_unit=common_pb2.QUANTITY_UNIT_BASE_ASSET)
        level.price.mantissa = price
        level.price.source_text = str(price)
        level.quantity.mantissa = 2
        level.quantity.source_text = "2"
    from qdl_sdk.projection import _payload
    payload = _payload(event)[3]
    quality = dict(state="LIVE", freshness_ms=1, gap_open=False, complete=True,
                   execution_eligible=True, policy_id="TEST_ONLY.policy", flags=[],
                   event_recency_state="LIVE", provider_session_state="LIVE", provider_session_liveness_ms=1)
    handoff = dict(instrument_uid=UID, instrument_id=instrument["instrument_id"],
                   instrument_revision=7, feed="BOOK_SNAPSHOT", observed_at_ns=NOW, received_at_ns=NOW,
                   revision=0, interval=None, payload=payload, quality=quality,
                   source=dict(venue=venue, provider=event.provider, source_id=event.source_id,
                               source_role="PRIMARY", authoritative=True),
                   contract=dict(schema_digest="a" * 64, contract_version="2.0.0",
                                 normalizer_version=event.normalizer_version, adapter_version=event.adapter_version,
                                 instrument_catalog_revision=7, source_policy_revision=2,
                                 authority_revision=3, config_revision=5, correlation_id=event.correlation_id),
                   watermark_offset=12)
    row = dict(requested_at_ns=NOW, completed_at_ns=NOW + 1_000_000, instrument_uid=UID,
               GetFeedStatus=dict(code="OK", status=copy.deepcopy(quality)),
               GetSnapshot=dict(code="OK", events_b64=[base64.b64encode(event.SerializeToString()).decode()],
                                watermark_offset=12, cursor_valid=True, stream_cursor="TEST_ONLY.cursor", data_as_of_ns=NOW))
    packet = dict(schema_version=1, side="BUY", instrument=instrument,
                  requirements={"BOOK_SNAPSHOT": dict(instrument_uid=UID, feed="BOOK_SNAPSHOT",
                      consumer_grade="EXECUTION", source_policy_id="TEST_ONLY.policy",
                      max_freshness_ms=2000, max_session_liveness_ms=2000)},
                  handoffs={"BOOK_SNAPSHOT": handoff}, observations=[dict(id="positive", views={"BOOK_SNAPSHOT": row})])
    return packet


def row(packet):
    return packet["observations"][0]["views"]["BOOK_SNAPSHOT"]


def change_event(packet, callback):
    target = row(packet)["GetSnapshot"]
    event = market_data_pb2.EventEnvelope.FromString(base64.b64decode(target["events_b64"][0]))
    callback(event)
    target["events_b64"] = [base64.b64encode(event.SerializeToString()).decode()]


@REQUIRES_TS
class ValidationTests(unittest.TestCase):
    def result(self, packet):
        return helper.validate(packet)["results"][0]

    def test_positive_actual_book_validator_both_venues(self):
        for venue in ("BINANCE", "OKX"):
            with self.subTest(venue=venue):
                result = self.result(fixture(venue))
                self.assertEqual(result["outcome"], "ACCEPTED", result)
                self.assertEqual(result["stage"], "ts_book_validator")
                self.assertEqual(result["book_proof"]["generation"], "1")

    def test_actual_status_gap_refuses_despite_clean_raw(self):
        packet = fixture()
        row(packet)["GetFeedStatus"]["status"].update(gap_open=True, state="GAPPED", complete=False)
        result = self.result(packet)
        self.assertEqual(result["outcome"], "APPLICATION_REFUSED", result)
        self.assertEqual(result["stage"], "sdk_status")
        self.assertFalse(result["book_validated"])
        self.assertIsNone(result["cache_apply"])

    def test_raw_gap_refuses_despite_healthy_status(self):
        packet = fixture()
        change_event(packet, lambda e: e.quality_flags.append(common_pb2.QUALITY_FLAG_SEQUENCE_GAP_BEFORE))
        result = self.result(packet)
        self.assertEqual(result["outcome"], "APPLICATION_REFUSED", result)
        self.assertEqual(result["stage"], "sdk_projection")

    def test_unverified_refuses(self):
        packet = fixture()
        change_event(packet, lambda e: setattr(e.book_snapshot, "sequence_verified", False))
        self.assertEqual(self.result(packet)["outcome"], "APPLICATION_REFUSED")

    def test_recovered_then_old_generation_fenced(self):
        packet = fixture(generation=2)
        old = fixture(generation=1)["observations"][0]
        old["id"] = "late-old"
        packet["observations"].append(old)
        results = helper.validate(packet)["results"]
        self.assertEqual([r["outcome"] for r in results], ["ACCEPTED", "FENCED_OLDER"])

    def test_new_generation_requires_real_adoption(self):
        packet = fixture()
        new = fixture(generation=2)["observations"][0]
        new["id"] = "new-unadopted"
        packet["observations"].append(new)
        result = helper.validate(packet)["results"][1]
        self.assertEqual(result["outcome"], "APPLICATION_REFUSED")
        self.assertIn("GENERATION_HANDOFF_REQUIRED", result["error"])

    def test_endpoint_refusal_not_application_success(self):
        packet = fixture()
        row(packet)["GetSnapshot"] = {"code": "FAILED_PRECONDITION"}
        result = self.result(packet)
        self.assertEqual(result["outcome"], "ENDPOINT_REFUSED")
        self.assertFalse(result["application_called"])

    def test_missing_quality_never_uses_model_defaults(self):
        packet = fixture()
        del row(packet)["GetFeedStatus"]["status"]["provider_session_state"]
        self.assertEqual(self.result(packet)["outcome"], "INPUT_ERROR")

    def test_quality_keys_emitted_only_when_set_are_optional_not_open(self):
        from qdl_sdk.models import QualityView
        packet = fixture()
        row(packet)["GetFeedStatus"]["status"]["not_a_quality_field"] = 1
        self.assertEqual(self.result(packet)["outcome"], "INPUT_ERROR")
        if "proof_observed_at_ns" not in QualityView.model_fields:
            self.skipTest("SDK before IDR-040 fix 2 has no proof_observed_at_ns")
        packet = fixture()
        row(packet)["GetFeedStatus"]["status"]["proof_observed_at_ns"] = NOW
        self.assertEqual(self.result(packet)["outcome"], "ACCEPTED")

    def test_missing_handoff_does_not_invent_authority(self):
        packet = fixture()
        packet["handoffs"] = {}
        self.assertEqual(self.result(packet)["outcome"], "INPUT_ERROR")

    def test_stale_validation_clock_refuses(self):
        packet = fixture()
        row(packet)["completed_at_ns"] += 10_000_000_000
        self.assertEqual(self.result(packet)["outcome"], "APPLICATION_REFUSED")

    def test_invalid_inputs_fail_closed(self):
        for key, value in (("cursor_valid", False), ("watermark_offset", -1), ("events_b64", ["!!"]),
                           ("events_b64", []), ("data_as_of_ns", 0)):
            with self.subTest(key=key, value=value):
                packet = fixture()
                row(packet)["GetSnapshot"][key] = value
                self.assertEqual(self.result(packet)["outcome"], "INPUT_ERROR")

    def test_foreign_identity_fails_closed(self):
        packet = fixture()
        change_event(packet, lambda e: setattr(e, "instrument_uid", "foreign"))
        self.assertEqual(self.result(packet)["outcome"], "APPLICATION_REFUSED")

    def test_delta_projection_is_not_snapshot_risk_admission(self):
        packet = fixture()
        target = row(packet)
        event = market_data_pb2.EventEnvelope.FromString(base64.b64decode(target["GetSnapshot"]["events_b64"][0]))
        event.ClearField("book_snapshot")
        event.book_delta.native_sequence_start = "42"
        event.book_delta.native_sequence_end = "42"
        event.book_delta.snapshot_sequence = "1"
        event.book_delta.book_generation = 1
        event.book_delta.sequence_verified = True
        target["GetSnapshot"]["events_b64"] = [base64.b64encode(event.SerializeToString()).decode()]
        from qdl_sdk.projection import _payload
        for name in ("requirements", "handoffs"):
            value = packet[name].pop("BOOK_SNAPSHOT")
            value["feed"] = "BOOK_DELTA"
            packet[name]["BOOK_DELTA"] = value
        packet["handoffs"]["BOOK_DELTA"]["payload"] = _payload(event)[3]
        packet["observations"][0]["views"] = {"BOOK_DELTA": target}
        result = self.result(packet)
        self.assertEqual(result["outcome"], "ACCEPTED", result)
        self.assertEqual(result["stage"], "ts_ordering")
        self.assertNotIn("book_proof", result)

    def test_stdio_contract(self):
        import subprocess
        import sys
        process = subprocess.run([sys.executable, "-B", helper.__file__],
                                 input=json.dumps(fixture()), text=True, capture_output=True, timeout=10,
                                 env=helper_subprocess_env())
        self.assertEqual(process.returncode, 0, process.stderr)
        result = json.loads(process.stdout)
        self.assertEqual(result["results"][0]["outcome"], "ACCEPTED")
        self.assertFalse(result["acceptance"])

    def test_real_status_uint64_strings(self):
        packet = fixture()
        status = row(packet)["GetFeedStatus"]["status"]
        status["freshness_ms"] = "1"
        status["provider_session_liveness_ms"] = "1"
        self.assertEqual(self.result(packet)["outcome"], "ACCEPTED")

    def test_not_applicable_session_preserves_explicit_policy(self):
        packet = fixture()
        row(packet)["GetFeedStatus"]["status"]["provider_session_state"] = "NOT_APPLICABLE"
        self.assertEqual(self.result(packet)["outcome"], "APPLICATION_REFUSED")
        packet["requirements"]["BOOK_SNAPSHOT"]["max_session_liveness_ms"] = None
        self.assertEqual(self.result(packet)["outcome"], "ACCEPTED")

    def test_endpoint_refusal_without_handoff(self):
        packet = fixture()
        packet["handoffs"] = {}
        row(packet)["GetSnapshot"] = {"code": "INVALID_ARGUMENT"}
        self.assertEqual(self.result(packet)["outcome"], "ENDPOINT_REFUSED")

    def test_missing_cursor_not_fabricated(self):
        packet = fixture()
        del row(packet)["GetSnapshot"]["stream_cursor"]
        self.assertEqual(self.result(packet)["outcome"], "INPUT_ERROR")

    def test_never_claims_full_acceptance(self):
        result = helper.validate(fixture())
        self.assertFalse(result["acceptance"])
        self.assertTrue(result["missing_coverage"])
        self.assertNotIn("/helper.py", result["application_modules"].values())


@REQUIRES_TS
@unittest.skipUnless(os.environ.get("P184_DR05_LIVE_TEST") == "YES", "isolated Redis DB2 required")
class LiveTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import subprocess
        import sys
        import time
        import urllib.request
        cls.ns = "p184-dr05-dl-0123456789abcdef"
        cls.server = subprocess.Popen([sys.executable, "-B", helper.__file__, "--serve"],
            env={**helper_subprocess_env(), "P184_DR05_ISOLATED_NAMESPACE": cls.ns},
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            for _ in range(60):
                if cls.server.poll() is not None:
                    raise AssertionError(cls.server.communicate())
                try:
                    with urllib.request.urlopen("http://127.0.0.1:8260/health", timeout=.2) as response:
                        assert json.load(response)["ready"]
                    return
                except OSError:
                    time.sleep(.05)
            raise AssertionError("server startup timeout")
        except BaseException:
            cls.server.terminate()
            cls.server.communicate(timeout=5)
            raise

    @classmethod
    def tearDownClass(cls):
        cls.server.terminate()
        stdout, stderr = cls.server.communicate(timeout=6)
        if cls.server.returncode:
            raise AssertionError((stdout, stderr))
        from redis import Redis
        client = Redis(host="127.0.0.1", port=6379, db=2)
        try:
            if client.dbsize() != 0:
                raise AssertionError("test-owned DB2 keys remain")
        finally:
            client.close()

    def fresh(self, generation=1):
        import time
        global NOW
        old = NOW
        NOW = time.time_ns() - 10_000_000
        try:
            packet = fixture(generation=generation)
        finally:
            NOW = old
        packet["namespace"] = self.ns
        return packet

    def post(self, packet, namespace=None):
        import urllib.request
        request = urllib.request.Request("http://127.0.0.1:8260/validate",
            data=json.dumps(packet).encode(), headers={"Content-Type": "application/json",
            "X-P184-Namespace": namespace or self.ns})
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.load(response)

    def test_01_live_adoption_and_fencing(self):
        packet = self.fresh()
        positive = self.post(packet)
        first = positive["results"][0]
        self.assertEqual(first["outcome"], "APPLIED", first)
        self.assertTrue(first["accepted"])
        self.assertTrue(first["book_validated"])
        refreshed = copy.deepcopy(packet)
        row(refreshed)["GetFeedStatus"]["status"]["freshness_ms"] = "17"
        row(refreshed)["GetFeedStatus"]["status"]["provider_session_liveness_ms"] = "17"
        unchanged = self.post(refreshed)["results"][0]
        self.assertEqual(unchanged["outcome"], "NO_WRITE", unchanged)
        self.assertTrue(unchanged["book_validated"])
        self.assertFalse(unchanged["accepted"])
        self.assertFalse(unchanged["applied"])
        self.assertEqual(unchanged["cache_apply"], {"accepted": False, "written": False})
        self.assertEqual(unchanged["cache_sha256"], first["cache_sha256"])
        self.assertTrue(positive["live"])
        self.assertFalse(positive["acceptance"])
        recovered = self.post(self.fresh(2))["results"][0]
        self.assertEqual(recovered["outcome"], "APPLIED", recovered)
        self.assertEqual(recovered["current_generation"], 2)
        old = self.post(self.fresh(1))["results"][0]
        self.assertEqual(old["outcome"], "FENCED_OLDER", old)
        self.assertFalse(old["accepted"])
        self.assertTrue(old["book_validated"])
        self.assertEqual(old["cache_apply"], {"accepted": False, "written": False})
        self.assertTrue(old["cache_unchanged"])
        self.assertEqual(old["cache_sha256"], recovered["cache_sha256"])
        self.assertEqual(old["current_generation"], 2)

    def test_02_expired_and_endpoint_refused(self):
        packet = fixture()
        packet["namespace"] = self.ns
        # Captured clocks lie in the future here: input must refuse before writing.
        self.assertFalse(self.post(packet)["results"][0]["accepted"])
        packet = self.fresh(3)
        target = row(packet)
        target["completed_at_ns"] -= 10_000_000_000
        target["requested_at_ns"] -= 10_000_000_000
        change_event(packet, lambda e: (setattr(e, "source_event_time_ns", e.source_event_time_ns - 10_000_000_000),
                                        setattr(e, "received_at_ns", e.received_at_ns - 10_000_000_000)))
        result = self.post(packet)["results"][0]
        self.assertEqual(result["outcome"], "APPLICATION_REFUSED", result)
        packet = self.fresh(3)
        row(packet)["GetSnapshot"] = {"code": "FAILED_PRECONDITION"}
        result = self.post(packet)["results"][0]
        self.assertEqual(result["outcome"], "ENDPOINT_REFUSED")
        self.assertFalse(result["accepted"])

    def test_03_namespace_and_shape_protection(self):
        from urllib.error import HTTPError
        packet = self.fresh()
        with self.assertRaises(HTTPError) as error:
            self.post(packet, "wrong")
        self.assertEqual(error.exception.code, 403)
        packet["namespace"] = "wrong"
        with self.assertRaises(HTTPError) as error:
            self.post(packet)
        self.assertEqual(error.exception.code, 400)
        packet = self.fresh()
        packet["now_ns"] = NOW
        with self.assertRaises(HTTPError):
            self.post(packet)


if __name__ == "__main__":
    unittest.main()
