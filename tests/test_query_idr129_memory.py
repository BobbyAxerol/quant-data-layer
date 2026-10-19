"""IDR-129: Query memory kept after warmup bursts.

Measured 2026-10-19 on an isolated replica of the production Query image under repeated 64-warmup bursts
(``scripts/idr129_query_rss_isolated.py``): RSS 472 -> 683 MiB over 12 bursts. Two retained pieces:

* A large warmup body went to the server as one ``http.response.body`` message. asyncio's TLS transport
  encrypts each ``transport.write`` into the connection's OpenSSL output ``ssl.MemoryBIO``, which keeps the
  capacity of its largest write for the connection's life, and a closed connection's objects are cyclic
  garbage that only a rare full collection frees (130 MiB of malloc memory freed from 39 closed
  connections). ``SlicedBodyResponse`` hands the same bytes over in bounded slices.
* A refused batch item's error kept its traceback, whose frames hold the error again (the IDR-114 residual
  ``work``/executor cycle). The batch keeps the typed problem and releases the frames.
"""

from __future__ import annotations

import asyncio
import gc
import importlib
import ssl
from types import SimpleNamespace
import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from qdl.query import (
    AccessPurpose,
    BatchRequirement,
    CanonicalErrorCode,
    ConsumerGrade,
    DataRequirement,
    FeedType,
    QueryProblem,
    V2QueryService,
)
from qdl.query.service import QueryServiceError, _LocalBatchAdmission, _queued_local_batch_lane_policy
from qdl.runtime.memory_maintenance import (
    DEFAULT_SOFT_LIMIT_MIB,
    SOFT_LIMIT_ENV,
    MemoryMaintenance,
    install_query_memory_maintenance,
    soft_limit_from_environment,
)
from qdl.warmup import WarmupSpecification
from qdl.warmup.executor import BoundedWarmupExecutor

router_module = importlib.import_module("qdl.api_v2.router")
SlicedBodyResponse = router_module.SlicedBodyResponse
SLICE = router_module._RESPONSE_SLICE_BYTES


async def _messages(response) -> list[dict]:
    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    async def receive():  # pragma: no cover - a response never reads
        return {"type": "http.disconnect"}

    await response({"type": "http", "method": "GET"}, receive, send)
    return sent


class SlicedBodyResponseTests(unittest.IsolatedAsyncioTestCase):
    async def test_large_body_is_sent_in_bounded_slices_with_the_same_bytes_and_length(self):
        body = bytes(range(256)) * (SLICE * 3 // 256 + 17)  # 3 slices and a partial one
        response = SlicedBodyResponse(content=body, media_type="application/json")
        messages = await _messages(response)
        start, *chunks = messages
        self.assertEqual(start["type"], "http.response.start")
        self.assertEqual(start["status"], 200)
        headers = dict(start["headers"])
        self.assertEqual(headers[b"content-length"], str(len(body)).encode())
        self.assertEqual(headers[b"content-type"], b"application/json")
        self.assertEqual(len(chunks), 4)
        self.assertTrue(all(len(chunk["body"]) <= SLICE for chunk in chunks))
        self.assertTrue(all(type(chunk["body"]) is bytes for chunk in chunks))
        self.assertEqual([chunk["more_body"] for chunk in chunks], [True, True, True, False])
        self.assertEqual(b"".join(chunk["body"] for chunk in chunks), body)

    async def test_small_and_empty_bodies_are_one_message_as_before(self):
        for body in (b"", b"{}", b"x" * SLICE):
            messages = await _messages(SlicedBodyResponse(content=body, media_type="application/json"))
            self.assertEqual(len(messages), 2)
            self.assertEqual(messages[1], {"type": "http.response.body", "body": body})

    async def test_warmup_responses_use_the_sliced_response(self):
        body = b'{"data":[' + b"1," * SLICE + b'1]}'
        original = router_module._render_warmup_result
        router_module._render_warmup_result = lambda result: body
        try:
            response = await router_module._warmup_json_off_loop(lambda: None)
        finally:
            router_module._render_warmup_result = original
        self.assertIsInstance(response, SlicedBodyResponse)
        self.assertEqual(response.body, body)


class SlicedBodyOverHttpTests(unittest.TestCase):
    def test_client_receives_identical_bytes_and_content_length(self):
        body = b"a" * (SLICE * 2 + 5)
        app = FastAPI()

        @app.get("/body")
        async def endpoint():
            return SlicedBodyResponse(content=body, media_type="application/json")

        with TestClient(app) as client:
            response = client.get("/body")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, body)
        self.assertEqual(response.headers["content-length"], str(len(body)))
        self.assertNotIn("transfer-encoding", response.headers)


class TlsOutputBufferTests(unittest.TestCase):
    """The stdlib/OpenSSL property the slices rely on, checked through ``ssl.MemoryBIO`` byte counts.

    asyncio's ``sslproto`` writes a ``transport.write`` into the SSL object at once and then drains the
    output BIO; the encrypted bytes of one write all pass through the BIO before the first is read out.
    """

    def test_one_write_puts_the_whole_body_in_the_output_bio_and_slices_bound_it(self):
        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        client_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        client_ctx.check_hostname = False
        client_ctx.verify_mode = ssl.CERT_NONE
        cert = _self_signed_pair()
        if cert is None:
            self.skipTest("no certificate tooling (cryptography) in this environment")
        server_ctx.load_cert_chain(*cert)

        def connected():
            si, so, ci, co = ssl.MemoryBIO(), ssl.MemoryBIO(), ssl.MemoryBIO(), ssl.MemoryBIO()
            server = server_ctx.wrap_bio(si, so, server_side=True)
            client = client_ctx.wrap_bio(ci, co, server_side=False)
            for _ in range(10):
                for side in (client, server):
                    try:
                        side.do_handshake()
                    except ssl.SSLWantReadError:
                        pass
                ci.write(so.read())
                si.write(co.read())
            return server, so

        body = b"x" * (4 * SLICE)
        server, out = connected()
        server.write(body)  # one transport.write of the whole body
        self.assertGreater(out.pending, len(body))
        out.read()
        server, out = connected()
        peak = 0
        for start in range(0, len(body), SLICE):  # SlicedBodyResponse: one write per slice
            server.write(body[start:start + SLICE])
            peak = max(peak, out.pending)
            out.read()
        self.assertLess(peak, SLICE + 16 * 1024)


def _self_signed_pair():
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import ec
        from cryptography.x509.oid import NameOID
    except ImportError:
        return None
    import datetime
    import os
    import tempfile

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(x509.random_serial_number()).not_valid_before(now - datetime.timedelta(minutes=1))
            .not_valid_after(now + datetime.timedelta(hours=1)).sign(key, hashes.SHA256()))
    directory = tempfile.mkdtemp(prefix="idr129-tls-")
    cert_path, key_path = os.path.join(directory, "c.pem"), os.path.join(directory, "k.pem")
    with open(cert_path, "wb") as handle:
        handle.write(cert.public_bytes(serialization.Encoding.PEM))
    with open(key_path, "wb") as handle:
        handle.write(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                       serialization.NoEncryption()))
    return cert_path, key_path


def _requirement(uid: str, rows: int) -> DataRequirement:
    return DataRequirement(
        instrument_uid=uid, feed=FeedType.BAR, consumer_grade=ConsumerGrade.ALPHA,
        source_policy_id="crypto_primary_v2", interval="1m",
        warmup=WarmupSpecification.for_rows(rows, deadline_ms=5_000),
    )


def _service_refusing_items():
    """The production shape of a refused local item: history_many hands back a typed error per item,
    ``work`` raises it and the real bounded executor stores it on the item's execution."""

    class Backend:
        @staticmethod
        def warmup_is_local(_requirement):
            return True

        @staticmethod
        def history_many(requirements):
            return {
                requirement: QueryServiceError(
                    QueryProblem(CanonicalErrorCode.DATA_STALE, "stale", True),
                    request_id="req-1", instrument_uid=requirement.instrument_uid,
                )
                for requirement in requirements
            }

    class Service(V2QueryService):
        def __init__(self):
            self.backend = Backend()
            self.instruments = SimpleNamespace()
            self.warmup_executor = BoundedWarmupExecutor()
            self.last_batch_evidence = {}
            self._small_local_warmup_lane = True
            self._queued_local_batch_lane = True
            self._local_batch_admission = _LocalBatchAdmission(policy=_queued_local_batch_lane_policy())

    return Service()


class RefusedItemCycleTests(unittest.TestCase):
    def test_a_refused_item_keeps_its_problem_and_leaves_no_work_frame_cycle(self):
        service = _service_refusing_items()

        async def warm():
            async def completion(result):
                return result
            return await service.warmup_batch_completed_async(
                BatchRequirement(consumer_id="alpha.okx", requirements=(_requirement("a", 2500),
                                                                         _requirement("b", 2500))),
                purpose=AccessPurpose.INTERNAL_ALPHA, completion=completion,
            )

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(warm())  # warm imports and lazily built pools
            gc.collect()
            gc.set_debug(gc.DEBUG_SAVEALL)
            try:
                result = loop.run_until_complete(warm())
                del result
                gc.collect()
            finally:
                gc.set_debug(0)
            frames = [
                f"{item.f_code.co_filename}:{item.f_code.co_qualname}"
                for item in gc.garbage if type(item).__name__ == "frame"
            ]
            gc.garbage.clear()
            result = loop.run_until_complete(warm())
        finally:
            loop.close()
        self.assertEqual([item.status for item in result.results], ["DATA_STALE", "DATA_STALE"])
        self.assertTrue(all(item.problem.retryable for item in result.results))
        self.assertEqual([f for f in frames if "qdl/" in f], [])


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class MemoryMaintenanceTests(unittest.TestCase):
    def _maintenance(self, *, rss=900.0, busy=False, **kwargs):
        self.clock, self.calls, self.records = _Clock(), [], []
        self.state = {"rss": rss, "busy": busy}
        return MemoryMaintenance(
            busy=lambda: self.state["busy"], rss=lambda: self.state["rss"],
            collect=lambda: self.calls.append("collect") or 7, trim=lambda: self.calls.append("trim") or 1,
            clock=self.clock, wall_ms=lambda: 1, record=self.records.append, **kwargs,
        )

    def test_runs_one_collection_and_trim_when_idle_and_above_the_soft_limit(self):
        maintenance = self._maintenance(soft_limit_mib=512, idle_seconds=3, min_interval_seconds=120)
        self.assertIsNone(maintenance.tick())  # idle for 0 s since construction
        self.clock.now += 3
        record = maintenance.tick()
        self.assertEqual(self.calls, ["collect", "trim"])
        self.assertEqual(record["kind"], "memory_maintenance")
        self.assertEqual((record["collected"], record["soft_limit_mib"], record["rss_before_mib"]), (7, 512, 900.0))
        self.assertEqual(self.records, [record])

    def test_never_runs_below_the_soft_limit_while_busy_or_inside_the_interval(self):
        maintenance = self._maintenance(soft_limit_mib=512, idle_seconds=3, min_interval_seconds=120)
        self.state["rss"] = 400.0
        self.clock.now += 10
        self.assertIsNone(maintenance.tick())
        self.state.update(rss=900.0, busy=True)
        self.assertIsNone(maintenance.tick())
        self.state["busy"] = False
        self.clock.now += 2  # the lane was busy 2 s ago
        self.assertIsNone(maintenance.tick())
        self.clock.now += 1
        self.assertIsNotNone(maintenance.tick())
        self.clock.now += 119
        self.assertIsNone(maintenance.tick())
        self.clock.now += 1
        self.assertIsNotNone(maintenance.tick())
        self.assertEqual(self.calls, ["collect", "trim"] * 2)

    def test_soft_limit_zero_disables_and_the_environment_is_validated(self):
        maintenance = self._maintenance(soft_limit_mib=0, idle_seconds=0, min_interval_seconds=0)
        self.clock.now += 100
        self.assertIsNone(maintenance.tick())
        self.assertEqual(self.calls, [])
        self.assertEqual(soft_limit_from_environment({}), DEFAULT_SOFT_LIMIT_MIB)
        self.assertEqual(soft_limit_from_environment({SOFT_LIMIT_ENV: "0"}), 0)
        self.assertEqual(soft_limit_from_environment({SOFT_LIMIT_ENV: "1024"}), 1024)
        with self.assertRaises(ValueError):
            soft_limit_from_environment({SOFT_LIMIT_ENV: "-1"})

    def test_a_failing_recorder_does_not_break_maintenance(self):
        maintenance = MemoryMaintenance(busy=lambda: False, rss=lambda: 900.0, collect=lambda: 0, trim=lambda: 0,
                                        idle_seconds=0, min_interval_seconds=0, record=_raise)
        self.assertIsNotNone(maintenance.tick())

    def test_installed_on_the_app_and_runs_with_its_lifespan(self):
        app = FastAPI()
        service = SimpleNamespace(large_warmup_lane_busy=lambda: True)
        maintenance = install_query_memory_maintenance(app, service)
        self.assertIs(app.state.memory_maintenance, maintenance)
        with TestClient(app):
            pass
        self.assertEqual(maintenance.runs, 0)  # the lane is busy

    def test_the_lane_busy_signal_follows_the_large_warmup_lane(self):
        service = _service_refusing_items()
        self.assertFalse(service.large_warmup_lane_busy())

        async def hold():
            gate = asyncio.Event()
            task = asyncio.create_task(service._local_batch_admission.run(gate.wait, consumer_id="a"))
            await asyncio.sleep(0)
            busy = service.large_warmup_lane_busy()
            gate.set()
            await task
            return busy, service.large_warmup_lane_busy()

        self.assertEqual(asyncio.run(hold()), (True, False))


def _raise(_record):
    raise RuntimeError("recorder down")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
