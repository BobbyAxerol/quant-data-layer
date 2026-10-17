"""IDR-114: a refused Query read leaves no reference cycle for the cyclic GC.

The isolated stage-100 replay (2026-10-17, DEBUG_SAVEALL on one replica) showed every
refused snapshot leaving ~190 cyclic objects - the frames of the hot thread, the lane
task and the ASGI stack, held by the refusal's traceback - which the GC then freed in
long GIL-holding collections. The handlers now render the typed refusal and release
the traceback chain; the response is unchanged.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import gc
import importlib
from types import SimpleNamespace
import unittest

from fastapi.testclient import TestClient

from qdl.api_v2 import create_v2_app
from qdl.query import CanonicalErrorCode, QueryProblem
from qdl.query.cold_work import await_in_thread
from qdl.query.results import release_exception_frames
from qdl.query.service import QueryServiceError
from qdl.security import DataPlaneAccessError

router_module = importlib.import_module("qdl.api_v2.router")
_PARAMS = {"feed": "QUOTE", "source_policy_id": "crypto_primary_v2", "consumer_grade": "EXECUTION"}
_HEADERS = {"X-QDL-Purpose": "INTERNAL_ALPHA"}


class _Access(SimpleNamespace):
    def __getattr__(self, name):
        if name.startswith("require_"):
            return lambda *args, **kwargs: None
        raise AttributeError(name)


class _RefusingService:
    """The production shape of a refused snapshot: lane task -> hot thread -> typed refusal."""

    def __init__(self):
        self.pool = ThreadPoolExecutor(1)

    @staticmethod
    def request_id():
        return "req-1"

    def snapshot(self, requirement, *, request_id):
        rows = [object() for _ in range(16)]  # a frame local the refusal's traceback would keep
        del rows
        raise QueryServiceError(
            QueryProblem(CanonicalErrorCode.DATA_STALE, "stale", True),
            request_id=request_id, instrument_uid=requirement.instrument_uid,
        )

    async def snapshot_async(self, requirement, *, purpose, consumer_id, request_id=None):
        async def execute():
            return await await_in_thread(self.pool, self.snapshot, requirement, request_id="req-1")

        task = asyncio.create_task(execute())
        return await asyncio.shield(task)


def _server_frames(garbage) -> list[str]:
    names = []
    for item in garbage:
        if type(item).__name__ == "frame":
            names.append(f"{item.f_code.co_filename}:{item.f_code.co_qualname}")
    return [name for name in names if "/httpx/" not in name and "/anyio/" not in name]


class RefusalCycleTests(unittest.TestCase):
    def _client(self, access):
        service = _RefusingService()
        self.addCleanup(service.pool.shutdown)
        app = create_v2_app(service)  # type: ignore[arg-type]
        app.dependency_overrides[router_module._data_access] = access
        return TestClient(app)

    def _garbage_after(self, client, count=10):
        for _ in range(3):  # warm imports and lazily built objects
            client.get("/v2/market-data/inst-1/snapshot", params=_PARAMS, headers=_HEADERS)
        enabled = gc.isenabled()
        gc.collect()
        gc.disable()
        try:
            gc.set_debug(gc.DEBUG_SAVEALL)
            responses = [client.get("/v2/market-data/inst-1/snapshot", params=_PARAMS, headers=_HEADERS)
                         for _ in range(count)]
            gc.collect()
            garbage = list(gc.garbage)
        finally:
            gc.set_debug(0)
            gc.garbage.clear()
            if enabled:
                gc.enable()
        return responses, garbage

    def test_a_refused_snapshot_keeps_its_typed_response_and_leaves_no_server_frame_cycle(self):
        client = self._client(lambda: _Access(consumer_id="alpha"))
        responses, garbage = self._garbage_after(client)
        for response in responses:
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.json(), {
                "type": "urn:qdl:error:data-stale", "title": "Data Stale", "status": 503, "code": "DATA_STALE",
                "detail": "stale", "request_id": "req-1", "retryable": True, "instrument_uid": "inst-1",
            })
        self.assertEqual(_server_frames(garbage), [])

    def test_a_data_plane_refusal_leaves_no_server_frame_cycle(self):
        def refuse():
            raise DataPlaneAccessError("RATE_LIMITED", "quota exhausted", status_code=429)

        client = self._client(refuse)
        responses, garbage = self._garbage_after(client)
        self.assertEqual({response.status_code for response in responses}, {429})
        self.assertEqual(responses[0].json()["code"], "RATE_LIMITED")
        self.assertEqual(_server_frames(garbage), [])

    def test_release_clears_the_whole_chain_and_keeps_the_typed_fields(self):
        problem = QueryProblem(CanonicalErrorCode.DATA_NOT_READY, "not ready", True, retry_after_ms=500)

        def inner():
            raise KeyError("cache")

        def outer():
            try:
                inner()
            except KeyError as error:
                try:
                    raise ValueError("context")
                except ValueError:
                    raise QueryServiceError(problem, request_id="r", instrument_uid="u") from error

        try:
            outer()
        except QueryServiceError as caught:
            error = caught
        cause, context = error.__cause__, error.__context__
        self.assertIsNotNone(error.__traceback__)
        self.assertIsNotNone(cause.__traceback__)
        release_exception_frames(error)
        for item in (error, cause, context, context.__context__):
            self.assertIsNone(item.__traceback__)
        self.assertIs(error.__cause__, cause)
        self.assertIs(error.problem, problem)
        self.assertEqual((error.request_id, error.instrument_uid), ("r", "u"))

    def test_release_terminates_on_a_self_referencing_chain(self):
        error = ValueError("loop")
        error.__context__ = error
        release_exception_frames(error)
        self.assertIsNone(error.__traceback__)


if __name__ == "__main__":
    unittest.main()
