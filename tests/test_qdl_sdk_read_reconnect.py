import unittest
from unittest.mock import patch

import httpx

from qdl_sdk.transport import RestQueryTransport


class ReadReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def test_read_methods_retry_closed_socket_once_with_remaining_budget(self):
        for method, path in (("get", "/v2/feeds/x/status"),
                             ("post", "/v2/market-data/warmup:batch"),
                             ("post", "/v2/market-data/reference:batch")):
            calls = []

            def handle(request):
                calls.append(request)
                if len(calls) == 1:
                    raise httpx.RemoteProtocolError("peer closed", request=request)
                return httpx.Response(200, json={"status": "OK"})

            async with httpx.AsyncClient(base_url="https://qdl.test", transport=httpx.MockTransport(handle)) as client:
                transport = RestQueryTransport("https://qdl.test", client=client, timeout_seconds=10)
                with patch("qdl_sdk.transport.time.monotonic", side_effect=[100.0, 102.0]):
                    response = await transport._read_request(method, path)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(len(calls), 2)
                self.assertEqual(calls[0].method, calls[1].method)
                self.assertEqual(calls[0].url, calls[1].url)
                self.assertEqual(calls[1].extensions["timeout"]["read"], 8.0)

    async def test_permanent_socket_failure_is_bounded_and_timeout_is_not_retried(self):
        for error_type, count in ((httpx.RemoteProtocolError, 2), (httpx.ReadTimeout, 1)):
            calls = []

            def handle(request):
                calls.append(request)
                raise error_type("unavailable", request=request)

            async with httpx.AsyncClient(base_url="https://qdl.test", transport=httpx.MockTransport(handle)) as client:
                transport = RestQueryTransport("https://qdl.test", client=client)
                with self.assertRaises(error_type):
                    await transport._read_request("get", "/v2/feeds/x/status")
                self.assertEqual(len(calls), count)

    async def test_exhausted_budget_and_http_errors_are_not_retried(self):
        calls = []

        def closed(request):
            calls.append(request)
            raise httpx.RemoteProtocolError("closed", request=request)

        async with httpx.AsyncClient(base_url="https://qdl.test", transport=httpx.MockTransport(closed)) as client:
            transport = RestQueryTransport("https://qdl.test", client=client, timeout_seconds=10)
            with patch("qdl_sdk.transport.time.monotonic", side_effect=[100.0, 111.0]):
                with self.assertRaises(httpx.RemoteProtocolError):
                    await transport._read_request("get", "/v2/feeds/x/status")
        self.assertEqual(len(calls), 1)
        for code in (401, 403, 429, 503):
            calls = []

            def handle(request):
                calls.append(request)
                return httpx.Response(code, json={"code": "DATA_STALE"})

            async with httpx.AsyncClient(base_url="https://qdl.test", transport=httpx.MockTransport(handle)) as client:
                transport = RestQueryTransport("https://qdl.test", client=client)
                result = await transport._read_request("get", "/v2/feeds/x/status")
                self.assertEqual(result.status_code, code)
                self.assertEqual(len(calls), 1)
