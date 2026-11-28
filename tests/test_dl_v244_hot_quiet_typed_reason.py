"""DL v2.4.4 (DL-DEF-2026-11-26-M follow-up): Query keeps the gateway's HOT_QUIET_BEYOND_MAX typed.

The stream gateway refuses a last value quiet past QDL_KN_MAX_QUIET_SECONDS with gRPC UNAVAILABLE and the detail
HOT_QUIET_BEYOND_MAX (rust/qdl-stream-gateway/src/hot_view.rs). Before this change Query's hot client reported every
UNAVAILABLE detail outside a fixed list as HOT_READER_UNAVAILABLE, so a quiet key looked like a reader outage.

Covered here:
- the client keeps the typed reason, still tries the second replica, and still hides untyped server text;
- the same through a real in-process gRPC server and channel (signature metadata, status code and detail on the
  wire, the PATH the gateway serves), not a fake callable;
- Query's backend refusal text carries the typed token, so refusal_log.reason_tokens reports it.
"""
from __future__ import annotations

import base64
from concurrent import futures
import hmac
import json
import threading
import unittest

import grpc
from google.protobuf.wrappers_pb2 import BytesValue

from qdl.runtime.kn_hot_view import (
    DOMAIN, PATH, SCHEMA, TYPED_TRANSIENT_DETAILS, CanonicalHotClient, HotViewUnavailable,
)
from qdl.runtime.refusal_log import reason_tokens
from tests import test_kn_hot_view as base

SECRET = b"q" * 32


def _refusal(code, detail):
    class Refused(grpc.RpcError):
        def code(self):
            return code

        def details(self):
            return detail

    def failed(*args, **kwargs):
        raise Refused()
    return failed


class TypedQuietReasonClientTests(base.HotClientTests):
    """Reuses the HotClientTests fixtures; the inherited tests run again unchanged."""

    def test_quiet_beyond_max_is_kept_typed(self):
        self.assertIn("HOT_QUIET_BEYOND_MAX", TYPED_TRANSIENT_DETAILS)
        quiet = _refusal(grpc.StatusCode.UNAVAILABLE, "HOT_QUIET_BEYOND_MAX")
        client = CanonicalHotClient(calls=(quiet, quiet), expectation=self.expectation, secret=SECRET)
        with self.assertRaisesRegex(HotViewUnavailable, "^HOT_QUIET_BEYOND_MAX$"):
            client.latest(self.binding, self.lpk)

    def test_quiet_on_one_replica_still_tries_the_other(self):
        quiet = _refusal(grpc.StatusCode.UNAVAILABLE, "HOT_QUIET_BEYOND_MAX")
        client = self.client()
        client._calls = (quiet, client._calls[0])
        self.assertEqual(client.latest(self.binding, self.lpk).boundary.offset, 0)

    def test_previous_typed_and_untyped_details_are_unchanged(self):
        for detail, expected in (("HOT_RECORD_MISSING", "HOT_RECORD_MISSING"),
                                 ("HOT_QUIET_BEYOND_MAX_AND_MORE", "HOT_READER_UNAVAILABLE"),
                                 (None, "HOT_READER_UNAVAILABLE")):
            with self.subTest(detail=detail):
                client = CanonicalHotClient(calls=(_refusal(grpc.StatusCode.UNAVAILABLE, detail),),
                    expectation=self.expectation, secret=SECRET)
                with self.assertRaisesRegex(HotViewUnavailable, f"^{expected}$"):
                    client.latest(self.binding, self.lpk)


class _GatewayDouble:
    """A real gRPC server on 127.0.0.1 answering the gateway's PATH: it verifies the request signature like the
    gateway does, then answers with the configured reply (a JSON body) or a (StatusCode, detail) refusal."""

    def __init__(self):
        self.answers = []
        self.requests = 0
        self.lock = threading.Lock()
        handler = grpc.unary_unary_rpc_method_handler(self._read,
            request_deserializer=BytesValue.FromString, response_serializer=BytesValue.SerializeToString)
        service, method = PATH.strip("/").split("/")
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        self.server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(service, {method: handler}),))
        self.port = self.server.add_insecure_port("127.0.0.1:0")
        self.server.start()
        self.channel = grpc.insecure_channel(f"127.0.0.1:{self.port}", options=(("grpc.enable_retries", 0),))
        self.call = self.channel.unary_unary(PATH, request_serializer=BytesValue.SerializeToString,
            response_deserializer=BytesValue.FromString)

    def _read(self, request, context):
        signature = dict(context.invocation_metadata()).get("x-qdl-hot-signature-bin")
        if signature != hmac.digest(SECRET, DOMAIN + request.value, "sha256"):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "HOT_SIGNATURE_INVALID")
        with self.lock:
            self.requests += 1
            answer = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        if isinstance(answer, tuple):
            context.abort(*answer)
        return BytesValue(value=json.dumps(answer).encode())

    def close(self):
        self.channel.close()
        self.server.stop(None)


class TypedQuietReasonOverGrpcTests(unittest.TestCase):
    def setUp(self):
        self.gateway = _GatewayDouble()
        self.addCleanup(self.gateway.close)
        fixture = base.HotClientTests("test_preserves_bytes_zero_offset_and_no_cache_generation")
        fixture.setUp()
        self.binding, self.lpk, self.expectation = fixture.binding, fixture.lpk, fixture.expectation
        self.reply = fixture.reply

    def client(self):
        return CanonicalHotClient(calls=(self.gateway.call,), expectation=self.expectation, secret=SECRET)

    def test_typed_quiet_refusal_crosses_the_wire(self):
        self.gateway.answers = [(grpc.StatusCode.UNAVAILABLE, "HOT_QUIET_BEYOND_MAX")]
        with self.assertRaisesRegex(HotViewUnavailable, "^HOT_QUIET_BEYOND_MAX$"):
            self.client().latest(self.binding, self.lpk)
        self.assertEqual(self.gateway.requests, 1)

    def test_valid_reply_then_quiet_refusal_on_the_same_channel(self):
        self.gateway.answers = [self.reply, (grpc.StatusCode.UNAVAILABLE, "HOT_QUIET_BEYOND_MAX")]
        client = self.client()
        self.assertEqual(client.latest(self.binding, self.lpk).rows[0].canonical, b"test-only")
        with self.assertRaisesRegex(HotViewUnavailable, "^HOT_QUIET_BEYOND_MAX$"):
            client.latest(self.binding, self.lpk)

    def test_untyped_server_text_is_still_hidden_over_the_wire(self):
        self.gateway.answers = [(grpc.StatusCode.UNAVAILABLE, "private server diagnostic")]
        with self.assertRaisesRegex(HotViewUnavailable, "^HOT_READER_UNAVAILABLE$"):
            self.client().latest(self.binding, self.lpk)


class TypedQuietReasonQueryTests(base.HotQuerySelectionTests):
    """Query's KN backend over the hot client: the refusal text, and so the refusal tokens, name the quiet key."""

    def test_query_refusal_carries_the_typed_quiet_token(self):
        from qdl.query.results import QueryBackendError
        self.assertTrue(self.backend.latest(self.requirement).quality.execution_eligible)
        self.backup = HotViewUnavailable("HOT_QUIET_BEYOND_MAX")
        with self.assertRaises(QueryBackendError) as refused:
            self.backend.latest(self.requirement)
        detail = refused.exception.problem.detail
        self.assertEqual(detail, "HOT_BACKUP_UNAVAILABLE_PRIMARY_BEHIND: HOT_QUIET_BEYOND_MAX")
        self.assertEqual(reason_tokens(detail), ("HOT_BACKUP_UNAVAILABLE_PRIMARY_BEHIND", "HOT_QUIET_BEYOND_MAX"))

    def test_query_refusal_through_the_real_grpc_client(self):
        from qdl.query.results import QueryBackendError
        self.assertTrue(self.backend.latest(self.requirement).quality.execution_eligible)
        gateway = _GatewayDouble()
        self.addCleanup(gateway.close)
        gateway.answers = [(grpc.StatusCode.UNAVAILABLE, "HOT_QUIET_BEYOND_MAX")]
        fixture = base.HotClientTests("test_preserves_bytes_zero_offset_and_no_cache_generation")
        fixture.setUp()
        client = CanonicalHotClient(calls=(gateway.call,), expectation=fixture.expectation, secret=SECRET)
        self.backend.hot_client = client
        with self.assertRaises(QueryBackendError) as refused:
            self.backend.latest(self.requirement)
        self.assertIn("HOT_QUIET_BEYOND_MAX", reason_tokens(refused.exception.problem.detail))
        self.assertEqual(gateway.requests, 1)


if __name__ == "__main__":
    unittest.main()
