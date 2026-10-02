"""Bounded private Query -> Rust canonical hot read, independent of projector.

The reply is NOT an execution verdict. Query must validate lineage and current
quality using its existing oracle. No public credential, provider fallback,
cache mutation, synthetic generation, or per-request Kafka reader is involved.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
import hmac
import json
import threading
import time
from typing import Callable, Mapping

import grpc
from google.protobuf.wrappers_pb2 import BytesValue

from qdl.runtime.kn_market_cache import CacheRow, SourceBoundary
from qdl.projection.state_contract import LogicalProductKey

PATH = "/qdl.internal.v2.CanonicalHotView/ReadLatest"
SCHEMA = "qdl.kn.canonical-hot-view.v1"
DOMAIN = SCHEMA.encode() + b"\0"
HOT_FEEDS = frozenset({"TRADE", "QUOTE", "MARK_INDEX_PRICE", "BOOK_SNAPSHOT", "BOOK_DELTA"})


class HotViewUnavailable(RuntimeError):
    """Bounded backup refused; never permission to relax primary quality."""


@dataclass(frozen=True)
class CanonicalHotView:
    """Source coordinates, not a fabricated materialized-cache generation/fence."""
    lpk: LogicalProductKey
    rows: tuple[CacheRow, ...]
    boundary: SourceBoundary


class CanonicalHotClient:
    def __init__(self, *, calls: tuple[Callable, ...], expectation: Mapping[str, object],
                 secret: bytes, timeout_seconds: float = 0.25, clock_ns=time.time_ns):
        if not 1 <= len(calls) <= 2 or len(secret) < 32:
            raise ValueError("hot backup needs one/two targets and a >=32-byte secret")
        if not 0 < timeout_seconds <= 0.5:
            raise ValueError("hot backup deadline must be bounded by 500ms")
        self._calls = calls
        self._expectation = dict(expectation)
        self._secret = secret
        self._timeout = timeout_seconds
        self._clock_ns = clock_ns
        self._slots = threading.BoundedSemaphore(8)

    @classmethod
    def connect(cls, *, targets: tuple[str, ...], ca: bytes, cert: bytes, key: bytes, **kwargs):
        if any("://" in target or not target for target in targets):
            raise ValueError("hot targets must be TLS host:port")
        credentials = grpc.ssl_channel_credentials(ca, key, cert)
        channels = tuple(grpc.secure_channel(target, credentials, options=(
            ("grpc.max_receive_message_length", 512 * 1024),
            ("grpc.max_send_message_length", 8256),
            ("grpc.enable_retries", 0),
        )) for target in targets)
        try:
            client = cls(calls=tuple(channel.unary_unary(PATH,
                request_serializer=BytesValue.SerializeToString,
                response_deserializer=BytesValue.FromString) for channel in channels), **kwargs)
        except Exception:
            for channel in channels:
                channel.close()
            raise
        client._channels = channels
        return client

    def close(self):
        for channel in getattr(self, "_channels", ()):
            channel.close()

    def latest(self, binding, lpk) -> CanonicalHotView:
        if binding.feed.value not in HOT_FEEDS or binding.interval is not None:
            raise HotViewUnavailable("HOT_FEED_UNSUPPORTED")
        if not self._slots.acquire(blocking=False):
            raise HotViewUnavailable("HOT_READ_CAPACITY")
        try:
            deadline = time.monotonic() + self._timeout
            error = "HOT_READER_UNAVAILABLE"
            for index, call in enumerate(self._calls):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                body = json.dumps({**self._expectation, "schema": SCHEMA,
                    "binding_id": binding.binding_id, "issued_at_ns": self._clock_ns()},
                    separators=(",", ":"), sort_keys=True).encode()
                signature = hmac.digest(self._secret, DOMAIN + body, "sha256")
                try:
                    reply = call(BytesValue(value=body), timeout=remaining / (len(self._calls) - index),
                        metadata=(("x-qdl-hot-signature-bin", signature),), wait_for_ready=False)
                except grpc.RpcError as failure:
                    # Auth/contract failures are not healed by trying another replica.
                    if failure.code() not in {grpc.StatusCode.UNAVAILABLE,
                            grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.RESOURCE_EXHAUSTED}:
                        raise HotViewUnavailable("HOT_AUTHORITY_OR_PROTOCOL_REFUSED") from failure
                    detail = getattr(failure, "details", lambda: None)()
                    error = detail if detail in {
                        "HOT_BROKER_UNCONFIRMED", "HOT_READER_CATCHING_UP",
                        "HOT_READER_STALLED", "HOT_READER_UNAVAILABLE", "HOT_READER_BUSY",
                        "HOT_RECORD_MISSING", "HOT_READ_DEADLINE", "HOT_READ_CAPACITY",
                    } else "HOT_READER_UNAVAILABLE"
                    continue
                if time.monotonic() > deadline:
                    raise HotViewUnavailable("HOT_READ_DEADLINE")
                return self._decode(reply.value, binding, lpk)
            raise HotViewUnavailable(error)
        finally:
            self._slots.release()

    def _decode(self, raw: bytes, binding, lpk) -> CanonicalHotView:
        if len(raw) > 512 * 1024:
            raise HotViewUnavailable("HOT_REPLY_TOO_LARGE")
        try:
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError("reply must be an object")
            expected = {**self._expectation, "schema": SCHEMA, "binding_id": binding.binding_id,
                "product_key": lpk.encode(), "physical_key": binding.partition_key}
            if any(type(value.get(name)) is not type(item) or value[name] != item
                   for name, item in expected.items()):
                raise ValueError("identity/authority mismatch")
            partition, offset = value["source_partition"], value["source_offset"]
            if type(partition) is not int or type(offset) is not int or not 0 <= partition < 2**31 or not 0 <= offset < 2**63:
                raise ValueError("invalid source coordinate")
            record_offset = value["record_offset"]
            if type(record_offset) is not int or not 0 <= record_offset <= offset:
                raise ValueError("invalid record coordinate")
            canonical = base64.b64decode(value["canonical"], validate=True)
            if not canonical or len(canonical) > 256 * 1024:
                raise ValueError("invalid canonical size")
            return CanonicalHotView(lpk, (CacheRow(canonical=canonical, source_offset=record_offset),),
                SourceBoundary(value["source_topic_id"], partition, offset))
        except (ValueError, TypeError, KeyError) as error:
            raise HotViewUnavailable("HOT_REPLY_INVALID") from error


def hot_client_from_environment(environ, *, settings, catalog, config):
    """Opt-in only. Uses Query's existing mTLS identity and domain-separated key."""
    from pathlib import Path
    raw = environ.get("QDL_KN_HOT_READ_TARGETS", "").strip()
    if not raw:
        return None
    targets = tuple(item.strip() for item in raw.split(","))
    secret_path = environ.get("QDL_KN_READ_VIEW_SECRET_FILE", "")
    if not secret_path:
        raise ValueError("hot backup requires QDL_KN_READ_VIEW_SECRET_FILE")
    return CanonicalHotClient.connect(targets=targets,
        ca=config.tls_ca_path.read_bytes(), cert=config.tls_certificate_path.read_bytes(),
        key=config.tls_private_key_path.read_bytes(),
        secret=bytes.fromhex(Path(secret_path).read_text().strip()),
        timeout_seconds=float(environ.get("QDL_KN_HOT_READ_TIMEOUT_MS", "250"))/1000,
        expectation={"environment":settings.environment,"stream":catalog.canonical_stream,
            "source_topic_id":settings.topic_id,"partition_plan_epoch":settings.partition_plan_epoch,
            "source_policy_revision":catalog.source_policy_revision,"catalog_revision":catalog.catalog_revision,
            "route_generation":settings.route_generation,"schema_major":2})
