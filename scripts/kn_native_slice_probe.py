#!/usr/bin/env python3
"""KN-1 K1.5 harness: the real SDK against the native Rust gateway (shadow).

Purpose: prove real committed canonical record -> Python-issued cursor v3 ->
native gateway -> real SDK over mTLS + JWT on a container network, measure
the owner's four latency quantities with source provenance (K1-T05), check
every delivered resume token with the Python codec (cross-language cursor
proof on live data), and run the negative authentication/cursor/quota matrix
over real gRPC (K1-T03).

Boundary: a read client. It talks only to the shadow gateway and to the
disposable quota Redis of the KN-1 packet (seeding one counter for the quota
case and deleting it). It never contacts production Query/Stream, never
writes production Kafka and never prints tokens or key material; evidence
carries codes, counts, offsets and timing distributions only.

Subcommands:
  capture  copy committed canonical records of named physical keys from the
           production spool (``mode=ro``) into a capture file - durably
           captured provider-derived bytes, unmodified;
  load     publish a capture into an ISOLATED broker with transactions:
           ``history`` at once, ``live`` paced by the original arrival gaps,
           logging each record's commit time for commit->client latency;
  run      the SDK slice and the negative matrix (default).
  matrix   KN-2 K2.5: every demanded (consumer, stream requirement) of the
           manifests through the real SDK against two replicas in waves that
           respect each consumer's stream quota, a Kafka oracle by coordinate,
           failover by resuming from the last token when replica A dies, the
           negative matrix, Replay and the snapshot/status read view.
Replayed capture is never reported as live freshness.

``run`` exits 0 only when ``slice_verdict`` finds no failure (KN-1 review F3);
the result file always carries the verdict and its failure list.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
from pathlib import Path
import statistics
import sys
import time
import types
import uuid
from decimal import Decimal
from typing import Any, Sequence

ROOT = Path(__file__).resolve().parents[1]
ISSUER = "https://identity.qdl.stable.internal"
AUDIENCE = "qdl-v2-stable"
ROLES = ("historical_reader", "market_data_reader", "stream_consumer")
SUBSCRIBE = "/qdl.query.v2.MarketDataStreamService/Subscribe"
# The one authoritative negative matrix: case id -> status the gateway must
# return. `negatives()` runs exactly these cases; the verdict requires exactly
# these ids, once each, with these expected statuses (KN-1 review R2 F3).
NEGATIVE_CASES = {
    "no_bearer": "UNAUTHENTICATED",
    "jwt_hs256_alg": "UNAUTHENTICATED",
    "jwt_unknown_kid": "UNAUTHENTICATED",
    "jwt_wrong_audience": "UNAUTHENTICATED",
    "jwt_wrong_issuer": "UNAUTHENTICATED",
    "jwt_expired": "UNAUTHENTICATED",
    "jwt_lifetime_over_policy": "UNAUTHENTICATED",
    "jwt_wrong_environment": "UNAUTHENTICATED",
    "jwt_manifest_revision_mismatch": "UNAUTHENTICATED",
    "jwt_missing_jti": "UNAUTHENTICATED",
    "jwt_key_subject_mismatch": "UNAUTHENTICATED",
    "consumer_header_mismatch": "PERMISSION_DENIED",
    "purpose_not_allowed": "PERMISSION_DENIED",
    "requirement_outside_manifest": "PERMISSION_DENIED",
    "cursor_of_other_consumer": "INVALID_ARGUMENT",
    "cursor_tampered": "INVALID_ARGUMENT",
    "cursor_legacy_v2": "OUT_OF_RANGE",
    "cursor_expired": "OUT_OF_RANGE",
    "cursor_route_generation": "OUT_OF_RANGE",
    "cursor_catalog_revision": "OUT_OF_RANGE",
    "jwt_iat_i64_min": "UNAUTHENTICATED",
    "requirement_invalid_execution_partial": "INVALID_ARGUMENT",
    "no_client_certificate": "UNAVAILABLE",
    "quota_exhausted_shared_redis": "RESOURCE_EXHAUSTED",
}
EXPECTED_NEGATIVES = len(NEGATIVE_CASES)


def _duplicates(values: Sequence[Any]) -> list[Any]:
    seen: set[Any] = set()
    return sorted({value for value in values if value in seen or seen.add(value)}, key=str)


def slice_verdict(result: dict[str, Any], *, expected_products: Sequence[str]) -> list[str]:
    """Every reason the slice did not pass; empty means PASS.

    Coverage is exact: the product identities are exactly ``expected_products``
    (the probes' physical keys), each once, and the negative ids are exactly
    ``NEGATIVE_CASES``, each once, with the authoritative expected status. A
    missing, duplicated, unexpected or malformed entry fails; counts alone
    never pass. Per product: records delivered, no decode or resume-token
    error, strictly increasing offsets, exact next-record resume, Python vs
    proto digest parity, REPLAYING then LIVE. Every negative observed its
    expected status.
    """

    failures: list[str] = []
    wanted = list(expected_products)
    if not wanted:
        failures.append("products: no expected product identities")
    if _duplicates(wanted):
        failures.append(f"products: duplicate expected identities {_duplicates(wanted)}")
    products = result.get("products")
    if not isinstance(products, list):
        failures.append("products: missing or not a list")
        products = []
    names = [product.get("product") if isinstance(product, dict) else None for product in products]
    if _duplicates(names):
        failures.append(f"products: duplicated {_duplicates(names)}")
    missing = sorted(set(wanted) - set(names))
    unexpected = sorted((set(names) - set(wanted)), key=str)
    if missing:
        failures.append(f"products: missing {missing}")
    if unexpected:
        failures.append(f"products: unexpected {unexpected}")
    for product in products:
        if not isinstance(product, dict):
            failures.append("products: an entry is not an object")
            continue
        name = product.get("product", "?")
        records = product.get("records")
        if not isinstance(records, int) or isinstance(records, bool) or records < 1:
            failures.append(f"{name}: no records delivered")
        for field in ("decode_errors", "token_errors"):
            value = product.get(field)
            if not isinstance(value, int) or isinstance(value, bool) or value != 0:
                failures.append(f"{name}: {field}={value}")
        if product.get("offsets_strictly_increasing") is not True:
            failures.append(f"{name}: offsets not strictly increasing")
        if product.get("resume_exactly_next_record") is not True:
            failures.append(f"{name}: resume did not deliver exactly the next record")
        if product.get("digest_python_equals_proto_path") is not True:
            failures.append(f"{name}: requirement digest differs between Python and proto path")
        controls = product.get("controls")
        controls = list(controls) if isinstance(controls, list) else []
        if ("REPLAYING" not in controls or "LIVE" not in controls
                or controls.index("REPLAYING") > controls.index("LIVE")):
            failures.append(f"{name}: controls {controls} are not REPLAYING then LIVE")
    negatives = result.get("negatives")
    if not isinstance(negatives, list):
        failures.append("negatives: missing or not a list")
        negatives = []
    ids = [case.get("case") if isinstance(case, dict) else None for case in negatives]
    if _duplicates(ids):
        failures.append(f"negatives: duplicated {_duplicates(ids)}")
    missing = sorted(set(NEGATIVE_CASES) - set(ids))
    unexpected = sorted(set(ids) - set(NEGATIVE_CASES), key=str)
    if missing:
        failures.append(f"negatives: missing {missing}")
    if unexpected:
        failures.append(f"negatives: unexpected {unexpected}")
    for case in negatives:
        if not isinstance(case, dict) or case.get("case") not in NEGATIVE_CASES:
            continue
        name = case["case"]
        expected, observed = case.get("expected"), case.get("observed")
        if expected != NEGATIVE_CASES[name]:
            failures.append(f"negative {name}: expected {expected!r}, authoritative {NEGATIVE_CASES[name]}")
        elif not isinstance(observed, str) or observed != expected:
            failures.append(f"negative {name}: expected {expected}, observed {observed}")
    return failures


def _dist(values: Sequence[float]) -> dict[str, float]:
    if not values:
        return {"n": 0}
    ordered = sorted(values)

    def pick(q: float) -> float:
        return round(ordered[min(len(ordered) - 1, int(round(q * (len(ordered) - 1))))], 3)

    return {"n": len(ordered), "mean": round(statistics.fmean(ordered), 3), "p50": pick(0.5),
            "p95": pick(0.95), "p99": pick(0.99), "max": round(ordered[-1], 3)}


class Slice:
    def __init__(self, args: argparse.Namespace) -> None:
        from qdl.consumer.manifest import ConsumerManifestLoader
        from qdl.replay.cursor_v3 import SignedCursorV3Codec

        self.args = args
        self.bundle = json.loads(Path(args.bundle).read_text(encoding="utf-8"))
        keys = json.loads(Path(args.cursor_keys).read_text(encoding="utf-8"))
        self.codec = SignedCursorV3Codec({k: bytes.fromhex(v) for k, v in keys.items()},
                                         active_key_id=sorted(keys)[0])
        profile = json.loads(Path(args.profile).read_text(encoding="utf-8"))
        self.identities = {item["id"]: item for item in profile["identities"]}
        self.manifests = {m.consumer_id: m for m in (
            ConsumerManifestLoader.load(path) for path in sorted((ROOT / "consumers/stable").glob("*.yaml")))}
        self.probes = json.loads(Path(args.probes).read_text(encoding="utf-8"))

    # ---------------------------------------------------------------- tokens
    def binding(self, uid: str, feed: str, interval: str | None, policy: str) -> dict[str, Any]:
        for row in self.bundle["catalog"]["bindings"]:
            if (row["instrument_uid"], row["feed"], row["interval"], row["source_policy_id"]) == (uid, feed, interval, policy):
                return row
        raise KeyError("no binding")

    def requirement(self, consumer_id: str, uid: str, feed: str):
        manifest = self.manifests[consumer_id]
        return next(r for r in manifest.requirements if r.instrument_uid == uid and r.feed.value == feed)

    def claims(self, consumer_id: str, domain_requirement, offset: int, *, partition: int, **overrides):
        from qdl.replay.cursor_v3 import CursorV3Claims, requirement_digest

        binding = self.binding(domain_requirement.instrument_uid, domain_requirement.feed.value,
                               domain_requirement.interval, domain_requirement.source_policy_id)
        now = time.time_ns()
        fields = dict(
            key_id=self.codec.active_key_id, environment=self.bundle["environment"], consumer_id=consumer_id,
            requirement_digest=requirement_digest(domain_requirement), schema_major=2,
            stream=self.bundle["catalog"]["canonical_stream"], product_key=binding["product_key"],
            snapshot_id=hashlib.sha256(f"{binding['product_key']}|{offset}".encode()).hexdigest(),
            source_topic_id=self.args.topic_id, source_partition=partition, source_offset=offset,
            partition_plan_epoch=1, source_policy_revision=self.bundle["catalog"]["source_policy_revision"],
            catalog_revision=self.bundle["catalog"]["catalog_revision"],
            route_generation=self.args.route_generation, issued_at_ns=now, expires_at_ns=now + 3_600_000_000_000)
        fields.update(overrides)
        return CursorV3Claims(**fields)

    def jwt(self, consumer_id: str, **overrides) -> str:
        import jwt

        identity = self.identities[consumer_id]
        manifest = self.manifests[consumer_id]
        now = int(time.time())
        claims = {"sub": manifest.subject, "iss": ISSUER, "aud": AUDIENCE, "iat": now, "exp": now + 300,
                  "jti": str(uuid.uuid4()), "environment": manifest.environment, "roles": list(ROLES),
                  "venues": ["BINANCE", "OKX"], "consumer_manifest_revision": manifest.manifest_revision}
        header = {"kid": identity["jwt"]["key_id"]}
        algorithm = "RS256"
        key: bytes = Path(identity["jwt"]["private_key_file"]).read_bytes()
        for name, value in overrides.items():
            if name == "_kid":
                header["kid"] = value
            elif name == "_hs256":
                algorithm, key = "HS256", b"not-a-trusted-key-kn1-negative-case"
            elif value is None:
                claims.pop(name, None)
            else:
                claims[name] = value
        return jwt.encode(claims, key, algorithm=algorithm, headers=header)

    # ----------------------------------------------------------------- transport
    def tls(self, consumer_id: str):
        from qdl_sdk.tls import WorkloadTlsConfig

        tls = self.identities[consumer_id]["tls"]
        return WorkloadTlsConfig(tls["ca_file"], tls["cert_file"], tls["key_file"])

    def transport(self, consumer_id: str):
        from qdl_sdk.credentials import RotatingJwtCredentialProvider
        from qdl_sdk.transport import GrpcStreamTransport

        identity = self.identities[consumer_id]
        manifest = self.manifests[consumer_id]
        credential = RotatingJwtCredentialProvider(
            private_key_file=identity["jwt"]["private_key_file"], key_id=identity["jwt"]["key_id"],
            algorithm="RS256", issuer=ISSUER, audience=AUDIENCE, subject=manifest.subject,
            environment=manifest.environment, roles=ROLES, venues=("BINANCE", "OKX"),
            consumer_manifest_revision=manifest.manifest_revision, lifetime_seconds=300, refresh_before_seconds=60)
        targets = getattr(self.args, "targets", None) or self.args.target
        return GrpcStreamTransport(targets, tls=self.tls(consumer_id), credential_provider=credential)

    def shared_transport(self, consumer_id: str):
        """One transport (one channel and connection per target, one JWT
        provider) per consumer, as a consumer process holds it: a stream matrix
        must not cost the gateway a TLS handshake per stream."""
        cache = self.__dict__.setdefault("_transports", {})
        if consumer_id not in cache:
            cache[consumer_id] = self.transport(consumer_id)
        return cache[consumer_id]

    # ------------------------------------------------------------- positive run
    async def run_product(self, probe: dict[str, Any]) -> dict[str, Any]:
        from qdl.certification.phase103_consumer_acceptance import sdk_requirement
        from qdl.replay.cursor_v3 import CursorV3Expectation, requirement_digest
        from qdl.stream.grpc_service import requirement_from_proto
        from qdl_sdk.models import ControlEvent, StreamEvent

        consumer_id = probe["consumer_id"]
        domain = self.requirement(consumer_id, probe["instrument_uid"], probe["feed"])
        sdk = sdk_requirement(types.SimpleNamespace(requirement=domain))
        server_side = requirement_from_proto(sdk.to_proto())
        digest_match = requirement_digest(server_side) == requirement_digest(domain)
        start_offset = max(0, int(probe["offset"]) - int(self.args.replay_back))
        token = self.codec.encode(self.claims(consumer_id, domain, start_offset, partition=int(probe["partition"])))
        expectation = CursorV3Expectation(
            environment=self.bundle["environment"], stream=self.bundle["catalog"]["canonical_stream"],
            source_topic_id=self.args.topic_id, partition_plan_epoch=1,
            source_policy_revision=self.bundle["catalog"]["source_policy_revision"],
            catalog_revision=self.bundle["catalog"]["catalog_revision"], route_generation=self.args.route_generation)
        transport = self.transport(consumer_id)
        result: dict[str, Any] = {"consumer_id": consumer_id, "product": probe["physical_key"],
                                  "digest_python_equals_proto_path": digest_match, "start_offset": start_offset}
        request_ms, age_ms, lag_ms, e2e_ms, gateway_ms = [], [], [], [], []
        controls: list[str] = []
        offsets: list[int] = []
        token_errors = 0
        decode_errors = 0
        received_by_event: dict[str, int] = {}
        live_since_ns: int | None = None
        deadline = time.monotonic() + float(self.args.seconds)
        tokens: list[str] = []
        called = time.perf_counter()
        first_record_ms = None
        stream = transport.subscribe(sdk, consumer_id=consumer_id, cursor_token=token,
                                     max_buffer_events=int(self.args.buffer)).__aiter__()
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    item = await asyncio.wait_for(stream.__anext__(), timeout=remaining)
                except (asyncio.TimeoutError, StopAsyncIteration):
                    break
                received_ns = time.time_ns()
                if isinstance(item, ControlEvent):
                    controls.append(item.code)
                    if len(controls) == 1:
                        request_ms.append((time.perf_counter() - called) * 1000)
                    if item.code == "LIVE" and live_since_ns is None:
                        live_since_ns = received_ns
                    continue
                if not isinstance(item, StreamEvent):
                    continue
                envelope = item.event
                try:
                    payload = getattr(envelope, envelope.WhichOneof("payload"))
                    price = Decimal(payload.price.source_text)
                    if not price.is_finite() or envelope.instrument_uid != probe["instrument_uid"]:
                        raise ValueError("invalid trade")
                except Exception:  # noqa: BLE001
                    decode_errors += 1
                applied_ns = time.time_ns()
                if first_record_ms is None:
                    first_record_ms = (time.perf_counter() - called) * 1000
                offsets.append(item.logical_offset)
                tokens.append(item.resume_token)
                try:
                    claims = self.codec.verify(item.resume_token, consumer_id=consumer_id,
                                               environment=self.bundle["environment"],
                                               requirement_digest_value=requirement_digest(server_side),
                                               expected=expectation, now_ns=time.time_ns())
                    if claims.source_offset != item.logical_offset:
                        token_errors += 1
                except Exception:  # noqa: BLE001
                    token_errors += 1
                source_ns = int(envelope.source_event_time_ns)
                age_ms.append((received_ns - source_ns) / 1e6)
                lag_ms.append((int(envelope.published_at_ns) - source_ns) / 1e6)
                e2e_ms.append((applied_ns - source_ns) / 1e6)
                gateway_ms.append((received_ns - int(envelope.published_at_ns)) / 1e6)
                received_by_event[envelope.event_id.hex()] = received_ns
        finally:
            await stream.aclose()
        # Resume from the token of a record in the middle: the first record
        # delivered again must be exactly the next one - no gap, no repeat.
        warm_ms = None
        resumed_after = None
        expected_next = None
        if len(tokens) >= 2:
            middle = len(tokens) // 2
            expected_next = offsets[middle + 1]
            called = time.perf_counter()
            resumed = transport.subscribe(sdk, consumer_id=consumer_id, cursor_token=tokens[middle],
                                          max_buffer_events=int(self.args.buffer)).__aiter__()
            try:
                while time.perf_counter() - called < 30:
                    try:
                        item = await asyncio.wait_for(resumed.__anext__(), timeout=30)
                    except (asyncio.TimeoutError, StopAsyncIteration):
                        break
                    if warm_ms is None:
                        warm_ms = (time.perf_counter() - called) * 1000
                    if isinstance(item, StreamEvent):
                        resumed_after = item.logical_offset
                        break
            finally:
                await resumed.aclose()
        await transport.close()
        result["_received_by_event"] = received_by_event
        result["_live_since_ns"] = live_since_ns
        result.update({
            "controls": controls, "records": len(offsets), "decode_errors": decode_errors,
            "token_errors": token_errors,
            "offsets_strictly_increasing": all(b > a for a, b in zip(offsets, offsets[1:])),
            "first_offset": offsets[0] if offsets else None, "last_offset": offsets[-1] if offsets else None,
            "resume_expected_offset": expected_next, "resume_first_offset": resumed_after,
            "resume_exactly_next_record": resumed_after is not None and resumed_after == expected_next,
            "request_latency_ms": {"cold_first_control": _dist(request_ms),
                                   "cold_first_record": round(first_record_ms, 3) if first_record_ms else None,
                                   "warm_first_frame": round(warm_ms, 3) if warm_ms else None},
            "durable_event_age_ms": _dist(age_ms),
            "delivery_lag_ms": _dist(lag_ms),
            "end_to_end_cache_ms": _dist(e2e_ms),
            "canonical_publish_to_client_ms": _dist(gateway_ms),
        })
        return result

    # ------------------------------------------------------------- negative run
    async def expect_status(self, consumer_id: str, requirement_domain, token: str,
                            metadata: Sequence[tuple[str, str]], tls_consumer: str | None,
                            mutate=None) -> str:
        import grpc
        from qdl.certification.phase103_consumer_acceptance import sdk_requirement
        from qdl.query.v2 import query_pb2

        sdk = sdk_requirement(types.SimpleNamespace(requirement=requirement_domain))
        if tls_consumer is None:
            channel = grpc.aio.secure_channel(self.args.target, grpc.ssl_channel_credentials(
                root_certificates=Path(self.identities[consumer_id]["tls"]["ca_file"]).read_bytes()))
        else:
            channel = grpc.aio.secure_channel(self.args.target, self.tls(tls_consumer).grpc_credentials())
        call = channel.unary_stream(SUBSCRIBE, request_serializer=query_pb2.SubscribeRequest.SerializeToString,
                                    response_deserializer=query_pb2.SubscribeResponse.FromString)
        requirement = sdk.to_proto()
        if mutate is not None:
            mutate(requirement)
        request = query_pb2.SubscribeRequest(consumer_id=consumer_id, requirement=requirement,
                                             cursor_token=token, max_buffer_events=100)
        try:
            async for _ in call(request, metadata=tuple(metadata), timeout=15):
                return "OK"
            return "OK"
        except grpc.aio.AioRpcError as error:
            return error.code().name
        finally:
            await channel.close()

    async def negatives(self) -> list[dict[str, Any]]:
        import redis

        from qdl.replay.handoff import SignedHandoffCursorCodec, _TokenPayload

        okx, binance = "alpha.okx.paper.stable", "alpha.binance.paper.stable"
        probe = next(p for p in self.probes if p["consumer_id"] == okx)
        domain = self.requirement(okx, probe["instrument_uid"], probe["feed"])
        other = next(p for p in self.probes if p["consumer_id"] == binance)
        other_domain = self.requirement(binance, other["instrument_uid"], other["feed"])
        part = int(probe["partition"])
        offset = int(probe["offset"])
        good = self.codec.encode(self.claims(okx, domain, offset, partition=part))
        meta = lambda bearer, consumer=okx, purpose="INTERNAL_ALPHA": (  # noqa: E731
            ("authorization", f"Bearer {bearer}"), ("x-qdl-consumer-id", consumer), ("x-qdl-purpose", purpose))
        legacy = SignedHandoffCursorCodec({"legacy": b"L" * 32}, active_key_id="legacy",
                                          generation_id="0" * 32).encode(_TokenPayload(
            consumer_id=okx, snapshot_id="s", stream="md.canonical.v2", partition_key=probe["physical_key"],
            watermark_offset=1, issued_at_ns=1, expires_at_ns=2**62, key_id="legacy", generation_id="0" * 32))
        now = time.time_ns()
        cases = [
            ("no_bearer", good, (("x-qdl-consumer-id", okx), ("x-qdl-purpose", "INTERNAL_ALPHA")), okx, domain, "UNAUTHENTICATED"),
            ("jwt_hs256_alg", good, meta(self.jwt(okx, _hs256=True)), okx, domain, "UNAUTHENTICATED"),
            ("jwt_unknown_kid", good, meta(self.jwt(okx, _kid="kn1-unknown-kid")), okx, domain, "UNAUTHENTICATED"),
            ("jwt_wrong_audience", good, meta(self.jwt(okx, aud="other-audience")), okx, domain, "UNAUTHENTICATED"),
            ("jwt_wrong_issuer", good, meta(self.jwt(okx, iss="https://evil.example")), okx, domain, "UNAUTHENTICATED"),
            ("jwt_expired", good, meta(self.jwt(okx, iat=int(time.time()) - 600, exp=int(time.time()) - 1)), okx, domain, "UNAUTHENTICATED"),
            ("jwt_lifetime_over_policy", good, meta(self.jwt(okx, exp=int(time.time()) + 901)), okx, domain, "UNAUTHENTICATED"),
            ("jwt_wrong_environment", good, meta(self.jwt(okx, environment="live")), okx, domain, "UNAUTHENTICATED"),
            ("jwt_manifest_revision_mismatch", good, meta(self.jwt(okx, consumer_manifest_revision=999)), okx, domain, "UNAUTHENTICATED"),
            ("jwt_missing_jti", good, meta(self.jwt(okx, jti=None)), okx, domain, "UNAUTHENTICATED"),
            ("jwt_key_subject_mismatch", good, meta(self.jwt(okx, sub=self.manifests[binance].subject,
                                                             consumer_manifest_revision=self.manifests[binance].manifest_revision)),
             okx, domain, "UNAUTHENTICATED"),
            ("consumer_header_mismatch", good, meta(self.jwt(okx), consumer=binance), okx, domain, "PERMISSION_DENIED"),
            ("purpose_not_allowed", good, meta(self.jwt(okx), purpose="INTERNAL_EXECUTION"), okx, domain, "PERMISSION_DENIED"),
            ("requirement_outside_manifest", good, meta(self.jwt(okx)), okx, other_domain, "PERMISSION_DENIED"),
            ("cursor_of_other_consumer", self.codec.encode(self.claims(binance, domain, offset, partition=part)),
             meta(self.jwt(okx)), okx, domain, "INVALID_ARGUMENT"),
            ("cursor_tampered", good[:-2] + ("AA" if not good.endswith("AA") else "BA"), meta(self.jwt(okx)), okx, domain, "INVALID_ARGUMENT"),
            ("cursor_legacy_v2", legacy, meta(self.jwt(okx)), okx, domain, "OUT_OF_RANGE"),
            ("cursor_expired", self.codec.encode(self.claims(okx, domain, offset, partition=part,
                                                             issued_at_ns=now - 7_200_000_000_000,
                                                             expires_at_ns=now - 1)), meta(self.jwt(okx)), okx, domain, "OUT_OF_RANGE"),
            ("cursor_route_generation", self.codec.encode(self.claims(okx, domain, offset, partition=part,
                                                                      route_generation="kn-other-route")),
             meta(self.jwt(okx)), okx, domain, "OUT_OF_RANGE"),
            ("cursor_catalog_revision", self.codec.encode(self.claims(okx, domain, offset, partition=part,
                                                                      catalog_revision=999)),
             meta(self.jwt(okx)), okx, domain, "OUT_OF_RANGE"),
        ]
        # KN-1 review F2 over the wire: exp - iat overflowed i64 and wrapped
        # into an accepted lifetime; it must be a lifetime refusal.
        cases.append(("jwt_iat_i64_min", good, meta(self.jwt(okx, iat=-(2**63))), okx, domain, "UNAUTHENTICATED"))
        results = []
        for name, token, metadata, consumer, requirement_domain, expected in cases:
            code = await self.expect_status(consumer, requirement_domain, token, metadata, consumer)
            results.append({"case": name, "expected": expected, "observed": code, "pass": code == expected})
        # KN-1 review F1 over the wire: a requirement the Python server refuses
        # (execution grade without full coverage) is INVALID_ARGUMENT before any
        # manifest check; the pre-fix gateway parsed it (unit test on 14c19a7)
        # and left it to the manifest match.
        def partial_execution(requirement) -> None:
            from qdl.query.v2 import query_pb2

            requirement.grade = query_pb2.CONSUMER_GRADE_EXECUTION
            requirement.require_full_coverage = False

        code = await self.expect_status(okx, domain, good, meta(self.jwt(okx)), okx, mutate=partial_execution)
        results.append({"case": "requirement_invalid_execution_partial", "expected": "INVALID_ARGUMENT",
                        "observed": code, "pass": code == "INVALID_ARGUMENT"})
        # No client certificate: the mTLS handshake itself must fail.
        code = await self.expect_status(okx, domain, good, meta(self.jwt(okx)), None)
        results.append({"case": "no_client_certificate", "expected": "UNAVAILABLE", "observed": code,
                        "pass": code == "UNAVAILABLE"})
        # Shared quota: seed this minute's counter at the manifest limit on the
        # disposable quota Redis, expect RESOURCE_EXHAUSTED, then remove it.
        client = redis.Redis.from_url(self.args.quota_redis_url)
        identity = hashlib.sha256(okx.encode()).hexdigest()[:24]
        key = f"{self.args.quota_prefix}:quota:minute:{identity}:{int(time.time() // 60)}"
        client.set(key, self.manifests[okx].quotas.requests_per_minute, px=120_000)
        code = await self.expect_status(okx, domain, good, meta(self.jwt(okx)), okx)
        client.delete(key)
        results.append({"case": "quota_exhausted_shared_redis", "expected": "RESOURCE_EXHAUSTED", "observed": code,
                        "pass": code == "RESOURCE_EXHAUSTED"})
        ran = {result["case"]: result["expected"] for result in results}
        if ran != NEGATIVE_CASES:
            raise RuntimeError("negative matrix drifted from NEGATIVE_CASES; update both together")
        return results


def capture(args: argparse.Namespace) -> dict[str, Any]:
    import base64
    import sqlite3

    live = sqlite3.connect(f"file:{args.spool}?mode=ro", uri=True, timeout=5)
    live.execute("PRAGMA query_only=ON")
    rows = []
    keys = list(args.keys or [])
    if args.demand:
        keys += [f"{key}={args.demand}" for key in sorted({row["physical_key"] for row in demanded_streams()})]
    args.keys = keys
    for spec in keys:
        key, _, limit = spec.partition("=")
        for event_id, payload, accepted in live.execute(
                "SELECT event_id, payload, accepted_at_ns FROM events WHERE stream='md.canonical.v2' "
                "AND partition_key=? ORDER BY logical_offset DESC LIMIT ?", (key, int(limit or 2000))):
            rows.append({"key": key, "event_id": bytes(event_id).hex(), "accepted_at_ns": int(accepted),
                         "payload": base64.b64encode(bytes(payload)).decode()})
    live.close()
    rows.sort(key=lambda row: (row["accepted_at_ns"], row["event_id"]))
    with open(args.out, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    digest = hashlib.sha256(Path(args.out).read_bytes()).hexdigest()
    return {"records": len(rows), "keys": args.keys, "sha256": digest}


def load(args: argparse.Namespace) -> dict[str, Any]:
    import base64

    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    rows = [json.loads(line) for line in Path(args.capture).read_text(encoding="utf-8").splitlines() if line]
    split = int(len(rows) * float(args.history_fraction))
    if args.split == "key":
        # Every key keeps its oldest share as history, so a subscriber of any
        # captured product has committed records before the live phase.
        by_key: dict[str, list] = {}
        for row in rows:
            by_key.setdefault(row["key"], []).append(row)
        history, live = [], []
        for key_rows in by_key.values():
            cut = max(1, int(len(key_rows) * float(args.history_fraction) + 0.999))
            history += key_rows[:cut]
            live += key_rows[cut:]
        order = lambda row: (row["accepted_at_ns"], row["event_id"])  # noqa: E731
        rows = sorted(history, key=order) + sorted(live, key=order)
        split = len(history)
    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    if args.topic not in admin.list_topics(timeout=10).topics:
        for future in admin.create_topics([NewTopic(args.topic, num_partitions=6, replication_factor=1)]).values():
            future.result(20)
    producer = Producer({"bootstrap.servers": args.bootstrap, "transactional.id": f"kn1-capture-{args.phase}",
                         "enable.idempotence": True, "linger.ms": 5})
    producer.init_transactions(20)
    selected = rows[:split] if args.phase == "history" else rows[split:]
    commits = []
    aborted = 0
    if args.phase == "history":
        # Offset 0 of every partition holds a filler record: the SDK's
        # StreamEvent requires a positive logical offset (recorded KN-2
        # finding), and a fresh isolated topic starts at 0.
        producer.begin_transaction()
        for partition in range(6):
            producer.produce(args.topic, key=b"kn-filler", value=b"", partition=partition)
        producer.commit_transaction(30)
        for start in range(0, len(selected), 500):
            producer.begin_transaction()
            for row in selected[start:start + 500]:
                producer.produce(args.topic, key=row["key"].encode(), value=base64.b64decode(row["payload"]))
            producer.commit_transaction(30)
    elif float(args.rate or 0) > 0:
        # Capacity challenge (K2-T08): the captured records at a fixed rate,
        # 25 ms transactions. Accelerated capture, never live freshness.
        rate = float(args.rate)
        began = time.monotonic()
        index = 0
        batches = 0
        # `--repeat` cycles the capture to sustain the rate for the whole
        # window (capacity only: repeated records are new offsets, so this
        # mode is never used for the exactness oracle).
        total = len(selected) * (int(args.repeat) if int(args.repeat or 1) > 1 else 1)
        # A repeated record is the same event id at a new offset, and the
        # gateway may rightly skip one copy (coalescing, cursor floor): commit
        # times are logged by Kafka coordinate from the delivery reports.
        coordinates: list[tuple[int, int]] = []

        def delivered(error: Any, message: Any) -> None:
            if error is None:
                coordinates.append((message.partition(), message.offset()))

        while index < total and time.monotonic() - began < float(args.live_seconds):
            due = began + index / rate
            time.sleep(max(0.0, due - time.monotonic()))
            producer.begin_transaction()
            batch = []
            coordinates.clear()
            window_end = time.monotonic() + 0.025
            while index < total and began + index / rate <= window_end:
                row = selected[index % len(selected)]
                producer.produce(args.topic, key=row["key"].encode(), value=base64.b64decode(row["payload"]),
                                 on_delivery=delivered)
                batch.append(row["event_id"])
                index += 1
            producer.commit_transaction(30)
            committed_ns = time.time_ns()
            commits.extend({"partition": partition, "offset": offset, "commit_ns": committed_ns}
                           for partition, offset in coordinates)
            batches += 1
            if int(args.abort_every or 0) and batch and batches % int(args.abort_every) == 0:
                producer.begin_transaction()
                for position in range(index - len(batch), index):
                    row = selected[position % len(selected)]
                    producer.produce(args.topic, key=row["key"].encode(), value=b"kn-aborted")
                producer.abort_transaction(30)
                aborted += len(batch)
        with open(args.commit_log, "w", encoding="utf-8") as handle:
            for row in commits:
                handle.write(json.dumps(row) + "\n")
    else:
        # Original inter-arrival gaps; batches close every 25 ms like the
        # canonical core, and each record's commit wall time is logged.
        batches = 0
        began = time.monotonic()
        origin = selected[0]["accepted_at_ns"] if selected else 0
        index = 0
        while index < len(selected) and time.monotonic() - began < float(args.live_seconds):
            due = began + (selected[index]["accepted_at_ns"] - origin) / 1e9
            time.sleep(max(0.0, due - time.monotonic()))
            producer.begin_transaction()
            batch = []
            window_end = time.monotonic() + 0.025
            while index < len(selected):
                row = selected[index]
                if began + (row["accepted_at_ns"] - origin) / 1e9 > window_end:
                    break
                producer.produce(args.topic, key=row["key"].encode(), value=base64.b64decode(row["payload"]))
                batch.append(row["event_id"])
                index += 1
            producer.commit_transaction(30)
            committed_ns = time.time_ns()
            commits.extend({"event_id": event_id, "commit_ns": committed_ns} for event_id in batch)
            # Aborted batches leave offsets a committed reader never sees
            # (K2-T01 over real Kafka): the same keys, then abort.
            batches += 1
            if int(args.abort_every or 0) and batch and batches % int(args.abort_every) == 0:
                producer.begin_transaction()
                for row in selected[index - len(batch):index]:
                    producer.produce(args.topic, key=row["key"].encode(), value=b"kn-aborted")
                producer.abort_transaction(30)
                aborted += len(batch)
        with open(args.commit_log, "w", encoding="utf-8") as handle:
            for row in commits:
                handle.write(json.dumps(row) + "\n")
    return {"aborted": aborted, "phase": args.phase, "published": len(selected) if args.phase == "history" else len(commits),
            "history_fraction": float(args.history_fraction)}



# ------------------------------------------------------------------ matrix
STREAM_FEEDS = frozenset({"TRADE", "QUOTE", "BAR", "BOOK_SNAPSHOT", "BOOK_DELTA", "MARK_INDEX_PRICE"})
MATRIX_ALLOWED_ERRORS = frozenset({"DEPENDENCY_UNAVAILABLE"})


def demanded_streams() -> list[dict[str, Any]]:
    """Every (consumer, requirement) a manifest may stream from a stable
    binding, from the Python manifest and catalog loaders."""
    from qdl.consumer.manifest import ConsumerManifestLoader
    from qdl.runtime.stable_catalog import StableSourceCatalog

    catalog = StableSourceCatalog.load(ROOT / "config/v2/stable-source-bindings.yaml")
    rows = []
    for path in sorted((ROOT / "consumers/stable").glob("*.yaml")):
        manifest = ConsumerManifestLoader.load(path)
        permissions = {getattr(item, "value", item) for item in manifest.allowed_permissions}
        if "stream:read" not in permissions:
            continue
        for requirement in manifest.requirements:
            if requirement.feed.value not in STREAM_FEEDS:
                continue
            try:
                binding = catalog.binding_for(requirement)
            except Exception:  # noqa: BLE001 - pass-through products have no stable binding
                continue
            rows.append({"consumer_id": manifest.consumer_id, "requirement": requirement,
                         "physical_key": binding.partition_key, "feed": requirement.feed.value,
                         "interval": requirement.interval, "max_streams": int(manifest.quotas.max_streams),
                         "buffer": int(manifest.quotas.max_buffer_events)})
    return rows


def kafka_oracle(bootstrap: str, topic: str) -> dict[str, list[tuple[int, int, bytes]]]:
    """Every committed record per physical key, by coordinate (read_committed)."""
    from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition

    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"kn-oracle-{uuid.uuid4().hex[:8]}",
                         "enable.auto.commit": False, "isolation.level": "read_committed",
                         "enable.partition.eof": True})
    try:
        partitions = sorted(consumer.list_topics(topic, timeout=10).topics[topic].partitions)
        consumer.assign([TopicPartition(topic, partition, OFFSET_BEGINNING) for partition in partitions])
        done: set[int] = set()
        records: dict[str, list[tuple[int, int, bytes]]] = {}
        deadline = time.monotonic() + 180
        while len(done) < len(partitions) and time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    done.add(message.partition())
                    continue
                raise RuntimeError(str(message.error()))
            key = (message.key() or b"").decode()
            records.setdefault(key, []).append((message.partition(), message.offset(), message.value() or b""))
        if len(done) < len(partitions):
            raise RuntimeError("oracle did not reach the end of every partition")
        return records
    finally:
        consumer.close()


def _signature(envelope) -> tuple:
    return (tuple(sorted(envelope.quality_flags)), envelope.authority_revision, envelope.source_id,
            envelope.source_role, envelope.provider, envelope.source_session_id,
            envelope.connection_generation, envelope.lease_epoch)


def _policy(feed: str, envelope) -> str:
    """`qdl_contracts::delivery`: the domain policy (LOSSLESS is fail-safe)."""
    if feed in {"TRADE", "BOOK_SNAPSHOT", "BOOK_DELTA"}:
        return "LOSSLESS"
    if feed == "BAR":
        return "LIFECYCLE_COALESCE" if envelope.bar.lifecycle == 1 else "LOSSLESS"
    return "LATEST_STATE"


def expected_delivery(row: dict[str, Any], records: list[tuple[int, int, bytes]], after: int,
                      started_ns: int, ended_ns: int | None = None) -> list[dict[str, Any]]:
    """The oracle view of one subscription: every product record after the
    cursor with its policy, lifecycle key/signature and whether the strict
    freshness predicate (BLOCK/PAUSE with a bound) must filter it."""
    from qdl.marketdata.v2 import market_data_pb2
    from qdl.runtime.stable_catalog import canonical_payload_interval

    requirement = row["requirement"]
    bound = requirement.max_freshness_ms
    strict = bound is not None and requirement.effective_event_recency_policy.value in {"BLOCK", "PAUSE"}
    out = []
    for partition, offset, payload in records:
        if offset <= after or not payload:
            continue
        try:
            envelope = market_data_pb2.EventEnvelope.FromString(payload)
        except Exception:  # noqa: BLE001 - aborted copies are never committed; defensive
            continue
        if (envelope.WhichOneof("payload") != row["feed"].lower()
                or canonical_payload_interval(envelope) != row["interval"]):
            continue
        observed = envelope.bar.close_time_ns if row["feed"] == "BAR" else envelope.source_event_time_ns
        # The gateway checks age when it delivers, anywhere in the run: a
        # record already too old at the start must be filtered, one still
        # fresh at the end of the run must be delivered, anything that aged
        # out during the run may go either way. Capture keeps original times.
        age_start_ms = (started_ns - int(observed)) / 1e6
        age_end_ms = ((ended_ns or started_ns) - int(observed)) / 1e6
        filtered = "must" if strict and age_start_ms > bound + 5_000 else (
            "no" if not strict or age_end_ms < bound - 5_000 else "either")
        out.append({"offset": offset, "partition": partition, "policy": _policy(row["feed"], envelope),
                    "key": envelope.bar.open_time_ns if row["feed"] == "BAR" else None,
                    "signature": _signature(envelope), "filtered": filtered})
    return out


def judge_subscription(expected: list[dict[str, Any]], delivered: list[int]) -> dict[str, int]:
    """Exactness of one subscription against its oracle view."""
    index = {item["offset"]: item for item in expected}
    seen = set()
    counts = {"duplicates": 0, "out_of_order": 0, "unexpected": 0, "missing_lossless": 0,
              "unsuperseded_drops": 0, "delivered_filtered": 0}
    for position, offset in enumerate(delivered):
        if offset in seen:
            counts["duplicates"] += 1
        seen.add(offset)
        if position and offset <= delivered[position - 1]:
            counts["out_of_order"] += 1
        item = index.get(offset)
        if item is None:
            counts["unexpected"] += 1
        elif item["filtered"] == "must":
            counts["delivered_filtered"] += 1
    delivered_items = [index[offset] for offset in delivered if offset in index]
    for item in expected:
        if item["filtered"] != "no" or item["offset"] in seen:
            continue
        if item["policy"] == "LOSSLESS":
            counts["missing_lossless"] += 1
        elif not any(other["offset"] > item["offset"] and other["key"] == item["key"]
                     and other["signature"] == item["signature"] for other in delivered_items):
            counts["unsuperseded_drops"] += 1
    return counts


def matrix_verdict(result: dict[str, Any], *, expected_ids: Sequence[str]) -> list[str]:
    """Every reason the matrix did not pass; empty means PASS. Exact coverage
    of the demanded subscriptions, per-subscription exactness, typed errors
    only, at least one failover with every failed-over stream resumed, the
    exact negative matrix, Replay pages and the typed read view."""
    failures: list[str] = []
    subscriptions = result.get("subscriptions")
    if not isinstance(subscriptions, list):
        subscriptions = []
        failures.append("subscriptions: missing or not a list")
    ids = [item.get("id") if isinstance(item, dict) else None for item in subscriptions]
    if _duplicates(ids):
        failures.append(f"subscriptions: duplicated {_duplicates(ids)[:5]}")
    missing = sorted(set(expected_ids) - set(ids))
    unexpected = sorted(set(ids) - set(expected_ids), key=str)
    if missing:
        failures.append(f"subscriptions: {len(missing)} missing, e.g. {missing[:3]}")
    if unexpected:
        failures.append(f"subscriptions: unexpected {unexpected[:3]}")
    if not expected_ids:
        failures.append("subscriptions: no expected identities")
    failovers = 0
    for item in subscriptions:
        if not isinstance(item, dict):
            continue
        name = item.get("id", "?")
        for field in ("duplicates", "out_of_order", "unexpected", "missing_lossless",
                      "unsuperseded_drops", "delivered_filtered", "token_errors", "cross_mix"):
            value = item.get(field)
            if not isinstance(value, int) or isinstance(value, bool) or value != 0:
                failures.append(f"{name}: {field}={value}")
        if item.get("reached_live") is not True:
            failures.append(f"{name}: never reached LIVE")
        for error in item.get("errors") or []:
            if error.get("code") not in MATRIX_ALLOWED_ERRORS:
                failures.append(f"{name}: error {error.get('code')}: {str(error.get('detail'))[:80]}")
        for failover in item.get("failovers") or []:
            failovers += 1
            if failover.get("resumed") is not True:
                failures.append(f"{name}: failover at {failover.get('at_ms')} ms did not resume")
    if int(result.get("failover_expected", 1)) and failovers == 0:
        failures.append("failover: no subscription failed over (replica A was not killed?)")
    if not int(result.get("checks", 1)):
        return failures
    negatives = result.get("negatives") or []
    wanted = dict(NEGATIVE_CASES)
    got = {case.get("case"): case for case in negatives if isinstance(case, dict)}
    if set(got) != set(wanted) or len(negatives) != len(wanted):
        failures.append(f"negatives: {len(negatives)} cases, expected exactly {len(wanted)}")
    for name, status in wanted.items():
        case = got.get(name, {})
        if case.get("expected") != status or case.get("observed") != status:
            failures.append(f"negative {name}: expected {status}, observed {case.get('observed')}")
    rpcs = result.get("rpcs") or []
    if not rpcs:
        failures.append("rpcs: Replay/GetSnapshot/GetFeedStatus not checked")
    for check in rpcs:
        if check.get("pass") is not True:
            failures.append(f"rpc {check.get('rpc')} {check.get('consumer_id')}: {check.get('detail')}")
    return failures


def commit_to_client_ms(received: list[tuple[int, int, int, int | None]],
                        commits: dict[tuple[int, int], int]) -> tuple[list[float], list[float]]:
    """Commit -> client latency by Kafka coordinate, from deliveries
    `(partition, offset, received_ns, live_since_ns)`. Returns `(live,
    catchup)`: records committed at or after the stream reached LIVE (the
    steady live path), and records committed before LIVE but delivered after
    it (queued while the replay ran: the catch-up, not the live path)."""
    live: list[float] = []
    catchup: list[float] = []
    for partition, offset, received_ns, live_since_ns in received:
        committed_ns = commits.get((partition, offset))
        if committed_ns is None or live_since_ns is None or received_ns < live_since_ns:
            continue
        (live if committed_ns >= live_since_ns else catchup).append((received_ns - committed_ns) / 1e6)
    return live, catchup


def _stream_id(row: dict[str, Any]) -> str:
    return f"{row['consumer_id']}|{row['physical_key']}|{row['feed']}|{row['interval'] or '-'}"


async def _matrix_stream(runner: Slice, row: dict[str, Any], records: list, until_ns: int,
                         started_ns: int, replay_back: int) -> dict[str, Any]:
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement
    from qdl.replay.cursor_v3 import CursorV3Expectation, requirement_digest
    from qdl.runtime.stable_catalog import canonical_payload_interval
    from qdl_sdk.errors import DataLayerError
    from qdl_sdk.models import ControlEvent, StreamEvent

    consumer_id = row["consumer_id"]
    requirement = row["requirement"]
    product = expected_delivery(row, records, -1, started_ns)
    offsets = [item["offset"] for item in product]
    if len(offsets) > replay_back:
        after = offsets[-replay_back - 1]
    elif offsets:
        after = max(0, offsets[0] - 1)
    else:
        after = max((offset for _, offset, _ in records), default=0)
    partition = records[0][0] if records else 0
    token = runner.codec.encode(runner.claims(consumer_id, requirement, after, partition=partition))
    expectation = CursorV3Expectation(
        environment=runner.bundle["environment"], stream=runner.bundle["catalog"]["canonical_stream"],
        source_topic_id=runner.args.topic_id, partition_plan_epoch=1,
        source_policy_revision=runner.bundle["catalog"]["source_policy_revision"],
        catalog_revision=runner.bundle["catalog"]["catalog_revision"],
        route_generation=runner.args.route_generation)
    digest = requirement_digest(requirement)
    sdk = sdk_requirement(types.SimpleNamespace(requirement=requirement))
    transport = runner.shared_transport(consumer_id)
    delivered: list[int] = []
    received: list[tuple[int, int, int, int | None]] = []
    controls: list[str] = []
    errors: list[dict[str, Any]] = []
    failovers: list[dict[str, Any]] = []
    token_errors = cross_mix = 0
    reached_live = False
    live_since_ns: int | None = None
    reconnect_from = None
    # Events per connection (segment 0 on the first replica, then one per
    # resume): reconciled against each replica's per-subscription report.
    segments: list[int] = []
    # The transport is the consumer's, shared by its streams (closed when
    # the matrix run ends).
    while time.time_ns() < until_ns:
        segments.append(0)
        stream = transport.subscribe(sdk, consumer_id=consumer_id, cursor_token=token,
                                     max_buffer_events=row["buffer"]).__aiter__()
        try:
            while True:
                remaining = (until_ns - time.time_ns()) / 1e9
                if remaining <= 0:
                    break
                try:
                    item = await asyncio.wait_for(stream.__anext__(), timeout=remaining)
                except (asyncio.TimeoutError, StopAsyncIteration):
                    break
                if reconnect_from is not None:
                    failovers[-1]["resumed"] = True
                    failovers[-1]["rto_ms"] = round((time.time_ns() - reconnect_from) / 1e6, 1)
                    reconnect_from = None
                if isinstance(item, ControlEvent):
                    controls.append(item.code)
                    reached_live = reached_live or item.code == "LIVE"
                    # Per connection: a resume replays again first.
                    live_since_ns = time.time_ns() if item.code == "LIVE" else (
                        None if item.code == "REPLAYING" else live_since_ns)
                    continue
                if not isinstance(item, StreamEvent):
                    continue
                envelope = item.event
                if (envelope.instrument_uid != requirement.instrument_uid
                        or envelope.WhichOneof("payload") != row["feed"].lower()
                        or canonical_payload_interval(envelope) != row["interval"]):
                    cross_mix += 1
                try:
                    claims = runner.codec.verify(item.resume_token, consumer_id=consumer_id,
                                                 environment=runner.bundle["environment"],
                                                 requirement_digest_value=digest, expected=expectation,
                                                 now_ns=time.time_ns())
                    if claims.source_offset != item.logical_offset:
                        token_errors += 1
                except Exception:  # noqa: BLE001
                    token_errors += 1
                delivered.append(item.logical_offset)
                segments[-1] += 1
                received.append((partition, item.logical_offset, time.time_ns(), live_since_ns))
                token = item.resume_token
            break
        except DataLayerError as error:
            errors.append({"code": error.code, "detail": str(error)[:200],
                           "at_ms": round((time.time_ns() - started_ns) / 1e6, 1)})
            if error.code in MATRIX_ALLOWED_ERRORS and error.retryable:
                failovers.append({"at_ms": errors[-1]["at_ms"], "resumed": False})
                reconnect_from = time.time_ns()
                await asyncio.sleep(0.2)
                continue
            break
        finally:
            with contextlib.suppress(Exception):
                await stream.aclose()
    filtered_all = bool(product) and all(item["filtered"] == "must" for item in product)
    # Judged after the run against the final oracle (live records included).
    return {"id": _stream_id(row), "consumer_id": consumer_id, "feed": row["feed"], "after": after,
            "delivered": len(delivered), "age_filtered_capture": filtered_all, "controls": controls[:6],
            "reached_live": reached_live, "errors": errors, "failovers": failovers,
            "token_errors": token_errors, "cross_mix": cross_mix, "segments": segments,
            "_delivered": delivered,
            "_received": received}


async def _rpc_checks(runner: Slice, rows: list[dict[str, Any]], oracle: dict, target: str) -> list[dict]:
    """Replay pages the cursor's product; snapshot/status are typed not-ready."""
    import grpc
    from qdl.certification.phase103_consumer_acceptance import sdk_requirement
    from qdl.query.v2 import query_pb2

    checks = []
    for consumer_id in sorted({row["consumer_id"] for row in rows}):
        consumer_rows = [row for row in rows if row["consumer_id"] == consumer_id]
        channel = grpc.aio.secure_channel(target, runner.tls(consumer_id).grpc_credentials())
        metadata = (("authorization", f"Bearer {runner.jwt(consumer_id)}"),
                    ("x-qdl-consumer-id", consumer_id),
                    ("x-qdl-purpose", sorted(p.value for p in runner.manifests[consumer_id].allowed_purposes)[0]))
        try:
            row = next((item for item in consumer_rows if len([
                o for o in expected_delivery(item, oracle.get(item["physical_key"], []), -1, time.time_ns())]) > 12),
                None)
            if row is not None:
                product = [item["offset"] for item in expected_delivery(
                    row, oracle[row["physical_key"]], -1, time.time_ns())]
                after = product[0]
                partition = oracle[row["physical_key"]][0][0]
                token = runner.codec.encode(runner.claims(consumer_id, row["requirement"], after,
                                                          partition=partition))
                call = channel.unary_stream("/qdl.query.v2.MarketDataStreamService/Replay",
                                            request_serializer=query_pb2.ReplayRequest.SerializeToString,
                                            response_deserializer=query_pb2.ReplayResponse.FromString)
                got = []
                async for response in call(query_pb2.ReplayRequest(consumer_id=consumer_id, cursor_token=token,
                                                                   limit=10), metadata=metadata, timeout=30):
                    got.append(response.record.logical_offset)
                checks.append({"rpc": "Replay", "consumer_id": consumer_id, "pass": got == product[1:11],
                               "detail": f"{len(got)} records, exact={got == product[1:11]}"})
            requirement = sdk_requirement(types.SimpleNamespace(requirement=consumer_rows[0]["requirement"])).to_proto()
            for rpc, request_type, response_type, body in (
                    ("GetSnapshot", query_pb2.GetSnapshotRequest, query_pb2.GetSnapshotResponse,
                     query_pb2.GetSnapshotRequest(consumer_id=consumer_id, requirement=requirement)),
                    ("GetFeedStatus", query_pb2.GetFeedStatusRequest, query_pb2.GetFeedStatusResponse,
                     query_pb2.GetFeedStatusRequest(consumer_id=consumer_id, requirement=requirement))):
                call = channel.unary_unary(f"/qdl.query.v2.MarketDataStreamService/{rpc}",
                                           request_serializer=request_type.SerializeToString,
                                           response_deserializer=response_type.FromString)
                try:
                    await call(body, metadata=metadata, timeout=15)
                    checks.append({"rpc": rpc, "consumer_id": consumer_id, "pass": False,
                                   "detail": "answered data before the KN-4 read view exists"})
                except grpc.aio.AioRpcError as error:
                    typed = (error.code() is grpc.StatusCode.FAILED_PRECONDITION
                             and (error.details() or "").startswith("DATA_NOT_READY:"))
                    permission = error.code() is grpc.StatusCode.PERMISSION_DENIED
                    checks.append({"rpc": rpc, "consumer_id": consumer_id, "pass": typed or permission,
                                   "detail": f"{error.code().name}:{(error.details() or '')[:60]}"})
        finally:
            await channel.close()
    return checks


def selected_streams(consumers: Sequence[str] | None) -> list[dict[str, Any]]:
    """The demanded streams, optionally only those of some consumers (one
    client process per consumer, as in the target profile)."""
    rows = demanded_streams()
    if consumers:
        wanted = set(consumers)
        unknown = wanted - {row["consumer_id"] for row in rows}
        if unknown:
            raise ValueError(f"no demanded streams for {sorted(unknown)}")
        rows = [row for row in rows if row["consumer_id"] in wanted]
    return rows


async def matrix_async(args: argparse.Namespace) -> dict[str, Any]:
    rows = selected_streams(args.consumers)
    oracle = kafka_oracle(args.bootstrap, args.topic)
    # Negative matrix products: the alpha TRADE streams with records.
    everything = demanded_streams()
    probes = []
    for consumer_id in ("alpha.okx.paper.stable", "alpha.binance.paper.stable"):
        row = next((item for item in everything if item["consumer_id"] == consumer_id and item["feed"] == "TRADE"
                    and oracle.get(item["physical_key"])), None)
        if row is None:
            raise RuntimeError(f"no committed TRADE record for {consumer_id}: the capture/load is incomplete")
        partition, offset, _ = oracle[row["physical_key"]][-1]
        probes.append({"consumer_id": consumer_id, "instrument_uid": row["requirement"].instrument_uid,
                       "feed": "TRADE", "physical_key": row["physical_key"], "partition": partition,
                       "offset": offset})
    probes_path = Path(args.out).with_suffix(".probes.json")
    probes_path.write_text(json.dumps(probes), encoding="utf-8")
    args.probes = str(probes_path)
    args.target = args.targets[0]
    runner = Slice(args)
    started_ns = time.time_ns()
    by_consumer: dict[str, list] = {}
    for row in rows:
        by_consumer.setdefault(row["consumer_id"], []).append(row)
    waves: list[list] = []
    for consumer_rows in by_consumer.values():
        size = max(1, consumer_rows[0]["max_streams"] - 4)
        for wave, start in enumerate(range(0, len(consumer_rows), size)):
            while len(waves) <= wave:
                waves.append([])
            waves[wave].extend(consumer_rows[start:start + size])
    results = []
    for wave, members in enumerate(waves):
        seconds = float(args.window_seconds) if wave == 0 else float(args.tail_seconds)
        until = time.time_ns() + int(seconds * 1e9)
        results += await asyncio.gather(*(
            _matrix_stream(runner, row, oracle.get(row["physical_key"], []), until, started_ns,
                           int(args.replay_back)) for row in members))
    # After the waves: the committed log grew during the live phase; judge
    # every subscription against the final oracle.
    ended_ns = time.time_ns()
    final = kafka_oracle(args.bootstrap, args.topic)
    commits: dict[tuple[int, int], int] = {}
    if args.commit_log and Path(args.commit_log).exists():
        for line in Path(args.commit_log).read_text(encoding="utf-8").splitlines():
            entry = json.loads(line)
            if "offset" in entry:
                commits[(int(entry["partition"]), int(entry["offset"]))] = int(entry["commit_ns"])
    latency_by_feed: dict[str, list[float]] = {}
    catchup_by_feed: dict[str, list[float]] = {}
    for row, item in zip([row for members in waves for row in members], results, strict=True):
        live, catchup = commit_to_client_ms(item.pop("_received"), commits)
        latency_by_feed.setdefault(row["feed"], []).extend(live)
        catchup_by_feed.setdefault(row["feed"], []).extend(catchup)
        delivered = item.pop("_delivered")
        expected = expected_delivery(row, final.get(row["physical_key"], []), item["after"], started_ns,
                                     ended_ns)
        item["expected_deliverable"] = sum(1 for entry in expected if entry["filtered"] == "no")
        item.update(judge_subscription(expected, delivered))
    for transport in runner.__dict__.get("_transports", {}).values():
        with contextlib.suppress(Exception):
            await transport.close()
    runner.args.target = args.targets[-1]
    negatives = await runner.negatives() if args.checks else []
    # Every consumer's RPCs, also from a one-consumer client process.
    rpcs = await _rpc_checks(runner, everything, final, args.targets[-1]) if args.checks else []
    latency = {feed: _dist(values) for feed, values in sorted(latency_by_feed.items())}
    latency["all"] = _dist([value for values in latency_by_feed.values() for value in values])
    catchup = {feed: _dist(values) for feed, values in sorted(catchup_by_feed.items())}
    catchup["all"] = _dist([value for values in catchup_by_feed.values() for value in values])
    return {"schema": "qdl.kn.v220.native-matrix.v1", "targets": args.targets, "source_mode": "capture",
            "commit_to_client_after_live_ms": {"not_live": "capture replay; isolated broker commit times",
                                               **latency},
            "catchup_commit_to_client_ms": catchup,
            "waves": [len(members) for members in waves], "subscriptions": results,
            "consumers": sorted({row["consumer_id"] for row in rows}), "checks": int(args.checks),
            "negatives": negatives, "rpcs": rpcs, "failover_expected": int(args.failover_expected),
            "oracle_keys": len(final), "oracle_records": sum(len(value) for value in final.values())}


async def main_async(args: argparse.Namespace) -> dict[str, Any]:
    runner = Slice(args)
    positives = await asyncio.gather(*(runner.run_product(probe) for probe in runner.probes))
    negatives = await runner.negatives()
    commits: dict[str, int] = {}
    if args.commit_log and Path(args.commit_log).exists():
        for line in Path(args.commit_log).read_text(encoding="utf-8").splitlines():
            row = json.loads(line)
            commits[row["event_id"]] = int(row["commit_ns"])
    for product in positives:
        received = product.pop("_received_by_event")
        live_since = product.pop("_live_since_ns")
        matched = [(received[event] - commits[event]) / 1e6 for event in received if event in commits]
        product["commit_to_client_ms"] = _dist(matched)
        # Records committed after this client reached LIVE: the replay backlog
        # it was still draining is excluded, the steady live path remains.
        after_live = [(received[event] - commits[event]) / 1e6 for event in received
                      if event in commits and live_since is not None and commits[event] >= live_since]
        product["commit_to_client_after_live_ms"] = _dist(after_live)
        if args.source_mode == "capture":
            # Captured records keep their original source times; age, delivery
            # lag and end-to-end describe the capture, not live freshness.
            for key in ("durable_event_age_ms", "delivery_lag_ms", "end_to_end_cache_ms",
                        "canonical_publish_to_client_ms"):
                product[key] = {"not_live": "capture replay; original source/publish times", **product[key]}
    return {"schema": "qdl.kn.v220.native-slice.v1", "target": args.target, "seconds": float(args.seconds),
            "source_mode": args.source_mode,
            "bundle_sha256": runner.bundle["sha256"], "route_generation": args.route_generation,
            "products": positives, "negatives": negatives,
            "negatives_pass": sum(1 for r in negatives if r["pass"]), "negatives_total": len(negatives)}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    cap = sub.add_parser("capture")
    cap.add_argument("--spool", default="/state/shared/canonical-cache.sqlite3")
    cap.add_argument("--keys", nargs="*", help="physical_key=limit")
    cap.add_argument("--demand", type=int, help="also every demanded stream key, this many records each")
    cap.add_argument("--out", required=True)
    lod = sub.add_parser("load")
    lod.add_argument("--capture", required=True)
    lod.add_argument("--bootstrap", required=True)
    lod.add_argument("--topic", default="md.canonical.v2")
    lod.add_argument("--phase", choices=("history", "live"), required=True)
    lod.add_argument("--history-fraction", default="0.6")
    lod.add_argument("--live-seconds", default="70")
    lod.add_argument("--commit-log", default="/dev/null")
    lod.add_argument("--rate", default="0", help="live phase at this many records/s (capacity challenge)")
    lod.add_argument("--abort-every", default="0", help="abort a copy of every Nth live batch")
    lod.add_argument("--repeat", default="1", help="with --rate: cycle the capture this many times at most")
    lod.add_argument("--split", choices=("time", "key"), default="time",
                     help="history/live split: by global time (KN-1) or per key (matrix)")
    run = sub.add_parser("run")
    run.add_argument("--target", required=True)
    run.add_argument("--profile", required=True)
    run.add_argument("--bundle", required=True)
    run.add_argument("--cursor-keys", required=True)
    run.add_argument("--probes", required=True)
    run.add_argument("--topic-id", required=True)
    run.add_argument("--route-generation", required=True)
    run.add_argument("--quota-redis-url", required=True)
    run.add_argument("--quota-prefix", required=True)
    run.add_argument("--source-mode", choices=("capture", "live"), required=True)
    run.add_argument("--commit-log")
    run.add_argument("--seconds", type=float, default=60.0)
    run.add_argument("--replay-back", type=int, default=2000)
    run.add_argument("--buffer", type=int, default=2000)
    run.add_argument("--out", required=True)
    mat = sub.add_parser("matrix")
    mat.add_argument("--targets", nargs=2, required=True, help="replica A (killed during the run), replica B")
    mat.add_argument("--bootstrap", required=True)
    mat.add_argument("--topic", default="md.canonical.v2")
    mat.add_argument("--profile", required=True)
    mat.add_argument("--bundle", required=True)
    mat.add_argument("--cursor-keys", required=True)
    mat.add_argument("--topic-id", required=True)
    mat.add_argument("--route-generation", required=True)
    mat.add_argument("--quota-redis-url", required=True)
    mat.add_argument("--quota-prefix", required=True)
    mat.add_argument("--window-seconds", type=float, default=90.0)
    mat.add_argument("--tail-seconds", type=float, default=20.0)
    mat.add_argument("--replay-back", type=int, default=25)
    mat.add_argument("--failover-expected", type=int, default=1)
    mat.add_argument("--commit-log", help="loader commit log for commit->client latency")
    mat.add_argument("--consumers", nargs="*", help="only these consumers' streams (one process each)")
    mat.add_argument("--checks", type=int, default=1, help="run the negative matrix and RPC checks")
    mat.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    if args.command == "matrix":
        result = asyncio.run(matrix_async(args))
        failures = matrix_verdict(result, expected_ids=[_stream_id(row) for row in selected_streams(args.consumers)])
        result["verdict"] = {"pass": not failures, "failures": failures}
        result["sha256"] = hashlib.sha256(json.dumps(result, sort_keys=True, default=str).encode()).hexdigest()
        Path(args.out).write_text(json.dumps(result, indent=1, sort_keys=True, default=str) + "\n",
                                  encoding="utf-8")
        print(json.dumps({"out": args.out, "sha256": result["sha256"], "pass": not failures,
                          "failures": failures[:25], "subscriptions": len(result["subscriptions"])}),
              file=sys.stderr)
        return 0 if not failures else 1
    if args.command == "capture":
        print(json.dumps(capture(args)), file=sys.stderr)
        return 0
    if args.command == "load":
        print(json.dumps(load(args)), file=sys.stderr)
        return 0
    result = asyncio.run(main_async(args))
    probes = json.loads(Path(args.probes).read_text(encoding="utf-8"))
    failures = slice_verdict(result, expected_products=[probe["physical_key"] for probe in probes])
    result["verdict"] = {"pass": not failures, "failures": failures}
    result["sha256"] = hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()
    Path(args.out).write_text(json.dumps(result, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"out": args.out, "sha256": result["sha256"], "pass": not failures,
                      "failures": failures[:20],
                      "negatives": f"{result['negatives_pass']}/{result['negatives_total']}"}), file=sys.stderr)
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
