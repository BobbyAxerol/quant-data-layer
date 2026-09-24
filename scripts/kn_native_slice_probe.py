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
Replayed capture is never reported as live freshness.

``run`` exits 0 only when ``slice_verdict`` finds no failure (KN-1 review F3);
the result file always carries the verdict and its failure list.
"""
from __future__ import annotations

import argparse
import asyncio
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
        return GrpcStreamTransport(self.args.target, tls=self.tls(consumer_id), credential_provider=credential)

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
    for spec in args.keys:
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
    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    if args.topic not in admin.list_topics(timeout=10).topics:
        for future in admin.create_topics([NewTopic(args.topic, num_partitions=6, replication_factor=1)]).values():
            future.result(20)
    producer = Producer({"bootstrap.servers": args.bootstrap, "transactional.id": f"kn1-capture-{args.phase}",
                         "enable.idempotence": True, "linger.ms": 5})
    producer.init_transactions(20)
    selected = rows[:split] if args.phase == "history" else rows[split:]
    commits = []
    if args.phase == "history":
        for start in range(0, len(selected), 500):
            producer.begin_transaction()
            for row in selected[start:start + 500]:
                producer.produce(args.topic, key=row["key"].encode(), value=base64.b64decode(row["payload"]))
            producer.commit_transaction(30)
    else:
        # Original inter-arrival gaps; batches close every 25 ms like the
        # canonical core, and each record's commit wall time is logged.
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
        with open(args.commit_log, "w", encoding="utf-8") as handle:
            for row in commits:
                handle.write(json.dumps(row) + "\n")
    return {"phase": args.phase, "published": len(selected) if args.phase == "history" else len(commits),
            "history_fraction": float(args.history_fraction)}


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
    cap.add_argument("--keys", nargs="+", required=True, help="physical_key=limit")
    cap.add_argument("--out", required=True)
    lod = sub.add_parser("load")
    lod.add_argument("--capture", required=True)
    lod.add_argument("--bootstrap", required=True)
    lod.add_argument("--topic", default="md.canonical.v2")
    lod.add_argument("--phase", choices=("history", "live"), required=True)
    lod.add_argument("--history-fraction", default="0.6")
    lod.add_argument("--live-seconds", default="70")
    lod.add_argument("--commit-log", default="/dev/null")
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
    args = parser.parse_args(argv)
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
