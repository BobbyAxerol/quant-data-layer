"""Bounded V1 fallback evidence for the Phase 10.5 no-order acceptance.

The stable route manifest is the authority for whether a V1 read is allowed.
This module only maps those already-approved products to their frozen V1
cached-read contract and validates a response without retaining its payload.
It never changes a deployed consumer route or opens a provider connection.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import time
from collections.abc import Iterable, Mapping

from qdl.certification.phase103_consumer_acceptance import AcceptanceProduct
from qdl.certification.phase105_handoff import (
    V1_FALLBACK_COMMIT,
    V1_FALLBACK_VERSION,
    validate_frozen_v1_provenance,
    validate_frozen_v1_runtime_binding,
)
from qdl.consumer import StableReleaseRoutePlan, requirement_key
from qdl.runtime.stable_catalog import StableSourceCatalog


PHASE105_PAPER_CONSUMER_ORDER = (
    "monitoring.multivenue.stable",
    "trading-system.paper.stable",
    "alpha.binance.paper.stable",
    "alpha.okx.paper.stable",
)


def _selected_consumer_order(
    consumer_ids: Iterable[str] | None = None,
) -> tuple[str, ...]:
    selected = (
        frozenset(PHASE105_PAPER_CONSUMER_ORDER)
        if consumer_ids is None
        else frozenset(str(item) for item in consumer_ids)
    )
    known = frozenset(PHASE105_PAPER_CONSUMER_ORDER)
    if not selected or not selected <= known:
        raise ValueError("Phase 10.5 fallback consumer scope is invalid")
    return tuple(
        consumer_id
        for consumer_id in PHASE105_PAPER_CONSUMER_ORDER
        if consumer_id in selected
    )
@dataclass(frozen=True, slots=True)
class V1FallbackProbe:
    """One manifest-approved V1 cached read, without any route mutation."""

    consumer_id: str
    requirement_key: str
    native_symbol: str
    feed: str
    interval: str | None
    policy: str
    path: str
    params: tuple[tuple[str, str], ...]
    max_freshness_ms: int | None

    @property
    def identity(self) -> tuple[str, str]:
        return self.consumer_id, self.requirement_key


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _product_routes(release: StableReleaseRoutePlan) -> dict[tuple[str, str], object]:
    return {
        (consumer.consumer_id, product.requirement_key): product
        for consumer in release.consumers
        for product in consumer.products
    }


def build_v1_fallback_probes(
    release: StableReleaseRoutePlan,
    *,
    catalog: StableSourceCatalog,
    products: Iterable[AcceptanceProduct],
    consumer_ids: Iterable[str] | None = None,
) -> tuple[V1FallbackProbe, ...]:
    """Materialize exactly the frozen V1-allowed subset of C2 products."""
    consumer_order = _selected_consumer_order(consumer_ids)
    by_identity = {(item.consumer_id, requirement_key(item.requirement)): item for item in products}
    routes = _product_routes(release)
    expected = {
        identity
        for identity, route in routes.items()
        if identity[0] in consumer_order
        and getattr(route, "route") == "V2_PRIMARY"
        and getattr(route, "fallback") == "V1"
    }
    if not expected or not expected <= set(by_identity):
        raise ValueError("Phase 10.5 V1 fallback products differ from the V2 acceptance scope")

    probes: list[V1FallbackProbe] = []
    for consumer_id in consumer_order:
        selected = sorted(
            (by_identity[identity] for identity in expected if identity[0] == consumer_id),
            key=lambda item: (item.native_symbol, item.feed.value, item.interval or ""),
        )
        for product in selected:
            binding = catalog.binding_for(product.requirement)
            identity = binding.instrument.identity
            if identity.venue != "BINANCE" or identity.market != "USDM":
                raise ValueError("Phase 10.5 V1 fallback is only certified for Binance USD-M")
            policy = binding.v1_compatibility
            if policy in {"BINANCE_TRADE_MARKET_AND_GENERIC", "BINANCE_TRADE_MARKET_ONLY"}:
                if product.feed.value != "TRADE" or product.interval is not None:
                    raise ValueError("Phase 10.5 V1 trade fallback contract differs from requirement")
                path = f"/v1/binance/price/{product.native_symbol}"
                params = (("market", "usdm"),)
            elif policy == "BINANCE_BAR_GENERIC":
                if product.feed.value != "BAR" or not product.interval:
                    raise ValueError("Phase 10.5 V1 bar fallback contract differs from requirement")
                path = f"/v1/binance/kline/{product.native_symbol}"
                params = (("interval", product.interval),)
            else:
                raise ValueError("Phase 10.5 route permits an unsupported V1 compatibility policy")
            probes.append(V1FallbackProbe(
                consumer_id=product.consumer_id,
                requirement_key=requirement_key(product.requirement),
                native_symbol=product.native_symbol,
                feed=product.feed.value,
                interval=product.interval,
                policy=policy,
                path=path,
                params=params,
                max_freshness_ms=product.requirement.max_freshness_ms,
            ))
    if {item.identity for item in probes} != expected:
        raise ValueError("Phase 10.5 V1 fallback probe scope is incomplete or duplicated")
    return tuple(probes)


def blocked_fallback_identities(
    release: StableReleaseRoutePlan,
    *,
    consumer_ids: Iterable[str] | None = None,
) -> tuple[tuple[str, str], ...]:
    """Return V2 products that must never choose V1 in the C2 probe."""
    consumer_order = _selected_consumer_order(consumer_ids)
    return tuple(sorted(
        (consumer_id, requirement_key)
        for (consumer_id, requirement_key), route in _product_routes(release).items()
        if consumer_id in consumer_order
        and getattr(route, "route") == "V2_PRIMARY"
        and getattr(route, "fallback") == "BLOCKED"
    ))


def validate_v1_provenance(release: StableReleaseRoutePlan, raw: object) -> dict[str, object]:
    """Require the exact frozen V1 fallback identity before an actual V1 read."""
    frozen = validate_frozen_v1_provenance(raw)
    if (
        frozen["source_commit"] != release.v1_fallback.source_commit
        or frozen["source_commit"] != V1_FALLBACK_COMMIT
        or not isinstance(raw, dict)
        or raw.get("version") != release.v1_fallback.release_tag
        or raw.get("version") != V1_FALLBACK_VERSION
    ):
        raise ValueError("Phase 10.5 V1 fallback provenance is not frozen and attestable")
    return frozen


def validate_v1_runtime_binding(
    provenance: Mapping[str, object], raw: object
) -> dict[str, object]:
    """Require the fallback receipt to name the V1 container serving this probe.

    The disposable client has no Docker socket. A host-side, payload-free
    binding is therefore produced immediately before the probe and proves the
    current container image is the same immutable digest covered by the frozen
    V1 source attestation.
    """
    return validate_frozen_v1_runtime_binding(provenance, raw)


def _positive_decimal(value: object, field: str, *, allow_zero: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"Phase 10.5 V1 {field} is not numeric")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"Phase 10.5 V1 {field} is not numeric") from error
    if not result.is_finite() or result < 0 or (not allow_zero and result == 0):
        raise ValueError(f"Phase 10.5 V1 {field} is not a valid positive decimal")
    return result


def _millis(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"Phase 10.5 V1 {field} is not a millisecond timestamp")
    try:
        result = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"Phase 10.5 V1 {field} is not a millisecond timestamp") from error
    if result <= 0:
        raise ValueError(f"Phase 10.5 V1 {field} must be positive")
    return result


def _source_age_ms(event_ms: int, *, maximum: int | None, now_ms: int) -> int:
    age_ms = now_ms - event_ms
    if age_ms < 0:
        raise ValueError("Phase 10.5 V1 source event is in the future")
    if maximum is not None and age_ms > maximum:
        raise ValueError("Phase 10.5 V1 fallback response is stale")
    return age_ms


def validate_v1_fallback_payload(
    probe: V1FallbackProbe,
    payload: object,
    *,
    now_ms: int | None = None,
) -> dict[str, object]:
    """Validate V1 generic cached response units, identity, finality and freshness.

    The returned record is deliberately payload-free: only the endpoint kind,
    a digest and bounded timing are retained in Phase 10.5 evidence.
    """
    if not isinstance(payload, dict):
        raise ValueError("Phase 10.5 V1 fallback response is not an object")
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if len(encoded) > 1_048_576:
        raise ValueError("Phase 10.5 V1 fallback response exceeds the bounded contract")
    received_ms = int(time.time() * 1000) if now_ms is None else now_ms
    if probe.policy.startswith("BINANCE_TRADE"):
        if str(payload.get("symbol", "")).upper() != probe.native_symbol:
            raise ValueError("Phase 10.5 V1 trade symbol differs from manifest")
        if str(payload.get("market", "")).lower() != "binance_usdm":
            raise ValueError("Phase 10.5 V1 trade market differs from Binance USD-M")
        _positive_decimal(payload.get("price"), "trade.price")
        _positive_decimal(payload.get("quantity"), "trade.quantity", allow_zero=True)
        event_ms = _millis(payload.get("trade_time", payload.get("event_time")), "trade_time")
        kind = "BINANCE_TRADE"
    elif probe.policy == "BINANCE_BAR_GENERIC":
        if payload.get("e") != "kline" or str(payload.get("s", "")).upper() != probe.native_symbol:
            raise ValueError("Phase 10.5 V1 bar envelope differs from manifest")
        bar = payload.get("k")
        if not isinstance(bar, dict):
            raise ValueError("Phase 10.5 V1 bar payload is unavailable")
        if (
            str(bar.get("s", "")).upper() != probe.native_symbol
            or bar.get("i") != probe.interval
            or bar.get("x") is not True
        ):
            raise ValueError("Phase 10.5 V1 bar identity, interval or finality differs")
        for field in ("o", "h", "l", "c"):
            _positive_decimal(bar.get(field), f"bar.{field}")
        _positive_decimal(bar.get("v"), "bar.v", allow_zero=True)
        open_ms = _millis(bar.get("t"), "bar.open_time")
        close_ms = _millis(bar.get("T"), "bar.close_time")
        if close_ms <= open_ms:
            raise ValueError("Phase 10.5 V1 bar close is not after open")
        event_ms = _millis(payload.get("E"), "bar.event_time")
        kind = "BINANCE_BAR"
    else:
        raise ValueError("Phase 10.5 V1 fallback policy is unsupported")
    source_age_ms = _source_age_ms(
        event_ms, maximum=probe.max_freshness_ms, now_ms=received_ms
    )
    return {
        "consumer_id": probe.consumer_id,
        "requirement_key": probe.requirement_key,
        "endpoint_kind": kind,
        "endpoint_sha256": hashlib.sha256(
            (probe.path + "?" + "&".join(f"{key}={value}" for key, value in probe.params)).encode()
        ).hexdigest(),
        "payload_sha256": hashlib.sha256(encoded).hexdigest(),
        "source_age_ms": source_age_ms,
        "response_bytes": len(encoded),
    }


def build_fallback_return_receipt(
    release: StableReleaseRoutePlan,
    probes: Iterable[V1FallbackProbe],
    *,
    consumer_ids: Iterable[str] | None = None,
) -> dict[str, object]:
    """Create the strict payload-free record consumed by Phase 10.5-D."""
    consumer_order = _selected_consumer_order(consumer_ids)
    routes = [
        {
            "consumer_id": probe.consumer_id,
            "requirement_key": probe.requirement_key,
            "before_route": "V2_PRIMARY",
            "fallback_route": "V1_FALLBACK",
            "returned_route": "V2_PRIMARY",
        }
        for probe in probes
    ]
    expected = {
        (consumer_id, requirement_key)
        for (consumer_id, requirement_key), route in _product_routes(release).items()
        if consumer_id in consumer_order
        and getattr(route, "route") == "V2_PRIMARY"
        and getattr(route, "fallback") == "V1"
    }
    actual = {(str(item["consumer_id"]), str(item["requirement_key"])) for item in routes}
    if actual != expected or len(routes) != len(actual):
        raise ValueError("Phase 10.5 fallback-return receipt differs from frozen routes")
    return {
        "schema": "qdl.phase105.v1-fallback-return.v1",
        "status": "PASS",
        "release_route_plan_sha256": release.digest,
        "routes": routes,
        "provider_connections": 0,
        "order_actions": 0,
        "cursor_directory_removed": True,
        "secret_values_recorded": False,
        "test_provenance": False,
    }
