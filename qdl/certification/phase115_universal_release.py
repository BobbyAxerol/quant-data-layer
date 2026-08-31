"""Payload-free no-order acceptance scope for a universal V2 release."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from qdl.consumer.universal_release import UniversalReleaseManifest


_ACTIONS_BY_PLANE = {
    "REALTIME": ("WARMUP", "FINAL_BAR_OR_LATEST", "SIGNED_CURSOR_REPLAY", "RECONNECT"),
    "REFERENCE": ("BATCH_HISTORY", "FRESH_SNAPSHOT"),
    "L2": ("SNAPSHOT_OR_RESYNC", "SIGNED_CURSOR_REPLAY", "RECONNECT"),
}


@dataclass(frozen=True, slots=True)
class UniversalNoOrderAcceptanceItem:
    consumer_id: str
    consumer_class: str
    requirement_id: str
    provider_plane: str
    actions: tuple[str, ...]
    fallback: str

    def __post_init__(self) -> None:
        if not self.consumer_id or not self.consumer_class or len(self.requirement_id) != 64:
            raise ValueError("universal acceptance identity is invalid")
        if self.provider_plane not in _ACTIONS_BY_PLANE or not self.actions:
            raise ValueError("universal acceptance provider plane is invalid")
        if self.fallback not in {"V1", "BLOCKED"}:
            raise ValueError("universal acceptance fallback is invalid")

    def canonical_mapping(self) -> dict[str, object]:
        return {
            "consumer_id": self.consumer_id,
            "consumer_class": self.consumer_class,
            "requirement_id": self.requirement_id,
            "provider_plane": self.provider_plane,
            "actions": list(self.actions),
            "fallback": self.fallback,
        }


@dataclass(frozen=True, slots=True)
class UniversalNoOrderAcceptanceScope:
    manifest_sha256: str
    items: tuple[UniversalNoOrderAcceptanceItem, ...]
    excluded_requirement_count: int

    def __post_init__(self) -> None:
        if len(self.manifest_sha256) != 64 or not self.items or self.excluded_requirement_count < 0:
            raise ValueError("universal acceptance scope is invalid")
        keys = {(item.consumer_id, item.requirement_id) for item in self.items}
        if len(keys) != len(self.items):
            raise ValueError("universal acceptance scope contains duplicate products")

    def report_payload(self) -> dict[str, object]:
        return {
            "schema": "qdl.phase115.no-order-acceptance-scope.v1",
            "manifest_sha256": self.manifest_sha256,
            "item_count": len(self.items),
            "excluded_requirement_count": self.excluded_requirement_count,
            "by_consumer_class": dict(sorted(Counter(item.consumer_class for item in self.items).items())),
            "by_provider_plane": dict(sorted(Counter(item.provider_plane for item in self.items).items())),
            "v1_fallback_drill_count": sum(item.fallback == "V1" for item in self.items),
            "blocked_fallback_count": sum(item.fallback == "BLOCKED" for item in self.items),
            "order_actions": 0,
            "direct_provider_connections": 0,
            "runtime_route_mutations": 0,
        }


def build_universal_no_order_acceptance_scope(
    manifest: UniversalReleaseManifest,
) -> UniversalNoOrderAcceptanceScope:
    """Build, but never execute, the exact acceptance inventory for Phase 11.5."""

    items = tuple(
        UniversalNoOrderAcceptanceItem(
            consumer_id=product.consumer_id,
            consumer_class=product.consumer_class.value,
            requirement_id=product.requirement_id,
            provider_plane=product.provider_plane,
            actions=_ACTIONS_BY_PLANE[product.provider_plane],
            fallback=product.fallback,
        )
        for product in manifest.products
    )
    return UniversalNoOrderAcceptanceScope(
        manifest_sha256=manifest.digest,
        items=items,
        excluded_requirement_count=len(manifest.exclusions),
    )
