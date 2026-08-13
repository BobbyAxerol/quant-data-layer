from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from qdl.ingestion.fencing import FencingGate
from qdl.projection.trade import ProjectionRecord, ProjectionTarget


class Authority(str, Enum):
    LEGACY = "LEGACY"
    CANONICAL = "CANONICAL"
    SHADOW = "SHADOW"


@dataclass(frozen=True)
class AuthorityDecision:
    accepted: bool
    write_canonical: bool
    write_legacy: bool
    reason: str


class FeedAuthorityRegistry:
    """Per-feed cutover switch with epoch fencing; default is isolated shadow."""

    def __init__(self, *, default: Authority = Authority.SHADOW):
        self._default = default
        self._authorities: dict[str, Authority] = {}
        self._fencing = FencingGate()

    def set(self, feed_key: str, authority: Authority) -> None:
        if not feed_key.strip():
            raise ValueError("feed_key is required")
        self._authorities[feed_key] = authority

    def get(self, feed_key: str) -> Authority:
        return self._authorities.get(feed_key, self._default)

    def decide(self, *, feed_key: str, shard_id: str, lease_epoch: int) -> AuthorityDecision:
        if not self._fencing.permits(shard_id, lease_epoch):
            return AuthorityDecision(False, False, False, "STALE_LEASE_EPOCH")
        authority = self.get(feed_key)
        if authority is Authority.LEGACY:
            return AuthorityDecision(True, False, False, "LEGACY_REMAINS_AUTHORITATIVE")
        if authority is Authority.SHADOW:
            return AuthorityDecision(True, True, False, "SHADOW_ONLY")
        return AuthorityDecision(True, True, True, "CANONICAL_WITH_V1_PROJECTION")


class AuthorityProjectionRouter:
    """Routes one feed slice between shadow/canonical authority without process restart."""

    def __init__(
        self,
        registry: FeedAuthorityRegistry,
        *,
        shadow_target: ProjectionTarget,
        authoritative_target: ProjectionTarget,
    ) -> None:
        self._registry = registry
        self._shadow = shadow_target
        self._authoritative = authoritative_target

    def apply(self, record: ProjectionRecord) -> bool:
        decision = self._registry.decide(
            feed_key=record.feed_key,
            shard_id=record.shard_id,
            lease_epoch=record.lease_epoch,
        )
        if not decision.accepted:
            return False
        authority = self._registry.get(record.feed_key)
        if authority is Authority.SHADOW:
            return self._shadow.apply(record)
        if authority is Authority.CANONICAL:
            return self._authoritative.apply(record)
        return False
