from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from qdl.ingestion.fencing import FencingGate


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
