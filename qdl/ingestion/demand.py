from __future__ import annotations

import time
from dataclasses import dataclass

from qdl.ingestion.contracts import ConnectionShard, Subscription, plan_shards


@dataclass(frozen=True)
class DemandLease:
    owner_id: str
    subscription: Subscription
    expires_at_ns: int


class DesiredSubscriptionRegistry:
    """Combines declared baseline subscriptions with expiring runtime demand."""

    def __init__(self, baseline: set[Subscription] | None = None, *, clock_ns=time.time_ns):
        self._baseline = set(baseline or set())
        self._leases: dict[tuple[str, str], DemandLease] = {}
        self._clock_ns = clock_ns

    def renew(
        self, owner_id: str, subscriptions: set[Subscription], *, ttl_seconds: int
    ) -> None:
        if not owner_id.strip():
            raise ValueError("owner_id is required")
        if ttl_seconds < 5 or ttl_seconds > 3600:
            raise ValueError("demand TTL must be between 5 and 3600 seconds")
        expires = self._clock_ns() + ttl_seconds * 1_000_000_000
        requested = {item.key for item in subscriptions}
        for key in [key for key in self._leases if key[0] == owner_id and key[1] not in requested]:
            del self._leases[key]
        for item in subscriptions:
            self._leases[(owner_id, item.key)] = DemandLease(owner_id, item, expires)

    def release(self, owner_id: str) -> int:
        keys = [key for key in self._leases if key[0] == owner_id]
        for key in keys:
            del self._leases[key]
        return len(keys)

    def desired(self, *, now_ns: int | None = None) -> set[Subscription]:
        now = self._clock_ns() if now_ns is None else now_ns
        expired = [key for key, lease in self._leases.items() if lease.expires_at_ns <= now]
        for key in expired:
            del self._leases[key]
        return self._baseline | {lease.subscription for lease in self._leases.values()}

    def shards(self, *, max_streams: int, config_revision: int) -> tuple[ConnectionShard, ...]:
        return plan_shards(
            self.desired(), max_streams=max_streams, config_revision=config_revision
        )

