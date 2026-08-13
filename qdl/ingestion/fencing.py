from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(frozen=True)
class Lease:
    shard_id: str
    owner_instance_id: str
    epoch: int
    expires_at_ns: int


class InMemoryLeaseStore:
    """Transaction-equivalent oracle for tests; PostgreSQL functions own production leases."""

    def __init__(self, *, clock_ns=time.time_ns):
        self._leases: dict[str, Lease] = {}
        self._clock_ns = clock_ns

    def acquire(self, shard_id: str, owner_id: str, ttl_seconds: int) -> Lease | None:
        now = self._clock_ns()
        current = self._leases.get(shard_id)
        if current is not None and current.expires_at_ns > now and current.owner_instance_id != owner_id:
            return None
        epoch = 1 if current is None else current.epoch + int(current.expires_at_ns <= now)
        lease = Lease(shard_id, owner_id, epoch, now + ttl_seconds * 1_000_000_000)
        self._leases[shard_id] = lease
        return lease

    def renew(self, lease: Lease, ttl_seconds: int) -> Lease | None:
        now = self._clock_ns()
        current = self._leases.get(lease.shard_id)
        if current != lease or lease.expires_at_ns <= now:
            return None
        renewed = Lease(lease.shard_id, lease.owner_instance_id, lease.epoch, now + ttl_seconds * 1_000_000_000)
        self._leases[lease.shard_id] = renewed
        return renewed

    def release(self, lease: Lease) -> bool:
        current = self._leases.get(lease.shard_id)
        if current != lease:
            return False
        self._leases[lease.shard_id] = Lease(
            lease.shard_id, lease.owner_instance_id, lease.epoch, self._clock_ns()
        )
        return True


class FencingGate:
    """Rejects any event from an epoch older than the newest observed shard epoch."""

    def __init__(self) -> None:
        self._epochs: dict[str, int] = {}

    def permits(self, shard_id: str, epoch: int) -> bool:
        if epoch <= 0:
            return False
        latest = self._epochs.get(shard_id, 0)
        if epoch < latest:
            return False
        if epoch > latest:
            self._epochs[shard_id] = epoch
        return True

