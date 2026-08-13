from __future__ import annotations

import hashlib

import redis

from qdl.projection.trade import ProjectionRecord


_APPLY_LUA = """
local current_epoch = redis.call('GET', KEYS[2])
if current_epoch and tonumber(current_epoch) > tonumber(ARGV[3]) then
  return -1
end
local current = redis.call('GET', KEYS[1])
if current then
  local current_offset = string.sub(current, 1, 20)
  if current_offset >= ARGV[1] then
    return 0
  end
end
for index = 3, #KEYS do
  redis.call('SET', KEYS[index], ARGV[index + 1])
end
redis.call('SET', KEYS[2], ARGV[3])
redis.call('SET', KEYS[1], ARGV[1] .. ':' .. ARGV[2])
return 1
"""


class RedisProjectionTarget:
    """Atomic, idempotent latest-state projection into an isolated namespace."""

    def __init__(self, client: redis.Redis, *, namespace: str = "shadow:qdl:v2"):
        self._client = client
        self._namespace = namespace.rstrip(":")
        if not self._namespace:
            raise ValueError("projection namespace is required")

    def apply(self, record: ProjectionRecord) -> bool:
        data_items = ((record.canonical_key, record.canonical_payload),) + record.legacy_items
        for key, _ in data_items:
            if not key.startswith(f"{self._namespace}:"):
                raise ValueError("projection key escapes configured namespace")
        partition_digest = hashlib.sha256(record.partition_key.encode()).hexdigest()
        checkpoint_key = f"{self._namespace}:checkpoint:{partition_digest}"
        shard_digest = hashlib.sha256(record.shard_id.encode()).hexdigest()
        epoch_key = f"{self._namespace}:lease-epoch:{shard_digest}"
        offset = f"{record.offset:020d}"
        keys = [checkpoint_key, epoch_key, *(key for key, _ in data_items)]
        args: list[str | bytes] = [
            offset,
            record.event_id_hex,
            str(record.lease_epoch),
            *(payload for _, payload in data_items),
        ]
        result = int(self._client.eval(_APPLY_LUA, len(keys), *keys, *args))
        if result < 0:
            return False
        return bool(result)

    def checksum(self) -> str:
        digest = hashlib.sha256()
        keys = sorted(
            key
            for key in self._client.scan_iter(match=f"{self._namespace}:*")
            if b":checkpoint:" not in key and b":lease-epoch:" not in key
        )
        for raw_key in keys:
            value = self._client.get(raw_key)
            if value is None:
                continue
            digest.update(len(raw_key).to_bytes(4, "big"))
            digest.update(raw_key)
            digest.update(hashlib.sha256(value).digest())
        return digest.hexdigest()

    def clear_namespace(self) -> int:
        keys = list(self._client.scan_iter(match=f"{self._namespace}:*"))
        return int(self._client.delete(*keys)) if keys else 0
