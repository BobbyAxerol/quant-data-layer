from __future__ import annotations

import hashlib
from collections.abc import Iterable


def deterministic_event_id(parts: Iterable[str | int | bytes], *, size: int = 16) -> bytes:
    """Hash length-delimited identity parts without delimiter collisions."""

    if size not in {16, 32}:
        raise ValueError("event ID size must be 16 or 32 bytes")
    digest = hashlib.blake2b(digest_size=size, person=b"qdl-event-v2")
    for part in parts:
        payload = part if isinstance(part, bytes) else str(part).encode("utf-8")
        digest.update(len(payload).to_bytes(4, "big", signed=False))
        digest.update(payload)
    return digest.digest()

