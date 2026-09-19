"""Small shared authentication helpers for stable-internal HTTP edges.

The stable projector and query roles already share an internal HMAC secret and
workload mTLS.  Keeping the signature exact in one module prevents a new
private read endpoint from drifting from the existing canonical-ingest boundary.
"""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
from urllib.parse import urlsplit


def stable_hmac_signature(secret: bytes, body: bytes) -> str:
    """Return the stable-internal signature for one exact request body."""

    return "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()


def is_stable_internal_url(value: str) -> bool:
    """Allow only the fixed internal stream-gateway address space."""

    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return False
    try:
        return ipaddress.ip_address(parsed.hostname).is_loopback
    except ValueError:
        return parsed.hostname in {
            "localhost",
            "stream_v2",
            "stream_v2_active",
            "stream_v2_passive",
            "qdl-stable-stream",
        } or parsed.hostname.endswith(".internal")
