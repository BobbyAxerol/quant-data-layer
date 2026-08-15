from __future__ import annotations

import time
import uuid

import jwt

from qdl.consumer import ConsumerManifestLoader, ConsumerManifestRegistry
from qdl.security import DataPlaneIdentityService, DataPlaneSecurityConfig


TEST_KEY_ID = "phase7-test"
TEST_SECRET = b"phase7-test-secret-material-32bytes"
TEST_ISSUER = "https://identity.qdl.test"
TEST_AUDIENCE = "qdl-v2-beta"


def manifest_mapping(
    *,
    consumer_id: str,
    subject: str,
    instrument_uid: str,
    feed: str = "BAR",
    interval: str | None = "1m",
    grade: str = "ALPHA",
    source_policy_id: str = "alpha_binance_v1",
    purposes: tuple[str, ...] = ("INTERNAL_ALPHA",),
    permissions: tuple[str, ...] = (
        "instruments:read",
        "snapshot:read",
        "history:read",
        "status:read",
        "quality:read",
        "stream:read",
    ),
) -> dict:
    requirement = {
        "instrument_uid": instrument_uid,
        "feed": feed,
        "consumer_grade": grade,
        "source_policy_id": source_policy_id,
        "warmup_limit": 500,
        "max_freshness_ms": 10_000,
        "require_full_coverage": True,
        "require_final_bars": True,
        "stale_policy": "BLOCK",
        "gap_policy": "BLOCK",
        "recovery": "SNAPSHOT_AND_REPLAY",
        "bar_revision_policy": "EMIT_REVISIONS",
    }
    if interval is not None:
        requirement["interval"] = interval
    return {
        "apiVersion": "qdl/v2",
        "kind": "DataRequirement",
        "metadata": {
            "id": consumer_id,
            "owner": "phase7-tests",
            "subject": subject,
            "environment": "paper",
            "revision": 1,
        },
        "spec": {
            "sdk_major": 2,
            "rollback_contract": "V1",
            "execution_dependency": "FORBIDDEN",
            "permissions": list(permissions),
            "purposes": list(purposes),
            "quotas": {
                "requests_per_minute": 1000,
                "max_batch_items": 50,
                "max_warmup_rows": 2000,
                "max_streams": 10,
                "max_buffer_events": 2000,
            },
            "requirements": [requirement],
        },
    }


def make_manifest(**kwargs):
    return ConsumerManifestLoader.from_mapping(manifest_mapping(**kwargs))


def make_identity(*manifests) -> DataPlaneIdentityService:
    return DataPlaneIdentityService(
        DataPlaneSecurityConfig(
            environment="paper",
            issuer=TEST_ISSUER,
            audience=TEST_AUDIENCE,
            keys_by_id={TEST_KEY_ID: TEST_SECRET},
            algorithms=("HS256",),
        ),
        ConsumerManifestRegistry(tuple(manifests)),
    )


def make_token(
    subject: str,
    *,
    audience: str = TEST_AUDIENCE,
    environment: str = "paper",
    issued_at: int | None = None,
    expires_at: int | None = None,
    not_before: int | None = None,
    key_id: str = TEST_KEY_ID,
    secret: bytes = TEST_SECRET,
    manifest_revision: int = 1,
    roles: tuple[str, ...] = (
        "market_data_reader",
        "historical_reader",
        "stream_consumer",
    ),
) -> str:
    now = int(time.time())
    issued_at = now if issued_at is None else issued_at
    expires_at = issued_at + 300 if expires_at is None else expires_at
    claims = {
        "sub": subject,
        "iss": TEST_ISSUER,
        "aud": audience,
        "iat": issued_at,
        "exp": expires_at,
        "jti": str(uuid.uuid4()),
        "environment": environment,
        "roles": list(roles),
        "consumer_manifest_revision": manifest_revision,
    }
    if not_before is not None:
        claims["nbf"] = not_before
    return jwt.encode(claims, secret, algorithm="HS256", headers={"kid": key_id})


def auth_headers(*, consumer_id: str, subject: str, purpose: str = "INTERNAL_ALPHA"):
    return {
        "Authorization": f"Bearer {make_token(subject)}",
        "X-QDL-Consumer-ID": consumer_id,
        "X-QDL-Purpose": purpose,
    }
