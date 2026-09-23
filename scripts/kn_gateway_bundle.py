#!/usr/bin/env python3
"""Compile the native gateway's authorization bundle (KN-1 K1.5, guide 18.3).

Purpose: the Rust Stream must enforce exactly what Python enforces today. It
therefore does not parse YAML itself: this script loads the consumer
manifests and the stable source catalog through the existing Python loaders
(`ConsumerManifestLoader`, `StableSourceCatalog`) and writes the fields the
gateway needs as one canonical JSON document with its SHA-256. The gateway
refuses a bundle whose hash does not match.

Boundary: reads repository config only; writes one file. No runtime access.

  python -B scripts/kn_gateway_bundle.py --environment paper --out bundle.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "qdl.kn.v220.gateway-bundle.v1"


def _value(item: Any) -> Any:
    return getattr(item, "value", item)


def compile_bundle(*, environment: str, catalog_path: Path, manifest_paths: Iterable[Path]) -> dict[str, Any]:
    from qdl.consumer.manifest import ConsumerManifestLoader
    from qdl.projection.state_contract import LogicalProductKey
    from qdl.runtime.stable_catalog import StableSourceCatalog

    catalog = StableSourceCatalog.load(catalog_path)
    bindings = []
    for binding in sorted(catalog.bindings, key=lambda item: item.binding_id):
        identity = binding.instrument.identity
        bindings.append({
            "binding_id": binding.binding_id,
            "instrument_uid": identity.instrument_uid,
            "venue": identity.venue,
            "market": identity.market,
            "feed": binding.feed.value,
            "interval": binding.interval,
            "source_policy_id": binding.source_policy_id,
            "physical_key": binding.partition_key,
            "source_id": binding.source_id,
            "stale_after_ms": binding.stale_after_ms,
            "product_key": LogicalProductKey.for_product(
                environment=environment, venue=identity.venue, market=identity.market,
                instrument_uid=identity.instrument_uid, feed=binding.feed.value,
                interval=binding.interval,
            ).encode(),
        })
    manifests = []
    for path in sorted(manifest_paths):
        manifest = ConsumerManifestLoader.load(path)
        manifests.append({
            "consumer_id": manifest.consumer_id,
            "subject": manifest.subject,
            "environment": manifest.environment,
            "manifest_revision": manifest.manifest_revision,
            "manifest_sha256": manifest.manifest_sha256,
            "allowed_purposes": sorted(_value(item) for item in manifest.allowed_purposes),
            "allowed_permissions": sorted(_value(item) for item in manifest.allowed_permissions),
            "quotas": {
                "requests_per_minute": manifest.quotas.requests_per_minute,
                "max_batch_items": manifest.quotas.max_batch_items,
                "max_warmup_rows": manifest.quotas.max_warmup_rows,
                "max_streams": manifest.quotas.max_streams,
                "max_buffer_events": manifest.quotas.max_buffer_events,
            },
            # The fields ConsumerManifest.requirement_allowed compares.
            "requirements": sorted(
                ({
                    "instrument_uid": item.instrument_uid,
                    "feed": _value(item.feed),
                    "interval": item.interval,
                    "consumer_grade": _value(item.consumer_grade),
                    "source_policy_id": item.source_policy_id,
                    "event_recency_policy": _value(item.event_recency_policy),
                    "max_session_liveness_ms": item.max_session_liveness_ms,
                } for item in manifest.requirements),
                key=lambda row: json.dumps(row, sort_keys=True),
            ),
        })
    document = {
        "schema": SCHEMA,
        "environment": environment,
        "catalog": {
            "catalog_revision": catalog.catalog_revision,
            "source_policy_revision": catalog.source_policy_revision,
            "canonical_stream": catalog.canonical_stream,
            "bindings": bindings,
        },
        "manifests": manifests,
    }
    document["sha256"] = bundle_sha256(document)
    return document


def bundle_sha256(document: dict[str, Any]) -> str:
    """SHA-256 over the canonical JSON of everything except the hash itself."""
    body = {key: value for key, value in document.items() if key != "sha256"}
    return hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--environment", required=True)
    parser.add_argument("--catalog", type=Path, default=ROOT / "config/v2/stable-source-bindings.yaml")
    parser.add_argument("--manifests", type=Path, nargs="*",
                        default=sorted((ROOT / "consumers/stable").glob("*.yaml")))
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    document = compile_bundle(environment=args.environment, catalog_path=args.catalog,
                              manifest_paths=args.manifests)
    args.out.write_text(json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    print(json.dumps({"out": str(args.out), "sha256": document["sha256"]}), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
