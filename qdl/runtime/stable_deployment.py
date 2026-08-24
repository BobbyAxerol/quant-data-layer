from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlsplit

import yaml

from qdl.query import FeedType
from qdl.runtime.stable_catalog import StableSourceBinding, StableSourceCatalog


_MODES = frozenset({"RUST_NATIVE", "PYTHON_REST", "PYTHON_VENDOR_SDK"})
_SEQUENCE_POLICIES = frozenset({"NONE", "MONOTONIC", "CONTIGUOUS"})
STABLE_TOPIC_PARTITIONS = 6
STABLE_CORE_WORKER_COUNT = 3
_PROVIDER_KINDS = {
    ("BINANCE", "TRADE"): frozenset({"binance_usdm_trade", "binance_spot_trade"}),
    ("BINANCE", "QUOTE"): frozenset({"binance_usdm_bbo", "binance_spot_bbo"}),
    ("BINANCE", "BAR"): frozenset({
        "binance_usdm_bar",
        "binance_spot_bar",
        "binance_usdm_rest_bar",
        "binance_spot_rest_bar",
    }),
    ("OKX", "TRADE"): frozenset({"okx_trade"}),
    ("OKX", "QUOTE"): frozenset({"okx_bbo"}),
    ("OKX", "BAR"): frozenset({"okx_bar"}),
    ("HNX", "TRADE"): frozenset({"dnse_trade"}),
    ("HNX", "BAR"): frozenset({"dnse_bar"}),
    ("HOSE", "TRADE"): frozenset({"dnse_trade"}),
    ("HOSE", "BAR"): frozenset({"dnse_bar"}),
}


@dataclass(frozen=True, slots=True)
class AuthorityPromotionScope:
    schema: str
    revision: int
    binding_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            self.schema != "qdl.v2.authority-promotion-scope.v1"
            or self.revision < 1
            or not self.binding_ids
            or any(not value.strip() for value in self.binding_ids)
            or len(self.binding_ids) != len(set(self.binding_ids))
        ):
            raise ValueError("authority promotion scope is invalid")

    @classmethod
    def load(
        cls, path: str | Path, *, catalog: StableSourceCatalog
    ) -> "AuthorityPromotionScope":
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or set(payload) != {
            "schema", "revision", "binding_ids",
        }:
            raise ValueError("authority promotion scope fields are incomplete or unknown")
        values = payload["binding_ids"]
        if not isinstance(values, list) or not values:
            raise ValueError("authority promotion scope requires binding IDs")
        result = cls(
            schema=str(payload["schema"]),
            revision=int(payload["revision"]),
            binding_ids=tuple(str(value) for value in values),
        )
        catalog_ids = {item.binding_id for item in catalog.bindings}
        unknown = set(result.binding_ids) - catalog_ids
        if unknown:
            raise ValueError(
                "authority promotion scope contains unknown bindings: "
                + ",".join(sorted(unknown))
            )
        return result

    def digest(self) -> str:
        payload = {
            "schema": self.schema,
            "revision": self.revision,
            "binding_ids": list(self.binding_ids),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class StableAcquisitionBinding:
    binding_id: str
    mode: str
    runtime: str
    provider_kind: str
    native_channel: str
    sequence_policy: str
    websocket_url: str | None
    business_websocket_url: str | None
    # Program rule 6: an unused feed is disabled by configuration and
    # zero-demand evidence, never deleted. The catalog keeps the capability so a
    # reviewed DataRequirement can re-enable it without a code change; this flag
    # is what stops the runtime acquiring it in the meantime. Until it existed,
    # the only way to stop acquiring a feed was to delete it from the catalog,
    # which deleted the capability the rule says to keep.
    enabled: bool = True

    def validate(self, source: StableSourceBinding) -> None:
        if (
            not self.binding_id
            or self.mode not in _MODES
            or not self.runtime
            or not self.provider_kind
            or not self.native_channel
            or self.sequence_policy not in _SEQUENCE_POLICIES
        ):
            raise ValueError("stable acquisition binding is incomplete or unsupported")
        allowed = _PROVIDER_KINDS.get(
            (source.instrument.identity.venue, source.feed.value), frozenset()
        )
        if self.provider_kind not in allowed:
            raise ValueError("stable acquisition provider kind differs from catalog feed")
        if self.provider_kind == "okx_bbo" and self.sequence_policy != "NONE":
            raise ValueError("OKX bbo-tbt is replace-only and cannot require sequence continuity")
        if self.mode == "RUST_NATIVE":
            if self.runtime not in {"BINANCE", "OKX"}:
                raise ValueError("Rust native acquisition supports Binance/OKX only")
            if self.runtime != source.instrument.identity.venue:
                raise ValueError("Rust runtime differs from stable venue")
            if self.provider_kind.endswith("_rest_bar"):
                raise ValueError("Rust native acquisition cannot use a REST BAR provider kind")
            self._require_wss(self.websocket_url)
            if self.runtime == "OKX":
                self._require_wss(self.business_websocket_url)
        elif self.mode == "PYTHON_REST":
            if (
                source.feed is not FeedType.BAR
                or self.runtime not in {"BINANCE", "OKX"}
                or self.runtime != source.instrument.identity.venue
                or self.websocket_url is not None
                or self.business_websocket_url is not None
            ):
                raise ValueError(
                    "Python REST acquisition is reserved for venue-owned BAR "
                    "without WebSocket"
                )
            if self.runtime == "BINANCE" and self.provider_kind not in {
                "binance_usdm_rest_bar", "binance_spot_rest_bar",
            }:
                raise ValueError("Binance Python REST BAR needs a REST provider kind")
        elif (
            source.instrument.identity.venue not in {"HNX", "HOSE"}
            or self.runtime != "DNSE"
            or self.websocket_url is not None
        ):
            raise ValueError("Python vendor SDK acquisition is reserved for VN sources")

    @staticmethod
    def _require_wss(value: str | None) -> None:
        parsed = urlsplit(value or "")
        if parsed.scheme != "wss" or not parsed.hostname:
            raise ValueError("stable native WebSocket URL must use wss")


@dataclass(frozen=True, slots=True)
class StableAcquisitionPlan:
    schema: str
    revision: int
    raw_topic: str
    canonical_topic: str
    quarantine_topic: str
    bindings: tuple[StableAcquisitionBinding, ...]

    def __post_init__(self) -> None:
        if self.schema != "qdl.v2.stable-acquisition-bindings.v1" or self.revision < 1:
            raise ValueError("unsupported stable acquisition schema/revision")
        topics = (self.raw_topic, self.canonical_topic, self.quarantine_topic)
        if any(not value for value in topics) or len(set(topics)) != 3:
            raise ValueError("stable acquisition topics must be non-empty and unique")
        identities = [item.binding_id for item in self.bindings]
        if not identities or len(identities) != len(set(identities)):
            raise ValueError("stable acquisition binding IDs must be non-empty and unique")

    @classmethod
    def load(
        cls, path: str | Path, *, catalog: StableSourceCatalog
    ) -> "StableAcquisitionPlan":
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or set(payload) != {
            "schema", "revision", "topics", "bindings",
        }:
            raise ValueError("stable acquisition plan fields are incomplete or unknown")
        topics = payload["topics"]
        values = payload["bindings"]
        if not isinstance(topics, dict) or set(topics) != {
            "raw", "canonical", "quarantine",
        }:
            raise ValueError("stable acquisition topics are incomplete or unknown")
        if not isinstance(values, list) or not 1 <= len(values) <= 100_000:
            raise ValueError("stable acquisition requires 1..100000 bindings")
        bindings = []
        for value in values:
            required = {
                "binding_id", "mode", "runtime", "provider_kind", "native_channel",
                "sequence_policy", "websocket_url", "business_websocket_url",
            }
            if not isinstance(value, dict) or not required <= set(value) or (
                set(value) - required - {"enabled"}
            ):
                raise ValueError("stable acquisition binding fields are incomplete or unknown")
            enabled = value.get("enabled", True)
            if not isinstance(enabled, bool):
                raise ValueError("stable acquisition 'enabled' must be a boolean")
            bindings.append(StableAcquisitionBinding(
                binding_id=str(value["binding_id"]),
                mode=str(value["mode"]).upper(),
                runtime=str(value["runtime"]).upper(),
                provider_kind=str(value["provider_kind"]),
                native_channel=str(value["native_channel"]),
                sequence_policy=str(value["sequence_policy"]).upper(),
                websocket_url=(
                    str(value["websocket_url"]) if value["websocket_url"] is not None else None
                ),
                business_websocket_url=(
                    str(value["business_websocket_url"])
                    if value["business_websocket_url"] is not None else None
                ),
                enabled=enabled,
            ))
        result = cls(
            schema=str(payload["schema"]),
            revision=int(payload["revision"]),
            raw_topic=str(topics["raw"]),
            canonical_topic=str(topics["canonical"]),
            quarantine_topic=str(topics["quarantine"]),
            bindings=tuple(bindings),
        )
        source_by_id = {item.binding_id: item for item in catalog.bindings}
        if set(source_by_id) != {item.binding_id for item in result.bindings}:
            raise ValueError("stable acquisition and source catalog binding sets differ")
        for item in result.bindings:
            item.validate(source_by_id[item.binding_id])
        if result.canonical_topic != catalog.canonical_stream:
            raise ValueError("stable acquisition canonical topic differs from catalog")
        return result

    def core_config(
        self,
        *,
        catalog: StableSourceCatalog,
        authority: Mapping[str, Any],
        max_events: int = 0,
        worker_index: int = 1,
        binding_ids: frozenset[str] | None = None,
        excluded_binding_ids: frozenset[str] = frozenset(),
    ) -> dict[str, Any]:
        self._validate_authority(authority)
        if not 1 <= worker_index <= STABLE_CORE_WORKER_COUNT:
            raise ValueError("stable core worker index is outside the topology bound")
        source_by_id = {item.binding_id: item for item in catalog.bindings}
        acquisitions = {item.binding_id: item for item in self.bindings}
        # A disabled binding keeps its capability in the catalog but is not
        # acquired, so no runtime role is configured to consume it.
        acquired = frozenset(
            item.binding_id for item in self.bindings if item.enabled
        )
        selected_ids = acquired if binding_ids is None else frozenset(binding_ids) & acquired
        selected_ids = selected_ids - frozenset(excluded_binding_ids)
        if not selected_ids or not selected_ids.issubset(source_by_id):
            raise ValueError("stable core binding selection is empty or unknown")
        bindings = []
        for binding_id in sorted(selected_ids):
            source = source_by_id[binding_id]
            acquisition = acquisitions[binding_id]
            identity = source.instrument.identity
            bindings.append({
                "provider": source.provider,
                "venue": identity.venue,
                "market": identity.market,
                "product_type": identity.product_type.value,
                "native_symbol": source.instrument.native_symbol,
                "native_channel": acquisition.native_channel,
                "provider_kind": acquisition.provider_kind,
                "instrument_uid": source.instrument.instrument_uid,
                "instrument_id": source.instrument.instrument_id,
                "instrument_revision": source.instrument.metadata_revision,
                "instrument_catalog_revision": catalog.catalog_revision,
                "source_id": source.source_id,
                "source_role": source.source_role,
                "normalizer_version": source.normalizer_version,
                "require_final_bar": source.require_final_bar,
                "sequence_policy": acquisition.sequence_policy,
            })
        return {
            "core": {
                "canonical_stream": self.canonical_topic,
                "quarantine_stream": self.quarantine_topic,
                "allow_test_provenance": False,
                "dedup_capacity": 1_000_000,
                "bindings": bindings,
            },
            "raw_topics": [self.raw_topic],
            "authority": dict(authority),
            "shard_id": f"qdl-v2-stable-core-{worker_index:03d}",
            "transactional_id": f"qdl-v2-stable-core-{worker_index:03d}",
            "batch_size": 256,
            "batch_wait_ms": 25,
            "max_events": max_events,
            "metrics_every_batches": 100,
        }

    def production_core_config(
        self,
        *,
        catalog: StableSourceCatalog,
        raw_authority: Mapping[str, Any],
        promotion_scope: AuthorityPromotionScope,
        worker_index: int,
        partition_plan_epoch: int = 1,
    ) -> dict[str, Any]:
        self._validate_authority(raw_authority)
        if partition_plan_epoch < 1:
            raise ValueError("production partition plan epoch must be positive")
        selected_ids = frozenset(promotion_scope.binding_ids)
        shadow = self.core_config(
            catalog=catalog,
            authority=raw_authority,
            worker_index=worker_index,
            binding_ids=selected_ids,
        )
        source_by_id = {item.binding_id: item for item in catalog.bindings}
        slices = []
        for source in sorted(
            (source_by_id[binding_id] for binding_id in selected_ids),
            key=lambda value: value.binding_id,
        ):
            identity = source.instrument.identity
            native = source.instrument.native_symbol.lower()
            slice_id = (
                f"production/{identity.venue.lower()}/{identity.market.lower()}/"
                f"{identity.product_type.value.lower()}/{source.feed.value.lower()}/"
                f"plan-{partition_plan_epoch}/{native}"
            )
            slices.append({
                "subscription_id": source.source_id,
                "slice_id": slice_id,
                "shard_id": source.binding_id,
                "raw_authority_revision": int(raw_authority["revision"]),
                "raw_lease_epoch": 1,
                "raw_partition_plan_epoch": partition_plan_epoch,
            })
        return {
            "core": shadow["core"],
            "topics": {
                "raw_inputs": [self.raw_topic],
                "authority_control": "qdl.authority.v1",
                "target_checkpoints": "qdl.target-checkpoint.v1",
                "canary_canonical": "md.canary.canonical.v2",
                "primary_canonical": self.canonical_topic,
                "public_v2": "md.projector.public.v2",
                "legacy_v1": "md.projector.legacy.v1",
                "quarantine": self.quarantine_topic,
            },
            "slices": slices,
            "transactional_id": f"qdl-v2-production-core-{worker_index:03d}",
            "promotion_scope_digest": promotion_scope.digest(),
            "partition_plan_epoch": partition_plan_epoch,
            "bootstrap_cursor_path": "/runtime/production-bootstrap.json",
            "batch_size": 128,
            "batch_wait_ms": 10,
            "max_events": 0,
            "metrics_every_batches": 100,
        }

    def native_ingestor_configs(
        self,
        *,
        catalog: StableSourceCatalog,
        authority: Mapping[str, Any],
        max_events: int = 0,
        max_runtime_seconds: int = 0,
        binding_ids: frozenset[str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        self._validate_authority(authority)
        source_by_id = {item.binding_id: item for item in catalog.bindings}
        enabled_ids = {item.binding_id for item in self.bindings if item.enabled}
        selected_ids = enabled_ids if binding_ids is None else frozenset(binding_ids)
        unknown_ids = selected_ids - set(source_by_id)
        if unknown_ids:
            raise ValueError("native ingestor selection contains unknown bindings")
        disabled_ids = selected_ids - enabled_ids
        if disabled_ids:
            raise ValueError("native ingestor selection contains disabled bindings")
        grouped: dict[tuple[str, str], list[StableAcquisitionBinding]] = {}
        for acquisition in self.bindings:
            if (
                acquisition.binding_id in selected_ids
                and acquisition.enabled
                and acquisition.mode == "RUST_NATIVE"
            ):
                source = source_by_id[acquisition.binding_id]
                grouped.setdefault(
                    (acquisition.runtime, source.instrument.identity.market), []
                ).append(acquisition)
        result = {}
        for (runtime, market), values in sorted(grouped.items()):
            first = values[0]
            bindings = []
            for acquisition in sorted(values, key=lambda item: item.binding_id):
                source = source_by_id[acquisition.binding_id]
                identity = source.instrument.identity
                bindings.append({
                    "provider": source.provider,
                    "venue": identity.venue,
                    "market": identity.market,
                    "product_type": identity.product_type.value,
                    "native_symbol": source.instrument.native_symbol,
                    "native_channel": acquisition.native_channel,
                    "subscription_id": source.source_id,
                    "adapter_version": source.adapter_version,
                    "instrument_catalog_revision": catalog.catalog_revision,
                    "feed": source.feed.value,
                    "delivery_class": (
                        "LATEST_STATE"
                        if source.feed is FeedType.QUOTE
                        else "LOSSLESS"
                    ),
                })
            key = f"{runtime.lower()}-{market.lower()}"
            result[key] = {
                "runtime": runtime,
                "websocket_url": first.websocket_url,
                "business_websocket_url": first.business_websocket_url,
                "raw_stream": self.raw_topic,
                "shard_id": f"qdl-v2-stable-{key}",
                "lease_epoch": 1,
                "partition_plan_epoch": 1,
                "config_revision": self.revision,
                "heartbeat_seconds": 15,
                "max_events": max_events,
                "max_runtime_seconds": max_runtime_seconds,
                "metrics_every_events": 1000,
                "generation_state_path": (
                    f"/var/lib/qdl-stable/runtime/generations/{key}"
                ),
                "max_inflight_publishes": 512,
                "max_subscriptions_per_connection": (
                    200 if runtime == "BINANCE" else 100
                ),
                "latest_state_flush_ms": 50,
                "authority": dict(authority),
                "bindings": bindings,
            }
        return result

    def demand_runtime_configs(
        self,
        *,
        catalog: StableSourceCatalog,
        authority: Mapping[str, Any],
        binding_ids: Iterable[str],
        worker_count: int = STABLE_CORE_WORKER_COUNT,
    ) -> dict[str, Any]:
        """Build one shared-core topology for a resolved demand revision.

        The returned `ingestors` map is keyed only by venue/market. Symbols are
        subscription data inside each role and never turn into Compose services.
        """
        self._validate_authority(authority)
        selected_ids = frozenset(str(value) for value in binding_ids)
        known_ids = {item.binding_id for item in catalog.bindings}
        if not selected_ids or not selected_ids.issubset(known_ids):
            raise ValueError("demand runtime selection is empty or contains unknown bindings")
        if not 1 <= worker_count <= STABLE_CORE_WORKER_COUNT:
            raise ValueError("demand runtime worker count is outside topology bound")
        core = tuple(
            self.core_config(
                catalog=catalog,
                authority=authority,
                worker_index=worker_index,
                binding_ids=selected_ids,
            )
            for worker_index in range(1, worker_count + 1)
        )
        ingestors = self.native_ingestor_configs(
            catalog=catalog,
            authority=authority,
            binding_ids=selected_ids,
        )
        return {
            "schema": "qdl.v2.universal-demand-runtime-config.v1",
            "demand_binding_count": len(selected_ids),
            "core_worker_count": worker_count,
            "core": core,
            "ingestors": ingestors,
        }

    @staticmethod
    def _validate_authority(authority: Mapping[str, Any]) -> None:
        digest = str(authority.get("candidate_image_digest", ""))
        if (
            authority.get("mode") != "RUST_SHADOW"
            or authority.get("public_write_allowed") is not False
            or authority.get("legacy_write_allowed") is not False
            or not digest.startswith("sha256:")
            or len(digest) != 71
        ):
            raise ValueError("stable authority is not an isolated Rust shadow record")


RUNTIME_CONFIG_MODE = 0o644


def _write_runtime_config(path: Path, encoded: bytes) -> None:
    """Write non-secret runtime JSON readable by the non-root containers."""
    path.write_bytes(encoded)
    path.chmod(RUNTIME_CONFIG_MODE)


def write_production_core_bundle(
    destination: Path,
    *,
    catalog: StableSourceCatalog,
    acquisition: StableAcquisitionPlan,
    promotion_scope: AuthorityPromotionScope,
    raw_authority: Mapping[str, Any],
    partition_plan_epoch: int = 1,
) -> dict[str, str]:
    destination.mkdir(parents=True, exist_ok=True)
    payloads = {
        f"production-core-{worker_index:03d}.json":
            acquisition.production_core_config(
                catalog=catalog,
                raw_authority=raw_authority,
                promotion_scope=promotion_scope,
                worker_index=worker_index,
                partition_plan_epoch=partition_plan_epoch,
            )
        for worker_index in range(1, STABLE_CORE_WORKER_COUNT + 1)
    }
    digests = {}
    for name, payload in sorted(payloads.items()):
        encoded = (
            json.dumps(payload, indent=2, sort_keys=True, separators=(",", ": ")) + "\n"
        ).encode()
        path = destination / name
        _write_runtime_config(path, encoded)
        digests[name] = hashlib.sha256(encoded).hexdigest()
    manifest = {
        "schema": "qdl.v2.production-core-bundle.v1",
        "partition_plan_epoch": partition_plan_epoch,
        "worker_count": STABLE_CORE_WORKER_COUNT,
        "promotion_scope_revision": promotion_scope.revision,
        "promotion_scope_digest": promotion_scope.digest(),
        "promotion_binding_count": len(promotion_scope.binding_ids),
        "files": digests,
    }
    encoded_manifest = (
        json.dumps(manifest, indent=2, sort_keys=True, separators=(",", ": ")) + "\n"
    ).encode()
    manifest_path = destination / "production-core-manifest.json"
    _write_runtime_config(manifest_path, encoded_manifest)
    digests[manifest_path.name] = hashlib.sha256(encoded_manifest).hexdigest()
    return digests


def stable_authority_record(
    *,
    rust_image_digest: str,
    capability_manifest: Path,
    contract: Path,
    partition_plan: bytes,
    effective_at_ns: int,
) -> dict[str, Any]:
    digest = rust_image_digest.removeprefix("sha256:")
    if len(digest) != 64 or any(value not in "0123456789abcdef" for value in digest):
        raise ValueError("stable Rust image digest must be SHA-256")
    if effective_at_ns <= 0:
        raise ValueError("stable authority effective time must be positive")
    return {
        "schema": "qdl.authority-record.v1",
        "slice_id": "qdl-v2-stable-multivenue-shadow",
        "revision": 1,
        "mode": "RUST_SHADOW",
        "candidate_image_digest": f"sha256:{digest}",
        "capability_manifest_digest": hashlib.sha256(capability_manifest.read_bytes()).hexdigest(),
        "contract_digest": hashlib.sha256(contract.read_bytes()).hexdigest(),
        "partition_plan_digest": hashlib.sha256(partition_plan).hexdigest(),
        "public_write_allowed": False,
        "legacy_write_allowed": False,
        "approved_by": "phase-b-isolated-stable-candidate",
        "effective_at_ns": effective_at_ns,
    }


def write_stable_runtime_bundle(
    destination: Path,
    *,
    catalog: StableSourceCatalog,
    acquisition: StableAcquisitionPlan,
    authority: Mapping[str, Any],
    promotion_scope: AuthorityPromotionScope | None = None,
) -> dict[str, str]:
    destination.mkdir(parents=True, exist_ok=True)
    excluded_binding_ids = (
        frozenset(promotion_scope.binding_ids) if promotion_scope is not None else frozenset()
    )
    payloads = {
        "authority.json": dict(authority),
        **{
            "core.json" if worker_index == 1 else f"core-{worker_index:03d}.json":
                acquisition.core_config(
                    catalog=catalog,
                    authority=authority,
                    worker_index=worker_index,
                    excluded_binding_ids=excluded_binding_ids,
                )
            for worker_index in range(1, STABLE_CORE_WORKER_COUNT + 1)
        },
        **{
            f"ingestor-{name}.json": payload
            for name, payload in acquisition.native_ingestor_configs(
                catalog=catalog, authority=authority
            ).items()
        },
    }
    digests = {}
    for name, payload in sorted(payloads.items()):
        encoded = (
            json.dumps(payload, indent=2, sort_keys=True, separators=(",", ": ")) + "\n"
        ).encode()
        path = destination / name
        _write_runtime_config(path, encoded)
        digests[name] = hashlib.sha256(encoded).hexdigest()
    return digests
