from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

import yaml

from qdl.consumer.manifest import ConsumerManifest, ConsumerManifestLoader, ConsumerRoute
from qdl.consumer.realtime_route import (
    RealtimeRoute,
    RealtimeSliceObservation,
    select_realtime_route,
)
from qdl.query import CoverageStatus
from qdl.runtime.stable_catalog import StableSourceCatalog


def _reference_requirement_eligible(
    catalog: StableSourceCatalog,
    requirement: object,
) -> bool:
    """Admit only declared alpha/research reference reads without a spool binding.

    The shared predicate retains the execution boundary: only the separately
    typed mark/index reference product may be execution-grade. All other
    reference metrics remain fresh-snapshot alpha/research reads.
    """

    from qdl.reference.runtime import reference_requirement_eligible

    try:
        instrument = catalog.instrument_for(requirement.instrument_uid)  # type: ignore[attr-defined]
    except (AttributeError, KeyError):
        return False
    return (
        getattr(requirement, "source_policy_id", None) == "crypto_liquid_v2"
        and reference_requirement_eligible(instrument, requirement)  # type: ignore[arg-type]
    )


@dataclass(frozen=True, slots=True)
class StableConsumerMigration:
    consumer_id: str
    manifest_path: Path
    manifest: ConsumerManifest
    state: str
    rollback_route: str
    cutover_authorized: bool


@dataclass(frozen=True, slots=True)
class StableConsumerMigrationPlan:
    schema: str
    revision: int
    contract_version: str
    authority: str
    target_route: str
    consumers: tuple[StableConsumerMigration, ...]

    def __post_init__(self) -> None:
        if self.schema != "qdl.v2.stable-consumer-migration.v1":
            raise ValueError("unsupported stable consumer migration schema")
        if self.revision < 1 or self.contract_version != "2.0.0":
            raise ValueError("stable consumer migration revision/version is invalid")
        if self.authority != "V1" or self.target_route != "V1_WITH_V2_SHADOW":
            raise ValueError("stable consumer migration must preserve V1 authority")
        if not self.consumers:
            raise ValueError("stable consumer migration requires consumers")
        identifiers = [item.consumer_id for item in self.consumers]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("stable consumer migration IDs must be unique")
        for item in self.consumers:
            if (
                item.state != "SHADOW"
                or item.rollback_route != "V1"
                or item.cutover_authorized
                or item.manifest.rollback_contract != "V1"
                or item.manifest.sdk_major != 2
            ):
                raise ValueError("stable consumer migration is not fail-closed")
            if item.consumer_id != item.manifest.consumer_id:
                raise ValueError("stable consumer migration identity mismatch")

    @classmethod
    def load(
        cls,
        path: str | Path,
        *,
        manifest_root: str | Path,
        catalog: StableSourceCatalog,
    ) -> "StableConsumerMigrationPlan":
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        return cls.from_mapping(
            payload,
            manifest_root=manifest_root,
            catalog=catalog,
        )

    @classmethod
    def from_mapping(
        cls,
        payload: Any,
        *,
        manifest_root: str | Path,
        catalog: StableSourceCatalog,
    ) -> "StableConsumerMigrationPlan":
        if not isinstance(payload, dict):
            raise ValueError("stable consumer migration plan must be a mapping")
        expected = {
            "schema", "revision", "contract_version", "authority",
            "target_route", "consumers",
        }
        if set(payload) != expected:
            raise ValueError("stable consumer migration fields are incomplete or unknown")
        consumers_raw = payload["consumers"]
        if not isinstance(consumers_raw, list) or not 1 <= len(consumers_raw) <= 10_000:
            raise ValueError("stable consumer migration requires 1..10000 consumers")
        root = Path(manifest_root).resolve()
        consumers: list[StableConsumerMigration] = []
        for item in consumers_raw:
            if not isinstance(item, dict) or set(item) != {
                "manifest", "consumer_id", "state", "rollback_route",
                "cutover_authorized",
            }:
                raise ValueError("stable consumer migration item is incomplete or unknown")
            declared = Path(str(item["manifest"]))
            relative = (
                Path(*declared.parts[2:])
                if declared.is_absolute() and len(declared.parts) > 1
                and declared.parts[1] == "app"
                else declared
            )
            manifest_path = (root / relative).resolve()
            try:
                manifest_path.relative_to(root)
            except ValueError as error:
                raise ValueError("stable consumer manifest escapes repository root") from error
            manifest = ConsumerManifestLoader.load(manifest_path)
            # Imported here, not at module scope: `qdl.security` imports
            # `qdl.consumer`, so a top-level import of anything under
            # `qdl.runtime` closes a cycle back onto this module. The same
            # reason `stable_catalog.entitlements` defers its import.
            from qdl.runtime.provider_history import pass_through_eligible

            for requirement in manifest.requirements:
                # A requirement is servable if a materialised binding covers it
                # or, failing that, if the pass-through is able to answer it.
                # Declaring only the first would make a manifest that the
                # runtime serves correctly fail to load here, which is how a
                # release check stops describing the system it gates.
                try:
                    catalog.binding_for(requirement)
                except (KeyError, ValueError):
                    if not (
                        pass_through_eligible(catalog, requirement)
                        or _reference_requirement_eligible(catalog, requirement)
                    ):
                        raise
            consumers.append(StableConsumerMigration(
                consumer_id=str(item["consumer_id"]),
                manifest_path=manifest_path,
                manifest=manifest,
                state=str(item["state"]).upper(),
                rollback_route=str(item["rollback_route"]).upper(),
                cutover_authorized=item["cutover_authorized"],
            ))
        if any(not isinstance(item.cutover_authorized, bool) for item in consumers):
            raise ValueError("stable consumer cutover_authorized must be boolean")
        return cls(
            schema=str(payload["schema"]),
            revision=int(payload["revision"]),
            contract_version=str(payload["contract_version"]),
            authority=str(payload["authority"]).upper(),
            target_route=str(payload["target_route"]).upper(),
            consumers=tuple(consumers),
        )


def _resolve_manifest_path(
    declared: Path,
    *,
    manifest_root: Path,
) -> Path:
    """Resolve a governed manifest path without allowing a root escape."""
    relative = (
        Path(*declared.parts[2:])
        if declared.is_absolute() and len(declared.parts) > 1
        and declared.parts[1] == "app"
        else declared
    )
    manifest_path = (manifest_root / relative).resolve()
    try:
        manifest_path.relative_to(manifest_root)
    except ValueError as error:
        raise ValueError("stable consumer manifest escapes repository root") from error
    return manifest_path


def _validate_manifest_servability(
    manifest: ConsumerManifest,
    *,
    catalog: StableSourceCatalog,
) -> None:
    """Ensure every governed requirement has a V2 materialized or pass-through path."""
    # Imported here rather than at module scope because provider-history imports
    # the consumer package through its runtime dependencies.
    from qdl.runtime.provider_history import pass_through_eligible

    for requirement in manifest.requirements:
        try:
            catalog.binding_for(requirement)
        except (KeyError, ValueError):
            if not (
                pass_through_eligible(catalog, requirement)
                or _reference_requirement_eligible(catalog, requirement)
            ):
                raise


@dataclass(frozen=True, slots=True)
class StablePrimaryConsumerRoute:
    """One V2 consumer eligible for an exact V1 rollback route."""

    consumer_id: str
    manifest_path: Path
    manifest: ConsumerManifest
    v1_compatible: bool


@dataclass(frozen=True, slots=True)
class StablePrimaryConsumerRoutePlan:
    """Reviewable V2-primary intent; it is not a runtime authority toggle.

    The checked-in plan binds registered consumer manifests to their exact V1
    rollback contract. A deployment packet subsequently seals it to one
    generated Rust-primary authority record and image digest.
    """

    schema: str
    revision: int
    contract_version: str
    target_route: str
    rollback_route: str
    consumers: tuple[StablePrimaryConsumerRoute, ...]

    def __post_init__(self) -> None:
        if self.schema != "qdl.v2.shared-primary-consumer-route.v1":
            raise ValueError("unsupported stable primary consumer route schema")
        if self.revision < 1 or self.contract_version != "2.0.0":
            raise ValueError("stable primary consumer route revision/version is invalid")
        if self.target_route != "V2_PRIMARY" or self.rollback_route != "V1":
            raise ValueError("stable primary consumer route must declare V2 primary and V1 rollback")
        if not self.consumers:
            raise ValueError("stable primary consumer route requires consumers")
        identifiers = [item.consumer_id for item in self.consumers]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("stable primary consumer route IDs must be unique")
        for item in self.consumers:
            if (
                not item.v1_compatible
                or item.consumer_id != item.manifest.consumer_id
                or item.manifest.sdk_major != 2
                or item.manifest.rollback_contract != "V1"
            ):
                raise ValueError("stable primary consumer route is not rollback-safe")

    @classmethod
    def load(
        cls,
        path: str | Path,
        *,
        manifest_root: str | Path,
        catalog: StableSourceCatalog,
    ) -> "StablePrimaryConsumerRoutePlan":
        payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        return cls.from_mapping(
            payload,
            manifest_root=manifest_root,
            catalog=catalog,
        )

    @classmethod
    def from_mapping(
        cls,
        payload: Any,
        *,
        manifest_root: str | Path,
        catalog: StableSourceCatalog,
    ) -> "StablePrimaryConsumerRoutePlan":
        if not isinstance(payload, dict):
            raise ValueError("stable primary consumer route must be a mapping")
        expected = {
            "schema", "revision", "contract_version", "target_route",
            "rollback_route", "consumers",
        }
        if set(payload) != expected:
            raise ValueError("stable primary consumer route fields are incomplete or unknown")
        raw_consumers = payload["consumers"]
        if not isinstance(raw_consumers, list) or not 1 <= len(raw_consumers) <= 10_000:
            raise ValueError("stable primary consumer route requires 1..10000 consumers")
        root = Path(manifest_root).resolve()
        consumers: list[StablePrimaryConsumerRoute] = []
        for item in raw_consumers:
            if not isinstance(item, dict) or set(item) != {
                "manifest", "consumer_id", "v1_compatible",
            }:
                raise ValueError("stable primary consumer route item is incomplete or unknown")
            compatible = item["v1_compatible"]
            if not isinstance(compatible, bool):
                raise ValueError("stable primary consumer route v1_compatible must be boolean")
            manifest_path = _resolve_manifest_path(
                Path(str(item["manifest"])),
                manifest_root=root,
            )
            manifest = ConsumerManifestLoader.load(manifest_path)
            _validate_manifest_servability(manifest, catalog=catalog)
            consumers.append(StablePrimaryConsumerRoute(
                consumer_id=str(item["consumer_id"]),
                manifest_path=manifest_path,
                manifest=manifest,
                v1_compatible=compatible,
            ))
        return cls(
            schema=str(payload["schema"]),
            revision=int(payload["revision"]),
            contract_version=str(payload["contract_version"]),
            target_route=str(payload["target_route"]).upper(),
            rollback_route=str(payload["rollback_route"]).upper(),
            consumers=tuple(consumers),
        )

    def _canonical_payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "revision": self.revision,
            "contract_version": self.contract_version,
            "target_route": self.target_route,
            "rollback_route": self.rollback_route,
            "consumers": [
                {
                    "consumer_id": item.consumer_id,
                    "manifest_revision": item.manifest.manifest_revision,
                    "manifest_sha256": item.manifest.manifest_sha256,
                    "v1_compatible": item.v1_compatible,
                }
                for item in self.consumers
            ],
        }

    @property
    def digest(self) -> str:
        return hashlib.sha256(
            json.dumps(
                self._canonical_payload(),
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()

    def seal(self, authority: Mapping[str, Any]) -> dict[str, object]:
        """Bind the route intent to one generated primary authority record."""
        # Deferred import avoids a consumer/runtime import cycle.
        from qdl.runtime.stable_deployment import validate_shared_authority_record

        validate_shared_authority_record(authority)
        if authority.get("mode") != "RUST_PRIMARY":
            raise ValueError("primary consumer route requires RUST_PRIMARY authority")
        authority_payload = dict(authority)
        authority_digest = hashlib.sha256(
            json.dumps(authority_payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return {
            "schema": "qdl.v2.sealed-primary-consumer-route.v1",
            "route_plan_revision": self.revision,
            "route_plan_sha256": self.digest,
            "authority_sha256": authority_digest,
            "authority_slice_id": str(authority["slice_id"]),
            "authority_revision": int(authority["revision"]),
            "authority_mode": "RUST_PRIMARY",
            "candidate_image_digest": str(authority["candidate_image_digest"]),
            "target_route": self.target_route,
            "rollback_route": self.rollback_route,
            "consumers": self._canonical_payload()["consumers"],
        }


def primary_fallback_return_drill(
    plan: StablePrimaryConsumerRoutePlan,
) -> dict[str, object]:
    """Prove route semantics for every declared consumer requirement.

    This is a deterministic contract drill, not a provider/realtime health
    claim. It exercises only the typed route evaluator: healthy V2, an eligible
    V2 failure with a fresh compatible V1 fallback, then healthy V2 again.
    """
    decisions: list[dict[str, str]] = []
    for item in plan.consumers:
        for requirement in item.manifest.requirements:
            ready = RealtimeSliceObservation(
                available=True,
                authoritative=True,
                coverage=CoverageStatus.FULL,
                source_age_ms=0,
                receive_age_ms=0,
                gap_open=False,
                final_bar_available=True,
            )
            failure = RealtimeSliceObservation(
                available=True,
                authoritative=True,
                coverage=CoverageStatus.FULL,
                source_age_ms=(
                    requirement.max_freshness_ms + 1
                    if requirement.max_freshness_ms is not None
                    else 0
                ),
                receive_age_ms=0,
                gap_open=requirement.max_freshness_ms is None,
                final_bar_available=True,
            )
            before = select_realtime_route(
                manifest=item.manifest,
                migration_route=ConsumerRoute.V2,
                requirement=requirement,
                v2_observation=ready,
                v1_observation=ready,
                v1_compatible=item.v1_compatible,
            )
            fallback = select_realtime_route(
                manifest=item.manifest,
                migration_route=ConsumerRoute.V2,
                requirement=requirement,
                v2_observation=failure,
                v1_observation=ready,
                v1_compatible=item.v1_compatible,
            )
            returned = select_realtime_route(
                manifest=item.manifest,
                migration_route=ConsumerRoute.V2,
                requirement=requirement,
                v2_observation=ready,
                v1_observation=ready,
                v1_compatible=item.v1_compatible,
            )
            if (
                before.route is not RealtimeRoute.V2_PRIMARY
                or fallback.route is not RealtimeRoute.V1_FALLBACK
                or returned.route is not RealtimeRoute.V2_PRIMARY
            ):
                raise RuntimeError("primary fallback-return drill is not route-safe")
            decisions.append({
                "consumer_id": item.consumer_id,
                "requirement_key": before.requirement_key,
                "before": before.route.value,
                "fallback": fallback.route.value,
                "returned": returned.route.value,
            })
    encoded = json.dumps(decisions, sort_keys=True, separators=(",", ":")).encode()
    return {
        "schema": "qdl.v2.primary-fallback-return-drill.v1",
        "route_plan_sha256": plan.digest,
        "consumer_count": len(plan.consumers),
        "requirement_count": len(decisions),
        "transitions": decisions,
        "transitions_sha256": hashlib.sha256(encoded).hexdigest(),
        "test_provenance": True,
    }
