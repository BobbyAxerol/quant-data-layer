from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from qdl.consumer.manifest import ConsumerManifest, ConsumerManifestLoader
from qdl.runtime.stable_catalog import StableSourceCatalog


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
            for requirement in manifest.requirements:
                catalog.binding_for(requirement)
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
