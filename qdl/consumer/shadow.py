from __future__ import annotations

from dataclasses import dataclass
from qdl.consumer.manifest import ConsumerManifest, ConsumerMigration, MigrationState
from qdl_sdk import AsyncDataLayerClient
from qdl_sdk.models import DataRequirement, StreamEvent


@dataclass(frozen=True)
class ShadowObservation:
    consumer_id: str
    instrument_uid: str
    feed: str
    logical_offset: int
    event_id_hex: str
    source_id: str
    revision: int


class ManifestShadowConsumer:
    """Audited, side-effect-free V2 consumer used before governed activation."""

    def __init__(
        self,
        *,
        manifest: ConsumerManifest,
        migration: ConsumerMigration,
        client: AsyncDataLayerClient,
    ) -> None:
        if migration.consumer_id != manifest.consumer_id:
            raise ValueError("migration and manifest consumer identities differ")
        if migration.manifest_sha256 != manifest.manifest_sha256:
            raise ValueError("migration does not approve this manifest revision")
        if migration.state not in {MigrationState.SHADOW, MigrationState.ACCEPTED}:
            raise ValueError("consumer must be SHADOW or ACCEPTED for observation")
        if client.consumer_id != manifest.consumer_id:
            raise ValueError("SDK and manifest consumer identities differ")
        self.manifest = manifest
        self.client = client

    async def observe_once(self, requirement_index: int = 0) -> ShadowObservation:
        try:
            domain = self.manifest.requirements[requirement_index]
        except IndexError as error:
            raise ValueError("manifest requirement index is invalid") from error
        requirement = DataRequirement(
            instrument_uid=domain.instrument_uid,
            feed=domain.feed.value,
            consumer_grade=domain.consumer_grade.value,
            source_policy_id=domain.source_policy_id,
            interval=domain.interval,
            warmup_limit=domain.warmup_limit,
            max_freshness_ms=domain.max_freshness_ms,
            require_full_coverage=domain.require_full_coverage,
            require_final_bars=domain.require_final_bars,
            stale_policy=domain.stale_policy.value,
            gap_policy=domain.gap_policy.value,
            recovery=domain.recovery.value,
            bar_revision_policy=domain.bar_revision_policy.value,
        )
        async with self.client.warmup_then_stream(requirement) as session:
            while True:
                item = await session.__anext__()
                if not isinstance(item, StreamEvent):
                    continue
                session.acknowledge(item)
                envelope = item.event
                payload_name = envelope.WhichOneof("payload")
                payload = getattr(envelope, payload_name) if payload_name else None
                return ShadowObservation(
                    consumer_id=self.manifest.consumer_id,
                    instrument_uid=envelope.instrument_uid,
                    feed=(payload_name or "").upper(),
                    logical_offset=item.logical_offset,
                    event_id_hex=bytes(envelope.event_id).hex(),
                    source_id=envelope.source_id,
                    revision=int(getattr(payload, "revision", 0)),
                )
