from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping

import yaml

from qdl.common.v1 import common_pb2
from qdl.domain.decimal import CanonicalDecimal
from qdl.domain.instrument import (
    AssetClass,
    InstrumentAlias,
    InstrumentIdentity,
    InstrumentRecord,
    InstrumentRegistry,
    ProductType,
)
from qdl.domain.quantity import quantity_unit_name
from qdl.marketdata.v2 import market_data_pb2
from qdl.query import (
    AccessPurpose,
    BarLifecycle,
    ContractMetadata,
    ConsumerGrade,
    CoverageStatus,
    DataProduct,
    DataRequirement,
    EntitlementGrant,
    EntitlementPolicy,
    FeedType,
    GapRecord,
    HistoryResult,
    InstrumentQuery,
    MarketDataItem,
    QualityMetadata,
    SourceMetadata,
    V2QueryService,
)
from qdl.replay import GapFreeHandoff
from qdl.stream import GrpcSnapshot
from qdl.transport import Cursor, SQLiteDurableSpool, StoredEvent
from qdl.transport.contracts import partition_key


_STREAM_BY_FEED = {FeedType.BAR: "md.canonical.v2.bar"}


def _interval_ns(interval: str) -> int:
    units = {
        "s": 1_000_000_000,
        "m": 60 * 1_000_000_000,
        "h": 60 * 60 * 1_000_000_000,
        "d": 24 * 60 * 60 * 1_000_000_000,
    }
    try:
        amount = int(interval[:-1])
        unit = units[interval[-1]]
    except (KeyError, ValueError, IndexError) as error:
        raise ValueError(f"unsupported canary bar interval: {interval}") from error
    if amount <= 0:
        raise ValueError("canary bar interval must be positive")
    return amount * unit


@dataclass(frozen=True, slots=True)
class CanaryReadPolicy:
    kind: str
    path: str
    params: Mapping[str, str]
    warmup_rows: int
    poll_rows: int
    poll_seconds: float
    timeout_seconds: float
    max_attempts: int
    max_backoff_seconds: float

    def __post_init__(self) -> None:
        if self.kind != "BINANCE_CRYPTO_OHLCV":
            raise ValueError(f"unsupported Phase 7.2 read policy: {self.kind}")
        if not self.path.startswith("/v1/") or ".." in self.path:
            raise ValueError("canary source path must be a bounded V1 route")
        if not 1 <= self.warmup_rows <= 1500 or not 1 <= self.poll_rows <= 100:
            raise ValueError("canary source row bounds are invalid")
        if min(
            self.poll_seconds,
            self.timeout_seconds,
            self.max_attempts,
            self.max_backoff_seconds,
        ) <= 0:
            raise ValueError("canary source retry and timing bounds must be positive")


@dataclass(frozen=True, slots=True)
class CanarySourceBinding:
    binding_id: str
    instrument: InstrumentRecord
    provider: str
    source_id: str
    source_role: str
    source_policy_id: str
    authoritative: bool
    adapter_version: str
    normalizer_version: str
    feed: FeedType
    interval: str
    read: CanaryReadPolicy
    stale_after_ms: int
    require_closed_bar: bool

    def __post_init__(self) -> None:
        required = (
            self.binding_id,
            self.provider,
            self.source_id,
            self.source_role,
            self.source_policy_id,
            self.adapter_version,
            self.normalizer_version,
            self.interval,
        )
        if not all(value.strip() for value in required):
            raise ValueError("canary binding identity and lineage fields are required")
        if self.feed is not FeedType.BAR:
            raise ValueError("Phase 7.2 reference bridge supports final bars only")
        if self.source_role not in {"PRIMARY", "SECONDARY", "REFERENCE", "BACKFILL"}:
            raise ValueError("canary source role is invalid")
        if self.stale_after_ms <= 0 or not self.require_closed_bar:
            raise ValueError("canary bar source must require closed bars and bounded freshness")

    @property
    def stream(self) -> str:
        return _STREAM_BY_FEED[self.feed]

    @property
    def partition_key(self) -> str:
        return partition_key(
            instrument_uid=self.instrument.instrument_uid,
            feed_type=self.feed.value,
            source_id=self.source_id,
        )

    @property
    def requirement_key(self) -> tuple[str, FeedType, str | None]:
        return self.instrument.instrument_uid, self.feed, self.interval


class CanarySourceCatalog:
    """Strict, immutable Phase 7.2 catalog; it never discovers provider symbols."""

    def __init__(
        self,
        *,
        bindings: tuple[CanarySourceBinding, ...],
        catalog_revision: int,
        source_policy_revision: int,
        authority_revision: int,
    ) -> None:
        if not bindings or min(
            catalog_revision, source_policy_revision, authority_revision
        ) < 1:
            raise ValueError("canary catalog requires bindings and positive revisions")
        keys = [item.requirement_key for item in bindings]
        ids = [item.binding_id for item in bindings]
        if len(keys) != len(set(keys)) or len(ids) != len(set(ids)):
            raise ValueError("canary source bindings must be unique")
        self.bindings = bindings
        self.catalog_revision = catalog_revision
        self.source_policy_revision = source_policy_revision
        self.authority_revision = authority_revision
        self._by_requirement = {item.requirement_key: item for item in bindings}
        self._by_envelope = {
            (item.instrument.instrument_uid, item.feed.value.lower(), item.interval): item
            for item in bindings
        }

    @classmethod
    def load(cls, path: str | Path) -> "CanarySourceCatalog":
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or raw.get("schema") != "qdl.phase7.2.source-bindings.v1":
            raise ValueError("unsupported Phase 7.2 source binding schema")
        if set(raw) != {
            "schema", "catalog_revision", "source_policy_revision",
            "authority_revision", "bindings",
        }:
            raise ValueError("canary source catalog contains unknown or missing fields")
        values = raw.get("bindings")
        if not isinstance(values, list) or not 1 <= len(values) <= 100:
            raise ValueError("canary source catalog requires 1..100 bindings")
        return cls(
            bindings=tuple(cls._binding(value) for value in values),
            catalog_revision=int(raw["catalog_revision"]),
            source_policy_revision=int(raw["source_policy_revision"]),
            authority_revision=int(raw["authority_revision"]),
        )

    @staticmethod
    def _binding(raw: Any) -> CanarySourceBinding:
        if not isinstance(raw, dict) or set(raw) != {
            "binding_id", "instrument", "feed", "interval", "source", "read", "quality",
        }:
            raise ValueError("canary source binding fields are incomplete or unknown")
        instrument_raw = raw["instrument"]
        source = raw["source"]
        read = raw["read"]
        quality = raw["quality"]
        if not all(isinstance(value, dict) for value in (instrument_raw, source, read, quality)):
            raise ValueError("canary source binding sections must be mappings")
        allowed_instrument = {
            "instrument_uid", "instrument_id", "venue", "market", "product_type",
            "canonical_symbol", "native_symbol", "asset_class", "base_asset",
            "quote_asset", "settlement_asset", "price_tick", "quantity_step",
            "contract_multiplier", "session_calendar_id",
        }
        if set(instrument_raw) != allowed_instrument:
            raise ValueError("canary instrument fields are incomplete or unknown")
        identity = InstrumentIdentity.create(
            venue=str(instrument_raw["venue"]),
            market=str(instrument_raw["market"]),
            product_type=ProductType(str(instrument_raw["product_type"]).upper()),
            canonical_symbol=str(instrument_raw["canonical_symbol"]),
        )
        if (
            identity.instrument_uid != str(instrument_raw["instrument_uid"])
            or identity.instrument_id != str(instrument_raw["instrument_id"]).upper()
        ):
            raise ValueError("canary instrument UID/ID is not deterministic")
        record = InstrumentRecord(
            identity=identity,
            metadata_revision=1,
            asset_class=AssetClass(str(instrument_raw["asset_class"]).upper()),
            native_symbol=str(instrument_raw["native_symbol"]).upper(),
            base_asset=str(instrument_raw["base_asset"]).upper(),
            quote_asset=str(instrument_raw["quote_asset"]).upper(),
            settlement_asset=str(instrument_raw["settlement_asset"]).upper(),
            price_tick=CanonicalDecimal.from_text(str(instrument_raw["price_tick"])),
            quantity_step=CanonicalDecimal.from_text(str(instrument_raw["quantity_step"])),
            contract_multiplier=CanonicalDecimal.from_text(
                str(instrument_raw["contract_multiplier"])
            ),
            session_calendar_id=str(instrument_raw["session_calendar_id"]),
        )
        if set(source) != {
            "provider", "source_id", "source_role", "source_policy_id", "authoritative",
            "adapter_version", "normalizer_version",
        }:
            raise ValueError("canary source lineage fields are incomplete or unknown")
        if set(read) != {
            "kind", "path", "params", "warmup_rows", "poll_rows", "poll_seconds",
            "timeout_seconds", "max_attempts", "max_backoff_seconds",
        }:
            raise ValueError("canary read policy fields are incomplete or unknown")
        if set(quality) != {"stale_after_ms", "require_closed_bar"}:
            raise ValueError("canary quality policy fields are incomplete or unknown")
        return CanarySourceBinding(
            binding_id=str(raw["binding_id"]),
            instrument=record,
            provider=str(source["provider"]),
            source_id=str(source["source_id"]),
            source_role=str(source["source_role"]).upper(),
            source_policy_id=str(source["source_policy_id"]),
            authoritative=bool(source["authoritative"]),
            adapter_version=str(source["adapter_version"]),
            normalizer_version=str(source["normalizer_version"]),
            feed=FeedType(str(raw["feed"]).upper()),
            interval=str(raw["interval"]),
            read=CanaryReadPolicy(
                kind=str(read["kind"]),
                path=str(read["path"]),
                params={str(key): str(value) for key, value in read["params"].items()},
                warmup_rows=int(read["warmup_rows"]),
                poll_rows=int(read["poll_rows"]),
                poll_seconds=float(read["poll_seconds"]),
                timeout_seconds=float(read["timeout_seconds"]),
                max_attempts=int(read["max_attempts"]),
                max_backoff_seconds=float(read["max_backoff_seconds"]),
            ),
            stale_after_ms=int(quality["stale_after_ms"]),
            require_closed_bar=bool(quality["require_closed_bar"]),
        )

    def binding_for(self, requirement: DataRequirement) -> CanarySourceBinding:
        try:
            binding = self._by_requirement[
                requirement.instrument_uid, requirement.feed, requirement.interval
            ]
        except KeyError as error:
            raise KeyError("requirement has no approved Phase 7.2 source binding") from error
        if binding.source_policy_id != requirement.source_policy_id:
            raise KeyError("requirement source policy is not bound to this source")
        return binding

    def binding_for_envelope(
        self, envelope: market_data_pb2.EventEnvelope
    ) -> CanarySourceBinding:
        payload_name = envelope.WhichOneof("payload")
        interval = envelope.bar.interval if payload_name == "bar" else ""
        try:
            binding = self._by_envelope[
                envelope.instrument_uid, payload_name, interval
            ]
        except KeyError as error:
            raise ValueError(
                "canonical event instrument/feed/interval is not in the beta catalog"
            ) from error
        if (
            envelope.instrument_id != binding.instrument.instrument_id
            or envelope.native_symbol != binding.instrument.native_symbol
            or envelope.source_id != binding.source_id
            or envelope.provider != binding.provider
            or payload_name != binding.feed.value.lower()
            or envelope.bar.interval != binding.interval
        ):
            raise ValueError("canonical event does not match its approved beta binding")
        return binding

    def instrument_registry(self) -> InstrumentRegistry:
        registry = InstrumentRegistry()
        for binding in self.bindings:
            record = binding.instrument
            registry.register(record, [InstrumentAlias(
                provider=binding.provider,
                market=record.identity.market,
                native_symbol=record.native_symbol,
                instrument_uid=record.instrument_uid,
                instrument_revision=record.metadata_revision,
                valid_from_ns=0,
            )])
        return registry

    def entitlements(self) -> EntitlementPolicy:
        source_ids = sorted({binding.source_id for binding in self.bindings})
        return EntitlementPolicy(tuple(
            EntitlementGrant(
                source_id=source_id,
                license_revision="internal-v1-readonly-canary-v1",
                purposes=frozenset({
                    AccessPurpose.INTERNAL_ALPHA,
                    AccessPurpose.INTERNAL_RESEARCH,
                }),
                products=frozenset({
                    DataProduct.CANONICAL_SNAPSHOT,
                    DataProduct.CANONICAL_HISTORY,
                }),
                valid_from_ns=0,
            )
            for source_id in source_ids
        ))


def _decimal_text(value) -> str:
    if value.source_text:
        parsed = CanonicalDecimal.from_text(value.source_text)
        coefficient = value.mantissa_text if value.WhichOneof("coefficient") == "mantissa_text" else value.mantissa
        if str(parsed.coefficient) != str(coefficient) or parsed.scale != value.scale:
            raise ValueError("canonical decimal source text does not match coefficient/scale")
        return value.source_text
    coefficient = value.mantissa_text if value.WhichOneof("coefficient") == "mantissa_text" else value.mantissa
    return format(CanonicalDecimal(coefficient, value.scale, str(coefficient)).as_decimal(), "f")


class SpoolCanonicalQueryBackend:
    """Read-only canonical query view over the same durable beta watermark as stream."""

    def __init__(
        self,
        spool: SQLiteDurableSpool,
        catalog: CanarySourceCatalog,
        *,
        schema_digest: str,
        config_revision: int = 1,
        clock_ns=time.time_ns,
    ) -> None:
        self.spool = spool
        self.catalog = catalog
        self.schema_digest = schema_digest
        self.config_revision = config_revision
        self._clock_ns = clock_ns

    def latest(self, requirement: DataRequirement) -> MarketDataItem | None:
        records = self._records(requirement)
        return self._items(requirement, records)[-1] if records else None

    def history(self, requirement: DataRequirement) -> HistoryResult | None:
        records = self._records(requirement)
        if not records:
            return None
        requested = requirement.warmup_limit or 1
        records = records[-requested:]
        items = self._items(requirement, records)
        last = records[-1]
        snapshot_id = hashlib.sha256(
            f"{last.cursor.stream}|{last.cursor.partition_key}|{last.cursor.offset}|"
            f"{last.event.event_id.hex()}".encode()
        ).hexdigest()
        return HistoryResult(
            items=items,
            coverage=(
                CoverageStatus.FULL
                if len(items) >= requested
                else CoverageStatus.PARTIAL
            ),
            snapshot_id=f"qdl-beta-{snapshot_id[:32]}",
            stream_cursor="CONSUMER_CURSOR_PENDING",
            watermark_offset=last.cursor.offset,
            data_as_of_ns=items[-1].observed_at_ns,
        )

    def feed_status(self, requirement: DataRequirement) -> QualityMetadata | None:
        item = self.latest(requirement)
        return item.quality if item else None

    def open_gaps(self):
        result = []
        for binding in self.catalog.bindings:
            result.extend(self._gaps(binding, self.spool.read(
                stream=binding.stream,
                partition_key=binding.partition_key,
                limit=10_000,
            )))
        return tuple(sorted(result, key=lambda item: (item.detected_at_ns, item.gap_id)))

    def stored_events(self, requirement: DataRequirement) -> tuple[StoredEvent, ...]:
        records = self._records(requirement)
        requested = requirement.warmup_limit or 1
        return records[-requested:]

    def _records(self, requirement: DataRequirement) -> tuple[StoredEvent, ...]:
        binding = self.catalog.binding_for(requirement)
        rows = self.spool.read(
            stream=binding.stream,
            partition_key=binding.partition_key,
            limit=10_000,
        )
        selected = []
        for row in rows:
            envelope = market_data_pb2.EventEnvelope.FromString(row.event.payload)
            self.catalog.binding_for_envelope(envelope)
            if envelope.bar.interval == requirement.interval:
                selected.append(row)
        return tuple(selected)

    def _items(
        self,
        requirement: DataRequirement,
        records: tuple[StoredEvent, ...],
    ) -> tuple[MarketDataItem, ...]:
        if not records:
            return ()
        binding = self.catalog.binding_for(requirement)
        envelopes = tuple(
            market_data_pb2.EventEnvelope.FromString(row.event.payload) for row in records
        )
        gaps = self._gaps(binding, records)
        latest_close_ns = envelopes[-1].bar.close_time_ns
        freshness_ms = max(0, (self._clock_ns() - latest_close_ns) // 1_000_000)
        stale = freshness_ms > binding.stale_after_ms
        quality = QualityMetadata(
            state="STALE" if stale else "LIVE",
            freshness_ms=int(freshness_ms),
            gap_open=bool(gaps),
            complete=not gaps,
            execution_eligible=False,
            policy_id=binding.source_policy_id,
            flags=("BACKFILLED", "OPEN_GAP") if gaps else ("BACKFILLED",),
        )
        return tuple(
            self._item(binding, row, envelope, quality)
            for row, envelope in zip(records, envelopes, strict=True)
        )

    def _gaps(
        self,
        binding: CanarySourceBinding,
        records: tuple[StoredEvent, ...],
    ) -> tuple[GapRecord, ...]:
        if len(records) < 2:
            return ()
        opens = sorted({
            int(market_data_pb2.EventEnvelope.FromString(row.event.payload).bar.open_time_ns)
            for row in records
        })
        expected_step = _interval_ns(binding.interval)
        detected_at_ns = self._clock_ns()
        gaps = []
        for previous, current in zip(opens, opens[1:], strict=False):
            expected = previous + expected_step
            if current == expected:
                continue
            digest = hashlib.sha256(
                f"{binding.binding_id}|{expected}|{current}".encode()
            ).hexdigest()[:24]
            gaps.append(GapRecord(
                gap_id=f"phase72-{digest}",
                instrument_uid=binding.instrument.instrument_uid,
                feed=binding.feed,
                source_id=binding.source_id,
                expected_sequence=str(expected),
                observed_sequence=str(current),
                detected_at_ns=detected_at_ns,
            ))
        return tuple(gaps)

    def _item(
        self,
        binding: CanarySourceBinding,
        stored: StoredEvent,
        envelope: market_data_pb2.EventEnvelope,
        quality: QualityMetadata,
    ) -> MarketDataItem:
        bar = envelope.bar
        lifecycle_name = market_data_pb2.BarLifecycle.Name(bar.lifecycle).removeprefix(
            "BAR_LIFECYCLE_"
        )
        origin_name = common_pb2.BarOrigin.Name(bar.origin).removeprefix("BAR_ORIGIN_")
        source_role = common_pb2.SourceRole.Name(envelope.source_role).removeprefix(
            "SOURCE_ROLE_"
        )
        return MarketDataItem(
            instrument_uid=envelope.instrument_uid,
            instrument_id=envelope.instrument_id,
            instrument_revision=int(envelope.instrument_revision),
            feed=FeedType.BAR,
            interval=bar.interval,
            observed_at_ns=int(envelope.source_event_time_ns),
            revision=int(bar.revision),
            payload={
                "open_time_ns": int(bar.open_time_ns),
                "close_time_ns": int(bar.close_time_ns),
                "open": _decimal_text(bar.open),
                "high": _decimal_text(bar.high),
                "low": _decimal_text(bar.low),
                "close": _decimal_text(bar.close),
                "volume": _decimal_text(bar.volume),
                "volume_unit": quantity_unit_name(bar.volume_unit),
                "base_volume": (
                    _decimal_text(bar.base_volume)
                    if bar.HasField("base_volume")
                    else None
                ),
                "quote_volume": (
                    _decimal_text(bar.quote_volume)
                    if bar.HasField("quote_volume")
                    else None
                ),
                "contract_volume": (
                    _decimal_text(bar.contract_volume)
                    if bar.HasField("contract_volume")
                    else None
                ),
                "trade_count": int(bar.trade_count),
                "origin": origin_name,
                "is_final": bool(bar.is_final),
            },
            source=SourceMetadata(
                venue=envelope.venue,
                provider=envelope.provider,
                source_id=envelope.source_id,
                source_role=source_role,
                authoritative=binding.authoritative,
            ),
            quality=quality,
            contract=ContractMetadata(
                schema_digest=self.schema_digest,
                contract_version="2.0.0-beta.1",
                normalizer_version=envelope.normalizer_version,
                adapter_version=envelope.adapter_version,
                instrument_catalog_revision=self.catalog.catalog_revision,
                source_policy_revision=self.catalog.source_policy_revision,
                authority_revision=self.catalog.authority_revision,
                config_revision=max(1, int(envelope.config_revision or self.config_revision)),
                correlation_id=envelope.correlation_id or stored.event.event_id.hex(),
            ),
            watermark_offset=stored.cursor.offset,
            bar_lifecycle=BarLifecycle(lifecycle_name),
            supersedes_event_id=(
                bytes(bar.supersedes_event_id).hex() if bar.HasField("supersedes_event_id") else None
            ),
        )


class ConsumerHandoffCursorIssuer:
    def __init__(
        self,
        handoff: GapFreeHandoff,
        catalog: CanarySourceCatalog,
        *,
        ttl_seconds: int,
    ) -> None:
        self.handoff = handoff
        self.catalog = catalog
        self.ttl_seconds = ttl_seconds

    def bind_item(
        self,
        requirement: DataRequirement,
        item: MarketDataItem,
        *,
        consumer_id: str,
    ) -> MarketDataItem:
        snapshot_id = self._snapshot_id(requirement, item.watermark_offset)
        token = self._issue(requirement, consumer_id, snapshot_id, item.watermark_offset)
        return replace(item, snapshot_id=snapshot_id, cursor=token)

    def bind_history(
        self,
        requirement: DataRequirement,
        history: HistoryResult,
        *,
        consumer_id: str,
    ) -> HistoryResult:
        token = self._issue(
            requirement,
            consumer_id,
            history.snapshot_id,
            history.watermark_offset,
        )
        return replace(
            history,
            stream_cursor=token,
            items=tuple(
                replace(item, snapshot_id=history.snapshot_id, cursor=token)
                for item in history.items
            ),
        )

    def _issue(
        self,
        requirement: DataRequirement,
        consumer_id: str,
        snapshot_id: str,
        offset: int,
    ) -> str:
        binding = self.catalog.binding_for(requirement)
        return self.handoff.issue(
            consumer_id=consumer_id,
            snapshot_id=snapshot_id,
            snapshot_watermark=Cursor(binding.stream, binding.partition_key, offset),
            ttl_seconds=self.ttl_seconds,
        ).token

    @staticmethod
    def _snapshot_id(requirement: DataRequirement, offset: int) -> str:
        digest = hashlib.sha256(
            f"{requirement.instrument_uid}|{requirement.feed.value}|"
            f"{requirement.interval or ''}|{offset}".encode()
        ).hexdigest()
        return f"qdl-beta-{digest[:32]}"


class CanaryGrpcSnapshotLoader:
    def __init__(
        self,
        *,
        service: V2QueryService,
        backend: SpoolCanonicalQueryBackend,
        issuer: ConsumerHandoffCursorIssuer,
    ) -> None:
        self.service = service
        self.backend = backend
        self.issuer = issuer

    def load(self, requirement: DataRequirement, *, consumer_id: str) -> GrpcSnapshot:
        purpose = {
            ConsumerGrade.ALPHA: AccessPurpose.INTERNAL_ALPHA,
            ConsumerGrade.RESEARCH: AccessPurpose.INTERNAL_RESEARCH,
            ConsumerGrade.EXECUTION: AccessPurpose.INTERNAL_EXECUTION,
        }[requirement.consumer_grade]
        result = self.service.warmup(requirement, purpose=purpose)
        history = self.issuer.bind_history(
            requirement, result.history, consumer_id=consumer_id
        )
        events = tuple(
            market_data_pb2.EventEnvelope.FromString(item.event.payload)
            for item in self.backend.stored_events(requirement)
        )
        return GrpcSnapshot(
            request_id=result.request_id,
            snapshot_id=history.snapshot_id,
            stream_cursor=history.stream_cursor,
            data_as_of_ns=history.data_as_of_ns,
            watermark_offset=history.watermark_offset,
            events=events,
        )


def build_canary_query_stack(
    *,
    spool: SQLiteDurableSpool,
    catalog: CanarySourceCatalog,
    schema_digest: str,
    handoff: GapFreeHandoff,
    cursor_ttl_seconds: int,
) -> tuple[V2QueryService, SpoolCanonicalQueryBackend, ConsumerHandoffCursorIssuer]:
    backend = SpoolCanonicalQueryBackend(
        spool, catalog, schema_digest=schema_digest
    )
    service = V2QueryService(
        instruments=InstrumentQuery(catalog.instrument_registry()),
        backend=backend,
        entitlements=catalog.entitlements(),
    )
    issuer = ConsumerHandoffCursorIssuer(
        handoff, catalog, ttl_seconds=cursor_ttl_seconds
    )
    return service, backend, issuer
