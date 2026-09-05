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
STABLE_CORE_DEDUP_CAPACITY = 100_000
V2_REALTIME_RAW_TOPIC = "md.raw.realtime.v2"
SHARED_REALTIME_CORE_GROUP_ID = "qdl-v2-realtime-core-v2"
SHARED_REALTIME_CORE_ID_PREFIX = "qdl-v2-realtime-core"
_SHARED_AUTHORITY_MODES = frozenset({"RUST_SHADOW", "RUST_PRIMARY"})
_PROVIDER_KINDS = {
    ("BINANCE", "TRADE"): frozenset({"binance_usdm_trade", "binance_spot_trade"}),
    ("BINANCE", "QUOTE"): frozenset({"binance_usdm_bbo", "binance_spot_bbo"}),
    ("BINANCE", "BAR"): frozenset({
        "binance_usdm_bar",
        "binance_spot_bar",
        "binance_usdm_rest_bar",
        "binance_spot_rest_bar",
    }),
    ("BINANCE", "BOOK_SNAPSHOT"): frozenset({"binance_usdm_book", "binance_spot_book"}),
    ("BINANCE", "BOOK_DELTA"): frozenset({"binance_usdm_book", "binance_spot_book"}),
    ("BINANCE", "MARK_INDEX_PRICE"): frozenset({"binance_usdm_mark_index"}),
    ("OKX", "TRADE"): frozenset({"okx_trade"}),
    ("OKX", "QUOTE"): frozenset({"okx_bbo"}),
    ("OKX", "BAR"): frozenset({"okx_bar"}),
    ("OKX", "BOOK_SNAPSHOT"): frozenset({"okx_book"}),
    ("OKX", "BOOK_DELTA"): frozenset({"okx_book"}),
    ("OKX", "MARK_INDEX_PRICE"): frozenset({"okx_mark_index"}),
    ("HNX", "TRADE"): frozenset({"dnse_trade"}),
    ("HNX", "BAR"): frozenset({"dnse_bar"}),
    ("HOSE", "TRADE"): frozenset({"dnse_trade"}),
    ("HOSE", "BAR"): frozenset({"dnse_bar"}),
}


@dataclass(frozen=True, slots=True)
class StableL2Acquisition:
    """One physical L2 edge shared by logical snapshot/delta bindings.

    The catalog deliberately keeps the two public products distinct.  This
    small runtime record is the complementary physical identity: it lets the
    existing venue ingestor and Rust core own exactly one subscription/state
    machine for that book.
    """

    provider_protocol: str
    depth_per_side: int
    rest_snapshot_url: str | None
    snapshot_refresh_seconds: int | None
    materialized_snapshot_interval_ms: int | None = None

    def validate(self, source: StableSourceBinding, acquisition: "StableAcquisitionBinding") -> None:
        if source.feed not in {FeedType.BOOK_SNAPSHOT, FeedType.BOOK_DELTA}:
            raise ValueError("L2 acquisition can only back book bindings")
        if not 1 <= self.depth_per_side <= 10_000:
            raise ValueError("L2 depth is outside bounded range")
        if acquisition.mode != "RUST_NATIVE" or acquisition.sequence_policy != "CONTIGUOUS":
            raise ValueError("L2 acquisition requires lossless Rust-native continuity")
        if self.materialized_snapshot_interval_ms is not None and not (
            100 <= self.materialized_snapshot_interval_ms <= 60_000
        ):
            raise ValueError("L2 materialized snapshot cadence is outside bounded range")
        if self.provider_protocol == "BINANCE_DIFF_DEPTH":
            if (
                source.instrument.identity.venue != "BINANCE"
                or source.instrument.identity.market not in {"USDM", "SPOT"}
                or acquisition.provider_kind
                != f"binance_{source.instrument.identity.market.lower()}_book"
                or acquisition.native_channel
                != f"{source.instrument.native_symbol.lower()}@depth@100ms"
                or self.rest_snapshot_url
                != (
                    "https://fapi.binance.com/fapi/v1/depth"
                    if source.instrument.identity.market == "USDM"
                    else "https://api.binance.com/api/v3/depth"
                )
                or self.snapshot_refresh_seconds is None
                or not 5 <= self.snapshot_refresh_seconds <= 300
            ):
                raise ValueError("Binance L2 acquisition differs from documented provider edge")
            return
        if self.provider_protocol == "OKX_PUBLIC_BOOKS":
            if (
                source.instrument.identity.venue != "OKX"
                or source.instrument.identity.market not in {"SWAP", "FUTURES", "SPOT"}
                or acquisition.provider_kind != "okx_book"
                or acquisition.native_channel != "books"
                or self.rest_snapshot_url is not None
                or self.snapshot_refresh_seconds is None
                or not 5 <= self.snapshot_refresh_seconds <= 300
            ):
                raise ValueError("OKX L2 acquisition differs from documented provider edge")
            return
        raise ValueError("L2 provider protocol is not certified")

    def core_mapping(self) -> dict[str, object]:
        result: dict[str, object] = {
            "provider_protocol": self.provider_protocol,
            "depth_per_side": self.depth_per_side,
            "snapshot_refresh_seconds": self.snapshot_refresh_seconds,
        }
        if self.materialized_snapshot_interval_ms is not None:
            result["materialized_snapshot_interval_ms"] = (
                self.materialized_snapshot_interval_ms
            )
        return result


@dataclass(frozen=True, slots=True)
class StableMarkIndexAcquisition:
    """One logical execution mark/index view, backed by provider-native frames."""

    provider_protocol: str
    index_native_symbol: str | None

    def validate(
        self,
        source: StableSourceBinding,
        acquisition: "StableAcquisitionBinding",
    ) -> None:
        identity = source.instrument.identity
        if (
            source.feed is not FeedType.MARK_INDEX_PRICE
            or acquisition.mode != "RUST_NATIVE"
            or acquisition.sequence_policy != "NONE"
        ):
            raise ValueError("MARK_INDEX acquisition requires native replace-only semantics")
        if self.provider_protocol == "BINANCE_MARK_PRICE":
            if (
                identity.venue != "BINANCE"
                or identity.market != "USDM"
                or acquisition.provider_kind != "binance_usdm_mark_index"
                or acquisition.native_channel
                != f"{source.instrument.native_symbol.lower()}@markPrice@1s"
                or self.index_native_symbol is not None
            ):
                raise ValueError("Binance MARK_INDEX acquisition differs from provider protocol")
            return
        if self.provider_protocol == "OKX_MARK_INDEX":
            index_symbol = (self.index_native_symbol or "").strip().upper()
            if (
                identity.venue != "OKX"
                or identity.market != "SWAP"
                or acquisition.provider_kind != "okx_mark_index"
                or acquisition.native_channel != "mark-price"
                or not index_symbol
                or index_symbol == source.instrument.native_symbol
            ):
                raise ValueError("OKX MARK_INDEX acquisition differs from provider protocol")
            return
        raise ValueError("MARK_INDEX provider protocol is not certified")

    def component_entries(
        self,
        source: StableSourceBinding,
        acquisition: "StableAcquisitionBinding",
    ) -> tuple[tuple[str, str, str, str], ...]:
        """Return physical symbol/channel/provider-kind/component entries.

        The index symbol is explicit in the signed acquisition plan.  We never
        infer it from a swap symbol at runtime, avoiding a silent USD/USDT or
        venue mapping error.
        """
        self.validate(source, acquisition)
        if self.provider_protocol == "BINANCE_MARK_PRICE":
            return ((
                source.instrument.native_symbol,
                acquisition.native_channel,
                "binance_usdm_mark_index",
                "BOTH",
            ),)
        return (
            (
                source.instrument.native_symbol,
                "mark-price",
                "okx_mark_price",
                "MARK",
            ),
            (
                str(self.index_native_symbol),
                "index-tickers",
                "okx_index_price",
                "INDEX",
            ),
        )


@dataclass(frozen=True, slots=True)
class StablePhysicalEntry:
    """A physical provider subscription projected into one logical binding."""

    source: StableSourceBinding
    acquisition: "StableAcquisitionBinding"
    physical_native_symbol: str
    physical_native_channel: str
    provider_kind: str
    mark_index_component: str | None = None

    def __iter__(self):
        """Keep the former private `(source, acquisition)` projection usable.

        Existing callers only need logical ownership. New native mark/index
        callers use the explicit physical fields above instead of inferring a
        second instrument from a tuple shape.
        """

        yield self.source
        yield self.acquisition


def validate_shared_authority_record(authority: Mapping[str, Any]) -> None:
    """Validate the one generated authority record used by every V2 role.

    The record is intentionally non-secret and mounted read-only.  Runtime
    environment values may describe the expected record, but they cannot grant
    authority independently from this bounded, generated contract.
    """
    digest = str(authority.get("candidate_image_digest", ""))
    digest_fields = (
        "capability_manifest_digest",
        "contract_digest",
        "partition_plan_digest",
    )
    revision = authority.get("revision")
    effective_at_ns = authority.get("effective_at_ns")
    if (
        authority.get("schema") != "qdl.authority-record.v1"
        or not isinstance(authority.get("slice_id"), str)
        or not authority["slice_id"].strip()
        or not isinstance(revision, int)
        or isinstance(revision, bool)
        or revision < 1
        or authority.get("mode") not in _SHARED_AUTHORITY_MODES
        or authority.get("public_write_allowed") is not False
        or authority.get("legacy_write_allowed") is not False
        or not digest.startswith("sha256:")
        or len(digest) != 71
        or any(value not in "0123456789abcdef" for value in digest.removeprefix("sha256:"))
        or any(
            len(str(authority.get(field, ""))) != 64
            or any(value not in "0123456789abcdef" for value in str(authority.get(field, "")))
            for field in digest_fields
        )
        or not isinstance(authority.get("approved_by"), str)
        or not authority["approved_by"].strip()
        or not isinstance(effective_at_ns, int)
        or isinstance(effective_at_ns, bool)
        or effective_at_ns <= 0
    ):
        raise ValueError("stable authority is not an isolated shared Rust authority record")


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
    l2: StableL2Acquisition | None = None
    mark_index: StableMarkIndexAcquisition | None = None
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
        if self.l2 is not None and self.mark_index is not None:
            raise ValueError("stable acquisition cannot combine L2 and MARK_INDEX")
        if self.l2 is not None:
            self.l2.validate(source, self)
        elif source.feed in {FeedType.BOOK_SNAPSHOT, FeedType.BOOK_DELTA}:
            raise ValueError("book binding requires an explicit L2 acquisition")
        if self.mark_index is not None:
            self.mark_index.validate(source, self)
        elif source.feed is FeedType.MARK_INDEX_PRICE:
            raise ValueError("MARK_INDEX binding requires an explicit acquisition contract")
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

    def l2_mapping(self) -> dict[str, object] | None:
        if self.l2 is None:
            return None
        return {
            "provider_protocol": self.l2.provider_protocol,
            "depth_per_side": self.l2.depth_per_side,
            "rest_snapshot_url": self.l2.rest_snapshot_url,
            "snapshot_refresh_seconds": self.l2.snapshot_refresh_seconds,
        }

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
                set(value) - required - {"enabled", "l2", "mark_index"}
            ):
                raise ValueError("stable acquisition binding fields are incomplete or unknown")
            enabled = value.get("enabled", True)
            if not isinstance(enabled, bool):
                raise ValueError("stable acquisition 'enabled' must be a boolean")
            l2_raw = value.get("l2")
            if l2_raw is not None:
                required_l2 = {
                    "provider_protocol", "depth_per_side", "rest_snapshot_url",
                    "snapshot_refresh_seconds",
                }
                optional_l2 = {"materialized_snapshot_interval_ms"}
                if (
                    not isinstance(l2_raw, dict)
                    or not required_l2 <= set(l2_raw)
                    or set(l2_raw) - required_l2 - optional_l2
                ):
                    raise ValueError("stable L2 acquisition fields are incomplete or unknown")
                l2 = StableL2Acquisition(
                    provider_protocol=str(l2_raw["provider_protocol"]).upper(),
                    depth_per_side=int(l2_raw["depth_per_side"]),
                    rest_snapshot_url=(
                        str(l2_raw["rest_snapshot_url"])
                        if l2_raw["rest_snapshot_url"] is not None
                        else None
                    ),
                    snapshot_refresh_seconds=(
                        int(l2_raw["snapshot_refresh_seconds"])
                        if l2_raw["snapshot_refresh_seconds"] is not None
                        else None
                    ),
                    materialized_snapshot_interval_ms=(
                        int(l2_raw["materialized_snapshot_interval_ms"])
                        if l2_raw.get("materialized_snapshot_interval_ms") is not None
                        else None
                    ),
                )
            else:
                l2 = None
            mark_index_raw = value.get("mark_index")
            if mark_index_raw is not None:
                required_mark_index = {"provider_protocol", "index_native_symbol"}
                if (
                    not isinstance(mark_index_raw, dict)
                    or set(mark_index_raw) != required_mark_index
                    or (
                        mark_index_raw["index_native_symbol"] is not None
                        and not isinstance(mark_index_raw["index_native_symbol"], str)
                    )
                ):
                    raise ValueError(
                        "stable MARK_INDEX acquisition fields are incomplete or unknown"
                    )
                mark_index = StableMarkIndexAcquisition(
                    provider_protocol=str(mark_index_raw["provider_protocol"]).upper(),
                    index_native_symbol=(
                        str(mark_index_raw["index_native_symbol"]).upper()
                        if mark_index_raw["index_native_symbol"] is not None
                        else None
                    ),
                )
            else:
                mark_index = None
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
                l2=l2,
                mark_index=mark_index,
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
        result._validate_l2_aliases(source_by_id)
        if result.canonical_topic != catalog.canonical_stream:
            raise ValueError("stable acquisition canonical topic differs from catalog")
        return result

    def _validate_l2_aliases(
        self, source_by_id: Mapping[str, StableSourceBinding]
    ) -> None:
        """Require a complete, equivalent snapshot/delta alias pair.

        A source identifier is intentionally shared only by the two logical
        book products.  Any partial/mismatched pair would let one public feed
        claim a different physical state machine, so reject it before a role
        can be configured.
        """
        grouped: dict[str, list[StableAcquisitionBinding]] = {}
        for item in self.bindings:
            if item.l2 is not None:
                grouped.setdefault(source_by_id[item.binding_id].source_id, []).append(item)
        for source_id, values in grouped.items():
            sources = [source_by_id[item.binding_id] for item in values]
            if (
                len(values) != 2
                or {item.feed for item in sources}
                != {FeedType.BOOK_SNAPSHOT, FeedType.BOOK_DELTA}
                or len({item.runtime for item in values}) != 1
                or len({item.provider_kind for item in values}) != 1
                or len({item.native_channel for item in values}) != 1
                or len({item.l2 for item in values}) != 1
                or len({item.instrument.instrument_uid for item in sources}) != 1
            ):
                raise ValueError(
                    "stable L2 source aliases must be one complete equivalent book pair: "
                    + source_id
                )

    @staticmethod
    def _runtime_lane(
        acquisition: StableAcquisitionBinding,
        source: StableSourceBinding,
    ) -> tuple[str, str]:
        """Map provider metadata to an existing shared role, never a symbol role."""
        if acquisition.runtime == "OKX" and source.instrument.identity.market in {"SWAP", "FUTURES"}:
            # OKX public books/trades use the same documented public socket.
            # Keep the long-lived role name stable while its binding set grows.
            return "OKX", "SWAP"
        return acquisition.runtime, source.instrument.identity.market

    def _physical_entries(
        self,
        *,
        source_by_id: Mapping[str, StableSourceBinding],
        selected_ids: frozenset[str],
    ) -> tuple[StablePhysicalEntry, ...]:
        """Project logical bindings into shared physical provider inputs.

        L2 coalesces two logical public products into one verified state
        machine. MARK_INDEX keeps one logical public product, but OKX exposes
        its mark and index values on two physical channels. Both cases remain
        inside the existing per-venue shared role.
        """
        aliases_by_source: dict[str, frozenset[str]] = {}
        for item in self.bindings:
            if item.l2 is not None:
                source_id = source_by_id[item.binding_id].source_id
                aliases_by_source[source_id] = frozenset(
                    value.binding_id
                    for value in self.bindings
                    if value.l2 is not None
                    and source_by_id[value.binding_id].source_id == source_id
                )
        result: list[StablePhysicalEntry] = []
        seen: set[tuple[str, str]] = set()
        for binding_id in sorted(selected_ids):
            source = source_by_id[binding_id]
            acquisition = next(item for item in self.bindings if item.binding_id == binding_id)
            if acquisition.l2 is None and acquisition.mark_index is None:
                identity = ("binding", binding_id)
            elif acquisition.l2 is not None:
                aliases = aliases_by_source[source.source_id]
                if not aliases.issubset(selected_ids):
                    raise ValueError(
                        "stable L2 runtime selection must include both snapshot/delta aliases"
                    )
                identity = ("l2", source.source_id)
            else:
                identity = ("mark-index", source.source_id)
            if identity not in seen:
                seen.add(identity)
                if acquisition.mark_index is None:
                    result.append(StablePhysicalEntry(
                        source=source,
                        acquisition=acquisition,
                        physical_native_symbol=source.instrument.native_symbol,
                        physical_native_channel=acquisition.native_channel,
                        provider_kind=acquisition.provider_kind,
                    ))
                else:
                    for physical_symbol, physical_channel, provider_kind, component in (
                        acquisition.mark_index.component_entries(source, acquisition)
                    ):
                        result.append(StablePhysicalEntry(
                            source=source,
                            acquisition=acquisition,
                            physical_native_symbol=physical_symbol,
                            physical_native_channel=physical_channel,
                            provider_kind=provider_kind,
                            mark_index_component=component,
                        ))
        return tuple(result)

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
        for entry in self._physical_entries(
            source_by_id=source_by_id, selected_ids=selected_ids
        ):
            source = entry.source
            acquisition = entry.acquisition
            identity = source.instrument.identity
            item = {
                "provider": source.provider,
                "venue": identity.venue,
                "market": identity.market,
                "product_type": identity.product_type.value,
                "native_symbol": source.instrument.native_symbol,
                "native_channel": acquisition.native_channel,
                "provider_kind": entry.provider_kind,
                "instrument_uid": source.instrument.instrument_uid,
                "instrument_id": source.instrument.instrument_id,
                "instrument_revision": source.instrument.metadata_revision,
                "instrument_catalog_revision": catalog.catalog_revision,
                "source_id": source.source_id,
                "source_role": source.source_role,
                "normalizer_version": source.normalizer_version,
                "require_final_bar": source.require_final_bar,
                "sequence_policy": acquisition.sequence_policy,
            }
            if acquisition.l2 is not None:
                item["l2"] = acquisition.l2.core_mapping()
            if entry.mark_index_component is not None:
                item["physical_native_symbol"] = entry.physical_native_symbol
                item["physical_native_channel"] = entry.physical_native_channel
                item["mark_index"] = {"component": entry.mark_index_component}
            bindings.append(item)
        return {
            "core": {
                "canonical_stream": self.canonical_topic,
                "quarantine_stream": self.quarantine_topic,
                "allow_test_provenance": False,
                # Kafka transactions atomically commit output and raw offsets;
                # this is a bounded in-process replay guard, not durable state.
                "dedup_capacity": STABLE_CORE_DEDUP_CAPACITY,
                "bindings": bindings,
            },
            "raw_topics": [self.raw_topic],
            # A dedicated V2 ingress topic has no legacy producer traffic.
            # The Rust core must therefore quarantine a bad envelope instead
            # of quietly skipping it and masking a topology/config error.
            "strict_subscription_scope": self.raw_topic == V2_REALTIME_RAW_TOPIC,
            "authority": dict(authority),
            "shard_id": f"{SHARED_REALTIME_CORE_ID_PREFIX}-{worker_index:03d}",
            "transactional_id": f"{SHARED_REALTIME_CORE_ID_PREFIX}-{worker_index:03d}",
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
        grouped: dict[tuple[str, str], list[StablePhysicalEntry]] = {}
        for entry in self._physical_entries(
            source_by_id=source_by_id, selected_ids=frozenset(selected_ids)
        ):
            source = entry.source
            acquisition = entry.acquisition
            if acquisition.enabled and acquisition.mode == "RUST_NATIVE":
                grouped.setdefault(self._runtime_lane(acquisition, source), []).append(entry)
        result = {}
        for (runtime, market), values in sorted(grouped.items()):
            first = values[0].acquisition
            bindings = []
            for entry in sorted(
                values,
                key=lambda item: (
                    item.acquisition.binding_id,
                    item.physical_native_channel,
                    item.physical_native_symbol,
                ),
            ):
                source = entry.source
                acquisition = entry.acquisition
                identity = source.instrument.identity
                binding = {
                    "provider": source.provider,
                    "venue": identity.venue,
                    "market": identity.market,
                    "product_type": identity.product_type.value,
                    "native_symbol": entry.physical_native_symbol,
                    "native_channel": entry.physical_native_channel,
                    "subscription_id": source.source_id,
                    "adapter_version": source.adapter_version,
                    "instrument_catalog_revision": catalog.catalog_revision,
                    "feed": (
                        "BOOK"
                        if acquisition.l2 is not None
                        else (
                            "MARK_INDEX"
                            if entry.mark_index_component is not None
                            else source.feed.value
                        )
                    ),
                    "delivery_class": (
                        "LATEST_STATE"
                        if (
                            source.feed is FeedType.QUOTE
                            or entry.mark_index_component is not None
                        ) and acquisition.l2 is None
                        else "LOSSLESS"
                    ),
                }
                if acquisition.l2 is not None:
                    binding["l2"] = acquisition.l2_mapping()
                if entry.mark_index_component is not None:
                    binding["mark_index_target"] = source.instrument.instrument_uid
                bindings.append(binding)
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
                "session_liveness_dir": (
                    f"/var/lib/qdl-stable/runtime/session-liveness/{key}"
                ),
                "session_liveness_write_interval_ms": 1000,
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
        validate_shared_authority_record(authority)


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
    mode: str = "RUST_SHADOW",
    revision: int = 1,
    slice_id: str | None = None,
    approved_by: str = "phase-b-isolated-stable-candidate",
) -> dict[str, Any]:
    digest = rust_image_digest.removeprefix("sha256:")
    if len(digest) != 64 or any(value not in "0123456789abcdef" for value in digest):
        raise ValueError("stable Rust image digest must be SHA-256")
    if effective_at_ns <= 0:
        raise ValueError("stable authority effective time must be positive")
    if mode not in _SHARED_AUTHORITY_MODES:
        raise ValueError("stable authority mode is unsupported")
    if revision < 1:
        raise ValueError("stable authority revision must be positive")
    resolved_slice_id = (
        slice_id
        if slice_id is not None
        else "qdl-v2-stable-multivenue-shadow"
    )
    if not resolved_slice_id or not approved_by:
        raise ValueError("stable authority identity is incomplete")
    return {
        "schema": "qdl.authority-record.v1",
        "slice_id": resolved_slice_id,
        "revision": revision,
        "mode": mode,
        "candidate_image_digest": f"sha256:{digest}",
        "capability_manifest_digest": hashlib.sha256(capability_manifest.read_bytes()).hexdigest(),
        "contract_digest": hashlib.sha256(contract.read_bytes()).hexdigest(),
        "partition_plan_digest": hashlib.sha256(partition_plan).hexdigest(),
        "public_write_allowed": False,
        "legacy_write_allowed": False,
        "approved_by": approved_by,
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
