#!/usr/bin/env python3
"""Bounded, authentic-provider certification for the C3.6 V2 product surface.

This verifier is intentionally a source-gate tool, not a runtime component. It
uses the existing V2 query/reference and L2 replay contracts in memory, reads
only documented public Binance/OKX provider endpoints, and retains no raw
provider body. Its default mode neither creates leases nor writes Kafka,
Redis, SQLite, V1, or V2 runtime state. The explicit C3.6 runtime mode writes
only its named Rust-admission coordination state; it never writes market data,
orders, or durable provider payloads.
"""
from __future__ import annotations

import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import resource
import tempfile
import time
from typing import Any, Iterable, Mapping, Sequence, TypeVar

from qdl.adapters.binance_usdm import parse_exchange_info
from qdl.adapters.okx.instruments import parse_public_instrument
from qdl.demand import DemandPurpose
from qdl.demand.liquid_crypto import (
    LiquidCryptoFeaturePolicy,
    LiquidCryptoFeatureSet,
    build_l2_feature_requirements,
    select_liquid_crypto_feature_set,
)
from qdl.domain.instrument import InstrumentRecord, ProductType
from qdl.query import (
    AccessPurpose,
    BatchRequirement,
    ConsumerGrade,
    DataRequirement,
    FeedType,
    RecoveryPolicy,
)
from qdl.query.reference import ReferenceBatchRequirement, ReferenceDataRequirement
from qdl.reference import BasisSeries, LongShortKind, ReferenceProduct, ReferenceStatus
from qdl.admission import ProviderAdmissionRuntime, RustHttpProviderAdmission
from qdl.runtime.stable_catalog import StableSourceBinding, StableSourceCatalog
from qdl.universe import (
    TopVolumeUniverse,
    TopVolumeUniversePolicy,
    resolve_binance_usdm_top_volume,
    resolve_okx_swap_top_volume,
)
from qdl.warmup import WarmupSpecification
from scripts.phase113_universal_warmup_reference_admission import _service
from scripts.phase114_l2_real_provider_capture import run as run_l2_capture
from scripts.refresh_top_volume_universes import (
    BINANCE_EXCHANGE_INFO,
    BINANCE_TICKERS,
    OKX_INSTRUMENTS,
    OKX_TICKERS,
    _get_json,
    _okx_data,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = ROOT / "config/v2/c36-liquid-crypto-feature-policy.yaml"
OKX_FUTURES = "https://www.okx.com/api/v5/public/instruments?instType=FUTURES"
_SCHEMA = "qdl.phasec36.real-provider-certification.v1"
_TOP_N = 350
_MAX_BATCH_ITEMS = 100
_REFERENCE_CERTIFICATION_BATCH_ITEMS = 12
_HOUR_MS = 3_600_000
_DAY_MS = 86_400_000
_FUNDING_MS = 8 * _HOUR_MS
_T = TypeVar("_T")


class CertificationError(RuntimeError):
    """An authentic provider result cannot certify the declared V2 product."""


@dataclass(frozen=True, slots=True)
class ProviderPayloads:
    binance_exchange_info: Mapping[str, Any]
    binance_tickers: tuple[Mapping[str, Any], ...]
    okx_swaps: tuple[Mapping[str, Any], ...]
    okx_swap_tickers: tuple[Mapping[str, Any], ...]
    okx_futures: tuple[Mapping[str, Any], ...]

    @property
    def metadata_sha256(self) -> dict[str, str]:
        return {
            "BINANCE:USDM": _digest(self.binance_exchange_info),
            "OKX:SWAP": _digest(self.okx_swaps),
            "OKX:FUTURES": _digest(self.okx_futures),
        }


@dataclass(frozen=True, slots=True)
class CertificationInput:
    binance_universe: TopVolumeUniverse
    okx_universe: TopVolumeUniverse
    warmup_records: tuple[InstrumentRecord, ...]
    feature_set: LiquidCryptoFeatureSet
    metadata_sha256: dict[str, str]
    inventory_sha256: str


@dataclass(frozen=True, slots=True)
class _ReferenceWork:
    requirement: ReferenceDataRequirement
    expected: str


def _runtime_native_basis_admission(
    *,
    required: bool,
) -> ProviderAdmissionRuntime | None:
    """Create the private Rust relay only for an explicit runtime certificate."""

    if not required:
        return None
    base_url = os.getenv("QDL_STABLE_PROVIDER_ADMISSION_URL")
    secret = os.getenv("QDL_STABLE_INTERNAL_INGEST_SECRET", "").encode("utf-8")
    if base_url != "http://rust_core:8300" or len(secret) < 32:
        raise CertificationError(
            "Rust provider admission certificate requires the sealed private query binding"
        )
    try:
        return RustHttpProviderAdmission(base_url=base_url, secret=secret)
    except ValueError as error:
        raise CertificationError("Rust provider admission binding is invalid") from error


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _chunks(
    values: Sequence[_T], size: int = _MAX_BATCH_ITEMS
) -> tuple[tuple[_T, ...], ...]:
    if not 1 <= size <= _MAX_BATCH_ITEMS:
        raise ValueError("C3.6 batch size is outside the V2 bound")
    return tuple(tuple(values[index : index + size]) for index in range(0, len(values), size))


def _require_object(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CertificationError(f"{label} provider response is not an object")
    return value


def _require_rows(value: Iterable[object], label: str) -> tuple[Mapping[str, Any], ...]:
    rows = tuple(value)
    if not rows or not all(isinstance(row, Mapping) for row in rows):
        raise CertificationError(f"{label} provider response has no valid rows")
    return tuple(row for row in rows if isinstance(row, Mapping))


def fetch_provider_payloads(*, timeout_seconds: float, max_attempts: int) -> ProviderPayloads:
    """Fetch the five bounded public discovery snapshots concurrently in memory."""

    endpoints = (
        ("binance_exchange_info", BINANCE_EXCHANGE_INFO),
        ("binance_tickers", BINANCE_TICKERS),
        ("okx_swaps", OKX_INSTRUMENTS),
        ("okx_swap_tickers", OKX_TICKERS),
        ("okx_futures", OKX_FUTURES),
    )
    with ThreadPoolExecutor(max_workers=len(endpoints), thread_name_prefix="qdl-c36") as executor:
        requests = {
            name: executor.submit(
                _get_json,
                endpoint,
                timeout_seconds=timeout_seconds,
                max_attempts=max_attempts,
            )
            for name, endpoint in endpoints
        }
        values = {name: future.result() for name, future in requests.items()}
    exchange_info = _require_object(values["binance_exchange_info"], "Binance exchangeInfo")
    tickers = _require_rows(values["binance_tickers"], "Binance tickers")
    return ProviderPayloads(
        binance_exchange_info=exchange_info,
        binance_tickers=tickers,
        okx_swaps=_require_rows(_okx_data(values["okx_swaps"], "swaps"), "OKX swaps"),
        okx_swap_tickers=_require_rows(
            _okx_data(values["okx_swap_tickers"], "swap tickers"),
            "OKX swap tickers",
        ),
        okx_futures=_require_rows(_okx_data(values["okx_futures"], "futures"), "OKX futures"),
    )


def _deduplicate(records: Iterable[InstrumentRecord]) -> tuple[InstrumentRecord, ...]:
    by_uid: dict[str, InstrumentRecord] = {}
    for record in records:
        previous = by_uid.setdefault(record.instrument_uid, record)
        if previous != record:
            raise CertificationError("provider metadata contains conflicting canonical identity")
    return tuple(sorted(by_uid.values(), key=lambda item: item.instrument_id))


def _select_universe_records(
    records: Iterable[InstrumentRecord],
    *,
    universe: TopVolumeUniverse,
    venue: str,
    market: str,
) -> tuple[InstrumentRecord, ...]:
    expected = {item.native_symbol for item in universe.members}
    selected = [
        record
        for record in records
        if record.identity.venue == venue
        and record.identity.market == market
        and record.identity.product_type is ProductType.PERPETUAL
        and record.native_symbol in expected
    ]
    by_symbol = {record.native_symbol: record for record in selected}
    if len(by_symbol) != len(selected) or set(by_symbol) != expected:
        missing = sorted(expected - set(by_symbol))
        raise CertificationError(
            f"{venue}/{market} top-volume membership has missing or duplicate metadata: "
            + ",".join(missing[:8])
        )
    return tuple(by_symbol[symbol] for symbol in sorted(expected))


def _selected_provider_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    symbols: Iterable[str],
    symbol_key: str,
    label: str,
) -> tuple[Mapping[str, Any], ...]:
    expected = {str(symbol).upper() for symbol in symbols}
    by_symbol: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        native = str(row.get(symbol_key) or "").upper()
        if native not in expected:
            continue
        if native in by_symbol:
            raise CertificationError(f"{label} provider metadata has a duplicate selected symbol")
        by_symbol[native] = row
    if set(by_symbol) != expected:
        raise CertificationError(f"{label} provider metadata omitted a selected universe symbol")
    return tuple(by_symbol[symbol] for symbol in sorted(expected))


def _l2_okx_futures_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    policy: LiquidCryptoFeaturePolicy,
) -> tuple[Mapping[str, Any], ...]:
    """Keep only real BTC/ETH quarter-pair candidates before strict parsing."""

    values = []
    for row in rows:
        if str(row.get("instType") or "").upper() != "FUTURES":
            continue
        if str(row.get("state") or "").lower() != "live":
            continue
        base = str(row.get("baseCcy") or row.get("ctValCcy") or "").upper()
        alias = str(row.get("alias") or "").lower()
        if base in policy.l2_base_assets and alias in {"quarter", "next_quarter"}:
            values.append(row)
    if not values:
        raise CertificationError("OKX provider metadata has no BTC/ETH dated L2 candidates")
    return tuple(values)


def build_certification_input(
    payloads: ProviderPayloads,
    *,
    policy: LiquidCryptoFeaturePolicy,
    now_ns: int,
    top_n: int = _TOP_N,
) -> CertificationInput:
    """Resolve real universe membership and exact canonical records in memory."""

    if top_n != _TOP_N:
        raise ValueError("C3.6 certification is intentionally fixed to top_n=350")
    binance_universe = resolve_binance_usdm_top_volume(
        exchange_info=payloads.binance_exchange_info,
        tickers=payloads.binance_tickers,
        policy=TopVolumeUniversePolicy("BINANCE", "USDM", size=top_n),
        generated_at_ns=now_ns,
    )
    okx_universe = resolve_okx_swap_top_volume(
        instruments=payloads.okx_swaps,
        tickers=payloads.okx_swap_tickers,
        policy=TopVolumeUniversePolicy("OKX", "SWAP", size=top_n),
        generated_at_ns=now_ns,
    )
    binance_records = parse_exchange_info(
        payloads.binance_exchange_info,
        valid_from_ns=now_ns,
    ).records
    selected_okx_swap_rows = _selected_provider_rows(
        payloads.okx_swaps,
        symbols=(item.native_symbol for item in okx_universe.members),
        symbol_key="instId",
        label="OKX Swap",
    )
    okx_swaps = tuple(
        parse_public_instrument(row, metadata_revision=1, valid_from_ns=now_ns)[0]
        for row in selected_okx_swap_rows
    )
    okx_futures = tuple(
        parse_public_instrument(row, metadata_revision=1, valid_from_ns=now_ns)[0]
        for row in _l2_okx_futures_rows(payloads.okx_futures, policy=policy)
    )
    binance_selected = _select_universe_records(
        binance_records,
        universe=binance_universe,
        venue="BINANCE",
        market="USDM",
    )
    okx_selected = _select_universe_records(
        okx_swaps,
        universe=okx_universe,
        venue="OKX",
        market="SWAP",
    )
    feature_set = select_liquid_crypto_feature_set(
        _deduplicate((*binance_records, *okx_swaps, *okx_futures)),
        policy=policy,
    )
    inventory_sha256 = _digest({
        "binance_selection_sha256": binance_universe.selection_sha256,
        "okx_selection_sha256": okx_universe.selection_sha256,
        "feature_policy": policy.canonical_mapping(),
        "l2_instrument_uids": [item.instrument_uid for item in feature_set.l2_books],
    })
    return CertificationInput(
        binance_universe=binance_universe,
        okx_universe=okx_universe,
        warmup_records=_deduplicate((*binance_selected, *okx_selected)),
        feature_set=feature_set,
        metadata_sha256=payloads.metadata_sha256,
        inventory_sha256=inventory_sha256,
    )


def _catalog(records: Sequence[InstrumentRecord], policy: LiquidCryptoFeaturePolicy) -> StableSourceCatalog:
    if not records:
        raise CertificationError("C3.6 catalog cannot be empty")
    anchor = records[0]
    return StableSourceCatalog(
        canonical_stream="md.canonical.v2",
        bindings=(StableSourceBinding(
            binding_id="c36-certification-anchor",
            instrument=anchor,
            provider=anchor.identity.venue,
            source_id="qdl-c36-certification-anchor-v1",
            source_role="REFERENCE",
            source_policy_id=policy.source_policy_id,
            authoritative=False,
            adapter_version="qdl-c36-certification/1",
            normalizer_version="qdl-canonical/2.0.0",
            feed=FeedType.BAR,
            interval="1m",
            stale_after_ms=120_000,
            require_final_bar=True,
            continuous_calendar=True,
            v1_compatibility="NONE",
            canonical_stream="md.canonical.v2",
        ),),
        catalog_revision=1,
        source_policy_revision=policy.revision,
        authority_revision=1,
        instruments=tuple(records),
    )


def _bar_requirements(
    records: Iterable[InstrumentRecord],
    *,
    policy: LiquidCryptoFeaturePolicy,
    deadline_ms: int,
) -> tuple[DataRequirement, ...]:
    return tuple(
        DataRequirement(
            instrument_uid=record.instrument_uid,
            feed=FeedType.BAR,
            interval="1m",
            consumer_grade=ConsumerGrade.ALPHA,
            source_policy_id=policy.source_policy_id,
            max_freshness_ms=120_000,
            require_final_bars=True,
            recovery=RecoveryPolicy.FRESH_SNAPSHOT,
            warmup=WarmupSpecification.for_rows(
                1,
                max_cache_age_ms=0,
                deadline_ms=deadline_ms,
            ),
        )
        for record in sorted(records, key=lambda item: item.instrument_id)
    )


def _history_digest(items: Sequence[object]) -> str:
    material = []
    for item in items:
        material.append({
            "instrument_uid": item.instrument_uid,
            "instrument_id": item.instrument_id,
            "interval": item.interval,
            "open_time_ns": item.payload.get("open_time_ns"),
            "close_time_ns": item.payload.get("close_time_ns"),
            "close": item.payload.get("close"),
            "is_final": item.payload.get("is_final"),
            "source_id": item.source.source_id,
            "authoritative": item.source.authoritative,
        })
    return _digest(material)


async def _admit_bars(
    service,
    requirements: Sequence[DataRequirement],
) -> tuple[dict[str, object], ...]:
    evidence = []
    for index, chunk in enumerate(_chunks(tuple(requirements)), start=1):
        result = await service.warmup_batch_async(
            BatchRequirement(
                consumer_id=f"qdl.c36.top350.final-bar.{index}",
                requirements=chunk,
                require_all=True,
            ),
            purpose=AccessPurpose.INTERNAL_ALPHA,
        )
        failures = [
            {
                "instrument_uid": item.instrument_uid,
                "code": item.problem.code.value,
            }
            for item in result.results
            if item.problem is not None
        ]
        if failures:
            raise CertificationError(
                "V2 final-BAR batch failed: "
                + json.dumps(failures[:8], sort_keys=True, separators=(",", ":"))
            )
        items = []
        for requirement, item in zip(chunk, result.results, strict=True):
            if item.result is None:
                raise CertificationError("V2 final-BAR batch returned no result")
            history = item.result.history
            if len(history.items) != 1:
                raise CertificationError("V2 final-BAR certificate received an unexpected row count")
            value = history.items[0]
            if (
                item.instrument_uid != requirement.instrument_uid
                or value.instrument_uid != requirement.instrument_uid
                or value.feed is not FeedType.BAR
                or value.interval != "1m"
                or value.payload.get("is_final") is not True
                or value.source.authoritative
                or value.quality.execution_eligible
                or value.quality.gap_open
                or not value.quality.complete
                or history.coverage.value != "FULL"
            ):
                raise CertificationError("V2 final-BAR identity/finality/authority invariant failed")
            items.append(value)
        evidence.append({
            "batch": index,
            "member_count": len(items),
            "member_sha256": _digest([
                {"instrument_uid": item.instrument_uid, "instrument_id": item.instrument_id}
                for item in items
            ]),
            "window_sha256": _history_digest(items),
        })
    return tuple(evidence)


def _reference_work(
    feature_set: LiquidCryptoFeatureSet,
    *,
    policy: LiquidCryptoFeaturePolicy,
    now_ms: int,
    deadline_ms: int,
) -> tuple[_ReferenceWork, ...]:
    """Build exact provider-bound reference calls with no cross-venue fallback."""

    end_hour_ms = ((now_ms // _HOUR_MS) - 2) * _HOUR_MS
    start_hour_ms = end_hour_ms - 2 * _HOUR_MS
    end_funding_ms = ((now_ms // _FUNDING_MS) - 2) * _FUNDING_MS
    start_funding_ms = end_funding_ms - _FUNDING_MS
    end_day_ms = ((now_ms // _DAY_MS) - 2) * _DAY_MS
    start_day_ms = end_day_ms - 29 * _DAY_MS
    result: list[_ReferenceWork] = []
    for record in feature_set.perpetuals:
        common = {
            "instrument_uid": record.instrument_uid,
            "consumer_grade": ConsumerGrade.ALPHA,
            "source_policy_id": policy.source_policy_id,
            "deadline_ms": deadline_ms,
        }
        if (record.identity.venue, record.identity.market) == ("BINANCE", "USDM"):
            result.extend((
                _ReferenceWork(ReferenceDataRequirement(
                    product=ReferenceProduct.FUNDING_RATE,
                    start_time_ns=start_funding_ms * 1_000_000,
                    end_time_ns=end_funding_ms * 1_000_000,
                    limit=2,
                    page_size=2,
                    max_pages=2,
                    **common,
                ), "OK"),
                _ReferenceWork(ReferenceDataRequirement(
                    product=ReferenceProduct.OPEN_INTEREST,
                    start_time_ns=start_hour_ms * 1_000_000,
                    end_time_ns=end_hour_ms * 1_000_000,
                    interval="1h",
                    limit=3,
                    page_size=3,
                    max_pages=2,
                    **common,
                ), "OK_OR_PARTIAL"),
                _ReferenceWork(ReferenceDataRequirement(
                    product=ReferenceProduct.LONG_SHORT_RATIO,
                    start_time_ns=start_hour_ms * 1_000_000,
                    end_time_ns=end_hour_ms * 1_000_000,
                    interval="1h",
                    limit=3,
                    page_size=3,
                    max_pages=2,
                    long_short_kind=LongShortKind.GLOBAL_ACCOUNT,
                    **common,
                ), "OK_OR_PARTIAL"),
                _ReferenceWork(ReferenceDataRequirement(
                    product=ReferenceProduct.TAKER_FLOW,
                    start_time_ns=start_hour_ms * 1_000_000,
                    end_time_ns=end_hour_ms * 1_000_000,
                    interval="1h",
                    limit=3,
                    page_size=3,
                    max_pages=2,
                    **common,
                ), "OK_OR_PARTIAL"),
                _ReferenceWork(ReferenceDataRequirement(
                    product=ReferenceProduct.MARK_INDEX_PRICE,
                    **common,
                ), "OK"),
                _ReferenceWork(ReferenceDataRequirement(
                    product=ReferenceProduct.CONTRACT_METADATA,
                    **common,
                ), "OK"),
                _ReferenceWork(ReferenceDataRequirement(
                    product=ReferenceProduct.BASIS,
                    start_time_ns=start_hour_ms * 1_000_000,
                    end_time_ns=end_hour_ms * 1_000_000,
                    interval="1h",
                    limit=3,
                    page_size=3,
                    max_pages=2,
                    basis_series=BasisSeries.NATIVE,
                    basis_contract_type="PERPETUAL",
                    **common,
                ), "OK"),
            ))
            if record.base_asset in {"BTC", "ETH"}:
                result.append(_ReferenceWork(ReferenceDataRequirement(
                    product=ReferenceProduct.BASIS,
                    start_time_ns=start_day_ms * 1_000_000,
                    end_time_ns=end_day_ms * 1_000_000,
                    interval="1d",
                    limit=30,
                    page_size=30,
                    max_pages=1,
                    basis_series=BasisSeries.CONTINUOUS,
                    basis_contract_type="CURRENT_QUARTER",
                    **common,
                ), "OK"))
        elif (record.identity.venue, record.identity.market) == ("OKX", "SWAP"):
            result.extend((
                _ReferenceWork(ReferenceDataRequirement(
                    product=ReferenceProduct.FUNDING_RATE,
                    start_time_ns=start_funding_ms * 1_000_000,
                    end_time_ns=end_funding_ms * 1_000_000,
                    limit=2,
                    page_size=2,
                    max_pages=2,
                    **common,
                ), "OK"),
                _ReferenceWork(ReferenceDataRequirement(
                    product=ReferenceProduct.OPEN_INTEREST,
                    **common,
                ), "OK"),
                _ReferenceWork(ReferenceDataRequirement(
                    product=ReferenceProduct.MARK_INDEX_PRICE,
                    **common,
                ), "OK"),
                _ReferenceWork(ReferenceDataRequirement(
                    product=ReferenceProduct.CONTRACT_METADATA,
                    **common,
                ), "OK"),
                _ReferenceWork(ReferenceDataRequirement(
                    product=ReferenceProduct.LONG_SHORT_RATIO,
                    start_time_ns=start_hour_ms * 1_000_000,
                    end_time_ns=end_hour_ms * 1_000_000,
                    interval="1h",
                    limit=3,
                    page_size=3,
                    max_pages=2,
                    long_short_kind=LongShortKind.GLOBAL_ACCOUNT,
                    **common,
                ), "UNAVAILABLE"),
                _ReferenceWork(ReferenceDataRequirement(
                    product=ReferenceProduct.TAKER_FLOW,
                    start_time_ns=start_hour_ms * 1_000_000,
                    end_time_ns=end_hour_ms * 1_000_000,
                    interval="1h",
                    limit=3,
                    page_size=3,
                    max_pages=2,
                    **common,
                ), "UNAVAILABLE"),
                _ReferenceWork(ReferenceDataRequirement(
                    product=ReferenceProduct.BASIS,
                    start_time_ns=start_hour_ms * 1_000_000,
                    end_time_ns=end_hour_ms * 1_000_000,
                    interval="1h",
                    limit=3,
                    page_size=3,
                    max_pages=2,
                    basis_series=BasisSeries.CONTINUOUS,
                    basis_contract_type="CURRENT_QUARTER",
                    **common,
                ), "UNAVAILABLE"),
            ))
        else:
            raise CertificationError("liquid feature policy selected an unsupported reference market")
    return tuple(result)


def _reference_certification_batches(
    works: Sequence[_ReferenceWork],
) -> tuple[tuple[_ReferenceWork, ...], ...]:
    """Keep the provider-native Binance basis endpoint off mixed product bursts."""

    regular = []
    native_basis = []
    for work in works:
        request = work.requirement
        if (
            request.product is ReferenceProduct.BASIS
            and request.basis_series is BasisSeries.NATIVE
            and request.basis_contract_type == "PERPETUAL"
        ):
            native_basis.append((work,))
        else:
            regular.append(work)
    # Native basis is the provider's most fragile public reference lane. It is
    # intentionally isolated and runs before unrelated regular products so a
    # certification-wide history burst cannot manufacture a false failure.
    return (*native_basis, *_chunks(tuple(regular), _REFERENCE_CERTIFICATION_BATCH_ITEMS))


def _reference_digest(result) -> str:
    return _digest({
        "instrument_uid": result.request.instrument.instrument_uid,
        "product": result.request.product.value,
        "status": result.status.value,
        "coverage": {
            "complete_left": result.coverage.complete_left,
            "complete_right": result.coverage.complete_right,
            "truncated": result.coverage.truncated,
            "terminal_reason": result.coverage.terminal_reason,
        },
        "observations": [
            {
                "at_ns": observation.observed_at_ns,
                "fields": [
                    (field.name, field.value.source_text, field.unit)
                    for field in observation.fields
                ],
                "labels": observation.labels,
            }
            for observation in result.observations
        ],
    })


def _is_truthful_partial_history(data: Any) -> bool:
    """Accept only a provider-declared, nonempty history boundary partial."""

    return bool(
        data.status is ReferenceStatus.OK
        and data.observations
        and data.request.is_history
        and (
            data.coverage.truncated
            or not data.coverage.complete_left
            or not data.coverage.complete_right
        )
    )


async def _admit_references(
    service,
    works: Sequence[_ReferenceWork],
    *,
    deadline_monotonic: float,
    clock=time.monotonic,
    sleep=asyncio.sleep,
) -> dict[str, object]:
    available = []
    partial = []
    unavailable = []
    scheduler = []
    deferred_count = 0
    deferred_wait_ms = 0
    for batch_index, chunk in enumerate(_reference_certification_batches(works), start=1):
        result, attempts, deferred_ms = await _reference_batch_until_terminal(
            service,
            chunk,
            batch_index=batch_index,
            deadline_monotonic=deadline_monotonic,
            clock=clock,
            sleep=sleep,
        )
        deferred_count += attempts - 1
        deferred_wait_ms += deferred_ms
        scheduler.append({
            "batch": batch_index,
            "item_count": len(chunk),
            "attempts": attempts,
            "deferred_wait_ms": deferred_ms,
            "evidence": dict(service.last_reference_batch_evidence),
        })
        for work, item in zip(chunk, result.results, strict=True):
            request = work.requirement
            if work.expected == "UNAVAILABLE":
                if (
                    item.problem is None
                    or item.problem.code.value != "UNSUPPORTED_FEED"
                    or item.result is None
                    or item.result.status is not ReferenceStatus.UNAVAILABLE
                    or item.result.observations
                ):
                    raise CertificationError("provider-unavailable reference product was not typed unavailable")
                unavailable.append({
                    "instrument_uid": request.instrument_uid,
                    "product": request.product.value,
                    "code": item.problem.code.value,
                })
                continue
            if item.result is None:
                code = item.problem.code.value if item.problem is not None else "MISSING_RESULT"
                retry_after_ms = (
                    item.problem.retry_after_ms if item.problem is not None else None
                )
                raise CertificationError(
                    "V2 reference request failed: "
                    f"{request.product.value}:{request.instrument_uid}:{code}:"
                    f"retry_after_ms={retry_after_ms}"
                )
            data = item.result
            if item.problem is not None:
                if (
                    work.expected != "OK_OR_PARTIAL"
                    or item.problem.code.value != "PARTIAL_RESULT"
                    or not _is_truthful_partial_history(data)
                ):
                    raise CertificationError(
                        "V2 reference request failed: "
                        f"{request.product.value}:{request.instrument_uid}:"
                        f"{item.problem.code.value}:status={data.status.value}:"
                        f"error_code={data.error_code}:retry_after_ms="
                        f"{item.problem.retry_after_ms}"
                    )
                partial.append({
                    "instrument_uid": request.instrument_uid,
                    "product": request.product.value,
                    "observation_count": len(data.observations),
                    "coverage": data.coverage.terminal_reason,
                    "semantic_sha256": _reference_digest(data),
                })
                continue
            if (
                data.status is not ReferenceStatus.OK
                or not data.observations
                or any(observation.instrument_uid != request.instrument_uid for observation in data.observations)
                or (data.request.is_history and (
                    not data.coverage.complete_left
                    or not data.coverage.complete_right
                    or data.coverage.truncated
                ))
            ):
                raise CertificationError("V2 reference observation/coverage invariant failed")
            available.append({
                "instrument_uid": request.instrument_uid,
                "product": request.product.value,
                "observation_count": len(data.observations),
                "semantic_sha256": _reference_digest(data),
            })
    return {
        "request_count": len(works),
        "available_count": len(available),
        "partial_count": len(partial),
        "unavailable_count": len(unavailable),
        "deferred_count": deferred_count,
        "deferred_wait_ms": deferred_wait_ms,
        "available_sha256": _digest(available),
        "partial_sha256": _digest(partial),
        "unavailable_sha256": _digest(unavailable),
        "batch_count": len(scheduler),
        "scheduler_sha256": _digest(scheduler),
    }


def _is_native_basis_singleton(chunk: Sequence[_ReferenceWork]) -> bool:
    if len(chunk) != 1:
        return False
    request = chunk[0].requirement
    return (
        request.product is ReferenceProduct.BASIS
        and request.basis_series is BasisSeries.NATIVE
        and request.basis_contract_type == "PERPETUAL"
    )


def _native_basis_retry_after_ms(item: Any) -> int | None:
    """Return only the exact retryable shape emitted through QueryService.

    The direct reference-batch unit seam exposes ``PROVIDER_RETRY_EXHAUSTED``
    as the outer problem.  At runtime, QueryService preserves the typed cause
    in an ``ERROR`` result while exposing its own ``SOURCE_UNAVAILABLE``
    problem.  Both representations name the same bounded provider cooldown;
    no other source error may acquire a certificate retry.
    """

    problem = getattr(item, "problem", None)
    retry_after_ms = getattr(problem, "retry_after_ms", None)
    if not isinstance(retry_after_ms, int) or retry_after_ms <= 0:
        return None
    outer_code = getattr(getattr(problem, "code", None), "value", None)
    if outer_code == "PROVIDER_RETRY_EXHAUSTED":
        return retry_after_ms
    result = getattr(item, "result", None)
    if (
        outer_code == "SOURCE_UNAVAILABLE"
        and getattr(result, "status", None) is ReferenceStatus.ERROR
        and getattr(result, "error_code", None) == "PROVIDER_RETRY_EXHAUSTED"
    ):
        return retry_after_ms
    return None


async def _reference_batch_until_terminal(
    service,
    chunk: Sequence[_ReferenceWork],
    *,
    batch_index: int,
    deadline_monotonic: float,
    clock=time.monotonic,
    sleep=asyncio.sleep,
):
    """Run one batch, honoring only Rust's bounded native-basis deferral."""

    attempts = 0
    deferred_ms = 0
    while True:
        attempts += 1
        result = await service.reference_data_batch_async(
            ReferenceBatchRequirement(
                consumer_id=f"qdl.c36.liquid-reference.{batch_index}",
                requirements=tuple(item.requirement for item in chunk),
                require_all=False,
            ),
            purpose=AccessPurpose.INTERNAL_ALPHA,
        )
        if not _is_native_basis_singleton(chunk):
            return result, attempts, deferred_ms
        item = result.results[0]
        retry_after_ms = _native_basis_retry_after_ms(item)
        if retry_after_ms is None:
            return result, attempts, deferred_ms
        remaining_ms = int((deadline_monotonic - clock()) * 1_000)
        # Preserve a final request window. Sleeping until the deadline would
        # turn a known provider cooldown into an ambiguous timeout.
        if retry_after_ms + 250 >= remaining_ms:
            raise CertificationError(
                "Rust native-basis admission cooldown exceeds the bounded certificate deadline"
            )
        await sleep(retry_after_ms / 1_000)
        deferred_ms += retry_after_ms


def _l2_admission_document(
    feature_set: LiquidCryptoFeatureSet,
    *,
    policy: LiquidCryptoFeaturePolicy,
    metadata_sha256: Mapping[str, str],
    inventory_sha256: str,
) -> dict[str, object]:
    records = {
        (
            item.identity.venue,
            item.identity.market,
            item.identity.product_type.value,
            item.native_symbol,
        ): item
        for item in feature_set.l2_books
    }
    requirements = build_l2_feature_requirements(
        feature_set,
        policy=policy,
        consumer_id="qdl.c36.l2-certification",
        purpose=DemandPurpose.ALPHA,
    )
    rows = []
    for requirement in requirements:
        selector = requirement.universe
        if len(selector.native_symbols) != 1:
            raise CertificationError("L2 certification requirement lacks one exact native symbol")
        key = (
            selector.venue,
            selector.market,
            selector.product_type,
            selector.native_symbols[0],
        )
        try:
            record = records[key]
        except KeyError as error:
            raise CertificationError("L2 requirement has no provider-discovered record") from error
        rows.append({
            "state": "ADMITTED",
            "venue": record.identity.venue,
            "market": record.identity.market,
            "product_type": record.identity.product_type.value,
            "feed": requirement.feed.value,
            "native_symbol": record.native_symbol,
            "instrument_uid": record.instrument_uid,
            "instrument_id": record.instrument_id,
            "requirement_id": requirement.requirement_id,
        })
    return {
        "schema": "qdl.c36.l2-admission.v1",
        "inventory_sha256": inventory_sha256,
        "metadata_sha256": dict(sorted(metadata_sha256.items())),
        "rows": sorted(rows, key=lambda item: (str(item["venue"]), str(item["market"]), str(item["native_symbol"]), str(item["feed"]))),
    }


async def _run_async(
    certification: CertificationInput,
    *,
    policy: LiquidCryptoFeaturePolicy,
    provider_timeout_seconds: float,
    deadline_ms: int,
    l2_timeout_seconds: float,
    l2_max_frames: int,
    native_basis_admission: ProviderAdmissionRuntime | None = None,
) -> dict[str, object]:
    all_records = _deduplicate((
        *certification.warmup_records,
        *certification.feature_set.perpetuals,
        *certification.feature_set.l2_books,
    ))
    catalog = _catalog(all_records, policy)
    service, source = _service(
        catalog,
        timeout_seconds=provider_timeout_seconds,
        native_basis_admission=native_basis_admission,
    )
    started = time.monotonic()
    cpu_before = time.process_time()
    references = await _admit_references(
        service,
        _reference_work(
            certification.feature_set,
            policy=policy,
            now_ms=time.time_ns() // 1_000_000,
            deadline_ms=deadline_ms,
        ),
        deadline_monotonic=started + deadline_ms / 1_000,
    )
    document = _l2_admission_document(
        certification.feature_set,
        policy=policy,
        metadata_sha256=certification.metadata_sha256,
        inventory_sha256=certification.inventory_sha256,
    )
    with tempfile.TemporaryDirectory(prefix="qdl-c36-certification-") as directory:
        admission_path = Path(directory) / "l2-admission.json"
        admission_path.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")
        l2 = await run_l2_capture(
            admission_path=admission_path,
            timeout_seconds=l2_timeout_seconds,
            max_frames=l2_max_frames,
        )
    bars = await _admit_bars(
        service,
        _bar_requirements(
            certification.warmup_records,
            policy=policy,
            deadline_ms=deadline_ms,
        ),
    )
    source_stats = source.stats()
    if source_stats["provider_source_failures"]:
        raise CertificationError("provider final-BAR source recorded failures")
    elapsed = time.monotonic() - started
    expected_l2_bindings = len(certification.feature_set.l2_books)
    observed_l2_bindings = (
        len(l2["binance_usdm"]) + len(l2["okx_swap"]) + len(l2["okx_futures"])
    )
    if observed_l2_bindings != expected_l2_bindings:
        raise CertificationError("L2 capture count differs from the declared provider-discovered set")
    return {
        "schema": _SCHEMA,
        "status": "PASS",
        "provenance": "REAL_PUBLIC_PROVIDER_READ_ONLY",
        "runtime_mutations": 0,
        "production_writes": 0,
        "raw_provider_bytes_persisted": 0,
        "rust_provider_admission": native_basis_admission is not None,
        "top_volume": {
            "binance_usdm": {
                "member_count": len(certification.binance_universe.members),
                "eligible_count": len(certification.binance_universe.eligible_symbols),
                "selection_sha256": certification.binance_universe.selection_sha256,
            },
            "okx_swap": {
                "member_count": len(certification.okx_universe.members),
                "eligible_count": len(certification.okx_universe.eligible_symbols),
                "selection_sha256": certification.okx_universe.selection_sha256,
            },
        },
        "inventory_sha256": certification.inventory_sha256,
        "metadata_sha256": dict(sorted(certification.metadata_sha256.items())),
        "final_bar": {
            "requested_count": len(certification.warmup_records),
            "passed_count": sum(int(item["member_count"]) for item in bars),
            "batch_count": len(bars),
            "batch_sha256": _digest(bars),
            "provider_source": source_stats,
            "scheduler": dict(service.last_batch_evidence),
        },
        "reference": references,
        "l2": {
            "declared_binding_count": expected_l2_bindings,
            "captured_binding_count": observed_l2_bindings,
            "binance_usdm_count": len(l2["binance_usdm"]),
            "okx_swap_count": len(l2["okx_swap"]),
            "okx_futures_count": len(l2["okx_futures"]),
            "replay_sha256": _digest({
                "binance_usdm": l2["binance_usdm"],
                "okx_swap": l2["okx_swap"],
                "okx_futures": l2["okx_futures"],
            }),
            "elapsed_ms": l2["elapsed_ms"],
        },
        "elapsed_seconds": elapsed,
        "cpu_seconds": time.process_time() - cpu_before,
        "rss_max_kib": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
    }


def run(
    *,
    policy_path: Path = DEFAULT_POLICY,
    top_n: int = _TOP_N,
    metadata_timeout_seconds: float = 15.0,
    metadata_attempts: int = 3,
    provider_timeout_seconds: float = 20.0,
    deadline_ms: int = 60_000,
    l2_timeout_seconds: float = 60.0,
    l2_max_frames: int = 64,
    require_rust_admission: bool = False,
) -> dict[str, object]:
    if not 5.0 <= metadata_timeout_seconds <= 30.0:
        raise ValueError("metadata timeout must be within [5, 30]")
    if not 1 <= metadata_attempts <= 5:
        raise ValueError("metadata attempts must be within [1, 5]")
    if not 5.0 <= provider_timeout_seconds <= 30.0:
        raise ValueError("provider timeout must be within [5, 30]")
    if not 10_000 <= deadline_ms <= 120_000:
        raise ValueError("warmup/reference deadline must be within [10000, 120000]")
    if not 10.0 <= l2_timeout_seconds <= 60.0:
        raise ValueError("L2 timeout must be within [10, 60]")
    if not 4 <= l2_max_frames <= 128:
        raise ValueError("L2 max frames must be within [4, 128]")
    policy = LiquidCryptoFeaturePolicy.load(policy_path)
    now_ns = time.time_ns()
    certification = build_certification_input(
        fetch_provider_payloads(
            timeout_seconds=metadata_timeout_seconds,
            max_attempts=metadata_attempts,
        ),
        policy=policy,
        now_ns=now_ns,
        top_n=top_n,
    )
    native_basis_admission = _runtime_native_basis_admission(
        required=require_rust_admission
    )
    try:
        return asyncio.run(_run_async(
            certification,
            policy=policy,
            provider_timeout_seconds=provider_timeout_seconds,
            deadline_ms=deadline_ms,
            l2_timeout_seconds=l2_timeout_seconds,
            l2_max_frames=l2_max_frames,
            native_basis_admission=native_basis_admission,
        ))
    finally:
        if isinstance(native_basis_admission, RustHttpProviderAdmission):
            asyncio.run(native_basis_admission.aclose())


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--top-n", type=int, default=_TOP_N)
    parser.add_argument("--metadata-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--metadata-attempts", type=int, default=3)
    parser.add_argument("--provider-timeout-seconds", type=float, default=20.0)
    parser.add_argument("--deadline-ms", type=int, default=60_000)
    parser.add_argument("--l2-timeout-seconds", type=float, default=60.0)
    parser.add_argument("--l2-max-frames", type=int, default=64)
    parser.add_argument("--require-rust-admission", action="store_true")
    args = parser.parse_args(argv)
    try:
        report = run(
            policy_path=args.policy,
            top_n=args.top_n,
            metadata_timeout_seconds=args.metadata_timeout_seconds,
            metadata_attempts=args.metadata_attempts,
            provider_timeout_seconds=args.provider_timeout_seconds,
            deadline_ms=args.deadline_ms,
            l2_timeout_seconds=args.l2_timeout_seconds,
            l2_max_frames=args.l2_max_frames,
            require_rust_admission=args.require_rust_admission,
        )
    except (CertificationError, OSError, RuntimeError, ValueError) as error:
        print(json.dumps({"status": "FAIL", "error": str(error)}, sort_keys=True))
        return 1
    print(json.dumps({
        "status": report["status"],
        "top350_final_bars": report["final_bar"]["passed_count"],
        "reference_requests": report["reference"]["request_count"],
        "l2_bindings": report["l2"]["captured_binding_count"],
        "elapsed_seconds": round(float(report["elapsed_seconds"]), 3),
        "rss_max_kib": report["rss_max_kib"],
        "runtime_mutations": report["runtime_mutations"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
