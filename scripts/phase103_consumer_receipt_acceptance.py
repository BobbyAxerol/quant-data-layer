#!/usr/bin/env python3
"""No-order Phase 10.3 V2 consumer-receipt acceptance.

This tool is intentionally an operator probe, never an ingest, execution or
provider client. It obtains all data only through the public V2 query/stream
SDK surfaces using the governed Trading System and alpha workload identities.
Run it only after the separate RUST_PRIMARY handoff packet is approved.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import shutil
import sys
import tempfile
import time
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import AsyncIterator, Callable, Iterator

ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.certification.phase103_consumer_acceptance import (
    AcceptanceProduct,
    DeliveryClass,
    build_consumer_acceptance_scope,
    compact_receipt_evidence,
    content_fingerprint,
    sdk_requirement,
    validate_product_view,
    validate_final_bar_warmup_windows,
    validate_replica_views,
    validate_resume_offsets,
    warmup_content_fingerprint,
)
from qdl.adapters.intervals import canonical_interval_ms, is_valid_bar_open_ms
from qdl.runtime.stable_bar_edge import durable_bar_history_capacity_rows
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.runtime.stable_deployment import StableAcquisitionPlan
from qdl.certification.phase105_release_observations import compact_view_quality
from scripts.phase103_prepare_shared_primary_packet import (
    validate_prepared_shared_primary_bundle,
)
from qdl_sdk import (
    AsyncDataLayerClient,
    ControlEvent,
    FileCursorStore,
    GrpcStreamTransport,
    RestQueryTransport,
    RotatingJwtCredentialProvider,
    StalePolicy as SdkStalePolicy,
    StreamEvent,
    WarmupSpecification as SdkWarmupSpecification,
    WarmupTimeRange as SdkWarmupTimeRange,
    WorkloadTlsConfig,
    market_data_view_from_stream,
)
from qdl_sdk.errors import ContinuityError, DataLayerError


DEFAULT_CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
DEFAULT_ACQUISITION = ROOT / "config/v2/stable-acquisition-bindings.yaml"
DEFAULT_TRADING_MANIFEST = ROOT / "consumers/stable/trading-system-paper.yaml"
DEFAULT_ALPHA_MANIFEST = ROOT / "consumers/stable/alpha-binance-paper.yaml"
_QUIET_QUOTE_RETRY_SECONDS = 0.2
_BOOK_SNAPSHOT_RETRY_SECONDS = 1.0
_QUIET_CONTINUITY_STREAM_OBSERVATION_SECONDS = 2.0
_QUIET_CONTINUITY_FEEDS = frozenset({"TRADE", "BOOK_DELTA"})
# C2 proves a representative retained BAR window, not an impossible request
# for the full per-consumer quota on every calendar interval. Production
# callers still declare their own bounded maxlen through the public SDK.
_C2_BAR_WARMUP_ROWS = 700


@dataclass(frozen=True, slots=True)
class WorkloadIdentity:
    consumer_id: str
    subject: str
    manifest_revision: int
    tls: WorkloadTlsConfig
    credential: RotatingJwtCredentialProvider


def _identity(
    *,
    product: AcceptanceProduct,
    certificate_file: str,
    private_key_file: str,
    jwt_private_key_file: str,
    jwt_key_id: str,
    tls_ca_file: str,
    issuer: str,
    audience: str,
) -> WorkloadIdentity:
    return WorkloadIdentity(
        consumer_id=product.consumer_id,
        subject=product.consumer_subject,
        manifest_revision=product.manifest_revision,
        tls=WorkloadTlsConfig(
            tls_ca_file,
            certificate_file,
            private_key_file,
        ),
        credential=RotatingJwtCredentialProvider(
            private_key_file=jwt_private_key_file,
            key_id=jwt_key_id,
            algorithm="RS256",
            issuer=issuer,
            audience=audience,
            subject=product.consumer_subject,
            environment="paper",
            roles=("historical_reader", "market_data_reader", "stream_consumer"),
            venues=("BINANCE", "OKX"),
            consumer_manifest_revision=product.manifest_revision,
            lifetime_seconds=300,
            refresh_before_seconds=60,
        ),
    )


def _client(
    identity: WorkloadIdentity,
    *,
    base_url: str,
    grpc_target: str,
    cursor_path: Path,
    timeout_seconds: float,
) -> AsyncDataLayerClient:
    return AsyncDataLayerClient(
        query_transport=RestQueryTransport(
            base_url,
            timeout_seconds=timeout_seconds,
            credential_provider=identity.credential,
            tls=identity.tls,
        ),
        stream_transport=GrpcStreamTransport(
            grpc_target,
            tls=identity.tls,
            credential_provider=identity.credential,
        ),
        consumer_id=identity.consumer_id,
        cursor_store=FileCursorStore(cursor_path),
        max_buffer_events=64,
        max_reconnect_attempts=2,
    )


def _receipt_client(
    identity: WorkloadIdentity,
    *,
    base_url: str,
    grpc_target: str,
    cursor_path: Path,
    timeout_seconds: float,
    client_factory: Callable[..., AsyncDataLayerClient] | None,
) -> AsyncDataLayerClient:
    """Create a receipt client, optionally through an acceptance-only wrapper.

    Production SDK construction remains `_client`.  C2 may supply a local
    wrapper for its own bounded request budget; the wrapper never changes the
    public client contract, identity, cursor or stream transport.
    """

    factory = _client if client_factory is None else client_factory
    return factory(
        identity,
        base_url=base_url,
        grpc_target=grpc_target,
        cursor_path=cursor_path,
        timeout_seconds=timeout_seconds,
    )


def _cursor_path(state_dir: Path, product: AcceptanceProduct) -> Path:
    identity = "|".join(product.identity).encode()
    return state_dir / f"{hashlib.sha256(identity).hexdigest()}.json"


def _stream_event_timeout_seconds(
    product: AcceptanceProduct,
    request_timeout_seconds: float,
) -> float:
    """Wait through a product's declared bounded arrival window."""
    max_freshness_ms = product.requirement.max_freshness_ms
    if product.feed.value == "BOOK_SNAPSHOT":
        if max_freshness_ms is None:
            raise ValueError("BOOK_SNAPSHOT receipt product must declare a freshness SLA")
        return max(request_timeout_seconds, max_freshness_ms / 1_000)
    if product.feed.value != "BAR":
        return request_timeout_seconds
    if product.interval is None:
        raise ValueError("BAR receipt product must declare its canonical interval")
    interval_seconds = canonical_interval_ms(product.interval) / 1_000
    settlement_seconds = min(15.0, max(5.0, interval_seconds / 4.0))
    wait_seconds = max(request_timeout_seconds, interval_seconds + settlement_seconds)
    if max_freshness_ms is not None:
        wait_seconds = min(wait_seconds, max_freshness_ms / 1_000)
    return wait_seconds


def _uses_historical_bar_replay(product: AcceptanceProduct) -> bool:
    """Whether C2 can prove bounded replay without waiting for a future close.

    Execution BARs stay on the ordinary strict/live path.  Their real consumers
    must never use the historical observation requirement below.  Alpha BAR
    receipts instead first validate the real current warmup strictly, then use
    a prior retained boundary solely to prove signed durable replay and
    cross-replica resume within C2's fixed observation window.
    """

    return (
        product.delivery is DeliveryClass.DURABLE
        and product.feed.value == "BAR"
        and product.requirement.consumer_grade.value != "EXECUTION"
    )


def _c2_requirement(requirement):
    """Keep the C2 BAR history proof bounded without reducing public quota.

    The registered manifest quota remains the caller's `1..10,000` ceiling.
    A fixed C2 proof must instead fit the real retained final-BAR window across
    all native intervals; otherwise a 12-hour or weekly route would be asked
    for decades of history merely because its client is allowed to request it.
    """

    if requirement.feed.value != "BAR":
        return requirement
    specification = requirement.warmup_specification
    if specification is None or specification.rows is None:
        raise ValueError("C2 BAR product requires a row-bounded warmup policy")
    if not requirement.interval:
        raise ValueError("C2 BAR product requires an interval")
    rows = min(
        _C2_BAR_WARMUP_ROWS,
        specification.rows,
        durable_bar_history_capacity_rows(requirement.interval),
    )
    return replace(
        requirement,
        warmup_limit=rows,
        warmup=(SdkWarmupSpecification(rows=rows) if requirement.warmup is not None else None),
    )


def _historical_bar_replay_requirement(
    requirement,
    *,
    latest_open_time_ns: int,
    calendar_provider: str | None = None,
):
    """Build one aligned, bounded cursor seed before two retained BAR records.

    The caller has already checked the current strict requirement.  This
    request therefore preserves identity, finality, recovery, source policy,
    coverage and gap behavior, while changing only freshness to OBSERVE so the
    older seed bar is not incorrectly presented as a live trading observation.
    """

    if requirement.feed.value != "BAR" or not requirement.interval:
        raise ValueError("historical replay seed requires a BAR interval")
    if requirement.consumer_grade.value == "EXECUTION":
        raise ValueError("execution BAR receipts must use the strict live path")
    interval_ns = canonical_interval_ms(requirement.interval) * 1_000_000
    latest_open_ms, sub_millisecond_ns = divmod(latest_open_time_ns, 1_000_000)
    if (
        latest_open_time_ns <= 2 * interval_ns
        or sub_millisecond_ns
        or not is_valid_bar_open_ms(
            requirement.interval,
            latest_open_ms,
            provider=calendar_provider,
        )
    ):
        raise ValueError("latest BAR open time cannot form a retained replay seed")
    original_warmup = requirement.warmup_specification
    if original_warmup is None:
        raise ValueError("historical replay seed requires the governed warmup policy")
    seed_end_ns = latest_open_time_ns - interval_ns
    seed_start_ns = seed_end_ns - interval_ns
    return replace(
        requirement,
        warmup_limit=0,
        max_freshness_ms=None,
        stale_policy=SdkStalePolicy.OBSERVE,
        warmup=SdkWarmupSpecification(
            time_range=SdkWarmupTimeRange(
                start_time_ns=seed_start_ns,
                end_time_ns=seed_end_ns,
            ),
            interval_source_policy=original_warmup.interval_source_policy,
            max_cache_age_ms=original_warmup.max_cache_age_ms,
            deadline_ms=original_warmup.deadline_ms,
        ),
    )


async def _next_data(session, *, timeout_seconds: float) -> tuple[StreamEvent, list[str]]:
    controls: list[str] = []
    for _ in range(8):
        item = await asyncio.wait_for(session.__anext__(), timeout=timeout_seconds)
        if isinstance(item, ControlEvent):
            controls.append(item.code)
            continue
        if isinstance(item, StreamEvent):
            return item, controls
        raise AssertionError("V2 SDK stream emitted an unknown event type")
    raise AssertionError("V2 SDK stream emitted controls without market data")


async def _next_data_or_timeout(
    session,
    *,
    timeout_seconds: float,
) -> tuple[StreamEvent | None, list[str]]:
    """Observe one stream without manufacturing data when a continuity feed is quiet."""
    controls: list[str] = []
    deadline = time.monotonic() + timeout_seconds
    for _ in range(8):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None, controls
        try:
            item = await asyncio.wait_for(session.__anext__(), timeout=remaining)
        except TimeoutError:
            return None, controls
        if isinstance(item, ControlEvent):
            controls.append(item.code)
            continue
        if isinstance(item, StreamEvent):
            return item, controls
        raise AssertionError("V2 SDK stream emitted an unknown event type")
    return None, controls


def _allows_quiet_continuity_observation(product: AcceptanceProduct, requirement) -> bool:
    """Whether C2 may observe a quiet, live continuity channel.

    BOOK_DELTA is not price data. It can be quiet while its verified book and
    transport session stay healthy, so C2 records it as non-executable
    continuity evidence rather than manufacturing a new update.
    """
    return (
        product.delivery is DeliveryClass.DURABLE
        and product.feed.value in _QUIET_CONTINUITY_FEEDS
        and requirement.effective_event_recency_policy is SdkStalePolicy.OBSERVE
        and requirement.max_session_liveness_ms is not None
    )


def _allows_quiet_final_bar_handoff(product: AcceptanceProduct) -> bool:
    """Allow a bounded no-event observation for retained non-execution BARs.

    A current alpha BAR channel can be quiet until the next interval closes.
    C2 still proves signed stream controls and a fresh final BAR from each
    replica; it never turns that quiet period into an execution price or a
    durable replay checkpoint. Execution BARs keep the ordinary live-event
    path.
    """

    return (
        product.delivery is DeliveryClass.DURABLE
        and product.feed.value == "BAR"
        and product.requirement.consumer_grade.value != "EXECUTION"
    )


def _quiet_continuity_status_is_observable(product: AcceptanceProduct, requirement, status) -> bool:
    """Keep a quiet connected continuity channel distinct from price data."""
    quality = status.quality
    return (
        _allows_quiet_continuity_observation(product, requirement)
        and status.instrument_uid == requirement.instrument_uid
        and status.feed is requirement.feed
        and quality.policy_id == requirement.source_policy_id
        and quality.state == "LIVE"
        and quality.event_recency_state == "STALE"
        and quality.provider_session_state == "LIVE"
        and quality.provider_session_liveness_ms is not None
        and quality.provider_session_liveness_ms <= requirement.max_session_liveness_ms
        and quality.complete
        and not quality.gap_open
        and not quality.execution_eligible
    )


def _fresh_continuity_status_is_observable(product: AcceptanceProduct, requirement, status) -> bool:
    """Accept a fresh verified continuity event during a bounded C2 probe."""
    quality = status.quality
    return (
        _allows_quiet_continuity_observation(product, requirement)
        and status.instrument_uid == requirement.instrument_uid
        and status.feed is requirement.feed
        and quality.policy_id == requirement.source_policy_id
        and quality.state == "LIVE"
        and quality.event_recency_state == "LIVE"
        and quality.provider_session_state == "LIVE"
        and quality.provider_session_liveness_ms is not None
        and quality.provider_session_liveness_ms <= requirement.max_session_liveness_ms
        and quality.complete
        and not quality.gap_open
        and quality.execution_eligible
    )


def _require_signed_cursor_controls(controls: list[str]) -> None:
    required = {"REPLAYING", "LIVE"}
    missing = required.difference(controls)
    if missing:
        raise ContinuityError(
            "CURSOR_INVALID",
            "C2 no-event continuity observation did not confirm the signed cursor stream",
        )


async def _classify_no_event_continuity_session(
    client: AsyncDataLayerClient,
    *,
    product: AcceptanceProduct,
    requirement,
    timeout_seconds: float,
) -> str:
    try:
        status = await asyncio.wait_for(
            client.feed_status(requirement),
            timeout=timeout_seconds,
        )
    except TimeoutError as timeout:
        raise ContinuityError(
            "DATA_STALE", "C2 quiet continuity status did not return before its deadline"
        ) from timeout
    if _fresh_continuity_status_is_observable(product, requirement, status):
        return "FRESH_EXECUTABLE"
    if _quiet_continuity_status_is_observable(product, requirement, status):
        return "QUIET_NON_EXECUTABLE"
    raise ContinuityError(
        "DATA_STALE",
        "C2 no-event continuity observation requires a live fresh/executable or quiet/non-executable session",
    )


async def _verify_quiet_final_bar_current(
    client: AsyncDataLayerClient,
    *,
    product: AcceptanceProduct,
    requirement,
    timeout_seconds: float,
) -> str:
    """Prove a quiet retained BAR remains final and current on this replica."""

    if not _allows_quiet_final_bar_handoff(product):
        raise ValueError("quiet final BAR verification requires a non-execution durable BAR")
    current = await _strict_snapshot_for_c2(
        client,
        product=product,
        requirement=requirement,
        timeout_seconds=timeout_seconds,
    )
    validate_product_view(product, current.data)
    return "CURRENT_FINAL_BAR"


def _stream_handoff_mode(
    product: AcceptanceProduct,
    *,
    acknowledged_offset: int | None,
    resumed_offset: int | None,
    no_event_sessions: tuple[str, ...] = (),
) -> str:
    if (acknowledged_offset is None) != (resumed_offset is None):
        raise ValueError("C2 stream handoff evidence is incomplete")
    if product.delivery is not DeliveryClass.DURABLE:
        return "NOT_APPLICABLE"
    if acknowledged_offset is None:
        if len(no_event_sessions) == 2 and all(
            item == "CURRENT_FINAL_BAR" for item in no_event_sessions
        ):
            return "CURRENT_FINAL_BAR_OBSERVED_NO_CURSOR"
        if (
            len(no_event_sessions) == 2
            and no_event_sessions[0] == "CURSOR_ACKNOWLEDGED"
            and no_event_sessions[1] in {
                "FRESH_EXECUTABLE_AFTER_CURSOR",
                "QUIET_NON_EXECUTABLE_AFTER_CURSOR",
            }
        ):
            return "SIGNED_CURSOR_REOPENED_NO_NEW_EVENT"
        if len(no_event_sessions) != 2:
            raise ValueError("C2 no-event stream evidence requires both sessions")
        if no_event_sessions[-1] == "EVENT_AFTER_REOPEN":
            if no_event_sessions[0] not in {"FRESH_EXECUTABLE", "QUIET_NON_EXECUTABLE"}:
                raise ValueError("C2 no-event stream evidence has an invalid initial session")
            return "LIVE_EVENT_AFTER_REOPEN_NO_CURSOR"
        if all(item == "FRESH_EXECUTABLE" for item in no_event_sessions):
            return "LIVE_OBSERVED_NO_NEW_CURSOR"
        if all(item == "QUIET_NON_EXECUTABLE" for item in no_event_sessions):
            return "QUIET_OBSERVED_NO_CURSOR"
        return "MIXED_LIVE_QUIET_NO_NEW_CURSOR"
    if no_event_sessions:
        raise ValueError("C2 cursor replay cannot include no-event session evidence")
    return "DURABLE_CURSOR_REPLAYED"


def _replay_precedes_handoff(*, logical_offset: int, watermark_offset: int) -> bool:
    """Whether a resumed frame restores state rather than supplies a live price.

    A reconnect checks a fresh query handoff, yet a durable cursor may correctly
    point before that handoff. Those records are required for deterministic
    state recovery. They retain identity, source, offset and gap validation,
    but cannot be claimed as an executable current price; a strict read-back
    is mandatory after the replay acknowledgement.
    """

    if logical_offset < 0 or watermark_offset < 0:
        raise ValueError("durable replay offsets must be non-negative")
    return logical_offset <= watermark_offset


async def _query_product(
    product: AcceptanceProduct,
    *,
    primary: AsyncDataLayerClient,
    secondary: AsyncDataLayerClient,
    timeout_seconds: float = 15.0,
) -> tuple[str, str | None, float, float | None]:
    primary_hash, secondary_hash, primary_ms, secondary_ms, _quality, _bar_alignment = (
        await _query_product_with_quality(
            product,
            primary=primary,
            secondary=secondary,
            timeout_seconds=timeout_seconds,
        )
    )
    return primary_hash, secondary_hash, primary_ms, secondary_ms


async def _query_product_with_quality(
    product: AcceptanceProduct,
    *,
    primary: AsyncDataLayerClient,
    secondary: AsyncDataLayerClient,
    timeout_seconds: float = 15.0,
) -> tuple[
    str,
    str | None,
    float,
    float | None,
    dict[str, object],
    dict[str, object] | None,
]:
    """Query both replicas and retain only compact quality evidence for B3."""
    requirement = _c2_requirement(sdk_requirement(product))
    bar_alignment: dict[str, object] | None = None
    snapshot_timeout_seconds = (
        _stream_event_timeout_seconds(product, timeout_seconds)
        if product.feed.value == "BOOK_SNAPSHOT"
        else timeout_seconds
    )
    primary_started = time.perf_counter()
    if product.feed.value == "BAR":
        primary_response = await primary.warmup(requirement)
        primary_latency_ms = (time.perf_counter() - primary_started) * 1000
        for view in primary_response.data[:-1]:
            validate_product_view(product, view, require_current_quality=False)
        primary_view = primary_response.data[-1]
        validate_product_view(product, primary_view)
        primary_hash = warmup_content_fingerprint(primary_response.data)
    else:
        primary_response = await _strict_snapshot_for_c2(
            primary,
            product=product,
            requirement=requirement,
            timeout_seconds=snapshot_timeout_seconds,
        )
        primary_latency_ms = (time.perf_counter() - primary_started) * 1000
        primary_view = primary_response.data
        validate_product_view(product, primary_view)
        primary_hash = content_fingerprint(primary_view)

    secondary_started = time.perf_counter()
    if product.feed.value == "BAR":
        secondary_response = await secondary.warmup(requirement)
        secondary_latency_ms = (time.perf_counter() - secondary_started) * 1000
        for view in secondary_response.data[:-1]:
            validate_product_view(product, view, require_current_quality=False)
        secondary_view = secondary_response.data[-1]
        validate_product_view(product, secondary_view)
        secondary_hash = warmup_content_fingerprint(secondary_response.data)
        if product.delivery is DeliveryClass.DURABLE:
            bar_alignment = validate_final_bar_warmup_windows(
                primary_response.data,
                secondary_response.data,
            )
            primary_hash = str(bar_alignment["primary_content_sha256"])
            secondary_hash = str(bar_alignment["secondary_content_sha256"])
        else:
            validate_replica_views(product, primary_view, secondary_view)
    else:
        secondary_response = await _strict_snapshot_for_c2(
            secondary,
            product=product,
            requirement=requirement,
            timeout_seconds=snapshot_timeout_seconds,
        )
        secondary_latency_ms = (time.perf_counter() - secondary_started) * 1000
        secondary_view = secondary_response.data
        primary_hash, secondary_hash = validate_replica_views(
            product, primary_view, secondary_view
        )
    observed_at_ns = time.time_ns()
    return (
        primary_hash,
        secondary_hash,
        primary_latency_ms,
        secondary_latency_ms,
        {
            "primary": compact_view_quality(primary_view, observed_at_ns=observed_at_ns),
            "secondary": compact_view_quality(secondary_view, observed_at_ns=observed_at_ns),
        },
        bar_alignment,
    )


def _quote_status_has_live_provider(
    product: AcceptanceProduct,
    requirement,
    status,
) -> bool:
    """Validate the common identity and provider-session invariants for a quote retry."""
    quality = status.quality
    return (
        product.feed.value == "QUOTE"
        and requirement.max_session_liveness_ms is not None
        and status.instrument_uid == requirement.instrument_uid
        and status.feed is requirement.feed
        and quality.policy_id == requirement.source_policy_id
        and quality.provider_session_state == "LIVE"
        and quality.provider_session_liveness_ms is not None
        and quality.provider_session_liveness_ms <= requirement.max_session_liveness_ms
        and quality.complete
        and not quality.gap_open
    )


def _quiet_quote_is_retryable(
    product: AcceptanceProduct,
    requirement,
    status,
) -> bool:
    """Allow a quiet, connected BBO session to wait for a fresh quote."""
    quality = status.quality
    return (
        _quote_status_has_live_provider(product, requirement, status)
        and quality.state == "STALE"
        and quality.event_recency_state == "STALE"
        and not quality.execution_eligible
    )


def _fresh_quote_status_is_retryable(
    product: AcceptanceProduct,
    requirement,
    status,
) -> bool:
    """Handle a quote that became fresh between a stale snapshot and status read."""
    quality = status.quality
    return (
        _quote_status_has_live_provider(product, requirement, status)
        and quality.state == "LIVE"
        and quality.event_recency_state == "LIVE"
        and quality.execution_eligible
    )


def _book_snapshot_status_is_refreshable(
    product: AcceptanceProduct,
    requirement,
    status,
) -> bool:
    """Permit a bounded retry while a verified book snapshot renews.

    Snapshot delivery has no provider-session liveness contract. The stale
    response remains rejected; this only permits a subsequent strict read while
    the typed book state is identity-matched, complete and gap-free.
    """

    quality = status.quality
    return (
        product.feed.value == "BOOK_SNAPSHOT"
        and status.instrument_uid == requirement.instrument_uid
        and status.feed is requirement.feed
        and quality.policy_id == requirement.source_policy_id
        and quality.state in {"LIVE", "STALE"}
        and quality.event_recency_state in {"LIVE", "STALE"}
        and quality.provider_session_state == "NOT_APPLICABLE"
        and quality.provider_session_liveness_ms is None
        and quality.complete
        and not quality.gap_open
    )


async def _wait_for_live_snapshot_retry(
    client: AsyncDataLayerClient,
    *,
    product: AcceptanceProduct,
    requirement,
    error: DataLayerError,
    deadline: float,
    retry_delay_seconds: float = _QUIET_QUOTE_RETRY_SECONDS,
) -> None:
    if error.code != "DATA_STALE":
        raise error
    if product.feed.value not in {"QUOTE", "BOOK_SNAPSHOT", *_QUIET_CONTINUITY_FEEDS}:
        raise error
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ContinuityError(
            "DATA_STALE",
            f"C2 strict {product.feed.value} did not obtain fresh data before its deadline",
        ) from error
    try:
        status = await asyncio.wait_for(client.feed_status(requirement), timeout=remaining)
    except TimeoutError as timeout:
        raise ContinuityError(
            "DATA_STALE", f"C2 live {product.feed.value} status did not return before its deadline"
        ) from timeout
    if product.feed.value == "QUOTE":
        retryable = _quiet_quote_is_retryable(
            product, requirement, status
        ) or _fresh_quote_status_is_retryable(product, requirement, status)
        retry_delay_seconds = _QUIET_QUOTE_RETRY_SECONDS
    elif product.feed.value == "BOOK_SNAPSHOT":
        retryable = _book_snapshot_status_is_refreshable(product, requirement, status)
        retry_delay_seconds = _BOOK_SNAPSHOT_RETRY_SECONDS
    else:
        retryable = (
            _quiet_continuity_status_is_observable(product, requirement, status)
            or _fresh_continuity_status_is_observable(product, requirement, status)
        )
    if not retryable:
        required_state = (
            "a verified complete, gap-free snapshot state"
            if product.feed.value == "BOOK_SNAPSHOT"
            else "a live provider session"
        )
        raise ContinuityError(
            "DATA_STALE",
            f"C2 strict {product.feed.value} retry requires {required_state}",
        ) from error
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ContinuityError(
            "DATA_STALE",
            f"C2 strict {product.feed.value} did not obtain fresh data before its deadline",
        ) from error
    await asyncio.sleep(min(retry_delay_seconds, remaining))


async def _strict_snapshot_for_c2(
    client: AsyncDataLayerClient,
    *,
    product: AcceptanceProduct,
    requirement,
    timeout_seconds: float,
) -> object:
    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ContinuityError(
                "DATA_STALE",
                f"C2 strict {product.feed.value} did not obtain fresh data before its deadline",
            )
        try:
            return await asyncio.wait_for(client.snapshot(requirement), timeout=remaining)
        except DataLayerError as error:
            await _wait_for_live_snapshot_retry(
                client,
                product=product,
                requirement=requirement,
                error=error,
                deadline=deadline,
            )
            continue
        except TimeoutError as timeout:
            raise ContinuityError(
                "DATA_STALE",
                f"C2 strict {product.feed.value} did not obtain fresh data before its deadline",
            ) from timeout


@asynccontextmanager
async def _strict_warmup_then_stream_for_c2(
    client: AsyncDataLayerClient,
    *,
    product: AcceptanceProduct,
    requirement,
    timeout_seconds: float,
    resume_restored_state: bool = False,
) -> AsyncIterator[object]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ContinuityError(
                "DATA_STALE",
                f"C2 strict {product.feed.value} did not obtain fresh data before its deadline",
            )
        context = client.warmup_then_stream(
            requirement,
            resume_restored_state=resume_restored_state,
        )
        try:
            session = await asyncio.wait_for(context.__aenter__(), timeout=remaining)
        except DataLayerError as error:
            await _wait_for_live_snapshot_retry(
                client,
                product=product,
                requirement=requirement,
                error=error,
                deadline=deadline,
            )
            continue
        except TimeoutError as timeout:
            raise ContinuityError(
                "DATA_STALE",
                f"C2 strict {product.feed.value} did not obtain fresh data before its deadline",
            ) from timeout
        try:
            yield session
        except BaseException:
            if await context.__aexit__(*sys.exc_info()):
                return
            raise
        else:
            await context.__aexit__(None, None, None)
            return


async def _stream_resume(
    product: AcceptanceProduct,
    *,
    identity: WorkloadIdentity,
    primary_url: str,
    secondary_url: str,
    grpc_target: str,
    state_dir: Path,
    timeout_seconds: float,
    client_factory: Callable[..., AsyncDataLayerClient] | None = None,
) -> tuple[int | None, int | None, tuple[str, ...], tuple[str, ...]]:
    if product.delivery is not DeliveryClass.DURABLE:
        return None, None, (), ()
    cursor_path = _cursor_path(state_dir, product)
    requirement = _c2_requirement(sdk_requirement(product))
    stream_requirement = requirement
    event_timeout_seconds = _stream_event_timeout_seconds(product, timeout_seconds)
    quiet_continuity_observation = _allows_quiet_continuity_observation(product, requirement)
    quiet_final_bar_handoff = _allows_quiet_final_bar_handoff(product)
    quiet_primary = False
    no_event_sessions: list[str] = []
    first_controls: list[str] = []
    first_offset: int | None = None
    first_client = _receipt_client(
        identity,
        base_url=primary_url,
        grpc_target=grpc_target,
        cursor_path=cursor_path,
        timeout_seconds=timeout_seconds,
        client_factory=client_factory,
    )
    try:
        async with _strict_warmup_then_stream_for_c2(
            first_client,
            product=product,
            requirement=stream_requirement,
            timeout_seconds=timeout_seconds,
        ) as session:
            if quiet_continuity_observation or quiet_final_bar_handoff:
                first, first_controls = await _next_data_or_timeout(
                    session,
                    timeout_seconds=min(
                        event_timeout_seconds,
                        _QUIET_CONTINUITY_STREAM_OBSERVATION_SECONDS,
                    ),
                )
                if first is None:
                    _require_signed_cursor_controls(first_controls)
                    if quiet_final_bar_handoff:
                        no_event_sessions.append(
                            await _verify_quiet_final_bar_current(
                                first_client,
                                product=product,
                                requirement=requirement,
                                timeout_seconds=timeout_seconds,
                            )
                        )
                    else:
                        no_event_sessions.append(await _classify_no_event_continuity_session(
                            first_client,
                            product=product,
                            requirement=requirement,
                            timeout_seconds=timeout_seconds,
                        ))
                    quiet_primary = True
            else:
                first, first_controls = await _next_data(
                    session,
                    timeout_seconds=event_timeout_seconds,
                )
            if not quiet_primary:
                assert first is not None
                first_replay_only = False
                try:
                    first_view = market_data_view_from_stream(
                        first,
                        template=session.warmup.data[-1],
                        requirement=stream_requirement,
                    )
                except ContinuityError as error:
                    # A snapshot cursor can race a delayed provider frame. The
                    # frame is never execution input: accept it only as signed
                    # replay state and immediately re-read strict current V2
                    # quality below. Gaps, identity violations and every other
                    # continuity error remain fail-closed.
                    if error.code != "DATA_STALE":
                        raise
                    first_view = market_data_view_from_stream(
                        first,
                        template=session.warmup.data[-1],
                        requirement=stream_requirement,
                        replay_only=True,
                    )
                    first_replay_only = True
                validate_product_view(
                    product,
                    first_view,
                    require_current_quality=not first_replay_only,
                    **({"state_replay": True} if first_replay_only else {}),
                )
                session.acknowledge(first)
                first_offset = first.logical_offset
                if first_replay_only:
                    current = await _strict_snapshot_for_c2(
                        first_client,
                        product=product,
                        requirement=requirement,
                        timeout_seconds=timeout_seconds,
                    )
                    validate_product_view(product, current.data)
    finally:
        await first_client.close()

    resumed_client = _receipt_client(
        identity,
        base_url=secondary_url,
        grpc_target=grpc_target,
        cursor_path=cursor_path,
        timeout_seconds=timeout_seconds,
        client_factory=client_factory,
    )
    try:
        if quiet_primary:
            # No first event means no cursor checkpoint exists to resume. Both
            # reconnect sessions must accept the signed cursor and independently
            # prove either fresh executable or quiet non-executable session
            # quality. This is observation evidence only, never replay.
            async with _strict_warmup_then_stream_for_c2(
                resumed_client,
                product=product,
                requirement=stream_requirement,
                timeout_seconds=timeout_seconds,
            ) as session:
                observed, observed_controls = await _next_data_or_timeout(
                    session,
                    timeout_seconds=min(
                        event_timeout_seconds,
                        _QUIET_CONTINUITY_STREAM_OBSERVATION_SECONDS,
                    ),
                )
                if observed is None:
                    _require_signed_cursor_controls(observed_controls)
                    if quiet_final_bar_handoff:
                        no_event_sessions.append(
                            await _verify_quiet_final_bar_current(
                                resumed_client,
                                product=product,
                                requirement=requirement,
                                timeout_seconds=timeout_seconds,
                            )
                        )
                    else:
                        no_event_sessions.append(await _classify_no_event_continuity_session(
                            resumed_client,
                            product=product,
                            requirement=requirement,
                            timeout_seconds=timeout_seconds,
                        ))
                else:
                    observed_view = market_data_view_from_stream(
                        observed,
                        template=session.warmup.data[-1],
                        requirement=stream_requirement,
                    )
                    validate_product_view(product, observed_view)
                    session.acknowledge(observed)
                    no_event_sessions.append("EVENT_AFTER_REOPEN")
                return (
                    None,
                    None,
                    tuple(first_controls + observed_controls),
                    tuple(no_event_sessions),
                )

        assert first_offset is not None
        async with _strict_warmup_then_stream_for_c2(
            resumed_client,
            product=product,
            requirement=stream_requirement,
            timeout_seconds=timeout_seconds,
            resume_restored_state=True,
        ) as session:
            acknowledged_offset = first_offset
            resumed_controls: list[str] = []
            # A historical seed intentionally starts before the current
            # snapshot so C2 can prove signed replay without waiting for the
            # next bar close. It must not be treated as a production consumer
            # catch-up cursor: authentic late backfills can make its append
            # offset far older than the current strict watermark. One
            # monotonic replay across replicas proves cursor continuity; the
            # strict snapshot below proves current executable quality.
            for _ in range(1):
                if quiet_continuity_observation:
                    resumed, controls = await _next_data_or_timeout(
                        session,
                        timeout_seconds=min(
                            event_timeout_seconds,
                            _QUIET_CONTINUITY_STREAM_OBSERVATION_SECONDS,
                        ),
                    )
                    if resumed is None:
                        _require_signed_cursor_controls(controls)
                        no_event_session = await _classify_no_event_continuity_session(
                            resumed_client,
                            product=product,
                            requirement=requirement,
                            timeout_seconds=timeout_seconds,
                        )
                        return (
                            None,
                            None,
                            tuple(first_controls + controls),
                            ("CURSOR_ACKNOWLEDGED", f"{no_event_session}_AFTER_CURSOR"),
                        )
                else:
                    resumed, controls = await _next_data(
                        session,
                        timeout_seconds=event_timeout_seconds,
                    )
                resumed_controls.extend(controls)
                # This context was opened with `resume_restored_state=True`.
                # Its bounded frame proves cursor recovery only, even when the
                # durable offset is newer than the reconnect snapshot watermark.
                # A strict V2 snapshot below is the sole current/executable
                # attestation after reconnect.
                replay_only = True
                resumed_view = market_data_view_from_stream(
                    resumed,
                    template=session.warmup.data[-1],
                    requirement=stream_requirement,
                    **({"replay_only": True} if replay_only else {}),
                )
                validate_product_view(
                    product,
                    resumed_view,
                    require_current_quality=not replay_only,
                    **({"state_replay": True} if replay_only else {}),
                )
                validate_resume_offsets(
                    acknowledged_offset=acknowledged_offset,
                    resumed_offset=resumed.logical_offset,
                )
                session.acknowledge(resumed)
                acknowledged_offset = resumed.logical_offset
                if replay_only:
                    # The replay frame is state recovery only. A fresh strict
                    # V2 read is required before C2 can attest current quality,
                    # including after a stale first stream frame.
                    current = await _strict_snapshot_for_c2(
                        resumed_client,
                        product=product,
                        requirement=requirement,
                        timeout_seconds=timeout_seconds,
                    )
                    validate_product_view(product, current.data)
                return (
                    first_offset,
                    acknowledged_offset,
                    tuple(first_controls + resumed_controls),
                    (),
                )
            raise AssertionError("signed cursor replay did not emit a data event")
    finally:
        await resumed_client.close()


async def _certify_product(
    product: AcceptanceProduct,
    *,
    identity: WorkloadIdentity,
    primary_url: str,
    secondary_url: str,
    grpc_target: str,
    state_dir: Path,
    timeout_seconds: float,
    client_factory: Callable[..., AsyncDataLayerClient] | None = None,
) -> dict[str, object]:
    primary = _receipt_client(
        identity,
        base_url=primary_url,
        grpc_target=grpc_target,
        cursor_path=state_dir / "query-primary.json",
        timeout_seconds=timeout_seconds,
        client_factory=client_factory,
    )
    secondary = _receipt_client(
        identity,
        base_url=secondary_url,
        grpc_target=grpc_target,
        cursor_path=state_dir / "query-secondary.json",
        timeout_seconds=timeout_seconds,
        client_factory=client_factory,
    )
    try:
        (
            primary_hash,
            secondary_hash,
            primary_ms,
            secondary_ms,
            quality,
            bar_alignment,
        ) = await _query_product_with_quality(
            product,
            primary=primary,
            secondary=secondary,
            timeout_seconds=timeout_seconds,
        )
    finally:
        await primary.close()
        await secondary.close()
    acknowledged, resumed, controls, no_event_sessions = await _stream_resume(
        product,
        identity=identity,
        primary_url=primary_url,
        secondary_url=secondary_url,
        grpc_target=grpc_target,
        state_dir=state_dir,
        timeout_seconds=timeout_seconds,
        client_factory=client_factory,
    )
    result = compact_receipt_evidence(
        product,
        primary_hash=primary_hash,
        secondary_hash=secondary_hash,
        primary_latency_ms=primary_ms,
        secondary_latency_ms=secondary_ms,
        acknowledged_offset=acknowledged,
        resumed_offset=resumed,
    )
    result["release_quality"] = quality
    if bar_alignment is not None:
        result["bar_replica_alignment"] = bar_alignment
    result["control_codes"] = list(controls)
    result["stream_handoff"] = _stream_handoff_mode(
        product,
        acknowledged_offset=acknowledged,
        resumed_offset=resumed,
        no_event_sessions=no_event_sessions,
    )
    if no_event_sessions:
        result["stream_no_event_sessions"] = list(no_event_sessions)
    return result


@contextmanager
def _cursor_directory(path: str | None) -> Iterator[Path]:
    if path is None:
        with tempfile.TemporaryDirectory(prefix="qdl-phase103-receipt-") as value:
            yield Path(value)
        return
    directory = Path(path).expanduser().resolve()
    if directory.exists():
        raise ValueError("Phase 10.3 cursor directory must not already exist")
    directory.mkdir(mode=0o700, parents=True)
    try:
        yield directory
    finally:
        shutil.rmtree(directory)


def _identities(args: argparse.Namespace, products: tuple[AcceptanceProduct, ...]):
    by_consumer: dict[str, AcceptanceProduct] = {}
    for product in products:
        by_consumer.setdefault(product.consumer_id, product)
    return {
        "trading-system.paper.stable": _identity(
            product=by_consumer["trading-system.paper.stable"],
            certificate_file=args.trading_tls_certificate_file,
            private_key_file=args.trading_tls_private_key_file,
            jwt_private_key_file=args.trading_jwt_private_key_file,
            jwt_key_id=args.trading_jwt_key_id,
            tls_ca_file=args.tls_ca_file,
            issuer=args.issuer,
            audience=args.audience,
        ),
        "alpha.binance.paper.stable": _identity(
            product=by_consumer["alpha.binance.paper.stable"],
            certificate_file=args.alpha_tls_certificate_file,
            private_key_file=args.alpha_tls_private_key_file,
            jwt_private_key_file=args.alpha_jwt_private_key_file,
            jwt_key_id=args.alpha_jwt_key_id,
            tls_ca_file=args.tls_ca_file,
            issuer=args.issuer,
            audience=args.audience,
        ),
    }


def _validated_packet(packet_path: Path, runtime_dir: Path) -> dict[str, object]:
    try:
        packet = json.loads(packet_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("Phase 10.3 handoff packet cannot be read") from error
    if not isinstance(packet, dict):
        raise ValueError("Phase 10.3 handoff packet root is invalid")
    bundle = validate_prepared_shared_primary_bundle(packet, runtime_dir=runtime_dir)
    if bundle.get("status") != "PASS" or packet.get("authority", {}).get("mode") != "RUST_PRIMARY":
        raise ValueError("Phase 10.3 handoff packet is not an active RUST_PRIMARY bundle")
    return packet


async def run(args: argparse.Namespace) -> dict[str, object]:
    packet = _validated_packet(args.handoff_packet, args.runtime_dir)
    catalog = StableSourceCatalog.load(args.catalog)
    acquisition = StableAcquisitionPlan.load(args.acquisition, catalog=catalog)
    scope = build_consumer_acceptance_scope(
        (args.trading_manifest, args.alpha_manifest),
        catalog=catalog,
        acquisition=acquisition,
    )
    identities = _identities(args, scope.products)
    semaphore = asyncio.Semaphore(args.concurrency)

    async def certify(product: AcceptanceProduct) -> dict[str, object]:
        async with semaphore:
            try:
                return await _certify_product(
                    product,
                    identity=identities[product.consumer_id],
                    primary_url=args.primary_url,
                    secondary_url=args.secondary_url,
                    grpc_target=args.grpc_target,
                    state_dir=state_dir,
                    timeout_seconds=args.timeout_seconds,
                )
            except Exception as error:
                raise RuntimeError(
                    "V2 receipt failed "
                    f"consumer={product.consumer_id} "
                    f"instrument={product.instrument_id} "
                    f"feed={product.feed.value} interval={product.interval}"
                ) from error

    with _cursor_directory(args.cursor_dir) as state_dir:
        results = await asyncio.gather(*(certify(product) for product in scope.products))
    return {
        "schema": "qdl.phase103.consumer-receipt-acceptance.v1",
        "status": "PASS",
        "expected_authority": "RUST_PRIMARY",
        "packet_sha256": packet["packet_sha256"],
        "scope_sha256": scope.sha256,
        "catalog_revision": scope.catalog_revision,
        "acquisition_revision": scope.acquisition_revision,
        "product_count": len(results),
        "durable_product_count": sum(
            item.delivery is DeliveryClass.DURABLE for item in scope.products
        ),
        "pass_through_product_count": sum(
            item.delivery is DeliveryClass.PROVIDER_PASS_THROUGH
            for item in scope.products
        ),
        "excluded": [item.evidence() for item in scope.excluded],
        "products": results,
        "cursor_directory_removed": True,
        "provider_connections": 0,
        "order_actions": 0,
        "secret_values_recorded": False,
    }


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    value.add_argument("--acquisition", type=Path, default=DEFAULT_ACQUISITION)
    value.add_argument("--trading-manifest", type=Path, default=DEFAULT_TRADING_MANIFEST)
    value.add_argument("--alpha-manifest", type=Path, default=DEFAULT_ALPHA_MANIFEST)
    value.add_argument("--primary-url", required=True)
    value.add_argument("--secondary-url", required=True)
    value.add_argument("--grpc-target", required=True)
    value.add_argument("--handoff-packet", type=Path, required=True)
    value.add_argument("--runtime-dir", type=Path, required=True)
    value.add_argument("--tls-ca-file", required=True)
    value.add_argument("--trading-tls-certificate-file", required=True)
    value.add_argument("--trading-tls-private-key-file", required=True)
    value.add_argument("--trading-jwt-private-key-file", required=True)
    value.add_argument("--trading-jwt-key-id", required=True)
    value.add_argument("--alpha-tls-certificate-file", required=True)
    value.add_argument("--alpha-tls-private-key-file", required=True)
    value.add_argument("--alpha-jwt-private-key-file", required=True)
    value.add_argument("--alpha-jwt-key-id", required=True)
    value.add_argument("--issuer", default="https://identity.qdl.stable.internal")
    value.add_argument("--audience", default="qdl-v2-stable")
    value.add_argument("--timeout-seconds", type=float, default=15.0)
    value.add_argument("--concurrency", type=int, default=4)
    value.add_argument("--cursor-dir")
    return value


def main() -> int:
    args = parser().parse_args()
    if not 5.0 <= args.timeout_seconds <= 60.0:
        raise SystemExit("--timeout-seconds must be between 5 and 60")
    if not 1 <= args.concurrency <= 8:
        raise SystemExit("--concurrency must be between 1 and 8")
    result = asyncio.run(run(args))
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
