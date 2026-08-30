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
from typing import AsyncIterator, Iterator

ROOT = Path(__file__).resolve().parents[1]

# A historical alpha BAR seed normally replays one retained event. A bounded
# repair can append a few older canonical records after that seed, so C2 must
# consume through the strict snapshot watermark rather than assume one event
# is enough. This is acceptance-only, never an unbounded consumer catch-up.
_MAX_HISTORICAL_REPLAY_CATCHUP_EVENTS = 16
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
from qdl.adapters.intervals import canonical_interval_ms
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
_QUIET_TRADE_STREAM_OBSERVATION_SECONDS = 2.0


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


def _cursor_path(state_dir: Path, product: AcceptanceProduct) -> Path:
    identity = "|".join(product.identity).encode()
    return state_dir / f"{hashlib.sha256(identity).hexdigest()}.json"


def _stream_event_timeout_seconds(
    product: AcceptanceProduct,
    request_timeout_seconds: float,
) -> float:
    """Wait through one final BAR close without exceeding its freshness SLA."""
    if product.feed.value != "BAR":
        return request_timeout_seconds
    if product.interval is None:
        raise ValueError("BAR receipt product must declare its canonical interval")
    interval_seconds = canonical_interval_ms(product.interval) / 1_000
    settlement_seconds = min(15.0, max(5.0, interval_seconds / 4.0))
    wait_seconds = max(request_timeout_seconds, interval_seconds + settlement_seconds)
    max_freshness_ms = product.requirement.max_freshness_ms
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


def _historical_bar_replay_requirement(requirement, *, latest_open_time_ns: int):
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
    if (
        latest_open_time_ns <= 2 * interval_ns
        or latest_open_time_ns % interval_ns
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
    """Observe one stream without manufacturing data when a trade channel is quiet."""
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


def _allows_quiet_trade_observation(product: AcceptanceProduct, requirement) -> bool:
    """Whether this no-order C2 probe may observe a quiet live trade channel."""
    return (
        product.delivery is DeliveryClass.DURABLE
        and product.feed.value == "TRADE"
        and requirement.effective_event_recency_policy is SdkStalePolicy.OBSERVE
        and requirement.max_session_liveness_ms is not None
    )


def _quiet_trade_status_is_observable(product: AcceptanceProduct, requirement, status) -> bool:
    """Keep a quiet session distinct from fresh/executable market data."""
    quality = status.quality
    return (
        _allows_quiet_trade_observation(product, requirement)
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


async def _assert_quiet_trade_session(
    client: AsyncDataLayerClient,
    *,
    product: AcceptanceProduct,
    requirement,
    timeout_seconds: float,
) -> None:
    try:
        status = await asyncio.wait_for(
            client.feed_status(requirement),
            timeout=timeout_seconds,
        )
    except TimeoutError as timeout:
        raise ContinuityError(
            "DATA_STALE", "C2 quiet TRADE status did not return before its deadline"
        ) from timeout
    if not _quiet_trade_status_is_observable(product, requirement, status):
        raise ContinuityError(
            "DATA_STALE", "C2 quiet TRADE observation requires a live non-executable session"
        )


def _stream_handoff_mode(
    product: AcceptanceProduct,
    *,
    acknowledged_offset: int | None,
    resumed_offset: int | None,
) -> str:
    if (acknowledged_offset is None) != (resumed_offset is None):
        raise ValueError("C2 stream handoff evidence is incomplete")
    if product.delivery is not DeliveryClass.DURABLE:
        return "NOT_APPLICABLE"
    if acknowledged_offset is None:
        return "QUIET_OBSERVED_NO_CURSOR"
    return "DURABLE_CURSOR_REPLAYED"


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
    requirement = sdk_requirement(product)
    bar_alignment: dict[str, object] | None = None
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
            timeout_seconds=timeout_seconds,
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
            timeout_seconds=timeout_seconds,
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


def _quiet_quote_is_retryable(
    product: AcceptanceProduct,
    requirement,
    status,
) -> bool:
    """Allow only quiet, connected BBO observation to wait for a fresh quote."""
    quality = status.quality
    return (
        product.feed.value == "QUOTE"
        and requirement.max_session_liveness_ms is not None
        and status.instrument_uid == requirement.instrument_uid
        and status.feed is requirement.feed
        and quality.policy_id == requirement.source_policy_id
        and quality.state == "STALE"
        and quality.event_recency_state == "STALE"
        and quality.provider_session_state == "LIVE"
        and quality.provider_session_liveness_ms is not None
        and quality.provider_session_liveness_ms <= requirement.max_session_liveness_ms
        and quality.complete
        and not quality.gap_open
        and not quality.execution_eligible
    )


async def _wait_for_quiet_quote_retry(
    client: AsyncDataLayerClient,
    *,
    product: AcceptanceProduct,
    requirement,
    error: DataLayerError,
    deadline: float,
    retry_delay_seconds: float = _QUIET_QUOTE_RETRY_SECONDS,
) -> None:
    if product.feed.value != "QUOTE" or error.code != "DATA_STALE":
        raise error
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ContinuityError(
            "DATA_STALE", "C2 strict QUOTE did not obtain fresh data before its deadline"
        ) from error
    try:
        status = await asyncio.wait_for(client.feed_status(requirement), timeout=remaining)
    except TimeoutError as timeout:
        raise ContinuityError(
            "DATA_STALE", "C2 quiet QUOTE status did not return before its deadline"
        ) from timeout
    if not _quiet_quote_is_retryable(product, requirement, status):
        raise ContinuityError(
            "DATA_STALE", "C2 quiet QUOTE retry requires a live provider session"
        ) from error
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ContinuityError(
            "DATA_STALE", "C2 strict QUOTE did not obtain fresh data before its deadline"
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
                "DATA_STALE", "C2 strict QUOTE did not obtain fresh data before its deadline"
            )
        try:
            return await asyncio.wait_for(client.snapshot(requirement), timeout=remaining)
        except DataLayerError as error:
            await _wait_for_quiet_quote_retry(
                client,
                product=product,
                requirement=requirement,
                error=error,
                deadline=deadline,
            )
            continue
        except TimeoutError as timeout:
            raise ContinuityError(
                "DATA_STALE", "C2 strict QUOTE did not obtain fresh data before its deadline"
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
                "DATA_STALE", "C2 strict QUOTE did not obtain fresh data before its deadline"
            )
        context = client.warmup_then_stream(
            requirement,
            resume_restored_state=resume_restored_state,
        )
        try:
            session = await asyncio.wait_for(context.__aenter__(), timeout=remaining)
        except DataLayerError as error:
            await _wait_for_quiet_quote_retry(
                client,
                product=product,
                requirement=requirement,
                error=error,
                deadline=deadline,
            )
            continue
        except TimeoutError as timeout:
            raise ContinuityError(
                "DATA_STALE", "C2 strict QUOTE did not obtain fresh data before its deadline"
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
) -> tuple[int | None, int | None, tuple[str, ...]]:
    if product.delivery is not DeliveryClass.DURABLE:
        return None, None, ()
    cursor_path = _cursor_path(state_dir, product)
    requirement = sdk_requirement(product)
    historical_replay = _uses_historical_bar_replay(product)
    stream_requirement = requirement
    strict_watermark: int | None = None
    event_timeout_seconds = _stream_event_timeout_seconds(product, timeout_seconds)
    quiet_trade_observation = _allows_quiet_trade_observation(product, requirement)
    quiet_primary = False
    first_controls: list[str] = []
    first_offset: int | None = None
    first_client = _client(
        identity,
        base_url=primary_url,
        grpc_target=grpc_target,
        cursor_path=cursor_path,
        timeout_seconds=timeout_seconds,
    )
    try:
        if historical_replay:
            # This remains the product's actual contract check.  The replay
            # cursor below is deliberately older only to make the bounded C2
            # reconnect proof independent of the next 15m/1h close.
            strict_warmup = await first_client.warmup(requirement)
            strict_current = strict_warmup.data[-1]
            validate_product_view(product, strict_current)
            strict_watermark = strict_warmup.watermark_offset
            stream_requirement = _historical_bar_replay_requirement(
                requirement,
                latest_open_time_ns=int(strict_current.payload.open_time_ns),
            )
            event_timeout_seconds = timeout_seconds
        async with _strict_warmup_then_stream_for_c2(
            first_client,
            product=product,
            requirement=stream_requirement,
            timeout_seconds=timeout_seconds,
        ) as session:
            if quiet_trade_observation:
                first, first_controls = await _next_data_or_timeout(
                    session,
                    timeout_seconds=min(
                        event_timeout_seconds,
                        _QUIET_TRADE_STREAM_OBSERVATION_SECONDS,
                    ),
                )
                if first is None:
                    await _assert_quiet_trade_session(
                        first_client,
                        product=product,
                        requirement=requirement,
                        timeout_seconds=timeout_seconds,
                    )
                    quiet_primary = True
            else:
                first, first_controls = await _next_data(
                    session,
                    timeout_seconds=event_timeout_seconds,
                )
            if not quiet_primary:
                assert first is not None
                first_view = market_data_view_from_stream(
                    first,
                    template=session.warmup.data[-1],
                    requirement=stream_requirement,
                )
                validate_product_view(
                    product,
                    first_view,
                    require_current_quality=not historical_replay,
                )
                session.acknowledge(first)
                first_offset = first.logical_offset
    finally:
        await first_client.close()

    resumed_client = _client(
        identity,
        base_url=secondary_url,
        grpc_target=grpc_target,
        cursor_path=cursor_path,
        timeout_seconds=timeout_seconds,
    )
    try:
        if quiet_primary:
            # No first event means no cursor checkpoint exists to resume. Both
            # replicas still open their governed stream and must either deliver
            # a validated event or independently prove a live, non-executable
            # quiet session. This is observation evidence only, never replay.
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
                        _QUIET_TRADE_STREAM_OBSERVATION_SECONDS,
                    ),
                )
                if observed is None:
                    await _assert_quiet_trade_session(
                        resumed_client,
                        product=product,
                        requirement=requirement,
                        timeout_seconds=timeout_seconds,
                    )
                else:
                    observed_view = market_data_view_from_stream(
                        observed,
                        template=session.warmup.data[-1],
                        requirement=stream_requirement,
                    )
                    validate_product_view(product, observed_view)
                return None, None, tuple(first_controls + observed_controls)

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
            maximum = (
                _MAX_HISTORICAL_REPLAY_CATCHUP_EVENTS
                if historical_replay
                else 1
            )
            for _ in range(maximum):
                resumed, controls = await _next_data(
                    session,
                    timeout_seconds=event_timeout_seconds,
                )
                resumed_controls.extend(controls)
                resumed_view = market_data_view_from_stream(
                    resumed,
                    template=session.warmup.data[-1],
                    requirement=stream_requirement,
                )
                validate_product_view(
                    product,
                    resumed_view,
                    require_current_quality=not historical_replay,
                )
                validate_resume_offsets(
                    acknowledged_offset=acknowledged_offset,
                    resumed_offset=resumed.logical_offset,
                )
                session.acknowledge(resumed)
                acknowledged_offset = resumed.logical_offset
                if (
                    strict_watermark is None
                    or acknowledged_offset >= strict_watermark
                ):
                    return (
                        first_offset,
                        acknowledged_offset,
                        tuple(first_controls + resumed_controls),
                    )
            raise AssertionError(
                "historical BAR replay did not converge through the strict current watermark"
            )
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
) -> dict[str, object]:
    primary = _client(
        identity,
        base_url=primary_url,
        grpc_target=grpc_target,
        cursor_path=state_dir / "query-primary.json",
        timeout_seconds=timeout_seconds,
    )
    secondary = _client(
        identity,
        base_url=secondary_url,
        grpc_target=grpc_target,
        cursor_path=state_dir / "query-secondary.json",
        timeout_seconds=timeout_seconds,
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
    acknowledged, resumed, controls = await _stream_resume(
        product,
        identity=identity,
        primary_url=primary_url,
        secondary_url=secondary_url,
        grpc_target=grpc_target,
        state_dir=state_dir,
        timeout_seconds=timeout_seconds,
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
    )
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
