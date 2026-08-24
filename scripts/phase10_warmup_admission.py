#!/usr/bin/env python3
"""Bounded read-only V2 warmup admission for every demanded crypto BAR slice."""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import resource
import time
from typing import Any, Callable, Iterable

from qdl.query import ConsumerGrade, DataRequirement, FeedType, RecoveryPolicy
from qdl.runtime.provider_history import (
    PASS_THROUGH_QUALITY_FLAG,
    ProviderBarHistorySource,
    ProviderHistoryUnavailable,
)
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.warmup import WarmupSpecification
from qdl.warmup.executor import (
    BoundedWarmupExecutor,
    RetryableWarmupError,
)
from scripts.phase10_real_provider_admission import (
    DEFAULT_DEMAND_PATH,
    DemandSlice,
    _load_slices,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CATALOG_PATH = ROOT / "config/v2/stable-source-bindings.yaml"
MAX_BAR_SLICES = 128


class WarmupAdmissionError(RuntimeError):
    """Demand, provider bytes or canonical V2 output failed admission."""


@dataclass(frozen=True, slots=True)
class _AdmissionWork:
    slice: DemandSlice
    requirement: DataRequirement


def _bar_slices(path: Path) -> tuple[DemandSlice, ...]:
    values = tuple(item for item in _load_slices(path) if item.feed == "BAR")
    if not values or len(values) > MAX_BAR_SLICES:
        raise WarmupAdmissionError("bounded demanded BAR slice count is invalid")
    return values


def _instrument_uid(catalog: StableSourceCatalog, item: DemandSlice) -> str:
    matches = tuple(
        record.instrument_uid
        for record in catalog.instruments
        if record.identity.venue == item.venue
        and record.identity.market == item.market
        and record.identity.product_type.value == item.product_type
        and record.native_symbol == item.native_symbol
    )
    if len(matches) != 1:
        raise WarmupAdmissionError(
            f"demanded BAR identity resolves to {len(matches)} catalog instruments: "
            f"{item.key}"
        )
    return matches[0]


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(1, math.ceil(len(ordered) * fraction))
    return ordered[min(len(ordered) - 1, rank - 1)]


async def _run_async(
    demand_path: Path,
    catalog_path: Path,
    *,
    rows: int,
    timeout_seconds: float,
    clock_ns: Callable[[], int],
    binance_fetcher: Callable[..., Any] | None,
    okx_fetcher: Callable[..., Any] | None,
) -> dict[str, Any]:
    if not 1 <= rows <= 1_000:
        raise WarmupAdmissionError("admission rows must be between 1 and 1000")
    if not 1 <= timeout_seconds <= 30:
        raise WarmupAdmissionError("timeout_seconds must be between 1 and 30")
    catalog = StableSourceCatalog.load(catalog_path)
    source_kwargs: dict[str, Any] = {
        "clock_ns": clock_ns,
        "fetch_timeout_seconds": timeout_seconds,
    }
    if binance_fetcher is not None:
        source_kwargs["binance_fetcher"] = binance_fetcher
    if okx_fetcher is not None:
        source_kwargs["okx_fetcher"] = okx_fetcher
    source = ProviderBarHistorySource(catalog, **source_kwargs)
    works = tuple(
        _AdmissionWork(
            item,
            DataRequirement(
                instrument_uid=_instrument_uid(catalog, item),
                feed=FeedType.BAR,
                interval=item.interval,
                consumer_grade=ConsumerGrade.ALPHA,
                source_policy_id=item.source_policy_id,
                recovery=RecoveryPolicy.FRESH_SNAPSHOT,
                warmup=WarmupSpecification.for_rows(
                    rows,
                    max_cache_age_ms=60_000,
                    deadline_ms=int(timeout_seconds * 1_000),
                ),
            ),
        )
        for item in _bar_slices(demand_path)
    )
    executor: BoundedWarmupExecutor[_AdmissionWork, dict[str, Any]] = (
        BoundedWarmupExecutor()
    )

    async def inspect(work: _AdmissionWork) -> dict[str, Any]:
        started = time.perf_counter()
        try:
            envelopes = await asyncio.to_thread(source.history, work.requirement)
            history = await asyncio.to_thread(
                source.history_result,
                work.requirement,
                schema_digest="0" * 64,
            )
        except ProviderHistoryUnavailable as error:
            if error.problem.retryable:
                raise RetryableWarmupError(
                    str(error),
                    retry_after_ms=error.problem.retry_after_ms,
                    cause=error,
                ) from error
            raise
        if len(envelopes) != rows or len(history.items) != rows:
            raise WarmupAdmissionError(
                f"warmup cardinality differs from demand: {work.slice.key}"
            )
        envelope_opens = tuple(int(item.bar.open_time_ns) for item in envelopes)
        output_opens = tuple(int(item.payload["open_time_ns"]) for item in history.items)
        if envelope_opens != output_opens or output_opens != tuple(sorted(set(output_opens))):
            raise WarmupAdmissionError(
                f"provider/canonical BAR boundaries differ: {work.slice.key}"
            )
        if any(
            envelope.instrument_uid != work.requirement.instrument_uid
            or envelope.bar.interval != work.requirement.interval
            or not envelope.bar.is_final
            or len(envelope.raw_payload_hash) != 32
            for envelope in envelopes
        ):
            raise WarmupAdmissionError(
                f"provider canonical lineage is incomplete: {work.slice.key}"
            )
        if history.coverage.value != "FULL" or any(
            item.instrument_uid != work.requirement.instrument_uid
            or item.interval != work.requirement.interval
            or item.payload.get("is_final") is not True
            or item.quality.complete is not True
            or item.source.authoritative is not False
            or item.quality.execution_eligible is not False
            or PASS_THROUGH_QUALITY_FLAG not in item.quality.flags
            for item in history.items
        ):
            raise WarmupAdmissionError(
                f"V2 warmup output is not complete reference data: {work.slice.key}"
            )
        lineage_digest = hashlib.sha256(
            b"".join(bytes(item.raw_payload_hash) for item in envelopes)
        ).hexdigest()
        output_digest = hashlib.sha256(
            json.dumps(
                [
                    {
                        "open_time_ns": item.payload["open_time_ns"],
                        "close_time_ns": item.payload["close_time_ns"],
                        "close": item.payload["close"],
                        "correlation_id": item.contract.correlation_id,
                    }
                    for item in history.items
                ],
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        return {
            "slice": work.slice.key,
            "instrument_uid": work.requirement.instrument_uid,
            "interval": work.requirement.interval,
            "rows": rows,
            "first_open_ns": output_opens[0],
            "last_open_ns": output_opens[-1],
            "data_as_of_ns": history.data_as_of_ns,
            "raw_lineage_sha256": lineage_digest,
            "canonical_window_sha256": output_digest,
            "latency_ms": (time.perf_counter() - started) * 1_000,
        }

    cpu_before = time.process_time()
    executions = await executor.execute(
        works,
        work=inspect,
        identity=lambda item: item.requirement,
        provider=lambda item: item.slice.venue,
        deadline_ms=lambda item: item.requirement.warmup_specification.deadline_ms,
    )
    failures = tuple(item for item in executions if not item.ok)
    if failures:
        detail = "; ".join(
            f"{item.item.slice.key}={type(item.error).__name__}:{item.error}"
            for item in failures
        )
        raise WarmupAdmissionError(f"real-provider warmup failed: {detail}")
    results = [item.value for item in executions]
    assert all(result is not None for result in results)
    typed_results = [result for result in results if result is not None]
    latencies = [float(item["latency_ms"]) for item in typed_results]
    source_stats = source.stats()
    execution_stats = executor.stats()
    if source_stats["provider_source_failures"]:
        raise WarmupAdmissionError("provider source failures were observed")
    return {
        "schema": "qdl.phase10.warmup-admission.v1",
        "status": "PASS",
        "provenance": "REAL_PROVIDER_READ_ONLY",
        "demand_path": str(demand_path.resolve()),
        "catalog_path": str(catalog_path.resolve()),
        "slice_count": len(typed_results),
        "rows_per_slice": rows,
        "production_writes": 0,
        "raw_payload_persisted": False,
        "p50_ms": _percentile(latencies, 0.50),
        "p95_ms": _percentile(latencies, 0.95),
        "cpu_seconds": time.process_time() - cpu_before,
        "rss_max_kib": int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss),
        "source": source_stats,
        "executor": execution_stats,
        "slices": typed_results,
    }


def run(
    demand_path: Path = DEFAULT_DEMAND_PATH,
    catalog_path: Path = DEFAULT_CATALOG_PATH,
    *,
    rows: int = 5,
    timeout_seconds: float = 20.0,
    clock_ns: Callable[[], int] = time.time_ns,
    binance_fetcher: Callable[..., Any] | None = None,
    okx_fetcher: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    return asyncio.run(_run_async(
        demand_path,
        catalog_path,
        rows=rows,
        timeout_seconds=timeout_seconds,
        clock_ns=clock_ns,
        binance_fetcher=binance_fetcher,
        okx_fetcher=okx_fetcher,
    ))


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--demand-file", type=Path, default=DEFAULT_DEMAND_PATH)
    parser.add_argument("--catalog-file", type=Path, default=DEFAULT_CATALOG_PATH)
    parser.add_argument("--rows", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        report = run(
            args.demand_file,
            args.catalog_file,
            rows=args.rows,
            timeout_seconds=args.timeout_seconds,
        )
    except (WarmupAdmissionError, OSError, ValueError) as error:
        print(json.dumps({"status": "FAIL", "error": str(error)}, sort_keys=True))
        return 1
    rendered = json.dumps(report, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
