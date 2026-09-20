#!/usr/bin/env python3
"""Read-only typed quality inventory for the sealed V2 binding catalog.

This auditor feeds durable facts, acquisition state and session evidence into
the same ``BindingQualityDecision`` policy used by the stable query edge. It
keeps event age, ingest-to-durable latency and last-durable-append age separate;
none of them is consumer-call-to-usable latency.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from qdl.common.v1 import common_pb2  # noqa: E402
from qdl.consumer.manifest import ConsumerManifestLoader  # noqa: E402
from qdl.data_quality.binding_decision import (  # noqa: E402
    BindingQualityInput,
    ComponentEvidence,
    evaluate_binding_quality,
)
from qdl.marketdata.v2 import market_data_pb2  # noqa: E402
from qdl.runtime.mark_index_lineage import paired_mark_index_lineage  # noqa: E402
from qdl.runtime.session_liveness import StableSessionLivenessReader  # noqa: E402
from qdl.runtime.stable_catalog import StableSourceCatalog  # noqa: E402
from qdl.runtime.stable_deployment import StableAcquisitionPlan  # noqa: E402


def newest_by_partition(db: Path) -> dict[tuple[str, str], tuple[bytes, int, int]]:
    """Return newest durable payload, commit and watermark facts per partition."""

    # Never use immutable=1: this spool may append while the external auditor
    # reads it, and immutable mode may return stale or torn WAL pages.
    connection = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            "SELECT e.stream, e.partition_key, e.payload, e.committed_at_ns, e.logical_offset "
            "FROM events AS e JOIN ("
            "  SELECT stream, partition_key, MAX(logical_offset) AS top "
            "  FROM events GROUP BY stream, partition_key"
            ") AS t ON e.stream = t.stream AND e.partition_key = t.partition_key "
            "      AND e.logical_offset = t.top"
        ).fetchall()
    finally:
        connection.close()
    return {
        (str(stream), str(partition)): (
            bytes(payload), int(committed), int(logical_offset)
        )
        for stream, partition, payload, committed, logical_offset in rows
    }


def _flag_names(envelope: market_data_pb2.EventEnvelope) -> tuple[str, ...]:
    return tuple(
        common_pb2.QualityFlag.Name(value).removeprefix("QUALITY_FLAG_")
        for value in envelope.quality_flags
    )


def _market_open(binding, now_ns: int) -> bool:
    if binding.continuous_calendar:
        return True
    from qdl.domain.calendar import trading_calendar_for_id

    return trading_calendar_for_id(binding.instrument.session_calendar_id).is_open_ns(now_ns)


def _requirements_by_key(manifest) -> dict[tuple[str, object, str | None], Any]:
    return {
        (item.instrument_uid, item.feed, item.interval): item
        for item in manifest.requirements
    }


def _default_policy(binding) -> tuple[str, int | None, int]:
    """Capability-only rows cannot claim an execution consumer policy."""

    policy = (
        "OBSERVE"
        if binding.feed.value in {"TRADE", "BOOK_DELTA", "MARK_INDEX_PRICE"}
        else "BLOCK"
    )
    return policy, None, binding.stale_after_ms


def _components(binding, acquisition, envelope, *, now_ns: int) -> tuple[ComponentEvidence, ...]:
    if binding.feed.value != "MARK_INDEX_PRICE" or acquisition.mark_index is None:
        return ()
    try:
        lineage = paired_mark_index_lineage(envelope)
    except ValueError:
        return ()
    cadence = dict(acquisition.mark_index.component_quiet_after_ms)
    if "BOTH" in cadence:
        cadence = {"MARK": cadence["BOTH"], "INDEX": cadence["BOTH"]}
    if set(cadence) != {"MARK", "INDEX"}:
        return ()
    return (
        ComponentEvidence(
            "MARK",
            max(0, (now_ns - lineage.mark_received_at_ns) // 1_000_000),
            int(cadence["MARK"]),
        ),
        ComponentEvidence(
            "INDEX",
            max(0, (now_ns - lineage.index_received_at_ns) // 1_000_000),
            int(cadence["INDEX"]),
        ),
    )


def decision_row(
    *,
    binding,
    acquisition,
    requirement,
    stored: tuple[bytes, int, int] | None,
    now_ns: int,
    session_reader: StableSessionLivenessReader,
) -> dict[str, object]:
    """Build one bounded audit row without changing runtime/durable state."""

    consumer_bound = requirement is not None
    if requirement is None:
        event_policy, session_limit_ms, event_limit_ms = _default_policy(binding)
    else:
        event_policy = requirement.effective_event_recency_policy.value
        session_limit_ms = requirement.max_session_liveness_ms
        event_limit_ms = min(
            binding.stale_after_ms,
            requirement.max_freshness_ms or binding.stale_after_ms,
        )

    envelope = None
    committed_at_ns = None
    watermark_offset = 0
    flags: tuple[str, ...] = ("CAPABILITY_ONLY",) if not consumer_bound else ()
    session_state = "NOT_APPLICABLE"
    session_liveness_ms = None
    components: tuple[ComponentEvidence, ...] = ()
    generation_matches = True
    config_matches = True
    gap_open = False
    book_verified = True
    final_bar = True
    event_age_ms = None
    ingest_to_durable_latency_ms = None
    if stored is not None:
        payload, committed_at_ns, watermark_offset = stored
        envelope = market_data_pb2.EventEnvelope()
        envelope.ParseFromString(payload)
        flags += _flag_names(envelope)
        observed_ns = (
            int(envelope.received_at_ns)
            if binding.freshness_basis == "PROVIDER_CONFIRMATION"
            else (
                int(envelope.bar.close_time_ns)
                if envelope.WhichOneof("payload") == "bar"
                else int(envelope.source_event_time_ns)
            )
        )
        event_age_ms = max(0, (now_ns - observed_ns) // 1_000_000)
        ingest_to_durable_latency_ms = max(
            0, (committed_at_ns - int(envelope.received_at_ns)) // 1_000_000
        )
        gap_open = any(
            value in {"SEQUENCE_GAP_BEFORE", "OUT_OF_ORDER", "RESYNC_REQUIRED"}
            for value in flags
        )
        payload_name = envelope.WhichOneof("payload")
        if payload_name in {"book_snapshot", "book_delta"}:
            book = getattr(envelope, payload_name)
            book_verified = bool(book.sequence_verified) and int(book.book_generation) >= 1
        if payload_name == "bar":
            final_bar = bool(envelope.bar.is_final)
        if session_limit_ms is not None:
            session = session_reader.status(
                venue=envelope.venue,
                market=envelope.market,
                source_session_id=envelope.source_session_id,
                connection_generation=int(envelope.connection_generation),
                config_revision=max(1, int(envelope.config_revision)),
                now_ns=now_ns,
            )
            session_state = session.state
            session_liveness_ms = session.liveness_ms
            flags += session.flags
            generation_matches = "SOURCE_SESSION_AMBIGUOUS" not in session.flags
            config_matches = "SOURCE_SESSION_CONFIG_MISMATCH" not in session.flags
        components = _components(binding, acquisition, envelope, now_ns=now_ns)

    decision = evaluate_binding_quality(
        BindingQualityInput(
            binding_id=binding.binding_id,
            instrument_uid=binding.instrument.instrument_uid,
            feed=binding.feed.value,
            source_role=binding.source_role,
            authoritative=binding.authoritative,
            acquisition_enabled=acquisition.enabled,
            acquisition_mode=acquisition.mode,
            market_open=_market_open(binding, now_ns),
            event_present=envelope is not None,
            event_age_ms=event_age_ms,
            event_limit_ms=event_limit_ms,
            event_recency_policy=event_policy,
            session_state=session_state,
            session_liveness_ms=session_liveness_ms,
            session_limit_ms=session_limit_ms,
            delivery_semantics=binding.delivery_semantics,
            components=components,
            generation_matches=generation_matches,
            config_matches=config_matches,
            gap_open=gap_open,
            book_verified=book_verified,
            final_bar=final_bar,
            require_final_bar=binding.require_final_bar,
            watermark_offset=watermark_offset,
            flags=flags,
        )
    ).as_mapping()
    decision.update({
        "consumer_scope": "BOUND" if consumer_bound else "CAPABILITY_ONLY",
        "event_age_ms": event_age_ms,
        "ingest_to_durable_latency_ms": ingest_to_durable_latency_ms,
        "last_durable_append_age_ms": (
            max(0, (now_ns - committed_at_ns) // 1_000_000)
            if committed_at_ns is not None
            else None
        ),
    })
    return decision


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spool", default="/st/shared/canonical-cache.sqlite3")
    parser.add_argument("--catalog", default="/runtime/stable-source-bindings.yaml")
    parser.add_argument("--acquisition", default="/runtime/stable-acquisition-bindings.yaml")
    parser.add_argument(
        "--manifest",
        default=str(ROOT / "consumers/stable/trading-system-paper.yaml"),
    )
    parser.add_argument("--session-liveness-root", default="/st/runtime/session-liveness")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--only-failures", action="store_true")
    args = parser.parse_args()

    catalog = StableSourceCatalog.load(args.catalog)
    acquisition = StableAcquisitionPlan.load(args.acquisition, catalog=catalog)
    manifest = ConsumerManifestLoader.load(args.manifest)
    requirements = _requirements_by_key(manifest)
    newest = newest_by_partition(Path(args.spool))
    now_ns = time.time_ns()
    session_reader = StableSessionLivenessReader(args.session_liveness_root)
    acquisition_by_id = {item.binding_id: item for item in acquisition.bindings}
    rows = [
        decision_row(
            binding=binding,
            acquisition=acquisition_by_id[binding.binding_id],
            requirement=requirements.get(binding.requirement_key),
            stored=newest.get((binding.canonical_stream, binding.partition_key)),
            now_ns=now_ns,
            session_reader=session_reader,
        )
        for binding in catalog.bindings
    ]
    unexpected = [
        row for row in rows
        if row["availability"] == "ACTIVE" and row["state"] != "LIVE"
    ]
    result = {
        "schema": "qdl.binding-quality-audit.v1",
        "catalog_binding_count": len(rows),
        "manifest_consumer_id": manifest.consumer_id,
        "manifest_revision": manifest.manifest_revision,
        "counts_by_availability": dict(sorted(Counter(
            str(row["availability"]) for row in rows
        ).items())),
        "counts_by_state": dict(sorted(Counter(str(row["state"]) for row in rows).items())),
        "unexpected_count": len(unexpected),
        "rows": sorted(
            unexpected if args.only_failures else rows,
            key=lambda value: str(value["binding_id"]),
        ),
    }
    if args.format == "json":
        print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    else:
        print(f"catalog bindings : {result['catalog_binding_count']}")
        print(f"consumer         : {manifest.consumer_id} revision={manifest.manifest_revision}")
        print(f"availability     : {result['counts_by_availability']}")
        print(f"states           : {result['counts_by_state']}")
        print(f"unexpected       : {result['unexpected_count']}")
        for row in result["rows"]:
            print(
                f"{row['binding_id']:55s} {row['feed']:16s} "
                f"{row['availability']:22s} {row['state']:13s} "
                f"event_age_ms={row['event_age_ms']} "
                f"ingest_to_durable_ms={row['ingest_to_durable_latency_ms']} "
                f"last_durable_append_age_ms={row['last_durable_append_age_ms']} "
                f"reasons={','.join(row['reason_codes'])}"
            )
    return 1 if unexpected else 0


if __name__ == "__main__":
    raise SystemExit(main())
