"""Offline candidate only: reuse the frozen Phase-3 workload, never run traffic.

Inputs are existing manifest/acceptance objects, not credentials. Quota admission
is not capacity certification. The parent owns the journal and runtime packet.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, replace
import math

from qdl.certification.phase3_consumer_load import (
    TargetIdentityDemand, TargetPoll, TargetWorkloadPlan,
    build_target_workload_plan,
)


@dataclass(frozen=True)
class TSReservation:
    consumer_id: str
    demanded_routes: int
    streams: int
    requests_per_minute: int

    def __post_init__(self):
        if (not self.consumer_id or self.demanded_routes != 60
                or any(type(v) is not int or v < 1 for v in
                       (self.streams, self.requests_per_minute))):
            raise ValueError("TS60 needs explicit positive stream and request reservations")


def _startup_requests(session):
    # SDK handoff is snapshot/warmup + Subscribe, also on resume.
    return (2 * len(session.streams) + len(session.startup_snapshots)
            + sum(p.feed.value == "BAR" for p in session.streams))


def build_capacity100_candidate(*, manifests, products_by_consumer,
                                venue_identity, instruments, ts: TSReservation,
                                gateway_subscription_limit: int,
                                profile: str = "realistic"):
    """Return (TargetWorkloadPlan, report), with no writes, sockets or admission.

    realistic: two unchanged 50-session plans, 40/30/20/10 class mix.
    stress5: 100 sessions each consuming BAR/TRADE/QUOTE/BOOK_DELTA/BOOK_SNAPSHOT,
    polling QUOTE at 1 Hz and anchoring BOOK_DELTA at startup. No extra sixth
    product. Both plans require explicit DURABLE delivery for every stream.

    TS reservations are caller-supplied workload assumptions, not observed usage.
    Gateway limit is an explicit per-replica config input. The check conservatively
    places all traffic on one replica (including headroom); no assumed balancing
    or HA multiplier. Replay RPCs have a separate limit and remain an open gate.
    """
    if profile not in {"realistic", "stress5"}:
        raise ValueError("unknown capacity profile")
    if type(gateway_subscription_limit) is not int or gateway_subscription_limit < 1:
        raise ValueError("gateway subscription limit must be a positive integer")
    alpha_ids = set(venue_identity.values())
    if not alpha_ids or ts.consumer_id in alpha_ids:
        raise ValueError("TS reservation must use a separate identity")
    if set(venue_identity) != set(instruments):
        raise ValueError("venue and instrument scopes differ")
    if any(len(values) < 2 or len(set(values)) != len(values)
           for values in instruments.values()):
        raise ValueError("each venue needs at least two distinct explicit instruments")
    if any(not value or "*" in value for values in instruments.values() for value in values):
        raise ValueError("wildcard or empty instruments are not a frozen workload")
    required_ids = alpha_ids | {ts.consumer_id}
    if not required_ids <= set(manifests) or not alpha_ids <= set(products_by_consumer):
        raise ValueError("missing alpha or TS manifest/products")
    for consumer_id in required_ids:
        quotas = manifests[consumer_id].quotas
        if any(type(v) is not int or v < 1 for v in
               (quotas.requests_per_minute, quotas.max_streams, quotas.max_buffer_events,
                quotas.max_batch_items, quotas.max_warmup_rows)):
            raise ValueError("invalid sealed quota")
        if quotas.max_batch_items > 100 or quotas.max_warmup_rows > 10_000:
            raise ValueError("query quota exceeds V2 batch/warmup service boundary")
        if quotas.max_buffer_events > 10_000:
            raise ValueError("buffer exceeds V2 10000-event service boundary")
    for consumer_id in alpha_ids:
        if any(p.consumer_id != consumer_id for p in products_by_consumer[consumer_id]):
            raise ValueError("products mix authenticated identities")
    base = build_target_workload_plan(
        stage=50, manifests=manifests, products_by_consumer=products_by_consumer,
        venue_identity=venue_identity, instruments=instruments, enforce_quota=False)
    sessions = tuple(replace(s, ordinal=i + 1)
                     for i, s in enumerate(base.sessions + base.sessions))
    if profile == "stress5":
        result = []
        for session in sessions:
            first = session.streams[0]
            selected = []
            for feed in ("BAR", "TRADE", "QUOTE", "BOOK_DELTA", "BOOK_SNAPSHOT"):
                matches = [p for p in products_by_consumer[session.consumer_id]
                           if p.venue == first.venue and p.native_symbol == first.native_symbol
                           and p.feed.value == feed and (feed != "BAR" or p.interval == "1m")]
                if len(matches) != 1:
                    raise ValueError(f"stress5 needs exactly one {feed} for {first.native_symbol}")
                selected.append(matches[0])
            result.append(replace(session, alpha_class="GRID", streams=tuple(selected),
                                  polls=(TargetPoll((selected[2],), 1.0, "SNAPSHOT"),),
                                  startup_snapshots=(selected[4],)))
        sessions = tuple(result)
    for session in sessions:
        products = (*session.streams, *session.startup_snapshots,
                    *(p for poll in session.polls for p in poll.products))
        if not 2 <= len({p.identity for p in products}) <= 5:
            raise ValueError("capacity session must declare 2..5 distinct products including startup")
        if any(getattr(getattr(p, "delivery", None), "value", None) != "DURABLE"
               for p in session.streams):
            raise ValueError("every planned stream must explicitly declare DURABLE delivery")
    coverage = {(p.venue, p.native_symbol) for s in sessions for p in s.streams}
    expected = {(v, symbol) for v, symbols in instruments.items() for symbol in symbols}
    if not expected <= coverage:
        raise ValueError("100-session profile does not cover the declared instruments")
    demands, rows = [], []
    for consumer_id in sorted(required_ids):
        mine = [s for s in sessions if s.consumer_id == consumer_id]
        is_ts = consumer_id == ts.consumer_id
        rpm = ts.requests_per_minute if is_ts else math.ceil(sum(s.requests_per_minute for s in mine))
        streams = ts.streams if is_ts else sum(len(s.streams) for s in mine)
        quota = manifests[consumer_id].quotas
        demand = TargetIdentityDemand(consumer_id, len(mine), rpm, streams,
                                      quota.requests_per_minute, quota.max_streams)
        # Bound one synchronized startup + one reconnect in a 25% burst minute.
        # For TS, caller's RPM must include its non-subscribe recovery requests.
        startup = ts.streams if is_ts else sum(_startup_requests(s) for s in mine)
        batch_items = max((len(p.products) for s in mine for p in s.polls), default=0)
        warmup_rows = 5000 if any(p.feed.value == "BAR" for s in mine for p in s.streams) else 0
        needed_rpm = max(demand.quota_needed, math.ceil(rpm * 1.25) + 2 * startup)
        rows.append(dict(consumer_id=consumer_id, role="TS" if is_ts else "ALPHA",
                         logical_alphas=len(mine), steady_rpm=rpm, streams=streams,
                         startup_requests=startup, candidate_rpm=needed_rpm,
                         candidate_streams=demand.streams_needed,
                         sealed_rpm=quota.requests_per_minute, sealed_streams=quota.max_streams,
                         rpm_shortfall=max(0, needed_rpm - quota.requests_per_minute),
                         stream_shortfall=max(0, demand.streams_needed - quota.max_streams),
                         rpm_headroom=needed_rpm - rpm,
                         stream_headroom=demand.streams_needed - streams,
                         max_buffer_events=quota.max_buffer_events,
                         required_batch_items=batch_items, required_warmup_rows=warmup_rows,
                         sealed_batch_items=quota.max_batch_items,
                         sealed_warmup_rows=quota.max_warmup_rows,
                         batch_shortfall=max(0, batch_items - quota.max_batch_items),
                         warmup_shortfall=max(0, warmup_rows - quota.max_warmup_rows)))
        if not is_ts:
            demands.append(demand)
    plan = TargetWorkloadPlan(100, sessions, tuple(demands))
    total_streams = sum(row["streams"] for row in rows)
    reserved_streams = sum(row["candidate_streams"] for row in rows)
    report = dict(
        schema="p184.capacity100.candidate.v1", status="CANDIDATE_NOT_CERTIFIED",
        profile=profile, logical_alphas=100, alpha_identity_count=len(alpha_ids),
        total_identity_count=len(required_ids), ts_demanded_routes=ts.demanded_routes,
        ts_reservation_source="CALLER_ASSUMPTION_NOT_MEASURED",
        workload_groups=(dict(zip(("CANDLE", "REALTIME", "GRID", "MULTI"), plan.class_counts))
                         if profile == "realistic" else {"STRESS5": 100}),
        products_per_alpha=dict(sorted(Counter(len({p.identity for p in
            (*s.streams, *s.startup_snapshots, *(p for poll in s.polls for p in poll.products))})
            for s in sessions).items())),
        alpha_streams=plan.stream_count, total_streams=total_streams,
        alpha_hot_rps=plan.hot_requests_per_second,
        total_steady_rpm=sum(row["steady_rpm"] for row in rows),
        candidate_total_rpm=sum(row["candidate_rpm"] for row in rows),
        identities=rows, reserved_streams=reserved_streams,
        gateway_subscription_limit=gateway_subscription_limit,
        gateway_stream_shortfall=max(0, reserved_streams - gateway_subscription_limit),
        sealed_identity_quotas_fit=all(not any(r[key] for key in
            ("rpm_shortfall", "stream_shortfall", "batch_shortfall", "warmup_shortfall")) for r in rows),
        gateway_slots_fit=reserved_streams <= gateway_subscription_limit,
        manifest_event_slots_at_reserved_streams=sum(r["candidate_streams"] * r["max_buffer_events"] for r in rows),
        assumptions=["100 logical sessions are not 100 independently authenticated identities",
                     "Fixed offered rates; no quota pacing or idle socket capacity claim",
                     "1.5x steady RPM floor; 1.25x burst plus startup and reconnect minute",
                     "1.2x stream headroom; all reserved slots must fit one replica",
                     "TS RPM input includes non-subscribe recovery operations; TS batch/warmup shape needs separate validation",
                     "BAR startup requests 5000 rows; MULTI uses a two-item reference batch",
                     "Five-stream stress consumes all five products, not five idle sockets"],
        pending_gates=["Owner approval of TS reservation and exact identity/config revisions",
                       "Driver stage100/duration and stress5 integration; existing driver stops at50",
                       "Rust MemoryPlan for exact ring/queue/replay/transport/cgroup bounds; event slots are not RSS",
                       "Replay RPC admission, per-stream byte/event bounds and recovery window",
                       "Real-provider sustained/burst throughput, CPU/throttle, RAM, I/O and queue/lag",
                       "Per-session delivery/apply latency, LOSSLESS ordering/gaps and reconnect; TS60 ready throughout"],
        cpu_budget="MEASURE_AND_SIZE_NO_FIXED_5_VCPU_CEILING",
    )
    # These are the existing Rust MemoryPlan envelopes, not new pool sizing.
    # Include Replay transport slots even though alpha_streams counts Subscribe.
    report["resource_envelopes"] = [
        dict(subscriptions=slots, replay_rpcs=32,
             transport_bytes_per_stream=96 * 1024,
             ring_mib=48, live_queue_mib=32, replay_mib=32,
             kafka_mib=32, process_reserve_mib=64,
             estimated_required_bytes=(48 + 32 + 32 + 32 + 64) * 2**20
                                      + (slots + 32) * 96 * 1024,
             observed_container_limit_bytes=512 * 2**20,
             active_stream_shortfall=max(0, total_streams - slots),
             reserved_stream_shortfall=max(0, reserved_streams - slots),
             status="SOURCE_BOUND_ESTIMATE_NOT_CAPACITY")
        for slots in (384, 1024)
    ]
    report["resource_observation"] = dict(
        source="HISTORICAL_RUNTIME_READBACK_NOT_REMEASURED", host_vcpu=16,
        stream_replica_vcpu=0.75, stream_replica_memory_mib=512,
        per_client_buffers="UNCHANGED", aggregate_live_queue_mib=32,
        warning="32 MiB global live pool does not guarantee fanout to 100 clients; "
                "measure pool contention and apply lag before sizing aggregate pools. "
                "Increasing per-client buffers to hide backlog is not optimization.")
    report["sources"] = [
        "qdl/certification/phase3_consumer_load.py: frozen stage50 and quota headroom",
        "qdl/consumer/manifest.py: ConsumerQuotas service boundaries",
        "rust/qdl-stream-gateway/src/memory.rs: existing 384/1024 MemoryPlan",
        "rust/qdl-stream-gateway/src/service.rs: separate Subscribe/Replay limits",
        "scripts/phase3_consumer_load_acceptance.py: existing driver stage restrictions",
        "DATA_LAYER_UNIFIED_IMPLEMENTATION_PLAN.md: owner amendment 2026-10-08",
    ]
    return plan, report
