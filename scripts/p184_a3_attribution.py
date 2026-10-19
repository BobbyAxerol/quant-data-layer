#!/usr/bin/env python3
"""A3 external attribution (P18.4 stage-100, owner 2026-10-18): pure classification of G4 samples and G9 losses.

Amendment config/v2/p184-stage100-a3-external-attribution.json (frozen before the run; rationale
upgrade/evidence/releases/v2.2.4/a3-external-attribution.md). A unit is EXTERNAL only when, for the SAME event or the
same seconds, ALL of the following hold; anything else is UNKNOWN (or INTERNAL_*), and UNKNOWN counts as FAIL exactly
as before:
 (i)   origin evidence: a fresh-connection probe record of the SAME venue event (same channel, symbol, venue id AND
       venue timestamp) that was on time while the production lane was late (EXTERNAL_SESSION_UPSTREAM; a late probe is
       not evidence in v1: EXTERNAL_VENUE_WIDE is never assigned); or a VENUE-originated session end logged by the
       ingestor on the unit's own lane (OKX cause EOF/CLOSE_FRAME/READ_ERROR/HEARTBEAT_TIMEOUT/NOTICE*, Binance peer
       reset/close or heartbeat failure; a DL-initiated end or connect failure in the episode makes it incomplete) with
       the joined lane timeline (EXTERNAL_VENUE_RESET / EXTERNAL_SESSION_INTERRUPTION). A different probe event in the
       same second is supporting evidence only;
 (ii)  internal segments of that unit within the frozen limits: G4 same-event split (Kafka CreateTime - DL receipt,
       observer receipt - CreateTime, and the internal total within the row limit); G9: the DL path (core raw_age,
       gateway dispatch lag) inside its limits over the recovery seconds; and the host-local checks at the unit's exact
       seconds (ingestor cgroup throttling 0, thread run-queue delay, socket receive queue not held, another lane of
       the same ingestor reading);
 (iii) the host clock bound recorded and within limit on both sides of the unit.
EXTERNAL units stay in every distribution and are reported (count, durations, recovery). Verdicts are recomputed on
the remaining units with the unchanged thresholds; a row whose EXTERNAL units exceed the frozen cap, or an episode whose
recovery exceeds the frozen budget, FAILs. G2 has no per-sample DL receipt clock on either trace basis (native writer
slices carry no receive/canonical/dispatch clock; the Python MARK records have envelope_received_at NOT_MEASURED and a
Query reference basis), so every G2 tail is UNKNOWN in A3 v1 by construction and G2 is scored exactly as before.

Inputs are the campaign's own files (observer aggregates, probe outputs, A3 evidence) parsed by the callers; this
module does no I/O except the small readers at the bottom. Stdlib only.
"""
from __future__ import annotations

import json
from pathlib import Path
import re

A3_SCHEMA = "qdl.p184.stage100-a3.v1"
A3_ID = "A3"
# EXTERNAL_VENUE_WIDE is defined by the owner's condition but NOT assigned in v1 (review finding 1): a late fresh probe
# can be the probe's own scheduling on this host, and no probe-side scheduler evidence is recorded.
EXTERNAL_CLASSES = ("EXTERNAL_SESSION_UPSTREAM", "EXTERNAL_VENUE_RESET",
                    "EXTERNAL_SESSION_INTERRUPTION")
FAULT_EVENTS = ("qdl_native_session_heartbeat_failed", "qdl_native_session_disconnected")
# Review 2026-10-18 (finding 2): only VENUE-originated session ends start an episode. OKX logs the cause of every
# session end (qdl-native-raw-ingestor.rs OkxSessionEnd::cause); DL-initiated ends (STALENESS_HANDOVER = IDR-112
# self-recycle of a late lane, BOOK_RESYNC, WRITE_ERROR) and connect failures never qualify, and one inside an
# episode's span makes the episode incomplete. Binance logs no cause: its I/O error string must name a peer reset or
# close; a heartbeat failure qualifies only with the other-lane-reading and local checks.
OKX_VENUE_CAUSES = ("EOF", "CLOSE_FRAME", "READ_ERROR", "HEARTBEAT_TIMEOUT", "NOTICE", "NOTICE_HANDOVER")
BINANCE_VENUE_ERRORS = ("Connection reset by peer", "reset without closing handshake", "provider closed the WebSocket",
                        "Connection closed normally", "Connection timed out")
NON_VENUE_EVENTS = ("qdl_native_connect_failed", "qdl_native_staleness_recycled", "qdl_native_standby_failed")
# Review finding 12: G9 losses eligible for A3 are session-type refusals only.
G9_SESSION_REASONS = ("TRADE_SESSION_UNVERIFIED", "EXECUTION_CONTEXT_NOT_EXECUTION_ELIGIBLE")
SESSION_RE = re.compile(r"^(?P<lane>[a-zA-Z]+(?:-[A-Za-z]+)*-\d{3})-(?P<gen>\d+)-(?P<started>\d{12,})$")
G9_FEED_TO_CANONICAL = {"TRADE": "trade", "QUOTE": "quote", "BOOK_DELTA": "book_delta",
                        "BOOK_SNAPSHOT": "book_snapshot", "MARK_INDEX_PRICE": "mark_index_price", "BAR": "bar"}
NS = 1_000_000_000
MS = 1_000_000


def segment_limits_for(row: str, by_row: dict) -> dict | None:
    """Frozen per-row internal-segment limits; an OKX inverse row uses its pooled OKX row (same pipeline)."""
    return by_row.get(row) or by_row.get(row.replace("|OKX_INVERSE|", "|OKX|"))


def is_external(attribution: str | None) -> bool:
    return attribution in EXTERNAL_CLASSES


# ---------------------------------------------------------------- identities

def lane_of_session(session_id) -> tuple[str, int] | None:
    """Canonical source_session_id -> (ingestor lane name, generation). 'binance-USDM-public-quote-001-64-<ns>' ->
    ('binance-public-quote-001', 64); 'okx-public-001-67019-<ns>' -> ('okx-public-001', 67019)."""
    m = SESSION_RE.match(str(session_id or ""))
    if not m:
        return None
    return m.group("lane").replace("binance-USDM-", "binance-"), int(m.group("gen"))


def venue_id(record: dict) -> tuple[str, int] | None:
    """(probe channel, venue id) of one canonical observer record, the key a fresh probe records for the same event:
    OKX books seqId (source_sequence g<gen>:<seqId>); Binance depth u (g<gen>:<u>), bookTicker u (quote seq), trade
    id (trade seq), markPrice E (mark seq E:E:receipt:receipt:digest). None when the venue/feed has no such probe."""
    identity = record.get("identity") or {}
    venue, feed, seq = identity.get("venue"), record.get("feed"), str(identity.get("source_sequence") or "")
    try:
        if venue == "OKX" and feed in ("book_delta", "book_snapshot") and seq.startswith("g"):
            return "books", int(seq.split(":", 1)[1])
        if venue == "BINANCE":
            if feed in ("book_delta", "book_snapshot") and seq.startswith("g"):
                return "depth", int(seq.split(":", 1)[1])
            if feed == "quote":
                return "bookTicker", int(seq)
            if feed == "trade":
                return "trade", int(seq)
            if feed == "mark_index_price":
                return "markPrice", int(seq.split(":", 1)[0])
    except (ValueError, IndexError):
        return None
    return None


def lane_map(records) -> dict[tuple[str, str, str], set]:
    """(venue, canonical feed, native symbol) -> {lane} from observed canonical identities (any window)."""
    out: dict[tuple[str, str, str], set] = {}
    for record in records:
        identity = record.get("identity") or {}
        lane = lane_of_session(identity.get("source_session_id"))
        if lane and record.get("feed"):
            out.setdefault((identity.get("venue"), record["feed"], identity.get("native_symbol")), set()).add(lane[0])
    return out


# ---------------------------------------------------------------- lane timeline (episodes)

def _progress(timeline, runtime=None):
    return [r for r in timeline if r.get("event") == "qdl_native_raw_ingestor_progress"
            and (runtime is None or r.get("runtime") == runtime)]


def _lane_at(progress, lane, at_ns, *, before=True):
    """The progress record (and its lane row) nearest at/before (or after) at_ns that carries `lane`."""
    rows = [(p, next((x for x in p["lanes"] if x.get("lane") == lane), None)) for p in progress]
    rows = [(p, x) for p, x in rows if x is not None]
    if before:
        rows = [(p, x) for p, x in rows if p["at_ns"] <= at_ns]
        return rows[-1] if rows else (None, None)
    rows = [(p, x) for p, x in rows if p["at_ns"] > at_ns]
    return rows[0] if rows else (None, None)


def venue_fault(record: dict) -> bool:
    """True for a session end the venue/network originated (see OKX_VENUE_CAUSES / BINANCE_VENUE_ERRORS)."""
    event = record.get("event")
    if event == "qdl_native_session_heartbeat_failed":
        return True
    if event != "qdl_native_session_disconnected":
        return False
    if record.get("runtime") == "OKX" or record.get("cause") is not None:
        return record.get("cause") in OKX_VENUE_CAUSES
    return any(text in str(record.get("error") or "") for text in BINANCE_VENUE_ERRORS)


def non_venue_end(record: dict) -> bool:
    """A session end or connect problem the DL itself caused or that does not prove a venue event."""
    if record.get("event") in NON_VENUE_EVENTS:
        return True
    return record.get("event") == "qdl_native_session_disconnected" and not venue_fault(record)


def fault_lane(timeline, fault) -> tuple[str | None, str]:
    """Lane of one fault event: its own `lane` field (OKX) else the unique lane of the same runtime whose generation in
    the latest progress record at/before the fault equals the fault's generation (Binance logs carry no lane)."""
    if fault.get("lane"):
        return fault["lane"], "EVENT_FIELD"
    progress = [p for p in _progress(timeline, fault.get("runtime")) if p["at_ns"] <= fault["at_ns"]]
    if not progress:
        return None, "NO_PROGRESS_BEFORE_FAULT"
    candidates = [x.get("lane") for x in progress[-1]["lanes"] if x.get("generation") == fault.get("generation")]
    if len(candidates) != 1:
        return None, f"AMBIGUOUS_GENERATION ({len(candidates)} lanes)"
    return candidates[0], "GENERATION_MATCH"


def episodes(timeline, *, merge_s: float = 60.0, recovery_budget_s: float = 60.0) -> list[dict]:
    """Per lane, faults <= merge_s apart form one episode with its joined timeline:
    last_good_ns (last frame of the faulting generation: progress at - last_frame_age), faults [{at_ns, event, gen,
    error}], first_new_bounds_ns [lower, upper] (the first progress interval in which the lane delivered frames on a
    newer generation), recovered_upper_ns, other_lane_reading (another lane of the same runtime had frames and
    read_gap max <= 1 s in the interval covering each fault), missing (timeline elements not found)."""
    faults = [r for r in timeline if r.get("role") == "ingestor" and venue_fault(r)]
    by_lane: dict[str, list] = {}
    unassigned = []
    for fault in faults:
        lane, how = fault_lane(timeline, fault)
        if lane is None:
            unassigned.append({**fault, "lane_resolution": how})
            continue
        by_lane.setdefault(lane, []).append({**fault, "lane_resolution": how})
    out = []
    for lane, items in sorted(by_lane.items()):
        items.sort(key=lambda r: r["at_ns"])
        groups, current = [], [items[0]]
        for item in items[1:]:
            if item["at_ns"] - current[-1]["at_ns"] <= merge_s * NS:
                current.append(item)
            else:
                groups.append(current)
                current = [item]
        groups.append(current)
        for group in groups:
            out.append(_episode(timeline, lane, group, merge_s=merge_s, recovery_budget_s=recovery_budget_s))
    for fault in unassigned:
        out.append({"lane": None, "runtime": fault.get("runtime"), "faults": [_fault_view(fault)],
                    "first_fault_ns": fault["at_ns"], "missing": [f"lane ({fault['lane_resolution']})"],
                    "complete": False})
    return out


def _fault_view(fault):
    return {k: fault.get(k) for k in ("at_ns", "event", "generation", "error", "cause", "lane_resolution", "attempt")}


def _episode(timeline, lane, group, *, merge_s: float, recovery_budget_s: float) -> dict:
    runtime = group[0].get("runtime")
    progress = _progress(timeline, runtime)
    first, last = group[0], group[-1]
    missing = []
    record, row = _lane_at(progress, lane, first["at_ns"], before=True)
    last_good = None
    if row is not None and row.get("last_frame_age_ms") is not None:
        last_good = int(record["at_ns"] - float(row["last_frame_age_ms"]) * MS)
    else:
        missing.append("last good frame")
    faulted_gens = {f.get("generation") for f in group if f.get("generation") is not None}
    newest = max(faulted_gens) if faulted_gens else None
    first_new = None
    previous_at = last["at_ns"]
    for p in progress:
        if p["at_ns"] <= last["at_ns"]:
            continue
        if p["at_ns"] > last["at_ns"] + recovery_budget_s * NS:  # review finding 13: bounded search
            break
        x = next((y for y in p["lanes"] if y.get("lane") == lane), None)
        if x is not None and newest is not None and (x.get("generation") or 0) > newest and (x.get("frames") or 0) > 0:
            first_new = [previous_at, p["at_ns"]]
            break
        previous_at = p["at_ns"]
    if first_new is None:
        missing.append("first frame of a newer generation")
    other = []
    for fault in group:
        record, _ = _lane_at(progress, lane, fault["at_ns"], before=False)
        if record is None:
            other.append(False)
            continue
        other.append(any(y.get("lane") != lane and (y.get("frames") or 0) > 0
                         and (y.get("read_gap_ms") or {}).get("max") is not None
                         and y["read_gap_ms"]["max"] <= 1000.0 for y in record["lanes"]))
    if not all(other):
        missing.append("another lane of the ingestor reading at the fault")
    if row is not None and first.get("generation") is not None and row.get("generation") != first.get("generation") \
            and first.get("lane_resolution") == "EVENT_FIELD":
        missing.append("fault generation differs from the lane's generation before it")
    ends = [r for r in timeline if r.get("role") == "ingestor" and non_venue_end(r)
            and r.get("runtime") == runtime and (r.get("lane") in (None, lane))
            and first["at_ns"] - merge_s * NS <= r["at_ns"] <= last["at_ns"] + merge_s * NS
            and (r.get("lane") == lane or fault_lane(timeline, r)[0] == lane)]
    if ends:
        missing.append(f"non-venue session end on the lane in the episode ({ends[0].get('cause') or ends[0]['event']})")
    return {"lane": lane, "runtime": runtime, "faults": [_fault_view(f) for f in group],
            "first_fault_ns": first["at_ns"], "last_fault_ns": last["at_ns"], "last_good_ns": last_good,
            "first_new_bounds_ns": first_new, "recovered_upper_ns": first_new[1] if first_new else None,
            "outage_upper_s": (round((first_new[1] - last_good) / NS, 3) if first_new and last_good else None),
            "other_lane_reading": other, "missing": missing, "complete": not missing}


# ---------------------------------------------------------------- local + DL path checks

def local_check(local: dict, runtime: str, from_ns: int, to_ns: int, limits: dict) -> tuple[bool, list[str]]:
    """Host-local checks of one ingestor over [from_ns, to_ns] (padded to whole seconds): every second has a local
    sample (gap <= 2 s) with 0 throttled periods and worst-thread run-queue delay <= limit, and no socket of its netns
    held unread bytes for >= RX_PERSIST samples; plus the host clock bound on both sides <= limit."""
    problems = []
    lo, hi = from_ns - NS, to_ns + NS
    samples = sorted((s for s in local.get("local") or [] if s.get("kind") == "local" and s.get("runtime") == runtime
                      and lo <= s["at_ns"] <= hi + NS), key=lambda s: s["at_ns"])
    if not samples or samples[0]["at_ns"] > lo + 2 * NS or samples[-1]["at_ns"] < hi - NS:
        problems.append("no local scheduler sample covering the seconds")
    elif any(b["at_ns"] - a["at_ns"] > 2 * NS for a, b in zip(samples, samples[1:])):
        problems.append("local scheduler samples have a gap > 2 s")
    if any(type(s.get("d_nr_throttled")) is not int or type(s.get("runq_wait_ms_max_thread")) not in (int, float)
           for s in samples):
        problems.append("a local sample without its throttling / run-queue fields")
    for s in samples:
        if s.get("d_nr_throttled"):
            problems.append(f"ingestor CPU throttled ({s['d_nr_throttled']} periods)")
            break
    worst = max((s.get("runq_wait_ms_max_thread") or 0 for s in samples), default=None)
    if worst is not None and worst > limits["ingestor_runq_wait_ms_max_thread"]:
        problems.append(f"ingestor thread run-queue delay {worst} ms > {limits['ingestor_runq_wait_ms_max_thread']}")
    lines = [json.loads(line) for line in (local.get("tcp") or {}).get(runtime, []) if line.strip()]
    if any(t.get("kind") == "tcp_error" and t.get("at_ns", 0) <= hi + NS for t in lines):
        problems.append("socket sampler error before the end of the seconds")
    tcp = sorted((t for t in lines if t.get("kind") == "tcp" and lo <= t["at_ns"] <= hi + NS), key=lambda t: t["at_ns"])
    if not tcp or tcp[0]["at_ns"] > lo + 2 * NS or tcp[-1]["at_ns"] < hi - NS \
            or any(b["at_ns"] - a["at_ns"] > 2 * NS for a, b in zip(tcp, tcp[1:])):
        problems.append("socket receive-queue samples do not cover the seconds without a gap > 2 s")
    elif any(type(t.get("rx_persist_samples_max")) is not int for t in tcp):
        problems.append("a socket sample without its receive-queue field")
    elif any(t["rx_persist_samples_max"] >= limits["rx_persist_samples"] for t in tcp):
        problems.append("ingestor socket receive queue held unread bytes (INTERNAL_RECEIVE_QUEUE)")
    clocks = sorted((c for c in local.get("clock") or [] if c.get("clock")), key=lambda c: c["at_ns"])
    before = [c for c in clocks if c["at_ns"] <= from_ns]
    after = [c for c in clocks if c["at_ns"] >= to_ns]
    if not before or not after or from_ns - before[-1]["at_ns"] > limits["clock_bracket_s"] * NS \
            or after[0]["at_ns"] - to_ns > limits["clock_bracket_s"] * NS:
        problems.append("host clock bound not recorded on both sides")
    elif max(before[-1]["clock"]["bound_ms"], after[0]["clock"]["bound_ms"]) > limits["host_clock_bound_ms"]:
        problems.append("host clock bound over limit")
    return not problems, problems


def dl_path_check(timeline, from_ns: int, to_ns: int, limits: dict) -> tuple[bool, list[str]]:
    """DL internal path over [from_ns, to_ns]: every core progress line has raw_age (receipt -> core normalize) max
    <= limit, and each gateway's dispatch-lag delta (dispatch - canonical CreateTime) has its highest non-empty bucket
    bound <= limit (the overflow bucket fails). Both must have coverage in the span (+-15 s)."""
    problems = []
    pad = 15 * NS
    containers = {r["container"] for r in timeline if r.get("role") == "core"}
    cores = [r for r in timeline if r.get("role") == "core" and from_ns - pad <= r["at_ns"] <= to_ns + pad]
    if not containers or {r["container"] for r in cores} != containers:
        problems.append("a core without progress covering the seconds")
    for r in cores:
        age = (r.get("raw_age_websocket_ms") or r.get("raw_age_ms") or {}).get("max")
        if age is None:
            problems.append(f"core progress without raw_age ({r['container']})")
            break
        if age > limits["core_raw_age_ms_max"]:
            problems.append(f"core raw_age {age} ms > {limits['core_raw_age_ms_max']} ({r['container']})")
            break
    gateways: dict[str, list] = {}
    for r in timeline:
        if r.get("role") == "gateway" and r.get("dispatch_lag_buckets"):
            gateways.setdefault(r["container"], []).append(r)
    bounds = list(limits["gateway_lag_bounds_ms"])
    covered = set()
    if not gateways:
        problems.append("no gateway dispatch-lag rows")
    for container, rows in gateways.items():
        rows.sort(key=lambda r: r["at_ns"])
        for a, b in zip(rows, rows[1:]):
            if b["at_ns"] < from_ns - pad or a["at_ns"] > to_ns + pad:
                continue
            covered.add(container)
            delta = [y - x for x, y in zip(a["dispatch_lag_buckets"], b["dispatch_lag_buckets"])]
            top = max((i for i, d in enumerate(delta) if d > 0), default=None)
            if any(d < 0 for d in delta) or top is None:
                problems.append(f"gateway dispatch counters reset or no dispatch in the interval ({container})")
                break
            edge = bounds[top] if top < len(bounds) else float("inf")
            if edge > limits["gateway_dispatch_lag_ms_max"]:
                problems.append(f"gateway dispatch lag up to {edge} ms > {limits['gateway_dispatch_lag_ms_max']} "
                                f"({container})")
                break
    if gateways and covered != set(gateways):
        problems.append("a gateway without a dispatch-lag delta covering the seconds")
    return not problems, problems


# ---------------------------------------------------------------- G4: one exceedance event

def classify_g4_event(event: dict, *, row_limit_ms: float, probes: dict, episodes_: list, local: dict,
                      limits: dict, segments: dict | None) -> dict:
    """One retained G4 event above its row limit -> {'attribution', 'reasons', ...}. probes: {(channel, SYMBOL, id):
    first fresh-connection receipt ns} of the window; episodes_: lane episodes of the run."""
    identity = event.get("identity") or {}
    source, received = event.get("source_event_ns"), event.get("received_at_ns")
    produced_ms, seen = event.get("producer_timestamp_ms"), event.get("observer_receipt_wall_ns")
    out = {"row_limit_ms": row_limit_ms, "native_symbol": identity.get("native_symbol"),
           "source_session_id": identity.get("source_session_id"), "source_sequence": identity.get("source_sequence"),
           "topic": event.get("topic"), "partition": event.get("partition"), "offset": event.get("offset"),
           "source_event_ns": source, "received_at_ns": received, "producer_timestamp_ms": produced_ms,
           "observer_receipt_wall_ns": seen,
           "visibility_ms": round((seen - source) / MS, 3) if (seen and source) else event.get("visibility_ms")}
    if not (source and received and produced_ms and seen):
        return {**out, "attribution": "UNKNOWN", "reasons": ["a same-event clock is missing"]}
    if not segments:
        return {**out, "attribution": "UNKNOWN", "reasons": ["no frozen internal-segment limit for the row"]}
    venue_ms = (received - source) / MS
    canonical_ms = produced_ms - received / MS
    consumer_ms = (seen - produced_ms * MS) / MS
    internal_ms = (seen - received) / MS
    out.update(venue_to_dl_ms=round(venue_ms, 3), receive_to_canonical_ms=round(canonical_ms, 3),
               canonical_to_consumer_ms=round(consumer_ms, 3), internal_total_ms=round(internal_ms, 3))
    if canonical_ms > segments["receive_to_canonical_ms"] or consumer_ms > segments["canonical_to_consumer_ms"] \
            or internal_ms > row_limit_ms:
        return {**out, "attribution": "INTERNAL_SEGMENT_OVER_LIMIT",
                "reasons": ["an internal segment of the same event is over its frozen limit"]}
    lane = lane_of_session(identity.get("source_session_id"))
    runtime = identity.get("venue")
    reasons = []
    ok, problems = local_check(local, runtime, source, received, limits)
    if not ok:
        return {**out, "attribution": "UNKNOWN", "reasons": problems}
    origin = None
    key = venue_id(event)
    fresh = probes.get((key[0], identity.get("native_symbol"), key[1])) if key else None
    if fresh is not None and source // MS not in set(fresh.get("ts_ms") or ()):
        reasons.append("fresh-connection record with the same id has a different venue timestamp (not the same event)")
        fresh = None
    elif fresh is None:
        reasons.append("no fresh-connection record of the same event (a different event is supporting only)")
    if fresh is not None:
        fresh_ms = (fresh["r"] - source) / MS
        out["fresh_same_event_lag_ms"] = round(fresh_ms, 3)
        if fresh_ms <= limits["probe_healthy_ms"] and venue_ms - fresh_ms >= limits["lane_late_margin_ms"]:
            origin = "EXTERNAL_SESSION_UPSTREAM"
        else:
            # A late fresh probe is NOT venue-wide evidence in v1 (review finding 1): no probe-side scheduler record.
            reasons.append("fresh-connection record of the same event is not on time while the lane is late")
    if origin is None and lane is not None:
        for ep in episodes_:
            if ep.get("lane") != lane[0] or not ep.get("complete"):
                continue
            span = (ep["first_fault_ns"] - limits["reset_pad_s"] * NS, ep["recovered_upper_ns"] + limits["reset_pad_s"] * NS)
            gens = {f.get("generation") for f in ep["faults"]}
            if source <= span[1] and received >= span[0] and (lane[1] in gens or lane[1] > max(g or 0 for g in gens)):
                origin = "EXTERNAL_VENUE_RESET"
                out["episode_first_fault_ns"] = ep["first_fault_ns"]
                start = min(ep["first_fault_ns"], ep.get("last_good_ns") or ep["first_fault_ns"])
                out["episode_recovery_s"] = round((ep["recovered_upper_ns"] - start) / NS, 3)  # from the outage start
                break
        else:
            reasons.append("no complete session-fault episode on the event's lane in its seconds")
    if origin is None:
        return {**out, "attribution": "UNKNOWN", "reasons": reasons}
    return {**out, "attribution": origin, "reasons": []}


def rescue_g4_window(retention: dict | None, *, row_limit_ms: float, n_admitted: int, probes: dict, episodes_: list,
                     local: dict, limits: dict, segments: dict | None) -> dict:
    """One failing row-window: every ADMITTED event above the row limit must be retained (threshold <= limit, no
    overflow) and EXTERNAL; then the remaining admitted samples are all <= limit (p95 and max inside)."""
    if not retention:
        return {"rescued": False, "status": "INCOMPLETE", "reasons": ["no exceedance retention for the row (observer < v7)"]}
    if retention.get("threshold_ms") is None or retention["threshold_ms"] > row_limit_ms:
        return {"rescued": False, "status": "INCOMPLETE", "reasons": ["retention threshold above the row limit"]}
    if retention.get("overflow"):
        return {"rescued": False, "status": "INCOMPLETE",
                "reasons": [f"{retention['overflow']} exceedances beyond the retention cap"]}
    exact = lambda e: ((e["observer_receipt_wall_ns"] - e["source_event_ns"]) / MS  # noqa: E731
                       if e.get("observer_receipt_wall_ns") and e.get("source_event_ns") else (e.get("visibility_ms") or 0))
    units = [e for e in retention.get("events") or [] if exact(e) > row_limit_ms]
    maximum = retention.get("max_event") or {}
    if maximum and exact(maximum) > row_limit_ms and not any(
            (u.get("topic"), u.get("partition"), u.get("offset")) ==
            (maximum.get("topic"), maximum.get("partition"), maximum.get("offset")) for u in units):
        return {"rescued": False, "status": "INCOMPLETE", "reasons": ["the window maximum is not among the retained"]}
    classified = [classify_g4_event(u, row_limit_ms=row_limit_ms, probes=probes, episodes_=episodes_, local=local,
                                    limits=limits, segments=segments) for u in units]
    external = [c for c in classified if is_external(c["attribution"])]
    over_budget = [c for c in external if (c.get("episode_recovery_s") or 0) > limits["recovery_budget_s"]]
    window_cap = limits["max_external_fraction"] * n_admitted  # review finding 10: per window as well as per row
    rescued = bool(units) and len(external) == len(units) and not over_budget and len(external) <= window_cap
    status = ("RESCUED" if rescued else "RECOVERY_OVER_BUDGET" if over_budget else
              "NO_UNITS" if not units else "NOT_ALL_EXTERNAL" if len(external) != len(units) else "WINDOW_CAP_EXCEEDED")
    return {"rescued": rescued, "status": status, "units": len(units), "external": len(external),
            "n_admitted": n_admitted, "window_cap_units": round(window_cap, 3), "classified": classified}


# ---------------------------------------------------------------- G9: one eligibility loss

def g9_loss_events(samples, *, from_ns=None, to_ns=None, stale_after_s=45.0, legit=("TRADE_EVENT_STALE",)):
    """Loss events with their sample time and the first later sample in which the slice is ready again (the same
    distinct-snapshot rule as p184_g9_g10_writer_counters.summarize_g9: a slice newly in not_ready)."""
    reads = sorted((s for s in samples if s.get("kind") == "heartbeat"
                    and (from_ns is None or s["at_ns"] >= from_ns) and (to_ns is None or s["at_ns"] <= to_ns)),
                   key=lambda s: s["at_ns"])
    good = [s for s in reads if (s.get("sample") or {}).get("readable")
            and (s["sample"].get("age_s") or 0) <= stale_after_s and s["sample"].get("purpose")]
    key = lambda row: "|".join(str(row.get(k)) for k in ("provider", "symbol", "feed", "interval"))  # noqa: E731
    events, previous = [], None
    for index, s in enumerate(good):
        current = {key(row): row for row in s["sample"].get("not_ready") or []}
        if previous is not None:
            for k, row in current.items():
                if k in previous or row.get("reason_code") in legit:
                    continue
                restored = next((later["at_ns"] for later in good[index + 1:]
                                 if k not in {key(r) for r in later["sample"].get("not_ready") or []}), None)
                events.append({"slice": k, "provider": row.get("provider"), "symbol": row.get("symbol"),
                               "feed": row.get("feed"), "reason_code": row.get("reason_code"),
                               "component_age_ms": row.get("component_age_ms"), "sample_ns": s["at_ns"],
                               "restored_ns": restored})
        previous = current
    return events


def writer_live(samples, from_ns: int, to_ns: int, *, stale_after_s: float = 45.0) -> tuple[bool, list[str]]:
    """Every heartbeat read in [from_ns, to_ns] readable, fresh and with every worker slot LIVE (review finding 12)."""
    reads = [s for s in samples if s.get("kind") == "heartbeat" and from_ns <= s["at_ns"] <= to_ns]
    if not reads:
        return False, ["no writer heartbeat sample in the loss seconds"]
    for s in reads:
        sample = s.get("sample") or {}
        if not sample.get("readable") or (sample.get("age_s") or 0) > stale_after_s:
            return False, ["writer heartbeat unreadable or stale in the loss seconds"]
        liveness = sample.get("worker_liveness")
        if not isinstance(liveness, dict) or any(v != "LIVE" for v in liveness.values()):
            return False, ["a writer worker slot not LIVE in the loss seconds"]
    return True, []


def classify_g9_loss(loss: dict, *, lanes: dict, episodes_: list, local: dict, timeline, limits: dict,
                     samples=()) -> dict:
    out = dict(loss)
    age = loss.get("component_age_ms")
    if type(age) not in (int, float):
        return {**out, "attribution": "UNKNOWN", "reasons": ["no component age"]}
    if loss.get("reason_code") not in G9_SESSION_REASONS:
        return {**out, "attribution": "UNKNOWN", "reasons": [f"reason {loss.get('reason_code')} is not session-type"]}
    onset = int(loss["sample_ns"] - age * MS)
    candidates = lanes.get((loss.get("provider"), G9_FEED_TO_CANONICAL.get(loss.get("feed")), loss.get("symbol")))
    if not candidates or len(candidates) != 1:
        return {**out, "attribution": "UNKNOWN", "reasons": ["the slice's ingestor lane is not uniquely observed"]}
    lane = next(iter(candidates))
    out["lane"] = lane
    for ep in episodes_:
        if ep.get("lane") != lane:
            continue
        pad = limits["reset_pad_s"] * NS
        if not onset - pad <= ep["first_fault_ns"] <= loss["sample_ns"]:
            continue
        reasons = [f"timeline: {m}" for m in ep.get("missing") or []]
        if loss.get("restored_ns") is None:
            reasons.append("timeline: eligibility not restored inside the load window")
        # Review finding 3: the episode must explain the onset (the slice went stale when the lane stopped).
        if ep.get("last_good_ns") is not None and not ep["last_good_ns"] - pad <= onset <= ep["first_fault_ns"] + pad:
            reasons.append("the loss began before the lane's last good frame (not explained by the episode)")
        if reasons:
            return {**out, "attribution": "UNKNOWN", "reasons": reasons}
        start = min(onset, ep["last_good_ns"])
        ok, problems = local_check(local, ep["runtime"], start - pad, loss["restored_ns"], limits)
        ok2, problems2 = dl_path_check(timeline, start - pad, loss["restored_ns"], limits)
        ok3, problems3 = writer_live(samples, start - pad, loss["restored_ns"])
        if not (ok and ok2 and ok3):
            return {**out, "attribution": "UNKNOWN", "reasons": problems + problems2 + problems3}
        recovery_s = (loss["restored_ns"] - start) / NS  # from the outage start (onset / last good frame)
        out.update(episode_first_fault_ns=ep["first_fault_ns"], outage_upper_s=ep.get("outage_upper_s"),
                   recovery_upper_s=round(recovery_s, 3), restoration_basis="writer heartbeat sample (~30 s)")
        if recovery_s > limits["recovery_budget_s"]:
            return {**out, "attribution": "RECOVERY_OVER_BUDGET", "reasons": [
                f"recovery {round(recovery_s, 1)} s > {limits['recovery_budget_s']} s"]}
        return {**out, "attribution": "EXTERNAL_SESSION_INTERRUPTION", "reasons": []}
    return {**out, "attribution": "UNKNOWN", "reasons": ["no session-fault episode on the slice's lane in its seconds"]}


def rescore_g9(losses_classified: list, *, demanded: int | None, readable_samples: int, limits: dict,
               episodes_: list) -> dict:
    external = [x for x in losses_classified if is_external(x["attribution"])]
    remaining = [x for x in losses_classified if not is_external(x["attribution"])]
    denominator = (demanded or 0) * readable_samples
    cap = limits["max_external_fraction"] * denominator
    per_venue = {}
    for ep in episodes_:
        if any(x.get("episode_first_fault_ns") == ep.get("first_fault_ns") for x in external):
            per_venue[ep.get("runtime")] = per_venue.get(ep.get("runtime"), 0) + 1
    over = [v for v, n in per_venue.items() if n > limits["max_external_episodes_per_venue"]]
    cap_ok = len(external) <= cap and not over
    return {"external": len(external), "remaining": len(remaining), "cap_units": round(cap, 3),
            "cap_ok": cap_ok, "episodes_per_venue": per_venue, "episodes_over_cap": over,
            "losses_after_a3": len(remaining) if cap_ok else len(losses_classified)}


# ---------------------------------------------------------------- readers

def read_jsonl(path: Path) -> list[dict]:
    path = Path(path)
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def okx_books_probe_index(probe_dir: Path) -> dict:
    """{('books', instId, seqId): {'r': first fresh receipt ns, 'ts_ms': (venue ts,)}} of one OKX books probe window."""
    out = {}
    for row in read_jsonl(Path(probe_dir) / "books.jsonl"):
        if "r" in row and "i" in row and row.get("seq") is not None:
            key = ("books", row["i"], int(row["seq"]))
            if key not in out or row["r"] < out[key]["r"]:
                out[key] = {"r": row["r"], "ts_ms": (int(row["ts"]),)}
    return out


def binance_probe_index(probe_dir: Path) -> dict:
    """{(channel, SYMBOL, id): first fresh receipt ns} of one Binance probe window."""
    from scripts import p184_g4_binance_probe as probe
    path = Path(probe_dir) / "frames.jsonl"
    if not path.exists():
        return {}
    return {k: {"r": v["r"], "ts_ms": tuple(int(t) for t in (v.get("E"), v.get("T")) if t is not None)}
            for k, v in probe.read_events(path.read_text().splitlines()).items()}
