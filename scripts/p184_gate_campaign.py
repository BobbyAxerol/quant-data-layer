#!/usr/bin/env python3
"""P18.4 phase D final gate-measurement campaign: one run, every probe, one G1-G14 scorecard.

Purpose: run the frozen measurement protocol of the TS gate table (upgrade/p184-phase-d-gates.md, FROZEN 2026-10-09:
steady state >= 30 min at the stage-100 workload plus one burst window and the fault windows) ONCE, with every
probe the v2.2.3 scorecard listed as missing, and score G1-G14 from that single run.

What `run` starts (all children of this process, tracked by PID, each with its own deadline; nothing matched by name):
* preflight (read-only): writer running, its delivery trace armed long enough (TS_DELIVERY_TRACE_UNTIL_UNIX_NS covers
  the load window; the writer must already have been recreated with the long-window trace, TS b642e0d), the market
  Redis the writer actually projects into (TRADING_MARKET_REDIS_URL read from the writer's settings), host disk;
* the stage-100 FINAL driver run (scripts/phase3_consumer_load_acceptance.py --mode target --sessions 100
  --duration-seconds 1800 with the frozen budget: BURST, SLOW_READER and RECONNECT fault windows), with the existing
  guard_v2.sh / mark_watch_v2.sh stop conditions (they SIGINT the driver PID only), and a profile derived from the base
  profile plus binance_bar_edge and stable_redis as monitored containers (G14);
* G4 observer windows (scripts/p184_g4_observer.py), four 120 s windows plus one 180 s window from 30 s before a UTC
  :00/:30 close (schedule v2, see g4_schedule: always five windows, the launch is held <= 220 s when no :00/:30 close
  would fit inside the load);
* G3 BAR cache poller, G8 Redis sampler, G9/G10 writer counters for the whole load (stopped by SIGTERM after it);
* G6 TS execution probe (scripts/p184_g6_ts_execution_probe.py, `docker exec -i <writer> python -B -`): the writer's own
  EXECUTION reads (QUOTE, TRADE, MARK/INDEX) classified against the frozen D3 policy for the whole load, read-only,
  self-bounded; stopped through its own in-container PID (SIGTERM) after the load; output g6-ts/summary.json;
* G11 (optional, after the load because any monitored-container restart aborts the driver): the restart probe arms and
  waits for the COORDINATOR to restart the writer; this tool never restarts anything.
Then `score` (also callable alone on a finished campaign dir) runs the trace analyses (G1, G2, G7, G10 duplicates) in a
disposable `--network none` container of the already present DL python image, and writes scorecard.json.

Scoring rule (v2.2.3 scorecard, unchanged): verdicts PASS / FAIL / NOT_MEASURED; a measurement outside the protocol
can FAIL a gate but cannot PASS it; missing coverage is NOT_MEASURED; nothing is ever upgraded. G12 and G13 are not part
of this campaign and are reported NOT_MEASURED with that reason.

Native writer (TS N1, MARKET_DATA_NATIVE_FEEDS, e.g. TRADE,QUOTE; thresholds unchanged, only the measurement source):
the native raw trace is one capped file per activation (a SAMPLE), so for the native feeds G2 transit and G7 segments
are read from the native final summary histograms (p95/p99 = bucket upper bounds, max exact; scope = the activation
window spanning the load, pre/post-load seconds reported) and the raw CAP is not a coverage gap for them; the summary's
own coverage gaps are. Python-worker feeds (BOOK_*, MARK) keep the raw-trace coverage rule. The summary is written when
the writer process stops, i.e. at the G11 restart: score after G11. The activation end is the summary's
observed_end_unix_ns, else the G11 probe's pinned process end (--process-end), never the configured expiry (S100-5).
G10 duplicates for native TRADE come from the summary's apply-decision counters (TS apply_integrity, whole activation);
without them they are read from the raw sample, which can FAIL G10 but never PASS it. Since Codex review SR2/SR3 a
native G10 PASS also needs every frozen TRADE route of the binding with validated counters, no lost apply reply, and
the native typed continuity counters (heartbeat trade_integrity) over one unbroken activation stretch bracketing the
load. G11 is re-judged from its summary with the current oracle (SR5): a v1 summary cannot PASS.
`score --out-dir NEW` writes the re-analysed trace and scorecard.json there, never into the campaign directory.

G4 addendum (owner decision 2026-10-11): rows outside the frozen 17-row G4 table are scored from
config/v2/p184-stage100-g4-addendum.json when that file exists (frozen once by scripts/p184_freeze_g4_addendum.py from
a `run --baseline` campaign, never from a candidate): candidate max <= baseline max x 1.2 on windows with >= 5 samples;
bar 1h/2h/4h/6h and rows whose baseline had no window of >= 5 samples are REPORT_ONLY (never pass, never fail, never
block). The original table and its p95 rule are unchanged; without the file the scoring is unchanged.
G4 amendment 1 (owner decision 2026-10-18, config/v2/p184-stage100-g4-amendment-1.json, frozen before the run):
ONE row, mark_index_price|OKX| (a paired mark+index event), is scored on the same-event internal split Kafka
CreateTime - newest component receipt (observer v6 `paired_split`): per window p95 <= the recomputed baseline p95 x 1.2
(the frozen-table shape, bucket straddle unresolved) AND, on windows with >= 5 samples, max <= baseline max x 1.2 (the
addendum shape). The composite visibility value, the component ts gap |mark.ts - index.ts| and the venue-direct OKX
probe (scripts/p184_g4_okx_mark_index_probe.py, one per G4 window) are reported, never scored; a window without
probe coverage has attribution UNKNOWN, which is not EXTERNAL (the row cannot PASS). The amendment applies only to a
campaign that recorded its sha256 at run time, frozen before the campaign started, for the same budget, addendum and
campaign tool; every other row and gate is unchanged, and an older campaign (none recorded) scores byte-identically.
G4 amendment 2 (owner decision 2026-10-18 ~08:50Z, "B1", config/v2/p184-stage100-g4-amendment-2.json, frozen before the
run): bar rows with an interval >= 8h that have no frozen baseline (frozen table and addendum hold none) are REPORT_ONLY,
the same class as the addendum's bar 1h-6h; no threshold changes. It also re-pins amendment 1 (unchanged file, same
sha256) to this campaign tool, because amendment 1 applies only to the tool sha256 it pins. Like amendment 1 it applies
only to a campaign that recorded its sha256 at run time; an older campaign scores byte-identically.
Report-only per G4 window of a candidate campaign: a fresh-connection OKX `books` probe plus a 1 Hz TcpExt / 10 Hz
socket-queue sampler of the ingestor_okx_swap netns (scripts/p184_g4_okx_books_probe.py); it attributes OKX book
tails (EXTERNAL only with the fresh connection healthy while the lane is late, and an empty host receive queue) and
never changes a verdict.
`run --baseline [--sessions 50|100]` records role=baseline in campaign.json and runs only the driver workload (+ guard
and MARK watch) and the G4 observer windows, then writes baseline.json; it never scores.

Boundary: no image builds, no container create/recreate/restart, no Redis/Kafka writes. Children: the driver's own
disposable client container (removed by the driver), the G4 observer (observer v3+: its own `docker run --rm` container
of the query_kn_1 image, read-only TLS mounts, bounded CPU/memory, removed by name after each window - no longer a
`docker exec` inside query_kn_1) and the trace analysis container (--rm, --network none, read-only mounts).
usage: p184_gate_campaign.py run [--dry-run] [--g11] ... | p184_gate_campaign.py score --campaign-dir DIR
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import p184_g8_redis_sampler as g8  # noqa: E402
from scripts import p184_g9_g10_writer_counters as g910  # noqa: E402
from scripts import p184_g11_writer_restart_probe as g11probe  # noqa: E402
from scripts import p184_g14_resource_table as g14  # noqa: E402
from scripts.p184_resp_readonly import parse_redis_url, read_jsonl  # noqa: E402
from scripts.phase3_consumer_load_acceptance import _PHASE3_CONSUMER_MANIFESTS  # noqa: E402

STATE = Path.home() / ".local/state/qdl-v2"
TOOLS = STATE / "p184-d3-capacity-20261009"
WRITER = "market_data_service_p184w"
QUERY_REFERENCE = "qdl_v2_stable_candidate-query_kn_1-1"
EXTRA_MONITORED = ("qdl_v2_stable_candidate-binance_bar_edge-1", "qdl_v2_stable_candidate-stable_redis-1")
LOAD_SECONDS = 1800
DRIVER_TIMEOUT = 2400
G4_SECONDS = 120
G4_BOUNDARY_SECONDS = 180
# G4 schedule v2 (owner 2026-10-14: fix sampling, thresholds and the frozen max rule unchanged). v1 placed the regular
# windows at 120 + 360 k, a multiple of 180 s, so every window had the same phase against the 3m/15m/30m closes, and
# dropped the boundary window for ~13.9 % of launch phases. v2: four regular windows 225 s apart (225 mod 60 = 45,
# mod 180 = 45, mod 300 = 225: four distinct second-of-minute phases, evenly spread over the 3m and 5m cycles); a
# 900 s jump (= lcm(180, 300), a multiple of 60, so every phase is kept) opens the gap for the boundary window.
G4_SCHEDULE_VERSION = "qdl.p184.g4-schedule.v2"
G4_FIRST_OFFSET = 120             # driver setup ends ~105 s after launch (receipts 2026-10-11/13)
G4_LOAD_END_OFFSET = LOAD_SECONDS + 80
G4_SPACING = 225
G4_REGULAR_WINDOWS = 4
G4_PHASE_JUMP = 900
G4_BOUNDARY_PERIOD = 1800         # UTC :00/:30
G4_BOUNDARY_PREROLL = 30
G4_WINDOW_GAP = 15                # observer start + summary run after a window
G4_MAX_LAUNCH_HOLD = 240
G4_RECORD_CAP = 10000
MIN_DISK_GB = 20
TRACE_MAX_WINDOW_SECONDS = 3600   # TS delivery_trace.MAX_WINDOW_SECONDS; the native writer refuses a longer window
TRACE_TAIL_SECONDS = 45           # 30 s settle before the samplers stop + 15 s before G11 arms
SAMPLER_MARGIN_SECONDS = 60       # samplers run load_timeout + 60 s
G11_DEFAULT_WAIT = 900
G11_MIN_WAIT = 120                # below this the coordinator has no practical restart window
TRACE_FEEDS_G1 = ("BOOK_DELTA", "BOOK_SNAPSHOT")
TRACE_FEEDS_G2 = ("TRADE", "QUOTE")
G7_FEEDS = ("BOOK_DELTA", "BOOK_SNAPSHOT", "QUOTE", "TRADE", "MARK_INDEX_PRICE")
VENUES = ("BINANCE", "OKX")
# G6 is decided only by D3_EXECUTION rows (reads made with an EXECUTION-grade requirement, i.e. the TS writer's own
# identity via the G6 TS execution probe). Required: QUOTE, MARK_INDEX_PRICE, TRADE per venue; BOOK_DELTA is not a
# Query shape of the frozen workload; BOOK_SNAPSHOT and BAR1m D3 rows are scored when present, not required.
G6_D3_BASIS = "D3_EXECUTION"
G6_REQUIRED = tuple(f"{feed}|{venue}|" for feed in ("TRADE", "QUOTE", "MARK_INDEX_PRICE") for venue in VENUES)
G6_GATED_FEEDS = ("TRADE", "QUOTE", "MARK_INDEX_PRICE", "BOOK_SNAPSHOT", "BOOK_DELTA", "BAR")
G6_TS_PROBE_SUFFIX = "#ts-probe"
G6_TS_PROBE_EMIT_SECONDS = 60
# G4 addendum (owner decision 2026-10-11): rows outside the frozen 17-row table get their own frozen file, built ONLY
# from a dedicated baseline run (run --baseline). The original table and its p95 rule stay byte-identical.
G4_ADDENDUM_PATH = ROOT / "config/v2/p184-stage100-g4-addendum.json"
G4_ADDENDUM_SCHEMA = "qdl.p184.stage100-g4-addendum.v1"
G4_ADDENDUM_MIN_SAMPLES = 5      # per window; fewer -> the window is reported, never scored
G4_ADDENDUM_FACTOR = 1.2         # candidate max <= baseline max x 1.2
G4_REPORT_ONLY_INTERVALS = ("1h", "2h", "4h", "6h")
# G4 amendment 1 (owner decision 2026-10-18): one frozen-table row scored on the same-event internal split.
G4_AMENDMENT_PATH = ROOT / "config/v2/p184-stage100-g4-amendment-1.json"
G4_AMENDMENT_SCHEMA = "qdl.p184.stage100-g4-amendment.v1"
G4_AMENDMENT_ROW = "mark_index_price|OKX|"
G4_AMENDMENT_QUANTITY = "internal_newest_receipt_to_producer_ts_ms"
G4_AMENDMENT_FACTOR = 1.2
G4_AMENDMENT_MIN_SAMPLES = 5      # max rule: windows with >= 5 samples (addendum shape)
G4_OKX_PROBE_LEAD_SECONDS = 8     # the venue probe starts before the observer window ...
G4_OKX_PROBE_TAIL_SECONDS = 10    # ... and ends after it (stopped off the window path: never delays a window)
# G4 amendment 2 (owner B1, 2026-10-18 ~08:50Z): unbaselined bar rows with interval >= 8h are REPORT_ONLY.
G4_AMENDMENT_2_PATH = ROOT / "config/v2/p184-stage100-g4-amendment-2.json"
G4_AMENDMENT_2_SCHEMA = "qdl.p184.stage100-g4-amendment-2.v1"
G4_AMENDMENT_2_ID = "G4-A2"
G4_B1_MIN_INTERVAL_MS = 8 * 3_600_000
# The owner's closed row set; keys already in the frozen table are excluded when the addendum is frozen.
G4_ADDENDUM_OWNER_ROWS = tuple(
    [f"bar|{venue}|{interval}" for venue in VENUES for interval in ("5m", "15m", "30m", "1h", "2h", "4h", "6h")]
    + [f"{feed}|OKX_INVERSE|" for feed in ("book_delta", "book_snapshot", "mark_index_price", "quote")])


# ---------------------------------------------------------------- plan helpers (pure, unit-tested)

def g4_boundary(launch_unix: float, *, boundary_seconds=G4_BOUNDARY_SECONDS,
                load_end_offset: float = G4_LOAD_END_OFFSET) -> int | None:
    """The UTC :00/:30 close whose window [close - 30 s, close + 150 s] lies inside [launch + 120, launch + 1880].
    The admissible closes span 1580 s < 1800 s, so for 220 of 1800 launch phases there is none (see g4_launch_unix)."""
    close = -(-int(launch_unix + G4_FIRST_OFFSET + G4_BOUNDARY_PREROLL) // G4_BOUNDARY_PERIOD) * G4_BOUNDARY_PERIOD
    while close - G4_BOUNDARY_PREROLL < launch_unix + G4_FIRST_OFFSET:
        close += G4_BOUNDARY_PERIOD
    return close if close - G4_BOUNDARY_PREROLL + boundary_seconds <= launch_unix + load_end_offset else None


def g4_launch_unix(earliest_unix: float) -> float:
    """Earliest launch >= earliest_unix (in whole seconds) for which a boundary window fits; holds <= 220 s."""
    for hold in range(G4_MAX_LAUNCH_HOLD + 1):
        if g4_boundary(earliest_unix + hold) is not None:
            return earliest_unix + hold
    raise AssertionError("no :00/:30 close fits within the launch hold")  # unreachable: 1800 - 1580 = 220 < 240


def g4_schedule(launch_unix: float, *, seconds=G4_SECONDS, boundary_seconds=G4_BOUNDARY_SECONDS,
                load_end_offset: float = G4_LOAD_END_OFFSET) -> list[dict]:
    """Observer windows as {offset from launch, seconds, boundary}, schedule v2.

    Regular windows start at 120 + 225 k (k = 0..3); the last 4 - m of them are moved 900 s later, with m the largest
    split that leaves the boundary window (30 s before the :00/:30 close, boundary_seconds long) clear of every regular
    window by >= 15 s. A 900 s move keeps each window's phase against the 1m/3m/5m/15m closes. A split exists for
    every boundary offset in [120, 1700]; the regular windows end at <= 1815 s. Without a fitting close (launch not
    chosen by g4_launch_unix) the regular windows are returned unsplit and the schedule has no boundary window."""
    base = [G4_FIRST_OFFSET + k * G4_SPACING for k in range(G4_REGULAR_WINDOWS)]
    close = g4_boundary(launch_unix, boundary_seconds=boundary_seconds, load_end_offset=load_end_offset)
    windows = [{"offset": float(o), "seconds": float(seconds), "boundary": None} for o in base]
    if close is None:
        return windows
    start = close - G4_BOUNDARY_PREROLL - launch_unix
    for before in range(G4_REGULAR_WINDOWS, -1, -1):
        offsets = [o if k < before else o + G4_PHASE_JUMP for k, o in enumerate(base)]
        clear = all(o + seconds + G4_WINDOW_GAP <= start or o >= start + boundary_seconds + G4_WINDOW_GAP
                    for o in offsets)
        if clear and offsets[-1] + seconds <= load_end_offset:
            windows = [{"offset": float(o), "seconds": float(seconds), "boundary": None} for o in offsets]
            break
    else:
        raise AssertionError(f"no split clears the boundary window at offset {start}")
    windows.append({"offset": round(start, 3), "seconds": float(boundary_seconds), "boundary": close})
    return sorted(windows, key=lambda w: w["offset"])


def g4_apparatus(launch_hold_seconds: float | None = None) -> dict:
    """Which G4 apparatus produced a campaign (recorded in campaign.json, echoed on the G4 scorecard row)."""
    from scripts import p184_g4_observer as observer
    return {"schedule_version": G4_SCHEDULE_VERSION, "observer_version": observer.OBSERVER_VERSION,
            "observer_sha256": sha256_file(ROOT / "scripts/p184_g4_observer.py"),
            "observer_launch_mode": observer.LAUNCH_MODE, "observer_cpus": observer.DEFAULT_CPUS,
            "observer_memory": observer.DEFAULT_MEMORY,
            "regular": {"windows": G4_REGULAR_WINDOWS, "seconds": G4_SECONDS, "first_offset": G4_FIRST_OFFSET,
                        "spacing": G4_SPACING, "phase_jump": G4_PHASE_JUMP},
            "boundary": {"seconds": G4_BOUNDARY_SECONDS, "preroll": G4_BOUNDARY_PREROLL,
                         "period": G4_BOUNDARY_PERIOD},
            "record_cap": G4_RECORD_CAP, "sample_ms": 1000, "bar_sampling_key": "venue,symbol,feed,interval",
            "launch_hold_seconds": launch_hold_seconds}


def g4_window_receipt(observations) -> dict:
    """Records and whether the observer hit its record cap (the reader stops there; later closes are unseen).
    Report-only additions (observer v3, never read by a verdict): how the observer was launched (v1/v2 windows have
    no `launch` line: DOCKER_EXEC_IN_QUERY), per feed|venue|interval coverage with cap truncation, and the
    produce-stage split. Observer v5 adds its own cgroup CPU use/throttling (`observer_cgroup`, report-only)
    so an observer-scheduling share of the receipt clock can be shown or excluded."""
    from scripts import p184_g4_observer as observer
    observations = list(observations)
    records, cap, done, version, launch = 0, None, None, None, None
    for item in observations:
        kind = item.get("kind")
        if kind == "scope":
            version = item.get("observer_version") or "v1 (unversioned)"
        elif kind == "launch":
            launch = item
        elif kind == "canonical_observation":
            records += 1
        elif kind == "canonical_record_cap_reached":
            cap = item
        elif kind == "DONE":
            done = item
    launch_info = ({k: launch.get(k) for k in ("launch_mode", "container_name", "image_id", "image_repo_digests",
                                                "cpus", "memory", "networks")}
                   if launch else {"launch_mode": "DOCKER_EXEC_IN_QUERY (pre-v3, no launch record)"})
    coverage = observer.window_coverage(observations)
    receipt = {"records": records, "record_cap_reached": bool(cap or (done or {}).get("record_cap_reached")),
               "cap_reached_after_seconds": (cap or {}).get("after_seconds"),
               "observer_version": version, "launch": launch_info,
               "coverage": coverage, "produce_split": observer.produce_split(observations),
               "observer_cgroup": observer.cgroup_summary(observations)}
    if coverage.get("coverage_basis") == "FULL_WINDOW_AGGREGATE":
        # v4: `records` counts the bounded raw samples only; the gated rows come from the full-window aggregate.
        receipt.update(records_basis="RAW_STRATIFIED_SAMPLES", events_admitted=coverage.get("events_admitted"),
                       baseline_comparability=observer.BASELINE_COMPARABILITY)
    return receipt


def g4_observer_cgroup_rollup(receipts: list[dict]) -> dict:
    """Report-only roll-up of the observer's own cgroup CPU evidence per G4 window (never read by a verdict)."""
    rows = {}
    for receipt in receipts:
        cg = receipt.get("observer_cgroup") or {}
        if not cg.get("available"):
            rows[str(receipt.get("index"))] = {"available": False, "reason": cg.get("reason", "NO_RECEIPT")}
            continue
        window = cg.get("window") or {}
        rows[str(receipt.get("index"))] = {
            "available": True, "cpu_max_cores": cg.get("cpu_max_cores"), "usage_cores": window.get("usage_cores"),
            "throttled_ratio": window.get("throttled_ratio"), "throttled_ms": window.get("throttled_ms"),
            "max_interval_throttled_ratio": cg.get("max_interval_throttled_ratio"),
            "max_interval_usage_cores": cg.get("max_interval_usage_cores")}
    return {"report_only": True, "basis": "OBSERVER_OWN_CGROUP_V2_CPU_STAT", "windows": rows}


def derive_profile(base: dict, extra=EXTRA_MONITORED) -> dict:
    profile = json.loads(json.dumps(base))
    monitored = list(profile.get("monitored_containers") or [])
    for name in extra:
        if name not in monitored:
            monitored.append(name)
    profile["monitored_containers"] = monitored
    return profile


def mount_source(inspect: dict, destination: str) -> str | None:
    for mount in inspect.get("Mounts", []):
        if mount.get("Destination") == destination:
            return mount.get("Source")
    return None


def host_path(inspect: dict, container_path: str) -> str | None:
    """Map a container path to its host path through the longest matching bind mount."""
    best = None
    for mount in inspect.get("Mounts", []):
        dest = mount.get("Destination", "")
        if container_path == dest or container_path.startswith(dest.rstrip("/") + "/"):
            if best is None or len(dest) > len(best["Destination"]):
                best = mount
    if best is None:
        return None
    return best["Source"] + container_path[len(best["Destination"]):]


def env_of(inspect: dict) -> dict[str, str]:
    return dict(item.split("=", 1) for item in inspect.get("Config", {}).get("Env", []) if "=" in item)


def read_manifest_identity(path) -> tuple[str, int] | None:
    """(metadata.id, metadata.revision) of a consumer manifest yaml, or None when unreadable/malformed."""
    import yaml
    try:
        meta = (yaml.safe_load(Path(path).read_text()) or {}).get("metadata") or {}
    except (OSError, ValueError, AttributeError, yaml.YAMLError):
        return None
    ident, revision = meta.get("id"), meta.get("revision")
    if not isinstance(ident, str) or not isinstance(revision, int) or isinstance(revision, bool):
        return None
    return ident, revision


def live_manifests(inspect: dict) -> tuple[dict[str, dict], list[str]]:
    """consumer id -> {revision, container_path, host_path} of a Query container, from its
    QDL_STABLE_CONSUMER_MANIFESTS and bind mounts only (read-only `docker inspect` data, no docker exec). A path not
    under a bind mount lives in the image: recorded as unresolved under the driver consumer whose baked path it is."""
    live, unresolved = {}, []
    by_baked_path = {path: consumer for consumer, path in _PHASE3_CONSUMER_MANIFESTS.items()}
    for container_path in filter(None, env_of(inspect).get("QDL_STABLE_CONSUMER_MANIFESTS", "").split(":")):
        source = host_path(inspect, container_path)
        identity = read_manifest_identity(source) if source else None
        if identity is None:
            unresolved.append(by_baked_path.get(container_path, container_path))
            continue
        live[identity[0]] = {"revision": identity[1], "container_path": container_path, "host_path": source}
    return live, unresolved


def manifest_revision_check(profile: dict, inspects: dict[str, dict | None], *, repo_root: Path = ROOT) -> tuple[
        list[dict], list[str]]:
    """Every consumer the driver mints workload tokens for (profile identities + manifest_overrides) must carry the
    same manifest revision as each live Query loads, or Query answers UNAUTHENTICATED ("workload token is not bound to
    the active consumer manifest revision"; campaign 2026-10-15T075842Z: 1,080 errors from a stale r15 override vs a
    live r16). Driver side: the override file, else the repo copy of the manifest the client image bakes. Fail
    closed: anything unresolvable is a problem."""
    overrides = profile.get("manifest_overrides") or {}
    consumers = sorted({item.get("id") for item in profile.get("identities") or [] if isinstance(item, dict)}
                       | set(overrides))
    rows, problems = [], []
    live_by_container = {}
    for name, inspect in inspects.items():
        if inspect is None:
            problems.append(f"manifest check: cannot inspect Query container {name}")
            continue
        live_by_container[name] = live_manifests(inspect)
    if not inspects:
        problems.append("manifest check: no Query container to compare against")
    for consumer in consumers:
        if consumer in overrides:
            driver_path, source = str(overrides[consumer]), "manifest_overrides"
        elif consumer in _PHASE3_CONSUMER_MANIFESTS:
            driver_path = str(repo_root / "consumers/stable" / Path(_PHASE3_CONSUMER_MANIFESTS[consumer]).name)
            source = "repo default (client image baked copy)"
        else:
            problems.append(f"manifest check: {consumer} has no driver manifest")
            continue
        identity = read_manifest_identity(driver_path)
        row = {"consumer": consumer, "driver_manifest": driver_path, "driver_source": source,
               "driver_revision": identity[1] if identity else None, "live": {}}
        rows.append(row)
        if identity is None or identity[0] != consumer:
            problems.append(f"manifest check: driver manifest for {consumer} unreadable or for another consumer "
                            f"({driver_path})")
            continue
        for name, (live, unresolved) in live_by_container.items():
            entry = live.get(consumer)
            row["live"][name] = entry
            if entry is None:
                why = "lives in the image (no bind mount)" if consumer in unresolved else "not loaded"
                problems.append(f"manifest check: {consumer} live manifest on {name} unresolvable ({why})")
            elif entry["revision"] != identity[1]:
                problems.append(f"manifest check: {consumer} driver revision {identity[1]} ({driver_path}) != live "
                                f"revision {entry['revision']} on {name} ({entry['host_path']})")
    return rows, problems


# ---------------------------------------------------------------- scoring (pure, unit-tested)

def _protocol(campaign: dict, receipt: dict | None, gates: dict | None) -> tuple[bool, list[str]]:
    problems = []
    if receipt is None:
        problems.append("no driver receipt")
    else:
        window = receipt.get("load_window_unix_s")
        if not window or window[1] - window[0] < LOAD_SECONDS:
            problems.append("load window < 1800 s or unknown")
        if not (receipt.get("fault_windows") or {}).get("BURST"):
            problems.append("no BURST window")
        if receipt.get("stage") != 100 or not receipt.get("final"):
            problems.append("not the stage-100 final run")
    if gates is None or gates.get("status") != "PASS":
        problems.append("driver acceptance not PASS: " + ",".join((gates or {}).get("failed_gates", ["no gates"]))[:300])
    return not problems, problems


def _gate(gid, quantity, rule, verdict, reason, protocol_ok, protocol_problems, **extra):
    if verdict == "PASS" and not protocol_ok:
        verdict, reason = "NOT_MEASURED", f"inside the rule but outside the protocol: {'; '.join(protocol_problems)}"
    return {"id": gid, "quantity": quantity, "rule": rule, "verdict": verdict, "reason": reason, **extra}


def with_scope(why: list[str], default: str, scope_note: list[str]) -> str:
    """Gate reason: the reasons (or the default), then the native scope note when there is one."""
    return "; ".join((list(why) or [default]) + list(scope_note))


def _trace_coverage(transit: dict | None, load_ns, until_ns: int | None, *, raw_kinds=("python", "native")) -> list[str]:
    """Trace-derived gates need the trace armed through the whole load and no worker capped (a CAP stop means the
    last allowed part filled and later deliveries were not recorded). raw_kinds: whose raw files must be complete;
    the native writer's raw file is a capped sample when its feeds are scored from the native summary, so callers
    scoring those feeds pass ("python",). Without files_by_kind (older analyses) every file counts."""
    if transit is None:
        return ["no trace analysis"]
    gaps = []
    if transit.get("error"):
        gaps.append(f"trace analysis: {transit['error']}")
    if not transit.get("records"):
        gaps.append("no trace records")
    kinds = transit.get("files_by_kind")
    if kinds is None:
        dropped, caps = transit.get("dropped_at_export_total"), (transit.get("stop_reasons") or {}).get("CAP")
    else:
        dropped = sum((kinds.get(k) or {}).get("dropped_at_export_total") or 0 for k in raw_kinds)
        caps = sum(((kinds.get(k) or {}).get("stop_reasons") or {}).get("CAP") or 0 for k in raw_kinds)
    label = "" if kinds is None or set(raw_kinds) == {"python", "native"} else f" ({'/'.join(raw_kinds)} writer)"
    if dropped:
        gaps.append(f"{dropped} trace records dropped at export{label}")
    if caps:
        gaps.append(f"{caps} trace activations stopped at their file cap{label}")
    if load_ns and (not until_ns or until_ns < load_ns[1]):
        gaps.append("trace expiry is before the end of the load window")
    return gaps


def native_feeds_of(campaign: dict, transit: dict | None) -> tuple[str, ...]:
    """TRADE/QUOTE feeds the native writer owned in this campaign: recorded at preflight (MARKET_DATA_NATIVE_FEEDS);
    a campaign recorded before that field falls back to the feeds present in the native summary analysis."""
    feeds = (campaign.get("trace") or {}).get("native_feeds")
    if feeds is None:
        feeds = {key.split("|", 1)[0] for key in (((transit or {}).get("native_summary") or {}).get("transit") or {})}
    return tuple(sorted(f for f in feeds if f in TRACE_FEEDS_G2))


def native_coverage(transit: dict | None) -> list[str]:
    summary = (transit or {}).get("native_summary")
    if summary is None:
        return ["no native writer summary analysis (the summary is written when the writer stops: G11 restart)"]
    return list(summary.get("coverage_gaps") or [])


def scoring_view(transit: dict | None, native: tuple[str, ...]) -> dict | None:
    """The transit analysis with the native feeds' transit and G7 rows taken from the native summary; the raw rows
    of those feeds are kept apart as raw_samples (reported, never scored)."""
    if transit is None or not native:
        return transit
    summary = transit.get("native_summary") or {}

    def mine(key):
        return key.split("|", 1)[0] in native
    view = dict(transit)
    view["raw_samples"] = {k: v for k, v in (transit.get("transit") or {}).items() if mine(k)}
    view["transit"] = {**{k: v for k, v in (transit.get("transit") or {}).items() if not mine(k)},
                       **{k: v for k, v in (summary.get("transit") or {}).items() if mine(k)}}
    view["g7"] = {**{k: v for k, v in (transit.get("g7") or {}).items() if not mine(k)},
                  **{k: v for k, v in (summary.get("g7") or {}).items() if mine(k)}}
    return view


def score_transit_rows(transit: dict | None, feeds) -> tuple[str, list, list[str]]:
    rows, missing, outside = [], [], []
    for feed in feeds:
        for venue in VENUES:
            block = ((transit or {}).get("transit") or {}).get(f"{feed}|{venue}")
            if not block or not block.get("n"):
                missing.append(f"{feed}|{venue}")
                continue
            row = {"feed_venue": f"{feed}|{venue}", **{k: block.get(k) for k in ("n", "p50", "p95", "p99", "max")},
                   "observed": block.get("verdict"), "split_p95_ms": {k: v.get("p95") for k, v in
                                                                   (block.get("split") or {}).items()}}
            if block.get("basis"):
                row["basis"] = block["basis"]
                sample = ((transit or {}).get("raw_samples") or {}).get(f"{feed}|{venue}")
                row["raw_sample"] = {k: sample.get(k) for k in ("n", "p95", "max")} if sample else None
            if block.get("stream_basis"):
                row["stream_basis"] = block["stream_basis"]
            for extra in ("distribution_scope", "catch_up_report_only"):  # native load slices (TS e35bb316)
                if block.get(extra) is not None:
                    row[extra] = block[extra]
            rows.append(row)
            if block.get("verdict") == "OUTSIDE":
                outside.append(f"{feed}|{venue}")
    if outside:
        return "FAIL", rows, [f"outside: {', '.join(outside)}"]
    if missing:
        return "NOT_MEASURED", rows, [f"no data: {', '.join(missing)}"]
    return "PASS", rows, []


def score_mark(mark: dict | None) -> tuple[str, list, list[str]]:
    rows, missing, outside = [], [], []
    for venue in VENUES:
        block = ((mark or {}).get("venues") or {}).get(venue)
        gated = (block or {}).get("new_event_transit_ms") or {}
        if not gated.get("n"):
            missing.append(f"MARK|{venue}")
            continue
        rows.append({"feed_venue": f"MARK_INDEX_PRICE|{venue}", **{k: gated.get(k) for k in ("n", "p50", "p95", "p99", "max")},
                     "observed": block.get("g2_verdict")})
        if block.get("g2_verdict") == "OUTSIDE":
            outside.append(f"MARK|{venue}")
    if outside:
        return "FAIL", rows, [f"outside: {', '.join(outside)}"]
    if missing:
        return "NOT_MEASURED", rows, [f"no data: {', '.join(missing)}"]
    return "PASS", rows, []


def score_g7(transit: dict | None) -> tuple[str, dict, list[str]]:
    g7 = (transit or {}).get("g7") or {}
    rows, outside, missing = {}, [], []
    for feed in G7_FEEDS:
        block = g7.get(feed)
        if not block or "verdict" not in block:
            missing.append(feed)
            continue
        rows[feed] = {name: block.get(name) for name in ("sdk_to_writer_ms", "writer_to_redis_ms")}
        for name, state in block["verdict"].items():
            if state == "OUTSIDE":
                outside.append(f"{feed}:{name}")
            elif state == "NO_DATA" and not (feed == "MARK_INDEX_PRICE" and name == "sdk_to_writer_ms"):
                missing.append(f"{feed}:{name}")
    if outside:
        return "FAIL", rows, [f"outside: {', '.join(outside)}"]
    if missing:
        return "NOT_MEASURED", rows, [f"no data: {', '.join(missing)}"]
    return "PASS", rows, []


def g4_window_stats(window: dict, key: str) -> dict:
    """{'n', 'p95', 'max'} of one row in one observer window: summary rows first, then the OKX inverse split."""
    value = ((window.get("summary") or {}).get(key) or {}).get("visibility_ms")
    if value is None:
        value = (window.get("inverse_rows") or {}).get(key)
    return value or {}


def g4_row_interval(key: str) -> str:
    return key.split("|")[2] if key.count("|") == 2 else ""


def validate_g4_addendum(addendum: dict, table: dict) -> list[str]:
    """Problems that keep an addendum from being applied (empty list = valid against this frozen table)."""
    problems = []
    if not isinstance(addendum, dict) or addendum.get("schema") != G4_ADDENDUM_SCHEMA:
        return ["unknown addendum schema"]
    rule = addendum.get("rule") or {}
    if (rule.get("min_samples_per_window") != G4_ADDENDUM_MIN_SAMPLES or rule.get("factor") != G4_ADDENDUM_FACTOR
            or rule.get("statistic") != "max"
            or list(rule.get("report_only_intervals") or []) != list(G4_REPORT_ONLY_INTERVALS)):
        problems.append("addendum rule differs from the owner rule")
    if not addendum.get("frozen_before_candidate") or (addendum.get("baseline") or {}).get("role") != "baseline":
        problems.append("addendum is not frozen from a baseline campaign")
    rows = addendum.get("rows") or {}
    expected = [key for key in G4_ADDENDUM_OWNER_ROWS if key not in table]
    if sorted(rows) != sorted(expected):
        problems.append("addendum row set differs from the owner row set minus the frozen table")
    overlap = sorted(set(rows) & set(table))
    if overlap:
        problems.append(f"addendum rows overlap the frozen table: {', '.join(overlap)}")
    for key, spec in rows.items():
        status = (spec or {}).get("status")
        if status not in ("GATED", "REPORT_ONLY"):
            problems.append(f"{key}: unknown status")
        elif status == "GATED" and (g4_row_interval(key) in G4_REPORT_ONLY_INTERVALS
                                    or not isinstance(spec.get("candidate_max_limit_ms"), (int, float))):
            problems.append(f"{key}: GATED row without a limit or on a report-only interval")
    return problems


def validate_g4_amendment(amendment: dict, table: dict) -> list[str]:
    """Problems that keep G4 amendment 1 from being applied (empty list = valid against this frozen table)."""
    if not isinstance(amendment, dict) or amendment.get("schema") != G4_AMENDMENT_SCHEMA:
        return ["unknown amendment schema"]
    problems = []
    rule = amendment.get("rule") or {}
    if (amendment.get("row") != G4_AMENDMENT_ROW or rule.get("quantity") != G4_AMENDMENT_QUANTITY
            or rule.get("factor") != G4_AMENDMENT_FACTOR or rule.get("max_min_samples_per_window") != G4_AMENDMENT_MIN_SAMPLES):
        problems.append("amendment row or rule differs from the owner decision")
    if G4_AMENDMENT_ROW not in table:
        problems.append("amended row is not in the frozen table")
    limits = amendment.get("limits") or {}
    if not all(isinstance(limits.get(k), (int, float)) for k in ("candidate_p95_limit_ms", "candidate_max_limit_ms")):
        problems.append("amendment without numeric limits")
    if not amendment.get("frozen_before_candidate") or not amendment.get("decision_owner"):
        problems.append("amendment is not frozen with a decision owner")
    return problems


def g4_long_bar_row(key: str) -> bool:
    """bar|VENUE|INTERVAL with a fixed interval >= 8h (G4 amendment 2). Unparsable intervals are not long. Pure."""
    parts = key.split("|")
    if len(parts) != 3 or parts[0] != "bar":
        return False
    from qdl.adapters.intervals import canonical_interval_ms
    try:
        return canonical_interval_ms(parts[2]) >= G4_B1_MIN_INTERVAL_MS
    except ValueError:
        return False


def validate_g4_amendment_2(amendment: dict) -> list[str]:
    """Problems that keep G4 amendment 2 (B1) from being applied (empty list = valid)."""
    if not isinstance(amendment, dict) or amendment.get("schema") != G4_AMENDMENT_2_SCHEMA:
        return ["unknown amendment-2 schema"]
    problems = []
    rule = amendment.get("rule") or {}
    if (amendment.get("amendment_id") != G4_AMENDMENT_2_ID or rule.get("feed") != "bar"
            or rule.get("report_only_min_interval_ms") != G4_B1_MIN_INTERVAL_MS
            or rule.get("applies_to") != "rows observed without a frozen baseline (not in the table, not in the addendum)"
            or rule.get("threshold_changes") != []):
        problems.append("amendment-2 rule differs from the owner decision")
    if not amendment.get("frozen_before_candidate") or not amendment.get("decision_owner"):
        problems.append("amendment-2 is not frozen with a decision owner")
    if not ((amendment.get("inherits") or {}).get("g4_amendment_1_sha256")):
        problems.append("amendment-2 does not name the amendment 1 it re-pins")
    return problems


def applicable_g4_amendment_2(campaign: dict) -> tuple[dict | None, list[str], dict | None]:
    """(amendment 2 or None, notes, source). Same rules as amendment 1: only when recorded at run time, unchanged,
    valid, frozen before the campaign started and pinned to the campaign's budget, addendum, amendment 1 and tool."""
    recorded = campaign.get("g4_amendment_2_path")
    if not recorded:
        return None, [], None
    path = Path(recorded)
    if not path.exists():
        return None, [f"G4 amendment 2 not applied: {path} recorded at run time is missing"], None
    source = {"path": str(path), "sha256": sha256_file(path)}
    amendment = load_json(path)
    problems = validate_g4_amendment_2(amendment) if amendment is not None else ["unreadable amendment-2"]
    if not problems:
        pins = amendment.get("pinned") or {}
        if campaign.get("g4_amendment_2_sha256") != source["sha256"]:
            problems.append("amendment-2 changed after the campaign recorded it")
        if amendment.get("base_budget_sha256") != campaign.get("budget_sha256"):
            problems.append("amendment-2 frozen for a different budget")
        if amendment.get("base_addendum_sha256") != campaign.get("g4_addendum_sha256"):
            problems.append("amendment-2 frozen for a different addendum")
        if (amendment.get("inherits") or {}).get("g4_amendment_1_sha256") != campaign.get("g4_amendment_sha256"):
            problems.append("amendment-2 frozen for a different amendment 1")
        if (pins.get("gate_campaign_tool") or {}).get("new_sha256") != campaign.get("tool_sha256"):
            problems.append("campaign tool differs from the one amendment-2 pins")
        frozen = amendment.get("frozen_at_unix")
        if not isinstance(frozen, (int, float)) or not frozen < float(campaign.get("started_unix") or 0):
            problems.append("amendment-2 not frozen before this campaign started")
    if problems:
        return None, ["G4 amendment 2 not applied: " + "; ".join(problems)], source
    return amendment, [], source


def applicable_g4_amendment(campaign: dict, table: dict) -> tuple[dict | None, list[str], dict | None]:
    """(amendment or None, notes, source). Applies ONLY to a campaign that recorded the amendment at run time: older
    campaigns (nothing recorded) score exactly as before. A recorded amendment applies when the file is unchanged,
    valid, frozen before the campaign started, and pinned to the campaign's budget, addendum and tool sha256 (the tool
    pin is amendment 1's own or, when the campaign recorded an applicable amendment 2, the one amendment 2 re-pins)."""
    recorded = campaign.get("g4_amendment_path")
    if not recorded:
        return None, [], None
    path = Path(recorded)
    if not path.exists():
        return None, [f"G4 amendment not applied: {path} recorded at run time is missing"], None
    source = {"path": str(path), "sha256": sha256_file(path)}
    amendment = load_json(path)
    problems = validate_g4_amendment(amendment, table) if amendment is not None else ["unreadable amendment"]
    if not problems:
        pins = amendment.get("pinned") or {}
        if campaign.get("g4_amendment_sha256") != source["sha256"]:
            problems.append("amendment changed after the campaign recorded it")
        if amendment.get("base_budget_sha256") != campaign.get("budget_sha256"):
            problems.append("amendment frozen for a different budget")
        if amendment.get("base_addendum_sha256") != campaign.get("g4_addendum_sha256"):
            problems.append("amendment frozen for a different addendum")
        tool_pins = {(pins.get("gate_campaign_tool") or {}).get("new_sha256")}
        if campaign.get("g4_amendment_2_path"):
            repin = applicable_g4_amendment_2(campaign)[0]
            if repin is not None:
                tool_pins.add(((repin.get("pinned") or {}).get("gate_campaign_tool") or {}).get("new_sha256"))
        if campaign.get("tool_sha256") not in tool_pins:
            problems.append("campaign tool differs from the one the amendment pins")
        frozen = amendment.get("frozen_at_unix")
        if not isinstance(frozen, (int, float)) or not frozen < float(campaign.get("started_unix") or 0):
            problems.append("amendment not frozen before this campaign started")
    if problems:
        return None, ["G4 amendment not applied: " + "; ".join(problems)], source
    return amendment, [], source


def okx_probe_window(probe_dir: Path, observations) -> dict | None:
    """Report-only venue probe statistics of one G4 window over the observer's own observed window, with the host
    clock bound of the probe receipt; None when the window had no probe."""
    from scripts import p184_g4_observer as observer
    from scripts import p184_g4_okx_mark_index_probe as probe
    path = Path(probe_dir) / "probe.jsonl"
    if not path.exists():
        return None
    aggregate = observer.last_aggregate(observations) or {}
    start, end = aggregate.get("observed_start_ns"), aggregate.get("observed_end_ns")
    if not start or not end:
        return {"covered": False, "coverage_problems": ["observer window bounds unknown"], "report_only": True}
    out = probe.analyze(path.read_text().splitlines(), int(start), int(end))
    receipt = load_json(Path(probe_dir) / "receipt.json") or {}
    out.update(host_clock_bound_ms_max=receipt.get("host_clock_bound_ms_max"), probe_exit=receipt.get("exit"),
               probe_sha256=sha256_file(path))
    if receipt.get("host_clock_bound_ms_max") is None:
        out["covered"] = False
        out["coverage_problems"] = out.get("coverage_problems", []) + ["no host clock bound"]
    return out


def _bucket_lower(v: dict):
    return (v.get("p95_bucket_lower") if v.get("percentile_basis") == "BUCKET_UPPER_BOUND" else v.get("p95")) \
        if v.get("n") else None


def score_g4_amended_row(windows: list[dict], key: str, frozen_limit: dict, amendment: dict) -> dict:
    """G4 amendment 1 row: verdict on the internal split; composite, component gap and probe report-only."""
    limits = amendment["limits"]
    p95_cap, max_cap = limits["candidate_p95_limit_ms"], limits["candidate_max_limit_ms"]
    blocks = [((w.get("summary") or {}).get(key) or {}) for w in windows]
    composite = [b.get("visibility_ms") or {} for b in blocks]
    splits = [b.get("paired_split") for b in blocks]
    internal = [(s or {}).get(G4_AMENDMENT_QUANTITY) or {} for s in splits]
    gap = [(s or {}).get("component_ts_gap_ms") or {} for s in splits]
    probes = [w.get("okx_probe") for w in windows]
    n = [v.get("n", 0) for v in internal]
    p95 = [v.get("p95") if v.get("n") else None for v in internal]
    lower = [_bucket_lower(v) for v in internal]
    maxes = [v.get("max") if v.get("n") else None for v in internal]
    over_p95 = [x is not None and x > p95_cap and lo is not None and lo > p95_cap for x, lo in zip(p95, lower)]
    unresolved = [x is not None and x > p95_cap and not o for x, o in zip(p95, over_p95)]
    over_max = [k >= G4_AMENDMENT_MIN_SAMPLES and m is not None and m > max_cap for m, k in zip(maxes, n)]
    problems, attribution = [], []
    for i, (c, s, k, probe) in enumerate(zip(composite, splits, n, probes), start=1):
        if c.get("n") and s is None:
            problems.append(f"window {i}: no paired split (observer older than v6)")
        elif s is not None:
            if s.get("paired_missing"):
                problems.append(f"window {i}: {s['paired_missing']} events without a parsable component split")
            if s.get("published_mismatch"):
                problems.append(f"window {i}: {s['published_mismatch']} events with published_at != newest receipt")
            if (s.get("missing") or {}).get(G4_AMENDMENT_QUANTITY):
                problems.append(f"window {i}: internal split without a producer timestamp")
        if not k:
            attribution.append(None)
        elif over_p95[i - 1] or over_max[i - 1] or unresolved[i - 1]:
            attribution.append("INTERNAL_OVER_OR_UNRESOLVED")
        elif probe and probe.get("covered"):
            attribution.append("EXTERNAL_VENUE_COMPONENT_CADENCE")
        else:
            attribution.append("UNKNOWN")
    unknown = [i for i, a in enumerate(attribution, start=1) if a == "UNKNOWN"]
    if unknown:
        problems.append(f"windows {unknown}: no covering venue probe, attribution UNKNOWN (not EXTERNAL)")
    verdict = ("NOT_MEASURED" if not any(n) else "FAIL" if any(over_p95) or any(over_max) else
               "NOT_MEASURED" if problems or any(unresolved) else "PASS")
    cap = frozen_limit["candidate_p95_limit_ms"]
    c_p95 = [v.get("p95") if v.get("n") else None for v in composite]
    c_lower = [_bucket_lower(v) for v in composite]
    c_over = [x is not None and x > cap and lo is not None and lo > cap for x, lo in zip(c_p95, c_lower)]
    keep = ("n", "p50", "p95", "p95_bucket_lower", "max", "negative", "percentile_basis")
    return {
        "row": key, "basis": "amendment", "amendment_id": amendment.get("amendment_id"), "verdict": verdict,
        "quantity": G4_AMENDMENT_QUANTITY, "p95_ms_per_window": p95, "p95_bucket_lower_ms_per_window": lower,
        "max_ms_per_window": maxes, "n_per_window": n, "limit_p95_ms": p95_cap, "limit_max_ms": max_cap,
        "max_gated_windows": [k >= G4_AMENDMENT_MIN_SAMPLES for k in n], "unresolved_windows": unresolved,
        "attribution_per_window": attribution, "problems": problems,
        "report_only": {
            "composite_visibility_ms_per_window": [{k: v.get(k) for k in keep} if v.get("n") else None
                                                   for v in composite],
            "composite_frozen_limit_p95_ms": cap,
            "composite_over_frozen_limit_per_window": c_over,
            "composite_outliers_per_window": [b.get("outliers") for b in blocks],
            "component_ts_gap_ms_per_window": [{k: v.get(k) for k in keep} if v.get("n") else None for v in gap],
            "paired_split_per_window": [{name: ({k: (s.get(name) or {}).get(k) for k in keep}
                                                if isinstance(s.get(name), dict) else s.get(name))
                                         for name in ("component_receipt_gap_ms", "mark_venue_to_dl_ms",
                                                      "index_venue_to_dl_ms", "older_component", "paired_missing",
                                                      "published_mismatch")} if s else None for s in splits],
            "internal_outliers_per_window": [(s or {}).get("internal_outliers") for s in splits],
            "okx_probe_per_window": probes}}


def score_g4(windows: list[dict], table: dict, addendum: dict | None = None,
             amendment: dict | None = None, amendment2: dict | None = None) -> tuple[str, list, list[str]]:
    """windows: [{'summary': {row: {'visibility_ms': {...}}}, 'inverse_rows': {...}}]; table: budget rows.
    addendum (validated, optional): GATED rows score windows with >= 5 samples on max vs the frozen limit; REPORT_ONLY
    rows are returned with verdict REPORT_ONLY and never change the gate; rows in neither stay NOT_MEASURED.
    amendment (validated, optional, G4 amendment 1): its one row is scored by score_g4_amended_row instead."""
    rows, failed, uncovered, straddled = [], [], [], []
    for key, limit in sorted(table.items()):
        if amendment is not None and key == amendment.get("row"):
            row = score_g4_amended_row(windows, key, limit, amendment)
            rows.append(row)
            if row["verdict"] == "FAIL":
                failed.append(key)
            elif row["verdict"] == "NOT_MEASURED":
                uncovered.append(key)
            continue
        values = [((w.get("summary") or {}).get(key) or {}).get("visibility_ms", {}) for w in windows]
        p95 = [v.get("p95") if v.get("n") else None for v in values]
        n = [v.get("n", 0) for v in values]
        cap = limit["candidate_p95_limit_ms"]
        # Observer v4 p95 = bucket upper bound: decisive FAIL only when the bucket's lower edge is over the limit; a
        # bucket straddling the limit is unresolved (NOT_MEASURED), never FAIL and never PASS. v1-v3 p95 are exact.
        lower = [(v.get("p95_bucket_lower") if v.get("percentile_basis") == "BUCKET_UPPER_BOUND" else v.get("p95"))
                 if v.get("n") else None for v in values]
        over = [x is not None and x > cap and (lo is not None and lo > cap) for x, lo in zip(p95, lower)]
        unresolved = [x is not None and x > cap and not o for x, o in zip(p95, over)]
        verdict = ("NOT_MEASURED" if not any(n) else "FAIL" if any(over) else
                   "NOT_MEASURED" if any(unresolved) else "PASS")
        row = {"row": key, "p95_ms_per_window": p95, "n_per_window": n, "limit_p95_ms": cap, "verdict": verdict}
        if any(v.get("percentile_basis") for v in values):
            row.update(p95_basis="BUCKET_UPPER_BOUND", p95_bucket_lower_ms_per_window=lower,
                       max_ms_per_window=[v.get("max") if v.get("n") else None for v in values])
        if any(unresolved) and verdict == "NOT_MEASURED":
            row["unresolved_windows"] = unresolved
            straddled.append(key)
        rows.append(row)
        if verdict == "FAIL":
            failed.append(key)
        elif verdict == "NOT_MEASURED" and key not in straddled:
            uncovered.append(key)
    thin = []
    for key, spec in sorted(((addendum or {}).get("rows") or {}).items()):
        stats = [g4_window_stats(w, key) for w in windows]
        n = [v.get("n", 0) for v in stats]
        maxes = [v.get("max") if v.get("n") else None for v in stats]
        row = {"row": key, "basis": "addendum", "max_ms_per_window": maxes, "n_per_window": n}
        if spec["status"] == "REPORT_ONLY":
            rows.append({**row, "verdict": "REPORT_ONLY", "reason": spec.get("reason")})
            continue
        limit = spec["candidate_max_limit_ms"]
        gated = [m for m, k in zip(maxes, n) if k >= G4_ADDENDUM_MIN_SAMPLES and m is not None]
        verdict = "NOT_MEASURED" if not gated else "FAIL" if any(m > limit for m in gated) else "PASS"
        rows.append({**row, "limit_max_ms": limit, "verdict": verdict,
                     "gated_windows": [k >= G4_ADDENDUM_MIN_SAMPLES for k in n]})
        if verdict == "FAIL":
            failed.append(key)
        elif verdict == "NOT_MEASURED":
            thin.append(key)
    observed = {key for w in windows for key in (w.get("summary") or {})}
    inverse = {key for w in windows for key in (w.get("inverse_rows") or {})}
    unbaselined = sorted((observed | inverse) - set(table) - set(((addendum or {}).get("rows") or {})))
    if amendment2 is not None:
        # G4 amendment 2 (B1): unbaselined bar rows >= 8h are reported, never scored; every other row is unchanged.
        for key in [k for k in unbaselined if g4_long_bar_row(k)]:
            stats = [g4_window_stats(w, key) for w in windows]
            rows.append({"row": key, "basis": "amendment_2", "amendment_id": amendment2.get("amendment_id"),
                         "max_ms_per_window": [v.get("max") if v.get("n") else None for v in stats],
                         "n_per_window": [v.get("n", 0) for v in stats], "verdict": "REPORT_ONLY",
                         "reason": "bar interval >= 8h without a frozen baseline (G4 amendment 2, owner B1)"})
        unbaselined = [k for k in unbaselined if not g4_long_bar_row(k)]
    reasons = []
    if failed:
        reasons.append(f"over limit: {', '.join(failed)}")
    amended = [r for r in rows if r.get("basis") == "amendment" and r["verdict"] == "NOT_MEASURED"]
    if amended:
        uncovered = [k for k in uncovered if k not in {r["row"] for r in amended}]
        reasons.append("amendment row not measured: " + "; ".join(
            f"{r['row']} ({'; '.join(r['problems']) or 'no internal-split sample or unresolved p95'})" for r in amended))
    if uncovered:
        reasons.append(f"no sample in any window: {', '.join(uncovered)}")
    if straddled:
        reasons.append(f"p95 bucket straddles the limit (unresolved, not a failure): {', '.join(straddled)}")
        uncovered = uncovered + straddled
    if thin:
        reasons.append(f"addendum rows without a window of >= {G4_ADDENDUM_MIN_SAMPLES} samples: {', '.join(thin)}")
        uncovered = uncovered + thin
    if unbaselined:
        reasons.append(f"observed rows without a frozen baseline: {', '.join(unbaselined)}")
    if failed:
        return "FAIL", rows, reasons
    if uncovered or unbaselined or amended:
        return "NOT_MEASURED", rows, reasons
    return "PASS", rows, []


def g4_inverse_rows(observations) -> dict:
    """Separate OKX inverse (native symbol *-USD-SWAP) visibility from the pooled OKX rows; same quantity.
    Observer v4: from the full-window aggregate (p95 BUCKET_UPPER_BOUND with its bucket lower edge, exact max)."""
    from scripts import p184_g4_observer as observer
    observations = list(observations)
    aggregate = observer.last_aggregate(observations)
    if aggregate is not None:
        out = {}
        for key, row in sorted((aggregate.get("inverse_rows") or {}).items()):
            stats = observer.Histogram.from_json((row.get("admitted") or {}).get("visibility_ms")).stats()
            if stats["n"]:
                out[key] = {k: stats[k] for k in ("n", "p95", "p95_bucket_lower", "max", "percentile_basis")}
        return out
    groups = {}
    for item in observations:
        if item.get("kind") != "canonical_observation":
            continue
        symbol = (item.get("identity") or {}).get("native_symbol") or ""
        if not symbol.endswith("-USD-SWAP"):
            continue
        source, seen = item.get("source_event_ns"), item.get("observer_receipt_wall_ns")
        if source and seen:
            key = f"{item['feed']}|OKX_INVERSE|{item.get('interval') or ''}"
            groups.setdefault(key, []).append((seen - source) / 1e6)
    out = {}
    for key, values in sorted(groups.items()):
        ordered = sorted(values)
        out[key] = {"n": len(ordered), "p95": round(ordered[min(len(ordered) - 1, int(.95 * len(ordered)))], 1),
                    "max": round(ordered[-1], 1)}
    return out


def _g6_base_key(key: str) -> str:
    return key.split(G6_TS_PROBE_SUFFIX, 1)[0].split("@", 1)[0]


def score_g6(quality: dict | None) -> tuple[str, dict, list[str]]:
    """Only D3_EXECUTION rows decide the gate. Rows of any other basis (e.g. the ALPHA reads of the stage-100 load,
    REQUIREMENT_ALPHA) are reported, and an unsafe view there is still a FAIL because it was eligible beyond its OWN
    requirement (a DL eligibility defect), but they can never make the D3 gate PASS. Rows without policy_basis
    (receipts from before the basis was recorded) were classified against an unknown basis: reported, not scored."""
    quality = quality or {}
    rows = {key: {k: row.get(k) for k in ("n", "policy_ms", "policy_basis", "age_basis", "age_ms", "max_age_ms",
                                          "max_client_age_ms", "classes", "refusals", "reasons", "unsafe")}
            for key, row in quality.items()}
    gated = {key: row for key, row in quality.items() if key.split("|", 1)[0] in G6_GATED_FEEDS}
    d3 = {key: row for key, row in gated.items() if row.get("policy_basis") == G6_D3_BASIS}
    own = {key: row for key, row in gated.items() if row.get("policy_basis") not in (None, G6_D3_BASIS)}
    legacy = sorted(key for key, row in quality.items() if not row.get("policy_basis"))
    unsafe_d3 = {key: row["unsafe"] for key, row in d3.items() if row.get("unsafe")}
    unsafe_own = {key: row["unsafe"] for key, row in own.items() if row.get("unsafe")}
    notes = [f"rows without policy_basis reported, not scored: {', '.join(legacy)}"] if legacy else []
    if unsafe_d3 or unsafe_own:
        why = []
        if unsafe_d3:
            why.append(f"unsafe eligibility under the D3 execution policy: {unsafe_d3}")
        if unsafe_own:
            why.append(f"unsafe eligibility under the read's own requirement (DL eligibility defect): {unsafe_own}")
        return "FAIL", rows, why + notes
    measured = {}
    for key, row in d3.items():
        measured[_g6_base_key(key)] = measured.get(_g6_base_key(key), 0) + (row.get("n") or 0)
    missing = [key for key in G6_REQUIRED if not measured.get(key)]
    if missing:
        other = sorted({row.get("policy_basis") for row in own.values()})
        return "NOT_MEASURED", rows, [f"no EXECUTION (D3) view for: {', '.join(missing)}" + (
            f"; rows of {', '.join(other)} are reported only and cannot pass the D3 gate" if other else "")] + notes
    return "PASS", rows, notes


def g6_ts_probe_rows(summary: dict | None) -> dict:
    """The TS execution probe's rows, keyed apart (G6_TS_PROBE_SUFFIX) so they never merge into a driver row."""
    return {f"{key}{G6_TS_PROBE_SUFFIX}": row for key, row in ((summary or {}).get("rows") or {}).items()}


def g6_ts_probe_coverage(summary: dict | None, load_s) -> list[str]:
    """The probe must have run as the writer for the whole load window; otherwise its rows cannot PASS G6."""
    if summary is None:
        return ["no G6 TS execution probe summary"]
    window = summary.get("window_unix_s")
    if not load_s or not window:
        return ["G6 TS execution probe or load window unknown"]
    gaps = []
    if window[0] > load_s[0] or window[1] < load_s[1]:
        gaps.append(f"G6 TS execution probe window {window} does not cover the load window {load_s}")
    if summary.get("kind") != "final":
        gaps.append("G6 TS execution probe summary is a partial line (the probe did not exit cleanly)")
    return gaps


def read_probe_lines(path: Path) -> dict | None:
    """Last complete JSON line of the probe's stdout (final if it exited cleanly, else the newest partial)."""
    last = None
    try:
        for line in Path(path).read_text().splitlines():
            try:
                item = json.loads(line)
            except ValueError:
                continue
            if item.get("kind") in ("partial", "final"):
                last = item
    except OSError:
        return None
    return last


def native_raw_sample_scope(transit: dict | None, load_ns) -> str:
    native = ((transit or {}).get("files_by_kind") or {}).get("native") or {}
    first, last = native.get("first_start_ns"), native.get("last_start_ns")
    if not load_ns or first is None or last is None:
        return "native raw sample: no records"
    lo, hi = max(first, load_ns[0]), min(last, load_ns[1])
    covered = max(0, hi - lo) / 1e9
    return (f"native raw sample spans {covered:.0f} s of the {(load_ns[1] - load_ns[0]) / 1e9:.0f} s load "
            f"({native.get('files')} capped file(s))")


def expected_trade_routes(campaign: dict) -> list[str] | None:
    """The frozen TRADE route roster (TRADE|VENUE|SYMBOL) of the campaign's binding (preflight.binding), or None."""
    path = (campaign.get("preflight") or {}).get("binding") or campaign.get("binding")
    binding = load_json(Path(path)) if path else None
    if not isinstance(binding, dict) or not isinstance(binding.get("products"), list):
        return None
    try:
        return sorted({f"TRADE|{str(p['venue']).upper()}|{str(p['native_symbol']).upper()}"
                       for p in binding["products"] if p.get("feed") == "TRADE"})
    except (KeyError, TypeError, AttributeError):
        return None


def _native_typed_verdict(native_typed: dict | None) -> tuple[dict, list[str], list[str]]:
    """(measured, fail reasons, coverage gaps) of the heartbeat typed TRADE counters (g910.summarize_g10_native)."""
    if not native_typed or not native_typed.get("present"):
        return ({"present": False}, [], ["native typed TRADE counters (heartbeat details.trade_integrity: "
                                         "snapshot_replaced, sequence_resets, gaps, continuity_errors, "
                                         "cursor_expired_errors, slow_consumer_errors, offset_regressions, "
                                         "duplicates_applied) not sampled: the native writer logs no typed "
                                         "disconnect/control line, so zero log events is not zero native failures"])
    measured = {k: native_typed.get(k) for k in ("complete", "failures", "blocking", "report_only", "breaks",
                                                 "samples_in_bracket", "bracket_from_ns", "bracket_to_ns", "basis",
                                                 "limits")}
    fails = [f"native typed {cls}={n}" for cls, n in (native_typed.get("failures") or {}).items() if n]
    gaps = []
    if not native_typed.get("complete"):
        breaks = native_typed.get("breaks") or ["no usable samples"]
        gaps.append(f"native typed TRADE counters do not cover the load window as one unbroken activation "
                    f"({len(breaks)} break(s), first: {breaks[0]})")
    blocking = {k: v for k, v in (native_typed.get("blocking") or {}).items() if v}
    if blocking:
        gaps.append(f"native typed {blocking}: stream offsets went back with no reset (unexplained, not zero)")
    return measured, fails, gaps


def score_g10(g10: dict, duplicates: dict | None, trace_gaps: list[str], *,
              sample_scope: str | None = None, native_g10: dict | None = None,
              native_gaps: list[str] | None = None, expected_routes: list[str] | None = None,
              native_typed: dict | None = None) -> tuple[str, dict, list[str]]:
    """sample_scope set (native TRADE): the raw-trace duplicates come from the capped raw sample; a duplicate there
    FAILs, zero in a sample is not a full-window zero. native_g10 (transit native_summary.g10, TS apply_integrity):
    validated per-route apply-decision counters. A PASS needs (Codex review SR2/SR3): every route of the frozen TRADE
    roster (expected_routes, from the binding) present with valid counters (a present route with no rows in scope is
    an explicit QUIET disposition; an absent one is not measured), an activation proven to span the load (native_gaps
    empty), zero lost apply replies (unconfirmed rows are INCOMPLETE unless reconciled; the summary carries no
    reconciliation, so any is blocking), zero APPLIED-after-lost-reply rows, and the native typed continuity counters
    (native_typed, g910.summarize_g10_native over the heartbeat) covering the load window with zero failures.
    `duplicates applied` for native rows is the offset high-water check (see DUPLICATE_APPLIED_BASIS), not an
    event-identity oracle. Missing counters -> NOT_MEASURED, never zero. The counters cannot prove that no source
    record is missing; DL-signalled gaps come from the typed counters and the Python log lines."""
    dup = {k: v for k, v in (duplicates or {}).items() if k.startswith("TRADE|")}
    raw_dup = sum(dup.values()) if duplicates is not None else None
    counters = {"gaps": g10.get("trade_gap"), "duplicates_applied": raw_dup,
                "duplicates_applied_scope": sample_scope or "full load window (raw trace)",
                "replay_expired": g10.get("trade_replay_expired"), "overflow": g10.get("trade_overflow"),
                "resumed_reconnects": g10.get("trade_resumed"), "log_polls": g10.get("log_polls"),
                "log_unreadable": g10.get("log_unreadable"), "codes": g10.get("codes"),
                "log_basis": "Python worker typed log lines (the native writer emits none)"}
    native_rows = None
    fails, gaps = [], []
    if sample_scope is not None:
        native_rows = {k: v for k, v in ((native_g10 or {}).get("per_feed_venue") or {}).items()
                       if k.startswith("TRADE|")}
        routes = {k: v for k, v in ((native_g10 or {}).get("per_route") or {}).items() if k.startswith("TRADE|")}
        if native_rows:
            counters.update(
                duplicates_applied=sum(r.get("duplicate_applied_in_window") or 0 for r in native_rows.values())
                + (raw_dup or 0),
                duplicates_applied_scope=(f"native apply-decision counters, {native_g10['scope']} (+ raw sample)"
                                          if (native_g10 or {}).get("scope") else
                                          "native apply-decision counters over the whole activation (+ raw sample)"),
                duplicates_applied_oracle=(native_g10 or {}).get("duplicate_applied_basis")
                or "offset high-water check (TS latency.rs), not an event-identity duplicate oracle",
                applied_at_or_below_offset_high_water={k: r.get("duplicate_applied_in_window")
                                                       for k, r in native_rows.items()},
                duplicates_applied_raw_sample=raw_dup, raw_sample_scope=sample_scope,
                replay_attempted={k: r.get("replay_attempted") for k, r in native_rows.items()},
                replay_decisions={k: r.get("replay_decisions") for k, r in native_rows.items()},
                ambiguous_after_unconfirmed={k: r.get("ambiguous_after_unconfirmed") for k, r in native_rows.items()},
                unconfirmed_rows={k: r.get("unconfirmed_rows") for k, r in native_rows.items()},
                unconfirmed_units={k: r.get("unconfirmed_units") for k, r in native_rows.items()},
                epochs_total={k: r.get("epochs_total") for k, r in native_rows.items()},
                decisions={k: r.get("decisions") for k, r in native_rows.items()},
                route_disposition={k: ("QUIET_NO_ROWS_IN_SCOPE" if r.get("rows") == 0 else "ACTIVE")
                                   for k, r in sorted(routes.items())},
                route_epochs={k: [{"worker": a.get("worker"), "pid": a.get("pid"), "epochs_total": a.get(
                    "epochs_total"), "epochs": a.get("epochs")} for a in r.get("activations") or []]
                              for k, r in sorted(routes.items())},
                expected_routes=expected_routes)
        typed, typed_fails, typed_gaps = _native_typed_verdict(native_typed)
        counters["native_typed"] = typed
        fails += typed_fails
        if native_rows:
            if expected_routes is None:
                gaps.append("no frozen TRADE route roster (campaign binding unreadable): route coverage unprovable")
            else:
                missing = [r for r in expected_routes if r not in routes]
                if missing:
                    gaps.append(f"{len(missing)} frozen TRADE route(s) without apply-decision counters (quiet and "
                                f"uninstrumented are indistinguishable): {', '.join(missing)}")
                extra = sorted(set(routes) - set(expected_routes))
                if extra:
                    counters["routes_outside_roster"] = extra
            invalid = [r for r in (native_g10 or {}).get("invalid_routes") or [] if str(r.get("route")).startswith(
                "TRADE|") or r.get("route") == "?"]
            if invalid:
                gaps.append(f"{len(invalid)} invalid apply-decision counter row(s): {invalid[0]}")
            missing_venues = [venue for venue in VENUES if f"TRADE|{venue}" not in native_rows]
            if missing_venues:
                gaps.append(f"no native apply-decision counters for TRADE on {', '.join(missing_venues)}")
            cover = list(native_gaps or []) + list((native_g10 or {}).get("coverage_gaps") or [])
            if cover:
                gaps.append("native counters do not provably cover the load window: " + "; ".join(cover))
            unconfirmed = sum((r.get("unconfirmed_rows") or 0) + (r.get("unconfirmed_units") or 0)
                              for r in native_rows.values())
            if unconfirmed:
                gaps.append(f"INCOMPLETE: TRADE apply replies lost (unconfirmed rows/units "
                            f"{counters['unconfirmed_rows']}/{counters['unconfirmed_units']}) with no reconciled "
                            "publication evidence in the summary")
            ambiguous = sum(r.get("ambiguous_after_unconfirmed") or 0 for r in native_rows.values())
            if ambiguous:
                gaps.append(f"{ambiguous} TRADE rows APPLIED after a lost apply reply: neither proven duplicate nor "
                            "unique")
        elif duplicates is None or trace_gaps:
            gaps.append("duplicates applied need the delivery trace: " + "; ".join(trace_gaps or ["no trace"]))
        else:
            gaps.append(f"duplicates applied (native TRADE) measured on a sample only ({sample_scope}); a sample can "
                        "fail, never pass; the native summary has no apply-decision counters (TS writer without "
                        "apply_integrity)")
        gaps += typed_gaps
    elif native_typed is not None and native_typed.get("present"):
        typed, typed_fails, _ = _native_typed_verdict(native_typed)  # Python-owned TRADE: typed counters can FAIL
        counters["native_typed"] = typed
        fails += typed_fails
    bad = {k: counters[k] for k in ("gaps", "duplicates_applied", "replay_expired", "overflow") if counters[k]}
    if bad:
        fails.insert(0, f"non-zero: {bad}")
    if fails:
        return "FAIL", counters, fails
    if not g10.get("log_polls") or g10.get("log_unreadable"):
        gaps.insert(0, "writer logs not read for the whole window")
    if sample_scope is None and (duplicates is None or trace_gaps):
        gaps.append("duplicates applied need the delivery trace: " + "; ".join(trace_gaps or ["no trace"]))
    return ("NOT_MEASURED", counters, gaps) if gaps else ("PASS", counters, [])


def utc_stamp(unix_s: float) -> str:
    return dt.datetime.fromtimestamp(unix_s, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def load_json(path: Path):
    try:
        return json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return None


def sha256_file(path: Path) -> str | None:
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError:
        return None


def score_g5(gates: dict | None) -> tuple[str, list, list, list]:
    """(verdict, verdict rows, failed verdict rows, display-only g5:* rows). The verdict reads latency:*/cold* rows
    only, exactly as before; g5:COLD_* (frozen cold-shape limits) rows are displayed in `measured` but already decide
    through the protocol (driver acceptance -> protocol_problems), so adding them here changes no verdict."""
    rows = (gates or {}).get("gates", [])
    latency = [g for g in rows if str(g.get("gate", "")).startswith(("latency:", "cold"))]
    failed = [g["gate"] for g in latency if g.get("status") != "PASS"]
    shape = [g for g in rows if str(g.get("gate", "")).startswith("g5:")]
    verdict = "FAIL" if failed else "PASS" if latency else "NOT_MEASURED"
    return verdict, latency, failed, shape


def applicable_g4_addendum(campaign: dict, table: dict) -> tuple[dict | None, list[str], dict | None]:
    """(addendum or None, notes, source) for one candidate campaign. Absent file -> (None, [], None): the original
    scoring is unchanged. A present file applies only when it is valid against this frozen table, was frozen for the
    campaign's budget, was frozen BEFORE the campaign started and (when the campaign recorded it) is the same file."""
    path = Path(campaign.get("g4_addendum_path") or G4_ADDENDUM_PATH)
    if not path.exists():
        if campaign.get("g4_addendum_sha256"):
            return None, [f"G4 addendum not applied: {path} recorded at run time is missing"], None
        return None, [], None
    source = {"path": str(path), "sha256": sha256_file(path)}
    addendum = load_json(path)
    problems = validate_g4_addendum(addendum, table) if addendum is not None else ["unreadable addendum"]
    if not problems:
        if addendum.get("base_budget_sha256") != campaign.get("budget_sha256"):
            problems.append("addendum frozen for a different budget")
        frozen = addendum.get("frozen_at_unix")
        if not isinstance(frozen, (int, float)) or not frozen < float(campaign.get("started_unix") or 0):
            problems.append("addendum not frozen before this campaign started")
        if campaign.get("g4_addendum_sha256") not in (None, source["sha256"]):
            problems.append("addendum changed after the campaign recorded it")
    if problems:
        return None, ["G4 addendum not applied: " + "; ".join(problems)], source
    return addendum, [], source


def build_scorecard(campaign_dir: Path, *, trace_dir: Path | None = None) -> dict:
    """trace_dir: where transit-load.json / mark-load.json are read (default <campaign>/trace; `score --out-dir`
    re-analyses into its own directory and never writes into the campaign)."""
    d = Path(campaign_dir)
    t = Path(trace_dir) if trace_dir is not None else d / "trace"
    campaign = load_json(d / "campaign.json") or {}
    receipt = load_json(d / "stage100" / "receipt.json")
    gates = load_json(d / "stage100" / "gates.json")
    host = load_json(d / "stage100" / "host.json")
    ok, problems = _protocol(campaign, receipt, gates)
    load_s = (receipt or {}).get("load_window_unix_s")
    load_ns = [int(load_s[0] * 1e9), int(load_s[1] * 1e9)] if load_s else None
    transit = load_json(t / "transit-load.json")
    mark = load_json(t / "mark-load.json")
    until_ns = int((campaign.get("trace") or {}).get("until_unix_ns") or 0)
    native = native_feeds_of(campaign, transit)
    trace_gaps = _trace_coverage(transit, load_ns, until_ns)  # every raw file complete (no native writer)
    python_gaps = _trace_coverage(transit, load_ns, until_ns, raw_kinds=("python",))
    native_gaps = native_coverage(transit) if native else []
    # G1 (BOOK) and MARK are Python-worker feeds; native feeds are read from the native summary.
    raw_gaps = python_gaps if native else trace_gaps
    mixed_gaps = list(dict.fromkeys(raw_gaps + native_gaps))
    scored = scoring_view(transit, native)
    out = []

    def add(gid, quantity, rule, verdict, reason, **extra):
        out.append(_gate(gid, quantity, rule, verdict, reason, ok, problems, **extra))

    trace_src = {"transit": sha256_file(t / "transit-load.json"), "mark": sha256_file(t / "mark-load.json")}
    v, rows, why = score_transit_rows(transit, TRACE_FEEDS_G1)
    if raw_gaps and v != "FAIL":
        v, why = "NOT_MEASURED", why + raw_gaps
    add("G1", "end-to-end to TS cache (venue event -> TS Redis APPLY reply)", "p95 <= 500 ms, p99 <= 1000 ms (n >= 100), "
        "max < 2000 ms per feed/venue", v, "; ".join(why) or "every BOOK row inside", measured=rows, sources=trace_src)
    v1, rows1, why1 = score_transit_rows(scored, TRACE_FEEDS_G2)
    v2, rows2, why2 = score_mark(mark)
    verdicts = {v1, v2}
    v = "FAIL" if "FAIL" in verdicts else "NOT_MEASURED" if "NOT_MEASURED" in verdicts else "PASS"
    if mixed_gaps and v != "FAIL":
        v = "NOT_MEASURED"
    native_src = {"native_feeds": list(native), "native_coverage_gaps": native_gaps,
                  "window_scope": ((transit or {}).get("native_summary") or {}).get("window_scope")} if native else {}
    # Native load-slice scope (or its cumulative fallback) is named in the reason: never a silent pass on either.
    slice_scope = ((transit or {}).get("native_summary") or {}).get("slice_scope") if native else None
    scope_note = ([f"native scope {slice_scope['mode']}: {slice_scope['distribution_scope']}"
                   + (f" ({'; '.join(slice_scope['slice_fallback_reasons'])})"
                      if slice_scope.get("slice_fallback_reasons") else "")] if slice_scope else [])
    add("G2", "end-to-end to TS cache, new-event transit", "TRADE p95 <= 500 ms and max < 1500 ms; QUOTE and MARK "
        "p95 <= 500 ms and max < 1000 ms", v, with_scope(why1 + why2 + (mixed_gaps if v == "NOT_MEASURED" else []),
                                                         "every row inside", scope_note),
        measured=rows1 + rows2, sources=trace_src, **native_src)
    g3 = load_json(d / "g3" / "g3-summary.json")
    add("G3", "end-to-end to TS cache, BAR from close (first visibility of the closed bar)", "p95 <= 5 s, max < 60 s",
        (g3 or {}).get("verdict", "NOT_MEASURED"), "g3 probe" if g3 else "no G3 summary",
        measured={k: (g3 or {}).get(k) for k in ("per_venue", "pooled", "unseen_total", "unresolved_slices",
                                                 "read_errors", "poll_ms")},
        sources={"g3": sha256_file(d / "g3" / "g3-summary.json")})
    budget = load_json(campaign.get("budget_path", "")) if campaign.get("budget_path") else None
    table = ((budget or {}).get("g4_delivery_visibility") or {}).get("table") or {}
    addendum, addendum_notes, addendum_src = applicable_g4_addendum(campaign, table)
    amendment, amendment_notes, amendment_src = applicable_g4_amendment(campaign, table)
    amendment2, amendment2_notes, amendment2_src = applicable_g4_amendment_2(campaign)
    g4_windows, g4_receipts, books_probes = [], [], []
    for item in campaign.get("g4_windows", []):
        summary = load_json(d / f"g4-{item['index']}-summary.json")
        obs = d / f"g4-{item['index']}" / "observations.jsonl"
        inverse = g4_inverse_rows(read_jsonl(obs)) if obs.exists() else {}
        g4_windows.append({"summary": summary, "inverse_rows": inverse})
        if amendment is not None:
            g4_windows[-1]["okx_probe"] = (okx_probe_window(d / f"g4-{item['index']}-okx-probe", read_jsonl(obs))
                                           if obs.exists() else None)
        if item.get("okx_books_probe_container"):
            from scripts import p184_g4_observer as observer
            from scripts import p184_g4_okx_books_probe as books
            books_probes.append(books.window_report(d / f"g4-{item['index']}-okx-books",
                                                    observer.last_aggregate(read_jsonl(obs)) if obs.exists() else None))
        g4_receipts.append({"index": item["index"], "offset": item.get("offset"), "seconds": item.get("seconds"),
                            "boundary": item.get("boundary"),
                            **(g4_window_receipt(read_jsonl(obs)) if obs.exists() else {"records": None})})
    # Report-only provenance (never changes a verdict): which schedule/observer produced the windows, record caps.
    g4_provenance = {"g4_apparatus": campaign.get("g4_apparatus") or {"schedule_version": "v1 (unversioned)"},
                     "g4_window_receipts": g4_receipts,
                     "observer_cgroup_report_only": g4_observer_cgroup_rollup(g4_receipts)}
    v, rows, why = (score_g4(g4_windows, table, addendum, amendment, amendment2) if table else
                    ("NOT_MEASURED", [], ["no frozen G4 baseline table"]))
    why = why + amendment_notes + amendment2_notes
    amendment_extra = {"g4_amendment": amendment_src} if amendment_src else {}
    if amendment2_src:
        amendment_extra["g4_amendment_2"] = amendment2_src
    if books_probes:
        amendment_extra["okx_books_probe_per_window_report_only"] = books_probes
    # Missing temporal coverage (a window whose observation ended before its planned end: a v1-v3 record cap or a
    # v4 partial aggregate) can FAIL but never PASS.
    truncated = [r["index"] for r in g4_receipts if (r.get("coverage") or {}).get("truncated_before_end")]
    if truncated and v == "PASS":
        v, why = "NOT_MEASURED", why + [f"G4 windows {truncated} observed only part of their planned window"]
    elif truncated:
        why = why + [f"G4 windows {truncated} observed only part of their planned window"]
    rule = "candidate p95 <= frozen baseline p95 x 1.2 per row; missing coverage blocks the gate"
    if amendment is not None:
        rule += (f"; amendment {amendment.get('amendment_id')} row {G4_AMENDMENT_ROW}: same-event internal split "
                 f"(CreateTime - newest component receipt) p95 <= {amendment['limits']['candidate_p95_limit_ms']} ms "
                 f"and max <= {amendment['limits']['candidate_max_limit_ms']} ms on windows with >= "
                 f"{G4_AMENDMENT_MIN_SAMPLES} samples (recomputed baseline x {G4_AMENDMENT_FACTOR}); composite, "
                 "component gap and venue probe report-only; a window without probe coverage is UNKNOWN, not EXTERNAL")
    if amendment2 is not None:
        rule += (f"; amendment {amendment2.get('amendment_id')}: bar rows with interval >= 8h and no frozen baseline "
                 "report-only (owner B1), no threshold changes")
    if addendum is None and not addendum_notes:
        add("G4", "delivery lag, venue -> canonical (read_committed visibility, upper bound)", rule, v,
            "; ".join(why) or "every row inside", measured=rows, inverse_rows=[w["inverse_rows"] for w in g4_windows],
            **g4_provenance, **amendment_extra)
    else:
        if addendum is not None:
            rule += (f"; addendum rows: candidate max <= baseline max x {G4_ADDENDUM_FACTOR} on windows with >= "
                     f"{G4_ADDENDUM_MIN_SAMPLES} samples, {'/'.join(G4_REPORT_ONLY_INTERVALS)} and thin-baseline "
                     "rows report-only")
        add("G4", "delivery lag, venue -> canonical (read_committed visibility, upper bound)", rule, v,
            "; ".join(why + addendum_notes) or "every row inside",
            measured=[r for r in rows if r["verdict"] != "REPORT_ONLY"],
            report_only_rows=[r for r in rows if r["verdict"] == "REPORT_ONLY"],
            inverse_rows=[w["inverse_rows"] for w in g4_windows], g4_addendum=addendum_src, **g4_provenance,
            **amendment_extra)
    v, latency, failed_latency, g5_shape = score_g5(gates)
    add("G5", "request latency", "certified stage-50 p95/p99 per class and venue; frozen cold-shape limits", v,
        f"failed: {failed_latency}" if failed_latency else f"{len(latency)} latency/cold gates" if latency else
        "no driver gates", measured=latency + g5_shape,
        **({"g5_shape_rows_note": "g5:* rows are displayed here; their verdict reaches G5 through the protocol "
                                  "(driver acceptance), unchanged"} if g5_shape else {}))
    g6_ts = load_json(d / "g6-ts" / "summary.json")
    v, rows, why = score_g6({**((receipt or {}).get("g6_view_quality") or {}), **g6_ts_probe_rows(g6_ts)})
    probe_gaps = g6_ts_probe_coverage(g6_ts, load_s)
    if probe_gaps and v == "PASS":
        v = "NOT_MEASURED"
    why = why + probe_gaps if v != "FAIL" else why
    add("G6", "durable-view event age and typed execution quality at the Query endpoint", "per-binding D3 policy of the "
        "TS EXECUTION consumer (TRADE 3000, QUOTE/MARK/BOOK_DELTA 2000, BOOK_SNAPSHOT 60000, BAR1m 180000 ms, min with "
        "the requirement); zero unsafe eligibility; decided by D3_EXECUTION rows only", v,
        "; ".join(why) or "every required D3 binding measured, 0 unsafe", measured=rows,
        sources={"g6_ts": sha256_file(d / "g6-ts" / "summary.json")},
        ts_probe={k: (g6_ts or {}).get(k) for k in ("window_unix_s", "reads", "errors", "targets",
                                                     "per_binding_period_s", "stopped_by")} if g6_ts else None)
    v, rows, why = score_g7(scored)
    if mixed_gaps and v != "FAIL":
        v, why = "NOT_MEASURED", why + mixed_gaps
    add("G7", "TS consumer path", "SDK -> writer p95 <= 100 ms; writer -> Redis reply p95 <= 50 ms per feed", v,
        with_scope(why, "every feed inside", scope_note), measured=rows, **native_src)
    samples8 = list(read_jsonl(d / "g8" / "g8-samples.jsonl")) if (d / "g8" / "g8-samples.jsonl").exists() else []
    s8 = g8.summarize(samples8, steady_from_ns=load_ns[0] if load_ns else None,
                      steady_to_ns=load_ns[1] if load_ns else None) if samples8 else None
    v, why = g8.verdict(s8) if s8 else ("NOT_MEASURED", ["no G8 samples"])
    add("G8", f"TS execution Redis ({campaign.get('market_redis', {}).get('container', '?')}) command CPU, SLOWLOG, RTT",
        "commandstats <= 60 % of one core per 10-min window; SLOWLOG (10 ms) 0 in steady state; PING p99 <= 5 ms", v,
        "; ".join(why) or "inside", measured=s8)
    path910 = d / "g9g10" / "g9-g10-samples.jsonl"
    samples910 = list(read_jsonl(path910)) if path910.exists() else []
    s9 = g910.summarize_g9(samples910, from_ns=load_ns[0] if load_ns else None,
                           to_ns=load_ns[1] if load_ns else None) if samples910 else None
    v, why = g910.verdict_g9(s9, expected_demanded=campaign.get("demanded_slices")) if s9 else (
        "NOT_MEASURED", ["no heartbeat samples"])
    add("G9", "readiness: demanded, session-ready, intent-eligible kept separate", "demanded == session-ready; 0 "
        "data-path eligibility losses (legitimate quiet TRADE refusals counted, excluded); v1 fallback 0", v,
        "; ".join(why) or "inside", measured=s9)
    s10 = g910.summarize_g10(samples910, from_s=utc_stamp(load_s[0]) if load_s else None,
                             to_s=utc_stamp(load_s[1]) if load_s else None) if samples910 else {}
    trade_native = "TRADE" in native
    typed = g910.summarize_g10_native(samples910, from_ns=load_ns[0] if load_ns else None,
                                      to_ns=load_ns[1] if load_ns else None) if samples910 else None
    v, counters, why = score_g10(s10, (transit or {}).get("duplicate_applied_event_id") if transit else None,
                                 _trace_coverage(transit, load_ns, until_ns, raw_kinds=()) if trade_native
                                 else trace_gaps,
                                 sample_scope=native_raw_sample_scope(transit, load_ns) if trade_native else None,
                                 native_g10=((transit or {}).get("native_summary") or {}).get("g10") if trade_native
                                 else None, native_gaps=native_coverage(transit) if trade_native else None,
                                 expected_routes=expected_trade_routes(campaign) if trade_native else None,
                                 native_typed=typed)
    add("G10", "TRADE integrity", "0 gaps, 0 duplicates applied, 0 replay-expired, 0 overflow", v,
        "; ".join(why) or "all zero", measured=counters,
        basis="typed continuity signals: native TRADE from the heartbeat trade_integrity counters (pooled live worker "
              "epochs, deltas over one unbroken activation stretch bracketing the load), Python workers from their "
              "typed log lines (DL-signalled gaps, CURSOR_EXPIRED, SlowConsumerError); duplicates applied: native "
              "TRADE from the TS apply-decision counters per frozen route (offset high-water check, not an "
              "event-identity oracle), else the trace duplicate event_id; a silent offset gap the DL does not signal "
              "is not observable")
    g11 = load_json(d / "g11" / "g11-summary.json")
    # Re-judged with the current oracle (SR5): a v1 summary's PASS cannot stand (g11probe.amend_v1); FAIL stays.
    try:
        v, g11_why = g11probe.verdict(g11) if isinstance(g11, dict) else ("NOT_MEASURED", [])
    except (KeyError, TypeError, AttributeError) as exc:
        v, g11_why = "NOT_MEASURED", [f"G11 summary malformed ({exc.__class__.__name__}: {exc})"]
    out.append({"id": "G11", "quantity": "writer restart recovery", "rule": "fault cleared -> first valid apply per "
                "slice <= 30 s, -> caught up <= 60 s; no rewind; cursor never ahead", "verdict": v,
                "reason": "; ".join(g11_why) or ("restart probe" if g11 else "G11 not run"),
                "summary_schema": (g11 or {}).get("schema"), "recorded_verdict": (g11 or {}).get("verdict"),
                "measured": {k: (g11 or {}).get(k) for k in ("restart", "outage_seconds", "recovery", "cursor_samples")}
                if g11 else None,
                "note": "a fault window after the load (a restart of a monitored container aborts the driver); its "
                        "verdict does not depend on the steady-state protocol"})
    out.append({"id": "G12", "quantity": "Kafka broker / projector / Stream restart recovery", "verdict": "NOT_MEASURED",
                "reason": "not part of this campaign (isolated G12 tooling, scripts/p184_g12_isolated_fault.py)"})
    out.append({"id": "G13", "quantity": "DR01-DR08 acceptance", "verdict": "NOT_MEASURED",
                "reason": "not part of this campaign (G13 evidence is separate)"})
    expect = (host or {}).get("monitored_containers") or campaign.get("monitored_containers") or []
    table14 = g14.table(host, expect=expect) if host else None
    if table14 is None:
        v, why = "NOT_MEASURED", ["no driver host.json"]
    elif table14["mem_at_or_over_limit"]:
        v, why = "FAIL", [f"memory at/over limit: {table14['mem_at_or_over_limit']}"]
    elif table14["missing_roles"]:
        v, why = "NOT_MEASURED", [f"roles without samples: {table14['missing_roles']}"]
    else:
        v, why = "PASS", []
    add("G14", "resources per role at stage 100", "CPU/RAM per role recorded with headroom; budget set by the owner",
        v, "; ".join(why) or f"{len(table14['roles'])} roles recorded", measured=table14)
    summary = {}
    for gate in out:
        summary.setdefault(gate["verdict"], []).append(gate["id"])
    return {"schema": "qdl.p184.gate-campaign-scorecard.v1", "campaign_dir": str(d),
            "scoring_rule": "steady state >= 30 min at the stage-100 workload plus one burst window; a measurement "
                            "outside the protocol can FAIL a gate but cannot PASS it; NOT_MEASURED is never upgraded",
            "protocol_met": ok, "protocol_problems": problems, "load_window_unix_s": load_s,
            "runtime_identity": campaign.get("runtime_identity"), "summary": summary, "gates": out,
            "sources": {"receipt": sha256_file(d / "stage100" / "receipt.json"),
                        "gates": sha256_file(d / "stage100" / "gates.json"),
                        "host": sha256_file(d / "stage100" / "host.json")}}


# ---------------------------------------------------------------- runtime (read-only inspection + children)

def docker_inspect(name: str) -> dict | None:
    result = subprocess.run(["docker", "inspect", name], capture_output=True, text=True, timeout=30)
    return json.loads(result.stdout)[0] if result.returncode == 0 else None


def writer_market_redis(writer: str) -> str | None:
    code = "from shared.config import settings; print(settings.TRADING_MARKET_REDIS_URL)"
    result = subprocess.run(["docker", "exec", writer, "python", "-B", "-c", code], capture_output=True, text=True,
                            timeout=60)
    return result.stdout.strip() or None if result.returncode == 0 else None


def preflight(args) -> dict:
    plan = {"writer": args.writer, "problems": [], "warnings": []}
    base = load_json(Path(args.base_profile))
    if not isinstance(base, dict):
        plan["problems"].append(f"base profile unreadable: {args.base_profile}")
    else:
        queries = list(base.get("query_containers") or [QUERY_REFERENCE])
        plan["manifests"], problems = manifest_revision_check(base, {name: docker_inspect(name) for name in queries})
        plan["problems"].extend(problems)
    inspect = docker_inspect(args.writer)
    if inspect is None or not inspect["State"]["Running"]:
        plan["problems"].append("writer not running")
        return plan
    env = env_of(inspect)
    plan["runtime_identity"] = {"writer_image": inspect["Image"], "writer_started": inspect["State"]["StartedAt"],
                                "writer_restarts": inspect["RestartCount"]}
    until = int(env.get("TS_DELIVERY_TRACE_UNTIL_UNIX_NS") or 0)
    now_ns = time.time_ns()
    needed = now_ns + int((args.load_timeout + 60) * 1e9)
    plan["trace"] = {"dir_in_container": env.get("TS_DELIVERY_TRACE_DIR"),
                     "dir_on_host": host_path(inspect, env.get("TS_DELIVERY_TRACE_DIR", "")) if env.get(
                         "TS_DELIVERY_TRACE_DIR") else None,
                     "until_unix_ns": until, "max_files": env.get("TS_DELIVERY_TRACE_MAX_FILES"),
                     "max_records": env.get("TS_DELIVERY_TRACE_MAX_RECORDS"),
                     "covers_load": until >= needed,
                     "native_feeds": sorted(native_feeds_env(env)),
                     "native_max_files": env.get("TS_DELIVERY_TRACE_NATIVE_MAX_FILES"),
                     "writer_mode": "native" if native_feeds_env(env) else "python"}
    if not getattr(args, "baseline", False):
        problems, warnings = native_trace_checks(env, until, now_ns, inspect["State"].get("StartedAt", ""),
                                                 g11=args.g11, writer_running_clean=bool(
                                                     inspect["State"].get("Running") and inspect.get("RestartCount") == 0))
        plan["problems"].extend(problems)
        plan["warnings"].extend(warnings)
    if not plan["trace"]["covers_load"]:
        # A baseline run collects G4 windows only; the delivery trace is not read.
        (plan["warnings"] if args.allow_no_trace or getattr(args, "baseline", False) else plan["problems"]).append(
            "delivery trace does not cover the load window (recreate the writer with the long trace first); "
            "G1/G2/G7/G10 would be NOT_MEASURED")
    plan["binding"] = mount_source(inspect, "/run/secrets/qdl-v2-binding/trading-system.paper.stable.json")
    plan["writer_log_dir"] = host_path(inspect, "/app/logs")
    binding = load_json(plan["binding"]) if plan["binding"] else None
    plan["demanded_slices"] = len((binding or {}).get("products", [])) or None
    resolved = writer_market_redis(args.writer)
    if resolved is None:
        plan["warnings"].append("could not read TRADING_MARKET_REDIS_URL from the writer; using the TS default")
    host, _, db = parse_redis_url(resolved or "redis://redis_marketdata:6379/1")
    plan["market_redis"] = {"url_host": host, "db": db, "container": args.market_redis or host,
                            "resolved_from": "writer shared.config settings" if resolved else "TS default"}
    if args.market_redis and args.market_redis != host:
        plan["warnings"].append(f"--market-redis {args.market_redis} differs from the writer's {host}")
    heartbeat_host, _, heartbeat_db = parse_redis_url(env.get("TRADING_REDIS_URL", "redis://redis_service:6379/0"))
    plan["heartbeat_redis"] = {"container": heartbeat_host, "db": heartbeat_db}
    free = shutil.disk_usage("/").free / 2**30
    plan["disk_free_gb"] = round(free, 1)
    if free < MIN_DISK_GB + 3:
        plan["problems"].append(f"disk {free:.1f} GB free < {MIN_DISK_GB + 3} GB")
    for name in EXTRA_MONITORED:
        state = docker_inspect(name)
        if state is None or not state["State"]["Running"]:
            plan["problems"].append(f"{name} not running (monitored for G14)")
    for tool in ("guard_v2.sh", "mark_watch_v2.sh"):
        if not (Path(args.state_tools) / tool).exists():
            plan["problems"].append(f"missing {tool} in {args.state_tools}")
    return plan


def native_feeds_env(env: dict) -> set[str]:
    return {v.strip().upper() for v in env.get("MARKET_DATA_NATIVE_FEEDS", "").split(",") if v.strip()}


def docker_time_ns(value: str) -> int | None:
    if not value or value.startswith("0001-"):
        return None
    head, _, frac = value.rstrip("Z").partition(".")
    try:
        base = dt.datetime.strptime(head, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=dt.timezone.utc)
    except ValueError:
        return None
    return int(base.timestamp()) * 10**9 + int((frac + "000000000")[:9])


def native_trace_checks(env: dict, until_ns: int, now_ns: int, started_at: str, *, g11: bool,
                        writer_running_clean: bool = False) -> tuple[list, list]:
    """Preflight of a native-writer candidate (pure): native mode, the <= 3600 s window, the native file count."""
    problems, warnings = [], []
    feeds = native_feeds_env(env)
    if not feeds:
        problems.append("writer is not in native mode (MARKET_DATA_NATIVE_FEEDS unset): the final stage-100 scores the "
                        "production NATIVE writer")
        return problems, warnings
    if feeds - set(TRACE_FEEDS_G2):
        problems.append(f"unexpected native feeds {sorted(feeds - set(TRACE_FEEDS_G2))}")
    if until_ns and until_ns - now_ns > TRACE_MAX_WINDOW_SECONDS * 10**9:
        problems.append(f"trace expiry is {(until_ns - now_ns) / 1e9:.0f} s away > {TRACE_MAX_WINDOW_SECONDS} s "
                        "(the native writer refuses a longer window)")
    started = docker_time_ns(started_at)
    if until_ns and started and until_ns - started > (TRACE_MAX_WINDOW_SECONDS + 60) * 10**9:
        problems.append(f"trace window from the writer start is {(until_ns - started) / 1e9:.0f} s > "
                        f"{TRACE_MAX_WINDOW_SECONDS} s: the native trace was refused or the writer was not recreated "
                        "with this window")
    native_max = env.get("TS_DELIVERY_TRACE_NATIVE_MAX_FILES")
    shared_max = env.get("TS_DELIVERY_TRACE_MAX_FILES", "1")
    if native_max is not None:
        if native_max.strip() != "1":
            problems.append(f"TS_DELIVERY_TRACE_NATIVE_MAX_FILES={native_max}: the native writer exports exactly 1 file")
        elif shared_max.strip() != "1":
            warnings.append("TS_DELIVERY_TRACE_NATIVE_MAX_FILES=1 with shared TS_DELIVERY_TRACE_MAX_FILES="
                            f"{shared_max}: needs a writer image that reads the native-only key (B12 03824833 "
                            "refuses a shared MAX_FILES != 1 in native mode)")
    elif shared_max.strip() != "1":
        if writer_running_clean:
            # The native planner refuses an unsupported trace at writer start, so a native writer running with this
            # env and RestartCount 0 proves its image reads the native-only key (default 1, B13 0b3bb49e+).
            warnings.append(f"shared TS_DELIVERY_TRACE_MAX_FILES={shared_max} without the native-only key accepted: "
                            "the running native writer started with this env (native default 1 file)")
        else:
            problems.append(f"native mode without TS_DELIVERY_TRACE_NATIVE_MAX_FILES needs the shared "
                            f"TS_DELIVERY_TRACE_MAX_FILES == 1 (is {shared_max})")
    else:
        warnings.append("shared TS_DELIVERY_TRACE_MAX_FILES=1 also caps each Python worker (BOOK/MARK) at one "
                        "4096-record file: G1 and the MARK row of G2 will be NOT_MEASURED")
    warnings.append("native TRADE/QUOTE G2/G7 come from the native summary, written when the writer stops: the trace "
                    "must cover the load and the G11 restart must happen after the load"
                    + ("" if g11 else " (run with --g11, or restart the writer before `score --rerun-trace`)"))
    return problems, warnings


def trace_fit(until_ns: int, now_ns: int, *, launch_hold: float, load_timeout: int, g11: bool,
              g11_wait: int | None) -> tuple[int | None, list[str], dict]:
    """(g11 wait to use, problems, detail): the planned worst case from now (launch hold + driver timeout + sampler
    margin [+ settle/arm + G11 wait]) must end before the trace expiry. An automatic G11 wait (None) is the default
    shortened to fit; an explicit one that does not fit, or an automatic one below G11_MIN_WAIT, is a problem."""
    budget = (until_ns - now_ns) / 1e9 if until_ns else 0.0
    fixed = 5 + launch_hold + load_timeout + SAMPLER_MARGIN_SECONDS + (TRACE_TAIL_SECONDS if g11 else 0)
    detail = {"trace_budget_seconds": round(budget, 1), "planned_without_g11_wait_seconds": round(fixed, 1)}
    problems = []
    wait = g11_wait
    if g11:
        room = int(budget - fixed)
        if g11_wait is None:
            wait = max(0, min(G11_DEFAULT_WAIT, room))
            if wait < G11_MIN_WAIT:
                problems.append(f"trace expiry leaves {room} s for the G11 wait (< {G11_MIN_WAIT} s): recreate the "
                                "writer with a later TS_DELIVERY_TRACE_UNTIL_UNIX_NS (<= 3600 s) closer to the run")
            detail["g11_wait_auto"] = wait
        elif g11_wait > room:
            problems.append(f"--g11-wait {g11_wait} s does not fit the trace window (room {room} s); lower it or "
                            "omit it for the automatic fit")
        detail["planned_seconds"] = round(fixed + max(wait or 0, 0), 1)
    else:
        detail["planned_seconds"] = round(fixed, 1)
    if budget < fixed:
        problems.append(f"planned run {fixed:.0f} s exceeds the trace window ({budget:.0f} s left)")
    return wait, problems, detail


class Children:
    """Every child is tracked by its Popen; cleanup signals exact PIDs only."""

    def __init__(self):
        self.items: list[tuple[str, subprocess.Popen]] = []
        self.lock = threading.Lock()

    def start(self, name, command, *, log: Path, cwd=None, stdin_path: Path | None = None,
              stdout_path: Path | None = None):
        handle = log.open("x")
        source = Path(stdin_path).open("rb") if stdin_path else subprocess.DEVNULL
        sink = Path(stdout_path).open("x") if stdout_path else handle
        process = subprocess.Popen(command, cwd=cwd, stdout=sink, stderr=handle if stdout_path else subprocess.STDOUT,
                                   stdin=source)
        with self.lock:
            self.items.append((name, process))
        return process

    def stop(self, process, *, grace=90):
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=grace)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)

    def stop_all(self):
        with self.lock:
            items = list(self.items)
        for _, process in items:
            self.stop(process, grace=30)


def log_event(d: Path, kind: str, **fields):
    with (d / "timeline.jsonl").open("a") as handle:
        handle.write(json.dumps({"kind": kind, "at_ns": time.time_ns(), **fields}, sort_keys=True) + "\n")


def g11_process_end(d: Path) -> dict | None:
    """Pinned end of the writer run that held the load, from the G11 probe's own observations (S100-5): Docker
    StartedAt of that run (the `armed` line), its FinishedAt (read after the restart) and the last poll that still
    saw it. None when the probe saw no restart. Bounds the native activation end when the summary has no
    observed_end_unix_ns; never a configured expiry."""
    path = d / "g11" / "g11-observations.jsonl"
    if not path.exists():
        return None
    armed = restart = None
    for item in read_jsonl(path):
        if item.get("kind") == "armed" and armed is None:
            armed = item
        elif item.get("kind") == "restart":
            restart = item
    finished = docker_time_ns(((restart or {}).get("new") or {}).get("finished"))
    if finished is None:
        return None
    seen = (restart or {}).get("last_old_seen_ms")
    return {"source": "g11 Docker StartedAt/FinishedAt of the old writer run + last poll that saw it",
            "started_ns": docker_time_ns(((armed or {}).get("writer") or {}).get("started")),
            "finished_ns": finished, "last_seen_ns": int(seen) * 1_000_000 if seen else None}


def run_trace_analysis(campaign: dict, d: Path, *, image: str, out: Path | None = None) -> None:
    """Trace files are root 0600 inside the writer's log mount: read them in a disposable, network-less container of
    an image already on the host, with read-only mounts; output is JSON on stdout, written to `out`
    (default <campaign>/trace)."""
    trace = campaign.get("trace") or {}
    load_s = (load_json(d / "stage100" / "receipt.json") or {}).get("load_window_unix_s")
    out = Path(out) if out is not None else d / "trace"
    out.mkdir(exist_ok=True)
    if not trace.get("dir_on_host") or not load_s:
        (out / "transit-load.json").write_text(json.dumps({"error": "no trace dir or load window"}) + "\n")
        return
    pattern = f"/trace/delivery-*-{trace['until_unix_ns']}*.json"
    native = ["--native-summary", f"/trace/summary-native-*-{trace['until_unix_ns']}.json"] if trace.get(
        "native_feeds") else []
    end = g11_process_end(d) if native else None
    if end:
        native += ["--process-end", ":".join("" if end[k] is None else str(end[k])
                                             for k in ("started_ns", "finished_ns", "last_seen_ns"))]
    window = ["--from-ns", str(int(load_s[0] * 1e9)), "--to-ns", str(int(load_s[1] * 1e9))]
    base = ["docker", "run", "--rm", "--network", "none", "--cpus", "1", "--memory", "2500m", "--user", "0:0",
            "--read-only", "--security-opt", "no-new-privileges", "-e", "PYTHONDONTWRITEBYTECODE=1",
            "-v", f"{ROOT}:/w:ro", "-v", f"{trace['dir_on_host']}:/trace:ro", "-w", "/w", "--entrypoint", "python",
            image, "-B"]
    for name, script in (("transit-load.json", "scripts/p184_trace_feed_transit.py"),
                         ("mark-load.json", "scripts/p184_g2_mark_trace_transit.py")):
        extra = native if script.endswith("p184_trace_feed_transit.py") else []
        result = subprocess.run([*base, script, pattern, *window, *extra], capture_output=True, text=True, timeout=1800)
        try:
            value = json.loads(result.stdout)
        except ValueError:
            value = {"error": f"analysis exit {result.returncode}", "stderr_tail": result.stderr[-400:]}
        value.pop("per_file", None)
        (out / name).write_text(json.dumps(value, indent=1, sort_keys=True) + "\n")


def stop_g6_ts_probe(writer: str, process, out: Path) -> None:
    """docker exec does not forward signals: SIGTERM the probe by the exact in-container PID it printed first (a
    signal to our own read-only process, nothing else in the writer), wait for its final line, then write the last
    complete line as summary.json. If the probe already exited this only writes the summary."""
    lines = out / "probe.jsonl"
    if process.poll() is None:
        start = None
        try:
            start = json.loads(lines.read_text().splitlines()[0])
        except (OSError, ValueError, IndexError):
            pass
        pid = (start or {}).get("pid")
        if isinstance(pid, int) and pid > 1:
            subprocess.run(["docker", "exec", writer, "python", "-B", "-c",
                            f"import os, signal; os.kill({pid}, signal.SIGTERM)"], capture_output=True, timeout=30)
        try:
            process.wait(timeout=60)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
    summary = read_probe_lines(lines)
    if summary is not None:
        (out / "summary.json").write_text(json.dumps(summary, indent=1, sort_keys=True) + "\n")


def driver_output(args, d: Path) -> Path:
    return d / f"stage{getattr(args, 'sessions', 100)}"


def stop_okx_probe(d: Path, window: dict, probe, kind: str = "okx_probe") -> None:
    """Wait for one window's venue probe (self-bounded), else SIGTERM; then remove its container by name."""
    from scripts import p184_g4_okx_mark_index_probe as okxprobe
    try:
        probe.wait(timeout=G4_OKX_PROBE_LEAD_SECONDS + G4_OKX_PROBE_TAIL_SECONDS + 60)
    except subprocess.TimeoutExpired:
        probe.terminate()
        try:
            probe.wait(timeout=20)
        except subprocess.TimeoutExpired:
            probe.kill()
            probe.wait(timeout=10)
    if not okxprobe.remove_container(window[f"{kind}_container"]):
        log_event(d, f"g4_{kind}_container_not_removed", index=window["index"], name=window[f"{kind}_container"])
    log_event(d, f"g4_{kind}_end", index=window["index"], code=probe.returncode)


def g4_observer_container(d: Path, index: int) -> str:
    from scripts import p184_g4_observer as observer
    return observer.container_name(d.name, d / f"g4-{index}")


def command_plan(args, plan: dict, d: Path, launch_unix: float) -> dict:
    tools = Path(args.state_tools)
    py = [sys.executable, "-B"]
    sampler_seconds = str(args.load_timeout + 60)
    baseline = getattr(args, "baseline", False)
    # G4 amendment 1: one venue-direct OKX probe per G4 window of a candidate campaign (report-only attribution).
    okx_probe = not baseline and getattr(args, "okx_probe", G4_AMENDMENT_PATH.exists())
    commands = {
        "driver": ["timeout", str(args.load_timeout), *py, "scripts/phase3_consumer_load_acceptance.py",
                   "--profile", str(d / "profile.json"), "--budget", str(Path(args.budget).resolve()),
                   "--output", str(driver_output(args, d)), "--mode", "target",
                   "--sessions", str(getattr(args, "sessions", 100)), "--duration-seconds", str(LOAD_SECONDS)],
        "guard": [str(tools / "guard_v2.sh"), str(args.load_timeout), "<driver-pid>"],
        "mark_watch": [str(tools / "mark_watch_v2.sh"), str(args.load_timeout), "<driver-pid>"],
        "g3": [*py, "scripts/p184_g3_bar_cache_probe.py", "--binding", str(plan.get("binding")),
               "--container", plan["market_redis"]["container"], "--db", str(plan["market_redis"]["db"]),
               "--seconds", sampler_seconds, "--out", str(d / "g3")],
        "g8": [*py, "scripts/p184_g8_redis_sampler.py", "--container", plan["market_redis"]["container"],
               "--seconds", sampler_seconds, "--out", str(d / "g8")],
        "g6_ts": ["docker", "exec", "-i", args.writer, "python", "-B", "-", "--duration", sampler_seconds,
                  "--emit-seconds", str(G6_TS_PROBE_EMIT_SECONDS)],
        "g6_ts_stdin": str(ROOT / "scripts/p184_g6_ts_execution_probe.py"),
        "g9g10": [*py, "scripts/p184_g9_g10_writer_counters.py", "--container", plan["heartbeat_redis"]["container"],
                  "--db", str(plan["heartbeat_redis"]["db"]), "--log-dir", str(plan.get("writer_log_dir")),
                  "--seconds", sampler_seconds, "--out", str(d / "g9g10")],
        "g4_windows": [{"index": i + 1, **w,
                        "command": [*py, "scripts/p184_g4_observer.py", "--launch", "--mode", "canonical",
                                    "--seconds", str(int(w["seconds"])), "--tail-records", "0", "--max-records",
                                    str(G4_RECORD_CAP), "--sample-ms", "1000", "--max-output-bytes", "16777216",
                                    "--output", str(d / f"g4-{i + 1}"), "--campaign-id", d.name],
                        "container_name": g4_observer_container(d, i + 1),
                        **({"okx_probe": [*py, "scripts/p184_g4_okx_mark_index_probe.py", "--launch", "--seconds",
                                          str(int(w["seconds"]) + G4_OKX_PROBE_LEAD_SECONDS + G4_OKX_PROBE_TAIL_SECONDS),
                                          "--output", str(d / f"g4-{i + 1}-okx-probe"),
                                          "--name", g4_observer_container(d, i + 1) + "-okxprobe"],
                            "okx_probe_container": g4_observer_container(d, i + 1) + "-okxprobe",
                            # Report-only (owner 2026-10-18): fresh-connection OKX books probe + ingestor netns TCP.
                            "okx_books_probe": [*py, "scripts/p184_g4_okx_books_probe.py", "--launch", "--seconds",
                                                str(int(w["seconds"]) + G4_OKX_PROBE_LEAD_SECONDS
                                                    + G4_OKX_PROBE_TAIL_SECONDS),
                                                "--output", str(d / f"g4-{i + 1}-okx-books"),
                                                "--name", g4_observer_container(d, i + 1) + "-okxbooks"],
                            "okx_books_probe_container": g4_observer_container(d, i + 1) + "-okxbooks"}
                           if okx_probe else {})}
                       for i, w in enumerate(g4_schedule(launch_unix))],
        "g11": ([*py, "scripts/p184_g11_writer_restart_probe.py", "--binding", str(plan.get("binding")),
                 "--writer", args.writer, "--redis-container", plan["market_redis"]["container"],
                 "--db", str(plan["market_redis"]["db"]), "--wait-seconds", str(args.g11_wait),
                 "--observe-seconds", "180", "--out", str(d / "g11")] if args.g11 else None),
    }
    if baseline:
        # Baseline (owner 2026-10-11): the same driver workload and G4 observer windows, nothing else.
        commands.update({name: None for name in BASELINE_SKIPPED})
    return commands


BASELINE_SKIPPED = ("g3", "g8", "g6_ts", "g6_ts_stdin", "g9g10", "g11")


def baseline_receipt(d: Path, commands: dict, driver_code: int | None) -> dict:
    """baseline.json: the completion marker the addendum freeze requires; lists each G4 window's files."""
    windows = []
    for window in commands["g4_windows"]:
        summary = d / f"g4-{window['index']}-summary.json"
        obs = d / f"g4-{window['index']}" / "observations.jsonl"
        windows.append({"index": window["index"], "boundary": window["boundary"],
                        "summary_sha256": sha256_file(summary), "observations_sha256": sha256_file(obs)})
    return {"schema": "qdl.p184.g4-baseline-run.v1", "role": "baseline", "completed_unix": time.time(),
            "driver_exit": driver_code, "g4_windows": windows}


def run(args) -> int:
    baseline = getattr(args, "baseline", False)
    if baseline and args.g11:
        raise SystemExit("--baseline collects G4 windows only; --g11 is a candidate fault window")
    if not baseline and args.sessions != 100:
        raise SystemExit("a candidate campaign is the stage-100 workload (--sessions 100)")
    plan = preflight(args)
    # Schedule v2: hold the launch (<= 220 s) until a UTC :00/:30 close fits inside the load with its boundary window.
    earliest = time.time() + 5
    launch_unix = g4_launch_unix(earliest)
    launch_hold = round(launch_unix - earliest, 3)
    if not baseline and not args.allow_no_trace and plan.get("trace"):
        args.g11_wait, fit_problems, plan["trace_fit"] = trace_fit(
            int(plan["trace"].get("until_unix_ns") or 0), time.time_ns(), launch_hold=launch_hold,
            load_timeout=args.load_timeout, g11=args.g11, g11_wait=args.g11_wait)
        plan["problems"].extend(fit_problems)
    if args.g11_wait is None:
        args.g11_wait = G11_DEFAULT_WAIT
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    prefix = "p184-g4-baseline" if baseline else "p184-final-campaign"
    d = Path(args.campaign_dir or STATE / f"{prefix}-{stamp}").resolve()
    commands = command_plan(args, plan, d, launch_unix)
    expected = (args.load_timeout + 60 + (args.g11_wait + 180 if args.g11 else 0) + 600)
    if args.dry_run:
        order = (["preflight (read-only)", "driver + guard + MARK watch", "G4 windows on schedule",
                  "baseline.json (no samplers, no G6 probe, no trace analysis, no scorecard)"] if baseline else
                 ["preflight (read-only)", "samplers G3/G8/G9-G10 + G6 TS execution probe (docker "
                  "exec -i into the writer, read-only) start", "driver + guard + MARK watch",
                  "G4 windows on schedule", "driver exit -> samplers SIGTERM, G6 probe SIGTERM by its "
                  "in-container PID -> g6-ts/summary.json",
                  "G11 probe arms; COORDINATOR restarts the writer" if args.g11 else "G11 skipped",
                  "trace analysis (--rm, --network none)", "scorecard.json"])
        print(json.dumps({"campaign_dir": str(d), "role": "baseline" if baseline else "candidate", "preflight": plan,
                          "commands": commands, "expected_wall_seconds_max": expected + launch_hold,
                          "g4_apparatus": g4_apparatus(launch_hold), "order": order},
                         indent=1, sort_keys=True, default=str))
        return 0 if not plan["problems"] else 2
    if plan["problems"]:
        print(json.dumps({"refused": plan["problems"]}), file=sys.stderr)
        return 2
    d.mkdir(parents=True, exist_ok=False)
    base = load_json(Path(args.base_profile))
    profile = derive_profile(base)
    (d / "profile.json").write_text(json.dumps(profile, indent=1, sort_keys=True) + "\n")
    os.chmod(d / "profile.json", 0o600)
    campaign = {"schema": "qdl.p184.gate-campaign.v1", "role": "baseline" if baseline else "candidate",
                "sessions": args.sessions, "started_unix": time.time(), "preflight": plan,
                "runtime_identity": plan.get("runtime_identity"), "trace": plan.get("trace"),
                "market_redis": plan.get("market_redis"), "demanded_slices": plan.get("demanded_slices"),
                "budget_path": str(Path(args.budget).resolve()), "budget_sha256": sha256_file(Path(args.budget)),
                "monitored_containers": profile["monitored_containers"],
                "g4_windows": [{k: v for k, v in w.items() if k != "command"} for w in commands["g4_windows"]],
                "g4_apparatus": g4_apparatus(launch_hold), "launch_unix": launch_unix,
                "tool_sha256": sha256_file(Path(__file__)), "source_sha": subprocess.run(
                    ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()}
    if not baseline and G4_ADDENDUM_PATH.exists():
        campaign.update(g4_addendum_path=str(G4_ADDENDUM_PATH), g4_addendum_sha256=sha256_file(G4_ADDENDUM_PATH))
    if not baseline and G4_AMENDMENT_PATH.exists():
        campaign.update(g4_amendment_path=str(G4_AMENDMENT_PATH), g4_amendment_sha256=sha256_file(G4_AMENDMENT_PATH))
    if not baseline and G4_AMENDMENT_2_PATH.exists():
        campaign.update(g4_amendment_2_path=str(G4_AMENDMENT_2_PATH),
                        g4_amendment_2_sha256=sha256_file(G4_AMENDMENT_2_PATH))
    (d / "campaign.json").write_text(json.dumps(campaign, indent=1, sort_keys=True, default=str) + "\n")
    children = Children()
    hard_deadline = time.monotonic() + expected + launch_hold + 600
    watchdog_fired = threading.Event()

    def watchdog():
        while not watchdog_fired.wait(15):
            if time.monotonic() > hard_deadline:
                log_event(d, "DEADLINE")
                children.stop_all()
                return
    threading.Thread(target=watchdog, daemon=True).start()
    code = None
    try:
        samplers, g6_probe = {}, None
        if launch_hold > 0:
            log_event(d, "g4_launch_hold", seconds=launch_hold, launch_unix=launch_unix)
            time.sleep(max(0.0, launch_unix - 5 - time.time()))  # samplers start 5 s before launch, as without a hold
        if not baseline:
            samplers = {name: children.start(name, commands[name], log=d / f"{name}.log", cwd=ROOT)
                        for name in ("g3", "g8", "g9g10")}
            (d / "g6-ts").mkdir()
            g6_probe = children.start("g6_ts", commands["g6_ts"], log=d / "g6-ts" / "probe.log", cwd=ROOT,
                                      stdin_path=Path(commands["g6_ts_stdin"]),
                                      stdout_path=d / "g6-ts" / "probe.jsonl")
            log_event(d, "g6_ts_started", pid=g6_probe.pid)
        time.sleep(max(0.0, launch_unix - time.time()))
        driver = children.start("driver", commands["driver"], log=d / "stage100.log", cwd=ROOT)
        log_event(d, "driver_started", pid=driver.pid)
        (d / "driver.pid").write_text(f"{driver.pid}\n")
        for name in ("guard", "mark_watch"):
            command = [str(driver.pid) if part == "<driver-pid>" else part for part in commands[name]]
            children.start(name, command, log=d / f"{name}.log")

        probe_stoppers = []

        def stop_probe_async(window, probe, kind="okx_probe"):
            # Waiting for the venue probe never delays the next window (windows may be only 15 s apart).
            stopper = threading.Thread(target=stop_okx_probe, args=(d, window, probe, kind), daemon=True)
            stopper.start()
            probe_stoppers.append(stopper)

        def g4_runner():
            for window in commands["g4_windows"]:
                probe = books_probe = None
                if window.get("okx_probe"):
                    delay = launch_unix + window["offset"] - G4_OKX_PROBE_LEAD_SECONDS - time.time()
                    if delay > 0 and driver.poll() is None:
                        time.sleep(delay)
                    if driver.poll() is None:
                        with (d / f"g4-{window['index']}-okx-probe.log").open("x") as probe_log:
                            probe = subprocess.Popen(window["okx_probe"], cwd=ROOT, stdout=subprocess.DEVNULL,
                                                     stderr=probe_log)
                        log_event(d, "g4_okx_probe_start", index=window["index"], pid=probe.pid)
                        if window.get("okx_books_probe"):
                            with (d / f"g4-{window['index']}-okx-books.log").open("x") as books_log:
                                books_probe = subprocess.Popen(window["okx_books_probe"], cwd=ROOT,
                                                               stdout=subprocess.DEVNULL, stderr=books_log)
                            log_event(d, "g4_okx_books_probe_start", index=window["index"], pid=books_probe.pid)
                delay = launch_unix + window["offset"] - time.time()
                if delay > 0 and driver.poll() is None:
                    time.sleep(delay)
                if driver.poll() is not None:
                    log_event(d, "g4_skipped", index=window["index"])
                    if probe is not None:
                        stop_probe_async(window, probe)
                    if books_probe is not None:
                        stop_probe_async(window, books_probe, "okx_books_probe")
                    continue
                log_event(d, "g4_start", index=window["index"])
                with (d / f"g4-{window['index']}.log").open("x") as handle:
                    try:
                        subprocess.run(window["command"], cwd=ROOT, stdout=handle, stderr=subprocess.STDOUT,
                                       timeout=window["seconds"] + 180)
                    except subprocess.TimeoutExpired:
                        log_event(d, "g4_timeout", index=window["index"])
                    finally:
                        # The launcher removes its container on its own exit paths; a SIGKILLed launcher cannot.
                        from scripts import p184_g4_observer as observer
                        if not observer.remove_container(window["container_name"]):
                            log_event(d, "g4_container_not_removed", index=window["index"],
                                      name=window["container_name"])
                        if probe is not None:
                            stop_probe_async(window, probe)
                        if books_probe is not None:
                            stop_probe_async(window, books_probe, "okx_books_probe")
                obs = d / f"g4-{window['index']}" / "observations.jsonl"
                if obs.exists():
                    cap = g4_window_receipt(read_jsonl(obs))
                    if cap["record_cap_reached"]:
                        log_event(d, "g4_record_cap_reached", index=window["index"], records=cap["records"],
                                  after_seconds=cap["cap_reached_after_seconds"])
                    if (cap.get("coverage") or {}).get("truncated_before_end"):
                        log_event(d, "g4_window_truncated", index=window["index"],
                                  unobserved_tail_seconds=cap["coverage"].get("unobserved_tail_seconds"))
                    summary = subprocess.run([sys.executable, "-B", "scripts/p184_g4_observer.py", "--summarize",
                                              str(obs)], cwd=ROOT, capture_output=True, text=True, timeout=300)
                    (d / f"g4-{window['index']}-summary.json").write_text(summary.stdout)
                log_event(d, "g4_end", index=window["index"])
            for stopper in probe_stoppers:
                stopper.join(timeout=G4_OKX_PROBE_LEAD_SECONDS + G4_OKX_PROBE_TAIL_SECONDS + 120)
        g4_thread = threading.Thread(target=g4_runner, daemon=True)
        g4_thread.start()
        code = driver.wait()
        log_event(d, "driver_exit", code=code)
        g4_thread.join(timeout=420)
        time.sleep(30)
        for process in samplers.values():
            children.stop(process)
        if g6_probe is not None:
            stop_g6_ts_probe(args.writer, g6_probe, d / "g6-ts")
            log_event(d, "samplers_stopped")
        if commands["g11"]:
            probe = children.start("g11", commands["g11"], log=d / "g11.log", cwd=ROOT)
            time.sleep(15)
            message = (f"G11_ARMED: restart the writer now (coordinator action, e.g. `docker restart {args.writer}`); "
                       f"the probe waits up to {args.g11_wait} s")
            log_event(d, "g11_armed", pid=probe.pid)
            print(message, flush=True)
            (d / "G11_ARMED").write_text(message + "\n")
            probe.wait()
            log_event(d, "g11_exit", code=probe.returncode)
    finally:
        watchdog_fired.set()
        children.stop_all()
    if baseline:
        receipt = baseline_receipt(d, commands, code)
        with (d / "baseline.json").open("x") as handle:
            handle.write(json.dumps(receipt, indent=1, sort_keys=True) + "\n")
        print(json.dumps({"campaign_dir": str(d), "role": "baseline", "driver_exit": code,
                          "g4_windows_with_summary": sum(1 for w in receipt["g4_windows"] if w["summary_sha256"])}))
        return 0
    run_trace_analysis(campaign, d, image=args.analysis_image or profile["image"])
    card = build_scorecard(d)
    (d / "scorecard.json").write_text(json.dumps(card, indent=1, sort_keys=True, default=str) + "\n")
    print(json.dumps({"campaign_dir": str(d), "summary": card["summary"], "protocol_met": card["protocol_met"]}))
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run")
    r.add_argument("--dry-run", action="store_true")
    r.add_argument("--campaign-dir")
    r.add_argument("--base-profile", default=str(TOOLS / "profile-stage100-rc4.json"))
    r.add_argument("--budget", default=str(ROOT / "config/v2/p184-stage100-budget.json"))
    r.add_argument("--state-tools", default=str(TOOLS))
    r.add_argument("--writer", default=WRITER)
    r.add_argument("--market-redis", help="container of TRADING_MARKET_REDIS_URL (default: read from the writer)")
    r.add_argument("--analysis-image", help="image for the trace analysis (default: the profile image)")
    r.add_argument("--load-timeout", type=int, default=DRIVER_TIMEOUT)
    r.add_argument("--g11", action="store_true", help="arm the G11 probe after the load (coordinator restarts)")
    r.add_argument("--g11-wait", type=int, default=None,
                   help=f"G11 restart wait (default: {G11_DEFAULT_WAIT} s shortened so the run fits the trace window)")
    r.add_argument("--allow-no-trace", action="store_true")
    r.add_argument("--baseline", action="store_true",
                   help="G4 baseline run (role=baseline): driver + G4 windows only, no scoring; input of "
                        "scripts/p184_freeze_g4_addendum.py")
    r.add_argument("--sessions", type=int, choices=(50, 100), default=100,
                   help="driver stage (a candidate campaign is always 100; a baseline may mirror the stage-50 one)")
    s = sub.add_parser("score")
    s.add_argument("--campaign-dir", required=True)
    s.add_argument("--analysis-image")
    s.add_argument("--rerun-trace", action="store_true")
    s.add_argument("--out-dir", help="new directory (created exclusively, outside the campaign) for the re-analysed "
                                     "trace and scorecard.json; the campaign directory is then never written")
    args = parser.parse_args(argv)
    if args.command == "run":
        return run(args)
    d = Path(args.campaign_dir)
    if (load_json(d / "campaign.json") or {}).get("role") == "baseline":
        raise SystemExit("a baseline campaign is not scored; freeze it with scripts/p184_freeze_g4_addendum.py")
    if args.out_dir:
        out = Path(args.out_dir).resolve()
        if out == d.resolve() or d.resolve() in out.parents:
            raise SystemExit("--out-dir must be outside the campaign directory")
        out.mkdir(parents=True, exist_ok=False)
        trace_dir = d / "trace"
        if args.rerun_trace or not (d / "trace" / "transit-load.json").exists():
            campaign = load_json(d / "campaign.json") or {}
            image = args.analysis_image or (load_json(d / "profile.json") or {}).get("image")
            run_trace_analysis(campaign, d, image=image, out=out / "trace")
            trace_dir = out / "trace"
        card = build_scorecard(d, trace_dir=trace_dir)
        card["rescored_into"] = str(out)
        path = out / "scorecard.json"
        with path.open("x") as handle:
            handle.write(json.dumps(card, indent=1, sort_keys=True, default=str) + "\n")
        print(json.dumps({"scorecard": str(path), "summary": card["summary"]}))
        return 0
    if args.rerun_trace or not (d / "trace" / "transit-load.json").exists():
        campaign = load_json(d / "campaign.json") or {}
        image = args.analysis_image or (load_json(d / "profile.json") or {}).get("image")
        run_trace_analysis(campaign, d, image=image)
    card = build_scorecard(d)
    path = d / f"scorecard-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.json"
    path.write_text(json.dumps(card, indent=1, sort_keys=True, default=str) + "\n")
    print(json.dumps({"scorecard": str(path), "summary": card["summary"]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
