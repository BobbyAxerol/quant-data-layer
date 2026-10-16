#!/usr/bin/env python3
"""P18.4 G12 / D4-R3: isolated Kafka broker / projector / Stream fault acceptance.

Purpose: run the G12 gate (TS upgrade/p184-phase-d-gates.md) once, in a disposable
namespace: three KRaft brokers (RF3, min ISR 2, mTLS), the production core,
projector, Stream and Query images, a test-provenance publisher (BOOK, and
TRADE / QUOTE / closed BAR 1m / MARK-INDEX on their own provider lanes), two TS
writer clones (the TS-owned ``WriterClone``, production entrypoint, 4 workers) and a
sampler that reads each clone's BOOK view through the TS at-use validator and receives
every TRADE / QUOTE / MARK / BAR the clone applies (its own ``events.market.v2.*``
notifications).
Faults: stop/start one broker, restart the projector, restart Stream. Per fault
it records the G11 four timestamps (injected, cleared, first valid apply,
caught up) and then, inside one fixed window, G1 (BOOK), G2 (TRADE, QUOTE and
MARK_INDEX_PRICE new-event transit) and G3 (BAR 1m from close). Caught up is predicate v2 (``caught_up``:
BOOK update transit plus update cadence against the core's materialization rule), its version and parameters
frozen in ``evidence/caught-up-predicate.json`` before any data. A fault passes only
when caught up in time and G1, every G2 feed and G3 pass; a part with too few samples or
pending events is NOT_MEASURED, never a pass. Each fault also carries a REPORT-ONLY
integrity part (``integrity_window``: BOOK / applied-notification rewinds, TRADE
duplicates applied, native typed counters; never gating: lossless TRADE is G10's gate).
A run without the frozen predicate v2 never PASSes (``g12_receipt_creditable``: verdict
v3 only). ``--run`` needs ``--auto-leader-rebalance true|false`` (the brokers'
auto.leader.rebalance.enable, set to production's value and recorded in the receipt) and
``--production-parity FILE`` (``--parity-snapshot``: production Kafka profile - rebalance, internal-topic decision C,
md.latest A' - plus the deployed writer reference, and the EFFECTIVE production configuration observed read-only:
``kafka-configs --describe`` of both internal topics and md.latest.v2 through the admin container + every broker's
rebalance env, hashed; the snapshot refuses when it differs from the committed plans or a temporary cleaner key is
present). The run mirrors that profile on its own brokers/topics (C applied before the baseline, A' at topic creation;
``production_parity`` MATCH or the run stops / is not credited) and re-observes production before and after the
faults: both hashes must equal the snapshot's, else the run stops (before) or is not credited (after; Codex K3) and prechecks that each WriterClone runs the deployed native writer (``writer_clone_precheck``:
clone env/image == the writer's run-time inspect == the snapshot) with production Redis CPU parity.

Feeds: the DR05 material is BOOK-only; the in-container ``--feeds`` step (network
none) seals TRADE, QUOTE, BAR 1m and MARK_INDEX_PRICE for the same two instruments
into the same material: core bindings from the production acquisition mapping
(``ProductionCatalogBuilder._acquisition``, checked equal to the production plan;
MARK/INDEX as the deployment's physical components), the source catalog's own
bindings, the TS paper manifest's requirement shape (event recency / session
liveness null like the DR05 BOOK requirements), the route binding and ts-consumer
via the DR05 ``consumer_packet`` and a recompiled bundle. The writer reads MARK
through Query reference reads woken by Stream: the Query role (``--query``) is the
DR05 Query of the code under test with the production KN execution MARK/INDEX
reader wired in (``qdl.runtime.stable`` kn3). Exits of every owned container
(``--rm`` removes it, so its own inspect is gone) are recorded from the daemon's
die/oom/kill events (``evidence/container-events.jsonl``, ``exit-states.json``);
the product roles' output is followed while they live (``evidence/logs/<role>.life<n>.log``,
bounded tail), so an unexpected exit keeps its stderr without a restart policy.

Reuse, not a new engine: material/frames/raw envelopes come from the DR05 tools
(``p184_dr05_dl_fault``, ``p184_dr05_raw_publisher``), the Query read view is the
DR05/DR08 ``serve_query``, cluster/ISR/committed-offset reads are the D4-R2
``kn_recovery_observe.KafkaAdminSource``, the TS clone is
``scripts/p184_dr05_isolated.WriterClone`` loaded read-only from the TS worktree.

Boundary: every container is ``--rm``, named ``<ns>-<role>`` and labelled
``p184.g12=<ns>``; all join one holder's ``--network none`` namespace (no Docker
network, no published port). The shared stack, shared Redis/Kafka/Postgres and
any container not created here are never touched; the deployed TS writer is only
``docker inspect``-ed (WriterClone's own contract). Production containers'
restart counts are watched as a stop condition. No secret is printed. Synthetic
data carries TEST_ONLY provenance; results are ISOLATED_FAULT rehearsal evidence,
not production or HA evidence.

Host modes (stdlib only): ``--run BASE`` (orchestrate, absolute deadline, teardown
in ``finally``), ``--down BASE`` (exact-name, label-checked teardown),
``--analyze BASE`` (recompute the verdict from the JSONL evidence).
Core-group modes (INCIDENT 2026-10-15): ``--core-group BASE`` (rolling replace of 3 members onto ``--new-core``),
``--core-group-simultaneous BASE`` (all members replaced at once, then a restart and a kill on the all-new group),
``--core-group-accept BASE`` (read-only acceptance of saved evidence); ``--core-*`` options set a production-like
core profile (``core_profile``), omitted they keep the B3 harness.

Image set: ``IMAGES`` below are DEFAULTS only. ``--run`` accepts ``--image-set FILE``
(a JSON object role -> sha256:<64hex>, any subset of the roles) and ``--image
ROLE=sha256:<64hex>`` (repeatable, applied last). The resolved set is checked against
the local image IDs, sealed in ``work/handoff.json`` ("g12_images", plus "images" in
the DR05 role names so the in-container ``--material`` step writes the core digest
from it), passed to every in-namespace helper as ``P184_G12_IMAGES`` (checked against
the seal by ``_packet``), and every container this run starts (including restarts and
the WriterClone containers) is ``docker inspect``-ed: its image ID must equal the
passed value for its role (``evidence/provenance.jsonl``). ``--query-code image``
runs Query from the image's own /app (no worktree mount); the default ``worktree``
keeps the earlier read-only worktree mount.
In-container modes (python image, inside the namespace): ``--topics``,
``--publish``, ``--monitor``; each bounded by ``--seconds`` / its own deadline;
``--feeds`` (network none, material only); ``--query`` (the Query role).
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

LABEL = "p184.g12"
NS_PATTERN = r"p184-g12-dl-[0-9a-f]{16}"
TS_ROOT = Path("/home/bobby/.worktrees/trading-system-next-upgrade")
TS_WRITER = "market_data_service_p184w"
IMAGES = {
    "kafka": "sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9",
    "core": "sha256:658a9570c5fc23f4906413aa2460f4d82e21772cc63c9ed900363d4d13d54023",
    "projector": "sha256:a696cfba6ab383173607f96413c814f74c7abcc98a4c41f57c9c6e78a41bb70b",
    "stream": "sha256:1f13408fe94698a45f18e59e61c6554e979308dcebfe21514a5d0534609a51e9",
    "python": "sha256:672503b7fe87265d12af3d45fc4d344fdef0a76e94e06efe52e8dc59cab24d0a",
    "redis": "sha256:dfa18828cbc07b3ae6a95ec7343f6c214fdee2d836197b4be8e9904420762cd8",
    "ts": "sha256:15524c0bea8c51949d7ced79291d4c72722292fea2f0a5d4f8e9c7fd61dbb5ca",
    "postgres": "sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685",
    "holder": "sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc",
}
BROKERS = (1, 2, 3)
CLUSTER_ID = "MkU3OEVBNTcwNTJENDM2Qk"
BOOTSTRAP = ",".join(f"localhost:{9092 + 100 * (n - 1)}" for n in BROKERS)
VENUES = ("BINANCE", "OKX")
CLONE_PORTS = {"BINANCE": (6391, 5441), "OKX": (6392, 5442)}
PROJECTOR_GROUPS = ("kn-projector-v3-a", "kn-projector-v3-b")
# Frozen before the run (packet sections 5-6).
GATE = {"caught_up_s": 600, "age_ms": 1000, "hold_s": 30, "window_s": 300,
        "p95_ms": 500, "p99_ms": 1000, "max_ms": 2000, "p99_min_n": 100}
LIMITS = {"disk_free_bytes": 20 * 10**9, "mem_available_bytes": 3 * 2**30,
          "deadline_s": 3600, "holder_s": 5400, "broker_outage_s": 60,
          "role_return_s": 120, "clone_ready_s": 300, "baseline_s": 300, "md_ready_s": 180,
          "monitor_sample_s": 5}
# BOOK publisher cadence (frames per second per venue); the orchestrator starts --publish with it.
PUBLISH_HZ = 4.0
# Synthetic top of book, a deterministic function of the frame sequence (``book_top``): in each CYCLE of frames the
# first CYCLE - QUIET frames move the top by one price step every frame (bid in [BASE, BASE + STEPS - 1], ask =
# bid + SPREAD, never crossed), the last QUIET frames hold it. Real books move their top most frames and sit still
# at times; the core materializes BOOK_SNAPSHOT on a top change (100 ms floor) and on the configured interval when
# quiet (rust/qdl-realtime-core/src/lib.rs due_l2_materialized_snapshot), so both rules run every cycle (10 s at
# 4 Hz: 32 frames moving, 2 s quiet). A fixed top (the DR05 frame default) left only the quiet rule.
BOOK_TOP = {"base": 60000, "steps": 8, "spread": 10, "cycle": 40, "quiet": 8}
# Caught-up predicate v2 (``caught_up``). Parameters come from the core, not from run data:
# - L2_TOP_CHANGE_MIN_INTERVAL_MS: rust/qdl-realtime-core/src/lib.rs const of the same name.
# - materialization interval: the run's core binding, as L2Binding::materialized_snapshot_interval_ms resolves it
#   (l2.materialized_snapshot_interval_ms, else snapshot_refresh_seconds * 1000); the default is
#   qdl.runtime.execution_l2.EXECUTION_L2_MATERIALIZATION_INTERVAL_MS (host modes are stdlib only: a unit test
#   keeps the two equal).
# - slack 1 ms: the core compares integer-ms provider time (l2_observed_at_ms: payload E / ts = stamp // 1e6), so
#   an elapsed of I ms in ns can read as I - 1 ms and the emission moves to the next frame.
CAUGHT_UP_VERSION = "p184.g12.caught_up.v2"
L2_TOP_CHANGE_MIN_INTERVAL_MS = 100
L2_DEFAULT_MATERIALIZATION_MS = 1000
CORE_MS_FLOOR_SLACK_MS = 1
PREDICATE_FILE = "caught-up-predicate.json"
# v3 (inheritance audit 2026-10-16, OF-G12-1/2): pending G2/G3 events block a PASS, per-fault integrity report (rewind,
# duplicate applied, native typed counters; REPORT-ONLY, never gating), PASS only under a frozen predicate v2.
VERDICT_SCHEMA = "p184.g12.verdict.v3"
# A fault whose clearance / catch-up interval has no valid monitor sample for longer than this is NOT_MEASURED:
# its fault-cleared time (the monitor's) would be uncertain by more than the gap (steady cadence: 1.0 s).
MONITOR_MAX_GAP_S = 5
# The BOOK sampler's coverage uses the same bound (``sampler_max_gap_s``, cleared -> caught up + hold): a longer
# hole without an observed view leaves the caught-up time (and the hold) uncertain by more than the monitor's own
# rule tolerates, so the fault is NOT_MEASURED. A healthy sampler logs a view row at least every 500 ms.
SAMPLER_MAX_GAP_S = MONITOR_MAX_GAP_S
# G2 / G3 (frozen TS gate rows; owner decision 2026-10-09). min_n: below it the part is NOT_MEASURED.
# G2 is new-event transit (publish -> first applied notification); G3 is BAR 1m from close.
GATE_G2_G3 = {"TRADE": {"p95_ms": 500, "max_ms": 1500, "min_n": 100},
              "QUOTE": {"p95_ms": 500, "max_ms": 1000, "min_n": 100},
              "MARK": {"p95_ms": 500, "max_ms": 1000, "min_n": 100},
              "BAR": {"p95_ms": 5000, "max_ms": 60000, "min_n": 3}}
G2_FEEDS = ("TRADE", "QUOTE", "MARK")  # MARK = MARK_INDEX_PRICE
# An event not applied this long after its reference is lost (TRADE/BAR: a FAIL). QUOTE (ON_CHANGE) and MARK (a
# latest-state reference read per Stream wake) may legitimately skip a superseded value: only counted as unseen.
# Before that an event is pending and not sampled.
LOST_AFTER_S = {"TRADE": 10, "QUOTE": 10, "MARK": 10, "BAR": 60}
LATEST_STATE_FEEDS = ("QUOTE", "MARK")
MD_FEEDS = ("TRADE", "QUOTE", "BAR", "MARK")
CATALOG_FEED = {"TRADE": "TRADE", "QUOTE": "QUOTE", "BAR": "BAR", "MARK": "MARK_INDEX_PRICE"}
# The production acquisition plan: read-only, only for the OKX index symbol of a MARK_INDEX binding (never inferred).
PRODUCTION_ACQUISITION = "config/v2/stable-acquisition-bindings.yaml"
BAR_INTERVAL, BAR_MS = "1m", 60_000
# Synthetic venue cadence per instrument: steady trades plus a short burst, on-change quotes, one final bar per
# minute shortly after its close (Binance's native lane measured 0.04-1.1 s), a few closed bars seeded at start
# (the bar edge's REST bootstrap role) so the writer's BAR warmup has a retained view, one MARK/INDEX observation
# per second (Binance @markPrice@1s; OKX mark-price + index-tickers pushed as one changed pair per second).
MD_RATE = {"trade_per_s": 3, "burst_every_s": 15, "burst_trades": 8, "quote_per_s": 2,
           "bar_delay_ms": 200, "seed_bars": 5, "mark_per_s": 1}
TEST_ADAPTER = "p184-synthetic-test-only"
# INCIDENT 2026-10-15 core-group reproduction (``--core-group``): the production group shape (3 members, 6 raw
# partitions, one transactional.id each) instead of G12's single core on a 1-partition raw topic. Absent
# ``work/core-group.json`` every G12 mode keeps its single-core shape.
CORE_MEMBERS = ("core", "core-2", "core-3")
CORE_GROUP_FILE = "core-group.json"
# formation_*: B2 (2026-10-15) the baseline opened at member start and counted core-2's formation retry ("rebalanced
# during the transaction" while members were still joining); the baseline now opens only after the group has been
# stable for formation_hold_s (bounded by formation_max_s, else the run is NOT_MEASURED).
CORE_GROUP = {"raw_partitions": 6, "baseline_s": 120, "hold_s": 150, "kill_member": "core-2",
              "kill_caught_up_s": 60, "formation_hold_s": 10, "formation_max_s": 120,
              "precondition_member": "core-2", "precondition_hold_s": 60, "restart_member": "core-3",
              "reform_s": 30, "replace_caught_up_s": 60}
CORE_EVENTS = ("qdl_realtime_core_started", "qdl_realtime_core_retry", "qdl_realtime_core_rebalance_rewind",
               "qdl_realtime_core_rewind_failed", "qdl_realtime_core_inflight_commit_failed",
               "qdl_realtime_core_stop_drain_failed")
# INCIDENT 2026-10-15 #2 (rc22 rollout 10:55Z, storm kept on the all-RUST18 group until 11:01Z). The production
# group differed from B3 in (kafka3 coordinator log, receipts, rust_core.json): the replacement joined 3.9 s after
# the old member's LeaveGroup (compose recreate; harness 1.2 s), after the leave rebalance had already stabilized,
# so its join was a second rebalance revoking both RUST18 survivors; the survivors' supervisor backoff never resets
# (rust_core_3 entered at the 24 s cap from the morning incident, rust_core_2 at 1.6 s), so their rejoins never
# coalesced with a leave rebalance again and each rejoin revoked the other; and batch 1024 / wait 25 ms / 1 CPU /
# group.initial.rebalance.delay.ms 3000. ``core_profile`` makes each of these a run option; every default keeps
# the B3 harness unchanged (batch 25 / wait 10 ms, 4 Hz, 0.5 CPU, restart at once, no precondition, delay 0).
CORE_PROFILE_DEFAULTS = {"batch_size": None, "batch_wait_ms": None, "hz": PUBLISH_HZ, "core_cpus": "0.5",
                         "replace_gap_s": 0.0, "precondition_restarts": 0, "initial_rebalance_delay_ms": None}
# precondition_*: K SIGTERM restarts of one member on the OLD image (each joins after ``replace_gap_s``, so its join
# revokes the other two) before the baseline: the never-reset backoff of the other members advances, as production's
# survivors had (diagnostic windows ``precondition_<n>``, never judged). restart_member: the single SIGTERM restart
# on the all-new group of ``--core-group-simultaneous``. reform_s / replace_caught_up_s: its replace-all bounds
# (all in CORE_GROUP above).


def core_profile(**values):
    """The run's validated core-group profile (CORE_PROFILE_DEFAULTS <- ``values``; None means the default)."""
    profile = dict(CORE_PROFILE_DEFAULTS)
    unknown = set(values) - set(profile)
    if unknown:
        raise ValueError("unknown core profile field(s): " + ",".join(sorted(unknown)))
    profile.update({k: v for k, v in values.items() if v is not None})
    bounded = (("batch_size", 1, 4096, int), ("batch_wait_ms", 1, 1000, int),
               ("initial_rebalance_delay_ms", 0, 10_000, int), ("precondition_restarts", 0, 5, int),
               ("replace_gap_s", 0, 30, (int, float)), ("hz", 0, 20, (int, float)))
    for key, lo, hi, kind in bounded:
        value = profile[key]
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, kind) or not lo <= value <= hi:
            raise ValueError(f"core profile {key} must be in [{lo}, {hi}]")
    if profile["hz"] <= 0:
        raise ValueError("core profile hz must be > 0")
    cpus = profile["core_cpus"] = str(profile["core_cpus"])
    if not re.fullmatch(r"\d+(\.\d+)?", cpus) or not 0.1 <= float(cpus) <= 2.0:
        raise ValueError("core profile core_cpus must be in [0.1, 2.0]")
    return profile


def apply_core_profile(config, profile):
    """core.json with the profile's batch_size / batch_wait_ms (unchanged where the profile keeps the default)."""
    out = dict(config)
    for key in ("batch_size", "batch_wait_ms"):
        if profile.get(key) is not None:
            out[key] = profile[key]
    return out


def core_group(work):
    """The run's core-group shape: {"members", "raw_partitions"}; G12 single core when the file is absent."""
    path = Path(work) / CORE_GROUP_FILE
    shape = json.loads(path.read_text()) if path.exists() else {"members": 1, "raw_partitions": 1}
    if shape["members"] not in (1, len(CORE_MEMBERS)) or shape["raw_partitions"] not in (1, 6):
        raise ValueError("core group is 1 member/1 partition or 3 members/6 partitions")
    return shape


RAW_LANE_ORDER = ("BOOK", "TRADE", "QUOTE", "MARK", "INDEX", "BAR")  # by publish rate, highest first


def raw_partition_plan(bindings, partitions):
    """{subscription key (source_id): raw partition}: stable per key (per-binding order kept) and deterministic.

    B2 (2026-10-15): ``crc32(key) % 6`` over the 10 sealed keys left raw/5 without a record for the whole run, so
    a kill clock on a member owning it could never close. The distinct keys are ordered by lane rate (BOOK,
    TRADE, QUOTE, MARK/INDEX, BAR; then source_id) and dealt round-robin, so with >= ``partitions`` keys every
    raw partition gets traffic and the six highest-rate lanes land on six different partitions."""
    ranked = {}
    for binding in bindings:
        lane = "BOOK" if binding.get("l2") else lane_feed(binding["provider_kind"])
        rank = RAW_LANE_ORDER.index(lane) if lane in RAW_LANE_ORDER else len(RAW_LANE_ORDER)
        key = binding["source_id"]
        ranked[key] = min(rank, ranked.get(key, rank))
    ordered = sorted(ranked, key=lambda key: (ranked[key], key))
    return {key: index % partitions for index, key in enumerate(ordered)}


def write_member_configs(work, members):
    """core-<n>.json for members 2..n: core.json with its own transactional.id (one per production member)."""
    work = Path(work)
    config = json.loads((work / "core.json").read_text())
    base = config["transactional_id"]
    for index in range(2, members + 1):
        member = {**config, "transactional_id": f"{base}-{index}"}
        (work / f"core-{index}.json").write_text(json.dumps(member))
# CAPTURE adapter mode (TEST_ONLY real-capture replay; ``Run.use_capture_adapter``): core and stream must be the
# isolated-replay-clock TEST builds (image label), never a release image.
TEST_CLOCK_LABEL = ("io.qdl.test-only", "isolated-replay-clock")
TEST_CLOCK_ROLES = ("core", "stream")
CAPTURE_PARITY_SCHEMA = "p184.g12.capture-adapter-parity.v1"
# Physical raw route of a core binding (the TS replay preparation's FIELDS, same physical_* precedence).
ROUTE_FIELDS = ("provider", "venue", "market", "product_type", "native_symbol", "native_channel")
# Requirement shape of the deployed TS paper manifest (consumers/stable/trading-system-paper.yaml r10) for these feeds;
# event recency and session liveness stay null like the DR05 BOOK requirements (no isolated provider-session signal).
MD_REQUIREMENTS = {"TRADE": {"max_freshness_ms": 3000, "warmup_limit": 0, "require_final_bars": True},
                   "QUOTE": {"max_freshness_ms": 2000, "warmup_limit": 0, "require_final_bars": True},
                   "BAR": {"max_freshness_ms": 180000, "warmup_limit": 1000, "interval": BAR_INTERVAL,
                           "require_final_bars": True},
                   "MARK": {"max_freshness_ms": 2000, "warmup_limit": 0, "require_final_bars": False,
                            "recovery": "FRESH_SNAPSHOT"}}
# provider kind suffix -> lane. MARK lanes: Binance one BOTH component; OKX a MARK and an INDEX component.
MD_KIND_FEED = {"_trade": "TRADE", "_bbo": "QUOTE", "_bar": "BAR", "_mark_index": "MARK", "_mark_price": "MARK",
                "_index_price": "INDEX"}
IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")
QUERY_CODE = ("worktree", "image")
# G12 role -> DR05/DR08 image role (handoff "images"; dr05 image_set() requires exactly these keys).
DR05_IMAGE_ROLES = {"query": "python", "core": "core", "stream": "stream", "python": "python",
                    "kafka": "kafka", "redis": "redis"}
PRIVATE_FILES = ("client.key", "server.p12", "binance.key", "okx.key", "cursor-keys.json",
                 "read-view.secret", "broker.properties", "broker-1.properties",
                 "broker-2.properties", "broker-3.properties")


def resolve_image_set(image_set=None, overrides=()):
    """IMAGES (defaults) <- JSON file/object <- ROLE=ID overrides; every role an immutable image ID."""
    images = dict(IMAGES)
    layers = []
    if image_set is not None:
        loaded = image_set if isinstance(image_set, dict) else json.loads(Path(image_set).read_text())
        if not isinstance(loaded, dict):
            raise ValueError("image set file must be a JSON object role -> sha256:<64hex>")
        layers.append(loaded)
    parsed = {}
    for value in overrides or ():
        role, sep, image = str(value).partition("=")
        if not sep or role in parsed:
            raise ValueError(f"--image ROLE=sha256:<64hex>, once per role: {value!r}")
        parsed[role] = image
    layers.append(parsed)
    for layer in layers:
        for role, image in layer.items():
            if role not in IMAGES:
                raise ValueError(f"unknown image role {role!r}; roles: {sorted(IMAGES)}")
            images[role] = image
    for role, image in images.items():
        if not isinstance(image, str) or not IMAGE_ID.fullmatch(image):
            raise ValueError(f"immutable {role} image ID required (sha256:<64hex>)")
    return images


def dr05_images(images):
    """The sealed set in DR05's role names (handoff.json "images")."""
    return {role: images[g12_role] for role, g12_role in DR05_IMAGE_ROLES.items()}


def role_image_key(role):
    """Which image-set role a container role runs."""
    base = re.sub(r"-(binance|okx)$", "", role)
    if base in CORE_MEMBERS:  # core-group members run the core image
        return "core"
    return {"net": "holder", "quota": "redis", "query": "python", "helper": "python", "monitor": "python",
            "publisher": "python", "sampler": "ts", "tswriter": "ts", "tsredis": "redis",
            "tspg": "postgres"}.get(base, "kafka" if base.startswith("kafka-") else base)


def namespace(value):
    if not isinstance(value, str) or not re.fullmatch(NS_PATTERN, value):
        raise ValueError("exact disposable G12 namespace required")
    return value


def broker_port(node):
    if node not in BROKERS:
        raise ValueError("unknown broker")
    return 9092 + 100 * (node - 1)


def topics(ns, raw_partitions=1, latest=None):
    """name -> (partitions, config). RF3 and min ISR 2 everywhere (production shape). ``latest``: the mirrored
    production profile's latest-topic keys (decision A': md.latest.v2 delete.retention.ms 21,600,000 since
    2026-10-16); without a profile the latest topic keeps the bars tombstone lifetime (the pre-A' harness)."""
    namespace(ns)
    data = {"min.insync.replicas": "2"}
    state = {**data, "cleanup.policy": "compact", "delete.retention.ms": "604800000"}
    if latest is not None and set(latest) - set(STATE_TOPIC_PROFILE_KEYS["latest"]):
        raise ValueError("latest topic profile may only set " + ",".join(STATE_TOPIC_PROFILE_KEYS["latest"]))
    return {ns + ".raw": (raw_partitions, data), ns + ".canonical": (1, data), ns + ".quarantine": (1, data),
            ns + ".latest": (3, {**state, **(latest or {})}),
            ns + ".bars": (3, {**state, "min.compaction.lag.ms": "3600000"})}


def broker_password(text):
    """The keystore password DR05 material wrote; never logged."""
    found = re.findall(r"^ssl\.keystore\.password=(\S+)$", text, flags=re.M)
    if len(found) != 1:
        raise ValueError("exactly one keystore password required")
    return found[0]


def broker_properties(password, node, initial_rebalance_delay_ms=0, auto_leader_rebalance=True):
    """One KRaft broker's properties. ``auto_leader_rebalance``: auto.leader.rebalance.enable, always written
    explicitly (``--auto-leader-rebalance``: production's value, recorded in the run receipt)."""
    if type(auto_leader_rebalance) is not bool:
        raise ValueError("auto_leader_rebalance must be a bool")
    port = broker_port(node)
    voters = ",".join(f"{n}@localhost:{broker_port(n) + 1}" for n in BROKERS)
    return f"""process.roles=broker,controller
node.id={node}
controller.quorum.voters={voters}
listeners=SSL://:{port},CONTROLLER://:{port + 1}
advertised.listeners=SSL://localhost:{port}
controller.listener.names=CONTROLLER
listener.security.protocol.map=SSL:SSL,CONTROLLER:PLAINTEXT
inter.broker.listener.name=SSL
ssl.keystore.type=PKCS12
ssl.keystore.location=/work/server.p12
ssl.keystore.password={password}
ssl.key.password={password}
ssl.truststore.type=PEM
ssl.truststore.location=/work/ca.crt
ssl.client.auth=required
log.dirs=/data
log.segment.bytes=16777216
metadata.log.segment.bytes=16777216
log.index.size.max.bytes=1048576
num.partitions=1
default.replication.factor=3
min.insync.replicas=2
offsets.topic.num.partitions=1
offsets.topic.replication.factor=3
transaction.state.log.num.partitions=1
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
unclean.leader.election.enable=false
auto.create.topics.enable=false
group.initial.rebalance.delay.ms={int(initial_rebalance_delay_ms)}
auto.leader.rebalance.enable={"true" if auto_leader_rebalance else "false"}
"""


def _base(ns, role, *, network, memory, cpus, readonly=True, uid=None):
    namespace(ns)
    argv = ["docker", "run", "-d", "--rm", "--pull=never", "--name", f"{ns}-{role}",
            "--label", f"{LABEL}={ns}", "--network", network, "--memory", memory, "--cpus", cpus,
            "--pids-limit", "256", "--log-opt", "max-size=4m", "--log-opt", "max-file=1"]
    if readonly:
        argv.append("--read-only")
    if uid is not None:
        argv += ["--user", uid]
    return argv


def _env(argv, env):
    for key, value in env.items():
        argv += ["-e", f"{key}={value}"]
    return argv


def topology(ns, work, topic_id="", *, uid=None, images=None, query_code="worktree", core_members=1,
             core_cpus="0.5"):
    """Docker argv per role (adapted from p184_dr05_dl_fault.topology: 3 brokers,
    a projector, one holder-owned loopback namespace). Executes nothing."""
    namespace(ns)
    images = images or IMAGES
    if query_code not in QUERY_CODE:
        raise ValueError("query_code must be one of " + ",".join(QUERY_CODE))
    work = Path(work).resolve()
    if work == ROOT or ROOT in work.parents or any(c in str(work) for c in (":", ",")):
        raise ValueError("external unambiguous work directory required")
    uid = uid or f"{os.getuid()}:{os.getgid()}"
    net = "container:" + ns + "-net"
    tls = {"QDL_KN_KAFKA_CA": "/work/ca.crt", "QDL_KN_KAFKA_CERT": "/work/client.crt",
           "QDL_KN_KAFKA_KEY": "/work/client.key"}
    common = {"QDL_ENVIRONMENT": "paper", "QDL_KN_CURSOR_KEYS_FILE": "/work/cursor-keys.json",
              "QDL_KN_CURSOR_ACTIVE_KEY_ID": "dr08", "QDL_KN_TOPIC_ID": topic_id,
              "QDL_KN_ROUTE_GENERATION": ns, "QDL_KN_READ_VIEW_SECRET_FILE": "/work/read-view.secret"}
    mount = ["-v", f"{work}:/work"]
    # The holder's hostname is the namespace's: Kafka/log4j resolve it (DR05 kafka: --hostname localhost).
    roles = {"net": _base(ns, "net", network="none", memory="16m", cpus="0.05")
             + ["--hostname", "localhost", "--entrypoint", "sleep", images["holder"], str(LIMITS["holder_s"])]}
    for node in BROKERS:
        argv = _base(ns, f"kafka-{node}", network=net, memory="1024m", cpus="0.75", uid=uid)
        argv += ["--tmpfs", "/tmp:rw,exec,size=64m,mode=1777", "-v", f"{work}/kafka-{node}:/data",
                 "-v", f"{work}:/work:ro"]
        for target in ("/etc/kafka/secrets", "/mnt/shared/config", "/var/lib/kafka/data"):
            argv += ["--tmpfs", target + ":rw,size=1m,mode=1777"]  # no anonymous volumes
        _env(argv, {"KAFKA_HEAP_OPTS": "-Xms256m -Xmx512m", "LOG_DIR": "/tmp/kafka-logs"})
        props = f"/work/broker-{node}.properties"
        roles[f"kafka-{node}"] = argv + ["--entrypoint", "/bin/sh", images["kafka"], "-ec",
            f"/opt/kafka/bin/kafka-storage.sh format --ignore-formatted -t {CLUSTER_ID} -c {props}"
            f" && exec /opt/kafka/bin/kafka-server-start.sh {props}"]
    roles["quota"] = _base(ns, "quota", network=net, memory="192m", cpus="0.1", uid=uid) + [
        "--tmpfs", "/data:rw,size=1m,mode=1777", "--entrypoint", "redis-server", images["redis"],
        "--save", "", "--appendonly", "no", "--maxmemory", "128mb", "--maxmemory-policy", "noeviction"]
    # One group (QDL_KAFKA_GROUP_ID ns) for every member; member n>1 reads core-<n>.json (own transactional.id).
    for index, role in enumerate(CORE_MEMBERS[:core_members], start=1):
        suffix = "" if index == 1 else f"-{index}"
        roles[role] = _env(_base(ns, role, network=net, memory="256m", cpus=str(core_cpus), uid=uid)
            + ["--tmpfs", "/tmp:rw,size=64m,mode=1777"] + mount,
            {"QDL_KAFKA_CERT_ROOT": "/work", "QDL_KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP,
             "QDL_KAFKA_CLIENT_ID": ns + suffix, "QDL_KAFKA_GROUP_ID": ns,
             "QDL_PROVIDER_ADMISSION_ENABLED": "false"}) + [
            "--entrypoint", "/usr/local/bin/qdl-realtime-core", images["core"], f"/work/core{suffix}.json"]
    roles["projector"] = _env(_base(ns, "projector", network=net, memory="256m", cpus="0.5", uid=uid)
        + ["--tmpfs", "/tmp:rw,size=64m,mode=1777"] + mount,
        {**tls, "QDL_KN_KAFKA_BOOTSTRAP": BOOTSTRAP, "QDL_KN_BUNDLE_PATH": "/work/bundle.json",
         "QDL_KN_TOPIC_ID": topic_id, "QDL_KN_REPLICA": "a", "QDL_KN_MATERIALIZER_EPOCH": "1",
         "QDL_KN_MARKET_CACHE_URL": "redis://localhost:6379/1",
         "QDL_KN_STATUS_PATH": "/work/g12-projector-status.json", "QDL_KN_STATUS_INTERVAL_S": "5",
         "QDL_KN_CANONICAL_TOPIC": ns + ".canonical", "QDL_KN_LATEST_TOPIC": ns + ".latest",
         "QDL_KN_BARS_TOPIC": ns + ".bars"}) + [
        "--entrypoint", "/usr/local/bin/qdl-projector", images["projector"], "run"]
    roles["stream"] = _env(_base(ns, "stream", network=net, memory="512m", cpus="0.5", uid=uid)
        + ["--tmpfs", "/tmp:rw,size=64m,mode=1777"] + mount,
        {**common, **tls, "QDL_KN_BUNDLE_FILE": "/work/bundle.json",
         "QDL_KN_JWT_CONFIG_FILE": "/work/jwt-config.json",
         "QDL_KN_QUOTA_REDIS_URL": "redis://localhost:6379/0", "QDL_KN_QUOTA_PREFIX": "qdl:stable:v2:paper:" + ns,
         "QDL_KN_TLS_CERT_FILE": "/work/client.crt", "QDL_KN_TLS_KEY_FILE": "/work/client.key",
         "QDL_KN_TLS_CLIENT_CA_FILE": "/work/ca.crt", "QDL_KN_KAFKA_BOOTSTRAP": BOOTSTRAP,
         "QDL_KN_KAFKA_TOPIC": ns + ".canonical", "QDL_KN_KAFKA_GROUP": "kn-" + ns,
         "QDL_KN_KAFKA_CLIENT_ID": ns + "-stream", "QDL_KN_READ_VIEW_URLS": "https://localhost:8200",
         "QDL_KN_READ_VIEW_CA_FILE": "/work/ca.crt", "QDL_KN_HOT_READ_ENABLED": "true",
         "QDL_KN_METRICS_SECONDS": "10", "QDL_KN_MAX_REPLAY_EVENTS": "10000",
         "QDL_KN_QUEUE_BYTES_TOTAL": "33554432"}) + [
        "--entrypoint", "/usr/local/bin/qdl-stream-gateway", images["stream"], "serve"]
    # "image": the Query code is the image's own /app (the release under test); "worktree": read-only mount.
    # Either way the entry is this harness's --query (serve_query): the DR05/DR08 Query of that code plus the
    # execution MARK/INDEX reader; with "image" only this one harness file is mounted, never the worktree.
    if query_code == "image":
        code = ["-v", f"{ROOT / 'scripts/p184_g12_isolated_fault.py'}:/harness/p184_g12_isolated_fault.py:ro",
                "-w", "/app"]
        entry = "/harness/p184_g12_isolated_fault.py"
    else:
        code, entry = ["-v", f"{ROOT}:/app:ro", "-w", "/app"], "scripts/p184_g12_isolated_fault.py"
    roles["query"] = _env(_base(ns, "query", network=net, memory="512m", cpus="0.5", uid=uid)
        + ["--tmpfs", "/tmp:rw,size=64m,mode=1777"] + mount + code,
        {**common, "PYTHONPATH": "/app:/app/generated/python", "PYTHONDONTWRITEBYTECODE": "1"}) + [
        "--entrypoint", "python", images["python"], "-B", entry, "--query", "/work"]
    return roles


SERVICE_ORDER = ("quota", "core", "projector", "stream", "query")
TEARDOWN_ORDER = ("sampler", "publisher", "monitor", "query", "stream", "projector", "core-3", "core-2", "core",
                  "quota",
                  "kafka-3", "kafka-2", "kafka-1", "helper", "net")


# IDR-107: a product role that exits unexpectedly is --rm-removed with its stderr; a per-life follower keeps the
# tail. No restart policy: an exit stays a STOP.
FOLLOWED_ROLES = SERVICE_ORDER + CORE_MEMBERS[1:] + tuple(f"kafka-{node}" for node in BROKERS)
FOLLOW_TAIL_BYTES = 1_000_000
# RB3 (review 2026-10-15): a core-group member logs ~1 MB per 5 min of progress lines, so the 1 MB tail dropped the
# start of every long life (A/B 2026-10-15: lifecycle events lost); the acceptance needs the whole life.
FOLLOW_TAIL_BYTES_CORE_GROUP = 32_000_000


def follow_argv(ns, role, seconds):
    """Bounded read-only ``docker logs --follow`` of one owned role: it ends when the container exits (or is
    removed), and ``timeout`` ends it even if the orchestrator is killed."""
    namespace(ns)
    if role not in FOLLOWED_ROLES or not 0 < seconds <= LIMITS["deadline_s"] + 900:
        raise ValueError("bounded follower of a product role required")
    return ["timeout", str(int(seconds)), "docker", "logs", "--follow", "--timestamps", f"{ns}-{role}"]


def helper_argv(ns, work, args, *, role="helper", detach=False, evidence=None, network=None,
                cpus="0.5", memory="512m", script="scripts/p184_g12_isolated_fault.py", extra=(), images=None):
    namespace(ns)
    images = images or IMAGES
    argv = _base(ns, role, network=network or "container:" + ns + "-net", memory=memory, cpus=cpus,
                 uid=f"{os.getuid()}:{os.getgid()}")
    if not detach:
        argv.remove("-d")
    argv += ["--tmpfs", "/tmp:rw,size=64m,mode=1777", "-v", f"{Path(work).resolve()}:/work",
             "-v", f"{ROOT}:/app:ro", "-w", "/app", "-e", "PYTHONPATH=/app:/app/generated/python",
             "-e", "PYTHONDONTWRITEBYTECODE=1", "-e", "P184_G12_NAMESPACE=" + ns,
             "-e", "P184_G12_IMAGES=" + json.dumps(images, sort_keys=True, separators=(",", ":")), *extra]
    if evidence is not None:
        argv += ["-v", f"{Path(evidence).resolve()}:/evidence"]
    return argv + ["--entrypoint", "python", images["python"], "-B", script, *args]


def md_event_row(channel, data):
    """One applied TS notification -> evidence fields, or None (not a G2/G3 sample).

    Runs inside the sampler (its source is embedded in SAMPLER). TRADE/QUOTE: the
    writer's per-event ``events.market.v2.execution.<feed>.*`` publish (every applied
    event; observed_at_ms = canonical source_event_time); id = native trade id, or the
    quote's event ms. MARK: the ordered MARK_INDEX_PRICE state publish
    (``execution.mark_index_price``, only when the CAS applies a newer view); id = the
    pair's observed ms. BAR: ``events.market.v2.bar.*`` closed 1m rows; id = open ms.
    """
    parts = str(channel).split(".")
    try:
        payload = json.loads(data)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict) or parts[:3] != ["events", "market", "v2"] or len(parts) < 5:
        return None
    if parts[3] == "execution" and parts[4] in ("trade", "quote", "mark_index_price"):
        feed = {"trade": "TRADE", "quote": "QUOTE", "mark_index_price": "MARK"}[parts[4]]
        event_ms = payload.get("observed_at_ms")
        ident = ((payload.get("payload") or {}).get("native_trade_id") if feed == "TRADE" else event_ms)
        return {"feed": feed, "id": None if ident is None else str(ident), "ev_ms": event_ms}
    if parts[3] == "bar" and payload.get("is_closed") is True and payload.get("interval") == "1m":
        opened = payload.get("timestamp_ms")
        return {"feed": "BAR", "id": None if opened is None else str(opened), "ev_ms": opened}
    return None


SAMPLER = r'''
import asyncio, json, signal, sys, time
from redis.asyncio import Redis
from services.market_data.cache_reader import MarketCacheReader
from services.risk_engine.execution_market_context import _mapping, _missing_reason, _validate_book
from domain.instrument_identity import CanonicalInstrumentId
IDS = {"BINANCE": ("BINANCE", "USD_M", "ETHUSDT", "ETHUSDT"), "OKX": ("OKX", "SWAP", "ETHUSDT", "ETH-USDT-SWAP")}
STOP = []
signal.signal(signal.SIGTERM, lambda *_: STOP.append(1))
async def view(r, venue):
    now_ms = time.time_ns() // 1_000_000
    c = await MarketCacheReader(r).execution_market_context(CanonicalInstrumentId(*IDS[venue]),
                                                           required_feeds=frozenset({"BOOK_SNAPSHOT"}))
    book = (c or {}).get("book") or {}
    seq = book.get("sequence") or book.get("native_sequence_end")
    try:
        _validate_book(_mapping((c or {}).get("book"), reason=_missing_reason(c or {}, "book",
                       "EXECUTION_CONTEXT_BOOK_MISSING")), now_ms=now_ms, max_age_ms=2000, side="BUY",
                       require_levels=True)
        verdict = "APPROVED"
    except Exception as e:
        verdict = (type(e).__name__ + ":" + str(e))[:160]
    try:
        seq = None if seq is None else int(seq)
    except (TypeError, ValueError):
        seq = None
    return seq, verdict
async def beat(r):
    keys = [k async for k in r.scan_iter(match="service:heartbeat:market_data:*", count=100)]
    if not keys:
        return {"heartbeat": False}
    b = json.loads(await r.get(keys[0]) or "{}"); d = b.get("details") or {}
    # Native typed TRADE counters kept raw (no coercion): the host analysis validates them (OF-G12-2).
    return {"heartbeat": True, "status": b.get("status"), "demanded": d.get("demanded_v2_slices"),
            "session_ready": d.get("session_ready_v2_slices"), "key_tail": str(keys[0])[-24:],
            "last_seen_unix": b.get("last_seen_unix"),
            **{k: d[k] for k in ("trade_integrity", "trade_integrity_scope", "worker_liveness") if k in d}}
MD_PATTERNS = ("events.market.v2.execution.trade.*", "events.market.v2.execution.quote.*",
               "events.market.v2.execution.mark_index_price.*", "events.market.v2.bar.*")
async def listen(url, venue, f, end):
    """PSUBSCRIBE only (read-only): receipt time of every applied TRADE/QUOTE/MARK/closed BAR of one clone."""
    while time.monotonic() < end and not STOP:
        r = Redis.from_url(url.rsplit("/", 1)[0] + "/0", socket_connect_timeout=1, decode_responses=True)
        p = r.pubsub(ignore_subscribe_messages=True)
        try:
            await p.psubscribe(*MD_PATTERNS)
            while time.monotonic() < end and not STOP:
                m = await p.get_message(ignore_subscribe_messages=True, timeout=0.5)
                if not m or m.get("type") != "pmessage":
                    continue
                t = time.time_ns()
                row = md_event_row(m["channel"], m["data"])
                if row is not None:
                    f.write(json.dumps({"k": "ev", "t": t, "venue": venue, **row}) + "\n")
        except Exception as e:
            f.write(json.dumps({"k": "ev_error", "t": time.time_ns(), "venue": venue, "error": type(e).__name__}) + "\n")
            await asyncio.sleep(1)
        finally:
            for close in (p.aclose, r.aclose):
                try:
                    await close()
                except Exception:
                    pass
async def main(out, seconds, interval_ms, targets):
    views = {v: Redis.from_url(u, socket_timeout=1, socket_connect_timeout=1) for v, u in targets.items()}
    beats = {v: Redis.from_url(u.rsplit("/", 1)[0] + "/2", socket_timeout=1, socket_connect_timeout=1,
                               decode_responses=True) for v, u in targets.items()}
    end = time.monotonic() + seconds
    last, last_t, last_beat, polled = {}, {}, 0.0, {}
    with open(out, "a", buffering=1) as f:
        listeners = [asyncio.create_task(listen(u, v, f, end)) for v, u in targets.items()]
        while time.monotonic() < end and not STOP:
            for venue, r in views.items():
                t = time.time_ns()
                try:
                    seq, verdict = await view(r, venue)
                    # pt: the previous successful poll (logged or not), so the analysis sees real sampler holes.
                    row = {"k": "view", "t": t, "venue": venue, "seq": seq, "v": verdict, "pt": polled.get(venue)}
                    polled[venue] = t
                except Exception as e:
                    row = {"k": "view", "t": t, "venue": venue, "seq": None,
                           "v": "READ_ERROR:" + type(e).__name__}
                key = (row["seq"], row["v"])
                if last.get(venue) != key or t - last_t.get(venue, 0) >= 500_000_000:
                    f.write(json.dumps(row) + "\n"); last[venue] = key; last_t[venue] = t
            if time.monotonic() - last_beat >= 2:
                last_beat = time.monotonic()
                for venue, r in beats.items():
                    try:
                        row = await beat(r)
                    except Exception as e:
                        row = {"heartbeat": None, "error": type(e).__name__}
                    f.write(json.dumps({"k": "hb", "t": time.time_ns(), "venue": venue, **row}) + "\n")
            await asyncio.sleep(interval_ms / 1000)
        await asyncio.wait(listeners, timeout=3)
        for task in listeners:
            task.cancel()
    for r in (*views.values(), *beats.values()):
        await r.aclose()
asyncio.run(main(sys.argv[1], float(sys.argv[2]), float(sys.argv[3]),
                 dict(item.split("=", 1) for item in sys.argv[4].split(","))))
'''
# The sampler runs the same parser the unit tests exercise.
SAMPLER = SAMPLER.replace("STOP = []\n", "STOP = []\n" + inspect.getsource(md_event_row), 1)


def sampler_argv(ns, evidence, seconds, interval_ms=50, images=None):
    images = images or IMAGES
    targets = ",".join(f"{v}=redis://localhost:{CLONE_PORTS[v][0]}/1" for v in VENUES)
    argv = _base(ns, "sampler", network="container:" + ns + "-net", memory="384m", cpus="0.25",
                 uid=f"{os.getuid()}:{os.getgid()}")
    return argv + ["--tmpfs", "/tmp:rw,size=16m,mode=1777", "-v", f"{Path(evidence).resolve()}:/evidence",
                   "-e", "PYTHONDONTWRITEBYTECODE=1", "--entrypoint", "python", images["ts"], "-B", "-c",
                   SAMPLER, "/evidence/sampler.jsonl", str(int(seconds)), str(interval_ms), targets]


# ---------------------------------------------------------------- analysis (pure)

def load_jsonl(path):
    rows = []
    try:
        with open(path) as handle:
            for line in handle:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass  # a torn last line while the writer is live
    except FileNotFoundError:
        pass
    return rows


def publish_index(rows):
    """(venue, sequence) -> publish ns (the frame's own event time)."""
    return {(r["venue"], r["seq"]): r["t"] for r in rows if r.get("k") == "pub"}


def view_series(rows):
    series = {}
    for r in rows:
        if r.get("k") == "view":
            series.setdefault(r["venue"], []).append((r["t"], r.get("seq"), r.get("v") == "APPROVED"))
    for values in series.values():
        values.sort(key=lambda item: item[0])
    return series


def view_polls(rows):
    """venue -> {sample ns: previous successful poll ns} (``pt``; a row logged before the field existed: absent)."""
    polls = {}
    for r in rows:
        if r.get("k") == "view" and r.get("pt") is not None:
            polls.setdefault(r["venue"], {})[r["t"]] = r["pt"]
    return polls


def sampler_max_gap_s(series, venue, start_ns, end_ns):
    """Longest interval (s) in [start, end] without an observed view of ``venue`` (a READ_ERROR row is none)."""
    points = [start_ns, *sorted(t for t, seq, _ok in series.get(venue, ()) if seq is not None
                                and start_ns < t < end_ns), end_ns]
    return round(max(b - a for a, b in zip(points, points[1:])) / 1e9, 3) if end_ns > start_ns else 0.0


def caught_up_predicate(core_config=None, hz=PUBLISH_HZ):
    """The caught-up predicate's version and parameters, frozen before data (``Run.prepare`` writes them to
    ``evidence/caught-up-predicate.json`` once the core material exists; ``evaluate`` reads them back).

    ``core_config``: the run's ``work/core.json``; each venue's materialization interval is the one its BOOK
    binding gives the core. None (offline analysis of a run sealed before v2): the production default.
    """
    intervals = {venue: L2_DEFAULT_MATERIALIZATION_MS for venue in VENUES}
    source = "default:EXECUTION_L2_MATERIALIZATION_INTERVAL_MS"
    if core_config is not None:
        source = "core.json"
        books = {b["venue"]: b["l2"] for b in core_config["core"]["bindings"] if b.get("l2")}
        if set(books) != set(VENUES):
            raise ValueError("one BOOK binding per venue required for the caught-up predicate")
        for venue, l2 in books.items():
            value = l2.get("materialized_snapshot_interval_ms")
            intervals[venue] = int(value) if value is not None else int(l2["snapshot_refresh_seconds"]) * 1000
    if not 0 < hz <= 20:
        raise ValueError("bounded publisher rate required")
    return {"version": CAUGHT_UP_VERSION, "transit_max_ms": GATE["age_ms"], "hold_s": GATE["hold_s"],
            "top_change_min_interval_ms": L2_TOP_CHANGE_MIN_INTERVAL_MS,
            "materialized_snapshot_interval_ms": intervals, "materialization_interval_source": source,
            "publisher_tick_ms": round(1000 / hz, 3), "slack_ms": CORE_MS_FLOOR_SLACK_MS,
            "book_top": dict(BOOK_TOP)}


def book_frames(rows):
    """Per venue, the publisher's BOOK frames in publish order: ``{"t", "seq", "gen", "top", "pos"}`` lists
    (``top`` = (bid, ask) as published, None for a run published before the top was recorded = a fixed top)."""
    frames = {}
    for r in sorted((r for r in rows if r.get("k") == "pub"), key=lambda r: r["t"]):
        f = frames.setdefault(r["venue"], {"t": [], "seq": [], "gen": [], "top": [], "pos": {}})
        f["pos"][r["seq"]] = len(f["t"])
        f["t"].append(r["t"])
        f["seq"].append(r["seq"])
        f["gen"].append(r.get("gen"))
        f["top"].append(tuple(r["top"]) if r.get("top") is not None else None)
    return frames


def frames_from_index(index):
    """``book_frames`` shape from a bare publish index (no generation / top recorded: a fixed top)."""
    return book_frames({"k": "pub", "t": t, "venue": venue, "seq": seq} for (venue, seq), t in index.items())


def regime_interval_ms(frames, venue, seq, predicate):
    """The core's materialization interval after it emitted frame ``seq``, for the observed top-change regime.

    TOP_CHANGE: some frame published within the configured interval has another top or generation than ``seq``
    -> max(top-change floor, that frame's offset) (a generation change: its offset). QUIET otherwise -> the
    configured interval. Mirrors due_l2_materialized_snapshot (the comparison is against the last emitted top).
    """
    interval = predicate["materialized_snapshot_interval_ms"][venue]
    f = frames.get(venue)
    pos = None if f is None else f["pos"].get(seq)
    if pos is None:
        return interval, "UNKNOWN"
    t0, gen0, top0 = f["t"][pos], f["gen"][pos], f["top"][pos]
    for k in range(pos + 1, len(f["t"])):
        offset_ms = (f["t"][k] - t0) / 1e6
        if offset_ms >= interval:
            break
        if f["gen"][k] != gen0:
            return offset_ms, "GENERATION"
        if f["top"][k] != top0:
            return max(predicate["top_change_min_interval_ms"], offset_ms), "TOP_CHANGE"
    return interval, "QUIET"


def next_emission(frames, venue, seq, predicate):
    """(due_ns, emission_ns, regime) after view frame ``seq``: due = publish + regime interval + slack; the
    emission is the first logged frame at/after due (so due -> emission is at most one publisher tick), or None
    while it is not logged yet. A frame of an unknown sequence -> (None, None, "UNKNOWN")."""
    f = frames.get(venue)
    pos = None if f is None else f["pos"].get(seq)
    if pos is None:
        return None, None, "UNKNOWN"
    interval_ms, regime = regime_interval_ms(frames, venue, seq, predicate)
    due = f["t"][pos] + int((interval_ms + predicate["slack_ms"]) * 1e6)
    for k in range(pos + 1, len(f["t"])):
        if f["t"][k] >= due:
            return due, f["t"][k], regime
    return due, None, regime


def caught_up(series, index, venue, cleared_ns, *, frames=None, predicate=None, hold_s=None, polls=None):
    """First valid apply and stable caught-up for one venue after ``cleared_ns`` (predicate v2).

    The core does not emit a BOOK snapshot per provider frame: it materializes one when the top changes (at
    most every 100 ms of provider time) or, quiet, every configured interval (1000 ms in production), each on the
    next provider frame. A view sample's age (sample time - frame time) therefore legitimately reaches interval +
    one publisher tick + transit, and an age limit of the transit budget alone fails a timely quiet view. v2
    separates the two things a caught-up consumer must show, per BOOK view sample (t, seq, approved):

    1. APPROVED, of a frame published at/after ``cleared_ns``;
    2. update transit: first sample showing ``seq`` (an upper bound of the view write time) - frame publish time
       <= transit_max_ms;
    3. update gap (closed): the view's previous update ``a`` -> this update ``seq``, on the frames' source time,
       is within the core's cadence: publish(seq) <= the emission after ``a`` = the first frame at/after
       publish(a) + regime interval(a) + slack, i.e. <= regime interval + one publisher tick + slack. Not checked
       when ``a`` precedes ``cleared_ns`` (that gap spans the fault; 1 and 2 bound the first update);
    4. update gap (open, catches a view stuck after the fault): sample time <= emission frame after ``seq`` +
       transit_max_ms (emission not logged yet: due + nominal tick + transit_max_ms).

    Caught up needs every sample of the following hold_s to satisfy 1-4, and samples past the hold (otherwise not
    yet decidable -> None). Parameters: ``caught_up_predicate`` (frozen in the run receipt).

    Sampler coverage: 2 and 3 judge ``seq`` from its first sample. When the sampler did not observe the view for
    longer than the top-change floor before that sample (its previous successful poll ``pt`` from ``polls``, else
    the previous logged row), an intermediate emission may have been applied and missed, and the first sample
    is late by up to the hole: a 3 failure, or a 2 failure whose last poll was still within the transit budget,
    is ``undersampled``, not a product rejection. No per-apply BOOK evidence is retained (the monitor logs no
    BOOK frames), so it is never resolved to a pass: it neither starts nor breaks a hold, and
    ``undersampled_in_hold`` > 0 makes the fault NOT_MEASURED (``evaluate_fault``). Before the final hold it can
    only delay the measured caught-up time. 4 (a stuck view) observes the view itself and is unaffected; a sample
    before its frame's publish time (an impossible apply) is ``future_publish``.
    """
    predicate = predicate or caught_up_predicate()
    frames = frames if frames is not None else frames_from_index(index)
    hold_ns = int((predicate["hold_s"] if hold_s is None else hold_s) * 1e9)
    transit_ns = int(predicate["transit_max_ms"] * 1e6)
    tick_ns = int(predicate["publisher_tick_ms"] * 1e6)
    floor_ns = int(predicate["top_change_min_interval_ms"] * 1e6)
    every = series.get(venue, ())
    polled = (polls or {}).get(venue, {})
    first, previous, hole, last_seq, last_t = {}, {}, {}, None, None
    for t, seq, _ok in every:
        if seq is None:
            continue
        if seq not in first:
            first[seq] = t
            previous[seq] = last_seq
            seen = polled.get(t, last_t)
            hole[seq] = None if seen is None else t - seen
        last_seq, last_t = seq, t
    emission = {}

    def undersampled(seq):
        return hole[seq] is not None and hole[seq] > floor_ns

    def after(seq):
        if seq not in emission:
            emission[seq] = next_emission(frames, venue, seq, predicate)
        return emission[seq]

    def reason(row):
        t, seq, ok = row
        published = index.get((venue, seq))
        if not ok:
            return "not_approved"
        if published is None or published < cleared_ns:
            return "no_post_clear_frame"
        if first[seq] < published:
            return "future_publish"
        if first[seq] - published > transit_ns:
            within = first[seq] - hole[seq] - published <= transit_ns if undersampled(seq) else False
            return "undersampled" if within else "update_transit"
        prior = previous.get(seq)
        prior_published = index.get((venue, prior)) if prior is not None else None
        if prior_published is not None and prior_published >= cleared_ns:
            _due, emitted, _regime = after(prior)
            if emitted is not None and published > emitted:
                return "undersampled" if undersampled(seq) else "update_gap"
        due, emitted, _regime = after(seq)
        if due is not None and t > (emitted if emitted is not None else due + tick_ns) + transit_ns:
            return "open_gap"
        return None

    rows = [r for r in every if r[0] >= cleared_ns]
    first_valid = None
    for t, seq, ok in rows:
        published = index.get((venue, seq))
        if ok and published is not None and published >= cleared_ns:
            first_valid = t
            break
    rejected = {}
    i = 0
    while i < len(rows):
        why = reason(rows[i])
        if why is not None:
            rejected[why] = rejected.get(why, 0) + 1
            i += 1
            continue
        start, j, broken, in_hold = rows[i][0], i, None, 0
        while j < len(rows) and rows[j][0] <= start + hold_ns:
            why = reason(rows[j])
            if why == "undersampled":
                rejected[why] = rejected.get(why, 0) + 1
                in_hold += 1
            elif why is not None:
                rejected[why] = rejected.get(why, 0) + 1
                broken = j
                break
            j += 1
        if broken is not None:
            i = broken + 1
            continue
        if j >= len(rows):
            break
        return {"first_valid_apply_ns": first_valid, "caught_up_ns": start, "rejected_samples": rejected,
                "undersampled_in_hold": in_hold}
    return {"first_valid_apply_ns": first_valid, "caught_up_ns": None, "rejected_samples": rejected,
            "undersampled_in_hold": 0}


def first_seen(series, venue):
    seen = {}
    for t, seq, ok in series.get(venue, ()):
        if seq is not None and ok and seq not in seen:
            seen[seq] = t
    return seen


def transit(series, index, venue, start_ns, end_ns):
    """ms from publish to the first APPROVED sample showing each frame, first seen in the window."""
    out = []
    for seq, t in first_seen(series, venue).items():
        published = index.get((venue, seq))
        if published is not None and start_ns <= t < end_ns:
            out.append((t - published) / 1e6)
    return out


def quantile(sorted_values, q):
    if not sorted_values:
        return None
    position = (len(sorted_values) - 1) * q
    low, high = math.floor(position), math.ceil(position)
    if low == high:
        return sorted_values[low]
    return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * (position - low)


def g1_stats(values):
    values = sorted(values)
    n = len(values)
    stats = {"n": n, "p50_ms": quantile(values, .5), "p95_ms": quantile(values, .95),
             "p99_ms": quantile(values, .99) if n >= GATE["p99_min_n"] else None,
             "max_ms": values[-1] if values else None}
    stats["pass"] = bool(n and stats["p95_ms"] <= GATE["p95_ms"] and stats["max_ms"] < GATE["max_ms"]
                         and (stats["p99_ms"] is None or stats["p99_ms"] <= GATE["p99_ms"]))
    if n and stats["p99_ms"] is None:
        stats["note"] = "p99 not reported (n < 100)"
    return {k: (round(v, 3) if isinstance(v, float) else v) for k, v in stats.items()}


def md_publish_index(rows):
    """(feed, venue, id) -> the publisher's row of one TRADE/QUOTE/BAR source event (k == pub_md)."""
    return {(r["feed"], r["venue"], str(r["id"])): r for r in rows if r.get("k") == "pub_md"}


def md_first_seen(rows):
    """(feed, venue, id) -> first receipt ns of the clone's applied notification (k == ev)."""
    seen = {}
    for r in rows:
        if r.get("k") == "ev" and r.get("id") is not None:
            key = (r["feed"], r["venue"], str(r["id"]))
            if key not in seen or r["t"] < seen[key]:
                seen[key] = r["t"]
    return seen


def md_reference_ns(row):
    """Transit origin: the bar close for BAR (open + 1 m), else the publish stamp (= the frame's event time)."""
    return (row["close_ms"] * 1_000_000) if row["feed"] == "BAR" else row["t"]


def md_sample(index, seen, feed, venue, start_ns, end_ns, covered_ns):
    """New-event transit (ms) of every non-seed event of one feed/venue whose reference lies in [start, end).

    An event never applied is lost once the sampler covered reference + LOST_AFTER_S (QUOTE / MARK: unseen,
    a latest-state feed may skip a superseded value), else pending: not yet decidable, so the cell cannot PASS
    (``md_stats``: pending > 0 is NOT_MEASURED, OF-G12-2).
    """
    values, counts = [], {"lost": 0, "unseen": 0, "pending": 0}
    for key, row in index.items():
        if key[0] != feed or key[1] != venue or row.get("seed"):
            continue
        reference = md_reference_ns(row)
        if not start_ns <= reference < end_ns:
            continue
        applied = seen.get(key)
        if applied is not None:
            values.append((applied - reference) / 1e6)
        elif covered_ns >= reference + LOST_AFTER_S[feed] * 1_000_000_000:
            counts["unseen" if feed in LATEST_STATE_FEEDS else "lost"] += 1
        else:
            counts["pending"] += 1
    return values, counts


def md_stats(feed, values, counts=None):
    """G2/G3 verdict of one feed/venue: PASS, FAIL, or NOT_MEASURED below the minimum sample count or while any
    event of the window is still pending (undecidable: absent observations never count toward a PASS, OF-G12-2)."""
    gate = GATE_G2_G3[feed]
    values = sorted(values)
    n = len(values)
    stats = {"n": n, "p50_ms": quantile(values, .5), "p95_ms": quantile(values, .95),
             "p99_ms": quantile(values, .99) if n >= 100 else None, "max_ms": values[-1] if values else None,
             **(counts or {"lost": 0, "unseen": 0, "pending": 0})}
    if stats["lost"]:
        stats["status"] = "FAIL"
        stats["note"] = "events never applied"
    elif stats["pending"]:
        stats["status"] = "NOT_MEASURED"
        stats["note"] = "events pending (sampler did not cover reference + LOST_AFTER_S)"
    elif n < gate["min_n"]:
        stats["status"] = "NOT_MEASURED"
        stats["note"] = f"n < {gate['min_n']}"
    else:
        stats["status"] = ("PASS" if stats["p95_ms"] <= gate["p95_ms"] and stats["max_ms"] < gate["max_ms"]
                           else "FAIL")
    stats["pass"] = stats["status"] == "PASS"
    return {k: (round(v, 3) if isinstance(v, float) else v) for k, v in stats.items()}


def part_status(stats):
    """One gate part over its cells: any FAIL -> FAIL, else any NOT_MEASURED -> NOT_MEASURED, else PASS."""
    statuses = [s["status"] for s in stats]
    if not statuses:
        return "NOT_MEASURED"
    return "FAIL" if "FAIL" in statuses else "NOT_MEASURED" if "NOT_MEASURED" in statuses else "PASS"


def md_window(publish_rows, sampler_rows, start_ns, end_ns):
    """G2 (TRADE, QUOTE, MARK_INDEX_PRICE) and G3 (BAR 1m) over one window, plus each part's status."""
    index, seen = md_publish_index(publish_rows), md_first_seen(sampler_rows)
    covered = max((r["t"] for r in sampler_rows if r.get("k") in ("view", "ev")), default=0)
    cell = lambda feed, venue: md_stats(feed, *md_sample(index, seen, feed, venue, start_ns, end_ns, covered))
    g2 = {feed: {venue: cell(feed, venue) for venue in VENUES} for feed in G2_FEEDS}
    g3 = {"BAR_" + BAR_INTERVAL: {venue: cell("BAR", venue) for venue in VENUES}}
    return {"g2": g2, "g3": g3,
            "g2_feed_status": {feed: part_status(list(cells.values())) for feed, cells in g2.items()},
            "g2_status": part_status([s for cells in g2.values() for s in cells.values()]),
            "g3_status": part_status([s for cells in g3.values() for s in cells.values()])}


# ---- OF-G12-2 integrity oracle (inheritance audit 2026-10-16): absent observations never count toward a PASS.
# Per fault, over [fault injected, end of its G1-G3 window] (high-water from every earlier observation of the run):
# - BOOK rewind: a BOOK view sequence below the high-water of every earlier view of its venue (post-to-post, the
#   G11 v2 rule). The publisher's sequence is monotonic across generations (``publish``: a resync bumps the
#   generation, never the sequence), so one domain per venue is exact here.
# - applied-notification rewind: a TRADE / QUOTE / MARK notification whose event time is below the high-water of
#   earlier notifications of its venue/feed (each notification is the TS cache write of that value:
#   services/market_data/cache_projector.py _execution_operations setex + publish), i.e. the cache went back.
#   BAR is report-only: project_bars re-writes a retained closed bar unconditionally (latest-state projection).
# - duplicate applied: a TRADE native id notified more than once (G10 LOSSLESS: 0 duplicates applied).
# - native typed counters: the clone heartbeat's pooled trade_integrity, judged by the G10 v2 sampler logic
#   (scripts/p184_g9_g10_writer_counters.summarize_g10_native: one unbroken activation stretch bracketing the
#   scope, typed and complete, else NOT_MEASURED; gaps / replay_expired / overflow / duplicates_applied /
#   offset_regressions > 0 FAIL).
# Missing coverage (no view sequence, no TRADE notification, a notification without identity or clock, a listener
# error inside the scope, native counters incomplete) is NOT_MEASURED. The whole part is REPORT-ONLY in the G12
# verdict (coordinator decision 2026-10-16): it is recorded per fault and never gates G12.
INTEGRITY_REWIND_FEEDS = ("TRADE", "QUOTE", "MARK")
INTEGRITY_REPORT_ONLY_FEEDS = ("BAR",)
NATIVE_HEARTBEAT_FIELDS = ("trade_integrity", "trade_integrity_scope", "worker_liveness")


def book_rewinds(sampler_rows):
    """venue -> every BOOK view whose sequence is below the high-water of all earlier views of that venue."""
    out, high = {venue: [] for venue in VENUES}, {}
    for r in sorted((r for r in sampler_rows if r.get("k") == "view" and type(r.get("seq")) is int),
                    key=lambda r: r["t"]):
        venue, top = r["venue"], high.get(r["venue"])
        if top is not None and r["seq"] < top:
            out.setdefault(venue, []).append({"t": r["t"], "before": top, "after": r["seq"]})
        high[venue] = r["seq"] if top is None else max(top, r["seq"])
    return out


def notification_integrity(sampler_rows):
    """(venue, feed) -> rewinds (event time below the earlier high-water), TRADE duplicates (native id notified
    again) and rows without identity/clock, in receipt order."""
    out, high, seen = {}, {}, set()
    for r in sorted((r for r in sampler_rows if r.get("k") == "ev"), key=lambda r: r["t"]):
        key = (r.get("venue"), r.get("feed"))
        cell = out.setdefault(key, {"rewinds": [], "duplicates": [], "unidentified": [], "rows": []})
        cell["rows"].append(r["t"])
        clock, ident = r.get("ev_ms"), r.get("id")
        if type(clock) is not int or ident is None:
            cell["unidentified"].append(r["t"])
            continue
        top = high.get(key)
        if top is not None and clock < top:
            cell["rewinds"].append({"t": r["t"], "before": top, "after": clock})
        high[key] = clock if top is None else max(top, clock)
        if key[1] == "TRADE":
            if (key, ident) in seen:
                cell["duplicates"].append({"t": r["t"], "id": ident})
            seen.add((key, ident))
    return out


def native_heartbeat_samples(sampler_rows, venue):
    """The clone's heartbeat rows as G10 v2 sampler samples (``heartbeat_sample`` validates, never coerces)."""
    from scripts.p184_g9_g10_writer_counters import heartbeat_sample
    samples = []
    for r in sampler_rows:
        if r.get("k") != "hb" or r.get("venue") != venue or type(r.get("t")) is not int:
            continue
        payload = None
        if r.get("heartbeat") is True:
            payload = {"status": r.get("status"), "last_seen_unix": r.get("last_seen_unix"),
                       "details": {k: r[k] for k in NATIVE_HEARTBEAT_FIELDS if k in r}}
        samples.append({"kind": "heartbeat", "at_ns": r["t"], "key_tail": r.get("key_tail"),
                        "sample": heartbeat_sample(payload, r["t"] / 1e9)})
    return samples


def native_integrity(sampler_rows, venue, from_ns, to_ns):
    """G10 v2 native typed counters of one clone over [from, to]: PASS / FAIL / NOT_MEASURED (incomplete)."""
    from scripts.p184_g9_g10_writer_counters import summarize_g10_native
    summary = summarize_g10_native(native_heartbeat_samples(sampler_rows, venue), from_ns=from_ns, to_ns=to_ns)
    failed = sum((summary.get("failures") or {}).values()) + sum((summary.get("blocking") or {}).values())
    summary["status"] = "FAIL" if failed else "PASS" if summary["complete"] else "NOT_MEASURED"
    return summary


def integrity_window(sampler_rows, from_ns, to_ns):
    """OF-G12-2 integrity of one fault scope (see the block comment above): per venue and overall status."""
    in_scope = lambda t: from_ns <= t <= to_ns  # noqa: E731
    books, notes = book_rewinds(sampler_rows), notification_integrity(sampler_rows)
    out = {"scope_ns": [from_ns, to_ns], "venues": {}}
    for venue in VENUES:
        cell = {"book_rewinds": [x for x in books.get(venue, ()) if in_scope(x["t"])],
                "book_views": sum(1 for r in sampler_rows if r.get("k") == "view" and r.get("venue") == venue
                                  and type(r.get("seq")) is int and in_scope(r["t"])),
                "listener_errors": sum(1 for r in sampler_rows if r.get("k") == "ev_error"
                                       and r.get("venue") == venue and in_scope(r.get("t", -1))),
                "rewinds": {}, "report_only_rewinds": {}, "unidentified": {}}
        for feed in MD_FEEDS:
            n = notes.get((venue, feed)) or {"rewinds": [], "duplicates": [], "unidentified": [], "rows": []}
            target = "rewinds" if feed in INTEGRITY_REWIND_FEEDS else "report_only_rewinds"
            cell[target][feed] = sum(1 for x in n["rewinds"] if in_scope(x["t"]))
            cell["unidentified"][feed] = sum(1 for t in n["unidentified"] if in_scope(t))
            if feed == "TRADE":
                cell["trade_duplicates"] = [x for x in n["duplicates"] if in_scope(x["t"])][:20]
                cell["trade_duplicates_n"] = sum(1 for x in n["duplicates"] if in_scope(x["t"]))
                cell["trade_notifications"] = sum(1 for t in n["rows"] if in_scope(t))
        native = native_integrity(sampler_rows, venue, from_ns, to_ns)
        cell["native"] = {k: native.get(k) for k in ("status", "complete", "failures", "blocking", "report_only",
                                                     "breaks", "samples_in_bracket", "present")}
        failed = []
        if cell["book_rewinds"]:
            failed.append("BOOK rewind")
        failed += [f"{feed} rewind" for feed, n in cell["rewinds"].items() if n]
        if cell["trade_duplicates_n"]:
            failed.append("TRADE duplicate applied")
        if native["status"] == "FAIL":
            failed.append("native typed counters")
        unmeasured = []
        if not cell["book_views"]:
            unmeasured.append("no BOOK view sequence")
        if not cell["trade_notifications"]:
            unmeasured.append("no TRADE notification")
        if cell["listener_errors"]:
            unmeasured.append("notification listener errors")
        if any(cell["unidentified"][feed] for feed in (*INTEGRITY_REWIND_FEEDS,)):
            unmeasured.append("notifications without identity or clock")
        if native["status"] == "NOT_MEASURED":
            unmeasured.append("native typed counters incomplete")
        cell["status"] = "FAIL" if failed else "NOT_MEASURED" if unmeasured else "PASS"
        cell["reasons"] = failed or unmeasured
        out["venues"][venue] = cell
    out["status"] = part_status(list(out["venues"].values()))
    return out


def verdict_undecided(verdict):
    """What keeps a fault verdict undecidable right after its window (the run waits for these to resolve):
    G2/G3 events still pending, or no heartbeat sample yet at/after the end of the integrity scope."""
    out = [f"{part}.{feed}.{venue} pending {cell['pending']}"
           for part in ("g2", "g3") for feed, cells in (verdict.get(part) or {}).items()
           for venue, cell in cells.items() if cell.get("pending")]
    for venue, cell in ((verdict.get("integrity") or {}).get("venues") or {}).items():
        if any("at or after the window end" in str(b) for b in (cell.get("native") or {}).get("breaks") or ()):
            out.append(f"integrity.{venue} no heartbeat after the scope")
    return out


def md_missing(sampler_rows):
    """FEED@VENUE never applied by a clone yet (start-up gate before the baseline)."""
    seen = {(key[0], key[1]) for key in md_first_seen(sampler_rows)}
    return [f"{feed}@{venue}" for venue in VENUES for feed in MD_FEEDS if (feed, venue) not in seen]


def publisher_md_summary(publish_rows):
    """Per feed: published source events (seed bars apart), broker ACKs and delivery errors."""
    out = {feed: {"published": 0, "seed": 0, "acked": 0, "delivery_errors": 0} for feed in MD_FEEDS}
    for r in publish_rows:
        feed = out.get(r.get("feed"))
        if feed is None:
            continue
        if r.get("k") == "pub_md":
            feed["seed" if r.get("seed") else "published"] += 1
        elif r.get("k") == "ack_md":
            feed["delivery_errors" if "error" in r else "acked"] += 1
    return out


def exit_states(event_rows):
    """role -> exit record of every owned container life, from the daemon's container events.

    ``--rm`` removes a container as it exits, so ``docker inspect`` has nothing left to read;
    the die event carries exitCode, an oom event marks the OOM kill of that life, a kill event
    the signal that was sent (docker stop / teardown). Attributes are the daemon's; never inferred.
    """
    states = {}
    pending = {}
    for row in sorted(event_rows, key=lambda r: r.get("timeNano") or 0):
        attrs = (row.get("Actor") or {}).get("Attributes") or {}
        match = re.fullmatch(NS_PATTERN + r"-(.+)", str(attrs.get("name", "")))
        if not match:
            continue
        role, action = match.group(1), row.get("Action") or row.get("status")
        life = pending.setdefault(role, {"oom_killed": False, "kill_signals": []})
        if action == "oom":
            life["oom_killed"] = True
        elif action == "kill":
            life["kill_signals"].append(attrs.get("signal"))
        elif action == "die":
            code = attrs.get("exitCode")
            life.update(exit_code=int(code) if str(code).lstrip("-").isdigit() else None,
                        died_ns=row.get("timeNano"),
                        oom_killed=life["oom_killed"] or str(attrs.get("oomKilled", "")).lower() == "true")
            states.setdefault(role, {"lives": []})["lives"].append(pending.pop(role))
    for role, value in states.items():
        value.update({k: value["lives"][-1][k] for k in ("exit_code", "oom_killed", "died_ns")})
    return states


def monitor_valid(monitor_rows):
    """Monitor samples that observed: a ``sample_error`` (crashed / timed-out sample) or the end marker is
    never evidence of anything."""
    return [row for row in monitor_rows if not row.get("k")]


def monitor_max_gap_s(monitor_rows, start_ns, end_ns):
    """Longest interval (s) in [start, end] without a valid monitor sample, the edges included."""
    points = [start_ns, *sorted(r["t"] for r in monitor_valid(monitor_rows) if start_ns < r["t"] < end_ns), end_ns]
    return round(max(b - a for a, b in zip(points, points[1:])) / 1e9, 3) if end_ns > start_ns else 0.0


def cleared_at(kind, monitor_rows, restart_ns, node=None):
    """Fault-cleared time from the monitor (packet section 5)."""
    for row in monitor_rows:
        if row.get("t", 0) < restart_ns or row.get("k"):
            continue
        if kind == "broker" and node in ((row.get("cluster") or {}).get("broker_ids") or ()):
            return row["t"]
        if kind == "projector" and ((row.get("groups") or {}).get("kn-projector-v3-a") or {}).get("members", 0) >= 1:
            return row["t"]
        if kind == "stream" and (row.get("ports") or {}).get("8210") is True:
            return row["t"]
    return None


def first_monitor(monitor_rows, start_ns, predicate):
    for row in monitor_rows:
        if row.get("t", 0) >= start_ns and not row.get("k") and predicate(row):
            return row["t"]
    return None


def evaluate_fault(fault, publish_rows, sampler_rows, monitor_rows, *, window_s=GATE["window_s"], predicate=None):
    index, series = publish_index(publish_rows), view_series(sampler_rows)
    frames, predicate = book_frames(publish_rows), predicate or caught_up_predicate()
    polls, hold_ns = view_polls(sampler_rows), int(predicate["hold_s"] * 1e9)
    cleared = cleared_at(fault["kind"], monitor_rows, fault["restart_ns"], fault.get("node"))
    result = {"fault": fault["name"], "kind": fault["kind"], "node": fault.get("node"),
              "fault_injected_ns": fault["injected_ns"], "restart_issued_ns": fault["restart_ns"],
              "fault_cleared_ns": cleared, "caught_up_predicate": predicate["version"], "venues": {}, "pass": False}
    if cleared is None:
        result["reason"] = "FAULT_NOT_CLEARED"
        return result
    ends = []
    for venue in VENUES:
        state = caught_up(series, index, venue, cleared, frames=frames, predicate=predicate, polls=polls)
        state["caught_up_s"] = (None if state["caught_up_ns"] is None
                                else round((state["caught_up_ns"] - cleared) / 1e9, 3))
        rows = [r for r in series.get(venue, ()) if fault["injected_ns"] <= r[0] <= (state["caught_up_ns"] or r[0])]
        ages = [(t - index[(venue, s)]) / 1e6 for t, s, ok in rows if (venue, s) in index]
        state["max_view_age_ms_injected_to_caught_up"] = round(max(ages), 3) if ages else None
        state["not_approved_samples_injected_to_caught_up"] = sum(1 for _, _, ok in rows if not ok)
        # Sampler coverage, cleared -> caught up + hold (not caught up: -> the last logged row of the run).
        end = (state["caught_up_ns"] + hold_ns if state["caught_up_ns"] is not None
               else max((r.get("t", 0) for r in (*publish_rows, *sampler_rows, *monitor_rows)), default=cleared))
        state["sampler_max_gap_s"] = sampler_max_gap_s(series, venue, cleared, end)
        result["venues"][venue] = state
        ends.append(state["caught_up_ns"])
    # A sampler hole over SAMPLER_MAX_GAP_S, or a caught-up hold holding an undersampled sample, leaves the
    # caught-up evidence incomplete: NOT_MEASURED, never a PASS nor a product FAIL.
    holes = any(s["sampler_max_gap_s"] > SAMPLER_MAX_GAP_S for s in result["venues"].values())
    unresolved = any(s["undersampled_in_hold"] for s in result["venues"].values())
    sampled = "NOT_MEASURED" if holes or unresolved else "PASS"
    result["sampler"] = sampled
    if any(end is None for end in ends):
        result["reason"] = "SAMPLER_GAP" if holes else "NOT_CAUGHT_UP"
        return result
    caught = max(ends)
    result["consumers_caught_up_ns"] = caught
    result["caught_up_s"] = round((caught - cleared) / 1e9, 3)
    window = (caught, caught + int(window_s * 1e9))
    result["g1_window_ns"] = list(window)
    result["g1"] = {venue: g1_stats(transit(series, index, venue, *window)) for venue in VENUES}
    last = max((r["t"] for r in sampler_rows if r.get("k") == "view"), default=0)
    complete = last >= window[1]
    result["g1_window_complete"] = complete
    # Committed offset vs LSO keeps 1 for the transaction's commit marker in steady state
    # (run 2026-10-10: lag 1 in 921 of 1,257 samples, 0 only twice), so "steady" is <= 1.
    result["projector_lag_steady_ns"] = first_monitor(
        monitor_rows, cleared, lambda r: r.get("projector_lag") is not None and r["projector_lag"] <= 1)
    result["isr_restored_ns"] = first_monitor(monitor_rows, cleared, lambda r: r.get("under_replicated") == 0)
    result["g12_caught_up_pass"] = result["caught_up_s"] <= GATE["caught_up_s"]
    # The fault-cleared time is the monitor's: with no valid sample for longer than MONITOR_MAX_GAP_S between the
    # restart and catch-up it is uncertain by that much, so the fault is NOT_MEASURED (never a PASS).
    result["monitor_max_gap_s"] = monitor_max_gap_s(monitor_rows, fault["restart_ns"], caught)
    result["monitor_sample_errors"] = [
        {k: r.get(k) for k in ("t", "error", "phase")} for r in monitor_rows
        if r.get("k") == "sample_error" and fault["injected_ns"] <= r["t"] <= caught]
    monitored = "PASS" if result["monitor_max_gap_s"] <= MONITOR_MAX_GAP_S else "NOT_MEASURED"
    if monitored != "PASS":
        result["reason"] = "MONITOR_GAP"
    elif sampled != "PASS":
        result["reason"] = "SAMPLER_GAP" if holes else "SAMPLER_UNDERSAMPLED"
    # G2/G3 over the same window as G1 (new events whose publish time / bar close lies in it).
    result.update(md_window(publish_rows, sampler_rows, *window))
    # OF-G12-2: rewinds, duplicates applied, native typed counters, from injection to the window end. REPORT-ONLY
    # (coordinator decision 2026-10-16): the frozen G12 row is caught up + G1-G3 + the four timestamps; lossless
    # TRADE is gated by G10. Neither a finding nor missing coverage here changes the G12 verdict.
    result["integrity"] = {**integrity_window(sampler_rows, fault["injected_ns"], window[1]), "gating": False}
    result["parts"] = {"caught_up": result["g12_caught_up_pass"], "window_complete": complete,
                       "g1": "PASS" if all(result["g1"][v]["pass"] for v in VENUES) else "FAIL",
                       "g2": result["g2_status"], "g2_feeds": result["g2_feed_status"],
                       "g3": result["g3_status"], "integrity_report_only": result["integrity"]["status"],
                       "monitor": monitored, "sampler": sampled}
    result["pass"] = (monitored == "PASS" and sampled == "PASS" and result["g12_caught_up_pass"] and complete
                      and all(result["g1"][v]["pass"] for v in VENUES)
                      and result["g2_status"] == "PASS" and result["g3_status"] == "PASS")
    return result


def load_predicate(evidence):
    """The run's frozen caught-up predicate (``evidence/caught-up-predicate.json``), else the default parameters
    marked ``frozen: False`` (a run sealed before v2: its verdict is a re-analysis, not the frozen gate)."""
    path = Path(evidence) / PREDICATE_FILE
    if path.is_file():
        predicate = json.loads(path.read_text())
        if predicate.get("version") != CAUGHT_UP_VERSION:
            raise ValueError(f"run predicate {predicate.get('version')!r} is not {CAUGHT_UP_VERSION}")
        return {**predicate, "frozen": True}
    return {**caught_up_predicate(), "frozen": False}


def publisher_book_summary(publish_rows):
    """Apparatus check: per venue, BOOK publish interval and schedule lateness (ms) as the publisher logged them."""
    out = {}
    for venue, f in book_frames(publish_rows).items():
        gaps = sorted((b - a) / 1e6 for a, b in zip(f["t"], f["t"][1:]))
        late = sorted(r["late_ms"] for r in publish_rows
                      if r.get("k") == "pub" and r.get("venue") == venue and r.get("late_ms") is not None)
        tops = {top for top in f["top"] if top is not None}
        out[venue] = {"frames": len(f["t"]), "distinct_tops": len(tops),
                      "interval_ms": {"p50": quantile(gaps, .5), "p99": quantile(gaps, .99),
                                      "max": gaps[-1] if gaps else None},
                      "late_ms": {"p99": quantile(late, .99), "max": late[-1] if late else None}}
    return out


def evaluate(evidence):
    evidence = Path(evidence)
    predicate = load_predicate(evidence)
    publish_rows = load_jsonl(evidence / "publish.jsonl")
    sampler_rows = load_jsonl(evidence / "sampler.jsonl")
    monitor_rows = sorted(load_jsonl(evidence / "monitor.jsonl"), key=lambda r: r.get("t", 0))
    faults = [r for r in load_jsonl(evidence / "faults.jsonl") if r.get("k") == "fault"]
    index, series = publish_index(publish_rows), view_series(sampler_rows)
    out = {"schema": VERDICT_SCHEMA, "provenance": "TEST_ONLY", "level": "ISOLATED_FAULT",
           "gate": GATE, "gate_g2_g3": GATE_G2_G3, "caught_up_predicate": predicate, "faults": [],
           "g2": "per fault: TRADE, QUOTE and MARK (MARK_INDEX_PRICE) new-event transit "
                 "(publish -> first applied notification)",
           "g3": "per fault: BAR 1m from close (open + 60 s -> first applied closed-bar notification)"}
    base = [r for r in load_jsonl(evidence / "faults.jsonl") if r.get("k") == "baseline"]
    if base:
        window = (base[0]["start_ns"], base[0]["end_ns"])
        out["baseline_g1"] = {v: g1_stats(transit(series, index, v, *window)) for v in VENUES}
        out["baseline_g2_g3"] = md_window(publish_rows, sampler_rows, *window)
    for fault in faults:
        out["faults"].append(evaluate_fault(fault, publish_rows, sampler_rows, monitor_rows, predicate=predicate))
    acks = [r for r in publish_rows if r.get("k") == "ack"]
    out["publisher"] = {"frames": sum(1 for r in publish_rows if r.get("k") == "pub"),
                        "acked": sum(1 for r in acks if "error" not in r),
                        "delivery_errors": sum(1 for r in acks if "error" in r),
                        "resyncs": sum(1 for r in publish_rows if r.get("k") == "resync"),
                        "resyncs_requested": sum(1 for r in publish_rows if r.get("k") == "resync"
                                                 and r.get("cause") == L2_RESNAPSHOT_REQUIRED_V1),
                        "resync_requests_refused": sum(1 for r in publish_rows
                                                       if r.get("k") == "resync_request_refused"),
                        "book": publisher_book_summary(publish_rows),
                        "market_data": publisher_md_summary(publish_rows)}
    out["exit_states"] = exit_states(load_jsonl(evidence / "container-events.jsonl"))
    errors = [r for r in monitor_rows if r.get("k") == "sample_error"]
    out["monitor"] = {"samples": len(monitor_valid(monitor_rows)), "sample_errors": len(errors),
                      "errors": sorted({(r.get("error"), r.get("phase")) for r in errors}, key=str),
                      "max_gap_rule_s": MONITOR_MAX_GAP_S}
    out["sampler"] = {"max_gap_rule_s": SAMPLER_MAX_GAP_S}
    precheck = evidence / WRITER_PRECHECK_FILE
    out["writer_precheck"] = json.loads(precheck.read_text()) if precheck.is_file() else None
    # OF-G12-1: only a run whose caught-up predicate v2 was frozen before its data can PASS; a re-analysis of a run
    # sealed without it (predicate v1 era) is reported, never credited.
    frozen = predicate.get("frozen") is True and predicate.get("version") == CAUGHT_UP_VERSION
    out["credit"] = ("CREDITABLE_IF_COMPLETED" if frozen else
                     "NOT_CREDITABLE: caught-up predicate v2 not frozen in this run (re-analysis only)")
    out["pass"] = (frozen and bool(out["faults"]) and len(out["faults"]) == 3
                   and all(f["pass"] for f in out["faults"]))
    return out


def production_effective_problems(config, current_effective=None):
    """Why the run's bound production observations do not support credit (empty = they do): the snapshot hash, the
    before AND after observations present, error-free, consistent with the profile and hash-equal to the snapshot,
    and (at credit time) the current snapshot's observation hash-equal too (Codex K3)."""
    effective = config.get("production_effective")
    profile = config.get("kafka_profile")
    if not isinstance(effective, dict) or not effective.get("parity_sha256") or not isinstance(profile, dict):
        return ["no production effective Kafka observation bound to the run"]
    problems = []
    for phase in ("before", "after"):
        observed = effective.get(phase)
        if not isinstance(observed, dict) or "error" in observed or not observed.get("sha256"):
            problems.append(f"production effective Kafka config not observed {phase} the run: "
                            f"{(observed or {}).get('error') if isinstance(observed, dict) else None}")
            continue
        problems += [f"{phase}: {p}" for p in effective_problems(observed, profile)]
        if observed.get("sha256") != effective["parity_sha256"]:
            problems.append(f"production effective Kafka config drifted ({phase} the run != parity snapshot)")
    if current_effective is not None and current_effective.get("sha256") != effective["parity_sha256"]:
        problems.append("production effective Kafka config at credit time != the run's parity snapshot")
    return problems


def g12_receipt_creditable(verdict, current_kafka=None, current_effective=None):
    """(creditable, reason) of a saved G12 ``verdict.json`` (OF-G12-1/2): only a COMPLETED PASS of this oracle
    (schema VERDICT_SCHEMA: pending blocks; the integrity part is report-only) under a frozen caught-up predicate v2,
    whose brokers mirrored a production Kafka profile (``production_parity`` MATCH, coordinator 2026-10-16).
    ``current_kafka``: the production profile at credit time (a fresh ``--parity-snapshot``); when given, the run's
    profile must equal it, so a run on a superseded config (e.g. rebalance=true before decision B) is not credited
    as the final G12. The EFFECTIVE production configuration bound to the run (``production_effective``: hashed
    describe/inspect observations before and after the faults) must be present, consistent and undrifted, also
    against ``current_effective`` (the credit-time snapshot's observation) when given - equal constants alone are not
    evidence (Codex K3). Every earlier receipt (verdict v1/v2, predicate v1, a re-analysis) is not credited, whatever
    its ``pass`` says."""
    if not isinstance(verdict, dict):
        return False, "not a verdict object"
    if verdict.get("schema") != VERDICT_SCHEMA:
        return False, f"verdict schema {verdict.get('schema')!r} is not {VERDICT_SCHEMA}"
    predicate = verdict.get("caught_up_predicate") or {}
    if predicate.get("version") != CAUGHT_UP_VERSION or predicate.get("frozen") is not True:
        return False, f"caught-up predicate {predicate.get('version')!r} frozen={predicate.get('frozen')!r}"
    if verdict.get("status") != "COMPLETED":
        return False, f"run status {verdict.get('status')!r}"
    faults = verdict.get("faults") or []
    if len(faults) != 3:
        return False, "three faults required"
    if verdict.get("pass") is not True:
        return False, "verdict not PASS"
    config = verdict.get("broker_config") or {}
    if config.get("production_parity") != "MATCH":
        return False, f"production Kafka parity {config.get('production_parity')!r}, not MATCH"
    if current_kafka is not None and config.get("kafka_profile") != validate_kafka_profile(current_kafka):
        return False, "run Kafka profile is not the current production profile (superseded config)"
    problems = production_effective_problems(config, current_effective)
    if problems:
        return False, "; ".join(problems[:4])
    problems = writer_lifecycle_problems(verdict.get("writer_precheck"))
    if problems:
        return False, "; ".join(problems[:4])
    return True, "PASS"


def writer_lifecycle_problems(precheck):
    """Why the WriterClones did not run the deployed writer through the production lifecycle (empty = they did):
    per venue the deployed-env precheck ok, a clean bootstrap exit with native checkpoints, the post-bootstrap strict
    argv equal to the deployed env, and the strict-resume start VERIFIED before the baseline."""
    problems = []
    for venue in VENUES:
        cell = (precheck or {}).get(venue) or {}
        lifecycle = cell.get("lifecycle") or {}
        if cell.get("ok") is not True:
            problems.append(f"writer {venue} precheck not ok")
        if (lifecycle.get("bootstrap_exit") or {}).get("exit_code") != 0 or not lifecycle.get("native_checkpoints"):
            problems.append(f"writer {venue} bootstrap not cleanly checkpointed")
        if (lifecycle.get("strict_precheck") or {}).get("ok") is not True:
            problems.append(f"writer {venue} strict env not the deployed env")
        if lifecycle.get("strict_resume") != "VERIFIED":
            problems.append(f"writer {venue} strict-resume start {lifecycle.get('strict_resume')!r}")
    return problems


# ---------------------------------------------------------------- in-container modes

def _packet(work):
    work = Path(work)
    packet = json.loads((work / "packet.json").read_text())
    ns = namespace(packet["namespace"])
    if os.environ.get("P184_G12_NAMESPACE") != ns or packet.get("bootstrap") != BOOTSTRAP:
        raise ValueError("G12 namespace attestation and isolated bootstrap required")
    check_sealed_images(work, os.environ.get("P184_G12_IMAGES"))
    return packet


def check_sealed_images(work, attested):
    """In-container: the parent's passed image set must equal the run's sealed set."""
    handoff = Path(work) / "handoff.json"
    sealed = json.loads(handoff.read_text()).get("g12_images") if handoff.is_file() else None
    if sealed is None and attested is None:
        return None  # a run sealed before the image set was a parameter
    if sealed is None or attested is None or resolve_image_set(json.loads(attested)) != resolve_image_set(sealed):
        raise ValueError("G12 image set attestation does not match the sealed run image set")
    return sealed


def _security(packet):
    from scripts.p184_dr08_isolated import kafka_security
    return kafka_security(packet)


def create_topics(work):
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import TopicCollection
    packet = _packet(work)
    admin = AdminClient(_security(packet))
    deadline = time.monotonic() + 120
    while True:
        try:
            if len(admin.describe_cluster(request_timeout=3).result(timeout=4).nodes) == len(BROKERS):
                break
        except Exception:  # noqa: BLE001 - not ready yet; the deadline decides
            pass
        if time.monotonic() >= deadline:
            raise TimeoutError("three-broker cluster readiness")
        time.sleep(1)
    profile = handoff_kafka_profile(work)
    wanted = topics(packet["namespace"], core_group(work)["raw_partitions"],
                    latest=None if profile is None else profile["state_topics"]["latest"])
    futures = admin.create_topics([NewTopic(name, parts, len(BROKERS), config=dict(config))
                                   for name, (parts, config) in wanted.items()], request_timeout=30)
    for future in futures.values():
        future.result(40)
    canonical = packet["namespace"] + ".canonical"
    described = describe_until_known(lambda: admin.describe_topics(TopicCollection([canonical]))[canonical]
                                     .result(20))
    packet["slice"]["topic_id"] = str(described.topic_id)
    (Path(work) / "packet.json").write_text(json.dumps(packet))
    return {"topics": sorted(wanted), "replication_factor": len(BROKERS), "min_isr": 2}


def describe_until_known(describe, seconds=30.0, pause=1.0):
    """A topic just created may not be in every broker's metadata yet (final-image run
    2026-10-10, attempt 1: UNKNOWN_TOPIC_OR_PART right after create); retry only that, bounded."""
    deadline = time.monotonic() + seconds
    while True:
        try:
            return describe()
        except Exception as error:  # noqa: BLE001 - only the propagation race is retried
            if "UNKNOWN_TOPIC_OR_PART" not in str(error) or time.monotonic() >= deadline:
                raise
        time.sleep(pause)


INTERNAL_TOPICS = ("__consumer_offsets", "__transaction_state")
# Decision C (internal-topic compaction) keys: a run is C evidence only when they are present and unchanged
# through the run (``internal_topic_parity``); otherwise its broker_config is B-only (Codex K1, 2026-10-16).
DECISION_C_KEYS = ("delete.retention.ms", "segment.ms")


def internal_topic_configs(work, admin=None):
    """In-container: the effective topic-level overrides (DYNAMIC_TOPIC_CONFIG) of the two internal topics and, under
    the key ``latest``, of this namespace's latest state topic (decision A' parity)."""
    from confluent_kafka.admin import AdminClient, ConfigResource, ConfigSource
    packet = _packet(work)
    admin = admin or AdminClient(_security(packet))
    out = {}
    for key, topic in (*((t, t) for t in INTERNAL_TOPICS), ("latest", packet["namespace"] + ".latest")):
        resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
        try:
            entries = admin.describe_configs([resource], request_timeout=20)[resource].result(30)
        except Exception as error:  # noqa: BLE001 - an absent internal topic is a recorded state
            out[key] = {"absent": True, "error": str(error)[:200]}
            continue
        overrides = {name: entry.value for name, entry in sorted(entries.items())
                     if entry.source == ConfigSource.DYNAMIC_TOPIC_CONFIG.value}
        out[key] = {"overrides": overrides, "sha256": hashlib.sha256(
            json.dumps(overrides, sort_keys=True).encode()).hexdigest()}
    return out


def handoff_kafka_profile(work):
    """The mirrored production Kafka profile sealed in handoff.json (None: no profile, e.g. core-group modes)."""
    handoff = Path(work) / "handoff.json"
    profile = json.loads(handoff.read_text()).get("kafka_profile") if handoff.is_file() else None
    return None if profile is None else validate_kafka_profile(profile)


def apply_internal_configs(work, seconds=180.0, pause=2.0):
    """In-container: set the sealed profile's decision-C keys on the two internal topics (incremental SET, exactly
    the profile keys; Kafka creates the topics lazily, so wait until both describe) and return the effective
    overrides (internal_topic_configs). Production applied C to running topics the same way (dynamic config)."""
    from confluent_kafka.admin import AdminClient, AlterConfigOpType, ConfigEntry, ConfigResource
    profile = handoff_kafka_profile(work)
    if profile is None:
        raise ValueError("no kafka_profile sealed in handoff.json")
    admin = AdminClient(_security(_packet(work)))
    deadline = time.monotonic() + seconds
    while True:
        seen = internal_topic_configs(work, admin)
        if all("overrides" in seen[t] for t in INTERNAL_TOPICS):
            break
        if time.monotonic() >= deadline:
            raise TimeoutError("internal topics not created: " + json.dumps(seen)[:300])
        time.sleep(pause)
    for topic in INTERNAL_TOPICS:
        keys = profile["internal_topics"][topic]
        if not keys:
            continue
        resource = ConfigResource(ConfigResource.Type.TOPIC, topic, incremental_configs=[
            ConfigEntry(k, v, incremental_operation=AlterConfigOpType.SET) for k, v in sorted(keys.items())])
        admin.incremental_alter_configs([resource], request_timeout=20)[resource].result(30)
    return internal_topic_configs(work, admin)


def internal_topic_parity(snapshots):
    """``snapshots``: phase -> internal_topic_configs(). Classifies the run's decision-C state."""
    phases = ("before", "after")  # other phases (``applied``) are recorded but do not classify
    if not set(phases) <= set(snapshots) or any(
            "overrides" not in (snapshots[p].get(t) or {}) for p in phases for t in INTERNAL_TOPICS):
        return "NOT_MEASURED"
    keys = {p: {t: {k: v for k, v in snapshots[p][t]["overrides"].items() if k in DECISION_C_KEYS}
                for t in INTERNAL_TOPICS} for p in phases}
    if keys["before"] != keys["after"]:
        return "CHANGED_DURING_RUN"
    return "C_PRESENT" if any(keys["before"].values()) else "C_ABSENT_B_ONLY"


class _Stop:
    def __init__(self):
        self.requested = False
        signal.signal(signal.SIGTERM, self._set)

    def _set(self, *_):
        self.requested = True


SYMBOLS = {"BINANCE": "ETHUSDT", "OKX": "ETH-USDT-SWAP"}  # the DR05 linear scopes' native symbols


def _md_frame(venue, feed, payload, at_ns, lane=None):
    return {"venue": venue, "native_symbol": SYMBOLS[venue], "feed": feed, "lane": lane or feed.lower(),
            "transport": "WEBSOCKET", "received_at_ns": at_ns, "test_provenance": True, "provenance": "TEST_ONLY",
            "adapter_version": TEST_ADAPTER, "payload": payload}


def _check_md(venue, at_ns):
    if venue not in SYMBOLS or type(at_ns) is not int or at_ns <= 0:
        raise ValueError("venue and positive integer timestamp required")
    return at_ns // 1_000_000


def trade_frame(venue, trade_id, at_ns):
    """One provider-shaped trade (Binance ``@trade`` / OKX ``trades``), event time = the stamp; deterministic."""
    ms = _check_md(venue, at_ns)
    if type(trade_id) is not int or trade_id <= 0:
        raise ValueError("positive integer trade id required")
    price = f"{60000 + (trade_id % 20) / 10:.1f}"
    if venue == "BINANCE":
        payload = {"e": "trade", "E": ms, "T": ms, "s": SYMBOLS[venue], "t": trade_id, "p": price,
                   "q": "0.010", "X": "MARKET", "m": trade_id % 2 == 0}
    else:
        payload = {"arg": {"channel": "trades", "instId": SYMBOLS[venue]}, "data": [{
            "instId": SYMBOLS[venue], "tradeId": str(trade_id), "px": price, "sz": "1",
            "side": "sell" if trade_id % 2 == 0 else "buy", "ts": str(ms)}]}
    return _md_frame(venue, "TRADE", payload, at_ns)


def quote_frame(venue, sequence, at_ns):
    """One top-of-book quote (Binance ``@bookTicker`` / OKX ``bbo-tbt``); every quote differs from the previous
    one (the bid size moves), so ON_CHANGE delivery has a change to deliver."""
    ms = _check_md(venue, at_ns)
    if type(sequence) is not int or sequence <= 0:
        raise ValueError("positive integer quote sequence required")
    size = f"{1 + (sequence % 97) / 100:.2f}"
    if venue == "BINANCE":
        payload = {"e": "bookTicker", "u": sequence, "s": SYMBOLS[venue], "b": "60000.0", "B": size,
                   "a": "60001.0", "A": "1.00", "T": ms, "E": ms}
    else:
        payload = {"arg": {"channel": "bbo-tbt", "instId": SYMBOLS[venue]}, "data": [{
            "asks": [["60001.0", "1", "0", "1"]], "bids": [["60000.0", size, "0", "1"]],
            "ts": str(ms), "seqId": sequence}]}
    return _md_frame(venue, "QUOTE", payload, at_ns)


def bar_frame(venue, open_ms, at_ns, trades=0):
    """One FINAL 1m bar (Binance closed kline x=true / OKX candle1m confirm=1), sent after its close."""
    ms = _check_md(venue, at_ns)
    if type(open_ms) is not int or open_ms <= 0 or open_ms % BAR_MS or ms < open_ms + BAR_MS:
        raise ValueError("a closed minute-aligned bar is required")
    if venue == "BINANCE":
        payload = {"e": "kline", "E": ms, "s": SYMBOLS[venue], "k": {
            "t": open_ms, "T": open_ms + BAR_MS - 1, "s": SYMBOLS[venue], "i": BAR_INTERVAL, "f": -1, "L": -1,
            "o": "60000.0", "c": "60001.0", "h": "60001.9", "l": "60000.0", "v": "1.000", "n": int(trades),
            "x": True, "q": "60000.50", "V": "0.500", "Q": "30000.25"}}
    else:
        payload = {"arg": {"channel": "candle" + BAR_INTERVAL, "instId": SYMBOLS[venue]}, "data": [[
            str(open_ms), "60000.0", "60001.9", "60000.0", "60001.0", "10", "0.1", "6000.05", "1"]]}
    return _md_frame(venue, "BAR", payload, at_ns)


INDEX_SYMBOLS = {"OKX": "ETH-USDT"}  # default for frames only; the sealed binding's physical symbol is used at run


def mark_frames(venue, sequence, at_ns, index_symbol=None):
    """One MARK/INDEX observation: Binance one ``@markPrice@1s`` frame (mark and index together, component BOTH);
    OKX a ``mark-price`` frame and an ``index-tickers`` frame of the index symbol, same ms (the core pairs them on
    one generation). Each frame's ``feed`` is its component lane (MARK / INDEX)."""
    ms = _check_md(venue, at_ns)
    if type(sequence) is not int or sequence <= 0:
        raise ValueError("positive integer mark sequence required")
    mark, index = f"{60000.5 + (sequence % 10) / 100:.2f}", f"{60000.4 + (sequence % 10) / 100:.2f}"
    if venue == "BINANCE":
        funding = (ms // 28_800_000 + 1) * 28_800_000
        return [_md_frame(venue, "MARK", {"e": "markPriceUpdate", "E": ms, "s": SYMBOLS[venue], "p": mark,
                                          "P": mark, "i": index, "r": "0.00010000", "T": funding}, at_ns, "mark")]
    index_symbol = index_symbol or INDEX_SYMBOLS[venue]
    mark_frame = _md_frame(venue, "MARK", {"arg": {"channel": "mark-price", "instId": SYMBOLS[venue]}, "data": [{
        "instType": "SWAP", "instId": SYMBOLS[venue], "markPx": mark, "ts": str(ms)}]}, at_ns, "mark")
    index_frame = _md_frame(venue, "INDEX", {"arg": {"channel": "index-tickers", "instId": index_symbol}, "data": [{
        "instId": index_symbol, "idxPx": index, "high24h": "60100.0", "low24h": "59900.0", "open24h": "60000.0",
        "sodUtc0": "60000.0", "sodUtc8": "60000.0", "ts": str(ms)}]}, at_ns, "mark")
    index_frame["native_symbol"] = index_symbol
    return [mark_frame, index_frame]


def market_plan(tick, hz, rate=MD_RATE):
    """Deterministic (trades, quotes, marks) to send at publisher tick ``tick`` of a ``hz`` loop: the per-second
    rates spread over the ticks, plus a burst of trades every ``burst_every_s``."""
    def spread(per_s):
        return math.floor((tick + 1) * per_s / hz) - math.floor(tick * per_s / hz)
    every = max(1, round(rate["burst_every_s"] * hz))
    trades = spread(rate["trade_per_s"]) + (rate["burst_trades"] if tick and tick % every == 0 else 0)
    return trades, spread(rate["quote_per_s"]), spread(rate["mark_per_s"])


def seed_opens(now_ms, count=MD_RATE["seed_bars"]):
    """Open times of the ``count`` closed bars just before the current minute (oldest first)."""
    current = now_ms // BAR_MS * BAR_MS
    return [current - (count - i) * BAR_MS for i in range(count)]


def bars_due(now_ms, last_open_ms, delay_ms=MD_RATE["bar_delay_ms"]):
    """Opens of every bar after ``last_open_ms`` whose close + delay has passed, in order (no gap after a stall)."""
    due, nxt = [], last_open_ms + BAR_MS
    while nxt + BAR_MS + delay_ms <= now_ms:
        due.append(nxt)
        nxt += BAR_MS
    return due


class MarketDataSource:
    """The synthetic venue's TRADE / QUOTE / BAR / MARK events, per tick of the publisher loop (no I/O).

    Each event is {venue, feed, id, frame | frames, extra}: id is the identity the TS notification carries back
    (trade id, quote event ms, bar open ms, mark observation ms); a frame is stamped by ``clock`` when it is
    built (an OKX MARK event is its mark + index pair). Trade ids and quote / mark sequences only increase; bars
    are contiguous from the seed on, each sent once after its close.
    """

    def __init__(self, venues=VENUES, rate=MD_RATE, clock=time.time_ns, index_symbols=None):
        self.venues, self.rate, self.clock = tuple(venues), rate, clock
        self.index_symbols = dict(INDEX_SYMBOLS if index_symbols is None else index_symbols)
        self.mark_seq = {v: 0 for v in self.venues}
        self.mark_ms = {v: 0 for v in self.venues}
        self.trade_id = {v: 0 for v in self.venues}
        self.quote_seq = {v: 0 for v in self.venues}
        self.quote_ms = {v: 0 for v in self.venues}
        self.last_open = {v: None for v in self.venues}
        self.minute_trades = {v: {} for v in self.venues}

    def _bar(self, venue, open_ms, at_ns, seed):
        frame = bar_frame(venue, open_ms, at_ns, self.minute_trades[venue].pop(open_ms, 0))
        self.last_open[venue] = open_ms
        return {"venue": venue, "feed": "BAR", "id": str(open_ms), "frame": frame,
                "extra": {"close_ms": open_ms + BAR_MS, "seed": seed}}

    def seed(self, now_ns):
        """The closed bars just before the current minute (a retained view for the writer's BAR warmup)."""
        return [self._bar(venue, open_ms, now_ns, True) for venue in self.venues
                for open_ms in seed_opens(now_ns // 1_000_000, self.rate["seed_bars"])]

    def tick(self, tick, hz):
        events = []
        trades, quotes, marks = market_plan(tick, hz, self.rate)
        for venue in self.venues:
            for _ in range(trades):
                self.trade_id[venue] += 1
                at_ns = self.clock()
                minute = at_ns // 1_000_000 // BAR_MS * BAR_MS
                self.minute_trades[venue][minute] = self.minute_trades[venue].get(minute, 0) + 1
                events.append({"venue": venue, "feed": "TRADE", "id": str(self.trade_id[venue]),
                               "frame": trade_frame(venue, self.trade_id[venue], at_ns), "extra": {}})
            for _ in range(quotes):
                at_ns = self.clock()
                if at_ns // 1_000_000 <= self.quote_ms[venue]:
                    continue  # the quote identity is its event ms: never two in one ms
                self.quote_seq[venue] += 1
                self.quote_ms[venue] = at_ns // 1_000_000
                events.append({"venue": venue, "feed": "QUOTE", "id": str(self.quote_ms[venue]),
                               "frame": quote_frame(venue, self.quote_seq[venue], at_ns), "extra": {}})
            for _ in range(marks):
                at_ns = self.clock()
                if at_ns // 1_000_000 <= self.mark_ms[venue]:
                    continue  # the mark identity is its observation ms
                self.mark_seq[venue] += 1
                self.mark_ms[venue] = at_ns // 1_000_000
                events.append({"venue": venue, "feed": "MARK", "id": str(self.mark_ms[venue]), "extra": {},
                               "frames": mark_frames(venue, self.mark_seq[venue], at_ns,
                                                     self.index_symbols.get(venue))})
            if self.last_open[venue] is not None:
                for open_ms in bars_due(self.clock() // 1_000_000, self.last_open[venue], self.rate["bar_delay_ms"]):
                    events.append(self._bar(venue, open_ms, self.clock(), False))
        return events


def lane_feed(provider_kind):
    """The lane of a sealed core binding by provider kind (TRADE/QUOTE/BAR/MARK/INDEX), None otherwise."""
    return next((f for suffix, f in MD_KIND_FEED.items() if provider_kind.endswith(suffix)), None)


def md_bindings(config):
    """(venue, lane) -> the sealed core binding of each TRADE/QUOTE/BAR/MARK lane (and the OKX INDEX
    component); exactly one each."""
    out = {}
    for binding in config["core"]["bindings"]:
        if binding.get("l2"):
            continue
        feed = lane_feed(binding["provider_kind"])
        if feed is None or (binding["venue"], feed) in out:
            raise ValueError("unexpected or duplicate market-data binding " + binding["source_id"])
        out[(binding["venue"], feed)] = binding
    expected = {(venue, feed) for venue in VENUES for feed in MD_FEEDS} | {(v, "INDEX") for v in INDEX_SYMBOLS}
    if set(out) != expected:
        raise ValueError("sealed TRADE/QUOTE/BAR/MARK lane per venue (and the OKX INDEX component) required")
    return out


def md_envelope(binding, frame, ns):
    """Raw envelope of a TRADE/QUOTE/BAR/MARK test frame on its own provider lane (session
    ``<ns>.<venue>.<lane>``, generation 1, never changed): the BOOK lane's resync generations never fence these
    lanes. A MARK/INDEX component carries its physical symbol/channel (the core's binding key)."""
    from qdl.raw.envelope import build_raw_envelope
    import uuid
    namespace(ns)
    if frame.get("test_provenance") is not True or frame.get("provenance") != "TEST_ONLY" \
            or frame.get("adapter_version") != TEST_ADAPTER or frame.get("transport") != "WEBSOCKET":
        raise ValueError("explicit test provenance and websocket transport required")
    symbol = binding.get("physical_native_symbol") or binding["native_symbol"]
    channel = binding.get("physical_native_channel") or binding["native_channel"]
    if (binding["venue"], symbol) != (frame["venue"], frame["native_symbol"]) or \
            lane_feed(binding["provider_kind"]) != frame["feed"]:
        raise ValueError("frame/binding identity mismatch")
    payload = json.dumps(frame["payload"], separators=(",", ":"), allow_nan=False).encode()
    return build_raw_envelope(
        capture_id=uuid.uuid4().bytes, provider=binding["provider"], venue=binding["venue"],
        market=binding["market"], product_type=binding["product_type"],
        native_symbol=symbol, native_channel=channel,
        subscription_id=binding["source_id"], source_session_id=f"{ns}.{binding['venue']}.{frame['lane']}",
        connection_generation=1, lease_epoch=1, authority_revision=1, partition_plan_epoch=1,
        received_at_ns=frame["received_at_ns"], transport_protocol=1, transport_compression=1,
        capture_boundary=2, raw_frame_bytes=payload, adapter_version=TEST_ADAPTER, config_revision=1,
        instrument_catalog_revision=binding["instrument_catalog_revision"],
        correlation_id=str(uuid.uuid4()), test_provenance=True)


def md_core_binding(book, source, acquisition):
    """Core binding of one TRADE/QUOTE/BAR lane: the BOOK binding's exported instrument identity, the source
    catalog's source, the production acquisition mapping's provider kind / channel / sequence policy."""
    if acquisition.get("mode") != "RUST_NATIVE":
        raise ValueError("native acquisition lane required: " + source["binding_id"])
    if source["instrument_uid"] != book["instrument_uid"] or source["source"]["provider"] != book["provider"]:
        raise ValueError("source/instrument identity mismatch: " + source["binding_id"])
    binding = {key: book[key] for key in ("provider", "venue", "market", "product_type", "native_symbol",
                                          "instrument_uid", "instrument_id", "instrument_revision",
                                          "instrument_catalog_revision")}
    binding.update(native_channel=acquisition["native_channel"], provider_kind=acquisition["provider_kind"],
                   source_id=source["source"]["source_id"], source_role=source["source"]["source_role"],
                   normalizer_version=source["source"]["normalizer_version"],
                   require_final_bar=bool(source["quality"]["require_final_bar"]),
                   sequence_policy=acquisition["sequence_policy"])
    return binding


def md_core_bindings(book, source, acquisition):
    """All core bindings of one lane: one, or the MARK_INDEX physical components exactly as the production
    deployment emits them (StableMarkIndexAcquisition.component_entries + the core mapping): Binance one BOTH
    component, OKX a MARK component and an INDEX component on the explicit index symbol, each with its signed
    quiet cadence."""
    binding = md_core_binding(book, source, acquisition)
    mark_index = acquisition.get("mark_index")
    if source["feed"] != "MARK_INDEX_PRICE":
        if mark_index is not None:
            raise ValueError("mark/index acquisition on a non-MARK lane")
        return [binding]
    cadence = mark_index["component_quiet_after_ms"]
    if mark_index["provider_protocol"] == "BINANCE_MARK_PRICE":
        entries = [(book["native_symbol"], acquisition["native_channel"], "binance_usdm_mark_index", "BOTH")]
    elif mark_index["provider_protocol"] == "OKX_MARK_INDEX":
        entries = [(book["native_symbol"], "mark-price", "okx_mark_price", "MARK"),
                   (str(mark_index["index_native_symbol"]), "index-tickers", "okx_index_price", "INDEX")]
    else:
        raise ValueError("MARK_INDEX provider protocol is not certified")
    out = []
    for symbol, channel, kind, component in entries:
        item = dict(binding, provider_kind=kind, physical_native_symbol=symbol, physical_native_channel=channel,
                    mark_index={"component": component})
        if cadence.get(component) is not None:
            item["mark_index"]["quiet_after_ms"] = cadence[component]
        out.append(item)
    return out


def _rewrite(path, value):
    """Replace one non-private material file in place (same 0644 mode the DR05 material used)."""
    path = Path(path)
    temporary = path.with_name(path.name + ".g12-tmp")
    with temporary.open("x") as handle:
        os.chmod(temporary, 0o644)
        handle.write(value if isinstance(value, str) else json.dumps(value))
    os.replace(temporary, path)


def seal_market_feeds(work, ns):
    """Add TRADE / QUOTE / BAR 1m / MARK_INDEX_PRICE of the two sealed instruments to the DR05 BOOK material
    (offline, no I/O outside ``work`` and the read-only source catalog / production acquisition plan): core.json,
    catalog.yaml, both manifests, route bindings, ts-consumers and bundle.json. Each generated acquisition must
    equal the production plan's entry for that binding. Once per run (handoff "g12_feeds")."""
    import yaml
    from qdl.query import ConsumerGrade, FeedType
    from qdl.runtime.production_catalog import ProductionCatalogBuilder, ProductionDemand
    from qdl.runtime.stable_catalog import StableSourceCatalog
    from scripts.kn_gateway_bundle import compile_bundle
    from scripts.kn_native_slice_probe import ISSUER, AUDIENCE
    from scripts.p184_dr05_case import BOOK_FEEDS, consumer_packet
    from scripts.p184_dr05_dl_fault import adapter_mode, source_catalog_path

    work = Path(work)
    handoff = json.loads((work / "handoff.json").read_text())
    if namespace(handoff["namespace"]) != namespace(ns) or handoff.get("g12_feeds") is not None:
        raise ValueError("unsealed G12 material of this namespace required")
    mode = adapter_mode(handoff)
    config = json.loads((work / "core.json").read_text())
    books = config["core"]["bindings"]
    if any(not b.get("l2") for b in books) or sorted(b["venue"] for b in books) != sorted(VENUES):
        raise ValueError("the DR05 two-venue BOOK material is required")
    source = yaml.safe_load(source_catalog_path(work).read_text())
    catalog = yaml.safe_load((work / "catalog.yaml").read_text())
    production = {item["binding_id"]: item for item in
                  yaml.safe_load((ROOT / PRODUCTION_ACQUISITION).read_text())["bindings"]}
    lanes = []
    for book in books:
        for feed in MD_FEEDS:
            interval = BAR_INTERVAL if feed == "BAR" else None
            matches = [b for b in source["bindings"] if b["instrument_uid"] == book["instrument_uid"]
                       and b["feed"] == CATALOG_FEED[feed] and b.get("interval") == interval
                       and b["source"]["source_role"] == "PRIMARY"]
            if len(matches) != 1:
                raise ValueError(f"exactly one primary {feed} source required for {book['instrument_id']}")
            planned = production.get(matches[0]["binding_id"]) or {}
            mark = ({"max_freshness_ms": MD_REQUIREMENTS["MARK"]["max_freshness_ms"], "require_live": True,
                     "index_native_symbol": (planned.get("mark_index") or {}).get("index_native_symbol")}
                    if feed == "MARK" else {})
            demand = ProductionDemand(
                consumer_id=ns + "." + book["venue"].lower(), consumer_grade=ConsumerGrade.EXECUTION,
                venue=book["venue"], market=book["market"], product_type=book["product_type"],
                native_symbol=book["native_symbol"], feed=FeedType(CATALOG_FEED[feed]), interval=interval,
                source_policy_id=matches[0]["source"]["source_policy_id"], **mark)
            acquisition = ProductionCatalogBuilder._acquisition(matches[0]["binding_id"], demand)
            fields = ("mode", "provider_kind", "native_channel", "sequence_policy", "mark_index")
            if {k: acquisition.get(k) for k in fields} != {k: planned.get(k) for k in fields}:
                raise ValueError("generated acquisition differs from the production plan: "
                                 + matches[0]["binding_id"])
            entry = copy.deepcopy(matches[0])
            if mode == "SYNTHETIC":
                entry["source"]["adapter_version"] = TEST_ADAPTER
            elif entry["source"].get("adapter_version") in (None, "", TEST_ADAPTER):
                raise ValueError("CAPTURE keeps the source catalog's own adapter_version: "
                                 + matches[0]["binding_id"])
            lanes.append((book, feed, entry, md_core_bindings(book, matches[0], acquisition)))
    config["core"]["bindings"] = books + [item for lane in lanes for item in lane[3]]
    digest = hashlib.sha256(json.dumps(config["core"]["bindings"], sort_keys=True).encode()).hexdigest()
    config["authority"].update(capability_manifest_digest=digest, contract_digest=digest,
                               partition_plan_digest=digest)
    if mode == "CAPTURE" and any(b["source"].get("adapter_version") in (None, "", TEST_ADAPTER)
                                 for b in catalog["bindings"]):
        raise ValueError("CAPTURE material must not carry the synthetic adapter_version")
    catalog["bindings"] = catalog["bindings"] + [lane[2] for lane in lanes]
    _rewrite(work / "core.json", config)
    _rewrite(work / "catalog.yaml", yaml.safe_dump(catalog))
    loaded = StableSourceCatalog.load(work / "catalog.yaml")
    manifests = []
    for book in books:
        label = book["venue"].lower()  # the linear scopes: scope id == venue
        path = work / (label + ".yaml")
        mapping = yaml.safe_load(path.read_text())
        template = next(r for r in mapping["spec"]["requirements"] if r["feed"] == "BOOK_SNAPSHOT")
        for _, feed, entry, _ in (lane for lane in lanes if lane[0] is book):
            requirement = {k: v for k, v in template.items() if k not in ("interval", "event_recency_policy",
                                                                          "max_session_liveness_ms")}
            requirement.update(feed=CATALOG_FEED[feed], source_policy_id=entry["source"]["source_policy_id"],
                               bar_revision_policy="LATEST", **MD_REQUIREMENTS[feed])
            mapping["spec"]["requirements"].append(requirement)
        mapping["spec"]["quotas"]["max_streams"] = 50  # the TS paper manifest's; 6 slices per clone now
        _rewrite(path, yaml.safe_dump(mapping))
        consumer, route = consumer_packet(path, loaded, book["venue"], ISSUER, AUDIENCE,
                                          feeds=BOOK_FEEDS + tuple(CATALOG_FEED[f] for f in MD_FEEDS))
        _rewrite(work / (label + ".binding.json"), route)
        _rewrite(work / ("ts-consumer." + label + ".json"), consumer)
        manifests.append(path)
    _rewrite(work / "bundle.json", compile_bundle(environment="paper", catalog_path=work / "catalog.yaml",
                                                  manifest_paths=manifests))
    handoff["g12_feeds"] = {"feeds": list(BOOK_FEEDS) + [CATALOG_FEED[f] for f in MD_FEEDS],
                            "bar_interval": BAR_INTERVAL, "source_ids": [lane[2]["source"]["source_id"] for lane in lanes],
                            "core_bindings_sha256": digest, "adapter_mode": mode,
                            "adapter_versions": {b["source"]["source_id"]: b["source"]["adapter_version"]
                                                 for b in catalog["bindings"]}}
    (work / "handoff.json").write_text(json.dumps(handoff, indent=2) + "\n")
    return handoff["g12_feeds"]


def physical_route(binding):
    """The raw route a core binding consumes: physical_native_symbol/channel when declared (MARK/INDEX)."""
    return tuple(binding["physical_" + key] if key in ("native_symbol", "native_channel")
                 and binding.get("physical_" + key) is not None else binding.get(key) for key in ROUTE_FIELDS)


def capture_adapter_parity(work, raws, raw_sha256):
    """CAPTURE-mode prepare-time proof (offline, read-only): every selected raw's adapter_version equals its
    binding's catalog adapter_version, with exactly one value per route, on sealed CAPTURE material of exactly
    this capture (``raw_sha256``: the caller's computed archive sha256, equal to the sealed pin).

    ``raws``: iterable of (route tuple in ROUTE_FIELDS order, adapter_version) of the selected raws, in any
    order. Raises ValueError on any mismatch, an unbound route or a bound route without raws; returns the
    receipt (per route: source_id, adapter_version, raw_frames). Raws are never relabelled here."""
    import yaml
    from scripts.p184_dr05_dl_fault import adapter_mode

    work = Path(work)
    handoff = json.loads((work / "handoff.json").read_text())
    feeds = handoff.get("g12_feeds") or {}
    if (adapter_mode(handoff) != "CAPTURE" or feeds.get("adapter_mode") != "CAPTURE"
            or handoff.get("capture_raw_sha256") != raw_sha256):
        raise ValueError("sealed CAPTURE material of exactly this capture required")
    catalog = yaml.safe_load((work / "catalog.yaml").read_text())
    versions = {}
    for item in catalog["bindings"]:
        versions.setdefault(item["source"]["source_id"], set()).add(item["source"].get("adapter_version"))
    routes = {}
    for binding in json.loads((work / "core.json").read_text())["core"]["bindings"]:
        found = versions.get(binding["source_id"], set())
        if len(found) != 1 or next(iter(found)) in (None, "", TEST_ADAPTER):
            raise ValueError("one real catalog adapter_version per bound source required: " + binding["source_id"])
        route = physical_route(binding)
        if route in routes:
            raise ValueError("duplicate physical route")
        routes[route] = {"source_id": binding["source_id"], "adapter_version": next(iter(found)), "raw_frames": 0}
    selected = 0
    for route, version in raws:
        item = routes.get(tuple(route))
        if item is None:
            raise ValueError("selected raw outside the bound routes")
        if version != item["adapter_version"]:
            raise ValueError(f"raw adapter_version {version!r} differs from its binding "
                             f"{item['adapter_version']!r} ({item['source_id']})")
        item["raw_frames"] += 1
        selected += 1
    if not selected or any(item["raw_frames"] == 0 for item in routes.values()):
        raise ValueError("every bound route needs selected raws")
    return {"schema": CAPTURE_PARITY_SCHEMA, "provenance": "TEST_ONLY", "status": "PROVEN",
            "relabelled": False, "raw_sha256": raw_sha256, "selected_raw_frames": selected,
            "adapter_versions": sorted({item["adapter_version"] for item in routes.values()}),
            "routes": [{"route": list(route), **item} for route, item in sorted(routes.items())]}


def with_execution_mark_index(build):
    """Wrap ``build_stable_query_stack`` with the production KN execution MARK/INDEX wiring of
    ``qdl.runtime.stable`` (query backend kn3): reference data enabled, execution reads from the market cache
    (``build_cache_execution_mark_index_reader``), alpha reads likewise. No acquisition quiet policy and no
    session-liveness reader: the sealed requirement is event-age BLOCK, which needs neither."""
    from qdl.reference.local_mark_index import (
        build_cache_alpha_mark_index_reader, build_cache_execution_mark_index_reader)

    def stack(*, catalog, backend, **kwargs):
        kwargs.update(
            reference_data_enabled=True,
            execution_mark_index_reader=build_cache_execution_mark_index_reader(
                catalog=catalog, backend=backend, acquisition=None, session_liveness_reader=None),
            alpha_mark_index_reader_factory=lambda: build_cache_alpha_mark_index_reader(
                catalog=catalog, backend=backend))
        return build(catalog=catalog, backend=backend, **kwargs)
    return stack


def isolated_state_topics(ns):
    """The market-cache reader's state-topic settings of this namespace (the projector's own topics; it takes
    their partition counts from the topic metadata, ``topics()`` creates them)."""
    from qdl.runtime.kn_market_cache import (
        BARS_PARTITIONS_ENV, BARS_TOPIC_ENV, LATEST_PARTITIONS_ENV, LATEST_TOPIC_ENV)
    wanted = topics(ns)
    return {LATEST_TOPIC_ENV: ns + ".latest", LATEST_PARTITIONS_ENV: str(wanted[ns + ".latest"][0]),
            BARS_TOPIC_ENV: ns + ".bars", BARS_PARTITIONS_ENV: str(wanted[ns + ".bars"][0])}


def with_isolated_state_topics(build_reader, ns):
    """Wrap ``reader_from_environment`` so the Query reads this namespace's state topics.

    The DR08 ``serve_query`` passes a literal two-key environment, so the reader fell back to the production
    defaults ``md.latest.v2`` / ``md.bars.v2`` with 6 partitions. A BAR read takes its source boundary from the
    bars checkpoint ``ckpt:<bars topic>:<state partition>``; under the wrong topic that hash does not exist, every
    BAR history is ``SOURCE_BOUNDARY_UNKNOWN`` (typed DATA_NOT_READY) and the writer's BAR slice never starts
    (G12 run 2026-10-11 p184-g12-rc10-full: 22 warmup refusals, BAR never applied; latest feeds were unaffected
    because a latest read also proves its boundary from the entry's own offset)."""
    extra = isolated_state_topics(ns)

    def reader(environ):
        return build_reader({**dict(environ), **extra})
    return reader


def serve_query(work):
    """In-namespace Query (--query): the unchanged DR05 diagnostic Query (DR08 serve_query, public HTTP boundary,
    KN market cache + hot view) of the code under test, with execution MARK/INDEX reads enabled and the cache
    reader on this namespace's state topics."""
    import qdl.runtime.kn_market_cache as kn_market_cache
    import qdl.runtime.stable_source as stable_source
    from scripts.p184_dr05_observer import diagnostic_query
    ns = namespace(json.loads((Path(work) / "packet.json").read_text())["namespace"])
    kn_market_cache.reader_from_environment = with_isolated_state_topics(kn_market_cache.reader_from_environment, ns)
    stable_source.build_stable_query_stack = with_execution_mark_index(stable_source.build_stable_query_stack)
    diagnostic_query(Path(work))


def extend_material(work):
    """In-container --feeds (network none): attested namespace / image set, then seal_market_feeds."""
    packet = _packet(work)
    return seal_market_feeds(work, packet["namespace"])


def book_top(seq, shape=BOOK_TOP):
    """Deterministic (bid, ask) price strings of BOOK frame ``seq`` (see BOOK_TOP): moving one step per frame in
    the first cycle - quiet frames of each cycle, held for the last quiet frames; never crossed."""
    phase = seq % shape["cycle"]
    moving = shape["cycle"] - shape["quiet"]
    step = (min(phase, moving - 1)) % shape["steps"]
    bid = shape["base"] + step
    return str(bid), str(bid + shape["spread"])


def book_levels(seq, previous, snapshot, quantity):
    """Levels of BOOK frame ``seq``: a snapshot is the one-level book at ``book_top(seq)``; a delta sets the new top
    and removes the previous frame's top level (qty "0") where it moved, so the book stays one level per side.
    The bid quantity is the publisher's per-frame quantity (as before); the ask keeps quantity 1."""
    bid, ask = book_top(seq)
    bids, asks = [(bid, quantity)], [(ask, "1")]
    if not snapshot and previous:
        old_bid, old_ask = book_top(previous)
        if old_bid != bid:
            bids.append((old_bid, "0"))
        if old_ask != ask:
            asks.append((old_ask, "0"))
    return {"bids": bids, "asks": asks}


def book_frame(venue, snapshot, seq, previous, gen, at_ns):
    """The publisher's BOOK frame: the DR05 provider shape with the deterministic moving top."""
    from scripts.p184_dr05_dl_fault import frame
    quantity = str(seq % 1000 + 1)
    return frame(venue, snapshot, seq, 0 if snapshot else previous, gen, quantity, at_ns,
                 levels=book_levels(seq, previous, snapshot, quantity))


L2_RESNAPSHOT_REQUIRED_V1 = "L2_RESNAPSHOT_REQUIRED_V1"


def book_resync_request(wire, event_id, binding, session_id, generation, now_ns, anchor=None):
    """Whether one ``<ns>.quarantine`` record asks THIS publisher lane for a new BOOK snapshot.

    The TEST twin of the production ingestor's ``matches_book_resync`` plus ``snapshot_satisfies_resync``
    (``rust/qdl-kafka/src/bin/qdl-native-raw-ingestor.rs``). It accepts a core ``L2_RESNAPSHOT_REQUIRED_V1``
    sequence-gap request whose raw frame is this lane's BOOK frame (binding identity, ``source_session_id``,
    current ``connection_generation``). The raw frame must be no older than the binding's
    ``l2.snapshot_refresh_seconds``, its frame digest, evidence digest and event id must hold, and it must not
    predate the snapshot this lane already published for that generation (``anchor`` = ``(generation,
    received_at_ns)``). One deliberate difference: the ingestor refuses ``test_provenance`` and this TEST lane
    requires it. Returns ``(accepted, refusal)``. ``refusal`` is None for a record that is no resnapshot request
    at all, and a short label for a request that was refused (apparatus evidence)."""
    from qdl.provider.v1.raw_provider_pb2 import QuarantineRecord, QuarantineReason
    try:
        request = QuarantineRecord.FromString(wire)
    except Exception:  # noqa: BLE001 - another record shape on the topic is not a request
        return False, None
    if request.safe_summary != L2_RESNAPSHOT_REQUIRED_V1 or not request.HasField("raw"):
        return False, None
    raw = request.raw
    if raw.subscription_id != binding["source_id"]:
        return False, None  # the other venue's lane
    if request.reason != QuarantineReason.Value("QUARANTINE_REASON_SEQUENCE_GAP"):
        return False, "REASON"
    identity = ("provider", "venue", "market", "product_type", "native_symbol", "native_channel",
                "instrument_catalog_revision")
    if any(getattr(raw, key) != binding[key] for key in identity) or raw.test_provenance is not True:
        return False, "IDENTITY"
    if raw.source_session_id != session_id:
        return False, "SESSION"
    if raw.connection_generation != generation:
        return False, "GENERATION"
    max_age_ns = int(binding["l2"].get("snapshot_refresh_seconds") or 0) * 1_000_000_000
    if (raw.received_at_ns <= 0 or request.quarantined_at_ns < raw.received_at_ns
            or now_ns < request.quarantined_at_ns or now_ns - raw.received_at_ns > max_age_ns):
        return False, "AGE"
    evidence = hashlib.sha256(raw.SerializeToString()
                              + request.reason.to_bytes(4, "big", signed=True)).digest()
    expected_id = hashlib.sha256(raw.capture_id + L2_RESNAPSHOT_REQUIRED_V1.encode()).digest()
    if (request.evidence_sha256 != evidence or event_id != expected_id
            or raw.raw_frame_sha256 != hashlib.sha256(raw.raw_frame_bytes).digest()):
        return False, "DIGEST"
    if anchor is not None and anchor[0] == raw.connection_generation and raw.received_at_ns <= anchor[1]:
        return False, "SATISFIED"
    return True, None


def assign_resync_listener(consumer, topic, offset, timeout=10.0):
    """Assign every partition of ``topic`` from ``offset`` (read-only: no group join, no commit). The core keys a
    quarantine record by its partition key, so a request can land on any partition (six in the capacity runs)."""
    from confluent_kafka import TopicPartition
    metadata = consumer.list_topics(topic, timeout=timeout).topics.get(topic)
    if metadata is None or metadata.error is not None or not metadata.partitions:
        raise RuntimeError("quarantine topic metadata unavailable: " + topic)
    partitions = sorted(metadata.partitions)
    consumer.assign([TopicPartition(topic, partition, offset) for partition in partitions])
    return partitions


def publish(work, out, seconds, hz):
    """Steady provider-shaped BOOK frames per venue; resync (new generation +
    snapshot) after a failed delivery, as a venue adapter would, and after the
    core's own ``L2_RESNAPSHOT_REQUIRED_V1`` request on ``<ns>.quarantine``, as
    the production ingestor's book-resync listener does (``book_resync_request``;
    a restarted core has lost its in-memory book and only a new snapshot
    rebuilds it). TRADE /
    QUOTE / closed BAR 1m / MARK-INDEX frames on their own lanes at the MD_RATE
    cadence; their delivery errors are recorded, never a BOOK resync.

    BOOK runs on the main thread on its own absolute schedule (stamped right before its produce, never behind the
    market-data sends: since 1a161f4 the shared tick sent TRADE/QUOTE/BAR/MARK first and, at 0.25 CPU, stamped
    BOOK late); market data runs on its own thread at the same rate and plan. The main thread alone polls the
    producer, so every delivery callback (and the BOOK resync state) runs there. Each BOOK ``pub`` row carries
    its top (bid, ask) and its lateness against the schedule (``late_ms``, apparatus evidence)."""
    import threading
    from confluent_kafka import Consumer, OFFSET_BEGINNING, Producer
    from scripts.p184_dr05_raw_publisher import _session_label, raw_envelope
    packet = _packet(work)
    ns = packet["namespace"]
    config = json.loads((Path(work) / "core.json").read_text())
    if config.get("raw_topics") != [ns + ".raw"] or config["core"].get("allow_test_provenance") is not True:
        raise ValueError("isolated test-only raw topic required")
    raw_partitions = core_group(work)["raw_partitions"]
    plan = raw_partition_plan(config["core"]["bindings"], raw_partitions) if raw_partitions > 1 else {}
    partition_of = (lambda key: plan.get(key, 0)) if raw_partitions == 1 else plan.__getitem__  # noqa: E731
    bindings = {b["venue"]: b for b in config["core"]["bindings"] if b.get("l2")}
    lanes = md_bindings(config)
    if set(bindings) != set(VENUES) or not 0 < hz <= 20 or not 0 < seconds <= LIMITS["deadline_s"]:
        raise ValueError("two-venue roster and bounded rate/duration required")
    producer = Producer({**_security(packet), "enable.idempotence": True, "acks": "all",
                         "linger.ms": 0, "message.timeout.ms": 30000, "queue.buffering.max.messages": 10000})
    state = {v: {"gen": 7, "seq": 1000, "snapshot": True} for v in VENUES}
    sessions = {v: ns + "." + _session_label(bindings[v]) for v in VENUES}
    anchors = {}  # venue -> (generation, received_at_ns) of the last published snapshot
    quarantine = config["core"]["quarantine_stream"]
    if quarantine != ns + ".quarantine":
        raise ValueError("isolated test-only quarantine topic required")
    # Read-only listener: assign (no group join, no commit); read_committed like the ingestor's source.
    requests = Consumer({**_security(packet), "group.id": ns + ".publisher-book-resync",
                         "enable.auto.commit": False, "isolation.level": "read_committed"})
    assign_resync_listener(requests, quarantine, OFFSET_BEGINNING)
    stop, end, period = _Stop(), time.monotonic() + seconds, 1.0 / hz
    lock = threading.Lock()
    with open(out, "a", buffering=1) as log:
        def write(row):
            with lock:
                log.write(json.dumps(row) + "\n")

        def serve_resync_requests():
            """Main thread only (it owns the BOOK state): turn a matching core request into gen+1 + snapshot."""
            try:
                messages = requests.consume(num_messages=64, timeout=0)
            except Exception as error:  # noqa: BLE001 - recorded; BOOK keeps its schedule
                write({"k": "resync_request_error", "t": time.time_ns(), "error": f"{type(error).__name__}"})
                return
            for message in messages:
                if message.error() is not None:
                    write({"k": "resync_request_error", "t": time.time_ns(), "error": str(message.error().code())})
                    continue
                event_id = dict(message.headers() or []).get("qdl-event-id", b"")
                for venue in VENUES:
                    s = state[venue]
                    accepted, refusal = book_resync_request(message.value(), event_id, bindings[venue],
                                                            sessions[venue], s["gen"], time.time_ns(),
                                                            anchors.get(venue))
                    if accepted:
                        write({"k": "resync", "t": time.time_ns(), "venue": venue, "after_seq": s["seq"],
                               "new_gen": s["gen"] + 1, "cause": L2_RESNAPSHOT_REQUIRED_V1,
                               "offset": message.offset()})
                        s["gen"] += 1
                        s["snapshot"] = True
                    elif refusal is not None:
                        write({"k": "resync_request_refused", "t": time.time_ns(), "venue": venue,
                               "reason": refusal, "offset": message.offset()})

        def ack(venue, seq, gen, sent_ns):
            def callback(error, message):
                row = {"k": "ack", "venue": venue, "seq": seq, "gen": gen,
                       "ack_ms": round((time.time_ns() - sent_ns) / 1e6, 3)}
                if error is not None:
                    row["error"] = str(error.code())
                    if state[venue]["gen"] == gen:
                        state[venue]["gen"] += 1
                        state[venue]["snapshot"] = True
                        write({"k": "resync", "t": time.time_ns(), "venue": venue,
                               "after_seq": seq, "new_gen": gen + 1})
                else:
                    row["offset"] = message.offset()
                write(row)
            return callback

        def md_ack(event, sent_ns):
            def callback(error, message):
                row = {"k": "ack_md", "venue": event["venue"], "feed": event["feed"], "id": event["id"],
                       "ack_ms": round((time.time_ns() - sent_ns) / 1e6, 3)}
                if error is not None:
                    row["error"] = str(error.code())
                else:
                    row["offset"] = message.offset()
                write(row)
            return callback

        def send_md(event):
            frames = event.get("frames") or [event["frame"]]  # an OKX MARK event is its mark + index pair
            at_ns = frames[0]["received_at_ns"]
            for item in frames:
                raw = md_envelope(lanes[(event["venue"], item["feed"])], item, ns)
                try:
                    producer.produce(config["raw_topics"][0], key=raw.subscription_id.encode(),
                                     partition=partition_of(raw.subscription_id),
                                     value=raw.SerializeToString(), headers=[("qdl-event-id", raw.capture_id)],
                                     on_delivery=md_ack(event, at_ns))
                except BufferError:
                    write({"k": "ack_md", "venue": event["venue"], "feed": event["feed"],
                           "id": event["id"], "error": "LOCAL_QUEUE_FULL"})
                    return
            write({"k": "pub_md", "t": at_ns, "venue": event["venue"], "feed": event["feed"],
                   "id": event["id"], **event["extra"]})

        def market_data():
            """TRADE / QUOTE / BAR / MARK on their own thread and absolute schedule (same rate and plan)."""
            try:
                source = MarketDataSource(index_symbols={venue: lane["physical_native_symbol"]
                                                         for (venue, feed), lane in lanes.items() if feed == "INDEX"})
                for event in source.seed(time.time_ns()):
                    send_md(event)
                tick, next_md = 0, time.monotonic()
                while time.monotonic() < end and not stop.requested:
                    for event in source.tick(tick, hz):
                        send_md(event)
                    tick += 1
                    next_md += period
                    left = next_md - time.monotonic()
                    if left > 0:
                        time.sleep(left)
            except BaseException as error:  # noqa: BLE001 - recorded; BOOK keeps its schedule
                write({"k": "md_thread_error", "t": time.time_ns(), "error": f"{type(error).__name__}: {error}"})
                raise

        md_thread = threading.Thread(target=market_data, name="g12-market-data", daemon=True)
        md_thread.start()
        next_at = time.monotonic()
        while time.monotonic() < end and not stop.requested:
            for venue in VENUES:
                s = state[venue]
                snapshot = s["snapshot"]
                seq = s["seq"] + 1
                late_ms = round((time.monotonic() - next_at) * 1e3, 3)
                at_ns = time.time_ns()
                raw = raw_envelope(bindings[venue], book_frame(venue, snapshot, seq, s["seq"], s["gen"], at_ns), ns)
                try:
                    producer.produce(config["raw_topics"][0], key=raw.subscription_id.encode(),
                                     partition=partition_of(raw.subscription_id),
                                     value=raw.SerializeToString(), headers=[("qdl-event-id", raw.capture_id)],
                                     on_delivery=ack(venue, seq, s["gen"], at_ns))
                except BufferError:
                    write({"k": "ack", "venue": venue, "seq": seq, "gen": s["gen"], "error": "LOCAL_QUEUE_FULL"})
                    s["gen"] += 1
                    s["snapshot"] = True
                    continue
                s["seq"], s["snapshot"] = seq, False
                if snapshot:
                    anchors[venue] = (s["gen"], at_ns)
                write({"k": "pub", "t": at_ns, "venue": venue, "seq": seq, "gen": s["gen"],
                       "snapshot": snapshot, "top": list(book_top(seq)), "late_ms": late_ms})
            next_at += period
            while True:
                producer.poll(0)
                serve_resync_requests()
                left = next_at - time.monotonic()
                if left <= 0:
                    break
                time.sleep(min(left, .02))
        md_thread.join(timeout=5)
        requests.close()
        remaining = producer.flush(10)
        write({"k": "end", "t": time.time_ns(), "unflushed": remaining, "md_thread_alive": md_thread.is_alive()})
    return {"stopped": True}


def kafka_sample(source, admin, roster, rosters, groups, phase=lambda name: None):
    """The Kafka part of one monitor sample (cluster/ISR, projector committed vs LSO, group membership).
    ``phase(name)`` is announced before each admin call, so a native crash names the call it died in."""
    from scripts.kn_recovery_observe import Deadline
    row = {}
    phase("describe_cluster")
    try:
        row["cluster"] = source.describe_cluster(Deadline(3.0))
    except Exception as error:  # noqa: BLE001 - recorded, never inferred
        row["cluster_error"] = type(error).__name__
    phase("describe_topics")
    try:
        described = source.describe_topics(sorted(roster), Deadline(3.0))
        parts = [p for values in described.values() for p in values]
        row["under_replicated"] = sum(1 for p in parts if len(p["isr"]) < len(p["replicas"]))
        row["offline"] = sum(1 for p in parts if p["leader"] is None)
        row["leaders"] = {f"{t}/{p['partition']}": p["leader"] for t, ps in described.items() for p in ps}
        row["min_isr_size"] = min(len(p["isr"]) for p in parts)
    except Exception as error:  # noqa: BLE001
        row["topics_error"] = type(error).__name__
    try:
        lag = {}
        for group, group_roster in rosters.items():
            phase("list_consumer_group_offsets:" + group)
            committed = source.committed_offsets(group, group_roster, Deadline(3.0))
            phase("list_offsets:" + group)
            ends = source.log_end_offsets(sorted(committed), Deadline(3.0))
            lag[group] = sum(max(0, ends[tp] - max(committed[tp], 0)) for tp in committed)
            if group not in PROJECTOR_GROUPS:  # RB2: the core group's [committed, log end] per partition
                row.setdefault("offsets", {})[group] = {f"{t}/{p}": [committed[(t, p)], ends[(t, p)]]
                                                        for t, p in sorted(committed)}
        row["lag"] = lag
        row["projector_lag"] = lag["kn-projector-v3-a"]
    except Exception as error:  # noqa: BLE001
        row["lag_error"] = type(error).__name__
    phase("describe_consumer_groups")
    try:
        described = admin.describe_consumer_groups(list(groups), request_timeout=3)
        row["groups"] = {}
        for group, future in described.items():
            value = future.result(timeout=4)
            row["groups"][group] = {"state": str(getattr(value, "state", "")).split(".")[-1],
                                    "members": len(value.members or ()),
                                    "assigned": sorted(len(getattr(m.assignment, "topic_partitions", None) or ())
                                                       for m in value.members or ()),
                                    # RB2: who owns what (member id, client.id, partitions), not only counts
                                    "detail": [{"member_id": m.member_id, "client_id": m.client_id,
                                                "host": m.host,
                                                "partitions": sorted(f"{tp.topic}/{tp.partition}" for tp in (
                                                    getattr(m.assignment, "topic_partitions", None) or ()))}
                                               for m in value.members or ()]}
    except Exception as error:  # noqa: BLE001
        row["groups_error"] = type(error).__name__
    phase("done")
    return row


def local_sample(work):
    """The non-Kafka part of one monitor sample: Query/Stream ports, disk, projector status file."""
    import socket
    row = {"ports": {}}
    for port in (8200, 8210):
        try:
            with socket.create_connection(("localhost", port), timeout=.5):
                row["ports"][str(port)] = True
        except OSError:
            row["ports"][str(port)] = False
    row["disk_free_bytes"] = shutil.disk_usage(str(work)).free
    try:
        row["projector_status_mtime"] = (Path(work) / "g12-projector-status.json").stat().st_mtime
    except FileNotFoundError:
        row["projector_status_mtime"] = None
    return row


def exit_reason(code):
    """A child's exit as a typed reason: SIGSEGV / SIGKILL / ... for a signal, EXIT_<n> otherwise."""
    if code is not None and code < 0:
        try:
            return signal.Signals(-code).name
        except ValueError:
            return f"SIGNAL_{-code}"
    return f"EXIT_{code}"


def _sample_child(conn, make_sampler):
    """Forked sampler: builds its own client on first use, answers one sample per request."""
    sampler = None
    while True:
        try:
            request = conn.recv()
        except (EOFError, OSError):
            return
        if request is None:
            return
        try:
            if sampler is None:
                sampler = make_sampler()
            fields = sampler(lambda name: conn.send(("phase", name)))
        except Exception as error:  # noqa: BLE001 - typed back to the parent, then this child ends
            conn.send(("error", type(error).__name__ + ":" + str(error)[:200]))
            return
        conn.send(("row", fields))


class SampleSupervisor:
    """Runs each sample in a long-lived forked child with a hard per-sample timeout.

    A native crash (librdkafka SIGSEGV, G12 run 2026-10-11 full2: the monitor exited 139 one second into a
    broker stop) or a hang loses only the sample in flight: it comes back as a typed error ({error, phase,
    exitcode}) and the next sample starts a fresh child. The parent never loads the Kafka client (fork-safe).
    """

    def __init__(self, make_sampler, timeout_s, context=None):
        import multiprocessing
        self.make_sampler, self.timeout_s = make_sampler, timeout_s
        self.context = context or multiprocessing.get_context("fork")
        self.proc = self.conn = None
        self.restarts, self.started = 0, False

    def _start(self):
        parent, child = self.context.Pipe()
        self.proc = self.context.Process(target=_sample_child, args=(child, self.make_sampler), daemon=True)
        self.proc.start()
        child.close()
        self.conn = parent

    def _stop(self, kill):
        proc, conn, self.proc, self.conn = self.proc, self.conn, None, None
        if proc is None:
            return None
        if kill and proc.is_alive():
            proc.kill()
        proc.join(5)
        if conn is not None:
            conn.close()
        return proc.exitcode

    def sample(self):
        """(fields, None) or (None, error)."""
        if self.proc is None or not self.proc.is_alive():
            if self.proc is not None:
                self._stop(kill=True)
            if self.started:
                self.restarts += 1
            self._start()
            self.started = True
        phase, limit = None, time.monotonic() + self.timeout_s
        try:
            self.conn.send("sample")
        except (BrokenPipeError, OSError):
            code = self._stop(kill=True)
            return None, {"error": exit_reason(code), "phase": None, "exitcode": code}
        while True:
            left = limit - time.monotonic()
            if left <= 0:
                code = self._stop(kill=True)
                return None, {"error": "TIMEOUT", "phase": phase, "exitcode": code}
            try:
                if self.conn.poll(min(left, .05)):
                    kind, value = self.conn.recv()
                    if kind == "phase":
                        phase = value
                        continue
                    if kind == "row":
                        return value, None
                    code = self._stop(kill=True)
                    return None, {"error": "EXCEPTION", "detail": value, "phase": phase, "exitcode": code}
            except (EOFError, OSError):
                pass
            if not self.proc.is_alive():
                code = self._stop(kill=False)
                return None, {"error": exit_reason(code), "phase": phase, "exitcode": code}

    def close(self):
        if self.conn is not None:
            try:
                self.conn.send(None)
            except (BrokenPipeError, OSError):
                pass
        self._stop(kill=True)


def monitor(work, out, seconds, period=1.0):
    """Read-only 1 s observation: cluster/ISR, projector committed vs LSO,
    group membership, Query/Stream ports, disk. No group join or commit.

    The Kafka admin calls run in a supervised child (SampleSupervisor): a crash or
    hang of one sample is written as ``{"k": "sample_error", "error": "SIGSEGV" |
    "TIMEOUT" | ..., "phase": <the admin call>}`` and never stops the monitor; the
    G12 analysis excludes those rows and refuses a fault whose clearance or
    catch-up falls in a monitor gap (MONITOR_MAX_GAP_S)."""
    packet = _packet(work)
    ns = packet["namespace"]
    shape = core_group(work)
    roster = topics(ns, shape["raw_partitions"])
    rosters = {"kn-projector-v3-a": {ns + ".canonical": roster[ns + ".canonical"][0]},
               "kn-projector-v3-b": {ns + ".latest": roster[ns + ".latest"][0],
                                     ns + ".bars": roster[ns + ".bars"][0]}}
    if shape["members"] > 1:  # core-group run: the core group's raw lag (committed vs log end)
        rosters[ns] = {ns + ".raw": shape["raw_partitions"]}
    groups = (*PROJECTOR_GROUPS, "kn-" + ns, ns)

    def make_sampler():
        from confluent_kafka.admin import AdminClient
        from scripts.kn_recovery_observe import KafkaAdminSource
        admin = AdminClient(_security(packet))
        source = KafkaAdminSource(admin)
        return lambda phase: kafka_sample(source, admin, roster, rosters, groups, phase)

    supervisor = SampleSupervisor(make_sampler, LIMITS["monitor_sample_s"])
    stop, end = _Stop(), time.monotonic() + seconds
    try:
        with open(out, "a", buffering=1) as log:
            while time.monotonic() < end and not stop.requested:
                started = time.monotonic()
                t = time.time_ns()
                fields, error = supervisor.sample()
                if error is not None:
                    row = {"t": t, "k": "sample_error", **error, "restarts": supervisor.restarts}
                else:
                    row = {"t": t, **fields, **local_sample(work)}
                log.write(json.dumps(row, default=list) + "\n")
                time.sleep(max(0.0, period - (time.monotonic() - started)))
            log.write(json.dumps({"k": "end", "t": time.time_ns(),
                                  "reason": "SIGTERM" if stop.requested else "seconds"}) + "\n")
    finally:
        supervisor.close()
    return {"stopped": True}


# ---------------------------------------------------------------- host orchestration

def sh(argv, timeout=60, check=True):
    result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"{argv[:4]} -> {result.returncode}: {result.stderr.strip()[-400:]}")
    return result.stdout


def owned_names(ns):
    """Exact names of running/stopped containers carrying this namespace's label."""
    namespace(ns)
    out = sh(["docker", "ps", "-a", "--filter", f"label={LABEL}={ns}", "--format", "{{.Names}}"])
    names = [n for n in out.split() if n]
    if any(not n.startswith(ns + "-") for n in names):
        raise RuntimeError("label/name ownership mismatch")
    return names


# N1 R2-B6: Run.sample_stats retries a docker stats sample without a role that exited during it, then
# skips and counts the sample; a failure while every sampled role still runs raises.
STATS_SAMPLE_RULE_VERSION = "R2B6_STATS_TOCTOU_RETRY_SKIP_AND_COUNT_V1"


def running_names(ns):
    out = sh(["docker", "ps", "--filter", f"label={LABEL}={ns}", "--format", "{{.Names}}"])
    return set(out.split())


def stop_role(ns, role, timeout=30, kill=False):
    """Stop (``kill``: SIGKILL) one owned --rm container and wait until it is gone (identity checked)."""
    name = f"{ns}-{role}"
    rows = json.loads(sh(["docker", "inspect", name], check=False) or "[]")
    if not rows:
        return False
    row = rows[0]
    if row["Config"]["Labels"].get(LABEL) != ns or row["Name"] != "/" + name:
        raise RuntimeError("refuse to stop a container this run does not own: " + name)
    if row["HostConfig"].get("AutoRemove") is not True:
        raise RuntimeError("refuse non-disposable container: " + name)
    sh(["docker", "kill", "--signal", "KILL", row["Id"]] if kill else
       ["docker", "stop", "--time", str(timeout), row["Id"]], timeout=timeout + 30, check=False)
    limit = time.monotonic() + 30
    while sh(["docker", "ps", "-aq", "--no-trunc", "--filter", "id=" + row["Id"]]).strip():
        if time.monotonic() >= limit:
            raise RuntimeError("--rm container still present: " + name)
        time.sleep(.5)
    return True


def production_guard():
    """Read-only: name -> [restart count, started at, status] of the running shared stack and TS writer."""
    names = sh(["docker", "ps", "--filter", "name=qdl_v2_stable_candidate-",
                "--format", "{{.Names}}"]).split() + [TS_WRITER]
    out = sh(["docker", "inspect", "--format", "{{.Name}}|{{.RestartCount}}|{{.State.StartedAt}}|{{.State.Status}}",
              *names])
    rows = {}
    for line in out.splitlines():
        if line:
            name, count, started, status = line.split("|")
            rows[name.lstrip("/")] = [int(count), started, status]
    return rows


def guard_breaches(before, now):
    """Stop on a crash signal (restart count up, or a running container gone/not running).

    A changed start time with the restart count unchanged is a recreate by
    another operator (the shared stack is not ours); it is recorded, not a stop.
    """
    breaches, recreated = {}, {}
    for name, (count, started, status) in before.items():
        current = now.get(name)
        if current is None or current[2] != "running" or current[0] > count:
            breaches[name] = [[count, started, status], current]
        elif current[1] != started:
            recreated[name] = [started, current[1]]
    return breaches, recreated


def host_resources():
    free = shutil.disk_usage("/").free
    available = None
    with open("/proc/meminfo") as handle:
        for line in handle:
            if line.startswith("MemAvailable:"):
                available = int(line.split()[1]) * 1024
    return {"disk_free_bytes": free, "mem_available_bytes": available}


def docker_inspect_record(name):
    """One container's ``docker inspect`` record (read-only)."""
    rows = json.loads(sh(["docker", "inspect", name], timeout=30) or "[]")
    if len(rows) != 1:
        raise RuntimeError("container not found " + name)
    return rows[0]


def container_image(name):
    """The image ID a container actually runs (read-only docker inspect)."""
    return sh(["docker", "inspect", "--format", "{{.Image}}", name], timeout=30).strip()


def image_record(image):
    """Local image ID, tags and OCI revision/version labels (provenance, read-only)."""
    rows = json.loads(sh(["docker", "image", "inspect", image], timeout=30) or "[]")
    if len(rows) != 1:
        raise RuntimeError("local image missing " + image)
    labels = (rows[0].get("Config") or {}).get("Labels") or {}
    return {"id": rows[0]["Id"], "tags": rows[0].get("RepoTags") or [],
            "revision": labels.get("org.opencontainers.image.revision"),
            "version": labels.get("org.opencontainers.image.version"),
            "test_only": labels.get(TEST_CLOCK_LABEL[0])}


def require_test_clock_images(records):
    """CAPTURE mode: core and stream image records carry the isolated-replay-clock TEST label."""
    for role in TEST_CLOCK_ROLES:
        if (records.get(role) or {}).get("test_only") != TEST_CLOCK_LABEL[1]:
            raise RuntimeError(role + " image lacks the isolated TEST data clock label")
    return True


def events_argv(ns, since_s, until_s):
    """Bounded read-only ``docker events`` of this namespace's containers: die (exitCode), oom, kill (signal).
    ``--until`` makes the follower exit by itself even if the orchestrator is killed."""
    namespace(ns)
    if not 0 < since_s < until_s:
        raise ValueError("bounded event window required")
    return ["docker", "events", "--since", str(int(since_s)), "--until", str(int(until_s)),
            "--filter", f"label={LABEL}={ns}", "--filter", "type=container", "--filter", "event=die",
            "--filter", "event=oom", "--filter", "event=kill", "--format", "{{json .}}"]


# WriterClone precheck (coordinator 2026-10-16, run-time reference 2026-10-16): G12 judges the production writer
# path, i.e. whatever native writer is DEPLOYED when the run starts (B16 then B17 ...), never a hard-coded build. The
# clone copies the deployed writer's env (WriterClone.writer_args, TS scripts/p184_dr05_isolated.py) from its own
# ``docker inspect`` of TS_WRITER; the precheck compares the clone's writer view (every MARKET_DATA_NATIVE_* key plus
# the non-secret tuning keys below) and image with that run-time inspect, requires the deployed writer to run a native
# feed, and, given the parity snapshot of ``--production-parity`` (taken from docker inspect before the run), that the
# deployed writer did not change since the snapshot. Only these non-secret configuration keys are ever recorded.
WRITER_ENV_PREFIXES = ("MARKET_DATA_NATIVE_",)
WRITER_TUNING_KEYS = ("DATA_LAYER_V2_BOOK_SNAPSHOT_BATCH_MAX_WAIT_MS", "DATA_LAYER_V2_STREAM_BATCH_MAX_ITEMS",
                      "DATA_LAYER_V2_STREAM_BATCH_MAX_WAIT_MS", "DATA_LAYER_V2_STREAM_PIPELINE_DEPTH",
                      "DATA_LAYER_V2_STREAM_READ_AHEAD_EVENTS", "DATA_LAYER_V2_MAX_BUFFER_EVENTS",
                      "DATA_LAYER_V2_MAX_RECORDS_PER_RESPONSE", "MARKET_DATA_FIXED_WORKERS")
WRITER_PRECHECK_FILE = "writer-precheck.json"
DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")


def argv_env(argv):
    """KEY -> value of every ``-e KEY=VALUE`` of a docker run argv."""
    env = {}
    for flag, value in zip(argv, argv[1:]):
        if flag == "-e" and "=" in value:
            key, _, val = value.partition("=")
            env[key] = val
    return env


def writer_env_view(env):
    """The non-secret writer configuration G12 compares: MARKET_DATA_NATIVE_* and WRITER_TUNING_KEYS."""
    return {k: env[k] for k in sorted(env) if k.startswith(WRITER_ENV_PREFIXES) or k in WRITER_TUNING_KEYS}


def writer_reference(inspect):
    """{container, image, env view} of one ``docker inspect`` record of the deployed writer."""
    inspect = inspect or {}
    env = dict(item.split("=", 1) for item in (inspect.get("Config") or {}).get("Env") or [] if "=" in item)
    return {"container": TS_WRITER, "image": inspect.get("Image"), "env": writer_env_view(env)}


def writer_clone_precheck(writer_argv, clone_image, deployed, reference=None):
    """Does this WriterClone run the deployed native writer? ``deployed``: writer_reference() of the WriterClone's own
    run-time inspect; ``reference``: the parity snapshot's writer (optional). {ok, problems, envs, images}."""
    deployed = deployed or {}
    deployed_env, deployed_image = deployed.get("env") or {}, deployed.get("image")
    clone_env = writer_env_view(argv_env(writer_argv))
    problems = []
    if not deployed_env.get("MARKET_DATA_NATIVE_FEEDS"):
        problems.append("deployed writer runs no native feed (MARKET_DATA_NATIVE_FEEDS empty/absent): G12 judges the "
                        "native writer path")
    if clone_env != deployed_env:
        differ = sorted(k for k in set(clone_env) | set(deployed_env) if clone_env.get(k) != deployed_env.get(k))
        problems.append(f"clone writer env differs from the deployed writer on {differ}")
    if not clone_image or not DIGEST_RE.fullmatch(str(deployed_image)) or clone_image != deployed_image:
        problems.append(f"clone image {clone_image} is not the deployed writer image {deployed_image}")
    if reference is not None:
        if reference.get("image") != deployed_image:
            problems.append(f"deployed writer image {deployed_image} != parity snapshot {reference.get('image')}")
        if (reference.get("env") or {}) != deployed_env:
            differ = sorted(k for k in set(reference.get("env") or {}) | set(deployed_env)
                            if (reference.get("env") or {}).get(k) != deployed_env.get(k))
            problems.append(f"deployed writer env changed since the parity snapshot on {differ}")
    return {"ok": not problems, "problems": problems, "clone_env": clone_env, "deployed_env": deployed_env,
            "clone_image": clone_image, "deployed_writer_image": deployed_image,
            "reference_image": None if reference is None else reference.get("image")}


def clone_redis_cpu_parity(nano_cpus):
    """WriterClone Redis CPU vs production ``redis_marketdata`` (no CPU quota; Redis runs commands on one thread).
    Burst amendment-1 harness-parity (TS 3a09fe19): uncapped or >= 1.0 CPU is parity; a cap below one CPU throttled
    the isolated Redis and is refused."""
    nano = int(nano_cpus or 0)
    if nano == 0:
        return {"nano_cpus": 0, "parity": "UNCAPPED", "ok": True}
    return {"nano_cpus": nano, "parity": "CAPPED_GE_1" if nano >= 10**9 else "CAPPED_BELOW_1", "ok": nano >= 10**9}


# WriterClone lifecycle (coordinator 2026-10-16, after the G12 final stop "TS clones never applied: TRADE@..,QUOTE@.."
# of p184-g12-v224-final-20261016T143156Z): since 13ef59c the clone copies the deployed writer env, including
# MARKET_DATA_NATIVE_REQUIRE_RESUME=1, but a fresh clone has no cursor checkpoint, so every native session refuses
# HANDOFF_RESUME_REQUIRED (TS rust/crates/market-data-writer/src/session.rs run_inner) and TRADE/QUOTE never apply.
# Production reached strict mode through a bootstrap: a native run that wrote verified checkpoints, then the strict
# restart (TS scripts/p184_n1_writer_cutover.py; the same order as TS p184_n1_capacity_run.strict_restart). The clone
# mirrors it: BOOTSTRAP (deployed env, REQUIRE_RESUME=0 only) until every MD feed applied, a clean stop that flushes the
# checkpoints, then STRICT = the exact deployed env (precheck compares this post-bootstrap argv) and proof that the
# strict sessions resumed (every MD feed applied again after the restart) before the baseline. The cursor root is a
# durable host directory bound where production binds its writer state (a tmpfs /app/state lost it at the restart).
# Also the clone Postgres gets the TS migration that holds service_heartbeats (the writer's heartbeat DB write failed
# with UndefinedTableError in the empty clone database; the Redis heartbeat the sampler reads was unaffected).
REQUIRE_RESUME_KEY = "MARKET_DATA_NATIVE_REQUIRE_RESUME"
CURSOR_DIR_KEY = "DATA_LAYER_V2_CURSOR_DIR"
CLONE_SCHEMA_MIGRATIONS = ("init-db/09-observability-security.sql",)  # TS migration owning service_heartbeats
STRICT_RESUME_REFUSAL = "HANDOFF_RESUME_REQUIRED"


def writer_state_destination(inspect):
    """The deployed writer's durable bind that holds its cursor root (DATA_LAYER_V2_CURSOR_DIR), from docker inspect;
    ValueError when the deployed cursor root is not on a bind mount (nothing to mirror)."""
    env = dict(item.split("=", 1) for item in ((inspect or {}).get("Config") or {}).get("Env") or [] if "=" in item)
    cursor = env.get(CURSOR_DIR_KEY)
    if not cursor or not cursor.startswith("/"):
        raise ValueError(f"deployed writer has no absolute {CURSOR_DIR_KEY}")
    best = None
    for mount in (inspect or {}).get("Mounts") or []:
        dest = str(mount.get("Destination") or "").rstrip("/")
        if mount.get("Type") == "bind" and dest and (cursor == dest or cursor.startswith(dest + "/")):
            if best is None or len(dest) > len(best):
                best = dest
    if best is None:
        raise ValueError(f"deployed writer cursor root {cursor} is not on a durable bind mount")
    return best


def lifecycle_writer_args(argv, image, state_dir, destination, phase):
    """The clone writer ``docker run`` argv of one lifecycle phase: the WriterClone argv (deployed env) plus the
    durable state bind; BOOTSTRAP replaces only the strict-resume flag by 0. The deployed argv must carry exactly one
    ``MARKET_DATA_NATIVE_REQUIRE_RESUME=1`` (a non-strict deployed writer has no bootstrap to mirror)."""
    if phase not in ("BOOTSTRAP", "STRICT"):
        raise ValueError("phase must be BOOTSTRAP or STRICT")
    argv = list(argv)
    flags = [i for i, v in enumerate(argv) if v.startswith(REQUIRE_RESUME_KEY + "=") and i and argv[i - 1] == "-e"]
    if len(flags) != 1 or argv[flags[0]] != REQUIRE_RESUME_KEY + "=1":
        raise ValueError(f"deployed writer argv must carry exactly one {REQUIRE_RESUME_KEY}=1")
    if argv.count(image) != 1:
        raise ValueError("writer image must appear exactly once in the clone argv")
    if any(v.endswith(":" + destination) or (":" + destination + ":") in v for v in argv):
        raise ValueError(f"clone argv already mounts {destination}")
    if phase == "BOOTSTRAP":
        argv[flags[0]] = REQUIRE_RESUME_KEY + "=0"
    index = argv.index(image)
    # --entrypoint precedes the image; the bind goes before both.
    if index >= 2 and argv[index - 2] == "--entrypoint":
        index -= 2
    argv[index:index] = ["-v", f"{state_dir}:{destination}:rw"]
    return argv


def bootstrap_env_delta(bootstrap_argv, strict_argv):
    """Writer-view keys whose value differs between the BOOTSTRAP and STRICT argv; must be exactly the flag."""
    a, b = writer_env_view(argv_env(bootstrap_argv)), writer_env_view(argv_env(strict_argv))
    return {k: {"bootstrap": a.get(k), "strict": b.get(k)} for k in sorted(set(a) | set(b)) if a.get(k) != b.get(k)}


def md_missing_since(sampler_rows, since_ns, venues=VENUES, feeds=MD_FEEDS):
    """FEED@VENUE with no applied notification received at or after ``since_ns`` (strict-resume start proof)."""
    seen = {(r.get("feed"), r.get("venue")) for r in sampler_rows
            if r.get("k") == "ev" and r.get("id") is not None and isinstance(r.get("t"), int) and r["t"] >= since_ns}
    return [f"{feed}@{venue}" for venue in venues for feed in feeds if (feed, venue) not in seen]


def cursor_inventory(state_dir):
    """Relative path -> {bytes, sha256} of every checkpoint file under the clone's state dir (evidence only; the
    contents stay private and are erased at teardown). Unreadable entries are recorded, not raised."""
    out = {}
    root = Path(state_dir)
    for path in sorted(root.rglob("*.json")):
        rel = str(path.relative_to(root))
        try:
            data = path.read_bytes()
            out[rel] = {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
        except OSError as error:
            out[rel] = {"error": type(error).__name__}
    return out


def native_checkpoints(inventory):
    """The native fixed-worker checkpoint files of an inventory (``fixed-workers-v1/native-<slot>/<consumer>.json``,
    TS services/market_data/native_plan.export_native_config)."""
    return sorted(rel for rel, row in inventory.items()
                  if re.search(r"(^|/)fixed-workers-v1/native-\d+/[^/]+\.json$", rel) and row.get("bytes"))


def apply_clone_schema(clone, sql, run=None):
    """Apply the TS schema SQL to a WriterClone's disposable Postgres (after it accepts connections, before the
    writer starts); ON_ERROR_STOP, so a failed statement stops the run."""
    (run or sh)(["docker", "exec", "-e", "PGPASSWORD=dr05", clone.pg_name, "psql", "-v", "ON_ERROR_STOP=1", "-q",
                 "-h", "127.0.0.1", "-p", str(clone.pg_port), "-U", "dr05", "-d", "dr05", "-c", sql], timeout=60)


def refusal_lines(text, limit=5):
    """(count, last ``limit`` lines truncated) of the writer log lines naming STRICT_RESUME_REFUSAL."""
    lines = [line[:300] for line in str(text or "").splitlines() if STRICT_RESUME_REFUSAL in line]
    return {"count": len(lines), "last": lines[-limit:]}


def clone_schema_sql(ts_root):
    """(sql text, provenance) of the TS migrations the clone Postgres needs (CLONE_SCHEMA_MIGRATIONS)."""
    root, parts, files = Path(ts_root), [], []
    for rel in CLONE_SCHEMA_MIGRATIONS:
        data = (root / rel).read_bytes()
        parts.append(data.decode())
        files.append({"path": rel, "sha256": hashlib.sha256(data).hexdigest()})
    sql = "\n".join(parts)
    if "CREATE TABLE IF NOT EXISTS service_heartbeats" not in sql:
        raise ValueError("clone schema migrations no longer define service_heartbeats")
    return sql, {"migrations": files}


# Production parity snapshot (``--production-parity``, coordinator 2026-10-16): the Kafka configuration G12 must mirror
# and the deployed writer, both derived at run time, never hard-coded here.
# - auto.leader.rebalance.enable: KAFKA_AUTO_LEADER_REBALANCE_ENABLE of the production kafka1-3 containers (docker
#   inspect env; absent = the Kafka default true; brokers that disagree = a roll in progress -> refused).
# - internal-topic decision C: scripts/kafka_internal_topic_compaction.PLAN (PLAN_STATUS MEASURED, owner "B + C 10
#   min"); production's effective overrides are confirmed read-only with that tool's --observe before the run.
# - A': md.latest.v2 delete.retention.ms from the budget (scripts/kafka_md_latest_marker_retention.contract()).
PARITY_SCHEMA = "p184.g12.production-parity.v2"  # v2: + ``effective`` (Codex K3); v1 documents are refused
PRODUCTION_KAFKA = tuple(f"qdl_v2_stable_candidate-kafka{n}-1" for n in BROKERS)
REBALANCE_ENV = "KAFKA_AUTO_LEADER_REBALANCE_ENABLE"
STATE_TOPIC_PROFILE_KEYS = {"latest": ("delete.retention.ms",)}


def production_rebalance(inspects):
    """bool auto.leader.rebalance.enable of the production brokers from their docker inspect records."""
    values = {}
    for name in PRODUCTION_KAFKA:
        record = inspects.get(name)
        if not record:
            raise ValueError(f"production broker {name} not inspected")
        env = dict(item.split("=", 1) for item in (record.get("Config") or {}).get("Env") or [] if "=" in item)
        raw = env.get(REBALANCE_ENV)
        if raw is None:
            values[name] = True  # Kafka's default
        elif raw.strip().lower() in ("true", "false"):
            values[name] = raw.strip().lower() == "true"
        else:
            raise ValueError(f"{name} {REBALANCE_ENV}={raw!r} is not true|false")
    if len(set(values.values())) != 1:
        raise ValueError(f"production brokers disagree on auto.leader.rebalance.enable {values} (roll in progress)")
    return next(iter(values.values()))


def kafka_profile(auto_leader_rebalance, internal_topics, latest_delete_retention_ms):
    """The Kafka profile G12 mirrors; validated (validate_kafka_profile)."""
    return validate_kafka_profile({
        "auto.leader.rebalance.enable": auto_leader_rebalance,
        "internal_topics": {t: dict(internal_topics.get(t) or {}) for t in INTERNAL_TOPICS},
        "state_topics": {"latest": {"delete.retention.ms": str(latest_delete_retention_ms)}}})


def validate_kafka_profile(profile):
    if not isinstance(profile, dict) or type(profile.get("auto.leader.rebalance.enable")) is not bool:
        raise ValueError("kafka profile: auto.leader.rebalance.enable must be a bool")
    internal = profile.get("internal_topics")
    if not isinstance(internal, dict) or set(internal) != set(INTERNAL_TOPICS):
        raise ValueError(f"kafka profile: internal_topics must name exactly {INTERNAL_TOPICS}")
    for topic, keys in internal.items():
        if not isinstance(keys, dict) or set(keys) - set(DECISION_C_KEYS) or not all(
                isinstance(v, str) and v.isdigit() for v in keys.values()):
            raise ValueError(f"kafka profile: {topic} may only set {DECISION_C_KEYS} to digit strings")
    state = profile.get("state_topics")
    if not isinstance(state, dict) or set(state) != set(STATE_TOPIC_PROFILE_KEYS):
        raise ValueError("kafka profile: state_topics must name exactly " + ",".join(STATE_TOPIC_PROFILE_KEYS))
    for topic, keys in state.items():
        if not isinstance(keys, dict) or set(keys) != set(STATE_TOPIC_PROFILE_KEYS[topic]) or not all(
                isinstance(v, str) and v.isdigit() for v in keys.values()):
            raise ValueError(f"kafka profile: state topic {topic} must set exactly {STATE_TOPIC_PROFILE_KEYS[topic]}")
    return profile


EFFECTIVE_SCHEMA = "p184.g12.production-kafka-effective.v1"
PRODUCTION_LATEST_TOPIC = "md.latest.v2"


def _canonical_sha256(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def seal_effective(observation):
    """The observation plus ``sha256`` over its canonical JSON (the time it was taken is kept outside the hash)."""
    body = {k: v for k, v in observation.items() if k not in ("sha256", "taken_utc")}
    return {**observation, "sha256": _canonical_sha256(body)}


def observe_production_kafka(runner, inspect, now=None):
    """Read-only EFFECTIVE production Kafka configuration (Codex K3, 2026-10-16): the dynamic topic overrides of the
    two internal topics and md.latest.v2 from ``kafka-configs --describe`` (admin container ``runner``,
    ``scripts/kn_state_topics_packet.AdminContainerRunner``) and every production broker's
    ``KAFKA_AUTO_LEADER_REBALANCE_ENABLE`` + image from docker inspect. Any failed read raises (never a default)."""
    from scripts.kn_state_topics_packet import parse_topic_overrides
    topics = {}
    for topic in (*INTERNAL_TOPICS, PRODUCTION_LATEST_TOPIC):
        result = runner.run(["kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic, "--describe"])
        if result.returncode != 0:
            raise RuntimeError(f"kafka-configs describe {topic} failed rc={result.returncode}: "
                               f"{result.stderr.strip()[-300:]}")
        topics[topic] = parse_topic_overrides(result.stdout, topic)
    brokers = {}
    for name in PRODUCTION_KAFKA:
        record = inspect(name)
        if not record:
            raise RuntimeError(f"production broker {name} not inspected")
        env = dict(item.split("=", 1) for item in (record.get("Config") or {}).get("Env") or [] if "=" in item)
        brokers[name] = {REBALANCE_ENV: env.get(REBALANCE_ENV), "image": record.get("Image")}
    return seal_effective({"schema": EFFECTIVE_SCHEMA,
                           "taken_utc": now or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                           "topics": topics, "brokers": brokers})


def effective_problems(effective, profile):
    """Why an effective observation is not the profile's production (empty list = consistent): hash, every topic
    read, decision-C keys, the other internal-topic overrides exactly as frozen (cleaner settings restored), the
    temporary cleaner key absent, md.latest A', and auto.leader.rebalance on EVERY broker."""
    from scripts.kafka_internal_topic_compaction import FROZEN_OTHERS, TEMP_OVERRIDES
    if not isinstance(effective, dict) or effective.get("schema") != EFFECTIVE_SCHEMA:
        return [f"effective observation schema is not {EFFECTIVE_SCHEMA}"]
    problems = []
    if seal_effective(effective)["sha256"] != effective.get("sha256"):
        problems.append("effective observation sha256 does not match its content")
    topics = effective.get("topics") or {}
    for topic in INTERNAL_TOPICS:
        overrides = topics.get(topic)
        if not isinstance(overrides, dict):
            problems.append(f"{topic} overrides not observed")
            continue
        c_keys = {k: v for k, v in overrides.items() if k in DECISION_C_KEYS}
        if c_keys != profile["internal_topics"][topic]:
            problems.append(f"{topic} decision-C keys {c_keys} != profile {profile['internal_topics'][topic]}")
        temp = sorted(k for k in TEMP_OVERRIDES.get(topic, {}) if k in overrides)
        if temp:
            problems.append(f"{topic} temporary cleaner key(s) still present: {temp}")
        others = {k: v for k, v in overrides.items() if k not in DECISION_C_KEYS}
        if others != FROZEN_OTHERS[topic]:
            problems.append(f"{topic} other overrides {others} != frozen {FROZEN_OTHERS[topic]}")
    latest = topics.get(PRODUCTION_LATEST_TOPIC)
    if not isinstance(latest, dict):
        problems.append(f"{PRODUCTION_LATEST_TOPIC} overrides not observed")
    else:
        for key, value in profile["state_topics"]["latest"].items():
            if latest.get(key) != value:
                problems.append(f"{PRODUCTION_LATEST_TOPIC} {key}={latest.get(key)!r} != profile {value}")
    brokers = effective.get("brokers") or {}
    for name in PRODUCTION_KAFKA:
        row = brokers.get(name)
        if not isinstance(row, dict):
            problems.append(f"broker {name} not observed")
            continue
        raw = row.get(REBALANCE_ENV)
        value = True if raw is None else {"true": True, "false": False}.get(str(raw).strip().lower())
        if value is not profile["auto.leader.rebalance.enable"]:
            problems.append(f"broker {name} {REBALANCE_ENV}={raw!r} != profile "
                            f"{profile['auto.leader.rebalance.enable']}")
    return problems


def validate_parity(doc):
    """A ``--production-parity`` document: schema, kafka profile, writer reference (image digest, env view keys) and
    the effective production observation consistent with the profile (Codex K3)."""
    if not isinstance(doc, dict) or doc.get("schema") != PARITY_SCHEMA:
        raise ValueError(f"production parity schema must be {PARITY_SCHEMA}")
    validate_kafka_profile(doc.get("kafka"))
    problems = effective_problems(doc.get("effective"), doc["kafka"])
    if problems:
        raise ValueError("production parity: effective Kafka observation " + "; ".join(problems[:4]))
    writer = doc.get("writer")
    if not isinstance(writer, dict) or not DIGEST_RE.fullmatch(str(writer.get("image"))):
        raise ValueError("production parity: writer.image must be a sha256 image ID")
    env = writer.get("env")
    if not isinstance(env, dict) or env != writer_env_view(env) or not env.get("MARKET_DATA_NATIVE_FEEDS"):
        raise ValueError("production parity: writer.env must be a native writer env view (no other key)")
    return doc


def production_parity_snapshot(inspect, observe, now=None):
    """Read-only (docker inspect + committed repo files + ``observe()`` = observe_production_kafka): the parity
    document of the CURRENT production. Refuses (ValueError) when the effective configuration is not the committed
    plans' (e.g. the temporary cleaner key still present, a broker not yet rolled)."""
    from scripts.kafka_internal_topic_compaction import PLAN, PLAN_STATUS, PLAN_EVIDENCE
    from scripts.kafka_md_latest_marker_retention import contract
    if PLAN_STATUS != "MEASURED":
        raise ValueError(f"decision C plan is {PLAN_STATUS}, not MEASURED")
    brokers = {name: inspect(name) for name in PRODUCTION_KAFKA}
    terms = contract()
    doc = {"schema": PARITY_SCHEMA, "taken_utc": now or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
           "kafka": kafka_profile(production_rebalance(brokers), PLAN, terms["target"]),
           "kafka_brokers": {name: {"image": (r or {}).get("Image"),
                                    REBALANCE_ENV: dict(item.split("=", 1) for item in
                                                        ((r or {}).get("Config") or {}).get("Env") or []
                                                        if "=" in item).get(REBALANCE_ENV)}
                             for name, r in brokers.items()},
           "writer": writer_reference(inspect(TS_WRITER)),
           "effective": observe(),
           "sources": {"auto.leader.rebalance.enable": f"docker inspect {', '.join(PRODUCTION_KAFKA)} env "
                                                       f"{REBALANCE_ENV} (absent = Kafka default true)",
                       "internal_topics": "scripts/kafka_internal_topic_compaction.py PLAN (MEASURED): "
                                          + str(PLAN_EVIDENCE)[:160],
                       "state_topics.latest": "config/v2/kn-v220-candidate-budget.json retention.state_topics "
                                              "md.latest.v2 delete.retention.ms (decision A')",
                       "writer": f"docker inspect {TS_WRITER} (image + env view: MARKET_DATA_NATIVE_* and "
                                 "WRITER_TUNING_KEYS only)",
                       "effective": "kafka-configs --describe (admin container, read-only) of "
                                    f"{', '.join(INTERNAL_TOPICS)}, {PRODUCTION_LATEST_TOPIC} + docker inspect env of "
                                    "every broker; sha256 bound into the run receipt before and after the faults"}}
    return validate_parity(doc)


def production_parity(profile, snapshots):
    """G12 broker_config vs the mirrored production profile: MATCH only when the effective decision-C keys of both
    internal topics and the latest topic's A' key equal the profile before AND after the faults."""
    if not profile:
        return "NO_PROFILE"
    if not {"before", "after"} <= set(snapshots) or any(
            "overrides" not in (snapshots[p].get(t) or {}) for p in ("before", "after")
            for t in (*INTERNAL_TOPICS, "latest")):
        return "NOT_MEASURED"
    want = {**{t: profile["internal_topics"][t] for t in INTERNAL_TOPICS}, **profile["state_topics"]}
    seen = {}
    for phase in ("before", "after"):
        seen[phase] = {t: {k: v for k, v in snapshots[phase][t]["overrides"].items()
                           if k in (DECISION_C_KEYS if t in INTERNAL_TOPICS else STATE_TOPIC_PROFILE_KEYS[t])}
                       for t in want}
    if seen["before"] != seen["after"]:
        return "CHANGED_DURING_RUN"
    return "MATCH" if seen["before"] == want else "MISMATCH"


def heartbeat_integrity_exposed(sampler_rows):
    """venue -> None when the clone's latest heartbeat carries complete typed trade_integrity over its full worker
    roster, else why not (report-only: the integrity part never gates G12)."""
    from scripts.p184_g9_g10_writer_counters import _native_sample_problem
    out = {}
    for venue in VENUES:
        samples = native_heartbeat_samples(sampler_rows, venue)
        out[venue] = (_native_sample_problem(samples[-1], 45.0) if samples else "no heartbeat sample")
    return out


def writer_clone_source(ts_root=None):
    """Provenance of the TS WriterClone module G12 loads: path, sha256 and the TS checkout HEAD (host git read)."""
    root = Path(ts_root or TS_ROOT)
    path = root / "scripts/p184_dr05_isolated.py"
    head = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True, timeout=30)
    dirty = subprocess.run(["git", "-C", str(root), "status", "--porcelain", "--untracked-files=no", "--",
                            "scripts/p184_dr05_isolated.py"], capture_output=True, text=True, timeout=30)
    return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "ts_head": head.stdout.strip() if head.returncode == 0 else None,
            "tracked_edit": bool(dirty.stdout.strip()) if dirty.returncode == 0 else None}


def load_writer_clone(ts_root=None):
    import importlib.util
    path = Path(ts_root or TS_ROOT) / "scripts/p184_dr05_isolated.py"
    spec = importlib.util.spec_from_file_location("ts_p184_dr05_isolated", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def labelled_run(ns):
    """WriterClone's runner with this namespace's label on every container it starts."""
    def run(*args, check=True, capture=True, timeout=120):
        args = list(args)
        if args[:3] == ["docker", "run", "-d"]:
            args[3:3] = ["--label", f"{LABEL}={ns}"]
        elif args[:3] == ["docker", "rm", "-f"]:
            # WriterClone removes with rm -f; the redis image's VOLUME /data would
            # otherwise stay behind as an anonymous volume (G12 run 2026-10-10: 2 left).
            args[3:3] = ["-v"]
        result = subprocess.run(args, capture_output=capture, text=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError(f"{args[:3]} -> {result.returncode}: {result.stderr[-300:]}")
        return result.stdout.strip() if capture else ""
    return run


class Run:
    def __init__(self, base, export, ns, images=None, query_code="worktree"):
        self.images = resolve_image_set(images)
        if query_code not in QUERY_CODE:
            raise ValueError("query_code must be one of " + ",".join(QUERY_CODE))
        self.query_code = query_code
        self.base = Path(base).resolve()
        self.work, self.evidence = self.base / "work", self.base / "evidence"
        self.export = Path(export).resolve()
        self.ns = namespace(ns)
        self.clones = {}
        self.expected = set()
        self.started = time.monotonic()
        self.guard0 = None
        self.last_guard = 0.0
        self.last_stats = 0.0
        self.stats_skipped = 0
        self.events_proc = None
        self.followers = []  # (role, docker logs process, tail process)
        self.lives = {}
        self.predicate = None  # frozen in prepare()
        self.adapter_mode = "SYNTHETIC"
        self.capture_raw_sha256 = None
        self.core_members = 1
        self.profile = core_profile()  # core-group runs only; the defaults are the B3 harness
        # Brokers' auto.leader.rebalance.enable (``--auto-leader-rebalance``); Kafka's default and production's
        # value on 2026-10-16 is true. Recorded in handoff.json, the notes and verdict.json.
        self.auto_leader_rebalance = True
        self.writer_precheck = {}  # venue -> writer_clone_precheck + heartbeat trade_integrity (report)
        # ``--production-parity`` document (validate_parity): the Kafka profile this run mirrors and the deployed
        # writer reference; None only for the core-group modes and unit tests.
        self.parity = None
        self.ts_root = TS_ROOT  # ``--ts-root``: the TS checkout WriterClone is loaded from (provenance recorded)

    def use_capture_adapter(self, raw_sha256):
        """TEST_ONLY real-capture replay: keep the source catalog adapter_version (CAPTURE adapter mode).
        Before ``prepare``; ``prepare`` then requires test-clock core/stream images and seals the pin."""
        if not isinstance(raw_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", raw_sha256):
            raise ValueError("pinned raw capture sha256 required")
        if self.work.exists():
            raise ValueError("adapter mode is fixed before prepare")
        self.adapter_mode, self.capture_raw_sha256 = "CAPTURE", raw_sha256

    # -- container exit states
    def start_events(self):
        """Follow this namespace's die/oom/kill events into the evidence for the whole run (bounded)."""
        now = time.time()
        out = (self.evidence / "container-events.jsonl").open("a")
        err = (self.evidence / "container-events.stderr").open("a")
        try:
            self.events_proc = subprocess.Popen(
                events_argv(self.ns, now - 1, now + LIMITS["deadline_s"] + 900), stdout=out, stderr=err,
                stdin=subprocess.DEVNULL)
        finally:
            out.close()
            err.close()

    def exit_states(self, roles=(), wait_s=3.0):
        """Exit states recorded so far; waits (bounded) until each named role has a die event."""
        limit = time.monotonic() + wait_s
        while True:
            states = exit_states(load_jsonl(self.evidence / "container-events.jsonl"))
            if all(role in states for role in roles) or time.monotonic() >= limit:
                return states
            time.sleep(.2)

    def follow_logs(self, role):
        """Follow one product role's output into ``evidence/logs/<role>.life<n>.log`` (last FOLLOW_TAIL_BYTES,
        written when the container exits or teardown ends the follower)."""
        if role not in FOLLOWED_ROLES:
            return None
        logs = self.evidence / "logs"
        logs.mkdir(exist_ok=True)
        life = self.lives[role] = self.lives.get(role, 0) + 1
        keep = FOLLOW_TAIL_BYTES_CORE_GROUP if role in CORE_MEMBERS and getattr(
            self, "core_members", 1) > 1 else FOLLOW_TAIL_BYTES
        seconds = max(1, int(LIMITS["deadline_s"] + 900 - (time.monotonic() - self.started)))
        with (logs / f"{role}.life{life}.log").open("xb") as out:
            source = subprocess.Popen(follow_argv(self.ns, role, seconds), stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
            try:
                sink = subprocess.Popen(["tail", "-c", str(keep)], stdin=source.stdout, stdout=out,
                                        stderr=subprocess.DEVNULL)
            except BaseException:
                source.kill()
                source.wait(timeout=5)
                raise
            finally:
                source.stdout.close()  # the tail owns the pipe
        self.followers.append((role, source, sink))
        return life

    def stop_followers(self, wait_s=10.0):
        """After teardown removed the containers: let each follower finish, then end any that did not."""
        followers, self.followers = self.followers, []
        limit = time.monotonic() + wait_s
        forced = []
        for role, source, sink in followers:
            for proc in (source, sink):
                try:
                    proc.wait(timeout=max(0.1, limit - time.monotonic()))
                except subprocess.TimeoutExpired:
                    forced.append(role)
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=5)
        return {"followers": len(followers), "forced": sorted(set(forced))}

    def stop_events(self):
        proc, self.events_proc = self.events_proc, None
        if proc is None:
            return None
        time.sleep(1.0)  # the last teardown die events reach the follower
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        return proc.returncode

    # -- evidence
    def note(self, step, **fields):
        row = {"t": time.time_ns(), "step": step, **fields}
        with (self.evidence / "orchestrator.jsonl").open("a") as handle:
            handle.write(json.dumps(row, default=str) + "\n")
        print(step, json.dumps(fields, default=str)[:400], flush=True)

    def verify_image(self, role):
        """Provenance: the started container runs exactly the passed image for its role."""
        name, expected = f"{self.ns}-{role}", self.images[role_image_key(role)]
        actual = container_image(name)
        row = {"t": time.time_ns(), "role": role, "image_role": role_image_key(role),
               "expected": expected, "actual": actual, "match": actual == expected}
        with (self.evidence / "provenance.jsonl").open("a") as handle:
            handle.write(json.dumps(row) + "\n")
        if actual != expected:
            raise RuntimeError(f"STOP provenance: {name} runs {actual}, expected {expected}")

    def write_writer_precheck(self):
        (self.evidence / WRITER_PRECHECK_FILE).write_text(json.dumps(self.writer_precheck, indent=2,
                                                                     sort_keys=True) + "\n")

    def broker_config(self):
        snapshots = getattr(self, "internal_topics", {})
        profile = (getattr(self, "parity", None) or {}).get("kafka")
        effective = getattr(self, "production_effective", None)
        return {"auto.leader.rebalance.enable": self.auto_leader_rebalance, "internal_topics": snapshots,
                "internal_topic_parity": internal_topic_parity(snapshots), "kafka_profile": profile,
                "production_parity": production_parity(profile, snapshots),
                "production_effective": effective}

    def bind_production(self, phase, observe):
        """Observe the EFFECTIVE production Kafka configuration at ``phase`` and bind it to the receipt next to the
        parity snapshot's hash. ``before``: a failed read or any drift from the snapshot STOPs the run (nothing to
        measure against); ``after``: recorded, the credit decides (Codex K3)."""
        parity_sha = self.parity["effective"]["sha256"]
        self.production_effective = getattr(self, "production_effective", None) or {"parity_sha256": parity_sha}
        try:
            observed = observe()
        except Exception as error:  # noqa: BLE001 - recorded; before -> STOP below
            observed = {"error": f"{type(error).__name__}: {error}"[:300]}
        self.production_effective[phase] = observed
        drift = "error" in observed or observed.get("sha256") != parity_sha \
            or effective_problems(observed, self.parity["kafka"])
        if phase == "before" and drift:
            raise RuntimeError("STOP production effective Kafka config is not the parity snapshot's: "
                               + json.dumps(observed.get("error") or effective_problems(observed, self.parity["kafka"])
                                            or "sha256 drift")[:300])
        return observed

    def apply_kafka_profile(self):
        """Mirror production's dynamic topic configs (decision C) on this run's internal topics before the baseline;
        STOP unless the effective decision-C keys then equal the profile (A' is set at topic creation)."""
        profile = self.parity["kafka"]
        self.internal_topics = getattr(self, "internal_topics", {})
        applied = json.loads(sh(self.helper(self.ns, self.work, ["--apply-internal-configs", "/work"]),
                                timeout=300).strip().splitlines()[-1])
        self.internal_topics["applied"] = applied
        self.note("internal_topics", phase="applied", configs=applied)
        verdict = production_parity(profile, {"before": applied, "after": applied})
        if verdict != "MATCH":
            raise RuntimeError(f"STOP kafka profile not mirrored after apply: {verdict}")

    def record_internal_topics(self, phase):
        """Effective internal-topic overrides at ``phase`` (before the faults / after them); a failed read is
        recorded and leaves the parity NOT_MEASURED, never a C claim."""
        self.internal_topics = getattr(self, "internal_topics", {})
        try:
            self.internal_topics[phase] = json.loads(sh(self.helper(
                self.ns, self.work, ["--internal-configs", "/work"]), timeout=120).strip().splitlines()[-1])
        except Exception as error:  # noqa: BLE001 - evidence, not a stop
            self.internal_topics[phase] = {"error": f"{type(error).__name__}: {error}"[:300]}
        self.note("internal_topics", phase=phase, configs=self.internal_topics[phase])

    def topology(self, topic_id=""):
        return topology(self.ns, self.work, topic_id, images=self.images, query_code=self.query_code,
                        core_members=self.core_members, core_cpus=self.profile["core_cpus"])

    def helper(self, *args, **kwargs):
        return helper_argv(*args, images=self.images, **kwargs)

    def fault_row(self, row):
        with (self.evidence / "faults.jsonl").open("a") as handle:
            handle.write(json.dumps(row) + "\n")

    # -- checks
    def check(self, faulted=()):
        resources = host_resources()
        if resources["disk_free_bytes"] < LIMITS["disk_free_bytes"]:
            raise RuntimeError(f"STOP disk free {resources['disk_free_bytes']} < 20 GB")
        if resources["mem_available_bytes"] < LIMITS["mem_available_bytes"]:
            raise RuntimeError(f"STOP memory available {resources['mem_available_bytes']} < 3 GiB")
        missing = {f"{self.ns}-{r}" for r in self.expected - set(faulted)} - running_names(self.ns)
        if missing:
            roles = sorted(name[len(self.ns) + 1:] for name in missing)
            states = self.exit_states(roles)
            detail = {role: ({k: states[role][k] for k in ("exit_code", "oom_killed")} if role in states
                             else "NO_DIE_EVENT") for role in roles}
            self.note("role_exited", roles=roles, exit_states={r: states.get(r) for r in roles})
            raise RuntimeError("STOP role exited: " + ",".join(sorted(missing)) + " " + json.dumps(detail))
        now = time.monotonic()
        if now - self.last_guard >= 60:
            self.last_guard = now
            breaches, recreated = guard_breaches(self.guard0, production_guard())
            if recreated and recreated != getattr(self, "recreated", None):
                self.recreated = recreated
                self.note("production_external_recreate_observed", containers=recreated)
            if breaches:
                raise RuntimeError("STOP production guard breach: " + json.dumps(breaches))
        if now - self.last_stats >= 30:
            self.last_stats = now
            self.sample_stats()
        return resources

    def sample_stats(self, attempts=2):
        """One `docker stats` sample of the running roles; returns rows written, or None when skipped.

        TOCTOU (N1 r2b5a-6k-1): a role that exits normally between running_names() and the
        sample makes docker stats fail (EOF). On a failure the roles are re-listed: if every
        sampled role is still running the failure is real and raises; otherwise the sample is
        retried once with the survivors, and if that also fails with a role gone the sample is
        skipped, counted (self.stats_skipped), noted in orchestrator.jsonl and marked in
        stats.jsonl. Never a silent pass: an expected role that is gone still stops the next
        check() with its exit state.
        """
        names = sorted(running_names(self.ns))
        gone = set()
        for _ in range(attempts):
            if not names:
                break
            result = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}", *names],
                                    capture_output=True, text=True, timeout=60)
            if result.returncode == 0:
                rows = 0
                with (self.evidence / "stats.jsonl").open("a") as handle:
                    for line in result.stdout.splitlines():
                        item = json.loads(line)
                        handle.write(json.dumps({"t": time.time_ns(), "name": item.get("Name"),
                                                 "cpu": item.get("CPUPerc"), "mem": item.get("MemUsage")}) + "\n")
                        rows += 1
                return rows
            still = running_names(self.ns)
            if set(names) <= still:
                raise RuntimeError(f"['docker', 'stats', '--no-stream', '--format'] -> {result.returncode}: "
                                   f"{result.stderr.strip()[-400:]}")
            gone |= set(names) - still
            names = sorted(set(names) & still)
        self.stats_skipped += 1
        roles = sorted(name[len(self.ns) + 1:] for name in gone)
        self.note("stats_skipped_role_exited", roles=roles, skipped_total=self.stats_skipped)
        with (self.evidence / "stats.jsonl").open("a") as handle:
            handle.write(json.dumps({"t": time.time_ns(), "skipped": "ROLE_EXITED_DURING_SAMPLE",
                                     "roles": roles, "skipped_total": self.stats_skipped}) + "\n")
        return None

    def wait(self, seconds, faulted=(), until=None, step=5.0):
        limit = time.monotonic() + seconds
        while time.monotonic() < limit:
            self.check(faulted)
            if until is not None:
                value = until()
                if value:
                    return value
            time.sleep(min(step, max(0.0, limit - time.monotonic())))
        return until() if until is not None else None

    def start(self, role, topic_id):
        argv = self.topology(topic_id)[role]
        sh(argv, timeout=60)
        self.expected.add(role)
        self.verify_image(role)
        self.follow_logs(role)

    # -- phases
    def prepare(self):
        self.base.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.work.mkdir(mode=0o700)
        self.evidence.mkdir(mode=0o755)
        resources = host_resources()
        self.note("preflight", **resources)
        if resources["disk_free_bytes"] < LIMITS["disk_free_bytes"]:
            raise RuntimeError("disk below 20 GB before start")
        records = {}
        for role, image in self.images.items():
            records[role] = image_record(image)
            if records[role]["id"] != image:
                raise RuntimeError("local image mismatch " + image)
        capture = {}
        if self.adapter_mode == "CAPTURE":
            require_test_clock_images(records)
            capture = {"adapter_mode": "CAPTURE", "capture_raw_sha256": self.capture_raw_sha256,
                       "test_clock_images": True}
        (self.evidence / "images.json").write_text(json.dumps(
            {"images": self.images, "local": records, "query_code": self.query_code,
             "defaults_overridden": sorted(r for r in self.images if self.images[r] != IMAGES[r])},
            indent=2) + "\n")
        self.note("image_set", images=self.images, query_code=self.query_code)
        if owned_names(self.ns) or sh(["docker", "ps", "-aq", "--filter", "name=" + self.ns]).strip():
            raise RuntimeError("namespace already in use")
        self.guard0 = production_guard()
        self.note("production_guard_before", containers=len(self.guard0), guard=self.guard0)
        self.start_events()
        self.volumes0 = len(sh(["docker", "volume", "ls", "-q"]).split())
        self.networks0 = len(sh(["docker", "network", "ls", "-q"]).split())
        (self.work / "handoff.json").write_text(json.dumps({
            "namespace": self.ns, "status": "PREPARED_HANDOFF_NOT_RUN", "provenance": "TEST_ONLY",
            "level": "ISOLATED_FAULT", "acceptance": False, "material_ready": False,
            "images": dr05_images(self.images), "g12_images": self.images,
            "query_code": self.query_code, "broker_config": self.broker_config(),
            "kafka_profile": (self.parity or {}).get("kafka"), **capture}, indent=2) + "\n")
        self.note("broker_config", **self.broker_config())
        sh(self.helper(self.ns, self.work, ["--material", "/work", "--core-config", "/export.json"],
                       network="none", script="scripts/p184_dr05_dl_fault.py",
                       extra=["-v", f"{self.export}:/export.json:ro"]), timeout=180)
        packet = json.loads((self.work / "packet.json").read_text())
        packet["bootstrap"] = BOOTSTRAP
        (self.work / "packet.json").write_text(json.dumps(packet))
        feeds = sh(self.helper(self.ns, self.work, ["--feeds", "/work"], network="none"), timeout=180)
        self.note("feeds_sealed", result=feeds.strip()[-400:])
        password = broker_password((self.work / "broker.properties").read_text())
        for node in BROKERS:
            path = self.work / f"broker-{node}.properties"
            with path.open("x") as handle:
                os.chmod(path, 0o600)
                handle.write(broker_properties(password, node,
                                               self.profile["initial_rebalance_delay_ms"] or 0,
                                               self.auto_leader_rebalance))
            (self.work / f"kafka-{node}").mkdir(mode=0o700)
        self.note("material_ready", namespace=self.ns, volumes_before=self.volumes0,
                  networks_before=self.networks0)
        # Freeze the caught-up predicate (version + parameters) before any data exists.
        self.predicate = caught_up_predicate(json.loads((self.work / "core.json").read_text()), PUBLISH_HZ)
        with (self.evidence / PREDICATE_FILE).open("x") as handle:
            handle.write(json.dumps(self.predicate, indent=2, sort_keys=True) + "\n")
        self.note("caught_up_predicate_frozen", predicate=self.predicate)

    def up(self):
        sh(self.topology()["net"])
        self.expected.add("net")
        self.verify_image("net")
        for node in BROKERS:
            self.start(f"kafka-{node}", "")
        result = sh(self.helper(self.ns, self.work, ["--topics", "/work"]), timeout=180)
        self.note("topics", result=result.strip()[-400:])
        topic_id = json.loads((self.work / "packet.json").read_text())["slice"]["topic_id"]
        self.topic_id = topic_id
        for role in SERVICE_ORDER:
            self.start(role, topic_id)
        limit = time.monotonic() + LIMITS["role_return_s"]
        while True:
            self.check()
            probe = subprocess.run(self.helper(self.ns, self.work, ["--probe", "/work"],
                                               script="scripts/p184_dr05_dl_fault.py"),
                                   capture_output=True, text=True, timeout=60)
            if probe.returncode == 0:
                self.note("services_reachable", out=probe.stdout.strip()[-300:])
                break
            if time.monotonic() >= limit:
                raise RuntimeError("services not reachable: " + probe.stderr.strip()[-400:])
            time.sleep(2)
        remaining = LIMITS["deadline_s"] - (time.monotonic() - self.started) - 60
        sh(self.helper(self.ns, self.work, ["--monitor", "/work", "--out", "/evidence/monitor.jsonl",
                                            "--seconds", str(int(remaining))], role="monitor", detach=True,
                       evidence=self.evidence, cpus="0.25", memory="256m"))
        self.expected.add("monitor")
        self.verify_image("monitor")
        sh(self.helper(self.ns, self.work, ["--publish", "/work", "--out", "/evidence/publish.jsonl",
                                            "--seconds", str(int(remaining)), "--hz", str(PUBLISH_HZ)],
                       role="publisher",
                       detach=True, evidence=self.evidence, cpus="0.25", memory="256m"))
        self.expected.add("publisher")
        self.verify_image("publisher")
        self.note("publisher_monitor_started", seconds=int(remaining))
        iso = load_writer_clone(self.ts_root)
        self.writer_precheck["writer_clone_source"] = writer_clone_source(self.ts_root)
        import tempfile
        logs = self.work / "tslogs"
        logs.mkdir(mode=0o700)
        tempfile.tempdir = str(logs)  # WriterClone's stderr dir stays under the run, not /tmp
        run = labelled_run(self.ns)
        for venue in VENUES:
            redis_port, pg_port = CLONE_PORTS[venue]
            spec = json.loads((self.work / f"ts-consumer.{venue.lower()}.json").read_text())
            clone = iso.WriterClone(self.images["ts"], redis_image=self.images["redis"],
                                    postgres_image=self.images["postgres"],
                                    venues=(venue,), network="container:" + self.ns + "-net",
                                    redis_port=redis_port, pg_port=pg_port, consumer=spec,
                                    dl_work=str(self.work), run=run, tag=venue.lower())
            label = venue.lower()
            clone.name = f"{self.ns}-tswriter-{label}"
            clone.redis_name = f"{self.ns}-tsredis-{label}"
            clone.pg_name = f"{self.ns}-tspg-{label}"
            clone.deadline = clone.clock() + LIMITS["clone_ready_s"]
            # Lifecycle (see WriterClone lifecycle above): durable state bind + BOOTSTRAP first, STRICT after.
            destination = writer_state_destination(clone.inspect)
            clone.state_dir = self.work / f"tsstate-{label}"
            clone.state_dir.mkdir(mode=0o700)
            os.chmod(clone.state_dir, 0o777)  # the writer's own uid writes the checkpoints; erased at teardown
            deployed_args = clone.writer_args
            clone.lifecycle = {"phase": "BOOTSTRAP"}
            clone.writer_args = (lambda c=clone, base=deployed_args, dest=destination: lifecycle_writer_args(
                base(), c.image, str(c.state_dir), dest, c.lifecycle["phase"]))
            strict_argv = lifecycle_writer_args(deployed_args(), clone.image, str(clone.state_dir), destination,
                                                "STRICT")
            check = writer_clone_precheck(strict_argv, self.images["ts"], writer_reference(clone.inspect),
                                          (self.parity or {}).get("writer"))
            delta = bootstrap_env_delta(clone.writer_args(), strict_argv)
            check["lifecycle"] = {"phase": "BOOTSTRAP", "state_destination": destination,
                                  "bootstrap_env_delta": delta, "strict_resume": "PENDING"}
            if delta != {REQUIRE_RESUME_KEY: {"bootstrap": "0", "strict": "1"}}:
                check["ok"] = False
                check["problems"].append(f"bootstrap env differs from the deployed env beyond the flag: {delta}")
            sql, check["clone_schema"] = clone_schema_sql(self.ts_root)
            clone._wait_ready = (lambda c=clone, ready=clone._wait_ready, sql=sql: (ready(), apply_clone_schema(c, sql)))
            self.writer_precheck[venue] = check
            self.write_writer_precheck()
            self.note("writer_clone_precheck", venue=venue, **check)
            if not check["ok"]:
                raise RuntimeError(f"STOP writer clone {venue} is not the production native writer: "
                                   + "; ".join(check["problems"]))
            self.clones[venue] = clone
            clone.__enter__()
            self.expected |= {f"tswriter-{label}", f"tsredis-{label}", f"tspg-{label}"}
            for kind in ("tswriter", "tsredis", "tspg"):
                self.verify_image(f"{kind}-{label}")
            redis_cpu = clone_redis_cpu_parity(sh(["docker", "inspect", "--format", "{{.HostConfig.NanoCpus}}",
                                                   clone.redis_name], timeout=30).strip() or 0)
            check["redis_cpu"] = redis_cpu
            self.write_writer_precheck()
            self.note("writer_clone_redis_cpu", venue=venue, **redis_cpu)
            if not redis_cpu["ok"]:
                raise RuntimeError(f"STOP writer clone {venue} Redis CPU below production parity: {redis_cpu}")
        self.note("ts_clones_started", names=[c.name for c in self.clones.values()])
        sh(sampler_argv(self.ns, self.evidence, remaining - 30, images=self.images))
        self.expected.add("sampler")
        self.verify_image("sampler")
        started = time.time_ns()
        if not self.wait(LIMITS["clone_ready_s"], until=lambda: self.views_fresh(started)):
            raise RuntimeError("STOP TS clones never showed a fresh APPROVED book view")
        self.note("ts_views_fresh")
        # Fail fast instead of an hour of NOT_MEASURED: every clone applies TRADE, QUOTE and a closed BAR.
        if not self.wait(LIMITS["md_ready_s"], until=lambda: not md_missing(
                load_jsonl(self.evidence / "sampler.jsonl"))):
            missing = md_missing(load_jsonl(self.evidence / "sampler.jsonl"))
            self.record_refusals("bootstrap_refusals")
            raise RuntimeError("STOP TS clones never applied: " + ",".join(missing))
        self.note("ts_market_data_applied", phase="BOOTSTRAP")
        self.strict_restart()
        # Report-only: does each clone's heartbeat expose the native typed trade_integrity (integrity report)?
        exposed = self.wait(30, until=lambda: (lambda r: r if not any(r.values()) else None)(
            heartbeat_integrity_exposed(load_jsonl(self.evidence / "sampler.jsonl"))))
        exposed = exposed or heartbeat_integrity_exposed(load_jsonl(self.evidence / "sampler.jsonl"))
        for venue, problem in exposed.items():
            self.writer_precheck.setdefault(venue, {})["heartbeat_trade_integrity"] = (
                "EXPOSED" if problem is None else "NOT_EXPOSED: " + problem)
        self.write_writer_precheck()
        self.note("writer_heartbeat_trade_integrity", **{v: p for v, p in exposed.items()})

    def views_fresh(self, since_ns):
        rows = load_jsonl(self.evidence / "publish.jsonl")
        index, frames = publish_index(rows), book_frames(rows)
        series = view_series(load_jsonl(self.evidence / "sampler.jsonl"))
        return all(caught_up(series, index, v, since_ns, frames=frames, predicate=self.predicate,
                             hold_s=10)["caught_up_ns"] for v in VENUES)

    def record_refusals(self, field):
        """Evidence of strict-resume refusals in each running clone's own log (its /tmp/logs dies with --rm)."""
        for venue, clone in self.clones.items():
            try:
                found = refusal_lines(clone.log_tail(400))
            except Exception as error:  # noqa: BLE001 - evidence is best effort
                found = {"error": type(error).__name__}
            self.writer_precheck.setdefault(venue, {}).setdefault("lifecycle", {})[field] = found
        self.write_writer_precheck()

    def strict_restart(self):
        """BOOTSTRAP -> STRICT of every clone (WriterClone lifecycle): clean stop (exit 0) that flushed native
        checkpoints, restart with the exact deployed env (precheck on this post-bootstrap argv), then proof that the
        strict sessions resumed: every MD feed applied again and a fresh book view after the restart."""
        restarted = {}
        for venue, clone in self.clones.items():
            role, lifecycle = f"tswriter-{venue.lower()}", self.writer_precheck[venue]["lifecycle"]
            stop_ns = time.time_ns()
            if not stop_role(self.ns, role, timeout=35):
                raise RuntimeError(f"STOP bootstrap writer {venue} missing before the strict restart")
            self.expected.discard(role)
            life = ((self.exit_states([role], wait_s=10).get(role) or {}).get("lives") or [None])[-1]
            lifecycle["bootstrap_exit"] = life
            inventory = cursor_inventory(clone.state_dir)
            lifecycle["checkpoints"], lifecycle["native_checkpoints"] = inventory, native_checkpoints(inventory)
            self.write_writer_precheck()
            if (not life or (life.get("died_ns") or 0) < stop_ns or life.get("exit_code") != 0
                    or life.get("oom_killed")):
                raise RuntimeError(f"STOP bootstrap writer {venue} did not stop cleanly: {life}")
            if not lifecycle["native_checkpoints"]:
                raise RuntimeError(f"STOP bootstrap writer {venue} left no native checkpoint: {sorted(inventory)}")
            clone.lifecycle["phase"] = "STRICT"
            argv = clone.writer_args()
            check = writer_clone_precheck(argv, self.images["ts"], writer_reference(clone.inspect),
                                          (self.parity or {}).get("writer"))
            lifecycle["strict_precheck"] = {k: check[k] for k in ("ok", "problems", "clone_env", "clone_image")}
            self.write_writer_precheck()
            if not check["ok"] or argv_env(argv).get(REQUIRE_RESUME_KEY) != "1":
                raise RuntimeError(f"STOP strict writer clone {venue} is not the deployed writer: "
                                   + "; ".join(check["problems"] or ["strict flag missing"]))
            clone.run(*argv, timeout=60)
            self.expected.add(role)
            self.verify_image(role)
            restarted[venue] = time.time_ns()
            lifecycle.update(phase="STRICT", strict_started_ns=restarted[venue])
            self.write_writer_precheck()
            self.note("writer_clone_strict_restart", venue=venue, bootstrap_exit=life,
                      native_checkpoints=lifecycle["native_checkpoints"])

        def missing():
            rows = load_jsonl(self.evidence / "sampler.jsonl")
            return [m for v, t in restarted.items() for m in md_missing_since(rows, t, venues=(v,))]

        resumed = self.wait(LIMITS["md_ready_s"], until=lambda: not missing())
        resumed = resumed and self.wait(LIMITS["clone_ready_s"], until=lambda: self.views_fresh(min(restarted.values())))
        self.record_refusals("strict_refusals")
        left = missing()
        for venue in restarted:
            lifecycle = self.writer_precheck[venue]["lifecycle"]
            mine = [m for m in left if m.endswith("@" + venue)]
            lifecycle["strict_resume"] = "VERIFIED" if resumed and not mine else "FAILED"
            lifecycle["strict_missing"] = mine
        self.write_writer_precheck()
        if not resumed or left:
            raise RuntimeError("STOP strict-resume start not verified: missing after restart " + ",".join(left)
                               + ("" if left else " (no fresh APPROVED book view)"))
        self.note("ts_market_data_applied", phase="STRICT", restarted_ns=restarted)

    def erase_clone_state(self):
        """Remove the clones' durable state dirs (written by the writer's uid) with a disposable holder container."""
        removed, errors = [], []
        for path in sorted(self.work.glob("tsstate-*")) if self.work.is_dir() else ():
            try:
                sh(["docker", "run", "--rm", "--label", f"{LABEL}={self.ns}", "--network", "none",
                    "-v", f"{path}:/state:rw", "--entrypoint", "find", self.images["holder"], "/state",
                    "-mindepth", "1", "-delete"], timeout=60)
                path.rmdir()
                removed.append(path.name)
            except Exception as error:  # noqa: BLE001 - teardown continues and reports
                errors.append(f"{path.name}:{type(error).__name__}")
        return {"removed": removed, "errors": errors}

    # -- INCIDENT 2026-10-15 core-group reproduction (no projector/Stream/Query/TS clones: the core group only)
    def core_group_up(self):
        self.core_members = len(CORE_MEMBERS)
        sh(self.topology()["net"])
        self.expected.add("net")
        self.verify_image("net")
        for node in BROKERS:
            self.start(f"kafka-{node}", "")
        (self.work / CORE_GROUP_FILE).write_text(json.dumps(
            {"members": self.core_members, "raw_partitions": CORE_GROUP["raw_partitions"]}))
        config = json.loads((self.work / "core.json").read_text())
        (self.work / "core.json").write_text(json.dumps(apply_core_profile(config, self.profile)))
        write_member_configs(self.work, self.core_members)
        (self.evidence / "core-profile.json").write_text(json.dumps(self.profile, indent=2, sort_keys=True) + "\n")
        self.note("core_profile", profile=self.profile)
        result = sh(self.helper(self.ns, self.work, ["--topics", "/work"]), timeout=180)
        self.note("topics", result=result.strip()[-400:], core_group=core_group(self.work))
        self.topic_id = json.loads((self.work / "packet.json").read_text())["slice"]["topic_id"]
        for role in CORE_MEMBERS:
            self.start(role, self.topic_id)
        remaining = int(LIMITS["deadline_s"] - (time.monotonic() - self.started) - 60)
        for role, mode, out in (("monitor", "--monitor", "monitor"), ("publisher", "--publish", "publish")):
            extra = ["--hz", str(self.profile["hz"])] if role == "publisher" else []
            sh(self.helper(self.ns, self.work, [mode, "/work", "--out", f"/evidence/{out}.jsonl",
                                                "--seconds", str(remaining), *extra], role=role, detach=True,
                           evidence=self.evidence, cpus="0.25", memory="256m"))
            self.expected.add(role)
            self.verify_image(role)
        self.note("core_group_started", members=list(CORE_MEMBERS), seconds=remaining)

    def core_group_formation(self, start_ns):
        """Diagnostic ``formation`` window (never judged) from member start until the group has been stable for
        CORE_GROUP["formation_hold_s"] (``core_group_stable_at`` on the live monitor), bounded by
        ``formation_max_s``; the baseline opens only after it. Never stable: CoreGroupNotMeasured."""
        shape = core_group(self.work)
        stable = self.wait(CORE_GROUP["formation_max_s"], step=1.0, until=lambda: core_group_stable_at(
            load_jsonl(self.evidence / "monitor.jsonl"), self.ns, shape["raw_partitions"], shape["members"],
            CORE_GROUP["formation_hold_s"], start_ns))
        row = {"k": "core_window", "name": "formation", "start_ns": start_ns, "end_ns": time.time_ns(),
               "diagnostic": True, "stable_at_ns": stable or None, "hold_s": CORE_GROUP["formation_hold_s"]}
        if not stable:
            row["reason"] = "GROUP_NEVER_STABLE"
        self.fault_row(row)
        self.note("core_group_formation", **row)
        if not stable:
            raise CoreGroupNotMeasured(f"GROUP_NEVER_STABLE within {CORE_GROUP['formation_max_s']} s "
                                       f"(hold {CORE_GROUP['formation_hold_s']} s)")
        return stable

    def core_group_window(self, name, seconds, **fields):
        start = time.time_ns()
        self.wait(seconds)
        self.fault_row({"k": "core_window", "name": name, "start_ns": start, "end_ns": time.time_ns(), **fields})

    def core_replace(self, role, image=None, kill=False, gap_s=0.0, kind=None):
        """Rolling replace (SIGTERM, as compose recreate) or SIGKILL of one member, restarted ``gap_s`` after its
        container is gone (0: at once). ``kind`` overrides the fault kind (``core_precondition``: a restart on
        the old image that is no roll, so it never becomes a roll target)."""
        if image is not None:
            self.images["core"] = image  # verify_image checks the replacement against the new image
        injected = time.time_ns()
        stop_role(self.ns, role, kill=kill)
        stopped = time.time_ns()
        if gap_s:
            time.sleep(gap_s)
        restart = time.time_ns()
        sh(self.topology(self.topic_id)[role], timeout=60)
        self.verify_image(role)
        self.follow_logs(role)
        row = {"k": kind or ("core_kill" if kill else "core_roll"), "role": role, "image": self.images["core"],
               "injected_ns": injected, "stopped_ns": stopped, "gap_s": gap_s, "restart_ns": restart}
        self.fault_row(row)
        self.note(row["k"], **row)
        return row

    def core_replace_all(self, image, gap_s=0.0):
        """``--core-group-simultaneous``: SIGTERM every member at once (parallel stops, each drains its own
        transaction), wait until all are gone and ``gap_s``, then start every member on ``image``."""
        from concurrent.futures import ThreadPoolExecutor
        self.images["core"] = image
        injected = time.time_ns()
        with ThreadPoolExecutor(max_workers=len(CORE_MEMBERS)) as pool:
            stopped_roles = list(pool.map(lambda role: stop_role(self.ns, role), CORE_MEMBERS))
        if not all(stopped_roles):
            raise RuntimeError("replace-all: a member was not running")
        stopped = time.time_ns()
        if gap_s:
            time.sleep(gap_s)
        restart = time.time_ns()
        for role in CORE_MEMBERS:
            sh(self.topology(self.topic_id)[role], timeout=60)
        started = time.time_ns()
        for role in CORE_MEMBERS:
            self.verify_image(role)
            self.follow_logs(role)
        row = {"k": "core_replace_all", "roles": list(CORE_MEMBERS), "image": image, "injected_ns": injected,
               "stopped_ns": stopped, "gap_s": gap_s, "restart_ns": restart, "started_ns": started}
        self.fault_row(row)
        self.note(row["k"], **row)
        return row

    def baseline(self):
        start = time.time_ns()
        self.wait(LIMITS["baseline_s"])
        self.fault_row({"k": "baseline", "start_ns": start, "end_ns": time.time_ns()})
        self.note("baseline_done")

    def canonical_leader(self):
        rows = load_jsonl(self.evidence / "monitor.jsonl")
        for row in reversed(rows):
            leader = (row.get("leaders") or {}).get(f"{self.ns}.canonical/0")
            if leader in BROKERS:
                return leader
        raise RuntimeError("canonical leader unknown")

    def fault(self, name, kind):
        self.check()
        node = self.canonical_leader() if kind == "broker" else None
        role = f"kafka-{node}" if kind == "broker" else kind
        injected = time.time_ns()
        stop_role(self.ns, role)
        self.note("fault_injected", fault=name, role=role)
        if kind == "broker":
            self.wait(LIMITS["broker_outage_s"], faulted=(role,))
        restart = time.time_ns()
        sh(self.topology(self.topic_id)[role], timeout=60)
        self.note("restart_issued", fault=name, role=role)
        self.verify_image(role)
        self.follow_logs(role)
        row = {"k": "fault", "name": name, "kind": kind, "node": node, "role": role,
               "injected_ns": injected, "restart_ns": restart}
        self.fault_row(row)
        monitor_rows = lambda: sorted(load_jsonl(self.evidence / "monitor.jsonl"), key=lambda r: r.get("t", 0))
        cleared = self.wait(LIMITS["role_return_s"],
                            until=lambda: cleared_at(kind, monitor_rows(), restart, node))
        if not cleared:
            raise RuntimeError(f"STOP {name}: fault not cleared within {LIMITS['role_return_s']} s")
        self.note("fault_cleared", fault=name, cleared_ns=cleared)

        def caught():
            result = evaluate_fault(row, load_jsonl(self.evidence / "publish.jsonl"),
                                    load_jsonl(self.evidence / "sampler.jsonl"), monitor_rows(), window_s=0,
                                    predicate=self.predicate)
            return result if result.get("consumers_caught_up_ns") else None

        result = self.wait(GATE["caught_up_s"] + GATE["hold_s"] + 10, until=caught)
        if not result or result["caught_up_s"] > GATE["caught_up_s"]:
            raise RuntimeError(f"STOP {name}: consumers not caught up within {GATE['caught_up_s']} s")
        self.note("caught_up", fault=name, caught_up_s=result["caught_up_s"])
        end = result["consumers_caught_up_ns"] + int(GATE["window_s"] * 1e9)
        self.wait(max(0.0, (end - time.time_ns()) / 1e9) + 2)

        def final():
            return evaluate_fault(row, load_jsonl(self.evidence / "publish.jsonl"),
                                  load_jsonl(self.evidence / "sampler.jsonl"), monitor_rows(), predicate=self.predicate)
        # OF-G12-2: pending events / the heartbeat after the scope must resolve before the verdict is read (an
        # undecided cell is NOT_MEASURED, never excluded); bounded by the longest LOST_AFTER_S.
        self.wait(max(LOST_AFTER_S.values()) + 10, until=lambda: not verdict_undecided(final()))
        verdict = final()
        self.note("fault_verdict", fault=name, verdict=verdict)
        if not verdict["pass"]:
            raise RuntimeError(f"STOP {name}: G12 verdict not PASS: " + json.dumps(verdict.get("parts")))

    def keep_logs(self):
        """Bounded log tails of every owned container (--rm loses them at stop), and each TS writer clone's own
        stderr file (the writer logs there, not to docker logs; the file is erased with work/tslogs)."""
        logs = self.evidence / "logs"
        logs.mkdir(exist_ok=True)
        for name in owned_names(self.ns):
            result = subprocess.run(["docker", "logs", "--tail", "120", name], capture_output=True,
                                    text=True, timeout=30)
            (logs / (name[len(self.ns) + 1:] + ".log")).write_text(
                (result.stdout + result.stderr)[-60000:])
        for venue, clone in self.clones.items():
            (logs / f"tswriter-{venue.lower()}.stderr.log").write_text(clone.stderr_tail(60000))

    def down(self):
        removed, errors = [], []
        try:
            self.keep_logs()
        except Exception as error:  # noqa: BLE001 - evidence is best effort, teardown is not
            errors.append("logs:" + type(error).__name__)
        for clone in self.clones.values():
            try:
                for item in clone.close():
                    removed.append(item)
            except Exception as error:  # noqa: BLE001 - cleanup continues
                errors.append(type(error).__name__)
        clone_state = self.erase_clone_state()
        report = teardown(self.ns)
        report["clone_state_erased"] = clone_state
        errors += clone_state["errors"]
        try:
            report["log_followers"] = self.stop_followers()
        except Exception as error:  # noqa: BLE001 - evidence is best effort, teardown is not
            report.setdefault("errors", []).append("log_followers:" + type(error).__name__)
        report["writer_clone_removed"] = removed
        report["errors"] = errors + report.get("errors", [])
        report["volumes_before"] = getattr(self, "volumes0", None)
        report["networks_before"] = getattr(self, "networks0", None)
        report["volumes_after"] = len(sh(["docker", "volume", "ls", "-q"]).split())
        report["networks_after"] = len(sh(["docker", "network", "ls", "-q"]).split())
        try:
            report["events_follower_exit"] = self.stop_events()
            states = exit_states(load_jsonl(self.evidence / "container-events.jsonl"))
            (self.evidence / "exit-states.json").write_text(json.dumps(states, indent=2) + "\n")
            report["exit_states"] = {role: {k: s[k] for k in ("exit_code", "oom_killed")}
                                     for role, s in sorted(states.items())}
        except Exception as error:  # noqa: BLE001 - evidence is best effort, teardown is not
            report["errors"].append("exit_states:" + type(error).__name__)
        erase_private(self.work)
        if self.guard0 is not None:
            breaches, recreated = guard_breaches(self.guard0, production_guard())
            report["production_guard_breaches"] = breaches
            report["production_external_recreates"] = recreated
        with (self.evidence / "teardown.json").open("w") as handle:
            json.dump(report, handle, indent=2)
        self.note("teardown", **report)
        return report


def teardown(ns):
    """Exact-name, label-checked stop of every owned container, then verify zero."""
    removed, errors = [], []
    for name in owned_names(ns):
        role = name[len(ns) + 1:]
        if role not in TEARDOWN_ORDER and not re.fullmatch(r"ts(writer|redis|pg)-(binance|okx)", role):
            errors.append("unknown role " + role)
    order = list(TEARDOWN_ORDER[:3]) + [f"ts{k}-{v}" for v in ("binance", "okx") for k in ("writer", "redis", "pg")] \
        + list(TEARDOWN_ORDER[3:])
    for role in order:
        try:
            if stop_role(ns, role, timeout=10):
                removed.append(role)
        except Exception as error:  # noqa: BLE001 - continue, then report
            errors.append(f"{role}:{type(error).__name__}:{error}")
    left = owned_names(ns)
    by_name = sh(["docker", "ps", "-aq", "--filter", "name=" + ns]).split()
    networks = sh(["docker", "network", "ls", "-q", "--filter", "name=" + ns]).split()
    volumes = sh(["docker", "volume", "ls", "-q", "--filter", "label=" + LABEL]).split()
    return {"removed_containers": removed, "removed_count": len(removed), "left_by_label": len(left),
            "left_by_name": len(by_name), "networks_by_name": len(networks), "volumes_by_label": len(volumes),
            "errors": errors}


def erase_private(work):
    work = Path(work)
    for name in PRIVATE_FILES:
        (work / name).unlink(missing_ok=True)
    for node in BROKERS:
        shutil.rmtree(work / f"kafka-{node}", ignore_errors=True)
    shutil.rmtree(work / "tslogs", ignore_errors=True)


def orchestrate(base, export, ns, images=None, query_code="worktree", auto_leader_rebalance=None, parity=None,
                ts_root=None, observe_production=None):
    if type(auto_leader_rebalance) is not bool:
        raise ValueError("--run needs an explicit --auto-leader-rebalance true|false (production's value)")
    if parity is not None:
        validate_parity(parity)
        if parity["kafka"]["auto.leader.rebalance.enable"] is not auto_leader_rebalance:
            raise ValueError("--auto-leader-rebalance differs from the production parity snapshot "
                             f"({parity['kafka']['auto.leader.rebalance.enable']})")
        if observe_production is None:
            raise ValueError("a parity run needs the production observer (effective config before/after, Codex K3)")
    run = Run(base, export, ns, images, query_code)
    run.auto_leader_rebalance = auto_leader_rebalance
    run.parity = parity
    if ts_root is not None:
        run.ts_root = Path(ts_root)

    def expire(_signum, _frame):
        raise TimeoutError("G12 orchestrator absolute deadline")
    def terminated(signum, _frame):
        # A session end / kill (SIGTERM, SIGHUP, SIGINT) still runs evidence + teardown in finally
        # (final-image attempt 2, 2026-10-10: the orchestrator died with the session, 18 roles orphaned).
        raise SystemExit(f"G12 orchestrator received signal {signum}")
    previous = signal.signal(signal.SIGALRM, expire)
    previous_term = {sig: signal.signal(sig, terminated) for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)}
    signal.alarm(LIMITS["deadline_s"])
    status, error = "FAIL", None
    try:
        if run.parity is not None:
            run.bind_production("before", observe_production)
        run.prepare()
        run.up()
        if run.parity is not None:
            run.apply_kafka_profile()
        run.baseline()
        run.record_internal_topics("before")
        for name, kind in (("broker_stop_start", "broker"), ("projector_restart", "projector"),
                           ("stream_restart", "stream")):
            run.fault(name, kind)
        run.record_internal_topics("after")
        if run.parity is not None:
            run.bind_production("after", observe_production)
        status = "COMPLETED"
    except BaseException as exc:  # noqa: BLE001 - evidence first, then teardown
        error = f"{type(exc).__name__}: {exc}"
        print("STOPPED", error, flush=True)
        if run.evidence.exists():
            run.note("stopped", error=error)
    finally:
        signal.alarm(0)
        try:
            if run.evidence.exists():
                verdict = evaluate(run.evidence)
                try:
                    config = run.broker_config()
                except Exception as exc:  # noqa: BLE001 - the verdict is written regardless
                    config = {"auto.leader.rebalance.enable": auto_leader_rebalance,
                              "internal_topic_parity": "NOT_MEASURED", "production_parity": "NOT_MEASURED",
                              "error": f"{type(exc).__name__}: {exc}"}
                verdict.update(status=status, error=error, namespace=ns, images=run.images,
                               query_code=run.query_code, broker_config=config)
                (run.evidence / "verdict.json").write_text(json.dumps(verdict, indent=2) + "\n")
        finally:
            run.down() if run.evidence.exists() else teardown(ns)
            signal.signal(signal.SIGALRM, previous)
            for sig, handler in previous_term.items():
                signal.signal(sig, handler)
    return 0 if status == "COMPLETED" else 1


def core_log_events(logs):
    """(t_ns, role, event, fields, life) of every core lifecycle event in ``logs/<member>.life<n>.log``
    (``life``: the n of the file, the member's n-th container)."""
    from datetime import datetime
    rows = []
    for path in sorted(Path(logs).glob("core*.life*.log")):
        role, _, rest = path.name.partition(".life")
        life = int(rest[:-len(".log")])
        for line in path.read_text(errors="replace").splitlines():
            stamp, _, body = line.partition(" ")
            if '"event"' not in body:
                continue
            try:
                value = json.loads(body)
                seconds, _, frac = stamp.rstrip("Z").partition(".")
                t = int(datetime.fromisoformat(seconds + "+00:00").timestamp()) * 10**9 + int((frac + "0" * 9)[:9])
            except ValueError:
                continue
            if value.get("event") in CORE_EVENTS:
                rows.append((t, role, value["event"], value, life))
    return sorted(rows, key=lambda r: r[0])


def core_life_images(evidence, event_rows, ns):
    """role -> [image id of life 1, life 2, ...] of every core member, or None per life where it is unknown.

    Two independent records: the daemon's die events (``container-events.jsonl``: the n-th die of ``<ns>-<role>``
    ends life n, ``Attributes.image``) and ``provenance.jsonl`` (``verify_image`` after each start; a row belongs to
    life 1 + the number of that role's dies before it, so a missing row cannot shift later lives; ``actual`` is the
    inspected image id). Either alone identifies a life; when both exist they must agree, and two different
    provenance images for one life also leave it None (unknown, never guessed)."""
    path = Path(evidence) / "provenance.jsonl"
    provenance = load_jsonl(path) if path.exists() else []
    out = {}
    for role in CORE_MEMBERS:
        dies = [row for row in sorted(event_rows, key=lambda r: r.get("timeNano") or 0)
                if (row.get("Action") or row.get("status")) == "die"
                and ((row.get("Actor") or {}).get("Attributes") or {}).get("name") == f"{ns}-{role}"]
        died = [((row.get("Actor") or {}).get("Attributes") or {}).get("image") for row in dies]
        started = {}
        for r in provenance:
            if r.get("role") == role and r.get("t") is not None:
                life = 1 + sum(1 for d in dies if (d.get("timeNano") or 0) < r["t"])
                started.setdefault(life, set()).add(r.get("actual"))
        images = []
        for index in range(max([len(died), *started])):
            seen = started.get(index + 1, set())
            a = next(iter(seen)) if len(seen) == 1 else None
            b = died[index] if index < len(died) else None
            conflict = len(seen) > 1 or (a and b and a != b)
            images.append(None if conflict else (a or b))
        out[role] = images
    return out


def core_client_id(ns, role):
    """The Kafka client.id of a core-group member (``topology``: member 1 ``ns``, member n ``ns-n``)."""
    index = CORE_MEMBERS.index(role) + 1
    return ns if index == 1 else f"{ns}-{index}"


def core_log_lives(logs, event_rows, ns):
    """role -> [{file, first_ns, end_ns, truncated}] of every followed core life, in life order.

    The follower keeps only the last FOLLOW_TAIL_BYTES of a life, so a long life starts mid-line: that partial
    line is dropped and the life covers only from its first complete line (``truncated``); events before it are
    lost and a window before it is not covered. ``end_ns``: the daemon's die event of that life (n-th die of the
    role), else its last line."""
    from datetime import datetime

    def parse(line):
        stamp, _, body = line.partition(" ")
        seconds, _, frac = stamp.rstrip("Z").partition(".")
        t = int(datetime.fromisoformat(seconds + "+00:00").timestamp()) * 10**9 + int((frac + "0" * 9)[:9])
        return t, json.loads(body)
    states = exit_states(event_rows)
    lives = {}
    for role in CORE_MEMBERS:
        paths = sorted(Path(logs).glob(f"{role}.life*.log"), key=lambda p: int(p.name.split(".life")[1][:-4]))
        died = [life.get("died_ns") for life in (states.get(role) or {}).get("lives", [])]
        rows = []
        for index, path in enumerate(paths):
            parsed = []
            for line in path.read_text(errors="replace").splitlines():
                try:
                    parsed.append(parse(line))
                except ValueError:
                    continue
            if not parsed:
                rows.append({"file": path.name, "first_ns": None, "end_ns": None, "truncated": True})
                continue
            first, head = parsed[0]
            truncated = not (head.get("event") == "qdl_realtime_core_started" and head.get("generation") == 1)
            end = died[index] if index < len(died) and died[index] else parsed[-1][0]
            rows.append({"file": path.name, "first_ns": first, "end_ns": max(end, parsed[-1][0]),
                         "truncated": truncated})
        lives[role] = rows
    return lives


def log_covered(lives, lo, hi, slack_ns):
    """Whether one member's retained log lines cover [lo, hi] with no hole longer than ``slack`` (a replaced
    member's new life starts just after the window opens; a truncated life covers only its retained tail)."""
    spans = sorted((life["first_ns"], life["end_ns"]) for life in lives if life["first_ns"] is not None)
    cursor = lo
    for first, end in spans:
        if end < cursor:
            continue
        if first > cursor + slack_ns:
            return False
        cursor = max(cursor, end)
        if cursor >= hi:
            return True
    return cursor >= hi


def group_view(row, ns, partitions):
    """(members, state, per-member partition counts, exact-coverage verdict, basis) of the core group in one sample.

    With per-member detail (monitor v2) coverage is exact: the members' partition sets are disjoint and their union
    is every raw partition. Without it (older monitor) the count list must sum to ``partitions`` in a STABLE group
    (one generation never assigns a partition twice), recorded as basis ``counts``."""
    value = (row.get("groups") or {}).get(ns) or {}
    members, state, counts = value.get("members", 0), value.get("state"), value.get("assigned") or []
    detail = value.get("detail")
    if detail is not None:
        owned = [p for member in detail for p in member.get("partitions", [])]
        full = {f"{ns}.raw/{p}" for p in range(partitions)}
        covered = len(owned) == len(set(owned)) and set(owned) == full
        return members, state, [len(m.get("partitions", [])) for m in detail], covered, "partitions"
    return members, state, counts, sum(counts) == partitions, "counts"


def healthy_view(view, members):
    """Exact roster, STABLE, every member owns >= 1 partition, all partitions covered."""
    count, state, counts, covered, _ = view
    return count == members and state in (None, "STABLE") and len(counts) == members and 0 not in counts and covered


def idle_partition(pre, post):
    """Why an affected raw partition is idle across a kill, or None when it has records to consume.

    ``pre`` / ``post``: the core group's [committed, log end] in the last sample before the kill and at
    ``ownership_restored``. Idle: at the fault its log end equals its committed position (no committed offset
    yet - Kafka's -1001 - counts as position 0) and no record arrived since. B2 (2026-10-15): raw/5 never had a
    record ([-1001, 0]) and its committed offset can never reach its log end, so NO_PROGRESS stood forever."""
    committed, end = pre
    if end > max(committed, 0) or post[1] != end:
        return None
    return "NO_RECORDS" if end == 0 else "COMMITTED_AT_LOG_END"


def core_group_stable_at(monitor_rows, ns, partitions, members, hold_s, since_ns=0):
    """The first sample time at which the core group has been continuously stable for >= ``hold_s`` since
    ``since_ns``, else None. Stable: per-member detail (monitor v2) shows exactly ``members`` members, STABLE,
    each owning >= 1 partition, every raw partition owned exactly once; a sample short of that, a sample without
    group detail, or a monitor gap > MONITOR_MAX_GAP_S restarts the hold."""
    start = previous = None
    for row in sorted((r for r in monitor_valid(monitor_rows) if r.get("t", 0) >= since_ns), key=lambda r: r["t"]):
        view = group_view(row, ns, partitions) if "groups" in row else None
        healthy = view is not None and view[4] == "partitions" and healthy_view(view, members)
        if not healthy or (previous is not None and row["t"] - previous > MONITOR_MAX_GAP_S * 10**9):
            start = None
        previous = row["t"]
        if not healthy:
            continue
        start = row["t"] if start is None else start
        if row["t"] - start >= hold_s * 10**9:
            return row["t"]
    return None


class CoreGroupNotMeasured(RuntimeError):
    """The core-group run cannot measure its frozen conditions (e.g. the group never formed stably)."""


def core_kill_recovery(kill, monitor_rows, event_rows, ns, partitions, members, base_lag):
    """RB2 (review 2026-10-15). Recovery from SIGKILL of one member, as separate clocks from injection.

    * ``exit_confirmed``: the daemon's die event of the killed container (never inferred from the kill call);
    * ``ownership_restored``: the first sample after that exit in which the dead member's member-id (found by its
      client.id in the last pre-kill sample) is gone, the roster is exact, every member owns >= 1 partition and
      every raw partition is owned once - a stale pre-detection 3-member view cannot close it;
    * ``progress``: the first sample from then on in which each non-idle affected partition (the dead member's)
      has a committed offset >= its log end at ``ownership_restored`` (``idle_partition``: no record to consume
      at the fault and none arrived by ``ownership_restored`` - excluded and reported in ``idle_partitions``);
      when every affected partition is idle, ``progress`` is ``ownership_restored`` (``progress_basis``
      ALL_AFFECTED_IDLE);
    * ``caught_up``: the first sample from ``progress`` on with core lag <= the baseline maximum.
    A clock needs monitor v2 detail (member ids, partitions, per-partition offsets); without it it stays null with
    ``reason`` and the acceptance reports NOT_MEASURED."""
    injected = kill["injected_ns"]
    out = {"injected_ns": injected, "role": kill["role"], "exit_confirmed_s": None, "exit_code": None,
           "ownership_restored_s": None, "progress_s": None, "caught_up_s": None, "dead_member_id": None,
           "affected_partitions": [], "idle_partitions": [], "post_fault_offsets": {}, "reason": None}
    seconds = lambda t: round((t - injected) / 1e9, 3)  # noqa: E731
    name = f"{ns}-{kill['role']}"
    die = next((row for row in sorted(event_rows, key=lambda r: r.get("timeNano") or 0)
                if (row.get("Action") or row.get("status")) == "die" and (row.get("timeNano") or 0) >= injected
                and ((row.get("Actor") or {}).get("Attributes") or {}).get("name") == name), None)
    if die is None:
        out["reason"] = "NO_EXIT_EVENT"
        return out
    exit_ns = die["timeNano"]
    out.update(exit_confirmed_s=seconds(exit_ns), exit_code=(die["Actor"]["Attributes"]).get("exitCode"))
    group = lambda row: (row.get("groups") or {}).get(ns) or {}  # noqa: E731
    before = [r for r in monitor_rows if r["t"] < injected]
    pre = before[-1] if before else None
    if pre is None or group(pre).get("detail") is None or ns not in (pre.get("offsets") or {}):
        out["reason"] = "NO_MEMBER_DETAIL"
        return out
    client = core_client_id(ns, kill["role"])
    dead = [m for m in group(pre)["detail"] if m.get("client_id") == client]
    if len(dead) != 1:
        out["reason"] = "DEAD_MEMBER_NOT_IDENTIFIED"
        return out
    dead_id, affected = dead[0]["member_id"], sorted(dead[0].get("partitions", []))
    out.update(dead_member_id=dead_id, affected_partitions=affected)
    restored = None
    for row in monitor_rows:
        if row["t"] < exit_ns or group(row).get("detail") is None:
            continue
        ids = {m.get("member_id") for m in group(row)["detail"]}
        if dead_id not in ids and healthy_view(group_view(row, ns, partitions), members):
            restored = row
            break
    if restored is None:
        out["reason"] = "OWNERSHIP_NOT_RESTORED"
        return out
    out["ownership_restored_s"] = seconds(restored["t"])
    marks = {p: (restored.get("offsets") or {}).get(ns, {}).get(p) for p in affected}
    pre_offsets = pre["offsets"][ns]
    missing = sorted(p for p in affected if marks[p] is None or pre_offsets.get(p) is None)
    if missing:
        out.update(reason="NO_PARTITION_OFFSETS", unmeasured_partitions=missing)
        return out
    idle = {p: reason for p in affected if (reason := idle_partition(pre_offsets[p], marks[p])) is not None}
    busy = {p: marks[p][1] for p in affected if p not in idle}
    out.update(post_fault_offsets=busy, idle_partitions=sorted(idle), idle_reasons=idle)
    if not busy:  # nothing to consume on the dead member's partitions: progress is the ownership transfer
        out["progress_s"] = out["ownership_restored_s"]
        out["progress_basis"] = "ALL_AFFECTED_IDLE"
        progressed = restored
    else:
        progressed = next((r for r in monitor_rows if r["t"] >= restored["t"]
                           and all(((r.get("offsets") or {}).get(ns, {}).get(p) or [-1])[0] >= mark
                                   for p, mark in busy.items())), None)
        if progressed is None:
            out["reason"] = "NO_PROGRESS"
            return out
        out["progress_s"] = seconds(progressed["t"])
        out["progress_basis"] = "NON_IDLE_PAST_POST_FAULT_OFFSETS"
    caught = next((r for r in monitor_rows if r["t"] >= progressed["t"] and base_lag is not None
                   and (r.get("lag") or {}).get(ns) is not None and r["lag"][ns] <= base_lag), None)
    if caught is None:
        out["reason"] = "NOT_CAUGHT_UP"
        return out
    out["caught_up_s"] = seconds(caught["t"])
    out["caught_up_monitor_max_gap_s"] = monitor_max_gap_s(monitor_rows, injected, caught["t"])
    return out


CORE_GROUP_VERDICT_SCHEMA = "p184.coregroup.verdict.v2"
CORE_GROUP_WINDOWS = ("baseline", *("after_roll_" + role for role in CORE_MEMBERS), "after_kill")
# ``--core-group-simultaneous``: every member replaced at once, then a SIGTERM restart and a SIGKILL on the all-new
# group (the judged windows of that mode; ``formation`` / ``precondition_<n>`` stay diagnostic in both modes).
CORE_GROUP_S_WINDOWS = ("baseline", "after_replace_all", "after_restart_" + CORE_GROUP["restart_member"],
                        "after_kill")
CORE_GROUP_TAIL_S = 30
# Faults after which a window's per-image attribution span opens at the fault's injection.
CORE_GROUP_CAUSES = ("core_roll", "core_kill", "core_replace_all")


def core_replace_all_recovery(row, monitor_rows, ns, partitions, members, base_lag, event_rows=()):
    """Clocks of a replace-all from the first new member's start (``restart_ns``): ``reformed_s`` (first sample
    with the exact roster, STABLE, all partitions owned exactly once - monitor v2 detail), ``stable_s`` (held so
    for CORE_GROUP["formation_hold_s"]), ``caught_up_s`` (first sample from ``reformed`` with core lag <= the
    baseline maximum); ``stopped_s``: all old members gone after injection. A clock that never closes is null with
    ``reason``.

    Reported, not judged (addendum-4 ``run_S.report``): ``pause_s``, the full core pause from the first member's
    stop - the daemon's first die event of a member container at/after injection (``first_stop_ns``), never the
    stop call - to ``reformed``; ``injected_to_reformed_s`` from the stop call. Null without a die event /
    re-formation (``pause_reason``)."""
    restart = row["restart_ns"]
    seconds = lambda t: round((t - restart) / 1e9, 3)  # noqa: E731
    out = {"stopped_s": round((row["stopped_ns"] - row["injected_ns"]) / 1e9, 3) if row.get("stopped_ns") else None,
           "gap_s": row.get("gap_s"), "reformed_s": None, "stable_s": None, "caught_up_s": None, "reason": None,
           "first_stop_ns": None, "pause_s": None, "injected_to_reformed_s": None, "pause_reason": None}
    names = {f"{ns}-{role}" for role in row.get("roles") or CORE_MEMBERS}
    first_die = next((r["timeNano"] for r in sorted(event_rows, key=lambda r: r.get("timeNano") or 0)
                      if (r.get("Action") or r.get("status")) == "die"
                      and (r.get("timeNano") or 0) >= row["injected_ns"]
                      and ((r.get("Actor") or {}).get("Attributes") or {}).get("name") in names), None)
    out["first_stop_ns"] = first_die
    after = [r for r in monitor_rows if r["t"] >= restart]
    reformed = next((r for r in after if (r.get("groups") or {}).get(ns, {}).get("detail") is not None
                     and group_view(r, ns, partitions)[4] == "partitions"
                     and healthy_view(group_view(r, ns, partitions), members)), None)
    if reformed is None:
        out["reason"] = out["pause_reason"] = "NEVER_REFORMED"
        return out
    out["reformed_s"] = seconds(reformed["t"])
    out["injected_to_reformed_s"] = round((reformed["t"] - row["injected_ns"]) / 1e9, 3)
    if first_die is None:
        out["pause_reason"] = "NO_EXIT_EVENT"
    else:
        out["pause_s"] = round((reformed["t"] - first_die) / 1e9, 3)
    stable = core_group_stable_at(monitor_rows, ns, partitions, members, CORE_GROUP["formation_hold_s"], restart)
    out["stable_s"] = None if stable is None else seconds(stable)
    caught = next((r for r in after if r["t"] >= reformed["t"] and base_lag is not None
                   and (r.get("lag") or {}).get(ns) is not None and r["lag"][ns] <= base_lag), None)
    if caught is None:
        out["reason"] = "NOT_CAUGHT_UP"
        return out
    out["caught_up_s"] = seconds(caught["t"])
    out["monitor_max_gap_s"] = monitor_max_gap_s(monitor_rows, restart, caught["t"])
    return out


def core_group_verdict(evidence, members=len(CORE_MEMBERS)):
    """Storm / no-storm diagnostic of a ``--core-group`` run from its core logs, faults and monitor (offline).

    Per window (baseline, after each roll, after the kill): retries, rebalance rewinds and their failures (total
    and per member), group samples short of the roster, monitor coverage (largest gap, whole window and its last
    30 s), the last 30 s roster/coverage health and each member's log coverage. ``storm``: any retry in the final
    window, or a member short in its last 30 s (diagnostic only - the acceptance is ``core_group_acceptance``).
    ``core_kill``: the RB2 recovery clocks (``core_kill_recovery``); ``core_kill_caught_up_s`` is its
    ``caught_up_s``. ``core_kill_first_healthy_sample_s`` keeps the old count-only reading for comparison and
    closes nothing."""
    evidence = Path(evidence)
    events = core_log_events(evidence / "logs")
    faults = load_jsonl(evidence / "faults.jsonl")
    monitor_rows = [r for r in monitor_valid(load_jsonl(evidence / "monitor.jsonl")) if "groups" in r]
    container_events = load_jsonl(evidence / "container-events.jsonl") if (
        evidence / "container-events.jsonl").exists() else []
    ns = json.loads((evidence / "verdict-ns.json").read_text())["namespace"]
    partitions = CORE_GROUP["raw_partitions"]
    lives = core_log_lives(evidence / "logs", container_events, ns)
    slack = MONITOR_MAX_GAP_S * 10**9

    def group(row):
        value = (row.get("groups") or {}).get(ns) or {}
        return value.get("members", 0), value.get("assigned") or [], (row.get("lag") or {}).get(ns)

    windows = [r for r in faults if r.get("k") == "core_window"]
    life_images = core_life_images(evidence, container_events, ns)
    targets = sorted({f.get("image") for f in faults if f.get("k") in ("core_roll", "core_replace_all")
                      and f.get("image")})
    replace_all = next((f for f in faults if f.get("k") == "core_replace_all"), None)
    profile_path = evidence / "core-profile.json"
    result = {"schema": CORE_GROUP_VERDICT_SCHEMA, "mode": "simultaneous" if replace_all else "rolling",
              "profile": json.loads(profile_path.read_text()) if profile_path.exists() else None,
              "windows": {}, "faults": faults,
              "log_lives": lives, "monitor_max_gap_rule_s": MONITOR_MAX_GAP_S,
              "core_images": {"target": targets[0] if len(targets) == 1 else None, "rolled_onto": targets,
                              "lives": life_images}}
    for window in windows:
        lo, hi = window["start_ns"], window["end_ns"]
        # Per-image attribution span: a window after a roll / kill opens at that fault's injection, so the
        # rolled member's stop and the restart gap are attributed too (the window counts keep [lo, hi]).
        cause = [f for f in faults if f.get("k") in CORE_GROUP_CAUSES and f.get("injected_ns", hi) <= lo]
        span_lo = cause[-1]["injected_ns"] if cause and window["name"] != "baseline" else lo
        by_life = {}
        for e in events:
            if span_lo <= e[0] <= hi and e[2] in ("qdl_realtime_core_retry", "qdl_realtime_core_rebalance_rewind",
                                                  "qdl_realtime_core_rewind_failed"):
                images = life_images.get(e[1]) or []
                key = f"{e[1]}.life{e[4]}"
                row = by_life.setdefault(key, {"role": e[1], "life": e[4], "retries": 0, "rewinds": 0,
                                               "rewind_failed": 0,
                                               "image": images[e[4] - 1] if e[4] <= len(images) else None})
                row[{"qdl_realtime_core_retry": "retries", "qdl_realtime_core_rebalance_rewind": "rewinds",
                     "qdl_realtime_core_rewind_failed": "rewind_failed"}[e[2]]] += 1
        # Members restarted / killed within the attribution span (addendum-5 run_S2: not survivors of that window):
        # the roles of a cause fault injected in the span (the next window's cause, injected at this window's end,
        # is not), and any role with a daemon die event in the span.
        restarted = {role for f in faults if f.get("k") in CORE_GROUP_CAUSES
                     and span_lo <= (f.get("injected_ns") or 0) < hi
                     for role in (f.get("roles") or [f.get("role")]) if role}
        restarted |= {name[len(ns) + 1:] for r in container_events
                      if (r.get("Action") or r.get("status")) == "die" and span_lo <= (r.get("timeNano") or 0) <= hi
                      for name in [((r.get("Actor") or {}).get("Attributes") or {}).get("name") or ""]
                      if name[len(ns) + 1:] in CORE_MEMBERS and name.startswith(ns + "-")}
        inside = [e for e in events if lo <= e[0] <= hi]
        samples = [group(r) for r in monitor_rows if lo <= r["t"] <= hi]
        tail_rows = [r for r in monitor_rows if hi - CORE_GROUP_TAIL_S * 10**9 <= r["t"] <= hi]
        tail = [group_view(r, ns, partitions) for r in tail_rows]
        last = [r for r in monitor_rows if lo <= r["t"] <= hi]
        count = lambda name, rows=inside: sum(1 for e in rows if e[2] == name)  # noqa: E731
        result["windows"][window["name"]] = {
            "seconds": round((hi - lo) / 1e9, 1),
            "retries": count("qdl_realtime_core_retry"),
            "rewinds": count("qdl_realtime_core_rebalance_rewind"),
            "rewind_failed": count("qdl_realtime_core_rewind_failed"),
            "inflight_commit_failed": count("qdl_realtime_core_inflight_commit_failed"),
            "stop_drain_failed": count("qdl_realtime_core_stop_drain_failed"),
            "by_member": {role: {"retries": count("qdl_realtime_core_retry", [e for e in inside if e[1] == role]),
                                 "rewinds": count("qdl_realtime_core_rebalance_rewind",
                                                  [e for e in inside if e[1] == role])}
                          for role in CORE_MEMBERS},
            "short_member_samples": sum(1 for m, a, _ in samples if m < members or 0 in a),
            "samples": len(samples), "max_lag": max((lag for *_, lag in samples if lag is not None), default=None),
            "monitor_max_gap_s": monitor_max_gap_s(monitor_rows, lo, hi),
            "tail_samples": len(tail),
            "tail_monitor_max_gap_s": monitor_max_gap_s(monitor_rows, hi - CORE_GROUP_TAIL_S * 10**9, hi),
            "tail_unhealthy_samples": sum(1 for view in tail if not healthy_view(view, members)),
            "tail_coverage_basis": sorted({view[4] for view in tail}),
            "end_members": group(last[-1])[0] if last else None,
            "log_covered": {role: log_covered(lives.get(role, []), lo, hi, slack) for role in CORE_MEMBERS},
            "retry_errors": sorted({e[3].get("error", "")[:120] for e in inside
                                    if e[2] == "qdl_realtime_core_retry"}),
            "span_start_ns": span_lo, "by_life": by_life, "restarted_roles": sorted(restarted)}
    result["totals"] = {name: sum(1 for e in events if e[2] == name) for name in CORE_EVENTS}
    named = {w["name"]: w for w in windows}
    if "baseline" in named:  # diagnostic: did the baseline open on a stable group (not during formation)?
        lo, formation = named["baseline"]["start_ns"], named.get("formation")
        stable = core_group_stable_at(monitor_rows, ns, partitions, members, CORE_GROUP["formation_hold_s"],
                                      formation["start_ns"] if formation else lo)
        early = [e for e in events if lo <= e[0] < (stable if stable is not None else named["baseline"]["end_ns"])
                 and e[2] in ("qdl_realtime_core_retry", "qdl_realtime_core_rebalance_rewind",
                              "qdl_realtime_core_rewind_failed")]
        result["baseline_formation"] = {
            "formation_recorded": formation is not None, "hold_s": CORE_GROUP["formation_hold_s"],
            "stable_at_ns": stable, "stable_after_baseline_start_s": None if stable is None else
            round((stable - lo) / 1e9, 3), "baseline_opened_on_stable_group": stable is not None and stable <= lo,
            "baseline_events_before_stable": [{"role": e[1], "life": e[4], "event": e[2], "t_ns": e[0]}
                                              for e in early]}
    final = windows[-1] if windows else None
    if final:
        tail = [group(r) for r in monitor_rows if final["end_ns"] - 30 * 10**9 <= r["t"] <= final["end_ns"]]
        result["storm"] = (result["windows"][final["name"]]["retries"] > 0
                           or not tail or any(m < members or 0 in a for m, a, _ in tail))
    kill = next((f for f in faults if f.get("k") == "core_kill"), None)
    base = result["windows"].get("baseline", {}).get("max_lag")
    if replace_all:
        result["replace_all"] = core_replace_all_recovery(replace_all, monitor_rows, ns, partitions, members, base,
                                                          container_events)
    if kill:
        result["core_kill"] = core_kill_recovery(kill, monitor_rows, container_events, ns, partitions, members, base)
        result["core_kill_caught_up_s"] = result["core_kill"]["caught_up_s"]
    if kill and base is not None:
        first = next((r["t"] for r in monitor_rows if r["t"] >= kill["injected_ns"]
                      and group(r)[0] == members and 0 not in group(r)[1] and group(r)[1]
                      and group(r)[2] is not None and group(r)[2] <= base), None)
        result["core_kill_first_healthy_sample_s"] = (None if first is None
                                                      else round((first - kill["injected_ns"]) / 1e9, 3))
    orchestrator = evidence / "orchestrator.jsonl"
    teardown_rows = [r for r in load_jsonl(orchestrator) if r.get("step") == "teardown"] if orchestrator.exists() \
        else []
    result["production_guard_breaches"] = (teardown_rows[-1].get("production_guard_breaches")
                                           if teardown_rows else None)
    return result


# RB3 (review 2026-10-15). Versioned acceptance of a core-group verdict against the frozen prereg criteria: each
# condition PASS / FAIL / NOT_MEASURED. A violation that was observed is FAIL even where coverage is incomplete;
# PASS needs complete monitor coverage (MONITOR_MAX_GAP_S) and every member's retained log over the window. The
# orchestrator's exit code (completion only) and the diagnostic ``storm`` are never the acceptance.
# v2 (owner decision 2026-10-15, prereg addendum-2 ``run_B2``): a mixed-version roll is judged per image - every
# retry / rewind is attributed to the member life that logged it and that life's image (``core_life_images``).
# The v1 evaluation of the original prereg (run_A_control, run_B_fix) is unchanged and still reported.
CORE_GROUP_ACCEPTANCE_SCHEMA_V1 = "p184.coregroup.acceptance.v1"
CORE_GROUP_ACCEPTANCE_SCHEMA = "p184.coregroup.acceptance.v2"
CORE_GROUP_LIMITS = {"rewinds_per_roll_window": 2, "caught_up_s": 60, "partitions_moved_s": 15,
                     "control_retries_per_member": 2, "new_image_rewinds_per_window": 2,
                     "old_image_survivor_retries_per_member": 1, "new_image_rewinds_per_survivor_per_window": 2}


# Frozen prereg addendum-4 (frozen 2026-10-15T11:21:07Z, sha256 06c561ed...) states its run_A_prime / run_S
# criteria as free text that differs from the rule keys below. This versioned map binds each frozen string, verbatim,
# to the existing ``rules_v2`` rule(s) that judge it (several keys: every one must PASS, ``_combine``). No threshold
# or rule semantics live here; a string in neither the rules nor this map stays UNKNOWN_CONDITION -> NOT_MEASURED.
CORE_GROUP_CRITERIA_MAP_VERSION = "p184.coregroup.criteria-map.addendum-4.v1"
CORE_GROUP_CRITERIA_MAP = {
    # run_A_prime
    "control valid iff each old-image survivor of the first roll retries >= 2 times in after_roll_core; else "
    "harness not production-like and S cannot be credited":
        ("reproduction: each old-image survivor of the first roll retries >= 2 times in after_roll_core",),
    # run_S ("storm=false" is a rule key verbatim)
    "group re-forms (3 members, 6 partitions owned exactly once) <= 30 s after the first new member starts and "
    "holds stable >= 10 s; lag back within the baseline maximum <= 60 s":
        ("replace-all: group re-formed (3 members, 6 partitions owned exactly once) <= 30 s after the first new "
         "member started, then stable >= 10 s",
         "replace-all: core lag within the baseline maximum <= 60 s after the first new member started"),
    "new-image lives: retries 0, rewind_failed 0, rewinds <= 2 per window":
        ("new-image member lives: retries == 0, rewind_failed == 0, rewinds <= 2 per window, 0 in baseline",),
    "old-image lives: 0 retries, rewinds, rewind failures in every judged window":
        ("old-image member lives: 0 retries, rewinds and rewind failures in every judged window",),
    "every window after the replace: retries 0 absolute":
        ("every window after the replace (after_replace_all, after_restart_core-3, after_kill): retries == 0 "
         "absolute",),
    "last 30 s of every window: 3 members, 6 partitions owned exactly once":
        ("last 30 s of every window: 3 members, all 6 partitions owned exactly once",),
    "core kill: ownership restored <= 15 s after exit confirmed, caught up <= 60 s":
        ("core kill (RB2 clocks): ownership restored <= 15 s after exit confirmed; caught up <= 60 s",),
    "no inflight_commit_failed / stop_drain_failed; 0 production guard breaches":
        ("no inflight_commit_failed / stop_drain_failed", "0 production guard breaches"),
    "monitor/log coverage gaps -> NOT_MEASURED, never PASS":
        ("coverage: monitor gap <= 5 s, member logs cover every window (else NOT_MEASURED, never PASS)",),
}


# Frozen prereg addendum-5 (frozen 2026-10-15T12:17:23Z; amends addendum-4 sha256 06c561ed...) run_S2: the
# addendum-4 run_S strings verbatim except the new-image one, whose rewind budget is per surviving member (owner
# decision 2026-10-15). Same contract as the addendum-4 map: no thresholds here, unknown strings NOT_MEASURED.
CORE_GROUP_CRITERIA_MAP_ADDENDUM_5_VERSION = "p184.coregroup.criteria-map.addendum-5.v1"
CORE_GROUP_CRITERIA_MAP_ADDENDUM_5 = {
    **{text: keys for text, keys in CORE_GROUP_CRITERIA_MAP.items()
       if text not in ("new-image lives: retries 0, rewind_failed 0, rewinds <= 2 per window",
                       "control valid iff each old-image survivor of the first roll retries >= 2 times in "
                       "after_roll_core; else harness not production-like and S cannot be credited")},
    "new-image lives: retries 0, rewind_failed 0, rewinds <= 2 per surviving member per window (a member "
    "restarted in that window is not a survivor)":
        ("new-image member lives: retries == 0, rewind_failed == 0, rewinds <= 2 per surviving member per window "
         "(members restarted / killed in the window excluded), 0 in baseline",),
}
# Which versioned map judges each addendum run block.
CORE_GROUP_CRITERIA_MAPS = {
    "run_A_prime": (CORE_GROUP_CRITERIA_MAP_VERSION, CORE_GROUP_CRITERIA_MAP),
    "run_S": (CORE_GROUP_CRITERIA_MAP_VERSION, CORE_GROUP_CRITERIA_MAP),
    "run_S2": (CORE_GROUP_CRITERIA_MAP_ADDENDUM_5_VERSION, CORE_GROUP_CRITERIA_MAP_ADDENDUM_5),
}


def _combine(results):
    """FAIL if any FAIL, else NOT_MEASURED if any NOT_MEASURED, else PASS (an empty list is NOT_MEASURED)."""
    if not results:
        return "NOT_MEASURED"
    if "FAIL" in results:
        return "FAIL"
    return "NOT_MEASURED" if "NOT_MEASURED" in results else "PASS"


def core_group_acceptance(verdict, prereg, addendum=None):
    """PASS / FAIL / NOT_MEASURED for every frozen condition of ``prereg["criteria"]`` (both run_A_control and
    run_B_fix), plus the harness conditions both rely on: the run completed, every window is present, the monitor
    covers each window and every member's log covers each window. A criterion text this version does not know is
    NOT_MEASURED (``UNKNOWN_CONDITION``), never guessed.

    ``addendum`` (addendum-2, ``run_B2["criteria"]``): the per-image conditions, evaluated in ``run_B2`` with the
    same harness rows plus image attribution; the output schema is then v2 (v1 when there is no addendum)."""
    windows = verdict.get("windows") or {}
    # The judged windows of the run's mode (``--core-group-simultaneous``: CORE_GROUP_S_WINDOWS).
    names = CORE_GROUP_S_WINDOWS if verdict.get("mode") == "simultaneous" else CORE_GROUP_WINDOWS
    gap_rule = verdict.get("monitor_max_gap_rule_s", MONITOR_MAX_GAP_S)
    present = [name for name in names if name in windows]
    missing = [name for name in names if name not in windows]
    v2 = verdict.get("schema") == CORE_GROUP_VERDICT_SCHEMA

    def monitored(name):
        gap = windows[name].get("monitor_max_gap_s")
        return gap is not None and gap <= gap_rule

    def logged(name):
        covered = windows[name].get("log_covered") or {}
        return bool(covered) and all(covered.get(role) for role in CORE_MEMBERS)

    def counted(fields, limit_of):
        """FAIL on an observed excess, NOT_MEASURED when a window or its log coverage is missing."""
        results, detail = [], {}
        for name in names:
            if name not in windows:
                results.append("NOT_MEASURED")
                detail[name] = "MISSING_WINDOW"
                continue
            values = {field: windows[name].get(field) for field in fields}
            limit = limit_of(name)
            if limit is None:
                detail[name] = values
                continue
            if any(v is None for v in values.values()) or not v2:
                results.append("NOT_MEASURED")
            elif any(v > limit for v in values.values()):
                results.append("FAIL")
            else:
                results.append("PASS" if logged(name) else "NOT_MEASURED")
            detail[name] = {**values, "limit": limit, "log_covered": windows[name].get("log_covered")}
        return _combine(results), detail

    def tail_health():
        results, detail = [], {}
        for name in names:
            if name not in windows:
                results.append("NOT_MEASURED")
                detail[name] = "MISSING_WINDOW"
                continue
            w = windows[name]
            if not v2:
                results.append("NOT_MEASURED")
            elif w["tail_unhealthy_samples"]:
                results.append("FAIL")
            elif not w["tail_samples"] or w["tail_monitor_max_gap_s"] > gap_rule:
                results.append("NOT_MEASURED")
            else:
                results.append("PASS")
            detail[name] = {k: w.get(k) for k in ("tail_samples", "tail_unhealthy_samples",
                                                   "tail_monitor_max_gap_s", "tail_coverage_basis")}
        return _combine(results), detail

    def storm_false():
        final = names[-1]
        if verdict.get("storm") is True:
            return "FAIL", {"storm": True}
        if verdict.get("storm") is None or final not in windows or not v2:
            return "NOT_MEASURED", {"storm": verdict.get("storm"), "final_window": final in windows}
        w = windows[final]
        ok = w["tail_samples"] and w["tail_monitor_max_gap_s"] <= gap_rule and logged(final)
        return ("PASS" if ok else "NOT_MEASURED"), {
            "storm": False, "tail_samples": w["tail_samples"], "tail_monitor_max_gap_s": w["tail_monitor_max_gap_s"],
            "log_covered": w["log_covered"]}

    def kill_recovery():
        clocks = verdict.get("core_kill")
        if not clocks:
            return "NOT_MEASURED", {"reason": "NO_CORE_KILL"}
        detail = {k: clocks.get(k) for k in ("exit_confirmed_s", "ownership_restored_s", "progress_s",
                                              "caught_up_s", "caught_up_monitor_max_gap_s", "reason",
                                              "progress_basis", "idle_partitions", "idle_reasons")}
        moved, caught = clocks.get("ownership_restored_s"), clocks.get("caught_up_s")
        if moved is not None and moved > CORE_GROUP_LIMITS["partitions_moved_s"]:
            return "FAIL", detail
        if caught is not None and caught > CORE_GROUP_LIMITS["caught_up_s"]:
            return "FAIL", detail
        if clocks.get("reason") in ("OWNERSHIP_NOT_RESTORED", "NO_PROGRESS", "NOT_CAUGHT_UP") and \
                "after_kill" in windows and monitored("after_kill"):
            return "FAIL", detail
        if moved is None or caught is None or clocks.get("caught_up_monitor_max_gap_s") is None \
                or clocks["caught_up_monitor_max_gap_s"] > gap_rule:
            return "NOT_MEASURED", detail
        return "PASS", detail

    def drain_failures():
        totals = verdict.get("totals") or {}
        values = {k: totals.get(k) for k in ("qdl_realtime_core_inflight_commit_failed",
                                              "qdl_realtime_core_stop_drain_failed")}
        if any(v for v in values.values()):
            return "FAIL", values
        complete = not missing and all(logged(name) for name in present)
        return ("PASS" if complete and None not in values.values() else "NOT_MEASURED"), values

    def guard():
        breaches = verdict.get("production_guard_breaches")
        if breaches or "production guard breach" in str(verdict.get("error") or ""):
            return "FAIL", {"breaches": breaches, "error": verdict.get("error")}
        return ("NOT_MEASURED" if breaches is None else "PASS"), {"breaches": breaches}

    def control():
        detail, results = {}, []
        storm = verdict.get("storm")
        detail["storm"] = storm
        results.append("PASS" if storm is True else "FAIL" if storm is False else "NOT_MEASURED")
        after = [n for n in names[-2:] if n in windows]
        per_member = {role: sum(windows[n]["by_member"][role]["retries"] for n in after)
                      for role in CORE_MEMBERS} if v2 and len(after) == 2 else None
        detail["retries_per_member_after_last_roll"] = per_member
        if per_member is None:
            results.append("NOT_MEASURED")
        elif all(v >= CORE_GROUP_LIMITS["control_retries_per_member"] for v in per_member.values()):
            results.append("PASS")
        else:
            results.append("FAIL" if all(logged(n) for n in after) else "NOT_MEASURED")
        ends = {n: windows[n].get("end_members") for n in present}
        detail["members_at_window_end"] = ends
        if any(v is not None and v < len(CORE_MEMBERS) for v in ends.values()):
            results.append("PASS")
        else:
            results.append("FAIL" if not missing and all(v is not None for v in ends.values()) else "NOT_MEASURED")
        reproduced = "PASS" if "PASS" in results else _combine(results)
        return reproduced, {**detail, "sub_results": results, "rule": "any sub-condition reproduces the storm; "
                            "retries rule read strictly (every member >= 2 after the last roll)"}

    rules = {
        "storm=false": storm_false,
        "retries == 0 and rewind_failed == 0 in every window":
            lambda: counted(("retries", "rewind_failed"), lambda name: 0),
        "rewinds <= 2 per roll window and 0 in baseline":
            lambda: counted(("rewinds",), lambda name: 0 if name == "baseline" else
                            CORE_GROUP_LIMITS["rewinds_per_roll_window"] if name.startswith("after_roll_") else None),
        "last 30 s of every window: 3 members, each >= 1 partition": tail_health,
        "core_kill_caught_up_s <= 60 and dead member's partitions moved <= 15 s": kill_recovery,
        "no inflight_commit_failed / stop_drain_failed": drain_failures,
        "0 production guard breaches": guard,
    }
    harness = [
        ("harness: run COMPLETED", ("PASS" if verdict.get("status") == "COMPLETED" else
                                    "NOT_MEASURED" if verdict.get("status") in (None, "NOT_MEASURED") else "FAIL"),
         {"status": verdict.get("status"), "error": verdict.get("error")}),
        ("harness: all windows present", "PASS" if not missing else "NOT_MEASURED", {"missing": missing}),
        ("harness: monitor coverage per window (gap <= %s s)" % gap_rule,
         _combine(["PASS" if monitored(n) else "NOT_MEASURED" for n in present] + ["NOT_MEASURED"] * len(missing)),
         {n: windows[n].get("monitor_max_gap_s") for n in present}),
        ("harness: every member's retained log covers every window",
         _combine(["PASS" if v2 and logged(n) else "NOT_MEASURED" for n in present]
                  + ["NOT_MEASURED"] * len(missing)),
         {n: windows[n].get("log_covered") for n in present}),
    ]

    # -- v2: per image (addendum-2 run_B2) --
    images = verdict.get("core_images") or {}
    target = images.get("target")
    roll_windows = [n for n in names if n.startswith("after_roll_")]
    # Rolling: once the last roll is done; simultaneous: every window after the baseline.
    all_new = (roll_windows[-1], names[-1]) if roll_windows else names[1:]

    def lives_of(name):
        """(per-life rows, attributable): rows of the window's span; not attributable when the target image or a
        counted life's image is unknown, or the verdict predates per-life attribution."""
        w = windows[name]
        rows = w.get("by_life")
        if rows is None or target is None:
            return [], False
        return list(rows.values()), all(r.get("image") for r in rows.values())

    def new_image_lives():
        results, detail = [], {}
        for name in names:
            if name not in windows:
                results.append("NOT_MEASURED")
                detail[name] = "MISSING_WINDOW"
                continue
            rows, known = lives_of(name)
            new = [r for r in rows if r.get("image") == target]
            limit = 0 if name == "baseline" else CORE_GROUP_LIMITS["new_image_rewinds_per_window"]
            values = {"retries": sum(r["retries"] for r in new), "rewind_failed": sum(r["rewind_failed"] for r in new),
                      "rewinds": sum(r["rewinds"] for r in new), "rewinds_limit": limit}
            if values["retries"] or values["rewind_failed"] or values["rewinds"] > limit:
                results.append("FAIL")
            elif not v2 or not known:
                results.append("NOT_MEASURED")
            else:
                results.append("PASS" if logged(name) else "NOT_MEASURED")
            detail[name] = {**values, "lives": {f"{r['role']}.life{r['life']}": r for r in new},
                            "images_known": known, "log_covered": windows[name].get("log_covered")}
        return _combine(results), {"target_image": target, "windows": detail}

    def new_image_lives_per_survivor():
        """addendum-5 run_S2: new-image lives log no retry and no rewind failure in any window, and every
        surviving member's new-image lives <= 2 rewinds per window (0 in baseline). A member restarted / killed in
        the window's attribution span (``restarted_roles``) is not a survivor: its rewinds are reported, not
        bounded. Retries / rewind failures of any new-image life, survivor or not, still FAIL."""
        results, detail = [], {}
        for name in names:
            if name not in windows:
                results.append("NOT_MEASURED")
                detail[name] = "MISSING_WINDOW"
                continue
            rows, known = lives_of(name)
            new = [r for r in rows if r.get("image") == target]
            restarted = windows[name].get("restarted_roles")
            survivors = None if restarted is None else [role for role in CORE_MEMBERS if role not in restarted]
            limit = 0 if name == "baseline" else CORE_GROUP_LIMITS["new_image_rewinds_per_survivor_per_window"]
            per_survivor = None if survivors is None else {
                role: sum(r["rewinds"] for r in new if r["role"] == role) for role in survivors}
            excluded = {role: sum(r["rewinds"] for r in new if r["role"] == role) for role in (restarted or ())}
            values = {"retries": sum(r["retries"] for r in new), "rewind_failed": sum(r["rewind_failed"] for r in new),
                      "rewinds_per_survivor": per_survivor, "rewinds_excluded_restarted": excluded,
                      "rewinds_limit_per_survivor": limit}
            if values["retries"] or values["rewind_failed"] or \
                    (per_survivor is not None and any(v > limit for v in per_survivor.values())):
                results.append("FAIL")
            elif not v2 or not known or survivors is None:
                results.append("NOT_MEASURED")
            else:
                results.append("PASS" if logged(name) else "NOT_MEASURED")
            detail[name] = {**values, "survivors": survivors, "restarted": restarted,
                            "lives": {f"{r['role']}.life{r['life']}": r for r in new},
                            "images_known": known, "log_covered": windows[name].get("log_covered")}
        return _combine(results), {"target_image": target, "windows": detail}

    def old_image_lives():
        """Each member's old-image lives: <= 1 retry in total, only in a roll window where the member survives
        (another member is rolled); baseline: no retry, rewind or rewind failure from any life."""
        results, detail, per_member = [], {}, {role: 0 for role in CORE_MEMBERS}
        misplaced = []
        for name in names:
            if name not in windows:
                results.append("NOT_MEASURED")
                detail[name] = "MISSING_WINDOW"
                continue
            rows, known = lives_of(name)
            old = [r for r in rows if r.get("image") and r.get("image") != target]
            survivors = {role for role in CORE_MEMBERS if name.startswith("after_roll_")
                         and role != name[len("after_roll_"):]}
            for r in old:
                per_member[r["role"]] += r["retries"]
                if r["retries"] and r["role"] not in survivors:
                    misplaced.append({"window": name, "life": f"{r['role']}.life{r['life']}", "retries": r["retries"]})
            if name == "baseline" and any(r["retries"] or r["rewinds"] or r["rewind_failed"] for r in rows):
                results.append("FAIL")
            elif not v2 or not known:
                results.append("NOT_MEASURED")
            else:
                results.append("PASS" if logged(name) else "NOT_MEASURED")
            detail[name] = {"lives": {f"{r['role']}.life{r['life']}": r for r in old}, "survivors": sorted(survivors),
                            "images_known": known}
        over = {role: n for role, n in per_member.items()
                if n > CORE_GROUP_LIMITS["old_image_survivor_retries_per_member"]}
        if over or misplaced:
            results.append("FAIL")
        return _combine(results), {"target_image": target, "retries_per_member": per_member, "over_limit": over,
                                   "outside_survivor_window": misplaced, "windows": detail}

    def old_image_zero():
        """Literal: no old-image life logs a retry, rewind or rewind failure in any judged window."""
        results, detail = [], {}
        for name in names:
            if name not in windows:
                results.append("NOT_MEASURED")
                detail[name] = "MISSING_WINDOW"
                continue
            rows, known = lives_of(name)
            old = [r for r in rows if r.get("image") and r.get("image") != target]
            if any(r["retries"] or r["rewinds"] or r["rewind_failed"] for r in old):
                results.append("FAIL")
            elif not v2 or not known or target is None:
                results.append("NOT_MEASURED")
            else:
                results.append("PASS" if logged(name) else "NOT_MEASURED")
            detail[name] = {"lives": {f"{r['role']}.life{r['life']}": r for r in old}, "images_known": known}
        return _combine(results), {"target_image": target, "windows": detail}

    def all_new_retries():
        results, detail = [], {}
        for name in all_new:
            if name not in windows:
                results.append("NOT_MEASURED")
                detail[name] = "MISSING_WINDOW"
                continue
            retries = windows[name].get("retries")
            results.append("FAIL" if retries else "NOT_MEASURED" if retries is None or not v2
                           else "PASS" if logged(name) else "NOT_MEASURED")
            detail[name] = {"retries": retries, "log_covered": windows[name].get("log_covered")}
        return _combine(results), detail

    def tail_exact():
        """``tail_health`` read strictly: "owned exactly once" needs per-member partition detail (monitor v2)."""
        result, detail = tail_health()
        bases = {b for d in detail.values() if isinstance(d, dict) for b in (d.get("tail_coverage_basis") or [])}
        if result == "PASS" and bases != {"partitions"}:
            result = "NOT_MEASURED"
        return result, {"bases": sorted(bases), "windows": detail}

    def kill_recovery_v2():
        clocks = verdict.get("core_kill")
        if not clocks:
            return "NOT_MEASURED", {"reason": "NO_CORE_KILL"}
        exit_s, restored, caught = (clocks.get("exit_confirmed_s"), clocks.get("ownership_restored_s"),
                                    clocks.get("caught_up_s"))
        moved = None if exit_s is None or restored is None else round(restored - exit_s, 3)
        detail = {k: clocks.get(k) for k in ("exit_confirmed_s", "ownership_restored_s", "progress_s",
                                              "caught_up_s", "caught_up_monitor_max_gap_s", "reason",
                                              "progress_basis", "idle_partitions", "idle_reasons")}
        detail["ownership_after_exit_s"] = moved
        if moved is not None and moved > CORE_GROUP_LIMITS["partitions_moved_s"]:
            return "FAIL", detail
        if caught is not None and caught > CORE_GROUP_LIMITS["caught_up_s"]:
            return "FAIL", detail
        if clocks.get("reason") in ("OWNERSHIP_NOT_RESTORED", "NO_PROGRESS", "NOT_CAUGHT_UP") and \
                "after_kill" in windows and monitored("after_kill"):
            return "FAIL", detail
        if moved is None or caught is None or clocks.get("caught_up_monitor_max_gap_s") is None \
                or clocks["caught_up_monitor_max_gap_s"] > gap_rule:
            return "NOT_MEASURED", detail
        return "PASS", detail

    def replace_all_reformed():
        clocks = verdict.get("replace_all")
        if not clocks:
            return "NOT_MEASURED", {"reason": "NO_REPLACE_ALL"}
        detail = dict(clocks, limit_s=CORE_GROUP["reform_s"], hold_s=CORE_GROUP["formation_hold_s"])
        reformed, stable = clocks.get("reformed_s"), clocks.get("stable_s")
        if reformed is not None and reformed > CORE_GROUP["reform_s"]:
            return "FAIL", detail
        if clocks.get("reason") == "NEVER_REFORMED" and "after_replace_all" in windows and \
                monitored("after_replace_all"):
            return "FAIL", detail
        if reformed is None or stable is None or "after_replace_all" not in windows \
                or not monitored("after_replace_all"):
            return "NOT_MEASURED", detail
        return "PASS", detail

    def replace_all_caught_up():
        clocks = verdict.get("replace_all")
        if not clocks:
            return "NOT_MEASURED", {"reason": "NO_REPLACE_ALL"}
        detail = dict(clocks, limit_s=CORE_GROUP["replace_caught_up_s"])
        caught = clocks.get("caught_up_s")
        if caught is not None and caught > CORE_GROUP["replace_caught_up_s"]:
            return "FAIL", detail
        if clocks.get("reason") in ("NEVER_REFORMED", "NOT_CAUGHT_UP") and "after_replace_all" in windows and \
                monitored("after_replace_all"):
            return "FAIL", detail
        if caught is None or clocks.get("monitor_max_gap_s") is None or clocks["monitor_max_gap_s"] > gap_rule:
            return "NOT_MEASURED", detail
        return "PASS", detail

    def first_roll_reproduced():
        """A' (rolling, production-like): the storm reproduced at its production onset - each old-image survivor
        of the first roll (core-2, core-3) logs >= 2 retries in ``after_roll_core`` (production: 4 each)."""
        name = "after_roll_" + CORE_MEMBERS[0]
        if name not in windows:
            return "NOT_MEASURED", {"reason": "MISSING_WINDOW"}
        rows, known = lives_of(name)
        per = {role: sum(r["retries"] for r in rows if r["role"] == role and r.get("image")
                         and r.get("image") != target) for role in CORE_MEMBERS[1:]}
        detail = {"old_image_survivor_retries": per, "min": 2, "images_known": known,
                  "log_covered": windows[name].get("log_covered")}
        if all(v >= 2 for v in per.values()):
            return "PASS", detail
        return ("FAIL" if v2 and known and logged(name) else "NOT_MEASURED"), detail

    def coverage():
        rows = [r for r in harness if r[0].startswith(("harness: monitor coverage", "harness: every member's"))]
        result = _combine([r[1] for r in rows])
        return ("NOT_MEASURED" if result == "FAIL" else result), {r[0]: r[2] for r in rows}

    rules_v2 = {
        "storm=false": storm_false,
        "new-image member lives: retries == 0, rewind_failed == 0, rewinds <= 2 per window, 0 in baseline":
            new_image_lives,
        "new-image member lives: retries == 0, rewind_failed == 0, rewinds <= 2 per surviving member per window "
        "(members restarted / killed in the window excluded), 0 in baseline": new_image_lives_per_survivor,
        "old-image (RUST18) member lives: <= 1 retry per member, only in the roll window in which it is a survivor "
        "of a roll; 0 in baseline": old_image_lives,
        "once all 3 members run the new image (after_roll_core-3 and after_kill): retries == 0 absolute":
            all_new_retries,
        "last 30 s of every window: 3 members, all 6 partitions owned exactly once": tail_exact,
        "core kill (RB2 clocks): ownership restored <= 15 s after exit confirmed; caught up <= 60 s":
            kill_recovery_v2,
        "no inflight_commit_failed / stop_drain_failed": drain_failures,
        "0 production guard breaches": guard,
        "coverage: monitor gap <= 5 s, member logs cover every window (else NOT_MEASURED, never PASS)": coverage,
        # INCIDENT 2026-10-15 #2: rolling A' (reproduction) and simultaneous S (``--core-group-simultaneous``).
        "reproduction: each old-image survivor of the first roll retries >= 2 times in after_roll_core":
            first_roll_reproduced,
        "old-image member lives: 0 retries, rewinds and rewind failures in every judged window": old_image_zero,
        "every window after the replace (after_replace_all, after_restart_core-3, after_kill): retries == 0 "
        "absolute": all_new_retries,
        "replace-all: group re-formed (3 members, 6 partitions owned exactly once) <= 30 s after the first new "
        "member started, then stable >= 10 s": replace_all_reformed,
        "replace-all: core lag within the baseline maximum <= 60 s after the first new member started":
            replace_all_caught_up,
    }

    def evaluate(texts, extra, known=None, more=(), criteria_map=None):
        known = rules if known is None else known
        map_version, text_map = criteria_map or (CORE_GROUP_CRITERIA_MAP_VERSION, CORE_GROUP_CRITERIA_MAP)
        rows = [{"condition": c, "result": r, "detail": d} for c, r, d in (*harness, *more)]
        for text in texts:
            rule = extra.get(text) or known.get(text)
            mapped = text_map.get(text) if rule is None and known is rules_v2 else None
            if rule:
                result, detail = rule()
            elif mapped and all(key in known for key in mapped):
                parts = {key: known[key]() for key in mapped}
                result = _combine([r for r, _ in parts.values()])
                detail = {"map": map_version,
                          "rules": {key: {"result": r, "detail": d} for key, (r, d) in parts.items()}}
            else:
                result, detail = "NOT_MEASURED", {"reason": "UNKNOWN_CONDITION"}
            rows.append({"condition": text, "result": result, "detail": detail})
        return {"result": _combine([row["result"] for row in rows]), "conditions": rows}

    criteria = prereg.get("criteria") or {}
    out = {"schema": CORE_GROUP_ACCEPTANCE_SCHEMA if addendum is not None else CORE_GROUP_ACCEPTANCE_SCHEMA_V1,
           "verdict_schema": verdict.get("schema"), "prereg_schema": prereg.get("schema"),
           "limits": CORE_GROUP_LIMITS}
    if addendum is not None:
        out["addendum_schema"] = addendum.get("schema")
        lives = images.get("lives") or {}
        unknown = [f"{role}.life{n + 1}" for role, ims in lives.items() for n, im in enumerate(ims) if not im]
        image_row = ("harness: every core life's image is known (provenance / die events) and one roll target",
                     "PASS" if target and lives and not unknown else "NOT_MEASURED",
                     {"target": target, "rolled_onto": images.get("rolled_onto"), "unknown_lives": unknown,
                      "lives": lives})
        mode_row = lambda wanted: ("harness: run mode is " + wanted,  # noqa: E731
                                   "PASS" if verdict.get("mode", "rolling") == wanted else "FAIL",
                                   {"mode": verdict.get("mode")})
        # run_B2 (addendum-2/3), run_A_prime (rolling, production-like profile), run_S (simultaneous replace),
        # run_S2 (addendum-5: run_S with the per-survivor rewind budget).
        used_maps = []
        for key, wanted in (("run_B2", None), ("run_A_prime", "rolling"), ("run_S", "simultaneous"),
                            ("run_S2", "simultaneous")):
            block = addendum.get(key)
            if block is None:
                continue
            # addendum-4 freezes run_A_prime's single condition as ``criterion`` (a string), run_S as ``criteria``.
            texts = block.get("criteria")
            if texts is None and isinstance(block.get("criterion"), str):
                texts = [block["criterion"]]
            more = (image_row,) if wanted is None else (image_row, mode_row(wanted))
            if not texts:  # a frozen run without a readable criterion fails closed
                more = (*more, ("harness: frozen criteria present", "NOT_MEASURED", {"keys": sorted(block)}))
            out[key] = evaluate(list(texts or ()), {}, rules_v2, more, CORE_GROUP_CRITERIA_MAPS.get(key))
            if key in CORE_GROUP_CRITERIA_MAPS and CORE_GROUP_CRITERIA_MAPS[key][0] not in used_maps:
                used_maps.append(CORE_GROUP_CRITERIA_MAPS[key][0])
        if used_maps:
            out["criteria_map"] = used_maps[0] if len(used_maps) == 1 else used_maps
    fix = criteria.get("run_B_fix")
    if fix is not None:
        out["run_B_fix"] = evaluate(list(fix), {})
    control_text = criteria.get("run_A_control")
    if control_text is not None:
        out["run_A_control"] = evaluate([control_text], {control_text: control})
    return out


def core_group_accept(base, prereg_path, addendum_path=None):
    """``--core-group-accept``: re-evaluate SAVED evidence (no workload, nothing written): recompute the verdict
    from ``<base>/evidence``, take status/error from its saved ``core-group-verdict.json``, apply the prereg (and,
    with ``--addendum``, addendum-2's per-image run_B2 criteria)."""
    evidence = Path(base) / "evidence"
    verdict = core_group_verdict(evidence)
    saved = evidence / "core-group-verdict.json"
    if saved.exists():
        recorded = json.loads(saved.read_text())
        verdict.update({k: recorded.get(k) for k in ("status", "error", "namespace", "new_core")})
    prereg = json.loads(Path(prereg_path).read_text())
    addendum = json.loads(Path(addendum_path).read_text()) if addendum_path is not None else None
    return {"base": str(base), "prereg_sha256": hashlib.sha256(Path(prereg_path).read_bytes()).hexdigest(),
            "addendum_sha256": (hashlib.sha256(Path(addendum_path).read_bytes()).hexdigest()
                                if addendum_path is not None else None),
            "core_images": verdict.get("core_images"),
            "by_life": {n: w.get("by_life") for n, w in verdict["windows"].items()},
            "verdict": {k: verdict.get(k) for k in ("schema", "mode", "status", "storm", "totals", "replace_all",
                                                     "core_kill", "baseline_formation",
                                                     "core_kill_first_healthy_sample_s", "production_guard_breaches")},
            "windows": {n: {k: w.get(k) for k in ("retries", "rewinds", "rewind_failed", "monitor_max_gap_s",
                                                   "tail_samples", "tail_unhealthy_samples", "tail_coverage_basis",
                                                   "end_members", "log_covered", "restarted_roles")}
                        for n, w in verdict["windows"].items()},
            "acceptance": core_group_acceptance(verdict, prereg, addendum)}


def orchestrate_core_group(base, export, ns, images, new_core, profile=None, simultaneous=False):
    """INCIDENT 2026-10-15 reproduction: 3 core members (one group, 6 raw partitions) on ``images["core"]``;
    baseline; rolling replace of each member onto ``new_core`` with a hold after each; SIGKILL of one member
    (restarted at once); teardown; ``evidence/core-group-verdict.json``. Exits 0 when it ran to the end.

    ``profile`` (``core_profile``; None = the B3 defaults): core batch/wait/CPU, publisher rate, broker initial
    rebalance delay, the gap between a replaced member's exit and its replacement's start, and K precondition
    restarts (old image, diagnostic windows) before the baseline. ``simultaneous`` (``--core-group-simultaneous``):
    instead of the rolling replace, stop every member at once and start all on ``new_core`` (window
    ``after_replace_all``), then a SIGTERM restart of CORE_GROUP["restart_member"] and the SIGKILL of
    CORE_GROUP["kill_member"] on the all-new group."""
    run = Run(base, export, ns, images)
    run.profile = core_profile(**(profile or {}))
    gap = run.profile["replace_gap_s"]

    def expire(_signum, _frame):
        raise TimeoutError("core-group orchestrator absolute deadline")

    def terminated(signum, _frame):
        raise SystemExit(f"core-group orchestrator received signal {signum}")
    previous = signal.signal(signal.SIGALRM, expire)
    previous_term = {sig: signal.signal(sig, terminated) for sig in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)}
    signal.alarm(LIMITS["deadline_s"])
    status, error = "FAIL", None
    try:
        run.prepare()
        (run.evidence / "verdict-ns.json").write_text(json.dumps({"namespace": ns}))
        formation_start = time.time_ns()
        run.core_group_up()
        run.core_group_formation(formation_start)
        for index in range(run.profile["precondition_restarts"]):
            run.core_replace(CORE_GROUP["precondition_member"], gap_s=gap, kind="core_precondition")
            run.core_group_window(f"precondition_{index + 1}", CORE_GROUP["precondition_hold_s"],
                                  role=CORE_GROUP["precondition_member"], diagnostic=True)
        run.core_group_window("baseline", CORE_GROUP["baseline_s"])
        if simultaneous:
            run.core_replace_all(new_core, gap_s=gap)
            run.core_group_window("after_replace_all", CORE_GROUP["hold_s"])
            restart = CORE_GROUP["restart_member"]
            run.core_replace(restart, image=new_core, gap_s=gap)
            run.core_group_window("after_restart_" + restart, CORE_GROUP["hold_s"], role=restart)
        else:
            for role in CORE_MEMBERS:
                run.core_replace(role, image=new_core, gap_s=gap)
                run.core_group_window("after_roll_" + role, CORE_GROUP["hold_s"], role=role)
        run.core_replace(CORE_GROUP["kill_member"], kill=True)
        run.core_group_window("after_kill", CORE_GROUP["hold_s"], role=CORE_GROUP["kill_member"])
        status = "COMPLETED"
    except BaseException as exc:  # noqa: BLE001 - evidence first, then teardown
        error = f"{type(exc).__name__}: {exc}"
        if isinstance(exc, CoreGroupNotMeasured):
            status = "NOT_MEASURED"
        print("STOPPED", error, flush=True)
        if run.evidence.exists():
            run.note("stopped", error=error)
    finally:
        signal.alarm(0)
        try:
            run.down() if run.evidence.exists() else teardown(ns)
        finally:
            if run.evidence.exists() and (run.evidence / "verdict-ns.json").exists():
                verdict = core_group_verdict(run.evidence)
                verdict.update(status=status, error=error, namespace=ns, images=run.images, new_core=new_core,
                               requested_mode="simultaneous" if simultaneous else "rolling")
                (run.evidence / "core-group-verdict.json").write_text(json.dumps(verdict, indent=2) + "\n")
            signal.signal(signal.SIGALRM, previous)
            for sig, handler in previous_term.items():
                signal.signal(sig, handler)
    return 0 if status == "COMPLETED" else 1


def keep_diagnostics(out):
    """Detached --rm roles lose their stderr when they exit: write it (and a native
    crash's faulthandler dump) next to the evidence file (final-image attempt 3,
    2026-10-10: the monitor exited during the broker fault with no log left)."""
    if not out:
        return None
    import faulthandler
    stream = open(str(out) + ".stderr", "a", buffering=1)
    sys.stderr = stream
    faulthandler.enable(file=stream, all_threads=True)
    stream.write(json.dumps({"k": "start", "t": time.time_ns(), "pid": os.getpid()}) + "\n")
    return stream


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    for flag in ("run", "core_group", "core_group_simultaneous", "core_group_accept", "down", "analyze", "topics", "publish", "monitor", "feeds",
                 "query", "internal_configs", "apply_internal_configs", "parity_snapshot", "credit"):
        mode.add_argument("--" + flag.replace("_", "-"), type=Path)
    parser.add_argument("--export", type=Path)
    parser.add_argument("--namespace")
    parser.add_argument("--out")
    parser.add_argument("--seconds", type=float)
    parser.add_argument("--hz", type=float, default=PUBLISH_HZ)
    parser.add_argument("--image-set", type=Path, help="JSON object role -> sha256:<64hex> (defaults: IMAGES)")
    parser.add_argument("--image", action="append", default=[], metavar="ROLE=sha256:<64hex>")
    parser.add_argument("--query-code", choices=QUERY_CODE, default="worktree")
    parser.add_argument("--auto-leader-rebalance", choices=("true", "false"),
                        help="--run (required): brokers' auto.leader.rebalance.enable, production's value; recorded; "
                             "must equal the --production-parity snapshot")
    parser.add_argument("--production-parity", type=Path,
                        help="--run (required) / --credit: a --parity-snapshot document (Kafka profile mirrored on the "
                             "brokers/topics + deployed writer reference); --credit: the CURRENT production snapshot")
    parser.add_argument("--cert-dir", type=Path,
                        help="--parity-snapshot / --run: stable cert dir (mounted ro into the admin container that "
                             "describes the production topics; default: the Kafka B runbook's)")
    parser.add_argument("--ts-root", type=Path,
                        help="--run: TS checkout the WriterClone module is loaded from (default %s)" % TS_ROOT)
    parser.add_argument("--new-core", metavar="sha256:<64hex>",
                        help="--core-group: the core image each member is rolled onto (old: the image set's core)")
    parser.add_argument("--prereg", type=Path,
                        help="--core-group-accept: the frozen prereg JSON whose criteria are evaluated")
    parser.add_argument("--addendum", type=Path,
                        help="--core-group-accept: a frozen prereg addendum (addendum-2: per-image run_B2 criteria)")
    # INCIDENT 2026-10-15 #2: production-like core-group profile (``core_profile``; omitted = the B3 harness).
    profile_flags = (("--core-batch-size", "batch_size", int, "core.json batch_size (production 1024)"),
                     ("--core-batch-wait-ms", "batch_wait_ms", int, "core.json batch_wait_ms (production 25)"),
                     ("--core-hz", "hz", float, "publisher BOOK frames/s per venue (<= 20; default 4)"),
                     ("--core-cpus", "core_cpus", str, "CPUs per core member (production 1.0; default 0.5)"),
                     ("--core-replace-gap-s", "replace_gap_s", float,
                      "seconds between a replaced member's exit and its replacement's start (default 0)"),
                     ("--core-precondition-restarts", "precondition_restarts", int,
                      "old-image restarts of %s before the baseline (default 0)" % CORE_GROUP["precondition_member"]),
                     ("--core-initial-rebalance-delay-ms", "initial_rebalance_delay_ms", int,
                      "brokers' group.initial.rebalance.delay.ms (production 3000; default 0)"))
    for flag, dest, kind, text in profile_flags:
        parser.add_argument(flag, dest="profile_" + dest, type=kind, help="--core-group[-simultaneous]: " + text)
    args = parser.parse_args(argv)
    profile = {dest: getattr(args, "profile_" + dest) for _, dest, _, _ in profile_flags
               if getattr(args, "profile_" + dest) is not None}
    if profile and not (args.core_group or args.core_group_simultaneous):
        parser.error("--core-* profile options need --core-group or --core-group-simultaneous")
    try:
        profile = core_profile(**profile)
    except ValueError as error:
        parser.error(str(error))
    if args.core_group_accept:
        if args.prereg is None:
            parser.error("--core-group-accept needs --prereg")
        print(json.dumps(core_group_accept(args.core_group_accept, args.prereg, args.addendum), indent=2,
                         default=str))
        return 0
    if args.core_group or args.core_group_simultaneous:
        if args.export is None or args.namespace is None or not re.fullmatch(r"sha256:[0-9a-f]{64}",
                                                                              args.new_core or ""):
            parser.error("--core-group needs --export, --namespace and --new-core sha256:<64hex>")
        images = resolve_image_set(args.image_set, args.image)
        if image_record(args.new_core)["id"] != args.new_core:
            parser.error("--new-core is not a local image id")
        return orchestrate_core_group(args.core_group or args.core_group_simultaneous, args.export, args.namespace,
                                      images, args.new_core, profile=profile,
                                      simultaneous=args.core_group_simultaneous is not None)
    def production_observer():
        from scripts.kafka_b_roll_runbook import CERT
        from scripts.kn_state_topics_packet import AdminContainerRunner
        cert = args.cert_dir or CERT
        if not Path(cert).is_dir():
            parser.error(f"--cert-dir {cert} is not a directory")
        runner = AdminContainerRunner(Path(cert))
        return lambda: observe_production_kafka(runner, docker_inspect_record)

    if args.parity_snapshot:
        doc = production_parity_snapshot(docker_inspect_record, production_observer())
        with Path(args.parity_snapshot).open("x") as handle:  # exclusive create: a snapshot is never overwritten
            handle.write(json.dumps(doc, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"written": str(args.parity_snapshot), "kafka": doc["kafka"],
                          "effective_sha256": doc["effective"]["sha256"],
                          "writer_image": doc["writer"]["image"]}, sort_keys=True))
        return 0
    if args.credit:
        verdict = json.loads((args.credit / "evidence" / "verdict.json").read_text())
        current = current_effective = None
        if args.production_parity is not None:
            current_doc = validate_parity(json.loads(args.production_parity.read_text()))
            current, current_effective = current_doc["kafka"], current_doc["effective"]
        ok, reason = g12_receipt_creditable(verdict, current, current_effective)
        print(json.dumps({"creditable": ok, "reason": reason, "final_profile_checked": current is not None,
                          "production_parity": (verdict.get("broker_config") or {}).get("production_parity"),
                          "kafka_profile": (verdict.get("broker_config") or {}).get("kafka_profile")}, sort_keys=True))
        return 0 if ok else 1
    if args.run:
        if args.export is None or args.namespace is None or args.auto_leader_rebalance is None \
                or args.production_parity is None:
            parser.error("--run needs --export, --namespace, --auto-leader-rebalance true|false and "
                         "--production-parity FILE")
        images = resolve_image_set(args.image_set, args.image)
        parity = validate_parity(json.loads(args.production_parity.read_text()))
        if parity["writer"]["image"] != images["ts"]:
            parser.error(f"image set ts {images['ts']} is not the deployed writer {parity['writer']['image']} "
                         "of the parity snapshot")
        return orchestrate(args.run, args.export, args.namespace, images, args.query_code,
                           auto_leader_rebalance=args.auto_leader_rebalance == "true", parity=parity,
                           ts_root=args.ts_root, observe_production=production_observer())
    if args.down:
        ns = namespace(json.loads((args.down / "work" / "handoff.json").read_text())["namespace"])
        report = teardown(ns)
        erase_private(args.down / "work")
        print(json.dumps(report))
        return 0 if not report["errors"] and report["left_by_label"] == 0 else 1
    if args.analyze:
        print(json.dumps(evaluate(args.analyze / "evidence" if (args.analyze / "evidence").exists()
                                  else args.analyze), indent=2))
        return 0
    if args.feeds:
        print(json.dumps(extend_material(args.feeds)))
        return 0
    if args.query:
        serve_query(args.query)
        return 0
    if args.internal_configs:
        print(json.dumps(internal_topic_configs(args.internal_configs), sort_keys=True))
        return 0
    if args.apply_internal_configs:
        print(json.dumps(apply_internal_configs(args.apply_internal_configs), sort_keys=True))
        return 0
    if args.seconds is None and not args.topics:
        parser.error("--seconds required")
    if args.topics:
        print(json.dumps(create_topics(args.topics)))
        return 0
    keep_diagnostics(args.out)
    if args.publish:
        print(json.dumps(publish(args.publish, args.out, args.seconds, args.hz)))
    else:
        print(json.dumps(monitor(args.monitor, args.out, args.seconds)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
