#!/usr/bin/env python3
"""P18.4 gate G13: run the DR01-DR08 steps that cannot be inherited, once, on the final DL + TS writer digests.

Purpose. G13 (TS upgrade/p184-phase-d-gates.md) closes every DR cell of the frozen R2.5 case matrix
(contracts/baseline/rust-first-v2/p18/p184-r2-case-matrix.v2.json in the TS repo: 76 DR cells = the 65 of v1 plus
11 inverse cells scoped OKX_INVERSE; v1 stays readable) on the final
Data Layer python/rust images and the final TS writer image. The inherit/rerun decision per DR is recorded in
upgrade/evidence/p184-g13-inherit-rerun-matrix.md; DECISIONS below is the same table in code. This runner calls
ONLY the existing harnesses, sequentially, each under a hard timeout:
  ISOLATED phase (disposable namespaces, nothing shared is written):
    dr05-upstream   DL scripts/p184_dr05_dl_fault.py + p184_dr05_reset.py + p184_dr05_channel.py, TS
                    scripts/p184_dr05_upstream.py (GAP_REFUSAL / GENERATION_CHANGE_FENCED; scopes BINANCE, OKX and
                    OKX_INVERSE = BTC-USD-SWAP BOOK, defined by the --catalog input `dr05_catalog`)
    dr08-isolated   DL scripts/p184_dr08_isolated.py --orchestrate (QUALIFICATION mode, >= 5 restart samples)
  RUNTIME phase (reads the shared stack after the final rollout; needs the owner-approved runtime packet):
    dr01-deployed   DL scripts/p184_dr01_deployed.py prepare/probe/record (7 DL cells); inputs dr01.manifest_dir
                    (the deployed reader realm dir) + dr01.manifest_revision, optional dr01.wrong_revision.
                    prepare runs offline in a --rm container of the final python image (network none, host uid,
                    read-only mounts of the DL checkout and the declared inputs); probe runs per venue in that
                    venue's alpha compose service, dr01.alpha_service = {"BINANCE": svc, "OKX": svc}
    dr01-wrong-account TS scripts/p184_dr01_wrong_account.py probe (inside the deployed Gateway) + record;
                    GATEWAY-DEPENDENT: refused as GATEWAY_NOT_RUNNING (other steps still run) unless the Gateway
                    container runs the declared gateway image; it closes with the TS release (IDR-085 Gateway).
                    The run id is fixed (dr01_wrong_account.run_id): the Gateway writer scope is an exact account
                    list checked before the API key, so the 4 runner accounts (plan field writer_scope_accounts)
                    must be in the Gateway's WRITER_ACCOUNT_SCOPE_JSON; checked before the probe. The probe refuses
                    a run id whose alphas exist (RUN_IDENTITY_NOT_FRESH), so each real run needs a new run id
    dr02-dr03-alpha TS scripts/p184_dr_alpha_reads.py probe (inside each alpha runtime) + record
    dr04-dr06-views TS scripts/p184_dr_views.py (inside the TS writer image; binance, okx, okx_inverse) + record;
                    also DR05.QUIET_TYPED (linear only: QUIET is a TRADE state, TRADE is not granted for inverse)
    dr05-transport  TS scripts/p184_dr05_isolated.py (writer clone, transport cut at the clone only; same 3 scopes)
    dr07-reference  TS scripts/p184_dr07_reference.py in a --rm container of the TS image (dr07.image, default
                    the writer image: alpha_sdk, qdl_sdk, asyncpg) on the DL consumer + TS database networks; mounts
                    the TS checkout, the connections file and the identity file dirs read-only, the cursor dirs and
                    the step dir read-write; POSTGRES_DSN only via --env-file dr07.env_file (0600, never printed).
                    Root with CAP_DAC_OVERRIDE only: the alpha identity dirs are uid 10001 mode 0700
Receipts: a NEW directory ~/.local/state/qdl-v2/p184-g13-<YYYYMMDD>T<HHMMSS>Z/ (exclusive create) with plan.json,
one <step>/receipt.json per step (argv, cwd, times, exit, timeout, output sha256, cleanup) and summary.json.

Boundary. Never builds an image, never starts/stops/recreates a shared container, never writes shared Redis/Kafka/
Postgres, never pushes. The DR05/DR08 harnesses get the final images as explicit arguments (--image ROLE=ID,
DR08 also --query-image-ref; their in-container checks compare against these attested values; kafka/redis keep the
harness defaults). It refuses to start a step when (a) the TS case matrix v2 DR cells differ from DR_CELLS, (b) the
DL checkout that the harness containers mount at /app is not the release commit or has tracked edits, (c) for
the RUNTIME phase, the deployed roles do not run the final digests or no approval packet is given. Cleanup: only
containers whose name starts with the step's own prefix AND that did not exist before the step are removed, by
exact ID; nothing else (no prune, no image/volume/network removal). The harnesses keep their own cleanup receipts.
A step result is the harness's own rows; this runner never writes a case row and never relabels a rehearsal.

usage: p184_g13_final.py --python-image sha256:.. --rust-image sha256:.. --python-image-ref qdl-v2-python:2.2.3-<sha7>
         --writer-image sha256:.. --ts-source-sha <40hex> --dl-source-sha <40hex> [--phase isolated|runtime|all]
         [--only STEP ...] [--inputs inputs.json] [--runtime-approval PACKET] [--dry-run] [--receipts DIR]
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import signal
import stat
import subprocess
import sys
import time
from datetime import datetime, timezone

DL_ROOT = Path(__file__).resolve().parents[1]
TS_ROOT_DEFAULT = Path("/home/bobby/.worktrees/trading-system-next-upgrade")
STATE_ROOT = Path.home() / ".local/state/qdl-v2"
VENUES = ("BINANCE", "OKX")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
SHA = re.compile(r"[0-9a-f]{40}")
STACK = "qdl_v2_stable_candidate-"
WRITER = "market_data_service_p184w"
# Deployed role -> which final image it must run (D5 rollout packet inventory, 2026-10-10).
ROLE_IMAGE = {
    "query_kn_1": "python", "query_kn_2": "python", "binance_bar_edge": "python",
    "ingestor_binance_usdm": "rust", "ingestor_okx_swap": "rust",
    "rust_core": "rust", "rust_core_2": "rust", "rust_core_3": "rust",
    "market_projector_1": "rust", "market_projector_2": "rust",
    "stream_kn_1": "rust", "stream_kn_2": "rust",
}
# Harness image roles set to the final images (scripts/p184_dr08_isolated.py IMAGES stay the defaults for the rest).
HARNESS_ROLES = {"query": "python", "python": "python", "core": "rust", "stream": "rust"}
MATRIX_V2 = "contracts/baseline/rust-first-v2/p18/p184-r2-case-matrix.v2.json"
GATEWAY_STEPS = ("dr01-wrong-account",)


def cells(case, names, venues=VENUES):
    return tuple(f"{case}.{name}@{venue}" for name in names for venue in venues)


INVERSE = ("OKX_INVERSE",)  # TS case matrix v2: OKX BTC-USD-SWAP, binding r23 QUOTE/BOOK/MARK_INDEX only
INV_DR04 = cells("DR04", ("L2_SNAPSHOT_DELTA_SEQUENCE_DEPTH", "MARK_INDEX_COMPONENTS", "QUOTE_BBO",
                          "RISK_REREADS_OWN_AUTHORITY"), INVERSE)
INV_DR05 = cells("DR05", ("APPROVED_FALLBACK_AND_RETURN_NO_FORBIDDEN_V1", "DISCONNECT_RECONNECT", "GAP_REFUSAL",
                          "GENERATION_CHANGE_FENCED", "STALE_TYPED"), INVERSE)
INV_DR06 = cells("DR06", ("EXPLICIT_LAST_L2_MARK_STILL_FAIL_WHEN_INVALID",
                          "UNRELATED_STALE_TRADE_DOES_NOT_BLOCK_MARKET_LIMIT"), INVERSE)


DR_CELLS = {
    "DR01": cells("DR01", ("BINDING_COMPILE", "WRONG_ACCOUNT_DENIED", "WRONG_REALM_DENIED", "WRONG_REVISION_DENIED"))
            + ("DR01.NO_GLOBAL_GRANT@ANY",),
    "DR02": cells("DR02", ("HISTORY_LIMIT_BOUNDED_BUFFER", "MIXED_ITEM_OUTCOMES_VISIBLE",
                           "WARMUP_SINGLE_SYMBOL_EXACT_CLOSED_ROWS", "WARMUP_UNIVERSE_CONTEXT_BATCH")),
    "DR03": cells("DR03", ("APPEND_1M_NO_FUTURE_SKIP_DUPLICATE", "NO_FULL_HISTORY_RELOAD_PER_CYCLE",
                           "RESTART_SAME_CANDLE_NO_DUPLICATE_DECISION")),
    "DR04": cells("DR04", ("EXPLICIT_LAST", "L2_SNAPSHOT_DELTA_SEQUENCE_DEPTH", "MARK_INDEX_COMPONENTS", "QUOTE_BBO",
                           "RISK_REREADS_OWN_AUTHORITY")) + INV_DR04,
    "DR05": cells("DR05", ("APPROVED_FALLBACK_AND_RETURN_NO_FORBIDDEN_V1", "DISCONNECT_RECONNECT", "GAP_REFUSAL",
                           "GENERATION_CHANGE_FENCED", "QUIET_TYPED", "STALE_TYPED")) + INV_DR05,
    "DR06": cells("DR06", ("EXPLICIT_LAST_L2_MARK_STILL_FAIL_WHEN_INVALID",
                           "UNRELATED_STALE_TRADE_DOES_NOT_BLOCK_MARKET_LIMIT")) + INV_DR06,
    "DR07": cells("DR07", ("FUNDING_RATE", "INSTRUMENT_METADATA_PROVENANCE", "LONG_SHORT_RATIO", "OPEN_INTEREST",
                           "TAKER_FLOW")) + ("DR07.FX_PROOF@ANY", "DR07.REFERENCE_NEVER_EXECUTION_AUTHORITY@ANY"),
    "DR08": tuple(f"DR08.{n}@ANY" for n in ("ALERT_PATH_OBSERVABLE", "CONSUMER_RESTART_RECOVERY_MEASURED",
                                            "NO_CROSS_CONSUMER_STARVATION", "SLOW_CONSUMER_BOUNDED_QUEUE")),
}
# Every DR cell has a producer: the inverse DR05 faults run in the DL fault harness (--scope OKX_INVERSE with a
# source catalog defining BTC-USD-SWAP) and the TS writer clone (--venues ... OKX_INVERSE). Kept for listing open cells.
NO_PRODUCER = ()
DR05_SCOPES = VENUES + INVERSE  # p184_dr05_dl_fault.py --scope / TS p184_dr05_upstream.py, p184_dr05_isolated.py --venues

# DR -> (decision, reason). Same table as upgrade/evidence/p184-g13-inherit-rerun-matrix.md.
DECISIONS = {
    "DR01": ("RERUN", "no deployed-probe receipt (only CONTROLLED_SIM rehearsal, zero provenance); "
                      "qdl/api_v2/router.py changed since rc2 43bbcf0"),
    "DR02": ("RERUN", "no receipt; warmup/batch path changed (65e0757, 76950c8, 47bec04, SDK 5c81c25/359c9ee)"),
    "DR03": ("RERUN", "no receipt; BAR history/cold path changed (76950c8, 65e0757)"),
    "DR04": ("RERUN", "no receipt; MARK/INDEX + public view changed (922a6ec, bd74faa, ec89a31, 57881a2, 616342c, "
                      "1a2daf1) and the writer digest is new"),
    "DR05": ("RERUN", "GAP/GENERATION r16 is an ISOLATED_FAULT rehearsal on core overlay baafcb48 (d68fb2f), rc2 "
                      "python, stream 1f13408f, writer 411a6a8; core 1a2daf1, query 57881a2/922a6ec, stream "
                      "0b374bc/fdd379e changed since; STALE/DISCONNECT/FALLBACK/QUIET never produced"),
    "DR06": ("RERUN", "no receipt; production validator reads the changed MARK/L2 views; new writer digest"),
    "DR07": ("RERUN", "no R2 DR07 rows exist (10-07 'DR07' was runtime hardening, a different label); "
                      "router.py changed"),
    "DR08": ("RERUN", "DEVELOPMENT rows only (n=1, zero TS provenance, stream binary overlay); stream fdd379e "
                      "(hub.rs) changed after the overlay; final mode needs >= 5 restart samples"),
}

STEP_SPEC = (
    # id, phase, cells, timeout_s, est_minutes, owned container prefixes
    ("dr05-upstream", "isolated", cells("DR05", ("GAP_REFUSAL", "GENERATION_CHANGE_FENCED"), DR05_SCOPES), 1800, 6,
     ("p184-dr05-",)),
    ("dr08-isolated", "isolated", DR_CELLS["DR08"], 1500, 12, ("p184-dr08-",)),
    ("dr01-deployed", "runtime", tuple(c for c in DR_CELLS["DR01"] if "WRONG_ACCOUNT" not in c), 600, 3,
     ("p184-g13-dr01-",)),
    ("dr01-wrong-account", "runtime", cells("DR01", ("WRONG_ACCOUNT_DENIED",)), 600, 2, ()),
    ("dr02-dr03-alpha", "runtime", DR_CELLS["DR02"] + DR_CELLS["DR03"], 1200, 10, ()),
    ("dr04-dr06-views", "runtime", DR_CELLS["DR04"] + DR_CELLS["DR06"] + cells("DR05", ("QUIET_TYPED",)), 1200, 14,
     ()),
    ("dr05-transport", "runtime", cells("DR05", ("STALE_TYPED", "DISCONNECT_RECONNECT",
                                                 "APPROVED_FALLBACK_AND_RETURN_NO_FORBIDDEN_V1"), DR05_SCOPES), 1500, 8,
     ("p184-dr05-",)),
    ("dr07-reference", "runtime", DR_CELLS["DR07"], 600, 2, ("p184-g13-dr07-",)),
)
STEPS = tuple(s[0] for s in STEP_SPEC)


class Blocked(Exception):
    """A typed refusal before any step side effect."""


# ---------------------------------------------------------------- pure plan logic (unit-tested)

def coverage():
    """Every DR cell is either produced by exactly one step or listed in NO_PRODUCER."""
    produced = [c for s in STEP_SPEC for c in s[2]]
    every = [c for case in sorted(DR_CELLS) for c in DR_CELLS[case]]
    duplicates = sorted({c for c in produced if produced.count(c) > 1})
    missing = sorted(set(every) - set(produced) - set(NO_PRODUCER))
    extra = sorted(set(produced) - set(every))
    return {"cells": len(every), "produced": len(set(produced)), "no_producer": list(NO_PRODUCER),
            "duplicates": duplicates + sorted(set(produced) & set(NO_PRODUCER)), "missing": missing, "extra": extra}


def harness_image_args(final):
    """--image ROLE=ID for every harness role that must run a final image (both harnesses accept the flag)."""
    out = []
    for role, kind in HARNESS_ROLES.items():
        out += ["--image", f"{role}={final[kind]}"]
    return out


def matrix_dr_cells(matrix):
    """DR cell ids of a loaded TS case matrix (v1 or v2); unknown schemas refused."""
    if matrix.get("schema") not in ("p184.r2-case-matrix.v1", "p184.r2-case-matrix.v2"):
        raise Blocked(f"CASE_MATRIX_SCHEMA: {matrix.get('schema')!r}")
    return sorted(c["id"] for c in matrix["cells"] if c["case"].startswith("DR"))


def matrix_mismatch(matrix):
    """Empty when DR_CELLS equals the TS matrix's DR cells exactly."""
    ours = sorted(c for case in DR_CELLS.values() for c in case)
    theirs = matrix_dr_cells(matrix)
    return {"missing_here": sorted(set(theirs) - set(ours)), "extra_here": sorted(set(ours) - set(theirs))} \
        if ours != theirs else {}


def validate_final(final):
    for key in ("python", "rust", "writer"):
        value = final.get(key) or ""
        if not DIGEST.fullmatch(value) or len(set(value[7:])) == 1:
            raise Blocked(f"FINAL_DIGEST_REQUIRED: --{key}-image must be a real sha256 image ID")
    for key in ("ts_sha", "dl_sha"):
        value = final.get(key) or ""
        if not SHA.fullmatch(value) or len(set(value)) == 1:
            raise Blocked(f"FINAL_SHA_REQUIRED: {key} must be a full 40-hex commit")
    if final["python"] == final["rust"]:
        raise Blocked("FINAL_DIGEST_REQUIRED: python and rust images must differ")
    if not re.fullmatch(r"qdl-v2-python:\S+", final.get("python_ref") or ""):
        raise Blocked("FINAL_REF_REQUIRED: --python-image-ref qdl-v2-python:<release tag>")


def deployed_mismatches(inventory, final):
    """inventory: {container name: image ID}. Empty when every listed role and the writer run the final digests."""
    found = []
    for role, kind in ROLE_IMAGE.items():
        name = STACK + role + "-1"
        if inventory.get(name) != final[kind]:
            found.append(f"{name} runs {inventory.get(name)} != final {kind} {final[kind]}")
    if inventory.get(WRITER) != final["writer"]:
        found.append(f"{WRITER} runs {inventory.get(WRITER)} != final writer {final['writer']}")
    return found


def select(phase, only=()):
    """Ordered step ids: isolated before runtime, matrix order otherwise; --only narrows, never reorders."""
    unknown = sorted(set(only) - set(STEPS))
    if unknown:
        raise Blocked("UNKNOWN_STEP: " + ", ".join(unknown))
    phases = {"isolated": ("isolated",), "runtime": ("runtime",), "all": ("isolated", "runtime")}[phase]
    chosen = [s[0] for p in phases for s in STEP_SPEC if s[1] == p
              and DECISIONS[s[2][0].split(".")[0]][0] == "RERUN" and (not only or s[0] in only)]
    if not chosen:
        raise Blocked("NOTHING_SELECTED")
    return chosen


def required_inputs(step):
    return {
        "dr05-upstream": ("dr05_public_export", "dr05_catalog", "redis_image", "postgres_image"),
        "dr08-isolated": (),
        "dr01-deployed": ("dr01.inventory", "dr01.deployments.BINANCE", "dr01.deployments.OKX", "dr01.compose",
                          "dr01.alpha_service.BINANCE", "dr01.alpha_service.OKX", "dr01.manifest_dir",
                          "dr01.manifest_revision"),
        "dr01-wrong-account": ("gateway_container", "gateway_image_digest", "dr01_wrong_account.run_id"),
        "dr02-dr03-alpha": ("alpha.containers.BINANCE", "alpha.containers.OKX", "alpha.symbols.BINANCE",
                            "alpha.symbols.OKX"),
        "dr04-dr06-views": ("ts_container",),
        "dr05-transport": ("redis_image", "postgres_image"),
        "dr07-reference": ("dr07.connections", "dr07.fx_account", "dr07.env_file"),
    }[step]


def lookup(inputs, dotted):
    value = inputs
    for part in dotted.split("."):
        if not isinstance(value, dict) or part not in value:
            return None
        value = value[part]
    return value


def missing_inputs(steps, inputs):
    return {s: [k for k in required_inputs(s) if lookup(inputs, k) in (None, "")] for s in steps
            if any(lookup(inputs, k) in (None, "") for k in required_inputs(s))}


RUN_ID = re.compile(r"[0-9a-z]{6,32}")  # TS scripts/p184_dr01_wrong_account.py identities()
DR07_NETWORKS = ("qdl_v2_native_consumer", "executor_network")  # DL readers/stream; TS Postgres
# DataLayerV2ConnectionConfig file fields (TS alpha_sdk/data_layer_v2.py): identity files are read-only, the SDK
# writes its cursor file.
DR07_READ_FILES = ("credential_file", "tls_ca_file", "tls_certificate_file", "tls_private_key_file",
                   "jwt_private_key_file")


def wrong_account_identities(run_id):
    """The runner alphas/accounts of a fixed run id; same derivation as TS p184_dr01_wrong_account.identities."""
    if not isinstance(run_id, str) or not RUN_ID.fullmatch(run_id):
        raise Blocked("DR01_RUN_ID_INVALID: dr01_wrong_account.run_id must match [0-9a-z]{6,32}")
    alphas = {label: f"p184-dr01-{run_id}-{label.lower()}" for label in ("A", "B")}
    return {"run_id": run_id, "alphas": alphas,
            "accounts": {label: {v: f"paper-{alphas[label]}-{v.lower()}" for v in VENUES} for label in alphas}}


def writer_scope_accounts(run_id):
    """The exact account ids the Gateway WRITER_ACCOUNT_SCOPE_JSON include list must carry for this run id."""
    ids = wrong_account_identities(run_id)
    return sorted(acc for per in ids["accounts"].values() for acc in per.values())


def writer_scope_problem(raw, accounts):
    """None when a Gateway WRITER_ACCOUNT_SCOPE_JSON value admits every runner account (same rules as TS
    shared/writer_account_scope.py: unset = disabled; {"include": [...]} or {"exclude": [...]})."""
    if raw is None:
        return None
    try:
        value = json.loads(raw)
    except ValueError:
        return "WRITER_SCOPE_INVALID"
    if (not isinstance(value, dict) or set(value) not in ({"include"}, {"exclude"})
            or not all(isinstance(v, list) for v in value.values())):
        return "WRITER_SCOPE_INVALID"
    if "include" in value:
        denied = sorted(set(accounts) - set(value["include"]))
    else:
        denied = sorted(set(accounts) & set(value["exclude"]))
    return ("WRITER_SCOPE_DENIES_RUNNER_ACCOUNTS: " + ", ".join(denied)) if denied else None


def alpha_services(d):
    """{venue: alpha compose service}; the probe checks the alpha identity per venue, so one service cannot serve
    both venues."""
    services = d.get("alpha_service")
    if not isinstance(services, dict) or set(services) != set(VENUES) or not all(
            isinstance(v, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", v) for v in services.values()):
        raise Blocked('DR01_ALPHA_SERVICE_PER_VENUE: dr01.alpha_service = {"BINANCE": svc, "OKX": svc}')
    if services["BINANCE"] == services["OKX"]:
        raise Blocked("DR01_ALPHA_SERVICE_PER_VENUE: one service cannot carry both venue identities")
    return services


def host_path(value, what):
    """An absolute host path that is safe inside a docker -v spec."""
    if not isinstance(value, str) or not value.startswith("/") or any(c in value for c in (":", ",", "\n")):
        raise Blocked(f"PATH_INVALID: {what} must be an absolute path without ':' or ','")
    return os.path.normpath(value)


def dr07_mounts(connections):
    """(read-only dirs, read-write dirs) a DR07 container needs for a loaded --connections file."""
    if not isinstance(connections, dict) or not set(VENUES) <= set(connections):
        raise Blocked("DR07_CONNECTIONS_INVALID: one entry per venue BINANCE, OKX")
    ro, rw = set(), set()
    for venue in VENUES:
        conf = connections[venue]
        if not isinstance(conf, dict):
            raise Blocked("DR07_CONNECTIONS_INVALID: " + venue)
        for key in DR07_READ_FILES:
            if conf.get(key):
                ro.add(os.path.dirname(host_path(conf[key], f"dr07 {venue}.{key}")))
        rw.add(os.path.dirname(host_path(conf.get("cursor_file"), f"dr07 {venue}.cursor_file")))
    ro -= rw
    return sorted(ro), sorted(rw)


def container_argv(image, name, *, user, networks=("none",), ro=(), rw=(), env=(), env_files=(), workdir,
                   cap_add=(), memory="1g"):
    """docker run --rm of an existing image (never pulled or built) with exactly the declared mounts, at their host
    paths, so every path a harness records is the host path."""
    argv = ["docker", "run", "--rm", "--pull=never", "--name", name, "--label", "p184.g13.step=" + name,
            "--user", user, "--read-only", "--tmpfs", "/tmp:rw,size=64m,mode=1777", "--cpus", "1",
            "--memory", memory, "--pids-limit", "256", "--log-opt", "max-size=4m", "--log-opt", "max-file=1",
            "--security-opt", "no-new-privileges", "--cap-drop", "ALL"]
    for cap in cap_add:
        argv += ["--cap-add", cap]
    for net in networks:
        argv += ["--network", net]
    for path in ro:
        argv += ["-v", f"{path}:{path}:ro"]
    for path in rw:
        argv += ["-v", f"{path}:{path}"]
    for path in env_files:
        argv += ["--env-file", path]
    for item in ("HOME=/tmp", "PYTHONDONTWRITEBYTECODE=1", *env):
        argv += ["-e", item]
    return argv + ["-w", str(workdir), "--entrypoint", "python", image, "-B"]


def cmd(argv, *, cwd, stdin=None, stdout=None):
    return {"argv": [str(a) for a in argv], "cwd": str(cwd), "stdin": stdin and str(stdin),
            "stdout": stdout and str(stdout)}


def commands(step, final, inputs, work, ts_root):
    """The exact harness invocations of one step (dr05-upstream's channel/watcher are built at run time)."""
    py = [sys.executable, "-B"]
    ts_ev = ["--source-sha", final["ts_sha"], "--image-digest", final["writer"]]
    alpha_record = ts_root / "scripts/p184_dr_alpha_reads.py"
    if step == "dr05-upstream":
        w = work / "work"
        scopes = [a for scope in DR05_SCOPES for a in ("--scope", scope)]
        return [cmd(py + ["scripts/p184_dr05_dl_fault.py", "--init", w, "--core-config", inputs["dr05_public_export"],
                          *harness_image_args(final), *scopes, "--catalog", inputs["dr05_catalog"]], cwd=DL_ROOT),
                cmd(py + ["scripts/p184_dr05_dl_fault.py", "--up", w], cwd=DL_ROOT),
                cmd(py + ["scripts/p184_dr05_dl_fault.py", "--ready", w], cwd=DL_ROOT),
                cmd(py + ["scripts/p184_dr05_upstream.py", "--work", w, "--namespace", "<from handoff.json>",
                          "--dl-channel", "<built from handoff namespace>", "--image", final["writer"],
                          "--redis-image", inputs["redis_image"], "--postgres-image", inputs["postgres_image"],
                          *ts_ev, "--output", work / "dr05-upstream-rows.jsonl", "--dl-root", DL_ROOT,
                          "--venues", *DR05_SCOPES], cwd=ts_root),
                cmd(py + ["scripts/p184_dr05_dl_fault.py", "--down", w], cwd=DL_ROOT)]
    if step == "dr08-isolated":
        return [cmd(py + ["scripts/p184_dr08_isolated.py", "--orchestrate", work / "work", "--ts-source-sha",
                          final["ts_sha"], "--ts-image-digest", final["writer"], "--restart-samples",
                          str(inputs.get("dr08_restart_samples", 5)), *harness_image_args(final),
                          "--query-image-ref", final["python_ref"]], cwd=DL_ROOT)]
    if step == "dr01-deployed":
        d = inputs["dr01"]
        services = alpha_services(d)
        # prepare imports qdl (redis, pydantic, yaml): offline, in the final python image, as the host user.
        inputs_ro = [host_path(d["inventory"], "dr01.inventory"), host_path(d["manifest_dir"], "dr01.manifest_dir")]
        if d.get("catalog"):
            inputs_ro.append(host_path(d["catalog"], "dr01.catalog"))
        out = []
        for venue in VENUES:
            packet = work / f"dr01-{venue.lower()}-packet.json"
            prepare = ["scripts/p184_dr01_deployed.py", "prepare", "--root", DL_ROOT, "--inventory", d["inventory"],
                       "--deployment", d["deployments"][venue], "--venue", venue, "--output", packet,
                       "--manifest-dir", d["manifest_dir"], "--manifest-revision", str(d["manifest_revision"])]
            if d.get("catalog"):
                prepare += ["--catalog", d["catalog"]]
            if d.get("wrong_revision") is not None:
                prepare += ["--wrong-revision", str(d["wrong_revision"])]
            box = container_argv(final["python"], f"p184-g13-dr01-prepare-{venue.lower()}",
                                 user=f"{os.getuid()}:{os.getgid()}", ro=[str(DL_ROOT), *inputs_ro], rw=[str(work)],
                                 env=[f"PYTHONPATH={DL_ROOT}:{DL_ROOT}/generated/python"], workdir=DL_ROOT)
            out.append(cmd(box + prepare, cwd=DL_ROOT))
            out.append(cmd(["docker", "compose", "-f", d["compose"], "run", "--rm", "--no-deps", "-T", "--entrypoint",
                            "python", "-v", f"{packet}:/run/p184/dr01-packet.json:ro", services[venue], "-B", "-",
                            "probe", "--packet", "/run/p184/dr01-packet.json"],
                           cwd=d.get("compose_cwd", "/home/bobby/execution_alpha"),
                           stdin=DL_ROOT / "scripts/p184_dr01_deployed.py",
                           stdout=work / f"dr01-{venue.lower()}-rows.jsonl"))
        record = ["scripts/p184_dr01_deployed.py", "record", "--rows", work / "dr01-binance-rows.jsonl",
                  work / "dr01-okx-rows.jsonl", "--source-sha", final["ts_sha"], "--image-digest", final["writer"],
                  "--output", work / "dr01-dl.jsonl"]
        if d.get("ci_confirmed") is True:
            record.append("--ci-confirmed")
        out.append(cmd(py + record, cwd=DL_ROOT))
        return out
    if step == "dr01-wrong-account":
        script = ts_root / "scripts/p184_dr01_wrong_account.py"
        run_id = inputs["dr01_wrong_account"]["run_id"]
        wrong_account_identities(run_id)
        return [cmd(["docker", "exec", "-i", inputs["gateway_container"], "python", "-B", "-", "probe", "--run-id",
                     run_id], cwd=ts_root, stdin=script, stdout=work / "dr01-wrong-account.rows"),
                cmd(py + [script, "record", "--rows", work / "dr01-wrong-account.rows", *ts_ev,
                          "--gateway-image-digest", inputs["gateway_image_digest"], "--output",
                          work / "dr01-wrong-account.jsonl"], cwd=ts_root)]
    if step == "dr02-dr03-alpha":
        a = inputs["alpha"]
        out = []
        for venue in VENUES:
            probe = ["docker", "exec", "-i", a["containers"][venue], "python", "-B", "-", "probe", "--venue",
                     venue.lower(), "--symbol", a["symbols"][venue]]
            if a.get("universe", {}).get(venue):
                probe += ["--universe", a["universe"][venue]]
            out.append(cmd(probe, cwd=ts_root, stdin=alpha_record, stdout=work / f"alpha-{venue.lower()}.rows"))
        out.append(cmd(py + [alpha_record, "record", "--rows", work / "alpha.rows", *ts_ev, "--output",
                             work / "dr02-dr03.jsonl"], cwd=ts_root))
        return out
    if step == "dr04-dr06-views":
        out = [cmd(["docker", "exec", "-i", inputs["ts_container"], "python", "-B", "-", "--venue", venue.lower()],
                   cwd=ts_root, stdin=ts_root / "scripts/p184_dr_views.py", stdout=work / f"views-{venue.lower()}.rows")
               for venue in VIEW_SCOPES]
        out.append(cmd(py + [alpha_record, "record", "--rows", work / "views.rows", *ts_ev, "--output",
                             work / "dr04-dr06.jsonl"], cwd=ts_root))
        return out
    if step == "dr05-transport":
        return [cmd(py + ["scripts/p184_dr05_isolated.py", "--image", final["writer"], "--redis-image",
                          inputs["redis_image"], "--postgres-image", inputs["postgres_image"], *ts_ev,
                          "--output", work / "dr05-transport.jsonl", "--venues", *DR05_SCOPES], cwd=ts_root)]
    if step == "dr07-reference":
        d = inputs["dr07"]
        if "_connections" not in d:
            raise Blocked("DR07_CONNECTIONS_UNREADABLE: " + str(d["connections"]))
        ro, rw = dr07_mounts(d["_connections"])
        networks = d.get("networks") or DR07_NETWORKS
        if not isinstance(networks, list | tuple) or not all(
                isinstance(n, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", n) for n in networks):
            raise Blocked("DR07_NETWORKS_INVALID")
        image = d.get("image") or final["writer"]
        if not DIGEST.fullmatch(image):
            raise Blocked("DR07_IMAGE_DIGEST_REQUIRED: dr07.image must be a sha256 image ID")
        box = container_argv(image, "p184-g13-dr07-reference", user="0", networks=networks,
                             ro=sorted({str(ts_root), os.path.dirname(host_path(d["connections"], "dr07.connections")),
                                        *ro} - {str(work), *rw}),
                             rw=[str(work), *rw], env_files=[host_path(d["env_file"], "dr07.env_file")],
                             workdir=ts_root, cap_add=("DAC_OVERRIDE",))
        return [cmd(box + ["scripts/p184_dr07_reference.py", "--connections", d["connections"], *ts_ev, "--output",
                           work / "dr07.jsonl", "--fx-account", d["fx_account"], "--dsn-env",
                           d.get("dsn_env", "POSTGRES_DSN")], cwd=ts_root)]
    raise Blocked("UNKNOWN_STEP: " + step)


# Rows printed by the two in-container probes are concatenated per step before `record` (it takes one file).
VIEW_SCOPES = ("BINANCE", "OKX", "OKX_INVERSE")  # p184_dr_views.py --venue binance|okx|okx_inverse
CONCAT = {"dr02-dr03-alpha": ("alpha.rows", ("alpha-binance.rows", "alpha-okx.rows")),
          "dr04-dr06-views": ("views.rows", ("views-binance.rows", "views-okx.rows", "views-okx_inverse.rows"))}


def dr05_channel_argv(helper_command, python_image, ns, work):
    """The JSONL channel inside the DR05 broker namespace (same construction as the 2026-10-09 joint runs)."""
    argv = helper_command(ns, work, ["--channel", "/work"], network="container:" + ns + "-kafka")
    argv.insert(2, "-i")
    argv[argv.index("scripts/p184_dr05_dl_fault.py")] = "scripts/p184_dr05_channel.py"
    pos = argv.index(python_image)
    argv[pos:pos] = ["-e", "P184_DR05_ISOLATED_NAMESPACE=" + ns]
    return argv


def new_owned(before, after, prefixes):
    """Containers the step created and left behind: owned prefix, absent before the step. {id: name}."""
    return {cid: name for cid, name in after.items()
            if cid not in before and any(name.startswith(p) for p in prefixes)}


def receipt_root(base, now):
    return Path(base) / ("p184-g13-" + now.strftime("%Y%m%dT%H%M%SZ"))


def plan(final, inputs, phase, only, ts_root, base, now, matrix=None):
    """matrix: the loaded TS case matrix (v2); when given, its DR cells must equal DR_CELLS for any step to run."""
    steps = select(phase, only)
    spec = {s[0]: s for s in STEP_SPEC}
    root = receipt_root(base, now)
    problems = {}
    drift = matrix_mismatch(matrix) if matrix is not None else {}
    missing = missing_inputs(steps, inputs)
    out = []
    for step in steps:
        _, ph, step_cells, timeout, est, prefixes = spec[step]
        blocked = []
        if drift:
            blocked.append("CASE_MATRIX_MISMATCH: " + json.dumps(drift, sort_keys=True))
        if step in missing:
            blocked.append("MISSING_INPUTS: " + ", ".join(missing[step]))
        if ph == "runtime" and not inputs.get("_runtime_approval"):
            blocked.append("RUNTIME_APPROVAL_REQUIRED: --runtime-approval <owner-approved packet>")
        entry = {"step": step, "phase": ph, "cells": list(step_cells), "timeout_s": timeout, "est_minutes": est,
                 "owned_prefixes": list(prefixes), "blocked": blocked, "gateway_dependent": step in GATEWAY_STEPS}
        if not blocked:
            try:
                entry["commands"] = commands(step, final, inputs, root / step, ts_root)
            except Blocked as exc:
                blocked.append(str(exc))
        if step == "dr01-wrong-account" and not blocked:
            # The Gateway compose must list exactly these in WRITER_ACCOUNT_SCOPE_JSON (checked before the probe).
            entry["writer_scope_accounts"] = writer_scope_accounts(inputs["dr01_wrong_account"]["run_id"])
        out.append(entry)
        if blocked:
            problems[step] = blocked
    return {"schema": "p184.g13-final-plan.v1", "receipts": str(root), "final": final,
            "case_matrix": {"schema": (matrix or {}).get("schema"), "sha256": (matrix or {}).get("sha256")},
            "decisions": {k: {"decision": v[0], "reason": v[1]} for k, v in DECISIONS.items()},
            "coverage": coverage(), "steps": out, "blocked": problems,
            "est_minutes_total": sum(e["est_minutes"] for e in out),
            "timeout_minutes_total": sum(e["timeout_s"] for e in out) // 60}


# ---------------------------------------------------------------- side effects (only without --dry-run)

def sh(argv, timeout=60):
    return subprocess.run(argv, check=True, text=True, capture_output=True, timeout=timeout).stdout


def containers():
    rows = sh(["docker", "ps", "-a", "--no-trunc", "--format", "{{.ID}} {{.Names}}"]).split("\n")
    return dict(r.split(" ", 1) for r in rows if " " in r)


def deployed_inventory():
    names = [STACK + r + "-1" for r in ROLE_IMAGE] + [WRITER]
    out = {}
    for name in names:
        try:
            out[name] = sh(["docker", "inspect", "--format", "{{.Image}}", name]).strip()
        except subprocess.CalledProcessError:
            out[name] = None
    return out


def gateway_problem(state, expected_image):
    """None when the Gateway container runs the declared image; else the GATEWAY_NOT_RUNNING reason.
    state: {"running": bool, "image": id} or None when the container does not exist."""
    if not state:
        return "GATEWAY_NOT_RUNNING: container absent"
    if not state.get("running"):
        return "GATEWAY_NOT_RUNNING: container not running"
    if state.get("image") != expected_image:
        return f"GATEWAY_NOT_RUNNING: runs {state.get('image')} != declared gateway image {expected_image}"
    return None


def gateway_writer_scope(name):
    """The Gateway's WRITER_ACCOUNT_SCOPE_JSON value only (no other env entry leaves docker), or None when unset."""
    line = sh(["docker", "inspect", "--format", '{{range .Config.Env}}{{if eq (index (split . "=") 0) '
               '"WRITER_ACCOUNT_SCOPE_JSON"}}{{println .}}{{end}}{{end}}', name]).strip()
    return line.split("=", 1)[1] if line else None


def env_file_problem(path):
    """None when the DSN env file is a regular file readable by its owner only."""
    try:
        st = os.stat(path)
    except OSError as exc:
        return f"DR07_ENV_FILE_UNREADABLE: {type(exc).__name__}"
    if not stat.S_ISREG(st.st_mode) or st.st_mode & 0o077:
        return "DR07_ENV_FILE_MODE: regular file, mode 0600 required"
    return None


def gateway_state(name):
    try:
        row = json.loads(sh(["docker", "inspect", name]))[0]
    except (subprocess.CalledProcessError, json.JSONDecodeError, IndexError):
        return None
    return {"running": bool(row["State"]["Running"]), "image": row["Image"]}


def checkout_problems(dl_sha):
    head = sh(["git", "-C", str(DL_ROOT), "rev-parse", "HEAD"]).strip()
    dirty = sh(["git", "-C", str(DL_ROOT), "status", "--porcelain", "--untracked-files=no", "--", "qdl", "rust",
                "scripts", "generated", "consumers", "config", "contracts"]).strip()
    found = []
    if head != dl_sha:
        found.append(f"DL checkout HEAD {head} != --dl-source-sha {dl_sha} (harness containers mount it at /app)")
    if dirty:
        found.append("DL checkout has tracked edits under mounted paths:\n" + dirty)
    return found


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def bounded(c, deadline, log):
    """One harness command in its own process group; killed (TERM, then KILL) at the step deadline."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("step deadline")
    stdin = open(c["stdin"], "rb") if c["stdin"] else subprocess.DEVNULL
    stdout = open(c["stdout"], "xb") if c["stdout"] else log
    try:
        proc = subprocess.Popen(c["argv"], cwd=c["cwd"], stdin=stdin, stdout=stdout, stderr=log,
                                start_new_session=True)
        try:
            return proc.wait(remaining)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(15)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait(15)
            raise TimeoutError("step deadline: " + shlex.join(c["argv"][:4]))
    finally:
        if c["stdin"]:
            stdin.close()
        if c["stdout"]:
            stdout.close()


def run_dr05_upstream(entry, final, inputs, work, deadline, log, ts_root):
    """init -> up -> ready -> reset watcher -> TS adapter over the channel; --down always runs."""
    sys.path.insert(0, str(DL_ROOT))
    from scripts import p184_dr05_dl_fault as dr  # host-side helpers only (stdlib imports)
    init, up, ready, adapter, down = entry["commands"]
    watcher, codes = None, []
    try:
        for c in (init, up, ready):
            codes.append(bounded(c, deadline, log))
            if codes[-1]:
                raise RuntimeError("DR05 DL stack step failed: " + c["argv"][3])
        ns = json.loads((work / "work/handoff.json").read_text())["namespace"]
        watcher = subprocess.Popen([sys.executable, "-B", "scripts/p184_dr05_reset.py", "--serve-reset-host",
                                    str(work / "work"), "--duration", str(int(max(60, deadline - time.monotonic())))],
                                   cwd=DL_ROOT, stdout=log, stderr=log, start_new_session=True)
        channel = shlex.join(dr05_channel_argv(dr.helper_command, dr.image_set(work / "work")["python"], ns,
                                               work / "work"))
        argv = [ns if a == "<from handoff.json>" else channel if a == "<built from handoff namespace>" else a
                for a in adapter["argv"]]
        codes.append(bounded({**adapter, "argv": argv}, deadline, log))
    finally:
        if watcher is not None and watcher.poll() is None:
            os.killpg(watcher.pid, signal.SIGTERM)
            try:
                watcher.wait(30)
            except subprocess.TimeoutExpired:
                os.killpg(watcher.pid, signal.SIGKILL)
        if (work / "work/handoff.json").exists():
            codes.append(bounded(down, time.monotonic() + 600, log))
    return codes


def run_step(entry, final, inputs, ts_root):
    work = Path(inputs["_root"]) / entry["step"]
    work.mkdir(mode=0o700)
    before = containers()
    clash = {cid: n for cid, n in before.items() if any(n.startswith(p) for p in entry["owned_prefixes"])}
    receipt = {"step": entry["step"], "cells": entry["cells"], "commands": entry["commands"],
               "started_utc": datetime.now(timezone.utc).isoformat(), "preexisting_owned": clash}
    t0 = time.monotonic()
    deadline = t0 + entry["timeout_s"]
    codes, error = [], None
    with open(work / "harness.log", "ab") as log:
        try:
            if clash:
                raise Blocked("OWNED_PREFIX_IN_USE: " + ", ".join(sorted(clash.values())))
            if entry.get("gateway_dependent"):
                problem = gateway_problem(gateway_state(inputs["gateway_container"]), inputs["gateway_image_digest"])
                if problem:
                    receipt["deferred"] = problem  # closes with the TS release; the other steps still run
                    raise Blocked(problem)
                problem = writer_scope_problem(gateway_writer_scope(inputs["gateway_container"]),
                                               entry["writer_scope_accounts"])
                if problem:
                    raise Blocked(problem)
            if entry["step"] == "dr07-reference":
                problem = env_file_problem(inputs["dr07"]["env_file"])
                if problem:
                    raise Blocked(problem)
            if entry["step"] == "dr05-upstream":
                codes = run_dr05_upstream(entry, final, inputs, work, deadline, log, ts_root)
            else:
                for c in entry["commands"]:
                    if c["argv"][2:3] == [str(ts_root / "scripts/p184_dr_alpha_reads.py")] and entry["step"] in CONCAT:
                        target, parts = CONCAT[entry["step"]]
                        with open(work / target, "xb") as joined:
                            for part in parts:
                                joined.write((work / part).read_bytes())
                    codes.append(bounded(c, deadline, log))
                    if codes[-1]:
                        break
        except (Blocked, TimeoutError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
            error = f"{type(exc).__name__}: {exc}"
    removed = {}
    for cid, name in new_owned(before, containers(), entry["owned_prefixes"]).items():
        subprocess.run(["docker", "rm", "-f", cid], capture_output=True, text=True, timeout=60)
        removed[cid] = name
    outputs = {p.name: sha256(p) for p in sorted(work.iterdir()) if p.is_file() and p.name != "receipt.json"}
    receipt.update(ended_utc=datetime.now(timezone.utc).isoformat(), duration_s=round(time.monotonic() - t0, 1),
                   exit_codes=codes, error=error, outputs_sha256=outputs, removed_leftovers=removed,
                   harness_ok=error is None and bool(codes) and not any(codes))
    (work / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


def load_dr07_connections(inputs):
    """The DR07 container mounts derive from the connections file (paths only, no secret bytes); unreadable -> the
    step is blocked with DR07_CONNECTIONS_UNREADABLE."""
    d = inputs.get("dr07")
    if isinstance(d, dict) and d.get("connections"):
        try:
            d["_connections"] = json.loads(Path(d["connections"]).read_text())
        except (OSError, ValueError):
            d.pop("_connections", None)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--python-image", required=True)
    p.add_argument("--rust-image", required=True)
    p.add_argument("--python-image-ref", required=True)
    p.add_argument("--writer-image", required=True)
    p.add_argument("--ts-source-sha", required=True)
    p.add_argument("--dl-source-sha", required=True)
    p.add_argument("--phase", choices=("isolated", "runtime", "all"), default="all")
    p.add_argument("--only", nargs="+", default=())
    p.add_argument("--inputs", type=Path)
    p.add_argument("--runtime-approval", type=Path)
    p.add_argument("--ts-root", type=Path, default=TS_ROOT_DEFAULT)
    p.add_argument("--receipts", type=Path, default=STATE_ROOT)
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args(argv)
    final = {"python": a.python_image, "rust": a.rust_image, "python_ref": a.python_image_ref,
             "writer": a.writer_image, "ts_sha": a.ts_source_sha, "dl_sha": a.dl_source_sha}
    inputs = json.loads(a.inputs.read_text()) if a.inputs else {}
    if a.runtime_approval:
        inputs["_runtime_approval"] = {"path": str(a.runtime_approval), "sha256": sha256(a.runtime_approval)}
    load_dr07_connections(inputs)
    try:
        validate_final(final)
        matrix_path = a.ts_root / MATRIX_V2
        if not matrix_path.is_file():
            raise Blocked(f"CASE_MATRIX_MISSING: {matrix_path}")
        matrix = json.loads(matrix_path.read_text())
        result = plan(final, inputs, a.phase, tuple(a.only), a.ts_root, a.receipts, datetime.now(timezone.utc), matrix)
    except Blocked as exc:
        print(json.dumps({"blocked": str(exc)}))
        return 2
    if a.dry_run:
        print(json.dumps(result, indent=2, default=str))
        return 0 if not result["blocked"] else 3
    extra = checkout_problems(final["dl_sha"])
    if any(e["phase"] == "runtime" for e in result["steps"]) or "dr08-isolated" in [e["step"] for e in result["steps"]]:
        # DR08 copies the deployed core config and refuses a core image other than its pin.
        extra += deployed_mismatches(deployed_inventory(), final)
    if result["blocked"] or extra:
        print(json.dumps({"blocked": result["blocked"], "preflight": extra}, indent=2))
        return 3
    root = Path(result["receipts"])
    root.mkdir(mode=0o700, parents=False, exist_ok=False)
    inputs["_root"] = str(root)
    (root / "plan.json").write_text(json.dumps(result, indent=2, default=str) + "\n")
    receipts = []
    for entry in result["steps"]:
        receipts.append(run_step(entry, final, inputs, a.ts_root))
        print(json.dumps({k: receipts[-1][k] for k in ("step", "duration_s", "exit_codes", "error", "harness_ok")}),
              flush=True)
    summary = {"schema": "p184.g13-final-summary.v1", "final": final, "steps": [
        {k: r[k] for k in ("step", "cells", "duration_s", "exit_codes", "error", "harness_ok", "removed_leftovers")}
        for r in receipts], "deferred": {r["step"]: r["deferred"] for r in receipts if r.get("deferred")},
        "note": "harness_ok is the harness exit status; cells close only via the TS matrix check; deferred steps "
                "(Gateway not on the declared image) stay open until the TS release"}
    (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return 0 if all(r["harness_ok"] or r.get("deferred") for r in receipts) else 1


if __name__ == "__main__":
    raise SystemExit(main())
