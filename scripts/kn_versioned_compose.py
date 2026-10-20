#!/usr/bin/env python3
"""Versioned compose for the production KN topology: inventory, render, secrets env, parity.

Purpose
    Since KN (2026-09-27) the running Data Layer V2 roles were created from per-role compose snapshots under
    ~/.local/state/qdl-v2 (scripts/kn_production_packet.py service_from_inspect) and later changed by
    ``docker update`` (OPT caps), so no file in the repository defined what runs (reconciliation drift R1/R4).
    This tool renders ONE committed compose, ``deploy/compose/qdl-v2-stable-kn.yml``, from the live
    ``docker inspect`` of the 17 roles of project ``qdl_v2_stable_candidate`` and proves it equal to them:

    * images by digest, CPU/memory/swap caps, env, command/entrypoint, user, mounts, networks/aliases, ports,
      tmpfs, init/pids, logging, stop grace and healthchecks exactly as live;
    * the variable parts are compose variables: images (``deploy/compose/qdl-v2-stable-kn.v<release>.env``,
      committed, no secret), the host paths of the release/realm/config/state directories (same file), the mesh
      TLS generation (``generation.env`` written by scripts/rotate_mesh_tls.py) and the three env-only secrets
      (a host-only 0600 env file written by ``secrets-env`` from the live values, never regenerated: CLAUDE.md
      section 4.2);
    * intended deltas against live, listed in INTENDED_DELTAS and checked by ``parity``: Docker healthchecks for the
      seven roles that had none (liveness, not readiness: IDR-108 is a Stream code change), log rotation for
      stable_redis, the stable_tls volume replaced by a byte-equal host directory of the TLS generation, nested
      binds for /kn/tls and the two client CA bundles (same bytes for the deployed generation), and the
      session-liveness bind expressed as a volume subpath of the same volume.

Modes
    inventory  --out FILE                read-only docker inspect of the 17 roles; secret env values replaced by
                                         ``<sha256:12hex>`` (never printed or written in clear).
    render     --inventory FILE --out F  write the compose file (stdout summary only).
    secrets-env --out FILE               write the 0600 env file of the env-only secrets from the live containers;
                                         refuses when the roles disagree on a value.
    parity     --compose F --env-file E [--env-file E ...]
                                         ``docker compose config`` with the given env files vs live inspect, per
                                         role and field; only INTENDED_DELTAS may differ. Exit 0 PASS, 2 FAIL.

Boundary: reads docker inspect and runs ``docker compose config`` only; never creates, starts, stops or
recreates a container, network or volume and never touches Kafka/Redis/Postgres. The window applies the compose
per role with ``up -d --no-deps <service>`` (never a project-wide ``up``, never ``--remove-orphans``: the exited
one-shot init containers belong to the same project).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import yaml

PROJECT = "qdl_v2_stable_candidate"
ROLES = ("kafka1", "kafka2", "kafka3", "rust_core", "rust_core_2", "rust_core_3", "ingestor_binance_usdm",
         "ingestor_okx_swap", "market_projector_1", "market_projector_2", "market_cache", "stable_redis",
         "query_kn_1", "query_kn_2", "stream_kn_1", "stream_kn_2", "binance_bar_edge")
# Env-only secrets (no *_FILE variant in code: provider_admission_server.rs:215, qdl/runtime/stable.py:353,397).
SECRET_ENV = ("QDL_PROVIDER_ADMISSION_SECRET", "QDL_STABLE_INTERNAL_INGEST_SECRET", "QDL_STABLE_CURSOR_KEYS_JSON")
SECRET_PATTERN = re.compile(r"(SECRET(?!_FILE)|PASSWORD|PASSWD|TOKEN|CURSOR_KEYS_JSON|PRIVATE)")
IMAGE_VARS = {  # role -> image variable (one variable per independently rolled image line)
    "kafka1": "QDL_IMAGE_KAFKA", "kafka2": "QDL_IMAGE_KAFKA", "kafka3": "QDL_IMAGE_KAFKA",
    "rust_core": "QDL_IMAGE_CORE", "rust_core_2": "QDL_IMAGE_CORE", "rust_core_3": "QDL_IMAGE_CORE",
    "ingestor_binance_usdm": "QDL_IMAGE_INGESTOR", "ingestor_okx_swap": "QDL_IMAGE_INGESTOR",
    "market_projector_1": "QDL_IMAGE_PROJECTOR", "market_projector_2": "QDL_IMAGE_PROJECTOR",
    "market_cache": "QDL_IMAGE_REDIS", "stable_redis": "QDL_IMAGE_REDIS",
    "query_kn_1": "QDL_IMAGE_QUERY", "query_kn_2": "QDL_IMAGE_QUERY",
    "stream_kn_1": "QDL_IMAGE_STREAM", "stream_kn_2": "QDL_IMAGE_STREAM",
    "binance_bar_edge": "QDL_IMAGE_BAR_EDGE",
}
STATE = "/home/bobby/.local/state/qdl-v2"
# Host path prefixes -> variables (longest first). Values live in the committed release env file.
PATH_VARS = (
    (f"{STATE}/releases/v2.2.0-02cd827/state", "QDL_KN_STATE_DIR"),
    (f"{STATE}/releases/v2.2.0-02cd827", "QDL_KN_RELEASE_DIR"),
    (f"{STATE}/p184-d3-capacity-20261009/reader-public-r16", "QDL_CONSUMER_REALM_DIR"),
    (f"{STATE}/releases/v2.2.3-rc18-core-batch1024", "QDL_CORE_CONFIG_DIR"),
    (f"{STATE}/releases/v2.2.3-rc7-ingestor-liveness", "QDL_INGESTOR_CONFIG_DIR"),
    (f"{STATE}/655d2106d01f/cert-material-rotate-20260822T144323Z", "QDL_TLS_KAFKA_DIR"),
)
SESSION_LIVENESS = "/var/lib/docker/volumes/qdl_v2_stable_candidate_stable_state/_data/runtime/session-liveness"
NETWORK_KEYS = {"qdl_v2_stable_candidate_stable_internal": "stable_internal",
                "qdl_v2_stable_candidate_stable_egress": "stable_egress",
                "qdl_v2_native_consumer": "kn_consumer", "executor_network": "executor_network"}
VOLUME_KEYS = {"qdl_v2_stable_candidate_kafka1_data": "kafka1_data", "qdl_v2_stable_candidate_kafka2_data": "kafka2_data",
               "qdl_v2_stable_candidate_kafka3_data": "kafka3_data", "qdl_v2_stable_candidate_stable_state": "stable_state"}
# New Docker healthchecks (liveness only). Sources: stream listener binds after the Kafka warm-open
# (rust/qdl-stream-gateway/src/main.rs:248,256,399); projector status file atomic rename every
# QDL_KN_STATUS_INTERVAL_S (rust/qdl-projector/src/main.rs:268-280,617-622); core admission listener
# (rust/qdl-realtime-core/src/provider_admission_server.rs:248). The rust image has bash but no curl/wget/nc.
NEW_HEALTHCHECKS = {
    "stream_kn": {"test": ["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8210"], "interval": "10s",
                  "timeout": "3s", "retries": 3, "start_period": "120s", "start_interval": "2s"},
    "market_projector": {"test": ["CMD-SHELL", "find /status/status.json -newermt '-30 seconds' 2>/dev/null | grep -q ."],
                         "interval": "15s", "timeout": "5s", "retries": 3, "start_period": "60s", "start_interval": "5s"},
    "rust_core": {"test": ["CMD", "bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/8300"], "interval": "15s",
                  "timeout": "3s", "retries": 3, "start_period": "30s", "start_interval": "2s"},
}
# Owner 2026-10-19 (C1): the live probes of stable_redis (2 s), Query (5 s, python + TLS) and Kafka (5 s) run too
# often for their value. Same test command, sane cadence; the total unhealthy budget stays about the same
# (interval x retries) and start_interval keeps start-up detection fast. Applies at each role's next recreate.
HEALTHCHECK_RETUNE = {
    "kafka": {"interval": "15s", "timeout": "5s", "retries": 12, "start_period": "20s", "start_interval": "2s"},
    "query_kn": {"interval": "15s", "timeout": "5s", "retries": 7, "start_period": "60s", "start_interval": "5s"},
    "stable_redis": {"interval": "15s", "timeout": "3s", "retries": 4, "start_period": "10s", "start_interval": "1s"},
}
STABLE_REDIS_LOGGING = {"driver": "json-file", "options": {"max-size": "20m", "max-file": "3"}}
INTENDED_DELTAS = (
    "healthcheck added: stream_kn_1/2, market_projector_1/2, rust_core/_2/_3 (liveness)",
    "logging: stable_redis gains json-file 20m x 3 rotation",
    "healthcheck cadence: kafka1-3 5s->15s, query_kn_1/2 5s->15s, stable_redis 2s->15s (same test; HEALTHCHECK_RETUNE)",
    "/stable-certs: volume qdl_v2_stable_candidate_stable_tls -> host dir of the TLS generation (byte-equal for the deployed generation)",
    "/kn/tls and /consumer-realms/{query,stream}-client-ca-bundle.crt: explicit nested binds from the TLS generation",
    "/session-liveness: bind of the stable_state volume path -> volume subpath runtime/session-liveness (same directory)",
    "image: by variable per release env file (equal to live for the v2.2.4 env)",
)


def sh(argv, **kw):
    return subprocess.run(argv, capture_output=True, text=True, check=True, **kw).stdout


def digest12(value: str) -> str:
    return "<sha256:" + hashlib.sha256(value.encode()).hexdigest()[:12] + ">"


def is_secret(key: str) -> bool:
    return bool(SECRET_PATTERN.search(key)) and not key.endswith(("_FILE", "_CREDENTIALS"))


# ------------------------------------------------------------------------------------------------ inventory
def inspect_role(role: str) -> dict:
    it = json.loads(sh(["docker", "inspect", f"{PROJECT}-{role}-1"]))[0]
    img = json.loads(sh(["docker", "image", "inspect", it["Image"]]))[0]
    image_env = dict(e.split("=", 1) for e in img["Config"].get("Env") or [])
    env = dict(e.split("=", 1) for e in it["Config"].get("Env") or [])
    hc = it["HostConfig"]
    return {
        "role": role, "image_id": it["Image"], "repo_digests": img.get("RepoDigests") or [],
        "repo_tags": img.get("RepoTags") or [],
        "revision": (img["Config"].get("Labels") or {}).get("org.opencontainers.image.revision"),
        "env": {k: (digest12(v) if is_secret(k) else v) for k, v in env.items()},
        "image_env": {k: v for k, v in image_env.items()},
        "entrypoint": it["Config"].get("Entrypoint"), "cmd": it["Config"].get("Cmd"),
        "image_entrypoint": img["Config"].get("Entrypoint"), "image_cmd": img["Config"].get("Cmd"),
        "user": it["Config"].get("User"), "image_user": img["Config"].get("User"),
        "working_dir": it["Config"].get("WorkingDir"), "image_working_dir": img["Config"].get("WorkingDir"),
        "healthcheck": it["Config"].get("Healthcheck"), "stop_timeout": it["Config"].get("StopTimeout"),
        "host": {k: hc.get(k) for k in ("NanoCpus", "Memory", "MemorySwap", "RestartPolicy", "LogConfig", "Tmpfs",
                                        "ReadonlyRootfs", "CapDrop", "CapAdd", "SecurityOpt", "PidsLimit", "Init",
                                        "PortBindings", "ShmSize", "Ulimits", "Sysctls")},
        "mounts": sorted(({"type": m["Type"], "name": m.get("Name"), "source": m["Source"],
                           "target": m["Destination"], "rw": m["RW"]} for m in it["Mounts"]),
                         key=lambda m: m["target"]),
        "networks": {n: sorted(set(v.get("Aliases") or [])) for n, v in it["NetworkSettings"]["Networks"].items()},
    }


def inventory() -> dict:
    return {"schema": "qdl.kn-compose-inventory.v1", "project": PROJECT,
            "roles": {role: inspect_role(role) for role in ROLES}}


# ------------------------------------------------------------------------------------------------ render
def literal(value: str) -> str:
    return value.replace("$", "$$")


def var_path(source: str) -> str | None:
    for prefix, var in PATH_VARS:
        if source == prefix or source.startswith(prefix + "/"):
            return "${" + var + ":?set " + var + "}" + source[len(prefix):]
    return None


def health_kind(role: str) -> str | None:
    for kind in NEW_HEALTHCHECKS:
        if role.startswith(kind):
            return kind
    return None


def duration(ns: int) -> str:
    return f"{ns // 1_000_000_000}s" if ns % 1_000_000_000 == 0 else f"{ns // 1_000_000}ms"


def service(r: dict) -> dict:
    role = r["role"]
    svc: dict = {"image": "${" + IMAGE_VARS[role] + ":?set " + IMAGE_VARS[role] + "}", "pull_policy": "never",
                 "restart": r["host"]["RestartPolicy"]["Name"]}
    if r["entrypoint"] is not None and r["entrypoint"] != r["image_entrypoint"]:
        svc["entrypoint"] = [literal(x) for x in r["entrypoint"]]
    if r["cmd"] is not None and (r["cmd"] != r["image_cmd"] or "entrypoint" in svc):
        svc["command"] = [literal(x) for x in r["cmd"]]
    if r["user"] and r["user"] != r["image_user"]:
        svc["user"] = r["user"]
    if r["working_dir"] and r["working_dir"] != r["image_working_dir"]:
        svc["working_dir"] = r["working_dir"]
    env = {}
    for key, value in sorted(r["env"].items()):
        if key in r["image_env"] and r["image_env"][key] == value:
            continue
        if key in SECRET_ENV:
            env[key] = "${" + key + ":?set " + key + " (host-only secrets env)}"
        elif is_secret(key):
            raise SystemExit(f"{role}: unexpected secret-like env {key}; extend SECRET_ENV with a *_FILE plan")
        else:
            env[key] = literal(value)
    svc["environment"] = env
    h = r["host"]
    svc["cpus"] = h["NanoCpus"] / 1e9
    svc["mem_limit"] = h["Memory"]
    svc["memswap_limit"] = h["MemorySwap"]
    if h.get("PidsLimit"):
        svc["pids_limit"] = h["PidsLimit"]
    if h.get("Init"):
        svc["init"] = True
    svc["read_only"] = bool(h["ReadonlyRootfs"])
    if h.get("CapDrop"):
        svc["cap_drop"] = h["CapDrop"]
    if h.get("CapAdd"):
        svc["cap_add"] = h["CapAdd"]
    if h.get("SecurityOpt"):
        svc["security_opt"] = h["SecurityOpt"]
    if h.get("Tmpfs"):
        svc["tmpfs"] = [f"{k}:{v}" if v else k for k, v in sorted(h["Tmpfs"].items())]
    if r["stop_timeout"] is not None:
        svc["stop_grace_period"] = f"{r['stop_timeout']}s"
    log = h["LogConfig"]
    svc["logging"] = STABLE_REDIS_LOGGING if role == "stable_redis" and not log.get("Config") else {
        "driver": log["Type"], "options": dict(sorted((log.get("Config") or {}).items()))}
    ports = []
    for container_port, binds in sorted((h.get("PortBindings") or {}).items()):
        for b in binds or []:
            ports.append(f"{b['HostIp']}:{b['HostPort']}:{container_port.split('/')[0]}")
    if ports:
        svc["ports"] = ports
    volumes = []
    targets = {m["target"] for m in r["mounts"]}
    for m in r["mounts"]:
        if m["type"] == "volume" and m["name"] == f"{PROJECT}_stable_tls":
            volumes.append(_bind("${QDL_TLS_STABLE_CERTS_DIR:?set QDL_TLS_STABLE_CERTS_DIR}", m["target"], m["rw"]))
        elif m["type"] == "volume" and m["name"] in VOLUME_KEYS:
            volumes.append({"type": "volume", "source": VOLUME_KEYS[m["name"]], "target": m["target"],
                            "read_only": not m["rw"]})
        elif m["type"] == "volume":
            continue  # anonymous image VOLUME (kafka /mnt/shared/config, redis /data): created by the engine
        elif m["source"] == SESSION_LIVENESS:
            volumes.append({"type": "volume", "source": "stable_state", "target": m["target"],
                            "read_only": not m["rw"], "volume": {"subpath": "runtime/session-liveness"}})
        else:
            src = var_path(m["source"])
            if src is None:
                raise SystemExit(f"{role}: unmapped host path {m['source']}")
            volumes.append(_bind(src, m["target"], m["rw"]))
    env_all = r["env"]
    if "/kn" in targets and "QDL_KN_KAFKA_CERT" in env_all:
        volumes.append(_bind("${QDL_TLS_KN_DIR:?set QDL_TLS_KN_DIR}", "/kn/tls", False))
    for key, var in (("QDL_STABLE_TLS_CLIENT_CA_FILE", "QDL_TLS_QUERY_CLIENT_CA_FILE"),
                     ("QDL_KN_TLS_CLIENT_CA_FILE", "QDL_TLS_STREAM_CLIENT_CA_FILE")):
        if env_all.get(key, "").startswith("/consumer-realms/"):
            volumes.append(_bind("${" + var + ":?set " + var + "}", env_all[key], False))
    svc["volumes"] = volumes
    nets = {}
    container = f"{PROJECT}-{role}-1"
    for net, aliases in sorted(r["networks"].items()):
        keep = sorted(a for a in aliases if a not in {container, role} and not re.fullmatch(r"[0-9a-f]{12}", a))
        nets[NETWORK_KEYS[net]] = {"aliases": keep} if keep else {}
    svc["networks"] = nets
    kind = health_kind(role)
    if r["healthcheck"] and r["healthcheck"].get("Test"):
        hcfg = r["healthcheck"]
        svc["healthcheck"] = {"test": [literal(x) for x in hcfg["Test"]]}
        for key, field in (("Interval", "interval"), ("Timeout", "timeout"), ("StartPeriod", "start_period"),
                           ("StartInterval", "start_interval")):
            if hcfg.get(key):
                svc["healthcheck"][field] = duration(hcfg[key])
        if hcfg.get("Retries"):
            svc["healthcheck"]["retries"] = hcfg["Retries"]
        for prefix, timing in HEALTHCHECK_RETUNE.items():
            if role.startswith(prefix):
                svc["healthcheck"] = {"test": svc["healthcheck"]["test"], **timing}
    elif kind:
        svc["healthcheck"] = NEW_HEALTHCHECKS[kind]
    else:
        raise SystemExit(f"{role}: no healthcheck and no proposal")
    return svc


def _bind(source: str, target: str, rw: bool) -> dict:
    return {"type": "bind", "source": source, "target": target, "read_only": not rw}


HEADER = """\
# Data Layer V2 production KN topology - versioned compose (rendered by scripts/kn_versioned_compose.py).
#
# One file for the 17 runtime roles of project qdl_v2_stable_candidate, rendered from the live docker inspect of
# {observed} and proven equal to it by `scripts/kn_versioned_compose.py parity` (intended deltas: see
# INTENDED_DELTAS in that script). Variables:
#   images + host paths  deploy/compose/qdl-v2-stable-kn.v<release>.env   (committed, no secret)
#   mesh TLS generation  <tls root>/<generation>/generation.env            (scripts/rotate_mesh_tls.py)
#   env-only secrets     ~/.local/state/qdl-v2/releases/<release>/kn-secrets.env (0600, host only)
#
# Apply ONLY per role and only inside an approved window:
#   docker compose -p qdl_v2_stable_candidate -f deploy/compose/qdl-v2-stable-kn.yml \\
#     --env-file <release env> --env-file <generation.env> --env-file <secrets env> up -d --no-deps <service>
# Never a project-wide `up`, never `--remove-orphans` (the exited one-shot init containers share the project), and
# never recreate market_cache or stable_redis for a certificate change (both are ephemeral caches: CLAUDE.md 4.4).
"""


def render(inv: dict, out: Path, observed: str) -> dict:
    doc = {"name": PROJECT,
           "networks": {key: {"name": name, "external": True} for name, key in NETWORK_KEYS.items()},
           "volumes": {key: {"name": name, "external": True} for name, key in VOLUME_KEYS.items()},
           "services": {role: service(inv["roles"][role]) for role in ROLES}}
    text = HEADER.format(observed=observed) + yaml.safe_dump(doc, sort_keys=False, width=120, allow_unicode=False)
    out.write_text(text)
    return doc


def release_env(inv: dict, release: str, overrides: dict) -> str:
    lines = [f"# Data Layer {release}: images and host paths for deploy/compose/qdl-v2-stable-kn.yml (no secrets).",
             "# Image references are digests (the engine's containerd store: RepoDigest == image id)."]
    seen = {}
    for role in ROLES:
        r = inv["roles"][role]
        var = IMAGE_VARS[role]
        ref = overrides.get(var) or next((d for d in r["repo_digests"] if "@sha256:" in d), r["image_id"])
        if var in seen and seen[var] != ref and var not in overrides:
            raise SystemExit(f"{var}: roles disagree on the image ({seen[var]} vs {ref})")
        seen[var] = ref
    for var in sorted(seen):
        tags = sorted({t for role in ROLES if IMAGE_VARS[role] == var for t in inv["roles"][role]["repo_tags"]})
        lines.append(f"# {var}: {', '.join(tags) if var not in overrides else 'override'}")
        lines.append(f"{var}={seen[var]}")
    for prefix, var in PATH_VARS:
        if var.startswith("QDL_TLS_"):
            continue  # from generation.env
        lines.append(f"{var}={prefix}")
    return "\n".join(lines) + "\n"


# ------------------------------------------------------------------------------------------------ secrets env
def secrets_env(out: Path) -> dict:
    values: dict[str, set] = {k: set() for k in SECRET_ENV}
    for role in ROLES:
        it = json.loads(sh(["docker", "inspect", f"{PROJECT}-{role}-1"]))[0]
        for item in it["Config"].get("Env") or []:
            key, value = item.split("=", 1)
            if key in values:
                values[key].add(value)
    for key, found in values.items():
        if len(found) != 1:
            raise SystemExit(f"{key}: {len(found)} distinct live values; refusing")
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        handle.write("# Env-only secrets of the KN topology, copied from the live containers (never regenerate).\n")
        for key in SECRET_ENV:
            value = next(iter(values[key]))
            handle.write(f"{key}='{value}'\n" if "'" not in value else f'{key}="{value}"\n')
    return {k: digest12(next(iter(v))) for k, v in values.items()}


# ------------------------------------------------------------------------------------------------ parity
def compose_config(compose: Path, env_files: list[Path], project: str = PROJECT) -> dict:
    """Rendered config. ``config`` keeps ``$$`` escaped in its output (it stays compose input); the container
    receives ``$``, so every string is unescaped here before it is compared with docker inspect."""
    argv = ["docker", "compose", "-p", project, "-f", str(compose)]
    for e in env_files:
        argv += ["--env-file", str(e)]
    raw = json.loads(sh(argv + ["config", "--format", "json"], env={**os.environ, "COMPOSE_PROFILES": ""}))
    return _unescape(raw)


def _unescape(value):
    if isinstance(value, str):
        return value.replace("$$", "$")
    if isinstance(value, list):
        return [_unescape(v) for v in value]
    if isinstance(value, dict):
        return {k: _unescape(v) for k, v in value.items()}
    return value


def image_id(ref: str) -> str:
    return json.loads(sh(["docker", "image", "inspect", ref]))[0]["Id"]


# docker compose hands ``cpus`` to the engine through a float32, so 2.25 arrives as NanoCpus 2249999872
# (1.3e-7 vCPU short; DL v2.2.5 window R2, 2026-10-20). Any real cap change is >= 0.01 vCPU.
CPUS_TOLERANCE = 1e-6


def cpus_equal(live_nano_cpus: int, compose_cpus) -> bool:
    """True when the live NanoCpus and the compose ``cpus`` value are the same cap within CPUS_TOLERANCE."""

    return abs(live_nano_cpus / 1e9 - float(compose_cpus or 0)) <= CPUS_TOLERANCE


def parity(compose: Path, env_files: list[Path]) -> dict:
    cfg = compose_config(compose, env_files)
    problems, deltas, checked = [], [], 0
    for role in ROLES:
        live = inspect_role(role)
        live_env_clear = dict(e.split("=", 1) for e in json.loads(sh(["docker", "inspect", f"{PROJECT}-{role}-1"]))[0]["Config"]["Env"])
        svc = cfg["services"][role]

        def same(field, a, b, equal=None):
            nonlocal checked
            checked += 1
            if not (equal(a, b) if equal else a == b):
                problems.append(f"{role}.{field}: live={_short(a)} compose={_short(b)}")

        same("image", live["image_id"], image_id(svc["image"]))
        want_env = {k: v for k, v in live_env_clear.items() if not (k in live["image_env"] and live["image_env"][k] == v)}
        got_env = {k: ("" if v is None else str(v)) for k, v in (svc.get("environment") or {}).items()}
        for key in sorted(set(want_env) | set(got_env)):
            a, b = want_env.get(key), got_env.get(key)
            if key in SECRET_ENV or is_secret(key):
                a, b = (digest12(a) if a is not None else None), (digest12(b) if b is not None else None)
            same(f"env.{key}", a, b)
        same("entrypoint", live["entrypoint"] if live["entrypoint"] != live["image_entrypoint"] else None,
             svc.get("entrypoint"))
        same("command", live["cmd"] if (live["cmd"] != live["image_cmd"] or svc.get("entrypoint")) else None,
             svc.get("command"))
        same("user", live["user"] if live["user"] != live["image_user"] else None, svc.get("user"))
        h = live["host"]
        same("cpus", h["NanoCpus"] / 1e9, float(svc.get("cpus", 0)),
             equal=lambda _a, b: cpus_equal(h["NanoCpus"], b))
        same("mem_limit", h["Memory"], int(svc.get("mem_limit", 0)))
        same("memswap_limit", h["MemorySwap"], int(svc.get("memswap_limit", 0)))
        same("read_only", bool(h["ReadonlyRootfs"]), bool(svc.get("read_only", False)))
        same("init", bool(h.get("Init")), bool(svc.get("init", False)))
        same("pids_limit", h.get("PidsLimit") or None, svc.get("pids_limit"))
        same("cap_drop", h.get("CapDrop") or None, svc.get("cap_drop"))
        same("security_opt", h.get("SecurityOpt") or None, svc.get("security_opt"))
        same("tmpfs", sorted(f"{k}:{v}" if v else k for k, v in (h.get("Tmpfs") or {}).items()) or None,
             sorted(svc.get("tmpfs") or []) or None)
        same("restart", h["RestartPolicy"]["Name"], svc.get("restart"))
        same("stop_grace", live["stop_timeout"], _seconds(svc.get("stop_grace_period")))
        ports = sorted(f"{b['HostIp']}:{b['HostPort']}:{p.split('/')[0]}"
                       for p, bs in (h.get("PortBindings") or {}).items() for b in bs or [])
        got_ports = sorted(f"{p.get('host_ip')}:{p.get('published')}:{p.get('target')}" for p in svc.get("ports") or [])
        same("ports", ports, got_ports)
        same("networks", {NETWORK_KEYS[n] for n in live["networks"]}, set((svc.get("networks") or {}).keys()))
        for net, aliases in live["networks"].items():
            keep = sorted(a for a in aliases if a not in {f"{PROJECT}-{role}-1", role}
                          and not re.fullmatch(r"[0-9a-f]{12}", a))
            got = sorted(((svc.get("networks") or {}).get(NETWORK_KEYS[net]) or {}).get("aliases") or [])
            same(f"aliases.{NETWORK_KEYS[net]}", keep, got)
        log = h["LogConfig"]
        if role == "stable_redis" and not log.get("Config"):
            deltas.append(f"{role}: logging rotation added (intended)")
        else:
            same("logging", (log["Type"], log.get("Config") or {}),
                 ((svc.get("logging") or {}).get("driver"), (svc.get("logging") or {}).get("options") or {}))
        if live["healthcheck"] and live["healthcheck"].get("Test"):
            same("healthcheck.test", live["healthcheck"]["Test"], (svc.get("healthcheck") or {}).get("test"))
            if any(role.startswith(prefix) for prefix in HEALTHCHECK_RETUNE):
                deltas.append(f"{role}: healthcheck cadence retuned (intended, owner C1)")
        else:
            if (svc.get("healthcheck") or {}).get("test"):
                deltas.append(f"{role}: healthcheck added (intended)")
            else:
                problems.append(f"{role}: no healthcheck")
        _mount_parity(role, live, svc, same, deltas)
    return {"schema": "qdl.kn-compose-parity.v1", "checked": checked, "problems": problems, "deltas": deltas,
            "verdict": "PASS" if not problems else "FAIL", "env_files": [str(e) for e in env_files]}


def _mount_parity(role, live, svc, same, deltas):
    got = {v["target"]: v for v in svc.get("volumes") or []}
    for m in live["mounts"]:
        if m["type"] == "volume" and not m["name"].startswith(PROJECT):
            continue  # anonymous image volume
        v = got.pop(m["target"], None)
        if v is None:
            same(f"mount{m['target']}", m["source"], None)
            continue
        same(f"mount{m['target']}.read_only", not m["rw"], bool(v.get("read_only")))
        if m["type"] == "volume" and m["name"] == f"{PROJECT}_stable_tls":
            equal = _tree_equal(m["source"], v["source"])
            same(f"mount{m['target']}.bytes(stable_tls volume vs generation dir)", True, equal)
            deltas.append(f"{role}: /stable-certs volume -> host dir {v['source']} (byte-equal={equal})")
        elif m["type"] == "volume":
            same(f"mount{m['target']}.volume", m["name"], f"{PROJECT}_{v.get('source')}")
        elif m["source"] == SESSION_LIVENESS:
            same(f"mount{m['target']}.subpath", "runtime/session-liveness", (v.get("volume") or {}).get("subpath"))
            deltas.append(f"{role}: session-liveness bind -> stable_state volume subpath (intended)")
        else:
            same(f"mount{m['target']}.source", m["source"], v.get("source"))
    for target, v in got.items():  # nested TLS binds added by the compose
        parent = next((m for m in live["mounts"] if target.startswith(m["target"] + "/")), None)
        if parent is None:
            same(f"mount{target}", None, v.get("source"))
            continue
        live_path = parent["source"] + target[len(parent["target"]):]
        equal = _tree_equal(live_path, v["source"])
        same(f"mount{target}.bytes(vs {live_path})", True, equal)
        deltas.append(f"{role}: nested bind {target} <- {v['source']} (byte-equal to live {live_path}={equal})")


def _tree_equal(a: str, b: str) -> bool:
    """Byte equality of two files or two directory trees (sudo: owner 10001 / root material)."""
    script = ("import hashlib,os,sys\n"
              "def t(p):\n"
              " if os.path.isfile(p): return {'':hashlib.sha256(open(p,'rb').read()).hexdigest()}\n"
              " out={}\n"
              " for d,_,fs in os.walk(p):\n"
              "  for f in fs:\n"
              "   q=os.path.join(d,f); out[os.path.relpath(q,p)]=hashlib.sha256(open(q,'rb').read()).hexdigest()\n"
              " return out\n"
              "print(t(sys.argv[1])==t(sys.argv[2]))\n")
    proc = subprocess.run(["sudo", "-n", "python3", "-B", "-c", script, a, b], capture_output=True, text=True)
    return proc.returncode == 0 and proc.stdout.strip() == "True"


def _seconds(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value) // 1_000_000_000 if value > 1_000_000 else int(value)
    m = re.fullmatch(r"(\d+)s", str(value))
    return int(m.group(1)) if m else value


def _short(value):
    text = json.dumps(value, sort_keys=True, default=str)
    return text if len(text) <= 160 else text[:150] + "...(" + str(len(text)) + ")"


# ------------------------------------------------------------------------------------------------ main
def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="mode", required=True)
    i = sub.add_parser("inventory")
    i.add_argument("--out", type=Path, required=True)
    r = sub.add_parser("render")
    r.add_argument("--inventory", type=Path, required=True)
    r.add_argument("--out", type=Path, required=True)
    r.add_argument("--release-env-out", type=Path, help="also write the committed release env (images + paths)")
    r.add_argument("--release", default="v2.2.4")
    r.add_argument("--image", action="append", default=[], help="VAR=ref override for the release env")
    s = sub.add_parser("secrets-env")
    s.add_argument("--out", type=Path, required=True)
    p = sub.add_parser("parity")
    p.add_argument("--compose", type=Path, required=True)
    p.add_argument("--env-file", type=Path, action="append", required=True)
    p.add_argument("--out", type=Path)
    args = parser.parse_args(argv)
    if args.mode == "inventory":
        inv = inventory()
        args.out.write_text(json.dumps(inv, indent=1, sort_keys=True) + "\n")
        os.chmod(args.out, 0o600)
        print(json.dumps({"inventory": str(args.out), "roles": len(inv["roles"])}))
        return 0
    if args.mode == "render":
        inv = json.loads(args.inventory.read_text())
        observed = args.inventory.stat().st_mtime
        import datetime as dt
        stamp = dt.datetime.fromtimestamp(observed, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        doc = render(inv, args.out, stamp)
        if args.release_env_out:
            overrides = dict(x.split("=", 1) for x in args.image)
            args.release_env_out.write_text(release_env(inv, args.release, overrides))
        print(json.dumps({"compose": str(args.out), "services": len(doc["services"])}))
        return 0
    if args.mode == "secrets-env":
        print(json.dumps({"secrets_env": str(args.out), "digests": secrets_env(args.out)}))
        return 0
    result = parity(args.compose, args.env_file)
    text = json.dumps(result, indent=1, sort_keys=True) + "\n"
    if args.out:
        args.out.write_text(text)
    print(json.dumps({"verdict": result["verdict"], "checked": result["checked"],
                      "problems": result["problems"][:40], "deltas": len(result["deltas"])}))
    return 0 if result["verdict"] == "PASS" else 2


if __name__ == "__main__":
    sys.exit(main())
