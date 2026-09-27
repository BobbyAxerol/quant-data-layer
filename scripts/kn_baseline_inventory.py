#!/usr/bin/env python3
"""KN-1 K1.1 baseline inventory for the Kafka-native V2.2.0 rearchitecture.

Purpose: one read-only receipt of what the Data Layer is *now* - source
contracts, public surface, catalog/manifests, runtime roles and images,
TLS/JWT/cursor identifiers, Kafka topics/ACL/lag, retained history and cache
usage - so later KN phases compare against recorded facts, not memory.

Boundary: never mutates anything. Every runtime probe is a read (docker
inspect/stats, Kafka ``--describe``/``--list``, SQLite ``mode=ro`` +
``query_only``, Redis ``INFO``/``DBSIZE``). Secrets are never written: cursor
and ingest secrets are reduced to key identifiers, JWT material to key ids and
subjects. Output is bounded JSON with its own SHA-256.

Two parts, because the source part needs the Data Layer's Python packages and
the runtime part needs the host's Docker CLI:

  # inside the existing qdl-v2-python image, network none, source read-only
  python -B scripts/kn_baseline_inventory.py source --source-sha SHA --out source.json
  # on the host
  python3 -B scripts/kn_baseline_inventory.py runtime --source-json source.json \
      --suite-log suite.log --out baseline.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import time
from typing import Any, Iterable, Mapping, Sequence

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "qdl_v2_stable_candidate"
INTERNAL_NETWORK = f"{PROJECT}_stable_internal"
KAFKA_BOOTSTRAP = "kafka1:9092,kafka2:9092,kafka3:9092"
KAFKA_ADMIN_CONFIG = "/etc/kafka/secrets/admin.properties"
STATE_VOLUME = f"{PROJECT}_stable_state"
TLS_VOLUME = f"{PROJECT}_stable_tls"
SPOOL_PATH = "/s/shared/canonical-cache.sqlite3"
SCHEMA = "qdl.kn.v220.baseline-inventory.v1"

# Non-secret configuration read from container environments. Anything not
# listed here is never copied into evidence.
_PLAIN_ENV = (
    "QDL_DATA_JWT_ISSUER", "QDL_DATA_JWT_AUDIENCE", "QDL_DATA_JWT_ALGORITHMS",
    "QDL_DATA_JWT_MAX_LIFETIME_SECONDS", "QDL_STABLE_CURSOR_ACTIVE_KEY_ID",
    "QDL_STABLE_CURSOR_TTL_SECONDS", "QDL_STABLE_CONSUMER_GROUP",
    "QDL_STABLE_REDIS_PREFIX", "QDL_STABLE_LEASE_SHARD_ID",
    "QDL_STABLE_MAX_REPLAY_EVENTS", "QDL_STABLE_MAX_BUFFER_EVENTS",
    "QDL_STABLE_MAX_STREAMS", "QDL_STABLE_ENVIRONMENT", "QDL_STABLE_CONFIG_REVISION",
)
# JSON environments reduced to identifiers only.
_KEY_ID_ENV = ("QDL_STABLE_CURSOR_KEYS_JSON", "QDL_DATA_JWT_KEYS_JSON")
_SUBJECT_ENV = "QDL_DATA_JWT_KEY_SUBJECTS_JSON"

# Evidence reuse map (guide 18.7 rule 3). Domain/provider evidence with pinned
# source and unchanged scope is inherited; every path KN replaces is re-proven.
EVIDENCE_REUSE = (
    {"scope": "venue normalization, canonical decimal/identity, Rust core math",
     "ledger": ["1.1", "1.2", "18", "45"], "decision": "REUSE_UNCHANGED",
     "reason": "KN keeps ingestors, rust_core and canonical contracts (KD13)"},
    {"scope": "final-BAR provider settlement and repair algorithm",
     "ledger": ["14", "40", "41"], "decision": "REUSE_UNCHANGED",
     "reason": "BAR edge keeps provider-authentic raw writes; only readback changes (KN-3)"},
    {"scope": "L2 book fence and Binance routed lanes",
     "ledger": ["36", "37", "38", "39"], "decision": "REUSE_UNCHANGED",
     "reason": "book reconstruction stays in rust_core"},
    {"scope": "OKX MARK/INDEX component freshness semantics",
     "ledger": ["20", "21"], "decision": "REUSE_SEMANTICS_REPROVE_READ",
     "reason": "the served read path moves to the market cache (KN-3/KN-4)"},
    {"scope": "stream delivery, replay, cursor, handoff",
     "ledger": ["10 C39.4", "24", "25"], "decision": "REPROVE",
     "reason": "native Rust Stream and cursor v3 replace the Python spool path (KN-2)"},
    {"scope": "Query snapshot/warmup/history/reference endpoints and latency",
     "ledger": ["13", "17", "19", "32"], "decision": "REPROVE",
     "reason": "Query backend moves from SQLite spool to market cache (KN-4)"},
    {"scope": "cache rebuild, boot recovery, restart policy",
     "ledger": ["7", "11", "12"], "decision": "REPROVE",
     "reason": "cache identity and rebuild runbook are replaced (KN-3)"},
    {"scope": "capacity 50 alpha + TS",
     "ledger": [], "decision": "NOT_CERTIFIED_CARRIED_TO_KN5",
     "reason": "v2.1.1 target closure never passed stage 50"},
)


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ---------------------------------------------------------------- source part

def proto_surface(root: Path) -> dict[str, Any]:
    services: dict[str, list[str]] = {}
    files = sorted((root / "contracts/proto").rglob("*.proto"))
    for path in files:
        service = None
        for line in path.read_text(encoding="utf-8").splitlines():
            match = re.match(r"\s*service\s+(\w+)", line)
            if match:
                service = f"{path.relative_to(root)}::{match.group(1)}"
                services[service] = []
                continue
            rpc = re.match(r"\s*rpc\s+(\w+)\s*\(\s*(stream\s+)?(\w+)\s*\)\s*returns\s*\(\s*(stream\s+)?(\w+)", line)
            if rpc and service:
                services[service].append(
                    f"{rpc.group(1)}({'stream ' if rpc.group(2) else ''}{rpc.group(3)})"
                    f"->{'stream ' if rpc.group(4) else ''}{rpc.group(5)}"
                )
    return {
        "services": services,
        "files": {str(p.relative_to(root)): file_sha256(p) for p in files},
    }


def http_surface(root: Path) -> dict[str, Any]:
    path = root / "contracts/v2/openapi.snapshot.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    operations = sorted(
        f"{method.upper()} {route}"
        for route, methods in document.get("paths", {}).items()
        for method in methods
        if method.lower() in {"get", "post", "put", "delete", "patch"}
    )
    return {"snapshot": str(path.relative_to(root)), "sha256": file_sha256(path),
            "operations": operations}


def catalog_summary(root: Path) -> dict[str, Any]:
    from qdl.runtime.stable_catalog import StableSourceCatalog

    path = root / "config/v2/stable-source-bindings.yaml"
    catalog = StableSourceCatalog.load(path)
    by_feed: dict[str, int] = {}
    by_venue_feed: dict[str, int] = {}
    physical: dict[str, list[str]] = {}
    for binding in catalog.bindings:
        feed = binding.feed.value
        venue = binding.instrument.identity.venue
        by_feed[feed] = by_feed.get(feed, 0) + 1
        key = f"{venue}|{feed}"
        by_venue_feed[key] = by_venue_feed.get(key, 0) + 1
        physical.setdefault(binding.partition_key, []).append(binding.binding_id)
    shared = {key: sorted(ids) for key, ids in physical.items() if len(ids) > 1}
    return {
        "path": str(path.relative_to(root)), "sha256": file_sha256(path),
        "catalog_revision": catalog.catalog_revision,
        "source_policy_revision": catalog.source_policy_revision,
        "authority_revision": catalog.authority_revision,
        "canonical_stream": catalog.canonical_stream,
        "instruments": len(catalog.instruments), "bindings": len(catalog.bindings),
        "physical_partition_keys": len(physical),
        "physical_keys_shared_by_bindings": len(shared),
        "shared_physical_key_examples": dict(sorted(shared.items())[:3]),
        "bindings_by_feed": dict(sorted(by_feed.items())),
        "bindings_by_venue_feed": dict(sorted(by_venue_feed.items())),
    }


def manifest_summary(root: Path) -> list[dict[str, Any]]:
    from qdl.consumer.manifest import ConsumerManifestLoader

    rows = []
    for path in sorted((root / "consumers/stable").glob("*.yaml")):
        manifest = ConsumerManifestLoader.load(path)
        feeds: dict[str, int] = {}
        warmup: dict[str, int] = {}
        recovery: dict[str, int] = {}
        for requirement in manifest.requirements:
            feed = requirement.feed.value if hasattr(requirement.feed, "value") else str(requirement.feed)
            feeds[feed] = feeds.get(feed, 0) + 1
            limit = int(getattr(requirement, "warmup_limit", 0) or 0)
            label = f"{feed}|{getattr(requirement, 'interval', None) or '-'}"
            warmup[label] = max(warmup.get(label, 0), limit)
            policy = getattr(requirement, "recovery_policy", None)
            if policy is not None:
                name = policy.value if hasattr(policy, "value") else str(policy)
                recovery[name] = recovery.get(name, 0) + 1
        quotas = manifest.quotas
        rows.append({
            "path": str(path.relative_to(root)), "sha256": file_sha256(path),
            "consumer_id": manifest.consumer_id, "subject": manifest.subject,
            "environment": manifest.environment,
            "manifest_revision": manifest.manifest_revision,
            "execution_dependency": manifest.execution_dependency,
            "purposes": sorted(str(getattr(p, "value", p)) for p in manifest.allowed_purposes),
            "requirements": len(manifest.requirements),
            "feeds": dict(sorted(feeds.items())),
            "max_warmup_by_feed_interval": dict(sorted(warmup.items())),
            "recovery_policies": dict(sorted(recovery.items())),
            "quotas": {
                "requests_per_minute": quotas.requests_per_minute,
                "max_batch_items": quotas.max_batch_items,
                "max_warmup_rows": quotas.max_warmup_rows,
                "max_streams": quotas.max_streams,
                "max_buffer_events": quotas.max_buffer_events,
            },
        })
    return rows


def compose_summary(root: Path) -> dict[str, Any]:
    import yaml

    path = root / "docker-compose.v2-stable.yml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    services = {}
    for name, spec in sorted((document.get("services") or {}).items()):
        networks = spec.get("networks") or {}
        aliases = {}
        if isinstance(networks, dict):
            for network, value in networks.items():
                aliases[network] = list((value or {}).get("aliases", []) or [])
        services[name] = {
            "cpus": spec.get("cpus"), "mem_limit": spec.get("mem_limit"),
            "restart": spec.get("restart"), "profiles": spec.get("profiles"),
            "network_aliases": aliases,
        }
    return {"path": str(path.relative_to(root)), "sha256": file_sha256(path), "services": services}


def budget_summary(root: Path) -> dict[str, Any]:
    path = root / "config/v2/v211-target-acceptance-budget.json"
    budget = json.loads(path.read_text(encoding="utf-8"))
    return {"path": str(path.relative_to(root)), "sha256": file_sha256(path),
            "stages": budget["stages"], "latency_ms": budget["latency_ms"],
            "trading_system": {k: budget["trading_system"][k]
                               for k in ("demanded_routes", "max_fallback")}}


def source_part(root: Path, source_sha: str) -> dict[str, Any]:
    return {
        "part": "source", "source_sha": source_sha,
        "public_grpc": proto_surface(root),
        "public_http": http_surface(root),
        "catalog": catalog_summary(root),
        "manifests": manifest_summary(root),
        "compose": compose_summary(root),
        "v211_budget": budget_summary(root),
        "evidence_reuse": list(EVIDENCE_REUSE),
    }


# --------------------------------------------------------------- runtime part

def _run(argv: Sequence[str], timeout: float = 120.0) -> str:
    return subprocess.run(
        list(argv), check=True, capture_output=True, text=True, timeout=timeout
    ).stdout


def _json_env_ids(raw: str | None) -> list[str] | None:
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return ["<unparseable>"]
    if isinstance(value, dict):
        return sorted(str(key) for key in value)
    if isinstance(value, list):
        return sorted(str(item.get("kid", "?")) for item in value if isinstance(item, dict))
    return ["<unexpected-shape>"]


def containers() -> list[dict[str, Any]]:
    names = _run(["docker", "ps", "-a", "--filter", f"label=com.docker.compose.project={PROJECT}",
                  "--format", "{{.Names}}"]).split()
    if not names:
        return []
    documents = json.loads(_run(["docker", "inspect", *names]))
    rows = []
    for doc in sorted(documents, key=lambda item: item["Name"]):
        env = dict(item.split("=", 1) for item in doc["Config"].get("Env") or [] if "=" in item)
        networks = {
            name: sorted(set(value.get("Aliases") or []) - {doc["Name"].lstrip("/"), doc["Config"]["Hostname"]})
            for name, value in (doc["NetworkSettings"].get("Networks") or {}).items()
        }
        state = doc["State"]
        subjects = None
        if env.get(_SUBJECT_ENV):
            try:
                subjects = json.loads(env[_SUBJECT_ENV])
            except json.JSONDecodeError:
                subjects = "<unparseable>"
        rows.append({
            "name": doc["Name"].lstrip("/"),
            "service": doc["Config"]["Labels"].get("com.docker.compose.service"),
            "image_ref": doc["Config"]["Image"], "image_id": doc["Image"],
            "revision_label": doc["Config"]["Labels"].get("org.opencontainers.image.revision"),
            "status": state.get("Status"), "health": (state.get("Health") or {}).get("Status"),
            "restart_count": doc.get("RestartCount"), "started_at": state.get("StartedAt"),
            "oom_killed": state.get("OOMKilled"),
            "cpus": (doc["HostConfig"].get("NanoCpus") or 0) / 1e9,
            "memory_bytes": doc["HostConfig"].get("Memory"),
            "restart_policy": (doc["HostConfig"].get("RestartPolicy") or {}).get("Name"),
            "network_aliases": networks,
            "config": {key: env[key] for key in _PLAIN_ENV if key in env},
            "key_ids": {key: _json_env_ids(env.get(key)) for key in _KEY_ID_ENV if key in env},
            "jwt_key_subjects": subjects,
        })
    return rows


def image_digests(image_ids: Iterable[str]) -> dict[str, Any]:
    result = {}
    for image_id in sorted(set(image_ids)):
        doc = json.loads(_run(["docker", "image", "inspect", image_id]))[0]
        result[image_id] = {"repo_tags": doc.get("RepoTags"), "repo_digests": doc.get("RepoDigests"),
                            "created": doc.get("Created"), "size": doc.get("Size")}
    return result


_TLS_SCRIPT = r"""
import json, pathlib, hashlib
from cryptography import x509
from cryptography.hazmat.primitives import hashes
out = {}
for path in sorted(pathlib.Path('/t').rglob('*.crt')):
    data = path.read_bytes()
    certs = x509.load_pem_x509_certificates(data)
    rows = []
    for cert in certs:
        try:
            san = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value.get_values_for_type(x509.DNSName)
        except x509.ExtensionNotFound:
            san = []
        rows.append({'subject': cert.subject.rfc4514_string(), 'issuer': cert.issuer.rfc4514_string(),
                     'san_dns': san, 'not_before': cert.not_valid_before_utc.isoformat(),
                     'not_after': cert.not_valid_after_utc.isoformat(),
                     'sha256': cert.fingerprint(hashes.SHA256()).hex()})
    out[str(path.relative_to('/t'))] = rows
print(json.dumps(out))
"""


def tls_inventory(python_image: str) -> dict[str, Any]:
    raw = _run(["docker", "run", "--rm", "--network", "none", "--read-only",
                "-v", f"{TLS_VOLUME}:/t:ro", "--entrypoint", "python3", python_image, "-B", "-c", _TLS_SCRIPT])
    return json.loads(raw)


def _kafka(image: str, cert: str, script: str, timeout: float = 240.0) -> str:
    return _run(["docker", "run", "--rm", "--network", INTERNAL_NETWORK, "-v", f"{cert}:/etc/kafka/secrets:ro",
                 image, "sh", "-c", script], timeout=timeout)


def kafka_inventory() -> dict[str, Any]:
    kafka1 = f"{PROJECT}-kafka1-1"
    image = _run(["docker", "inspect", "--format", "{{.Config.Image}}", kafka1]).strip()
    cert = _run(["docker", "inspect", "--format",
                 '{{range .Mounts}}{{if eq .Destination "/etc/kafka/secrets"}}{{.Source}}{{end}}{{end}}',
                 kafka1]).strip()
    base = f"--bootstrap-server {KAFKA_BOOTSTRAP} --command-config {KAFKA_ADMIN_CONFIG}"
    script = (
        f"echo '#TOPICS'; /opt/kafka/bin/kafka-topics.sh {base} --describe 2>/dev/null | grep -v '^\\s*Topic:.*Partition:';"
        f"echo '#ACLS'; /opt/kafka/bin/kafka-acls.sh {base} --list 2>/dev/null;"
        f"echo '#GROUPS'; /opt/kafka/bin/kafka-consumer-groups.sh {base} --list 2>/dev/null;"
        f"echo '#LAG'; /opt/kafka/bin/kafka-consumer-groups.sh {base} --describe --group stable-projector-v1 2>/dev/null"
    )
    text = _kafka(image, cert, script)
    sections: dict[str, list[str]] = {}
    current = None
    for line in text.splitlines():
        if line.startswith("#") and line[1:].isupper():
            current = line[1:]
            sections[current] = []
        elif current and line.strip():
            sections[current].append(line.rstrip())
    topics = {}
    for line in sections.get("TOPICS", []):
        match = re.match(r"Topic:\s*(\S+)\s+TopicId:\s*(\S+)\s+PartitionCount:\s*(\d+)\s+ReplicationFactor:\s*(\d+)\s+Configs:\s*(.*)", line)
        if match:
            topics[match.group(1)] = {
                "topic_id": match.group(2), "partitions": int(match.group(3)),
                "replication_factor": int(match.group(4)),
                "configs": dict(item.split("=", 1) for item in match.group(5).split(",") if "=" in item),
            }
    acls: list[dict[str, Any]] = []
    resource = None
    for line in sections.get("ACLS", []):
        match = re.search(r"ResourcePattern\(resourceType=(\w+), name=([^,]+), patternType=(\w+)\)", line)
        if match:
            resource = {"type": match.group(1), "name": match.group(2), "pattern": match.group(3)}
            continue
        entry = re.search(r"principal=([^,]+), host=([^,]+), operation=(\w+), permissionType=(\w+)", line)
        if entry and resource:
            acls.append({**resource, "principal": entry.group(1), "operation": entry.group(3),
                         "permission": entry.group(4)})
    lag = []
    for line in sections.get("LAG", []):
        parts = line.split()
        if len(parts) >= 6 and parts[0] == "stable-projector-v1":
            lag.append({"partition": int(parts[2]), "current": int(parts[3]),
                        "log_end": int(parts[4]), "lag": int(parts[5])})
    return {"kafka_image": image, "topics": topics, "acls": acls,
            "groups": sorted(sections.get("GROUPS", [])), "projector_lag": sorted(lag, key=lambda r: r["partition"])}


_SPOOL_SCRIPT = r"""
import json, sqlite3, collections
c = sqlite3.connect('file:__SPOOL__?mode=ro', uri=True, timeout=5)
c.execute('PRAGMA query_only=ON')
feeds = collections.Counter(); keys = collections.Counter(); written = {}
for key, n in c.execute('select partition_key, next_offset-1 from partitions where stream=?', ('md.canonical.v2',)):
    feed = key.split('/')[1] if '/' in key else '?'
    feeds[feed] += n; keys[feed] += 1; written[key] = n
bars = []
expr = "json_extract(headers_json,'$.\"qdl.final_bar_close_time_ns\"')"
for key in sorted(k for k in written if k.split('/')[1] == 'bar'):
    row = c.execute('select count(*), min(logical_offset), max(logical_offset), min(%s), max(%s), sum(length(payload)) '
                    'from events where stream=? and partition_key=?' % (expr, expr), ('md.canonical.v2', key)).fetchone()
    bars.append({'partition_key': key, 'retained_rows': row[0], 'min_offset': row[1], 'max_offset': row[2],
                 'min_close_ns': row[3], 'max_close_ns': row[4], 'payload_bytes': row[5]})
total = c.execute('select count(*), sum(length(payload)), sum(length(headers_json)) from events').fetchone()
print(json.dumps({'written_events_by_feed': dict(feeds), 'keys_by_feed': dict(keys),
                  'retained_rows': total[0], 'retained_payload_bytes': total[1], 'retained_header_bytes': total[2],
                  'bar_history': bars}))
"""


def spool_inventory(python_image: str) -> dict[str, Any]:
    script = _SPOOL_SCRIPT.replace("__SPOOL__", SPOOL_PATH)
    raw = _run(["docker", "run", "--rm", "--network", "none", "--read-only", "-v", f"{STATE_VOLUME}:/s:ro",
                "--entrypoint", "python3", python_image, "-B", "-c", script], timeout=900)
    return json.loads(raw)


def redis_inventory() -> dict[str, Any]:
    name = f"{PROJECT}-stable_redis-1"
    info = _run(["docker", "exec", name, "redis-cli", "--no-auth-warning", "INFO", "memory"])
    wanted = ("used_memory", "used_memory_peak", "maxmemory", "maxmemory_policy", "mem_fragmentation_ratio")
    memory = {}
    for line in info.splitlines():
        key, _, value = line.strip().partition(":")
        if key in wanted:
            memory[key] = value
    keyspace = _run(["docker", "exec", name, "redis-cli", "--no-auth-warning", "INFO", "keyspace"]).strip().splitlines()[1:]
    return {"memory": memory, "keyspace": keyspace}


def host_inventory() -> dict[str, Any]:
    import os
    import shutil
    stat = shutil.disk_usage("/")
    meminfo = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines() if ":" in line)
    return {"cpus": os.cpu_count(), "mem_total": meminfo.get("MemTotal", "").strip(),
            "mem_available": meminfo.get("MemAvailable", "").strip(),
            "disk_total_bytes": stat.total, "disk_free_bytes": stat.free,
            "load_avg": list(os.getloadavg())}


def suite_failures(log: Path | None) -> dict[str, Any] | None:
    if log is None:
        return None
    text = log.read_text(encoding="utf-8", errors="replace")
    ids = re.findall(r"^(ERROR|FAIL): (\S+) \(([^)]+)\)", text, flags=re.M)
    summary = re.findall(r"^Ran (\d+) tests? in", text, flags=re.M)
    result = re.findall(r"^(OK|FAILED)(?: \(([^)]*)\))?$", text, flags=re.M)
    return {"log_sha256": hashlib.sha256(text.encode()).hexdigest(),
            "ran": int(summary[-1]) if summary else None,
            "result": list(result[-1]) if result else None,
            "failures": [{"kind": kind, "test": f"{owner}.{name}"} for kind, name, owner in ids]}


def runtime_part(source: Mapping[str, Any], suite_log: Path | None, python_image: str) -> dict[str, Any]:
    rows = containers()
    return {
        "part": "runtime", "collected_at_ns": time.time_ns(),
        "host": host_inventory(), "containers": rows,
        "images": image_digests(row["image_id"] for row in rows),
        "tls": tls_inventory(python_image), "kafka": kafka_inventory(),
        "spool": spool_inventory(python_image), "control_redis": redis_inventory(),
        "suite": suite_failures(suite_log),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="part", required=True)
    src = sub.add_parser("source")
    src.add_argument("--source-sha", required=True)
    src.add_argument("--out", type=Path, required=True)
    run = sub.add_parser("runtime")
    run.add_argument("--source-json", type=Path, required=True)
    run.add_argument("--suite-log", type=Path)
    run.add_argument("--python-image", required=True)
    run.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.part == "source":
        payload = source_part(ROOT, args.source_sha)
    else:
        source = json.loads(args.source_json.read_text(encoding="utf-8"))
        payload = {"schema": SCHEMA, "source": source,
                   "runtime": runtime_part(source, args.suite_log, args.python_image)}
    payload["sha256"] = canonical_sha256(payload)
    args.out.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"out": str(args.out), "sha256": payload["sha256"]}), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
