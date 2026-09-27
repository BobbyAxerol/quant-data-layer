#!/usr/bin/env python3
"""Prepare KN-5 runtime material from inspected production, without deploying.

The output is private (credentials never enter Git). This script does not run
compose, mutate Kafka, copy SQLite, or change an existing runtime directory.
Activation and rollback are separate, journalled KN-5 operations.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess

PROJECT = "qdl_v2_stable_candidate"
PYTHON_IMAGE = "sha256:f7351c3bda080a6d1266d49480dc65d7e590375f178252f858f87a04b1a4d685"
RUST_IMAGE = "sha256:7fe348060734e4f51824b02faed7020465bb8dc754ad5299cb88befba7f9f69f"
REDIS_IMAGE = "redis@sha256:dfa18828cbc07b3ae6a95ec7343f6c214fdee2d836197b4be8e9904420762cd8"


def inspected(role: str) -> dict:
    return json.loads(subprocess.check_output([
        "docker", "inspect", f"{PROJECT}-{role}-1",
    ]))[0]


def write_private(path: Path, content: str) -> None:
    with path.open("x") as handle:
        handle.write(content)
    path.chmod(0o640)
    os.chown(path, -1, 10001)


def environment(inspect: dict) -> dict[str, str]:
    return dict(value.split("=", 1) for value in inspect["Config"]["Env"])


def image_service(image: str, cpu: float, memory: str) -> dict:
    return {
        "image": image, "pull_policy": "never", "restart": "unless-stopped",
        "init": True, "read_only": True, "user": "10001:10001",
        "cpus": cpu, "mem_limit": memory, "pids_limit": 256,
        "security_opt": ["no-new-privileges:true"], "cap_drop": ["ALL"],
        "stop_grace_period": "45s", "tmpfs": ["/tmp:rw,noexec,nosuid,size=67108864"],
        "logging": {"driver": "json-file", "options": {"max-size": "20m", "max-file": "3"}},
    }


def service_from_inspect(item: dict) -> dict:
    """Preserve a role's exact effective config for bounded recreate/rollback."""
    host, config = item["HostConfig"], item["Config"]
    service = {"image": item["Image"], "pull_policy": "never",
        "container_name": item["Name"].lstrip("/"),
        "environment": environment(item), "command": config["Cmd"],
        "entrypoint": config["Entrypoint"], "user": config["User"],
        "working_dir": config["WorkingDir"], "read_only": host["ReadonlyRootfs"],
        "restart": host["RestartPolicy"]["Name"],
        "logging": {"driver": host["LogConfig"]["Type"], "options": host["LogConfig"]["Config"]},
        "volumes": [{"type": m["Type"], "source": m.get("Name", m["Source"]) if m["Type"] == "volume" else m["Source"],
                     "target": m["Destination"], "read_only": not m["RW"]} for m in item["Mounts"]],
        "networks": {name: {"aliases": sorted(set(v.get("Aliases") or []))}
                     for name, v in item["NetworkSettings"]["Networks"].items()}}
    for source, target in (("Memory", "mem_limit"), ("MemoryReservation", "mem_reservation"),
                           ("PidsLimit", "pids_limit"), ("Init", "init")):
        if host.get(source) is not None:
            service[target] = host[source]
    if host.get("NanoCpus"):
        service["cpus"] = host["NanoCpus"] / 1e9
    for source, target in (("CapDrop", "cap_drop"), ("CapAdd", "cap_add"),
                           ("SecurityOpt", "security_opt"), ("Sysctls", "sysctls")):
        if host.get(source):
            service[target] = host[source]
    if host.get("Tmpfs"):
        service["tmpfs"] = [f"{key}:{value}" for key, value in host["Tmpfs"].items()]
    if config.get("StopTimeout"):
        service["stop_grace_period"] = f"{config['StopTimeout']}s"
    if config.get("Healthcheck"):
        service["healthcheck"] = {k.lower(): v for k, v in config["Healthcheck"].items()}
        for key in ("interval", "timeout", "startperiod", "startinterval"):
            if key in service["healthcheck"]:
                value = service["healthcheck"].pop(key)
                service["healthcheck"][{"startperiod": "start_period", "startinterval": "start_interval"}.get(key, key)] = f"{value}ns"
    if host.get("PortBindings"):
        service["ports"] = [{"target": int(port.split('/')[0]), "protocol": port.split('/')[1],
                             "published": v["HostPort"], "host_ip": v["HostIp"]}
                            for port, values in host["PortBindings"].items() for v in values or []]
    return service


def validate_revision_boundary(cores: dict, ingestors: dict, environments: dict,
                               catalog: dict, acquisition: dict) -> dict:
    """Validate effective mounted JSON for every replica, not path assumptions."""
    required_roles = {"rust_core", "rust_core_2", "rust_core_3"}
    if set(cores) != required_roles or set(environments) != required_roles:
        raise ValueError("all three effective core replicas are required")
    revision = catalog["catalog_revision"]
    catalog_ids = {item["binding_id"] for item in catalog["bindings"]}
    acquisition_ids = {item["binding_id"] for item in acquisition["bindings"]}
    if catalog_ids != acquisition_ids:
        raise ValueError("catalogue and acquisition binding sets differ")
    maps = []
    admission_keys = {"QDL_PROVIDER_ADMISSION_" + suffix for suffix in (
        "ENABLED", "LISTEN_ADDR", "POLICY_PATH", "POLICY_SHA256", "REDIS_PREFIX",
        "REDIS_URL", "SECRET")}
    for role, core in cores.items():
        bindings = core["core"]["bindings"]
        if not bindings or any(b["instrument_catalog_revision"] != revision for b in bindings):
            raise ValueError(f"{role}: producer/core catalogue revision mismatch")
        maps.append({(b["provider"], b.get("physical_native_symbol") or b["native_symbol"],
                      b.get("physical_native_channel") or b["native_channel"])
                     for b in bindings})
        env = environments[role]
        if env.get("QDL_PROVIDER_ADMISSION_ENABLED") not in ("1", "true", "TRUE"):
            raise ValueError(f"{role}: provider admission is disabled")
        if any(not env.get(key) for key in admission_keys):
            raise ValueError(f"{role}: incomplete provider admission configuration")
    if any(mapping != maps[0] for mapping in maps[1:]):
        raise ValueError("core replica subscription maps differ")
    if set(ingestors) != {"ingestor_binance_usdm", "ingestor_okx_swap"}:
        raise ValueError("both effective ingestors are required")
    for role, ingestor in ingestors.items():
        for b in ingestor["bindings"]:
            if b["instrument_catalog_revision"] != revision:
                raise ValueError(f"{role}: producer/core catalogue revision mismatch")
            if (b["provider"], b["native_symbol"], b["native_channel"]) not in maps[0]:
                raise ValueError(f"{role}: subscription missing from core map")
    return {"catalog_revision": revision, "cores": len(cores),
            "ingestors": len(ingestors), "binding_count": len(catalog_ids)}


def compose_literal(value):
    """Docker inspect is already expanded; Compose must not expand it again."""
    if isinstance(value, str):
        return value.replace("$", "$$")
    if isinstance(value, list):
        return [compose_literal(item) for item in value]
    if isinstance(value, dict):
        return {key: compose_literal(item) for key, item in value.items()}
    return value


def external_compose(services: dict) -> dict:
    networks = {network for service in services.values() for network in service["networks"]}
    volumes = {m["source"] for s in services.values() for m in s["volumes"] if m["type"] == "volume"}
    return {"name": PROJECT, "services": compose_literal(services),
            "networks": {n: {"external": True, "name": n} for n in networks},
            "volumes": {n: {"external": True, "name": n} for n in volumes}}


def query_environment(current: dict[str, str], *, topic_id: str,
                      generation: str, replica: int) -> dict[str, str]:
    result = dict(current)
    result.update({
        "QDL_STABLE_QUERY_BACKEND": "kn3",
        "QDL_STABLE_EXECUTION_MARK_INDEX_URLS_JSON": "[]",
        "QDL_KN3_MARKET_CACHE_URL": "redis://market_cache:6379/0",
        "QDL_KN3_ENVIRONMENT": "paper",
        "QDL_KN_CURSOR_KEYS_FILE": "/kn/cursor-keys.json",
        "QDL_KN_CURSOR_ACTIVE_KEY_ID": "kn-v220-k1",
        "QDL_KN_TOPIC_ID": topic_id,
        "QDL_KN_ROUTE_GENERATION": generation,
        "QDL_KN_READ_VIEW_SECRET_FILE": "/kn/read-view.secret",
        "QDL_STABLE_STATE_DIR": "/var/lib/qdl-kn/runtime",
        "QDL_STABLE_DURABLE_STATE_DIR": "/var/lib/qdl-kn/shared",
        "QDL_STABLE_AUDIT_PATH": f"/var/lib/qdl-kn/runtime/query-{replica}-audit.jsonl",
        "QDL_STABLE_INSTANCE_ID": f"stable-kn-query-{replica}",
        "MALLOC_ARENA_MAX": "2", "PYTHONDONTWRITEBYTECODE": "1",
    })
    return result


def prepare(out: Path, bundle_path: Path, topic_id: str, generation: str) -> dict:
    if out.exists():
        raise ValueError("refusing to overwrite runtime directory")
    if not topic_id or not generation:
        raise ValueError("explicit topic identity and generation required")
    query = inspected("query_v2_1")
    current = environment(query)
    bundle = json.loads(bundle_path.read_text())
    if bundle["catalog"]["catalog_revision"] != 11:
        raise ValueError("expected reviewed catalog revision 11")
    runtime = next(m["Source"] for m in query["Mounts"] if m["Destination"] == "/runtime")
    out.mkdir(mode=0o750, parents=True)
    os.chown(out, -1, 10001)
    for name in ("state", "tls", "kafka-admin"):
        (out / name).mkdir(mode=0o750)
        os.chown(out / name, -1, 10001)
    write_private(out / "bundle.json", json.dumps(bundle, sort_keys=True))
    write_private(out / "cursor-keys.json", json.dumps({"kn-v220-k1": secrets.token_hex(32)}))
    write_private(out / "read-view.secret", secrets.token_hex(32))
    write_private(out / "jwt-config.json", json.dumps({
        "issuer": current["QDL_DATA_JWT_ISSUER"], "audience": current["QDL_DATA_JWT_AUDIENCE"],
        "keys": json.loads(current["QDL_DATA_JWT_KEYS_JSON"]),
        "subjects": json.loads(current["QDL_DATA_JWT_KEY_SUBJECTS_JSON"]),
        "algorithms": "RS256", "max_lifetime_seconds": 900,
    }))
    services = {}
    cache = image_service(REDIS_IMAGE, 1.0, "9g")
    cache.update({"command": ["redis-server", "--save", "", "--appendonly", "no",
        "--maxmemory", "8000000000", "--maxmemory-policy", "noeviction",
        "--hash-max-listpack-entries", "128", "--hash-max-listpack-value", "2048"],
        "networks": ["stable_internal", "kn_consumer"],
        "healthcheck": {"test": ["CMD", "redis-cli", "ping"], "interval": "10s", "timeout": "3s", "retries": 3}})
    services["market_cache"] = cache
    for replica in (1, 2):
        state = out / "state" / f"projector-{replica}"
        state.mkdir(mode=0o750)
        os.chown(state, 10001, 10001)
        proj = image_service(RUST_IMAGE, 0.75, "512m")
        proj.update({"entrypoint": ["/usr/local/bin/qdl-projector"], "command": ["run"],
            "networks": ["stable_internal"], "volumes": [f"{out}:/kn:ro", f"{state}:/status"],
            "environment": {
                "QDL_KN_KAFKA_BOOTSTRAP": "kafka1:9092,kafka2:9092,kafka3:9092",
                "QDL_KN_KAFKA_CA": "/kn/tls/broker-ca.crt",
                "QDL_KN_KAFKA_CERT": "/kn/tls/kn-projector.crt",
                "QDL_KN_KAFKA_KEY": "/kn/tls/kn-projector.key",
                "QDL_KN_BUNDLE_PATH": "/kn/bundle.json", "QDL_KN_TOPIC_ID": topic_id,
                "QDL_KN_MATERIALIZER_EPOCH": "1", "QDL_KN_REPLICA": f"production-{replica}",
                "QDL_KN_MARKET_CACHE_URL": "redis://market_cache:6379/0",
                "QDL_KN_STATUS_PATH": "/status/status.json", "QDL_KN_STATUS_INTERVAL_S": "5",
            }})
        services[f"market_projector_{replica}"] = proj
        qenv = query_environment(current, topic_id=topic_id, generation=generation, replica=replica)
        write_private(out / f"query-{replica}.env", "".join(f"{k}={v}\n" for k, v in qenv.items()))
        state = out / "state" / f"query-{replica}"
        for sub in (state, state / "runtime", state / "shared"):
            sub.mkdir(mode=0o750)
            os.chown(sub, 10001, 10001)
        (state / "runtime" / "session-liveness").symlink_to("/session-liveness")
        state_mount = next(m["Source"] for m in query["Mounts"] if m["Destination"] == "/var/lib/qdl-stable")
        q = image_service(PYTHON_IMAGE, 1.5, "1536m")
        q.update({"command": ["python", "-m", "app.entrypoints.query_v2_stable"],
            "env_file": [{"path": str(out / f"query-{replica}.env"), "format": "raw"}],
            "networks": {"stable_internal": {}, "stable_egress": {},
                         "kn_consumer": {"aliases": [f"query_v2_{replica}", "qdl-v2-query"]}},
            "volumes": [f"{out}:/kn:ro", "stable_tls:/stable-certs:ro", f"{runtime}:/runtime:ro",
                f"{state}:/var/lib/qdl-kn", f"{state_mount}/runtime/session-liveness:/session-liveness:ro"],
            "ports": [f"127.0.0.1:{18300 + replica}:8200"],
            "healthcheck": {"test": ["CMD", "python", "-m", "app.entrypoints.query_v2_stable", "--healthcheck"],
                "interval": "10s", "timeout": "5s", "retries": 6}})
        # The entrypoint has no healthcheck CLI: same authenticated local probe as production.
        q["healthcheck"] = query["Config"]["Healthcheck"]
        q["healthcheck"] = {k.lower(): v for k,v in q["healthcheck"].items()}
        for key in ("interval", "timeout", "startperiod", "startinterval"):
            if key in q["healthcheck"]:
                value = q["healthcheck"].pop(key)
                q["healthcheck"][{"startperiod":"start_period","startinterval":"start_interval"}.get(key,key)] = f"{value}ns"
        services[f"query_kn_{replica}"] = q
        s = image_service(RUST_IMAGE, 0.75, "512m")
        s.update({"entrypoint": ["/usr/local/bin/qdl-stream-gateway"], "command": ["serve"],
            "networks": {"stable_internal": {}, "kn_consumer": {"aliases": [f"qdl-v2-stream-{'a' if replica == 1 else 'b'}"]}},
            "volumes": [f"{out}:/kn:ro", "stable_tls:/stable-certs:ro"],
            "ports": [f"127.0.0.1:{18319+replica}:8210"],
            "environment": {
                "QDL_ENVIRONMENT": "paper", "QDL_KN_BUNDLE_FILE": "/kn/bundle.json",
                "QDL_KN_JWT_CONFIG_FILE": "/kn/jwt-config.json",
                "QDL_KN_CURSOR_KEYS_FILE": "/kn/cursor-keys.json", "QDL_KN_CURSOR_ACTIVE_KEY_ID": "kn-v220-k1",
                "QDL_KN_TOPIC_ID": topic_id, "QDL_KN_ROUTE_GENERATION": generation,
                "QDL_KN_QUOTA_REDIS_URL": current["QDL_STABLE_REDIS_URL"],
                "QDL_KN_QUOTA_PREFIX": current["QDL_STABLE_REDIS_PREFIX"] + ":identity",
                "QDL_KN_TLS_CERT_FILE": "/stable-certs/stream/server.crt",
                "QDL_KN_TLS_KEY_FILE": "/stable-certs/stream/server.key",
                "QDL_KN_TLS_CLIENT_CA_FILE": "/stable-certs/stream/client-ca-bundle.crt",
                "QDL_KN_KAFKA_BOOTSTRAP": "kafka1:9092,kafka2:9092,kafka3:9092",
                "QDL_KN_KAFKA_CA": "/kn/tls/broker-ca.crt",
                "QDL_KN_KAFKA_CERT": "/kn/tls/kn-stream.crt", "QDL_KN_KAFKA_KEY": "/kn/tls/kn-stream.key",
                "QDL_KN_KAFKA_GROUP": f"kn-stream-production-{replica}",
                "QDL_KN_KAFKA_CLIENT_ID": f"kn-stream-production-{replica}",
                "QDL_KN_READ_VIEW_URLS": "https://qdl-v2-query:8200",
                "QDL_KN_READ_VIEW_SECRET_FILE": "/kn/read-view.secret",
                "QDL_KN_READ_VIEW_CA_FILE": "/stable-certs/stream/ca.crt",
                "QDL_KN_MAX_SUBSCRIPTIONS": "384", "QDL_KN_MAX_REPLAY_RPCS": "32",
            }})
        services[f"stream_kn_{replica}"] = s
    compose = {"name": PROJECT, "services": services, "networks": {
        "stable_internal": {"external": True, "name": f"{PROJECT}_stable_internal"},
        "stable_egress": {"external": True, "name": f"{PROJECT}_stable_egress"},
        "kn_consumer": {"name": "qdl_v2_native_consumer", "internal": True}},
        "volumes": {"stable_tls": {"external": True, "name": f"{PROJECT}_stable_tls"}}}
    write_private(out / "compose.json", json.dumps(compose_literal(compose), indent=2))
    summary = {"schema": "qdl.kn5.production-packet.v1", "status": "PREPARED_NOT_APPLIED",
        "topic_id": topic_id, "route_generation": generation, "images": [PYTHON_IMAGE, RUST_IMAGE],
        "bundle_sha256": hashlib.sha256(bundle_path.read_bytes()).hexdigest(),
        "compose_sha256": hashlib.sha256((out / "compose.json").read_bytes()).hexdigest(),
        "roles": sorted(services), "old_runtime": runtime, "production_mutations": []}
    write_private(out / "receipt.json", json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--topic-id", required=True)
    parser.add_argument("--generation", required=True)
    args = parser.parse_args()
    os.umask(0o027)
    print(json.dumps(prepare(args.out, args.bundle, args.topic_id, args.generation)))
