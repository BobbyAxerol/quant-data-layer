#!/usr/bin/env python3
"""Decision B of P18.4 G12 (owner, 2026-10-16): ``auto.leader.rebalance.enable=false`` on kafka1-3, one broker at a time.

Purpose: with the broker default ``true`` the active KRaft controller moves every partition back to its preferred
replica ~300 s after a broker returns, including the ``__consumer_offsets`` / ``__transaction_state`` partitions
that host the realtime core's coordinators; the coordinator load stalls the core's transactional commit (G12 RCA,
32d2cf6). With ``false`` leadership comes back only through ``scripts/kafka_preferred_leader_election.py`` at a
declared moment, its stall measured and recorded. The setting is a static controller config, so it needs a
recreate of each broker (apache/kafka image: env ``KAFKA_AUTO_LEADER_REBALANCE_ENABLE`` -> server.properties).

How the live brokers were made: compose JSON of a removed packet directory (label
``com.docker.compose.project.config_files``), with later ``docker update`` drift (e.g. NanoCpus). The packet is
therefore derived from ``docker inspect`` of the running containers (``kn_production_packet.service_from_inspect``
/ ``external_compose`` / ``compose_literal``: exact image ID, env, mounts incl. the anonymous ``/mnt/shared/config``
volumes by name, network aliases, limits, tmpfs, healthcheck), not from ``docker-compose.v2-stable.yml``.

Modes:

* ``--prepare OUT`` (read-only: docker inspect only): writes ``rollback.compose.json`` (the live state),
  ``candidate.compose.json`` (the same plus the one env key) and ``receipt.json`` (per-broker inspect fingerprint,
  file digests) into a new private directory; refuses when a broker is not running+healthy or already carries the
  key with another value.
* ``--check PACKET`` (read-only): packet integrity, live fingerprints equal the receipt (no drift since prepare),
  cluster health (every broker healthy, no under-replicated or leaderless partition, quorum leader) and the
  recommended order (quorum leader last, so the active controller ends on a rolled node).
* ``--roll PACKET --broker kafkaN`` / ``--rollback PACKET --broker kafkaN``: dry-run by default (prints the exact
  argv); with ``--apply``: refuses under a foreign host-measurement lock, on drift, on an unhealthy cluster or on a
  broker already in the requested state; then ``docker stop --time 120`` (controlled shutdown inside the window;
  compose's default stop is 10 s), ``docker compose -p qdl_v2_stable_candidate -f <file> up -d --no-deps
  --force-recreate kafkaN``, waits healthy and zero under-replicated partitions, verifies the new container differs
  from the receipt only by the key and that the broker reports ``auto.leader.rebalance.enable`` as wanted. A roll
  that fails after the recreate rolls that broker back automatically (rollback compose) and checks the same
  postconditions (healthy, zero URP, env, fingerprint, broker setting): verified -> ``RESTORED_VERIFIED``, exit 1;
  broker up but a check failing -> ``BROKER_STARTED_RESTORATION_UNVERIFIED``, exit 3; not up -> ``ROLLBACK_FAILED``,
  exit 3.

Boundary: one broker per invocation; never two brokers down; no topic/ACL/TLS/volume/image/limit change; the admin
CLI runs in a throwaway ``stable_admin``-equal container (``AdminContainerRunner``, cert dir mounted ro, never read).
Not covered here: rebalancing leadership afterwards (``kafka_preferred_leader_election.py``, a separate declared
step). Exit codes: 0 OK, 1 verify failed (rolled back), 3 rollback after failure also failed (HARD STOP), 4 refused,
5 command error.
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, Callable, Mapping, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.kafka_md_latest_marker_retention import foreign_lock  # noqa: E402
from scripts.kafka_preferred_leader_election import parse_partitions  # noqa: E402
from scripts.kn_production_packet import PROJECT, compose_literal, external_compose, service_from_inspect  # noqa: E402
from scripts.kn_state_topics_packet import AdminContainerRunner  # noqa: E402

BROKERS = ("kafka1", "kafka2", "kafka3")
ENV_KEY = "KAFKA_AUTO_LEADER_REBALANCE_ENABLE"
CONFIG_KEY = "auto.leader.rebalance.enable"
CANDIDATE_VALUE = "false"
SCHEMA = "qdl.kafka.auto-leader-rebalance-roll.v1"
STOP_TIMEOUT_S = 120
HEALTHY_TIMEOUT_S = 300
URP_TIMEOUT_S = 900


class Refused(Exception):
    pass


def container(broker: str) -> str:
    if broker not in BROKERS:
        raise Refused(f"broker must be one of {BROKERS}")
    return f"{PROJECT}-{broker}-1"


def docker_inspect(name: str) -> dict:
    return json.loads(subprocess.check_output(["docker", "inspect", name], timeout=30))[0]


def env_of(item: Mapping[str, Any]) -> dict[str, str]:
    return dict(value.split("=", 1) for value in item["Config"]["Env"])


def fingerprint(item: Mapping[str, Any]) -> dict[str, Any]:
    """Everything a recreate must keep, minus ENV_KEY (the one intended change) and per-container identity."""
    host, config = item["HostConfig"], item["Config"]
    env = env_of(item)
    env.pop(ENV_KEY, None)
    return {
        "image": item["Image"], "env": dict(sorted(env.items())), "cmd": config["Cmd"],
        "entrypoint": config["Entrypoint"], "user": config["User"],
        "mounts": sorted([m["Type"], m.get("Name") if m["Type"] == "volume" else m["Source"], m["Destination"],
                          bool(m["RW"])] for m in item["Mounts"]),
        "nano_cpus": host.get("NanoCpus"), "memory": host.get("Memory"), "tmpfs": host.get("Tmpfs"),
        "security_opt": host.get("SecurityOpt"), "restart": host["RestartPolicy"]["Name"],
        "log": [host["LogConfig"]["Type"], host["LogConfig"]["Config"]], "healthcheck": config.get("Healthcheck"),
        "networks": {name: sorted(set(v.get("Aliases") or []) - {item["Id"][:12]})
                     for name, v in item["NetworkSettings"]["Networks"].items()},
    }


def diff(before: Mapping[str, Any], after: Mapping[str, Any]) -> list[str]:
    return sorted(k for k in set(before) | set(after) if before.get(k) != after.get(k))


def healthy(item: Mapping[str, Any]) -> bool:
    state = item["State"]
    return state.get("Running") is True and (state.get("Health") or {}).get("Status") == "healthy"


def build_services(inspects: Mapping[str, Mapping[str, Any]]) -> tuple[dict, dict]:
    rollback, candidate = {}, {}
    for broker in BROKERS:
        item = inspects[broker]
        if not healthy(item):
            raise Refused(f"{broker} is not running+healthy")
        current = env_of(item).get(ENV_KEY)
        if current not in (None, CANDIDATE_VALUE):
            raise Refused(f"{broker} already sets {ENV_KEY}={current!r}")
        service = service_from_inspect(item)
        rollback[broker] = service
        changed = copy.deepcopy(service)
        changed["environment"][ENV_KEY] = CANDIDATE_VALUE
        candidate[broker] = changed
    return rollback, candidate


def _write_private(path: Path, text: str) -> str:
    with path.open("x", encoding="utf-8") as handle:
        handle.write(text)
    path.chmod(0o600)
    return hashlib.sha256(text.encode()).hexdigest()


def prepare(out: Path, inspect: Callable[[str], dict] = docker_inspect) -> dict[str, Any]:
    if out.exists():
        raise Refused("refusing to overwrite a packet directory")
    inspects = {broker: inspect(container(broker)) for broker in BROKERS}
    rollback, candidate = build_services(inspects)
    out.mkdir(mode=0o700, parents=True)
    digests = {
        "rollback.compose.json": _write_private(out / "rollback.compose.json",
                                                json.dumps(external_compose(rollback), indent=1)),
        "candidate.compose.json": _write_private(out / "candidate.compose.json",
                                                 json.dumps(external_compose(candidate), indent=1)),
    }
    receipt = {"schema": SCHEMA, "status": "PREPARED_NOT_APPLIED", "project": PROJECT,
               "prepared_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
               "change": {"env": {ENV_KEY: CANDIDATE_VALUE}, "broker_config": {CONFIG_KEY: CANDIDATE_VALUE}},
               "brokers": {b: {"container": container(b), "image": inspects[b]["Image"],
                               "env_before": env_of(inspects[b]).get(ENV_KEY),
                               "fingerprint_sha256": hashlib.sha256(json.dumps(
                                   fingerprint(inspects[b]), sort_keys=True).encode()).hexdigest(),
                               "fingerprint": fingerprint(inspects[b])} for b in BROKERS},
               "files": digests, "unchanged": ["image", "TLS/ACL", "volumes", "topics/offsets", "CPU/memory caps",
                                               "networks", "other services"],
               "production_mutations": []}
    _write_private(out / "receipt.json", json.dumps(receipt, indent=1))
    return receipt


def load_packet(packet: Path) -> dict[str, Any]:
    receipt = json.loads((packet / "receipt.json").read_text(encoding="utf-8"))
    if receipt.get("schema") != SCHEMA or receipt.get("project") != PROJECT:
        raise Refused("not a packet of this tool")
    for name, digest in receipt["files"].items():
        if hashlib.sha256((packet / name).read_bytes()).hexdigest() != digest:
            raise Refused(f"{name} changed since prepare")
    candidate = json.loads((packet / "candidate.compose.json").read_text(encoding="utf-8"))
    rollback = json.loads((packet / "rollback.compose.json").read_text(encoding="utf-8"))
    for broker in BROKERS:
        c_env, r_env = candidate["services"][broker]["environment"], rollback["services"][broker]["environment"]
        if c_env.get(ENV_KEY) != CANDIDATE_VALUE or {k: v for k, v in c_env.items() if k != ENV_KEY} != {
                k: v for k, v in r_env.items() if k != ENV_KEY}:
            raise Refused(f"{broker}: candidate differs from rollback by more than {ENV_KEY}")
        if {k: v for k, v in candidate["services"][broker].items() if k != "environment"} != {
                k: v for k, v in rollback["services"][broker].items() if k != "environment"}:
            raise Refused(f"{broker}: candidate differs from rollback outside the environment")
    return receipt


def check_drift(receipt: Mapping[str, Any], inspects: Mapping[str, Mapping[str, Any]]) -> dict[str, list[str]]:
    return {b: diff(receipt["brokers"][b]["fingerprint"], json.loads(json.dumps(fingerprint(inspects[b]))))
            for b in BROKERS}


def cluster_health(describe_text: str) -> dict[str, Any]:
    rows = parse_partitions(describe_text)
    urp = [f"{r['topic']}-{r['partition']}" for r in rows if set(r["isr"]) != set(r["replicas"])]
    leaderless = [f"{r['topic']}-{r['partition']}" for r in rows if r["leader"] < 0]
    return {"partitions": len(rows), "under_replicated": urp, "leaderless": leaderless,
            "ok": bool(rows) and not urp and not leaderless}


def quorum_leader(status_text: str) -> int | None:
    for line in status_text.splitlines():
        if line.strip().startswith("LeaderId:"):
            return int(line.split(":", 1)[1].strip())
    return None


def broker_setting(text: str) -> str | None:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith(CONFIG_KEY + "="):
            return stripped.split("=", 1)[1].split(" ", 1)[0]
    return None


def recommended_order(leader: int | None) -> list[str]:
    others = [b for b in BROKERS if leader is None or b != f"kafka{leader}"]
    return others + ([f"kafka{leader}"] if leader is not None else [])


def describe_cmd() -> list[str]:
    return ["kafka-topics.sh", "--describe"]


def quorum_cmd() -> list[str]:
    return ["kafka-metadata-quorum.sh", "describe", "--status"]


def broker_config_cmd(broker: str) -> list[str]:
    container(broker)
    return ["kafka-configs.sh", "--entity-type", "brokers", "--entity-name", broker.removeprefix("kafka"),
            "--describe", "--all"]


def recreate_argv(packet: Path, broker: str, which: str) -> list[list[str]]:
    if which not in ("candidate", "rollback"):
        raise Refused("which must be candidate or rollback")
    return [["docker", "stop", "--time", str(STOP_TIMEOUT_S), container(broker)],
            ["docker", "compose", "-p", PROJECT, "-f", str(packet / f"{which}.compose.json"), "up", "-d",
             "--no-deps", "--force-recreate", broker]]


class Live:
    """Side effects, injectable for tests."""

    def __init__(self, runner: AdminContainerRunner) -> None:
        self.runner = runner

    def inspect(self, broker: str) -> dict:
        return docker_inspect(container(broker))

    def admin(self, command: Sequence[str]) -> str:
        result = self.runner.run(command)
        if result.returncode != 0:
            raise RuntimeError(f"{command[0]} failed rc={result.returncode}: {result.stderr.strip()[-300:]}")
        return result.stdout

    def execute(self, argv: Sequence[str]) -> None:
        subprocess.run(argv, check=True, timeout=STOP_TIMEOUT_S + 120)

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def now(self) -> float:
        return time.monotonic()


def wait_for(live: Live, what: str, predicate: Callable[[], bool], timeout_s: float, period_s: float = 5.0) -> float:
    start = live.now()
    while True:
        if predicate():
            return live.now() - start
        if live.now() - start > timeout_s:
            raise RuntimeError(f"timeout waiting for {what} ({timeout_s:.0f} s)")
        live.sleep(period_s)


def preconditions(live: Live, receipt: Mapping[str, Any]) -> dict[str, Any]:
    inspects = {b: live.inspect(b) for b in BROKERS}
    drift = {b: d for b, d in check_drift(receipt, inspects).items() if d}
    unhealthy = [b for b in BROKERS if not healthy(inspects[b])]
    health = cluster_health(live.admin(describe_cmd()))
    leader = quorum_leader(live.admin(quorum_cmd()))
    return {"drift": drift, "unhealthy": unhealthy, "cluster": health, "quorum_leader": leader,
            "recommended_order": recommended_order(leader),
            "env": {b: env_of(inspects[b]).get(ENV_KEY) for b in BROKERS},
            "ok": not drift and not unhealthy and health["ok"]}


def transition(live: Live, packet: Path, receipt: Mapping[str, Any], broker: str, which: str) -> dict[str, Any]:
    wanted = CANDIDATE_VALUE if which == "candidate" else receipt["brokers"][broker]["env_before"]
    pre = preconditions(live, receipt)
    if which == "rollback":  # recovery must not depend on the broker being rolled back being healthy
        pre["ok"] = not pre["drift"] and all(b == broker for b in pre["unhealthy"])
    if not pre["ok"]:
        raise Refused(f"preconditions failed: {json.dumps({k: pre[k] for k in ('drift', 'unhealthy', 'cluster')})}")
    if pre["env"][broker] == wanted:
        raise Refused(f"{broker} already has {ENV_KEY}={wanted!r}")
    report: dict[str, Any] = {"broker": broker, "target": which, "pre": pre,
                              "started_utc": dt.datetime.now(dt.timezone.utc).isoformat()}
    for argv in recreate_argv(packet, broker, which):
        live.execute(argv)
    report.update(postconditions(live, receipt, broker, which))
    return report


def postconditions(live: Live, receipt: Mapping[str, Any], broker: str, which: str) -> dict[str, Any]:
    """The same checks after a roll and after a rollback: healthy, zero under-replicated partitions, the env key
    as wanted, no other inspect difference from the receipt, the broker's effective setting."""
    wanted = CANDIDATE_VALUE if which == "candidate" else receipt["brokers"][broker]["env_before"]
    out: dict[str, Any] = {}
    out["healthy_after_s"] = wait_for(live, f"{broker} healthy", lambda: healthy(live.inspect(broker)),
                                      HEALTHY_TIMEOUT_S)
    out["isr_restored_after_s"] = wait_for(live, "zero under-replicated partitions",
                                           lambda: cluster_health(live.admin(describe_cmd()))["ok"], URP_TIMEOUT_S)
    item = live.inspect(broker)
    out["env_after"] = env_of(item).get(ENV_KEY)
    out["fingerprint_diff"] = diff(receipt["brokers"][broker]["fingerprint"],
                                   json.loads(json.dumps(fingerprint(item))))
    out["broker_config"] = broker_setting(live.admin(broker_config_cmd(broker)))
    expected_config = "false" if which == "candidate" else "true"
    out["ok"] = out["env_after"] == wanted and not out["fingerprint_diff"] and out["broker_config"] == expected_config
    return out


def main(argv: Sequence[str] | None = None, live_factory: Callable[[Path], Live] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--prepare", type=Path, metavar="OUT")
    mode.add_argument("--check", type=Path, metavar="PACKET")
    mode.add_argument("--roll", type=Path, metavar="PACKET")
    mode.add_argument("--rollback", type=Path, metavar="PACKET")
    parser.add_argument("--broker", choices=BROKERS)
    parser.add_argument("--cert-dir", type=Path, help="stable cert dir (mounted ro); needed by --check/--apply")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--lock-owner", help="host-measurement lock owner of this window: a lock held by this owner does not block --apply")
    args = parser.parse_args(argv)
    live_factory = live_factory or (lambda cert_dir: Live(AdminContainerRunner(cert_dir)))
    try:
        if args.prepare:
            os.umask(0o077)
            print(json.dumps({k: v for k, v in prepare(args.prepare).items() if k != "brokers"}, indent=1))
            return 0
        packet = args.check or args.roll or args.rollback
        receipt = load_packet(packet)
        if args.check:
            if args.cert_dir is None:
                raise Refused("--check needs --cert-dir")
            print(json.dumps(preconditions(live_factory(args.cert_dir), receipt), indent=1))
            return 0
        if args.broker is None:
            raise Refused("--roll/--rollback need --broker")
        which = "candidate" if args.roll else "rollback"
        if not args.apply:
            print(json.dumps({"mode": "dry-run", "broker": args.broker, "target": which,
                              "steps": recreate_argv(packet, args.broker, which),
                              "then": ["wait healthy", "wait zero under-replicated partitions",
                                       "verify env/fingerprint/broker config", "on failure: rollback compose"],
                              "undo": recreate_argv(packet, args.broker,
                                                    "rollback" if which == "candidate" else "candidate")},
                             indent=1))
            return 0
        if args.cert_dir is None:
            raise Refused("--apply needs --cert-dir")
        lock = foreign_lock(own_owner=args.lock_owner)
        if lock is not None:
            raise Refused(f"host-measurement lock active: {lock}")
        live = live_factory(args.cert_dir)
        try:
            report = transition(live, packet, receipt, args.broker, which)
        except (RuntimeError, subprocess.SubprocessError) as error:
            report = {"broker": args.broker, "target": which, "ok": False, "error": str(error)}
        if not report["ok"] and which == "candidate":
            try:
                for step in recreate_argv(packet, args.broker, "rollback"):
                    live.execute(step)
                restored = postconditions(live, receipt, args.broker, "rollback")
            except (RuntimeError, subprocess.SubprocessError) as error:
                restored = {"ok": False, "error": str(error)}
            report["rollback_postconditions"] = restored
            if restored["ok"]:
                report["auto_rollback"] = "RESTORED_VERIFIED"
            else:
                started = False
                try:
                    started = healthy(live.inspect(args.broker))
                except (RuntimeError, subprocess.SubprocessError, OSError):
                    pass
                report["auto_rollback"] = ("BROKER_STARTED_RESTORATION_UNVERIFIED" if started
                                           else "ROLLBACK_FAILED")
                print(json.dumps(report, indent=1))
                return 3
        print(json.dumps(report, indent=1))
        return 0 if report["ok"] else 1
    except Refused as error:
        print(json.dumps({"refused": str(error)}), file=sys.stderr)
        return 4
    except (RuntimeError, OSError, subprocess.SubprocessError) as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)
        return 5


if __name__ == "__main__":
    sys.exit(main())
