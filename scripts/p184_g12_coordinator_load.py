#!/usr/bin/env python3
"""P18.4 G12 decision C: coordinator state-load time vs internal-topic state, on a disposable 3-broker cluster.

Purpose: answer "how long does a new group / transaction coordinator take to load its partition, as a function of
what the partition holds", so the ``__consumer_offsets`` / ``__transaction_state`` compaction settings of
``scripts/kafka_internal_topic_compaction.py`` rest on a measured curve instead of the G12 harness's ~2.5 MB of
young state. The workload is the realtime core's commit shape (3 transactional producers of one consumer group,
2 input partitions each, one output record + ``send_offsets_to_transaction`` + ``commit_transaction`` per
transaction), run fast to age the internal topics (``--load``), with the same per-transaction latency log at any
cadence. A coordinator move is the auto-rebalance case: a warm follower becomes leader (replica order rewritten by
``kafka-reassign-partitions`` - same replica set, no data movement - then one preferred election); the broker's own
``Finished loading`` lines give the load time, the workload's commit latency gives the client-visible stall.

Reuse: namespace / label / holder image / Kafka image / disposable-container helpers of
``scripts/p184_g12_isolated_fault.py`` (``namespace``, ``LABEL``, ``IMAGES``, ``CLUSTER_ID``, ``BROKERS``,
``broker_port``, ``_base``, ``owned_names``, ``stop_role``, ``sh``). Differences from production, recorded in every
receipt: PLAINTEXT listeners (TLS costs replication CPU, not the coordinator's local replay), one partition per
internal topic (the measured partition is the only one), synthetic group/transactional ids.

Boundary: every container is ``--rm``, named ``<ns>-<role>``, labelled ``p184.g12=<ns>``, joined to the holder's
``--network none`` namespace (no Docker network, no published port); broker data lives under ``WORK`` and is
deleted by ``--down``. Nothing in the production project is touched. Broker caps equal production's
(``--cpus 1.75``, 1536 MiB, heap 512-768 MiB, G1, cleaner IO 4 MiB/s, one cleaner thread).

Host modes: ``--up WORK`` (holder + 3 brokers + topics), ``--load WORK`` (start/stop the workload container),
``--observe WORK`` (internal partition sizes, leaders), ``--move WORK`` (move one internal partition's leader and
report the load lines), ``--down WORK`` (label-checked teardown, data removed). In-container: ``--worker``.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
import re
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.kafka_preferred_leader_election import GROUP_LOAD_LINE, TXN_LOAD_LINE  # noqa: E402
from scripts.p184_g12_isolated_fault import (  # noqa: E402
    BROKERS, CLUSTER_ID, IMAGES, LABEL, _base, broker_port, namespace, owned_names, sh, stop_role)

PYTHON_IMAGE = "sha256:b9ca6c36fb0ad003b77e47bb3a777c3e565944d59cb3a2e9bc5dfb68e3448481"  # qdl-v2-python rc19
BOOTSTRAP = ",".join(f"localhost:{broker_port(n)}" for n in BROKERS)
INTERNAL = ("__consumer_offsets", "__transaction_state")
INPUT, OUTPUT, GROUP = "g12c.raw", "g12c.out", "g12c-core"
MEMBERS, PARTITIONS_PER_MEMBER = 3, 2
# Production broker shape (docker inspect 2026-10-16: NanoCpus 1.75e9, Memory 1.5-2 GiB; compose heap/GC).
BROKER_CAPS = {"cpus": "1.75", "memory": "1536m", "heap": "-Xms512m -Xmx768m"}
# Production internal-topic overrides (2026-10-16 describe) set on the isolated topics so the C tool runs unchanged.
INTERNAL_OVERRIDES = {"min.insync.replicas": "2", "retention.ms": "86400000",
                      "unclean.leader.election.enable": "false"}
LOAD_LINE, TXN_LINE = GROUP_LOAD_LINE, TXN_LOAD_LINE  # one parser for harness and production election


def broker_properties(node):
    port = broker_port(node)
    voters = ",".join(f"{n}@localhost:{broker_port(n) + 1}" for n in BROKERS)
    return f"""process.roles=broker,controller
node.id={node}
controller.quorum.voters={voters}
listeners=PLAINTEXT://:{port},CONTROLLER://:{port + 1}
advertised.listeners=PLAINTEXT://localhost:{port}
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=/data
log.segment.bytes=16777216
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
group.initial.rebalance.delay.ms=0
auto.leader.rebalance.enable=false
log.cleaner.io.max.bytes.per.second=4194304
log.cleaner.threads=1
"""


def broker_argv(ns, work, node, uid):
    argv = _base(ns, f"kafka-{node}", network=f"container:{ns}-net", memory=BROKER_CAPS["memory"],
                 cpus=BROKER_CAPS["cpus"], uid=uid)
    argv += ["--tmpfs", "/tmp:rw,exec,size=64m,mode=1777", "-v", f"{work}/kafka-{node}:/data",
             "-v", f"{work}/broker-{node}.properties:/work/broker.properties:ro"]
    for target in ("/etc/kafka/secrets", "/mnt/shared/config", "/var/lib/kafka/data"):
        argv += ["--tmpfs", target + ":rw,size=1m,mode=1777"]  # no anonymous volumes
    argv += ["-e", f"KAFKA_HEAP_OPTS={BROKER_CAPS['heap']}",
             "-e", "KAFKA_JVM_PERFORMANCE_OPTS=-XX:+UseG1GC -XX:MaxGCPauseMillis=100",
             "-e", "LOG_DIR=/tmp/kafka-logs"]
    return argv + ["--entrypoint", "/bin/sh", IMAGES["kafka"], "-ec",
                   f"/opt/kafka/bin/kafka-storage.sh format --ignore-formatted -t {CLUSTER_ID} "
                   f"-c /work/broker.properties && exec /opt/kafka/bin/kafka-server-start.sh /work/broker.properties"]


def kafka(ns, tool, *args, node=1, timeout=120, check=True):
    return sh(["docker", "exec", f"{ns}-kafka-{node}", f"/opt/kafka/bin/{tool}",
               "--bootstrap-server", f"localhost:{broker_port(node)}", *args], timeout=timeout, check=check)


def wait(predicate, timeout, what, period=2.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            if predicate():
                return
        except RuntimeError:
            pass
        time.sleep(period)
    raise RuntimeError("timeout: " + what)


def up(work, ns):
    namespace(ns)
    work = Path(work).resolve()
    if ROOT in work.parents or work == ROOT:
        raise ValueError("work directory must be outside the repository")
    work.mkdir(parents=True, exist_ok=False)
    uid = f"{os.getuid()}:{os.getgid()}"
    (work / "namespace").write_text(ns + "\n")
    sh(_base(ns, "net", network="none", memory="16m", cpus="0.05")
       + ["--hostname", "localhost", "--entrypoint", "sleep", IMAGES["holder"], "86400"])
    for node in BROKERS:
        (work / f"kafka-{node}").mkdir()
        (work / f"broker-{node}.properties").write_text(broker_properties(node))
        sh(broker_argv(ns, work, node, uid))
    wait(lambda: len(re.findall(r"id: \d", kafka(ns, "kafka-broker-api-versions.sh"))) >= 3, 180, "3 brokers")
    kafka(ns, "kafka-topics.sh", "--create", "--topic", INPUT, "--partitions", str(MEMBERS * PARTITIONS_PER_MEMBER),
          "--replication-factor", "3")
    kafka(ns, "kafka-topics.sh", "--create", "--topic", OUTPUT, "--partitions", "6", "--replication-factor", "3")
    return {"namespace": ns, "work": str(work), "caps": BROKER_CAPS, "images": {"kafka": IMAGES["kafka"],
            "holder": IMAGES["holder"], "python": PYTHON_IMAGE}}


def internal_overrides(ns):
    """After the first transaction created both internal topics: production's extra overrides."""
    for topic in INTERNAL:
        pairs = ",".join(f"{k}={v}" for k, v in INTERNAL_OVERRIDES.items())
        kafka(ns, "kafka-configs.sh", "--entity-type", "topics", "--entity-name", topic, "--alter",
              "--add-config", pairs)


def load_argv(ns, work, workers, seconds, rate, tag):
    uid = f"{os.getuid()}:{os.getgid()}"
    out = f"/out/{tag}"
    (Path(work) / "load" / tag).mkdir(parents=True, exist_ok=False)
    return _base(ns, f"load-{tag}", network=f"container:{ns}-net", memory="768m", cpus="2.0", uid=uid) + [
        "--tmpfs", "/tmp:rw,size=16m,mode=1777", "-v", f"{work}/load:/out",
        # the whole scripts/ directory: the module's host-side imports (scripts.p184_g12_isolated_fault, ...)
        # run at import time in the worker too
        "-v", f"{ROOT / 'scripts'}:/harness/scripts:ro",
        "-e", "PYTHONDONTWRITEBYTECODE=1", "--entrypoint", "python", PYTHON_IMAGE, "-B",
        "/harness/scripts/p184_g12_coordinator_load.py", "--worker", out, "--workers", str(workers),
        "--seconds", str(seconds), "--rate", str(rate)]


# ------------------------------------------------------------------------------------------------ in-container


def worker_loop(out, index, members, seconds, rate):
    """One core-like member: its own transactional.id, 2 input partitions, 1 output record per transaction."""
    from confluent_kafka import Consumer, Producer, TopicPartition  # in-container only

    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": GROUP, "enable.auto.commit": False})
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "transactional.id": f"g12c-core-{Path(out).name}-{index}",
                         "linger.ms": 0, "enable.idempotence": True,
                         # back-to-back transactions wait on client retries: measured 2026-10-16 ~110 ms per
                         # transaction with librdkafka's default 100 ms backoff, ~33 ms with 5 ms - too slow to age
                         # the internal topics otherwise. Same requests and records, shorter client-side wait.
                         "retry.backoff.ms": 5})
    producer.init_transactions(30)
    member = index % members
    parts = [member * PARTITIONS_PER_MEMBER + k for k in range(PARTITIONS_PER_MEMBER)]
    deadline, period = time.monotonic() + seconds, (1.0 / rate if rate > 0 else 0.0)
    offset, rows, next_at = int(time.time()), [], time.monotonic()
    with open(f"{out}/worker-{index}.jsonl", "a", encoding="utf-8") as log:
        while time.monotonic() < deadline:
            if period:
                next_at += period
                pause = next_at - time.monotonic()
                if pause > 0:
                    time.sleep(pause)
            offset += 1
            started = time.time_ns()
            try:
                producer.begin_transaction()
                producer.produce(OUTPUT, value=b"x" * 64, partition=parts[0] % 6)
                positions = [TopicPartition(INPUT, p, offset) for p in parts]
                producer.send_offsets_to_transaction(positions, consumer.consumer_group_metadata(), 30)
                producer.commit_transaction(30)
                status = "ok"
            except Exception as error:  # noqa: BLE001 - a failed transaction is a measurement, not a crash
                status = type(error).__name__ + ":" + str(error)[:160]
                try:
                    producer.abort_transaction(30)
                except Exception:  # noqa: BLE001
                    pass
            done = time.time_ns()
            log.write(json.dumps({"t": started, "ms": round((done - started) / 1e6, 2), "s": status}) + "\n")
            if status != "ok":
                log.flush()
    consumer.close()


def worker_main(out, workers, seconds, rate):
    from multiprocessing import Process
    procs = [Process(target=worker_loop, args=(out, i, MEMBERS, seconds, rate)) for i in range(workers)]
    for proc in procs:
        proc.start()
    for proc in procs:
        proc.join()
    return 0


# ------------------------------------------------------------------------------------------------ observe / move


def log_dirs(ns):
    text = kafka(ns, "kafka-log-dirs.sh", "--describe", "--topic-list", ",".join(INTERNAL), timeout=60)
    body = json.loads(next(line for line in text.splitlines() if line.startswith("{")))
    sizes = {}
    for broker in body["brokers"]:
        for log_dir in broker["logDirs"]:
            for part in log_dir["partitions"]:
                sizes.setdefault(part["partition"], {})[broker["broker"]] = part["size"]
    return sizes


def end_offsets(ns):
    text = kafka(ns, "kafka-get-offsets.sh", "--topic-partitions", "__consumer_offsets:0,__transaction_state:0",
                 "--time", "-1", timeout=60)
    early = kafka(ns, "kafka-get-offsets.sh", "--topic-partitions", "__consumer_offsets:0,__transaction_state:0",
                  "--time", "-2", timeout=60)
    parse = lambda t: {f"{a}-{b}": int(c) for a, b, c in (line.split(":") for line in t.split() if line.count(":") == 2)}
    return {"end": parse(text), "start": parse(early)}


def leaders(ns):
    text = kafka(ns, "kafka-topics.sh", "--describe", "--topic", ",".join(INTERNAL), timeout=60)
    found = {}
    for line in text.splitlines():
        m = re.search(r"Topic:\s*(\S+)\s+Partition:\s*(\d+)\s+Leader:\s*(-?\d+)\s+Replicas:\s*([\d,]+)\s+Isr:\s*([\d,]*)",
                      line)
        if m:
            found[f"{m.group(1)}-{m.group(2)}"] = {"leader": int(m.group(3)), "replicas": m.group(4),
                                                   "isr": m.group(5)}
    return found


def observe(ns):
    return {"at_ns": time.time_ns(), "sizes": log_dirs(ns), "offsets": end_offsets(ns), "leaders": leaders(ns)}


def container_logs(ns, node, since):
    """stdout + stderr of one owned broker since an RFC3339 time (Kafka logs to the console)."""
    result = subprocess.run(["docker", "logs", "--since", since, f"{ns}-kafka-{node}"], capture_output=True,
                            text=True, timeout=60)
    return result.stdout + result.stderr


def load_lines(text, topic):
    pattern = LOAD_LINE if topic == "__consumer_offsets" else TXN_LINE
    return [m.groupdict() for m in pattern.finditer(text)]


def move(ns, work, topic, to):
    if topic not in INTERNAL or to not in BROKERS:
        raise ValueError("topic must be internal, to must be a broker id")
    order = [to] + [n for n in BROKERS if n != to]
    plan = {"version": 1, "partitions": [{"topic": topic, "partition": 0, "replicas": order}]}
    path = Path(work) / "move.json"
    path.write_text(json.dumps(plan))
    # brokers run --read-only (docker cp refuses even a tmpfs target): stream the plan into the /tmp tmpfs
    subprocess.run(["docker", "exec", "-i", f"{ns}-kafka-1", "sh", "-c", "cat > /tmp/move.json"],
                   input=path.read_text(), text=True, capture_output=True, timeout=60, check=True)
    since = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(time.time() - 1))
    before = leaders(ns)[f"{topic}-0"]
    kafka(ns, "kafka-reassign-partitions.sh", "--reassignment-json-file", "/tmp/move.json", "--execute")
    wait(lambda: "complete" in kafka(ns, "kafka-reassign-partitions.sh", "--reassignment-json-file",
                                     "/tmp/move.json", "--verify"), 60, "reassignment")
    started = time.time_ns()
    kafka(ns, "kafka-leader-election.sh", "--election-type", "preferred", "--topic", topic, "--partition", "0")
    wait(lambda: leaders(ns)[f"{topic}-0"]["leader"] == to, 60, "leader moved", period=0.5)
    found = []
    end = time.monotonic() + 600
    while not found and time.monotonic() < end:
        found = load_lines(container_logs(ns, to, since), topic)
        if not found:
            time.sleep(1)
    return {"topic": topic, "from": before["leader"], "to": to, "election_ns": started, "load": found[:1]}


def down(work):
    work = Path(work).resolve()
    ns = namespace((work / "namespace").read_text().strip())
    removed = []
    for name in sorted(owned_names(ns), key=lambda n: n.endswith("-net")):
        role = name[len(ns) + 1:]
        if stop_role(ns, role, timeout=30):
            removed.append(role)
    left = owned_names(ns)
    for node in BROKERS:
        shutil.rmtree(work / f"kafka-{node}", ignore_errors=True)
    return {"namespace": ns, "removed": removed, "left": left}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    for flag in ("up", "load", "observe", "move", "down", "worker"):
        mode.add_argument("--" + flag, type=Path, metavar="DIR")
    parser.add_argument("--namespace")
    parser.add_argument("--workers", type=int, default=MEMBERS)
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--rate", type=float, default=0, help="transactions/s per worker; 0 = as fast as possible")
    parser.add_argument("--tag")
    parser.add_argument("--topic", choices=INTERNAL)
    parser.add_argument("--to", type=int)
    parser.add_argument("--overrides", action="store_true", help="--load: set production's internal overrides")
    args = parser.parse_args(argv)
    if args.worker:
        return worker_main(str(args.worker), args.workers, args.seconds, args.rate)
    if args.up:
        print(json.dumps(up(args.up, args.namespace)))
        return 0
    work = Path(args.load or args.observe or args.move or args.down).resolve()
    if args.down:
        print(json.dumps(down(work)))
        return 0
    ns = namespace((work / "namespace").read_text().strip())
    if args.load:
        if args.overrides:
            internal_overrides(ns)
            print(json.dumps({"overrides": INTERNAL_OVERRIDES}))
            return 0
        sh(load_argv(ns, work, args.workers, args.seconds, args.rate, args.tag))
        print(json.dumps({"started": f"{ns}-load-{args.tag}", "workers": args.workers, "seconds": args.seconds,
                          "rate": args.rate}))
        return 0
    if args.observe:
        print(json.dumps(observe(ns)))
        return 0
    print(json.dumps(move(ns, work, args.topic, args.to)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
