#!/usr/bin/env python3
"""Paired real-capture replay through existing core images, isolated Kafka only.

No builds or shared writes. Raw captures/output are temporary and deleted after
hashing. Message rates use provider-window seconds, not accelerated replay time.
"""
from __future__ import annotations
import argparse
import asyncio
import base64
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import threading
import time

BASELINE = "sha256:658a9570c5fc23f4906413aa2460f4d82e21772cc63c9ed900363d4d13d54023"
CANDIDATE = "sha256:69e3b2b214331a7ebb6a110dd8159c899bbbd6439aaf87293c8c68d145e3713d"
PYTHON = "sha256:77682eea5054ae9ddbd858323b083286669a55eeb4bc7bc065b04bae8854e32f"
KAFKA = "apache/kafka@sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9"
ROOT = Path(__file__).resolve().parents[1]
PREFIX = "qdl-idr062-pair"


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def distribution(values):
    values = sorted(values)
    if not values:
        return {"n": 0, "unit": "ms", "p50": None, "p95": None, "max": None}
    return {"n": len(values), "unit": "ms", "p50": values[math.ceil(.5 * len(values))-1],
            "p95": values[math.ceil(.95 * len(values))-1], "max": values[-1]}


def top_observations(capture):
    """Provider sequence oracle, no approximation of the core publication policy."""
    bids, asks = {}, {}
    sequence = None
    buffered = []
    observations = []
    venue = capture["binding"]["venue"]
    def apply(levels, side):
        for level in levels:
            price, qty = Decimal(level[0]), Decimal(level[1])
            if qty == 0:
                side.pop(price, None)
            else:
                side[price] = qty
    def emit(seq, stamp):
        if not bids or not asks or max(bids) >= min(asks):
            raise ValueError("oracle empty/crossed book")
        observations.append({"seq": str(seq), "ms": stamp,
                             "top": (str(max(bids).normalize()), str(min(asks).normalize()))})
    def delta(frame):
        nonlocal sequence
        end = int(frame["u"])
        if end <= sequence:
            return
        if not observations:
            if not (int(frame["U"]) <= sequence + 1 <= end or int(frame["pu"]) == sequence):
                raise ValueError("oracle Binance bridge gap")
        elif int(frame["pu"]) != sequence:
            raise ValueError("oracle Binance sequence gap")
        apply(frame["b"], bids); apply(frame["a"], asks)
        sequence = end
        emit(sequence, int(frame["E"]))
    for row in capture["rows"]:
        frame = json.loads(row["raw"])
        if venue == "BINANCE":
            if row["protocol"] == "HTTP":
                bids.clear(); asks.clear()
                apply(frame["bids"], bids); apply(frame["asks"], asks)
                sequence = int(frame["lastUpdateId"])
                for item in buffered:
                    delta(item)
                buffered.clear()
            elif sequence is None:
                buffered.append(frame)
            else:
                delta(frame)
        else:
            for item in frame["data"]:
                if frame["action"] == "snapshot":
                    bids.clear(); asks.clear()
                elif int(item["prevSeqId"]) != sequence:
                    raise ValueError("oracle OKX sequence gap")
                apply(item["bids"], bids); apply(item["asks"], asks)
                sequence = int(item["seqId"])
                emit(sequence, int(item["ts"]))
    return observations


def top_delay(observations, snapshots):
    """Matching top at its own source sequence; count all superseded intervals."""
    by_seq = {r["seq"]: r for r in observations}
    emitted = {}
    for snap in snapshots:
        row = by_seq.get(snap["seq"])
        if row is None or tuple(snap["top"]) != tuple(row["top"]):
            raise ValueError("snapshot differs from exact provider top/sequence")
        emitted[(snap["seq"], snap.get("ms", row["ms"]))] = snap
    delays, pending, last = [], None, None
    coverage_delays, uncovered = [], []
    changed = superseded = 0
    for row in observations:
        if last is not None and row["top"] != last:
            changed += 1
            uncovered.append(row["ms"])
            if pending is not None:
                superseded += 1
            pending = row["ms"]
        if (row["seq"], row["ms"]) in emitted:
            coverage_delays.extend(row["ms"] - stamp for stamp in uncovered)
            uncovered.clear()
        if pending is not None and (row["seq"], row["ms"]) in emitted:
            delay = row["ms"] - pending
            if delay < 0:
                raise ValueError("provider clock regressed")
            delays.append(delay)
            pending = None
        last = row["top"]
    return {"matched_delay": distribution(delays), "top_changes": changed,
            "next_verified_snapshot_delay": distribution(coverage_delays),
            "next_snapshot_uncovered_tail": len(uncovered),
            "coverage_semantics": "first snapshot at same or later sequence; superseded tops coalesce, not exact historical price delivery",
            "superseded_before_snapshot": superseded, "unmatched_tail": int(pending is not None),
            "denominator_check": changed == len(delays) + superseded + int(pending is not None),
            "scope": "provider event time to snapshot materialization source time, NOT wall-clock consumer latency"}


def client_capture(work, seconds):
    from scripts.phase114_l2_real_provider_capture import capture_replay_window
    cfg = json.loads((work / "core.json").read_text())
    result = asyncio.run(capture_replay_window(cfg["core"]["bindings"], seconds))
    for cap in result["captures"]:
        if len(top_observations(cap)) < 2:
            raise ValueError("no continuous oracle window")
    write(work / "capture.json", result)


def client_replay(work):
    from confluent_kafka import Producer, Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
    from qdl.provider.v1.raw_provider_pb2 import RawProviderEnvelope
    from qdl.marketdata.v2.market_data_pb2 import EventEnvelope
    security = {"bootstrap.servers": "kafka:9092", "security.protocol": "SSL",
                "ssl.ca.location": "/work/ca.crt", "ssl.certificate.location": "/work/client.crt",
                "ssl.key.location": "/work/client.key"}
    admin = AdminClient(security)
    for future in admin.create_topics([NewTopic(n, 1, 1) for n in ("idr.raw", "idr.canonical", "idr.quarantine")]).values():
        future.result(30)
    capture = json.loads((work / "capture.json").read_text())
    rows = sorted([(r["received_at_ns"], cap["binding"], r) for cap in capture["captures"] for r in cap["rows"]], key=lambda x:x[0])
    cfg = json.loads((work / "core.json").read_text()); cfg["max_events"] = len(rows); write(work / "core.json", cfg)
    (work / "client-ready").touch()
    deadline = time.monotonic() + 90
    while not (work / "core-ready").exists():
        if time.monotonic() > deadline: raise TimeoutError("core orchestration")
        time.sleep(.1)
    producer = Producer(dict(security, **{"enable.idempotence": True, "acks": "all", "linger.ms": 0}))
    errors = []
    def delivery(error, _):
        if error: errors.append(str(error))
    def publish():
        try:
            start = time.monotonic()
            for idx, (ns, b, row) in enumerate(rows):
                time.sleep(max(0, (ns - rows[0][0])/1e9 - (time.monotonic()-start)))
                raw = row["raw"].encode(); sha = hashlib.sha256(raw).digest()
                env = RawProviderEnvelope(raw_schema_name="qdl.provider.raw", raw_schema_major=1,
                    capture_id=hashlib.sha256(str(idx).encode()+sha).digest()[:16], provider=b["provider"], venue=b["venue"],
                    market=b["market"], product_type=b["product_type"], native_symbol=b["native_symbol"],
                    native_channel=b["native_channel"], subscription_id=b["source_id"],
                    source_session_id="idr062-offline-"+b["venue"], connection_generation=1, lease_epoch=1,
                    authority_revision=1, partition_plan_epoch=1, received_at_ns=ns,
                    transport_protocol=2 if row["protocol"]=="HTTP" else 1, transport_compression=1,
                    capture_boundary=2, raw_frame_bytes=raw, raw_frame_sha256=sha,
                    adapter_version="idr062-real-capture/1", config_revision=1,
                    instrument_catalog_revision=b["instrument_catalog_revision"], correlation_id=str(idx), test_provenance=False)
                producer.produce("idr.raw", partition=0, key=b["source_id"].encode(), value=env.SerializeToString(), headers=[("qdl-event-id", env.capture_id)], callback=delivery)
                producer.poll(0)
            if producer.flush(15): errors.append("producer flush incomplete")
        except Exception as exc:
            errors.append(str(exc))
    worker = threading.Thread(target=publish); worker.start()
    consumer = Consumer(dict(security, **{"group.id":"idr062-observer", "enable.auto.commit":False,
        "isolation.level":"read_committed", "auto.offset.reset":"earliest"}))
    consumer.assign([TopicPartition("idr.canonical",0,0), TopicPartition("idr.quarantine",0,0)])
    output = []; last = time.monotonic(); deadline = last + capture["seconds"] + 75
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(.1)
            if msg is None:
                if not worker.is_alive() and time.monotonic()-last > 8: break
                continue
            if msg.error(): raise RuntimeError(msg.error())
            last = time.monotonic()
            output.append({"topic":msg.topic(), "bytes":base64.b64encode(msg.value()).decode()})
    finally:
        worker.join(20); consumer.close()
    if worker.is_alive() or errors: raise RuntimeError(errors or "publisher alive")
    write(work / "output.json", output)


def analyse(work):
    from qdl.marketdata.v2.market_data_pb2 import EventEnvelope
    capture = json.loads((work/"capture.json").read_text())
    output = json.loads((work/"output.json").read_text())
    rows = {}
    quarantines = []
    for item in output:
        if item["topic"] != "idr.canonical":
            from qdl.provider.v1.raw_provider_pb2 import QuarantineRecord, QUARANTINE_REASON_SEQUENCE_GAP
            quarantines.append(QuarantineRecord.FromString(base64.b64decode(item["bytes"])))
            continue
        payload = base64.b64decode(item["bytes"]); event = EventEnvelope.FromString(payload)
        key = event.venue+"/"+event.native_symbol
        entry = rows.setdefault(key, {"snapshots":[], "snapshot_bytes":0, "deltas":0, "delta_hashes":[]})
        kind = event.WhichOneof("payload")
        if kind == "book_snapshot":
            snap = event.book_snapshot
            if not snap.sequence_verified: raise ValueError("unverified snapshot")
            levels = {1:[], 2:[]}
            for level in snap.levels:
                p = level.price
                value = Decimal(p.mantissa_text if p.WhichOneof("coefficient")=="mantissa_text" else p.mantissa).scaleb(-p.scale)
                levels[level.side].append(value)
            entry.setdefault("bootstrap_received_at_ns", event.received_at_ns)
            entry["snapshots"].append({"seq":snap.native_sequence,"ms":event.source_event_time_ns//1000000,
                "top":[str(max(levels[1]).normalize()), str(min(levels[2]).normalize())]})
            entry["snapshot_bytes"] += len(payload)
        elif kind == "book_delta":
            entry["deltas"] += 1
            entry["delta_hashes"].append(hashlib.sha256(event.book_delta.SerializeToString()).hexdigest())
    for q in quarantines:
        key = q.raw.venue+"/"+q.raw.native_symbol
        entry = rows[key]
        if q.reason != QUARANTINE_REASON_SEQUENCE_GAP or q.safe_summary != "L2_RESNAPSHOT_REQUIRED_V1" or q.raw.received_at_ns > entry["bootstrap_received_at_ns"]:
            raise ValueError(f"non-bootstrap quarantine: {q.reason} {q.safe_summary}, raw={q.raw.received_at_ns}, bootstrap={entry['bootstrap_received_at_ns']}")
        entry["bootstrap_resnapshot_requests"] = entry.get("bootstrap_resnapshot_requests", 0) + 1
    for cap in capture["captures"]:
        key = cap["binding"]["venue"]+"/"+cap["binding"]["native_symbol"]
        entry = rows[key]; obs = top_observations(cap)
        elapsed = (obs[-1]["ms"]-obs[0]["ms"])/1000
        if elapsed <= 0: raise ValueError("empty provider duration")
        entry["provider_seconds"] = elapsed
        entry["captured_frames"] = len(cap["rows"])
        entry["provider_observations"] = len(obs)
        entry["received_start_ns"] = cap["rows"][0]["received_at_ns"]
        entry["received_end_ns"] = cap["rows"][-1]["received_at_ns"]
        entry["snapshot_count"] = len(entry["snapshots"])
        entry["snapshot_messages_per_second"] = entry["snapshot_count"]/elapsed
        entry["snapshot_protobuf_bytes_per_second"] = entry["snapshot_bytes"]/elapsed
        entry["top_delay"] = top_delay(obs, entry.pop("snapshots"))
        entry["delta_payload_chain_sha256"] = hashlib.sha256("".join(entry.pop("delta_hashes")).encode()).hexdigest()
    write(work/"metrics.json", rows)


def command(args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()


def stop_disposable(name):
    if not name.startswith(PREFIX + "-"):
        raise ValueError("refusing non-test container")
    subprocess.run(["docker", "stop", "-t", "3", name], capture_output=True)
    deadline = time.monotonic() + 20
    while subprocess.run(["docker", "inspect", name], capture_output=True).returncode == 0:
        if time.monotonic() > deadline:
            raise TimeoutError("disposable auto-remove did not finish: " + name)
        time.sleep(.2)


def orchestrate(work, seconds):
    work.mkdir(parents=True, exist_ok=False); work.chmod(0o755)
    runtime = json.loads(command(["docker","inspect","qdl_v2_stable_candidate-rust_core-1"]))[0]
    if runtime["Image"] != BASELINE: raise ValueError("deployed baseline drift")
    cfg = json.loads(command(["docker","exec",runtime["Id"],"cat","/runtime/core.json"]))
    cfg["core"]["bindings"] = [b for b in cfg["core"]["bindings"] if b.get("l2") and b["native_symbol"] in {"ETHUSDT","ETH-USDT-SWAP"}]
    if len(cfg["core"]["bindings"]) != 2: raise ValueError("expected two active ETH BOOK bindings")
    cfg["core"].update(canonical_stream="idr.canonical", quarantine_stream="idr.quarantine")
    cfg.update(raw_topics=["idr.raw"], transactional_id="idr062-core", shard_id="idr062-core", metrics_every_batches=10)
    cfg["authority"].update(mode="RUST_SHADOW",slice_id="idr062-offline",approved_by="BobbyAxerol offline replay")
    write(work/"core.json",cfg)
    before = {x["Name"]:{"image":x["Image"],"started":x["State"]["StartedAt"],"restarts":x["RestartCount"]}
              for x in json.loads(command(["docker","inspect",*command(["docker","ps","-aq","--filter","name=qdl_v2_stable_candidate-"]).splitlines()]))}
    write(work/"runtime-before.json",before)
    disk_before = shutil.disk_usage(work).free
    def client(mode, network, detach=False):
        args=["docker","run","--rm","--name",PREFIX+("-client" if mode=="replay" else "-"+mode),"--user",str(os.getuid()),"--cpus","1","--memory","512m",
              "--network",network,"--tmpfs","/tmp:rw,exec,size=32m","--tmpfs","/app/logs:rw,uid=1001",
              "-e","PYTHONDONTWRITEBYTECODE=1","-e","PYTHONPATH=/src:/src/generated/python",
              "-v",str(ROOT)+":/src:ro","-v",str(work)+":/work","-w","/src","--entrypoint","python"]
        if detach: args.insert(3,"-d")
        return command(args+[PYTHON,"-B","scripts/measure_l2_top_materialization.py",mode,"--work","/work","--seconds",str(seconds)])
    client("capture","bridge")
    capture_hash=digest(work/"capture.json")
    # Fresh self-signed test identity only; never read production TLS.
    subprocess.run(["openssl","req","-x509","-newkey","rsa:2048","-nodes","-days","1","-subj","/CN=kafka",
        "-addext","subjectAltName=DNS:kafka","-keyout",str(work/"client.key"),"-out",str(work/"ca.crt")],check=True,capture_output=True)
    shutil.copyfile(work/"ca.crt",work/"client.crt")
    (work/"client.key").chmod(0o644)
    subprocess.run(["openssl","pkcs12","-export","-in",str(work/"ca.crt"),"-inkey",str(work/"client.key"),
        "-out",str(work/"server.p12"),"-passout","pass:idr062-test-only"],check=True,capture_output=True)
    (work/"server.p12").chmod(0o644)
    properties = """process.roles=broker,controller
node.id=1
controller.quorum.voters=1@kafka:9093
listeners=SSL://:9092,CONTROLLER://:9093
advertised.listeners=SSL://kafka:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=SSL:SSL,CONTROLLER:PLAINTEXT
inter.broker.listener.name=SSL
ssl.keystore.type=PKCS12
ssl.keystore.location=/work/server.p12
ssl.keystore.password=idr062-test-only
ssl.key.password=idr062-test-only
ssl.truststore.type=PEM
ssl.truststore.location=/work/ca.crt
ssl.client.auth=required
log.dirs=/data
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
"""
    (work/"broker.properties").write_text(properties)
    result={"capture_sha256":capture_hash,"source":command(["git","-C",str(ROOT),"rev-parse","HEAD"]),
            "baseline":BASELINE,"candidate":CANDIDATE,"runs":{},"resource_scope":"core container only, 1 CPU / 256 MiB, paced same capture"}
    command(["docker","network","create","--internal",PREFIX])
    try:
        for label,image in [("baseline",BASELINE),("candidate",CANDIDATE)]:
            for f in ["client-ready","core-ready"]: (work/f).unlink(missing_ok=True)
            broker=PREFIX+"-kafka";core=PREFIX+"-core"
            command(["docker","run","--rm","-d","--name",broker,"--network",PREFIX,"--network-alias","kafka",
                "--cpus","1","--memory","1g","--tmpfs","/data:rw,size=512m,mode=1777",
                "-e","KAFKA_HEAP_OPTS=-Xms256m -Xmx512m","-v",str(work)+":/work:ro","--entrypoint","/bin/sh",KAFKA,"-c",
                "/opt/kafka/bin/kafka-storage.sh format -t MkU3OEVBNTcwNTJENDM2Qk -c /work/broker.properties && exec /opt/kafka/bin/kafka-server-start.sh /work/broker.properties"])
            try:
                time.sleep(12)
                client("replay",PREFIX,True)
                end=time.monotonic()+45
                while not (work/"client-ready").exists():
                    if time.monotonic()>end: raise TimeoutError("replay client ready")
                    time.sleep(.2)
                args=["docker","run","--rm","-d","--name",core,"--network",PREFIX,"--cpus","1","--memory","256m",
                    "-v",str(work)+":/work:ro","--entrypoint","/bin/sh"]
                for k,v in {"QDL_KAFKA_CERT_ROOT":"/work","QDL_KAFKA_BOOTSTRAP_SERVERS":"kafka:9092",
                    "QDL_KAFKA_CLIENT_ID":"idr062-core","QDL_KAFKA_GROUP_ID":"idr062-core"}.items(): args += ["-e",k+"="+v]
                command(args+[image,"-c","/usr/local/bin/qdl-realtime-core /work/core.json; code=$?; echo IDR062_EXIT=$code; sleep 45; exit $code"])
                time.sleep(2);(work/"core-ready").touch()
                samples=[];end=time.monotonic()+seconds+65
                while time.monotonic()<end:
                    stats=command(["docker","exec",core,"sh","-c","cat /sys/fs/cgroup/cpu.stat; cat /sys/fs/cgroup/memory.current; for p in /proc/[0-9]*/status; do if grep -q 'Name:.*qdl-realtime' $p; then grep -E 'VmRSS|VmHWM' $p; echo CORE_TICKS=$(cut -d ' ' -f 14,15 ${p%status}stat); fi; done"])
                    samples.append({"monotonic":time.monotonic(),"stats":stats})
                    logs=command(["docker","logs",core],stderr=subprocess.STDOUT)
                    if "IDR062_EXIT=" in logs: break
                    time.sleep(.5)
                else: raise TimeoutError("bounded core replay")
                if "IDR062_EXIT=0" not in logs: raise RuntimeError(logs[-2000:])
                (work/(label+"-core.log")).write_text(logs)
                write(work/(label+"-resource.json"),samples)
                def field(text, name):
                    return next((int(line.split()[1]) for line in text.splitlines() if line.startswith(name+" ") or line.startswith(name+":")), 0)
                ticks = [sum(map(int, line.split("=",1)[1].split())) for sample in samples for line in sample["stats"].splitlines() if line.startswith("CORE_TICKS=") and line.split("=",1)[1].strip()]
                cpu_s = max(ticks, default=0)/os.sysconf("SC_CLK_TCK")
                elapsed = samples[-1]["monotonic"]-samples[0]["monotonic"]
                resources = {"samples":len(samples), "sample_period_seconds":.5,
                    "core_process_cpu_seconds_observed":cpu_s,
                    "core_vcpu_per_capture_second":cpu_s/seconds,
                    "sampled_process_rss_high_water_kib":max(field(x["stats"],"VmHWM") for x in samples),
                    "sampled_wall_seconds":elapsed,
                    "container_cpu_seconds_including_probe_exec":(field(samples[-1]["stats"],"usage_usec")-field(samples[0]["stats"],"usage_usec"))/1e6}
                code=command(["docker","wait",PREFIX+"-client"])
                if code != "0": raise RuntimeError("replay client exit "+code)
                client("analyse","none")
                result["runs"][label]={"metrics":json.loads((work/"metrics.json").read_text()),"output_sha256":digest(work/"output.json"),"config_sha256":digest(work/"core.json"),"resources":resources}
                shutil.copyfile(work/"metrics.json",work/(label+"-metrics.json"))
                (work/"output.json").unlink()
            finally:
                for name in [PREFIX+"-client",core,broker]:
                    stop_disposable(name)
        for key,b in result["runs"]["baseline"]["metrics"].items():
            c=result["runs"]["candidate"]["metrics"][key]
            if b["delta_payload_chain_sha256"]!=c["delta_payload_chain_sha256"]: raise ValueError("delta parity failed")
            c["estimated_snapshot_work_multiplier"]=c["snapshot_count"]/b["snapshot_count"]
            c["estimated_total_book_messages_multiplier"]=(c["snapshot_count"]+c["deltas"])/(b["snapshot_count"]+b["deltas"])
        write(work/"paired-receipt.json",result)
    finally:
        for name in [PREFIX+"-client",PREFIX+"-core",PREFIX+"-kafka"]:
            stop_disposable(name)
        subprocess.run(["docker","network","rm",PREFIX],capture_output=True)
        for name in ["capture.json","output.json","client.key","client.crt","ca.crt","server.p12"]:
            (work/name).unlink(missing_ok=True)
        after={x["Name"]:{"image":x["Image"],"started":x["State"]["StartedAt"],"restarts":x["RestartCount"]}
               for x in json.loads(command(["docker","inspect",*command(["docker","ps","-aq","--filter","name=qdl_v2_stable_candidate-"]).splitlines()]))}
        write(work/"cleanup.json",{"runtime_unchanged":before==after,"disk_free_before":disk_before,"disk_free_after":shutil.disk_usage(work).free,
            "capture_sha256":capture_hash,"capture_deleted":not(work/"capture.json").exists()})


def main():
    p=argparse.ArgumentParser();p.add_argument("mode",choices=["run","capture","replay","analyse"])
    p.add_argument("--work",type=Path,required=True);p.add_argument("--seconds",type=float,default=60)
    a=p.parse_args()
    if a.mode=="run": orchestrate(a.work,a.seconds)
    elif a.mode=="capture": client_capture(a.work,a.seconds)
    elif a.mode=="replay": client_replay(a.work)
    else: analyse(a.work)

if __name__=="__main__": main()
