#!/usr/bin/env python3
"""KN-4 D38: mirror committed canonical records into an ISOLATED shadow broker.

Purpose: the live input of a KN shadow is the production canonical Kafka log,
not the SQLite spool. This tool reads ``md.canonical.v2`` ``read_committed`` in
assign mode (no group join, no offset commit, no auto offset store: the
production consumer groups and offsets are untouched) and re-publishes every
committed record of a bundle product to the SAME partition of the isolated
broker in bounded transactions, so the shadow projectors keep reading
``read_committed``. Aborted records and markers never leave a
``read_committed`` fetch, so per partition the shadow holds the committed
canonical sequence in source order.

Continuity (D38): ``start`` resolves per-partition source offsets for a
timestamp (``offsets_for_times``) and writes them to a file BEFORE the history
import runs; ``run`` starts exactly there. Import and mirror overlap (stage A/B
treat the overlap as duplicates), so history joins the tail by a source
watermark, never by a wall-clock restart.

Filter: stage A stops on a record outside the bundle (``products.rs``), so only
records whose Kafka key is a bundle physical key and whose payload feed is
bound for that key are forwarded; the rest are counted by reason, never sent.

Provenance: every mirrored record keeps its key, value, headers and Kafka
timestamp, plus ``qdl-mirror-source-partition``/``-offset``/``-timestamp``
headers. ``--commit-log`` gets one line per mirrored record
``{partition, offset, commit_ns, source_offset, source_timestamp_ms,
source_timestamp_type}`` (the probe's commit-log shape plus the source clock),
so a latency report never has to pose a receive time as a commit time. The log
is bounded: past ``--commit-log-max-bytes`` it rotates to ``<log>.1`` (the
previous ``.1`` is dropped), so it never holds more than twice the bound.

Boundary: reads only ``--source-bootstrap``; writes only ``--dest-bootstrap``,
which must be a plaintext broker that is not a stable production broker
(``kafka1..3``, ``qdl_v2_stable_candidate*``). Bounded by ``--deadline-seconds``
and ``--max-bytes-per-second``; exits cleanly at the deadline or on SIGTERM.
Prints no secret (TLS file paths only).

  python -B scripts/kn_canonical_mirror.py start --source-bootstrap kafka1:9092,... \\
      --ca ca.crt --cert client.crt --key client.key --since-seconds 5400 --out start.json
  python -B scripts/kn_canonical_mirror.py run --source-bootstrap ... --ca ... --cert ... --key ... \\
      --start start.json --bundle bundle.json --dest-bootstrap kn4-kafka:9092 \\
      --deadline-seconds 14400 --commit-log commit-log.jsonl
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import signal
import sys
import time
import uuid
from typing import Any, Callable, Iterable

TOPIC = "md.canonical.v2"
PRODUCTION_HOSTS = frozenset({"kafka1", "kafka2", "kafka3"})
PRODUCTION_PROJECT = "qdl_v2_stable_candidate"
# Group namespaces the mirror may name. librdkafka always looks up the group
# coordinator (FindCoordinator) even in assign mode, so the principal needs READ
# on the group; the stable broker grants the projector principal exactly the
# prefixed read-only audit namespaces of bounded control-plane readers
# (`phaseb_bootstrap_stable_broker.py` READ_ONLY_AUDIT_GROUP_PREFIXES; C40's
# live handoff collector uses `qdl-c40-handoff-`). A unique id under one of
# them is never joined and never committed. The default is for isolated tests.
GROUP_PREFIXES = ("kn-shadow-mirror-", "qdl-c40-handoff-")
MIRROR_HEADERS = ("qdl-mirror-source-partition", "qdl-mirror-source-offset", "qdl-mirror-source-timestamp")
PARTITION_EOF = -191  # librdkafka _PARTITION_EOF: informational


class RotatingLineLog:
    """Append-only line log bounded to two files of ``max_bytes``: the live
    file rotates to ``<path>.1`` (replacing the previous one) when it passes
    the bound. Line-buffered, so a killed mirror keeps every line it logged."""

    def __init__(self, path: str | Path, max_bytes: int) -> None:
        if max_bytes < 1:
            raise ValueError("commit log bound must be positive")
        self.path = Path(path)
        self.max_bytes = max_bytes
        self.handle = open(self.path, "a", encoding="utf-8", buffering=1)
        self.size = self.path.stat().st_size

    def write(self, line: str) -> None:
        if self.size >= self.max_bytes:
            self.handle.close()
            self.path.replace(self.path.with_name(self.path.name + ".1"))
            self.handle = open(self.path, "a", encoding="utf-8", buffering=1)
            self.size = 0
        self.handle.write(line)
        self.size += len(line.encode("utf-8"))

    def close(self) -> None:
        self.handle.close()

    def __enter__(self) -> "RotatingLineLog":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


class MirrorRefused(ValueError):
    """The mirror would write a production broker or has no valid plan."""


def refuse_production_destination(bootstrap: str) -> None:
    hosts = [item.strip().rsplit(":", 1)[0].lower() for item in bootstrap.split(",") if item.strip()]
    if not hosts or any(host in PRODUCTION_HOSTS or PRODUCTION_PROJECT in host for host in hosts):
        raise MirrorRefused("the mirror destination must be an isolated broker, never a stable production broker")


def bundle_products(bundle: dict[str, Any]) -> dict[str, frozenset[str]]:
    """Kafka key (physical key) -> the payload feeds bound for it."""

    products: dict[str, set[str]] = {}
    for binding in bundle["catalog"]["bindings"]:
        products.setdefault(binding["physical_key"], set()).add(binding["feed"])
    return {key: frozenset(feeds) for key, feeds in products.items()}


def payload_feed(payload: bytes) -> str | None:
    from qdl.marketdata.v2 import market_data_pb2

    try:
        envelope = market_data_pb2.EventEnvelope.FromString(payload)
    except Exception:  # noqa: BLE001 - counted as undecodable, never forwarded
        return None
    name = envelope.WhichOneof("payload")
    return name.upper() if name else None


def classify(key: bytes | None, payload: bytes | None, products: dict[str, frozenset[str]],
             feed_of: Callable[[bytes], str | None] = payload_feed) -> str:
    """``forward`` or the reason a record is not mirrored."""

    if key is None or payload is None:
        return "no_key_or_value"
    feeds = products.get(key.decode("utf-8", "replace"))
    if feeds is None:
        return "key_outside_bundle"
    feed = feed_of(payload)
    if feed is None:
        return "undecodable"
    return "forward" if feed in feeds else "feed_not_bound"


def source_config(args: argparse.Namespace) -> dict[str, Any]:
    config: dict[str, Any] = {
        "bootstrap.servers": args.source_bootstrap,
        # A group id is required by librdkafka; with assign() and no commit it
        # never joins a group and never writes __consumer_offsets.
        "group.id": f"{args.group_prefix}{'' if args.group_prefix == GROUP_PREFIXES[0] else 'kn4-mirror-'}"
                    f"{uuid.uuid4().hex[:12]}",
        "client.id": "kn-shadow-mirror",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "isolation.level": "read_committed",
        "auto.offset.reset": "error",
        "enable.partition.eof": False,
        "fetch.max.bytes": 8 * 1024 * 1024,
        "max.partition.fetch.bytes": 2 * 1024 * 1024,
        # Bounded prefetch: librdkafka's default queues up to 64 MiB per
        # partition (six partitions OOM-killed a 256 MiB mirror, measured).
        "queued.max.messages.kbytes": 4096,
    }
    if args.ca:
        config.update({"security.protocol": "ssl", "ssl.ca.location": args.ca,
                       "ssl.certificate.location": args.cert, "ssl.key.location": args.key})
    return config


def resolve_start(consumer, topic: str, since_ms: int, partitions: Iterable[int]) -> dict[int, int]:
    from confluent_kafka import TopicPartition

    resolved = consumer.offsets_for_times([TopicPartition(topic, p, since_ms) for p in partitions], timeout=20)
    start = {}
    for item in resolved:
        if item.error is not None:
            raise MirrorRefused(f"offset lookup failed for partition {item.partition}")
        if item.offset < 0:  # nothing at or after the time: the current end
            _low, high = consumer.get_watermark_offsets(TopicPartition(topic, item.partition), timeout=20)
            start[item.partition] = high
        else:
            start[item.partition] = item.offset
    return start


CHECKPOINT_TOPIC = "kn.mirror.checkpoint.v1"
CHECKPOINT_KEY = b"next-source-offsets"


def checkpoint_value(source_topic: str, next_offsets: dict[int, int]) -> bytes:
    return json.dumps({"source_topic": source_topic,
                       "next": {str(p): o for p, o in sorted(next_offsets.items())}},
                      sort_keys=True).encode()


def checkpoint_resume_offsets(bootstrap: str, checkpoint_topic: str, source_topic: str,
                              *, look_back: int = 64) -> dict[int, int]:
    """The next source offset per partition from the mirror's durable checkpoint.

    Written in the same producer transaction as the mirrored records (KN-4
    D47-4), so it is exactly as far as the committed copy - whatever else
    (e.g. the shadow core's history) the destination log holds. Only the
    mirror writes this compacted one-partition topic: every transaction adds
    one record and one marker, so its last ``look_back`` offsets hold the
    latest value. Empty when the mirror never committed."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"kn-shadow-mirror-{uuid.uuid4().hex[:12]}",
                         "enable.auto.commit": False, "isolation.level": "read_committed",
                         "enable.partition.eof": True})
    try:
        low, high = consumer.get_watermark_offsets(TopicPartition(checkpoint_topic, 0), timeout=10)
        if high <= low:
            return {}
        consumer.assign([TopicPartition(checkpoint_topic, 0, max(low, high - look_back))])
        latest = None
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                if message.error().code() == PARTITION_EOF:
                    break
                raise RuntimeError(str(message.error()))
            if message.key() == CHECKPOINT_KEY:
                latest = json.loads(message.value())
        if latest is None:
            raise MirrorRefused("the mirror checkpoint topic holds no readable checkpoint")
        if latest["source_topic"] != source_topic:
            raise MirrorRefused("the mirror checkpoint belongs to another source topic")
        return {int(p): int(o) for p, o in latest["next"].items()}
    finally:
        consumer.close()


def mirrored_headers(message) -> list[tuple[str, bytes]]:
    headers = [(key, value) for key, value in (message.headers() or []) if key not in MIRROR_HEADERS]
    kind, stamp = message.timestamp()
    return headers + [
        ("qdl-mirror-source-partition", str(message.partition()).encode()),
        ("qdl-mirror-source-offset", str(message.offset()).encode()),
        ("qdl-mirror-source-timestamp", f"{kind}:{stamp}".encode()),
    ]


def run_mirror(consumer, producer, *, topic: str, start: dict[int, int], products: dict[str, frozenset[str]],
               deadline_s: float, max_bytes_per_second: float, log: Callable[[dict[str, Any]], None],
               feed_of: Callable[[bytes], str | None] = payload_feed, stop: Callable[[], bool] = lambda: False,
               clock=time.monotonic, sleep=time.sleep, batch_records: int = 2000,
               batch_seconds: float = 0.05, dest_topic: str | None = None,
               checkpoint_topic: str | None = None) -> dict[str, Any]:
    """Copy committed bundle records partition-for-partition until the deadline
    (``dest_topic`` defaults to the source name on the isolated broker)."""

    from confluent_kafka import TopicPartition

    consumer.assign([TopicPartition(topic, p, o) for p, o in sorted(start.items())])
    began = clock()
    counts: dict[str, int] = {}
    mirrored_bytes = transactions = 0
    next_offset = dict(start)
    while clock() - began < deadline_s and not stop():
        messages = consumer.consume(batch_records, timeout=batch_seconds)
        batch = []
        for message in messages:
            error = message.error()
            if error is not None:
                if error.code() == PARTITION_EOF:
                    continue
                raise RuntimeError(f"source fetch error on partition {message.partition()}: {error}")
            if message.offset() < next_offset.get(message.partition(), 0):
                continue  # never mirror a source offset twice
            next_offset[message.partition()] = message.offset() + 1
            reason = classify(message.key(), message.value(), products, feed_of)
            counts[reason] = counts.get(reason, 0) + 1
            if reason == "forward":
                batch.append(message)
        if not batch:
            continue
        delivered: list[tuple[int, int, int, int, int]] = []
        producer.begin_transaction()
        for message in batch:
            kind, stamp = message.timestamp()
            source = (message.offset(), stamp, kind)

            def on_delivery(error, produced, source=source):
                if error is None:
                    delivered.append((produced.partition(), produced.offset(), *source))

            producer.produce(dest_topic or topic, key=message.key(), value=message.value(), partition=message.partition(),
                             headers=mirrored_headers(message), timestamp=stamp, on_delivery=on_delivery)
            mirrored_bytes += len(message.value())
        if checkpoint_topic is not None:
            # Committed with the copies: a restart resumes exactly here.
            producer.produce(checkpoint_topic, key=CHECKPOINT_KEY, value=checkpoint_value(topic, next_offset),
                             partition=0)
        producer.commit_transaction(30)
        committed_ns = time.time_ns()
        transactions += 1
        for partition, offset, source_offset, stamp, kind in delivered:
            log({"partition": partition, "offset": offset, "commit_ns": committed_ns,
                 "source_offset": source_offset, "source_timestamp_ms": stamp, "source_timestamp_type": kind})
        elapsed = max(clock() - began, 1e-6)
        if max_bytes_per_second > 0 and mirrored_bytes / elapsed > max_bytes_per_second:
            sleep(min(1.0, mirrored_bytes / max_bytes_per_second - elapsed))
    return {"counts": dict(sorted(counts.items())), "bytes": mirrored_bytes, "transactions": transactions,
            "next_source_offsets": {str(p): o for p, o in sorted(next_offset.items())}}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="mode", required=True)
    for name in ("start", "run"):
        item = sub.add_parser(name)
        item.add_argument("--source-bootstrap", required=True)
        item.add_argument("--topic", default=TOPIC)
        item.add_argument("--ca")
        item.add_argument("--cert")
        item.add_argument("--key")
        item.add_argument("--group-prefix", choices=GROUP_PREFIXES, default=GROUP_PREFIXES[0])
    sub.choices["start"].add_argument("--since-seconds", type=int, required=True)
    sub.choices["start"].add_argument("--out", required=True)
    run = sub.choices["run"]
    run.add_argument("--start", required=True)
    run.add_argument("--bundle", required=True)
    run.add_argument("--dest-bootstrap", required=True)
    run.add_argument("--deadline-seconds", type=float, required=True)
    run.add_argument("--max-bytes-per-second", type=float, default=8 * 1024 * 1024)
    run.add_argument("--commit-log", required=True)
    run.add_argument("--commit-log-max-bytes", type=int, default=64 * 1024 * 1024,
                     help="rotate the commit log past this size (at most two files are kept)")
    run.add_argument("--resume-from-destination", action="store_true",
                     help="start after the mirror's durable checkpoint (written with every copy)")
    run.add_argument("--checkpoint-topic", default=CHECKPOINT_TOPIC,
                     help="compacted one-partition topic on the isolated broker holding the checkpoint")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if bool(args.ca) != bool(args.cert) or bool(args.ca) != bool(args.key):
        raise MirrorRefused("TLS needs --ca, --cert and --key together")
    if args.mode == "run":
        refuse_production_destination(args.dest_bootstrap)
    from confluent_kafka import Consumer, Producer

    consumer = Consumer(source_config(args))
    try:
        partitions = sorted(consumer.list_topics(args.topic, timeout=20).topics[args.topic].partitions)
        if args.mode == "start":
            since_ms = int(time.time() * 1000) - args.since_seconds * 1000
            offsets = {str(p): o for p, o in sorted(resolve_start(consumer, args.topic, since_ms, partitions).items())}
            Path(args.out).write_text(json.dumps({"topic": args.topic, "since_ms": since_ms, "offsets": offsets}),
                                      encoding="utf-8")
            print(json.dumps({"mode": "start", "since_ms": since_ms, "offsets": offsets}))
            return 0
        plan = json.loads(Path(args.start).read_text(encoding="utf-8"))
        start = {int(p): int(o) for p, o in plan.get("offsets", {}).items()}
        if plan.get("topic") != args.topic or sorted(start) != partitions:
            raise MirrorRefused("the start file does not cover every source partition of the topic")
        # A fixed transactional id: a restarted mirror fences its killed
        # predecessor and aborts its open transaction at once (a random id left
        # it open until the transaction timeout, stalling read_committed readers).
        producer = Producer({"bootstrap.servers": args.dest_bootstrap, "enable.idempotence": True,
                             "transactional.id": f"kn-shadow-mirror-{args.topic}", "linger.ms": 5,
                             # A transaction holds at most one consume batch; 1 GiB default queue.
                             "queue.buffering.max.kbytes": 32768})
        dest = producer.list_topics(args.topic, timeout=20).topics.get(args.topic)
        if dest is None or dest.error is not None or sorted(dest.partitions) != partitions:
            raise MirrorRefused("the isolated topic must have the source's partitions (same partition mirror)")
        checkpoint = producer.list_topics(args.checkpoint_topic, timeout=20).topics.get(args.checkpoint_topic)
        if checkpoint is None or checkpoint.error is not None or sorted(checkpoint.partitions) != [0]:
            raise MirrorRefused("the checkpoint topic must exist on the isolated broker with one partition")
        producer.init_transactions(30)  # fences a predecessor before its checkpoint is read
        if args.resume_from_destination:
            for partition, offset in checkpoint_resume_offsets(args.dest_bootstrap, args.checkpoint_topic,
                                                               args.topic).items():
                start[partition] = max(start[partition], offset)
        products = bundle_products(json.loads(Path(args.bundle).read_text(encoding="utf-8")))
        stopping: list[bool] = []
        signal.signal(signal.SIGTERM, lambda *_: stopping.append(True))
        with RotatingLineLog(args.commit_log, args.commit_log_max_bytes) as handle:
            result = run_mirror(consumer, producer, topic=args.topic, start=start, products=products,
                                deadline_s=args.deadline_seconds, max_bytes_per_second=args.max_bytes_per_second,
                                log=lambda row: handle.write(json.dumps(row, sort_keys=True) + "\n"),
                                stop=lambda: bool(stopping), checkpoint_topic=args.checkpoint_topic)
        print(json.dumps({"mode": "run", **result}, sort_keys=True))
        return 0
    finally:
        consumer.close()


if __name__ == "__main__":
    sys.exit(main())
