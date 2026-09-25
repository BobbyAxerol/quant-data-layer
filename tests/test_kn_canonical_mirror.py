"""KN-4 D38: the canonical mirror feeds the isolated shadow from Kafka.

Fixtures are real ``md.canonical.v2`` records of the committed codec golden
(``contracts/golden/kn_v220/state_codec.json``). Fakes cover the classification,
destination guard and the loop; the Kafka case needs ``QDL_KN_TEST_KAFKA`` = a
disposable broker (topics ``kn4-mirror-*`` are created and deleted by the
test) and is skipped loudly otherwise. It proves, on a real broker: aborted
source transactions are never mirrored, every record lands on its source
partition with its key, value, headers and timestamp, provenance headers name
the source offset, out-of-bundle records are counted and dropped, the start
resolved by time is honoured, and the mirror never commits a group offset.
"""
from __future__ import annotations

import base64
import contextlib
import io
import importlib.util
import json
import os
from pathlib import Path
import sys
import time
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "contracts/golden/kn_v220/state_codec.json"
spec = importlib.util.spec_from_file_location("kn_canonical_mirror", ROOT / "scripts/kn_canonical_mirror.py")
mirror = importlib.util.module_from_spec(spec)
sys.modules["kn_canonical_mirror"] = mirror
spec.loader.exec_module(mirror)


def golden() -> list[tuple[str, bytes]]:
    records = json.loads(GOLDEN.read_text(encoding="utf-8"))["records"]
    return [(item["spool_partition_key"], base64.b64decode(item["canonical_b64"])) for item in records
            if item.get("spool_stream") == "md.canonical.v2" and not item.get("synthetic")]


def bundle_for(records: list[tuple[str, bytes]]) -> dict:
    return {"catalog": {"bindings": [{"physical_key": key, "feed": mirror.payload_feed(payload)}
                                     for key, payload in records]}}


class MirrorGuardTests(unittest.TestCase):
    def test_the_destination_is_never_a_stable_production_broker(self):
        for bootstrap in ("kafka1:9092", "kn4-kafka:9092,kafka3:9092", "qdl_v2_stable_candidate-kafka2-1:9092", ""):
            with self.subTest(bootstrap=bootstrap), self.assertRaises(mirror.MirrorRefused):
                mirror.refuse_production_destination(bootstrap)
        mirror.refuse_production_destination("kn4-kafka:9092")

    def test_the_source_reader_never_joins_or_commits_and_reads_committed_only(self):
        args = mirror._parser().parse_args(["start", "--source-bootstrap", "kafka1:9092", "--since-seconds", "1",
                                            "--out", "x", "--ca", "c", "--cert", "t", "--key", "k"])
        config = mirror.source_config(args)
        self.assertEqual(config["isolation.level"], "read_committed")
        self.assertFalse(config["enable.auto.commit"])
        self.assertFalse(config["enable.auto.offset.store"])
        self.assertEqual(config["security.protocol"], "ssl")
        self.assertLessEqual(config["queued.max.messages.kbytes"] * 6, 64 * 1024, "bounded prefetch")
        self.assertTrue(config["group.id"].startswith("kn-shadow-mirror-"))

    def test_the_group_is_unique_under_a_granted_read_only_namespace_only(self):
        base = ["start", "--source-bootstrap", "kafka1:9092", "--since-seconds", "1", "--out", "x"]
        audit = mirror.source_config(mirror._parser().parse_args(base + ["--group-prefix", "qdl-c40-handoff-"]))
        self.assertRegex(audit["group.id"], r"^qdl-c40-handoff-kn4-mirror-[0-9a-f]{12}$")
        again = mirror.source_config(mirror._parser().parse_args(base + ["--group-prefix", "qdl-c40-handoff-"]))
        self.assertNotEqual(audit["group.id"], again["group.id"])
        for production in ("stable-projector-v1", "stable-query-1", "qdl-v2-realtime-core-v2"):
            with self.assertRaises(SystemExit):
                with contextlib.redirect_stderr(io.StringIO()):
                    mirror._parser().parse_args(base + ["--group-prefix", production])

    def test_only_bound_products_are_forwarded_and_the_rest_are_named(self):
        records = golden()
        key, payload = records[0]
        products = mirror.bundle_products(bundle_for(records[:1]))
        self.assertEqual(mirror.classify(key.encode(), payload, products), "forward")
        self.assertEqual(mirror.classify(b"other/bar/x", payload, products), "key_outside_bundle")
        self.assertEqual(mirror.classify(key.encode(), b"\xff\xff\xff", products), "undecodable")
        self.assertEqual(mirror.classify(key.encode(), payload, {key: frozenset({"TRADE"})}), "feed_not_bound")
        self.assertEqual(mirror.classify(None, payload, products), "no_key_or_value")


@unittest.skipUnless(os.environ.get("QDL_KN_TEST_KAFKA"), "SKIPPED LOUDLY: QDL_KN_TEST_KAFKA is not set")
class MirrorKafkaTests(unittest.TestCase):
    def setUp(self):
        from confluent_kafka.admin import AdminClient, NewTopic

        self.bootstrap = os.environ["QDL_KN_TEST_KAFKA"]
        self.admin = AdminClient({"bootstrap.servers": self.bootstrap})
        tag = uuid.uuid4().hex[:8]
        self.source, self.dest = f"kn4-mirror-src-{tag}", f"kn4-mirror-dst-{tag}"
        for future in self.admin.create_topics([NewTopic(self.source, 3, 1), NewTopic(self.dest, 3, 1)]).values():
            future.result(20)

    def tearDown(self):
        for future in self.admin.delete_topics([self.source, self.dest]).values():
            future.result(20)

    def _produce(self, rows, *, abort: bool, stamp_ms: int) -> None:
        from confluent_kafka import Producer

        producer = Producer({"bootstrap.servers": self.bootstrap, "transactional.id": f"kn4-src-{uuid.uuid4().hex}"})
        producer.init_transactions(20)
        producer.begin_transaction()
        for partition, key, payload in rows:
            producer.produce(self.source, key=key.encode(), value=payload, partition=partition,
                             headers=[("qdl-source", b"test")], timestamp=stamp_ms)
        # Flush first: an abort purges unsent records, and the aborted records
        # must really be on the broker for the read_committed claim to mean anything.
        producer.flush(20)
        (producer.abort_transaction if abort else producer.commit_transaction)(20)

    def test_committed_records_are_mirrored_to_their_partition_with_provenance_and_nothing_else(self):
        from confluent_kafka import Consumer, Producer

        records = golden()
        (key_a, pay_a), (key_b, pay_b) = records[0], records[1]
        products = mirror.bundle_products(bundle_for([records[0], records[1]]))
        early, late = int(time.time() * 1000) - 60_000, int(time.time() * 1000)
        self._produce([(0, key_a, pay_a)], abort=False, stamp_ms=early)          # before the start time
        self._produce([(1, key_a, pay_a), (2, key_b, pay_b)], abort=True, stamp_ms=late)
        self._produce([(1, key_a, pay_a), (2, key_b, pay_b), (2, "outside/bar/x", pay_b)], abort=False,
                      stamp_ms=late)
        reader = Consumer(mirror.source_config(mirror._parser().parse_args(
            ["start", "--source-bootstrap", self.bootstrap, "--since-seconds", "1", "--out", "x"])))
        start = mirror.resolve_start(reader, self.source, late - 1_000, range(3))
        self.assertEqual(start[0], 1, "the pre-start record is behind the start")
        producer = Producer({"bootstrap.servers": self.bootstrap, "enable.idempotence": True,
                             "transactional.id": f"kn4-mirror-{uuid.uuid4().hex}"})
        producer.init_transactions(20)
        logged = []
        result = mirror.run_mirror(reader, producer, topic=self.source, dest_topic=self.dest, start=start,
                                   products=products, deadline_s=8, max_bytes_per_second=0,
                                   log=logged.append, batch_seconds=0.2)
        reader.close()
        self.assertEqual(result["counts"], {"forward": 2, "key_outside_bundle": 1})
        check = Consumer({"bootstrap.servers": self.bootstrap, "group.id": f"kn4-check-{uuid.uuid4().hex}",
                          "isolation.level": "read_committed", "auto.offset.reset": "earliest",
                          "enable.auto.commit": False})
        check.subscribe([self.dest])
        seen, deadline = [], time.monotonic() + 20
        while len(seen) < 2 and time.monotonic() < deadline:
            message = check.poll(0.5)
            if message is not None and message.error() is None:
                seen.append(message)
        check.close()
        self.assertEqual(len(seen), 2)
        by_partition = {message.partition(): message for message in seen}
        self.assertEqual(sorted(by_partition), [1, 2])
        for partition, (key, payload) in ((1, (key_a, pay_a)), (2, (key_b, pay_b))):
            message = by_partition[partition]
            headers = dict(message.headers())
            self.assertEqual((message.key(), message.value()), (key.encode(), payload))
            self.assertEqual(headers["qdl-source"], b"test")
            self.assertEqual(headers["qdl-mirror-source-partition"], str(partition).encode())
            self.assertEqual(message.timestamp()[1], late)
            self.assertTrue(headers["qdl-mirror-source-timestamp"].endswith(f":{late}".encode()))
        # The aborted records (offset 0) and their marker (1) precede the
        # committed record on partitions 1 and 2: its real source offset is 2.
        self.assertEqual({int(dict(m.headers())["qdl-mirror-source-offset"]) for m in seen}, {2})
        self.assertEqual(start[1], 0, "the start by time points at the aborted record, which read_committed skips")
        self.assertEqual(sorted((row["partition"], row["source_timestamp_ms"]) for row in logged),
                         [(1, late), (2, late)])
        groups = self.admin.list_consumer_groups().result(20)
        self.assertFalse([g for g in groups.valid if g.group_id.startswith("kn-shadow-mirror-")],
                         "the mirror never registers or commits a consumer group")


if __name__ == "__main__":
    unittest.main()
