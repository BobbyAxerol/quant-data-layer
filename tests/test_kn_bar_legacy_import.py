"""KN-3 K3.6 (D16a): legacy BAR export/import from the canonical SQLite spool.

Fixtures are real ``md.canonical.v2`` BAR records (the committed codec golden,
``contracts/golden/kn_v220/state_codec.json``, provenance "real records read
from the stable spool") written into a spool built by the real spool code
(``SQLiteDurableSpool``). Rows derived from them (other opens, an in-progress
and a revised row) are synthetic test fixtures and marked as such by their
event ids (``k36-test|...``). ``QDL_KN_CANONICAL_SAMPLE`` may point at the
114-record read-only spool sample for one extra case (skipped otherwise).

The Kafka case needs ``QDL_KN_TEST_KAFKA`` = bootstrap of a disposable broker
(topics named ``kn3-k36-*`` are created and deleted by the test); it is skipped
loudly otherwise.
"""
from __future__ import annotations

import base64
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import unittest
import uuid
from contextlib import redirect_stdout

from qdl.adapters.intervals import canonical_interval_ms
from qdl.marketdata.v2 import market_data_pb2
from qdl.projection.kn_state_codec import (
    LEGACY_PROVENANCE,
    FrameKind,
    LegacyLineage,
    bar_key,
    decode_frame,
    state_partition,
)
from qdl.projection.state_contract import LogicalProductKey
from qdl.runtime.stable_catalog import StableSourceCatalog
from qdl.transport import DurableEvent, SQLiteDurableSpool, SpoolConfig

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location("kn_bar_legacy_import", ROOT / "scripts/kn_bar_legacy_import.py")
assert _SPEC is not None and _SPEC.loader is not None
IMPORT = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = IMPORT
_SPEC.loader.exec_module(IMPORT)

GOLDEN = ROOT / "contracts/golden/kn_v220/state_codec.json"
CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
STREAM = "md.canonical.v2"
ENVIRONMENT = "paper"
PARTITIONS = 6
EPOCH = 7


# ------------------------------------------------------------------ fixtures (shared with test_kn_bar_readback)

def golden_records(*, bar: bool | None = None) -> list[tuple[str, bytes]]:
    """Real canonical records of the codec golden: (spool partition key, canonical bytes)."""

    records = json.loads(GOLDEN.read_text(encoding="utf-8"))["records"]
    rows = []
    for record in records:
        if record["synthetic"]:
            continue
        is_bar = record["tag"].startswith("bar|")
        if bar is None or bar == is_bar:
            rows.append((record["spool_partition_key"], base64.b64decode(record["canonical_b64"])))
    return rows


def derived_bar(canonical: bytes, shift: int, *, final: bool = True, revision: int = 0,
                source_id: str | None = None) -> bytes:
    """A synthetic test row derived from a real BAR: another open/finality/revision."""

    envelope = market_data_pb2.EventEnvelope.FromString(canonical)
    step_ns = canonical_interval_ms(envelope.bar.interval) * 1_000_000
    envelope.bar.open_time_ns += shift * step_ns
    envelope.bar.close_time_ns += shift * step_ns
    if not final:
        envelope.bar.is_final = False
        envelope.bar.lifecycle = market_data_pb2.BAR_LIFECYCLE_IN_PROGRESS
    if revision:
        envelope.bar.revision = revision
        envelope.bar.lifecycle = market_data_pb2.BAR_LIFECYCLE_REVISED
    if source_id is not None:
        envelope.source_id = source_id
    envelope.event_id = hashlib.sha256(
        f"k36-test|{shift}|{final}|{revision}|{source_id}|".encode() + canonical).digest()[:16]
    return envelope.SerializeToString()


def open_ms_of(canonical: bytes) -> int:
    return market_data_pb2.EventEnvelope.FromString(canonical).bar.open_time_ns // 1_000_000


def build_spool(path: Path, rows: list[tuple[str, bytes]], *, event_ids: list[bytes] | None = None) -> None:
    """Write ``rows`` through the real spool code (as the projector appends them)."""

    spool = SQLiteDurableSpool(SpoolConfig(
        path=path, max_records=50_000, max_payload_bytes=200_000_000, max_event_bytes=64_000,
        max_storage_bytes=400_000_000, min_free_disk_bytes=0))
    try:
        events = []
        for index, (partition_key, payload) in enumerate(rows):
            event_id = (event_ids[index] if event_ids is not None
                        else market_data_pb2.EventEnvelope.FromString(payload).event_id)
            events.append(DurableEvent(stream=STREAM, partition_key=partition_key, event_id=bytes(event_id),
                                       payload=payload, accepted_at_ns=1_000 + index))
        for start in range(0, len(events), 500):
            spool.append_many(events[start:start + 500])
    finally:
        spool.close()


def standard_rows() -> list[tuple[str, bytes]]:
    """15 real BAR records, each with two older opens; +1 in-progress, +1 revision."""

    rows: list[tuple[str, bytes]] = []
    for partition_key, canonical in golden_records(bar=True):
        rows.append((partition_key, derived_bar(canonical, -2)))
        rows.append((partition_key, derived_bar(canonical, -1)))
        rows.append((partition_key, canonical))
        if partition_key.endswith("okx-swap-doge-usdt-swap-bar-1m-primary-v2"):
            rows.append((partition_key, derived_bar(canonical, 1, final=False)))
        if partition_key.endswith("binance-usdm-dogeusdt-bar-1d-primary-v2"):
            rows.append((partition_key, derived_bar(canonical, -1, revision=1)))
    return rows


def expected_facts_sha256(payloads: list[bytes]) -> str:
    facts = sorted(
        (open_ms_of(payload), market_data_pb2.EventEnvelope.FromString(payload).bar.revision,
         hashlib.sha256(payload).hexdigest())
        for payload in payloads)
    return hashlib.sha256("".join(f"{o}|{r}|{s}\n" for o, r, s in facts).encode()).hexdigest()


def run_main(args: list[str]) -> tuple[int, dict]:
    receipt = Path(args[args.index("--receipt") + 1])
    with redirect_stdout(io.StringIO()):
        code = IMPORT.main(args)
    return code, json.loads(receipt.read_text(encoding="utf-8"))


def export_args(sqlite_path: Path, receipt: Path, *extra: str) -> list[str]:
    return ["export", "--sqlite", str(sqlite_path), "--catalog", str(CATALOG), "--environment", ENVIRONMENT,
            "--partitions", str(PARTITIONS), "--materializer-epoch", str(EPOCH), "--receipt", str(receipt),
            *extra]


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class _SpoolCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="kn3-k36-")
        self.dir = Path(self._tmp.name)
        self.sqlite = self.dir / "canonical-cache.sqlite3"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def bar_bindings(self):
        bindings, _info = IMPORT.load_bar_bindings(CATALOG, ENVIRONMENT)
        return bindings

    def collect(self, rows_sqlite: Path, *, page_rows: int = 2):
        frames: dict[str, list] = {}
        connection = IMPORT.open_spool_readonly(rows_sqlite)
        try:
            result = IMPORT.export(connection, self.bar_bindings(), STREAM, materializer_epoch=EPOCH,
                                   partitions=PARTITIONS, page_rows=page_rows,
                                   on_binding=lambda summary, items: frames.setdefault(
                                       summary["binding_id"], []).extend(items))
        finally:
            connection.close()
        return result, frames


# ------------------------------------------------------------------ unit cases

class LegacyExportTests(_SpoolCase):
    def test_export_counts_every_bar_binding_with_facts_hash(self):
        rows = standard_rows()
        build_spool(self.sqlite, rows)
        code, receipt = run_main(export_args(self.sqlite, self.dir / "r.json", "--page-rows", "2"))
        self.assertEqual((code, receipt["status"]), (0, "PASS"))
        catalog = StableSourceCatalog.load(CATALOG)
        bar_count = sum(1 for item in catalog.bindings if item.feed.value == "BAR")
        self.assertEqual(receipt["totals"], {
            "bindings": bar_count, "bindings_with_rows": 15, "rows": len(rows),
            "finals": len(rows) - 1, "in_progress": 1, "distinct_keys": len(rows)})
        by_pk: dict[str, list[bytes]] = {}
        for partition_key, payload in rows:
            by_pk.setdefault(partition_key, []).append(payload)
        seen = 0
        for summary in receipt["bindings"]:
            payloads = by_pk.get(summary["physical_key"])
            if payloads is None:
                self.assertEqual(summary["rows"], 0)
                continue
            seen += 1
            opens = [open_ms_of(item) for item in payloads]
            self.assertEqual(summary["rows"], len(payloads))
            self.assertEqual((summary["first_open_ms"], summary["last_open_ms"]), (min(opens), max(opens)))
            self.assertEqual(summary["facts_sha256"], expected_facts_sha256(payloads))
            lpk = LogicalProductKey.parse(summary["lpk"])
            self.assertEqual(summary["state_partition"], state_partition(lpk, PARTITIONS))
            self.assertEqual(receipt["replay_cutoff"]["spool"]["cutoff_logical_offset_by_partition_key"][
                summary["physical_key"]], summary["spool_cutoff_logical_offset"])
        self.assertEqual(seen, 15)
        self.assertEqual(receipt["replay_cutoff"]["canonical_topic"]["captured"], False)
        self.assertEqual(receipt["mutations"], 0)

    def test_frames_are_legacy_bars_with_original_identity_and_lineage(self):
        rows = standard_rows()
        build_spool(self.sqlite, rows)
        result, frames = self.collect(self.sqlite, page_rows=3)
        payload_by_hash = {hashlib.sha256(payload).hexdigest(): (pk, payload) for pk, payload in rows}
        count = 0
        offsets: dict[str, list[int]] = {}
        for binding_id, items in frames.items():
            for item in items:
                frame = decode_frame(item.value)
                count += 1
                self.assertIs(frame.kind, FrameKind.LEGACY_BAR)
                self.assertIsNone(frame.source)
                self.assertEqual(frame.header()["provenance"], LEGACY_PROVENANCE)
                partition_key, payload = payload_by_hash[frame.content_sha256]
                envelope = market_data_pb2.EventEnvelope.FromString(payload)
                self.assertEqual(frame.envelope, payload)
                self.assertEqual(frame.event_id, envelope.event_id.hex())
                self.assertEqual(frame.legacy.spool_stream, STREAM)
                self.assertEqual(frame.legacy.spool_partition_key, partition_key)
                self.assertIsInstance(frame.legacy, LegacyLineage)
                self.assertEqual(item.key, bar_key(frame.lpk, frame.open_time_ms, frame.is_final, frame.revision,
                                                   frame.content_sha256))
                self.assertEqual(item.partition, state_partition(frame.lpk, PARTITIONS))
                offsets.setdefault(binding_id, []).append(frame.legacy.spool_logical_offset)
        self.assertEqual(count, len(rows))
        for values in offsets.values():  # keyset pages: every offset once, ascending
            self.assertEqual(values, sorted(set(values)))
        self.assertEqual(result["totals"]["rows"], len(rows))

    def test_receipt_is_deterministic_and_a_rerun_yields_the_same_keys(self):
        build_spool(self.sqlite, standard_rows())
        _code, first = run_main(export_args(self.sqlite, self.dir / "a.json", "--page-rows", "1"))
        _code, second = run_main(export_args(self.sqlite, self.dir / "b.json", "--page-rows", "1000"))
        self.assertEqual(first, second)  # page size does not change anything
        _result, frames_a = self.collect(self.sqlite, page_rows=1)
        _result, frames_b = self.collect(self.sqlite, page_rows=7)
        flat = lambda frames: sorted((item.partition, item.key, item.value) for items in frames.values()
                                     for item in items)
        self.assertEqual(flat(frames_a), flat(frames_b))

    def test_the_spool_is_opened_read_only_and_left_unchanged(self):
        build_spool(self.sqlite, standard_rows())
        before = file_sha256(self.sqlite)
        connection = IMPORT.open_spool_readonly(self.sqlite)
        try:
            with self.assertRaises(sqlite3.OperationalError):
                connection.execute("DELETE FROM events")
        finally:
            connection.close()
        run_main(export_args(self.sqlite, self.dir / "r.json"))
        self.assertEqual(file_sha256(self.sqlite), before)

    def test_queries_use_only_the_primary_key_index(self):
        build_spool(self.sqlite, standard_rows())
        connection = IMPORT.open_spool_readonly(self.sqlite)
        try:
            index = IMPORT.primary_key_index(connection)
            pk = golden_records(bar=True)[0][0]
            page = IMPORT.assert_primary_key_plan(connection, IMPORT.page_sql(index), (STREAM, pk, -1, 10, 5), index)
            self.assertTrue(any(f"USING INDEX {index} (stream=? AND partition_key=? AND logical_offset>? AND "
                                "logical_offset<?)" in detail for detail in page), page)
            IMPORT.assert_primary_key_plan(connection, IMPORT.cutoff_sql(index), (STREAM, pk), index)
            for bad in ("SELECT payload FROM events WHERE accepted_at_ns > ?",
                        "SELECT payload FROM events WHERE stream = ? AND partition_key = ? ORDER BY accepted_at_ns"):
                params = (1,) if bad.count("?") == 1 else (STREAM, pk)
                with self.assertRaises(IMPORT.Refused):
                    IMPORT.assert_primary_key_plan(connection, bad, params, index)
        finally:
            connection.close()

    def test_a_row_outside_the_binding_identity_is_refused_with_binding_and_offset(self):
        bars = dict(golden_records(bar=True))
        pk_2h = next(pk for pk in bars if pk.endswith("binance-usdm-dogeusdt-bar-2h-primary-v2"))
        pk_1d = next(pk for pk in bars if pk.endswith("binance-usdm-dogeusdt-bar-1d-primary-v2"))
        trade = golden_records(bar=False)[0][1]
        cases = {
            "another binding's BAR": [(pk_2h, bars[pk_2h]), (pk_2h, bars[pk_1d])],
            "a TRADE": [(pk_2h, bars[pk_2h]), (pk_2h, trade)],
            "another source id": [(pk_2h, bars[pk_2h]), (pk_2h, derived_bar(bars[pk_2h], 1, source_id="other"))],
        }
        for label, rows in cases.items():
            with self.subTest(label):
                path = self.dir / f"{abs(hash(label))}.sqlite3"
                build_spool(path, rows)
                code, receipt = run_main(export_args(path, self.dir / "refused.json"))
                self.assertEqual((code, receipt["status"]), (4, "REFUSED"))
                self.assertIn("binding=binance-usdm-dogeusdt-bar-2h", receipt["error"])
                self.assertRegex(receipt["error"], r"logical_offset=\d+")

    def test_a_row_whose_stored_hash_or_event_id_differs_is_refused(self):
        partition_key, canonical = golden_records(bar=True)[0]
        build_spool(self.sqlite, [(partition_key, canonical)])
        writer = sqlite3.connect(self.sqlite)  # test fixture corruption, not the tool
        writer.execute("UPDATE events SET payload_sha256 = ?", ("0" * 64,))
        writer.commit()
        writer.close()
        code, receipt = run_main(export_args(self.sqlite, self.dir / "r.json"))
        self.assertEqual(code, 4)
        self.assertIn("payload hash differs", receipt["error"])
        other = self.dir / "event-id.sqlite3"
        build_spool(other, [(partition_key, canonical)], event_ids=[b"\x01" * 16])
        code, receipt = run_main(export_args(other, self.dir / "r2.json"))
        self.assertEqual(code, 4)
        self.assertIn("event id differs", receipt["error"])

    def test_the_readback_lpk_rule_is_the_gateway_bundle_rule(self):
        bindings, info = IMPORT.load_bar_bindings(CATALOG, ENVIRONMENT)
        bundle = IMPORT._load_bundle_module().compile_bundle(environment=ENVIRONMENT, catalog_path=CATALOG,
                                                             manifest_paths=())
        expected = {item["binding_id"]: item["product_key"] for item in bundle["catalog"]["bindings"]
                    if item["feed"] == "BAR"}
        self.assertEqual({item.binding.binding_id: item.lpk.encode() for item in bindings}, expected)
        self.assertEqual(info["bar_bindings"], len(expected))

    @unittest.skipUnless(os.environ.get("QDL_KN_CANONICAL_SAMPLE"),
                         "needs QDL_KN_CANONICAL_SAMPLE = the read-only 114-record spool sample")
    def test_the_real_spool_sample_exports_every_bar(self):
        records = json.loads(Path(os.environ["QDL_KN_CANONICAL_SAMPLE"]).read_text(encoding="utf-8"))
        rows = [(item["physical_key"], base64.b64decode(item["payload_b64"])) for item in records]
        bars = [row for item, row in zip(records, rows) if item["feed"] == "bar"]
        build_spool(self.sqlite, rows)  # non-BAR partitions are not BAR bindings and are not read
        code, receipt = run_main(export_args(self.sqlite, self.dir / "r.json"))
        self.assertEqual((code, receipt["status"]), (0, "PASS"))
        self.assertEqual(receipt["totals"]["rows"], len(bars))
        self.assertEqual(receipt["totals"]["bindings_with_rows"], len({pk for pk, _ in bars}))
        _result, frames = self.collect(self.sqlite)
        self.assertEqual(sum(len(items) for items in frames.values()), len(bars))


class PagedLiveSpoolExportTests(_SpoolCase):
    """KN-4 D43: a live production spool is read in one short transaction per
    page, so its writer can checkpoint between pages; the facts are equal."""

    def test_page_transactions_equal_one_snapshot_and_release_the_writer_between_pages(self):
        rows = standard_rows()
        build_spool(self.sqlite, rows)
        writer = sqlite3.connect(self.sqlite)
        writer.execute("PRAGMA journal_mode=WAL")
        pauses = []

        def pause(seconds):
            # Between pages no read snapshot is held: a TRUNCATE checkpoint
            # completes (busy == 0), as the production spool writer needs.
            busy, _log, _checkpointed = writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            pauses.append((seconds, busy))

        bindings = self.bar_bindings()
        connection = IMPORT.open_spool_readonly(self.sqlite)
        try:
            index = IMPORT.primary_key_index(connection)
            paged = [IMPORT.export_binding(connection, index, STREAM, item, materializer_epoch=EPOCH,
                                           partitions=PARTITIONS, page_rows=1, keep_frames=False,
                                           page_transactions=True, page_pause_s=0.05, sleep=pause)[0]
                     for item in bindings]
            single = [IMPORT.export_binding(connection, index, STREAM, item, materializer_epoch=EPOCH,
                                            partitions=PARTITIONS, page_rows=1, keep_frames=False)[0]
                      for item in bindings]
        finally:
            connection.close()
            writer.close()
        self.assertEqual([{k: v for k, v in item.items()} for item in paged], single)
        self.assertGreaterEqual(len(pauses), len(rows))
        self.assertTrue(all(seconds == 0.05 and busy == 0 for seconds, busy in pauses), pauses[:3])
        self.assertTrue(all(item["spool_first_logical_offset_read"] == item["spool_first_logical_offset"]
                            for item in paged if item["rows"]))


class LegacyImportGuardTests(_SpoolCase):
    def setUp(self) -> None:
        super().setUp()
        build_spool(self.sqlite, standard_rows())

    def args(self, *extra: str) -> list[str]:
        base = export_args(self.sqlite, self.dir / "r.json")
        base[0] = "import"
        return [*base, *extra]

    def test_import_without_isolated_or_token_is_refused_before_kafka(self):
        code, receipt = run_main(self.args("--bootstrap", "kn3-k36-unused:9092"))
        self.assertEqual((code, receipt["status"]), (4, "REFUSED"))
        self.assertTrue(receipt["confirmation_token"].startswith("IMPORT_QDL_KN3_LEGACY_BARS_"))
        self.assertEqual(receipt["mutations"], 0)
        code, wrong = run_main(self.args("--bootstrap", "kn3-k36-unused:9092", "--confirm", "WRONG"))
        self.assertEqual(wrong["status"], "REFUSED")
        self.assertEqual(wrong["confirmation_token"], receipt["confirmation_token"])

    def test_isolated_refuses_a_production_broker_tls_and_the_canonical_topic(self):
        for extra in (("--bootstrap", "kafka1:9092", "--isolated"),
                      ("--bootstrap", "kn3-k36-kafka:9092", "--isolated", "--ca", "a", "--cert", "b", "--key", "c"),
                      ("--bootstrap", "kn3-k36-kafka:9092", "--isolated", "--bars-topic", STREAM),
                      ("--bootstrap", "kn3-k36-kafka:9092", "--ca", "only-ca")):
            with self.subTest(extra):
                code, receipt = run_main(self.args(*extra))
                self.assertEqual((code, receipt["status"]), (4, "REFUSED"))

    def test_dry_run_seals_the_plan_and_contacts_nothing(self):
        code, receipt = run_main(self.args("--bootstrap", "kn3-k36-unused:9092", "--dry-run"))
        self.assertEqual((code, receipt["status"]), (0, "DRY_RUN"))
        digest, token = IMPORT.seal(receipt["plan"])
        self.assertEqual((receipt["plan_sha256"], receipt["confirmation_token"]), (digest, token))
        self.assertFalse(receipt["replay_cutoff"]["canonical_topic"]["captured"])
        self.assertTrue(receipt["plan"]["transactional_id"].startswith("kn-projector-v3-legacy-import-"))
        _code, again = run_main(self.args("--bootstrap", "kn3-k36-unused:9092", "--dry-run"))
        self.assertEqual(again["confirmation_token"], token)
        _code, other = run_main(self.args("--bootstrap", "kn3-k36-other:9092", "--dry-run"))
        self.assertNotEqual(other["confirmation_token"], token)
        self.assertEqual(other["plan"]["transactional_id"], receipt["plan"]["transactional_id"])


# ------------------------------------------------------------------ Kafka integration

@unittest.skipUnless(os.environ.get("QDL_KN_TEST_KAFKA"),
                     "needs QDL_KN_TEST_KAFKA = bootstrap of a disposable isolated Kafka broker")
class LegacyImportKafkaTests(_SpoolCase):
    def setUp(self) -> None:
        super().setUp()
        from confluent_kafka import Producer
        from confluent_kafka.admin import AdminClient, NewTopic

        self.bootstrap = os.environ["QDL_KN_TEST_KAFKA"]
        run = uuid.uuid4().hex[:8]
        self.bars_topic = f"kn3-k36-bars-{run}"
        self.canonical_topic = f"kn3-k36-canonical-{run}"
        self.admin = AdminClient({"bootstrap.servers": self.bootstrap})
        futures = self.admin.create_topics([
            NewTopic(self.bars_topic, PARTITIONS, 1, config={"cleanup.policy": "compact"}),
            NewTopic(self.canonical_topic, 3, 1)])
        for future in futures.values():
            future.result(60)
        producer = Producer({"bootstrap.servers": self.bootstrap})
        for index in range(5):
            producer.produce(self.canonical_topic, key=b"k", value=b"v", partition=index % 3)
        producer.flush(30)
        self.canonical_expected = {"0": {"earliest": 0, "latest": 2}, "1": {"earliest": 0, "latest": 2},
                                   "2": {"earliest": 0, "latest": 1}}

    def tearDown(self) -> None:
        futures = self.admin.delete_topics([self.bars_topic, self.canonical_topic])
        for future in futures.values():
            future.result(60)
        super().tearDown()

    def consume_all(self, expected: int) -> list[tuple[int, str, bytes]]:
        from confluent_kafka import Consumer, OFFSET_BEGINNING, TopicPartition

        consumer = Consumer({"bootstrap.servers": self.bootstrap, "group.id": f"kn3-k36-read-{uuid.uuid4().hex}",
                             "enable.auto.commit": False, "isolation.level": "read_committed",
                             "auto.offset.reset": "earliest"})
        consumer.assign([TopicPartition(self.bars_topic, p, OFFSET_BEGINNING) for p in range(PARTITIONS)])
        records: list[tuple[int, str, bytes]] = []
        deadline = time.monotonic() + 90
        quiet_until = None
        try:
            while time.monotonic() < deadline:
                message = consumer.poll(0.5)
                if message is None:
                    if len(records) >= expected:
                        quiet_until = quiet_until or time.monotonic() + 3
                        if time.monotonic() >= quiet_until:
                            break
                    continue
                if message.error():
                    raise AssertionError(str(message.error()))
                records.append((message.partition(), message.key().decode(), message.value()))
        finally:
            consumer.close()
        return records

    def import_args(self, receipt: str) -> list[str]:
        args = export_args(self.sqlite, self.dir / receipt, "--page-rows", "2")
        args[0] = "import"
        return [*args, "--bootstrap", self.bootstrap, "--isolated", "--bars-topic", self.bars_topic,
                "--canonical-topic", self.canonical_topic, "--batch-frames", "2"]

    def test_import_is_read_back_committed_and_a_rerun_keeps_the_key_set(self):
        rows = standard_rows()
        build_spool(self.sqlite, rows)
        code, receipt = run_main(self.import_args("first.json"))
        self.assertEqual((code, receipt["status"]), (0, "PASS"), receipt.get("error"))
        self.assertEqual(receipt["kafka"]["frames"], len(rows))
        self.assertEqual(receipt["mutations"], len(rows))
        expected_transactions = sum(-(-item["rows"] // 2) for item in receipt["bindings"])
        self.assertEqual(receipt["kafka"]["transactions"], expected_transactions)
        cutoff = receipt["replay_cutoff"]["canonical_topic"]
        self.assertTrue(cutoff["captured"])
        self.assertEqual(cutoff["partitions"], self.canonical_expected)

        first = self.consume_all(len(rows))
        self.assertEqual(len(first), len(rows))
        by_lpk: dict[str, list[bytes]] = {}
        for partition, key, value in first:
            frame = decode_frame(value)
            self.assertIs(frame.kind, FrameKind.LEGACY_BAR)
            self.assertEqual(key, frame.key())
            self.assertEqual(partition, state_partition(frame.lpk, PARTITIONS))
            by_lpk.setdefault(frame.lpk.encode(), []).append(frame.envelope)
        for summary in receipt["bindings"]:
            payloads = by_lpk.get(summary["lpk"], [])
            self.assertEqual(len(payloads), summary["rows"])
            if payloads:
                self.assertEqual(expected_facts_sha256(payloads), summary["facts_sha256"])

        code, rerun = run_main(self.import_args("second.json"))
        self.assertEqual((code, rerun["status"]), (0, "PASS"))
        self.assertEqual(rerun["plan"]["transactional_id"], receipt["plan"]["transactional_id"])
        self.assertEqual(rerun["export_sha256"], receipt["export_sha256"])
        both = self.consume_all(2 * len(rows))
        self.assertEqual(len(both), 2 * len(rows))
        first_set = {(key, value) for _p, key, value in first}
        both_set = {(key, value) for _p, key, value in both}
        self.assertEqual(both_set, first_set)  # same keys, same bytes: compaction/stage B see duplicates
        out = os.environ.get("QDL_KN_TEST_EVIDENCE_OUT")
        if out:
            Path(out).write_text(json.dumps({"first": receipt, "rerun": rerun}, indent=1, sort_keys=True) + "\n",
                                 encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
