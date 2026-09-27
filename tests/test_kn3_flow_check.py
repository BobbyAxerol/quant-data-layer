"""KN-3 K3-T07: the full-flow parity checker (``scripts/kn3_flow_check.py``).

Fixtures are the real canonical records of the committed codec golden
(``contracts/golden/kn_v220/state_codec.json``) plus synthetic derivations
tagged ``k36-test`` (see ``tests/test_kn_bar_legacy_import.py``). The cache is
written by a Python mirror of ``rust/qdl-projector/src/apply.lua`` (ops B/L and
the pointer), so these tests prove the checker's comparisons and read paths -
not that the Rust stage B writes this layout; that is the lead's isolated
full-flow run with the real projector.

The integration case needs ``QDL_KN_TEST_KAFKA`` (disposable broker; topics
``kn3-k36-*`` are created and deleted) and ``QDL_KN_TEST_REDIS`` (disposable
Redis; only exact keys under a unique ``kn3:k36<run>:`` prefix are written and
deleted); it is skipped loudly otherwise.
"""
from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
from contextlib import redirect_stdout
from pathlib import Path
import sys
import tempfile
import unittest
import uuid

from qdl.adapters.intervals import canonical_interval_ms
from qdl.marketdata.v2 import market_data_pb2
from qdl.projection.kn_state_codec import encode_bar_row, encode_latest_value
from qdl.projection.state_contract import MAX_OFFSET
from qdl.runtime import kn_bar_readback
from qdl.runtime.kn_bar_readback import bucket_of
from tests.test_kn_bar_legacy_import import (
    build_spool,
    derived_bar,
    golden_records,
    open_ms_of,
    standard_rows,
)

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location("kn3_flow_check", ROOT / "scripts/kn3_flow_check.py")
assert _SPEC is not None and _SPEC.loader is not None
CHECK = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = CHECK
_SPEC.loader.exec_module(CHECK)
IMPORTER = CHECK.IMPORTER

CATALOG = ROOT / "config/v2/stable-source-bindings.yaml"
EPOCH = 5


# ------------------------------------------------------------------ fake Redis + Rust-layout writer

class FakeRedis:
    """Hashes and lists; records every command name (reads must stay HMGET/HLEN/LRANGE)."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, bytes]] = {}
        self.lists: dict[str, list[bytes]] = {}
        self.commands: list[str] = []

    def hset(self, key, field, value):
        self.hashes.setdefault(key, {})[str(field)] = value if isinstance(value, bytes) else str(value).encode()

    def hdel(self, key, field):
        self.hashes.get(key, {}).pop(str(field), None)

    def lpush(self, key, value):
        self.lists.setdefault(key, []).insert(0, value.encode() if isinstance(value, str) else value)

    def hmget(self, key, fields):
        self.commands.append("HMGET")
        stored = self.hashes.get(key, {})
        return [stored.get(str(field)) for field in fields]

    def hlen(self, key):
        self.commands.append("HLEN")
        return len(self.hashes.get(key, {}))

    def lrange(self, key, start, stop):
        self.commands.append("LRANGE")
        return self.lists.get(key, [])[start:stop + 1]

    def pipeline(self, transaction=True):
        assert transaction is False
        client, calls = self, []

        class Pipe:
            def __getattr__(self, name):
                return lambda *args: calls.append((name, args))

            def execute(self):
                return [getattr(client, name)(*args) for name, args in calls]

        return Pipe()


def set_pointer(client, env: str, lpk: str, *, ready=None, staging=None, fence=1, written=None) -> None:
    key = f"kn3:{env}:ptr:{lpk}"
    if ready is not None:
        client.hset(key, "ready", str(ready))
    if staging is not None:
        client.hset(key, "staging", str(staging))
    client.hset(key, "fence", str(fence))
    if written is not None:
        written.append(key)


def write_bars(client, env: str, generation: int, lpk, interval: str, payloads, *, offset: int = MAX_OFFSET,
               floor: int | None = None, meta_rows_delta: int = 0, written=None) -> None:
    """``apply.lua`` op B for rows with distinct opens, then the ``bm`` meta."""

    interval_ms = canonical_interval_ms(interval)
    prefix = f"kn3:{env}:"
    opens, finals = [], []
    for payload in payloads:
        open_ms = open_ms_of(payload)
        if floor is not None and open_ms < floor:
            continue  # the floor removed it (op F)
        key = f"{prefix}b:{generation}:{lpk.encode()}:{bucket_of(open_ms, interval_ms)}"
        client.hset(key, str(open_ms), encode_bar_row(payload, lpk, offset, EPOCH))
        opens.append(open_ms)
        if market_data_pb2.EventEnvelope.FromString(payload).bar.is_final:
            finals.append(open_ms)
        if written is not None:
            written.append(key)
    meta = f"{prefix}bm:{generation}:{lpk.encode()}"
    fields = {"rows": len(opens) + meta_rows_delta}
    if opens:
        fields.update(first=max(min(opens), floor or 0), last=max(opens))
    if finals:
        fields["last_final"] = max(finals)
    if floor is not None:
        fields["floor"] = floor
    for name, value in fields.items():
        client.hset(meta, name, str(value))
    if written is not None:
        written.append(meta)


def write_latest(client, env: str, generation: int, lpk: str, canonical: bytes, *, topic_id: str, partition: int,
                 offset: int, written=None) -> None:
    """``apply.lua`` op L: ``l:<g>:<lpk>`` {v, t, p, o}."""

    key = f"kn3:{env}:l:{generation}:{lpk}"
    for name, value in (("v", encode_latest_value(canonical, offset, EPOCH)), ("t", topic_id),
                        ("p", str(partition)), ("o", str(offset))):
        client.hset(key, name, value)
    if written is not None:
        written.append(key)


def materialize_spool_bars(client, env: str, rows, *, drop=None, floor_for=None, written=None,
                           meta_rows_delta=0) -> dict[str, list[bytes]]:
    """Write every binding's expected spool finals (+ in-progress rows) as stage B would."""

    bindings, _info = IMPORTER.load_bar_bindings(CATALOG, env)
    by_pk = {item.physical_key: item for item in bindings}
    grouped: dict[str, list[bytes]] = {}
    for partition_key, payload in rows:
        grouped.setdefault(partition_key, []).append(payload)
    for partition_key, payloads in grouped.items():
        item = by_pk[partition_key]
        current: dict[int, bytes] = {}
        for payload in payloads:  # bar_revision_decision for these fixtures: higher revision / final wins
            envelope = market_data_pb2.EventEnvelope.FromString(payload)
            open_ms = open_ms_of(payload)
            existing = current.get(open_ms)
            if existing is not None:
                old = market_data_pb2.EventEnvelope.FromString(existing).bar
                if (old.is_final and not envelope.bar.is_final) or (
                        old.is_final and envelope.bar.revision <= old.revision):
                    continue
            current[open_ms] = payload
        keep = [payload for open_ms, payload in sorted(current.items()) if drop is None or not drop(item, open_ms)]
        floor = floor_for(item, sorted(current)) if floor_for else None
        set_pointer(client, env, item.lpk.encode(), ready=4, written=written)
        write_bars(client, env, 4, item.lpk, item.binding.interval, keep, floor=floor, written=written,
                   meta_rows_delta=meta_rows_delta)
    return grouped


def run_cli(args: list[str], client=None) -> tuple[int, dict]:
    out = Path(args[args.index("--out") + 1])
    with redirect_stdout(io.StringIO()):
        code = CHECK.main(args, client=client)
    return code, json.loads(out.read_text(encoding="utf-8"))


def write_bundle(path: Path, env: str) -> dict:
    bundle = IMPORTER._load_bundle_module().compile_bundle(environment=env, catalog_path=CATALOG, manifest_paths=())
    path.write_text(json.dumps(bundle), encoding="utf-8")
    return bundle


def golden_latest_records() -> list[tuple[str, bytes]]:
    return golden_records(bar=False)


# ------------------------------------------------------------------ unit cases

class BarComparisonTests(unittest.TestCase):
    def setUp(self) -> None:
        bindings, _info = IMPORTER.load_bar_bindings(CATALOG, "paper")
        pk, self.real = next((pk, p) for pk, p in golden_records(bar=True)
                             if pk.endswith("okx-swap-doge-usdt-swap-bar-1m-primary-v2"))
        self.item = next(item for item in bindings if item.physical_key == pk)
        self.base = open_ms_of(self.real)
        self.step = canonical_interval_ms("1m")

    def facts(self, payloads):
        out = []
        for index, payload in enumerate(payloads):
            bar = market_data_pb2.EventEnvelope.FromString(payload).bar
            out.append((open_ms_of(payload), bar.is_final, bar.lifecycle, bar.revision,
                        hashlib.sha256(payload).hexdigest(), index))
        return out

    def row(self, payload):
        return encode_bar_row(payload, self.item.lpk, MAX_OFFSET, EPOCH)

    def test_expected_is_the_highest_final_revision_per_open(self):
        rev1 = derived_bar(self.real, -1, revision=1)
        rev0 = derived_bar(self.real, -1)
        progress = derived_bar(self.real, 1, final=False)
        expected = CHECK.spool_expected(self.facts([rev0, rev1, self.real, progress]))
        self.assertEqual(sorted(expected), [self.base - self.step, self.base])
        self.assertEqual(expected[self.base - self.step].content_sha256, hashlib.sha256(rev1).hexdigest())
        self.assertEqual(expected[self.base - self.step].logical_offset, 1)

    def test_every_open_is_classified(self):
        payloads = {shift: derived_bar(self.real, shift) for shift in range(-6, 0)}
        payloads[0] = self.real
        expected = CHECK.spool_expected(self.facts(list(payloads.values())))
        opens = {shift: self.base + shift * self.step for shift in payloads}
        higher = derived_bar(payloads[-2], 0, revision=2)  # a later venue revision at the same open
        conflict = derived_bar(payloads[-3], 0, source_id=None)  # same revision, other content
        cache_rows = {
            opens[0]: self.row(self.real),                               # equal
            opens[-1]: None,                                             # missing
            opens[-2]: self.row(higher),                                 # differs, higher revision
            opens[-3]: self.row(conflict),                               # differs, cx record
            opens[-4]: self.row(derived_bar(payloads[-4], 0, final=False)),  # in progress in cache
            opens[-5]: b"\x00" * 60,                                     # undecodable
            opens[-6]: None,                                             # below the floor (deleted)
        }
        cx = [json.dumps({"open_ms": opens[-3], "revision": 0, "kept": hashlib.sha256(conflict).hexdigest(),
                          "refused": hashlib.sha256(payloads[-3]).hexdigest(), "offset": MAX_OFFSET})]
        for floor_aware in (False, True):
            with self.subTest(floor_aware=floor_aware):
                result = CHECK.compare_bars(expected, cache_rows, self.item.binding, self.item.lpk,
                                            floor=opens[-5], floor_aware=floor_aware, conflicts=cx)
                self.assertEqual(result["spool_final"], 7)
                self.assertEqual(result["below_floor"], 1)
                self.assertEqual(result["checked"], 6 if floor_aware else 7)
                self.assertEqual(result["missing"], 2 if floor_aware else 3)
                self.assertEqual((result["covered"], result["content_equal"], result["content_differs"],
                                  result["undecodable"]), (3, 1, 2, 1))
                self.assertEqual(result["covered_opens"], {opens[0], opens[-2], opens[-3]})
                explained = {item["open_ms"]: item["explained_by"] for item in result["details"]
                             if item["kind"] == "content_differs"}
                self.assertEqual(explained, {opens[-2]: "higher_revision", opens[-3]: "conflict_record"})
                reasons = {item["open_ms"]: item["reason"] for item in result["details"] if item["kind"] == "missing"}
                self.assertEqual(reasons[opens[-4]], "not_final_in_cache")

    def test_a_row_of_another_source_is_an_identity_mismatch(self):
        expected = CHECK.spool_expected(self.facts([self.real]))
        foreign = derived_bar(self.real, 0, source_id="okx-other")
        result = CHECK.compare_bars(expected, {self.base: self.row(foreign)}, self.item.binding, self.item.lpk,
                                    floor=None, floor_aware=False)
        self.assertEqual((result["identity_mismatch"], result["covered"]), (1, 0))


class BarsCheckTests(unittest.TestCase):
    """``bars`` end to end: real spool code + the fake cache in the Rust layout."""

    ENV = "paper"

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="kn3-k36-flow-")
        self.dir = Path(self._tmp.name)
        self.sqlite = self.dir / "canonical-cache.sqlite3"
        self.rows = standard_rows()
        build_spool(self.sqlite, self.rows)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def bars(self, client, *extra):
        return run_cli(["bars", "--sqlite", str(self.sqlite), "--catalog", str(CATALOG), "--environment", self.ENV,
                        "--cache-url", "redis://unused", "--out", str(self.dir / "bars.json"), "--page-rows", "2",
                        *extra], client=client)

    def test_the_checker_buckets_with_the_readback_rule(self):
        # One owner of the bucket rule (112 opens, `cache.rs` BUCKET_OPENS):
        # the checker uses the readback's function, never its own literal.
        self.assertIs(CHECK.bucket_of, kn_bar_readback.bucket_of)
        self.assertEqual(kn_bar_readback.BUCKET_OPENS, 112)
        self.assertNotRegex((ROOT / "scripts/kn3_flow_check.py").read_text(encoding="utf-8"),
                            r"\b11[26]\b")

    def test_a_faithful_cache_passes_and_reads_only(self):
        client = FakeRedis()
        materialize_spool_bars(client, self.ENV, self.rows)
        code, receipt = self.bars(client)
        self.assertEqual((code, receipt["verdict"]), (0, "PASS"), receipt["failures"])
        totals = receipt["totals"]
        self.assertEqual((totals["spool_rows"], totals["spool_final"], totals["covered"], totals["content_equal"],
                          totals["missing"], totals["content_differs"]), (47, 45, 45, 45, 0, 0))
        self.assertEqual(totals["bindings_with_spool_final"], 15)
        with_rows = [p for p in receipt["products"] if p["spool_final"]]
        self.assertTrue(all(p["readback_equal"] and p["meta_rows_equal"] for p in with_rows))
        self.assertEqual(set(client.commands), {"HMGET", "HLEN"})

    def test_missing_rows_meta_drift_and_not_ready_fail(self):
        dropped = FakeRedis()
        materialize_spool_bars(dropped, self.ENV, self.rows,
                               drop=lambda item, open_ms: item.binding.interval == "1h" and open_ms == min(
                                   open_ms_of(p) for pk, p in self.rows if pk == item.physical_key))
        code, receipt = self.bars(dropped)
        self.assertEqual((code, receipt["verdict"]), (1, "FAIL"))
        self.assertEqual(receipt["totals"]["missing"], 1)
        failing = {f["binding_id"]: f["failures"] for f in receipt["failures"]}
        self.assertEqual(failing, {"okx-swap-doge-usdt-swap-bar-1h": ["bar_parity"]})
        product = next(p for p in receipt["products"] if p["binding_id"] == "okx-swap-doge-usdt-swap-bar-1h")
        self.assertTrue(product["readback_equal"])  # the readback agrees on what is covered

        drift = FakeRedis()
        materialize_spool_bars(drift, self.ENV, self.rows, meta_rows_delta=1)
        _code, receipt = self.bars(drift)
        self.assertEqual({tuple(f["failures"]) for f in receipt["failures"]}, {("meta_rows",)})

        not_ready = FakeRedis()
        _code, receipt = self.bars(not_ready)
        self.assertEqual(receipt["verdict"], "FAIL")
        self.assertEqual(len(receipt["failures"]), 15)
        self.assertTrue(all(f["failures"] == ["not_ready"] for f in receipt["failures"]))
        self.assertEqual(receipt["totals"]["missing"], 45)

    def test_floor_aware_excludes_opens_below_the_floor(self):
        client = FakeRedis()
        materialize_spool_bars(client, self.ENV, self.rows, floor_for=lambda item, opens: opens[1])
        code, receipt = self.bars(client)
        self.assertEqual(code, 1)
        self.assertEqual(receipt["totals"]["below_floor"], 15)
        code, receipt = self.bars(client, "--floor-aware")
        self.assertEqual((code, receipt["verdict"]), (0, "PASS"), receipt["failures"])
        self.assertEqual(receipt["totals"]["checked"], 45 - 15)

    def test_an_unreadable_spool_is_refused(self):
        code, receipt = run_cli(["bars", "--sqlite", str(self.dir / "absent.sqlite3"), "--environment", self.ENV,
                                 "--cache-url", "redis://unused", "--out", str(self.dir / "r.json")],
                                client=FakeRedis())
        self.assertEqual((code, receipt["verdict"]), (4, "REFUSED"))

    def test_an_import_receipt_limits_the_check_to_the_imported_window(self):
        import sqlite3

        export = self.dir / "import-receipt.json"
        with redirect_stdout(io.StringIO()):
            self.assertEqual(IMPORTER.main(["export", "--sqlite", str(self.sqlite), "--catalog", str(CATALOG),
                                            "--environment", self.ENV, "--partitions", "6",
                                            "--materializer-epoch", "1", "--receipt", str(export)]), 0)
        client = FakeRedis()
        materialize_spool_bars(client, self.ENV, self.rows)  # the cache holds what was imported
        # After the import the live spool appends newer opens and trims the oldest row of one binding.
        appended = [(pk, derived_bar(payload, 3)) for pk, payload in golden_records(bar=True)]
        build_spool(self.sqlite, appended)
        trimmed_pk = next(pk for pk, _p in golden_records(bar=True)
                          if pk.endswith("binance-usdm-dogeusdt-bar-1d-primary-v2"))
        writer = sqlite3.connect(self.sqlite)  # test fixture: the spool's retention trim
        writer.execute("DELETE FROM events WHERE partition_key = ? AND logical_offset = "
                       "(SELECT MIN(logical_offset) FROM events WHERE partition_key = ?)", (trimmed_pk, trimmed_pk))
        writer.commit()
        writer.close()

        code, receipt = self.bars(client)
        self.assertEqual(code, 1)
        self.assertEqual(receipt["totals"]["missing"], len(appended))  # the false missing of the live spool
        code, receipt = self.bars(client, "--import-receipt", str(export))
        self.assertEqual((code, receipt["verdict"]), (0, "PASS"), receipt["failures"])
        totals = receipt["totals"]
        self.assertEqual((totals["appended_since_import"], totals["trimmed_since_import"]), (len(appended), 1))
        self.assertEqual(totals["checked"], 45 - 1)
        sealed = json.loads(export.read_text(encoding="utf-8"))
        self.assertEqual(receipt["inputs"]["import_receipt"]["receipt_sha256"], sealed["receipt_sha256"])
        self.assertEqual(receipt["inputs"]["import_receipt"]["export_sha256"], sealed["export_sha256"])
        trimmed = next(p for p in receipt["products"] if p["binding_id"] == "binance-usdm-dogeusdt-bar-1d")
        self.assertEqual((trimmed["trimmed_since_import"], trimmed["spool_rows_at_import"], trimmed["spool_rows"]),
                         (1, 4, 3))
        self.assertTrue(all(p.get("import_rows_accounted") for p in receipt["products"]))

        tampered = dict(sealed, totals=dict(sealed["totals"], rows=1))
        cases = {
            "tampered": (tampered, self.ENV),
            "resealed without export hash": (dict(tampered, receipt_sha256=IMPORTER.canonical_sha256(
                {k: v for k, v in tampered.items() if k != "receipt_sha256"})), self.ENV),
            "other environment": (sealed, "live"),
        }
        for label, (document, environment) in cases.items():
            with self.subTest(label):
                path = self.dir / "bad-receipt.json"
                path.write_text(json.dumps(document), encoding="utf-8")
                code, refused = run_cli(["bars", "--sqlite", str(self.sqlite), "--environment", environment,
                                         "--cache-url", "redis://unused", "--out", str(self.dir / "refused.json"),
                                         "--import-receipt", str(path)], client=client)
                self.assertEqual((code, refused["verdict"]), (4, "REFUSED"))
        bindings, info = IMPORTER.load_bar_bindings(CATALOG, self.ENV)
        with self.assertRaisesRegex(CHECK.Refused, "catalog"):
            CHECK.load_import_receipt(export, environment=self.ENV, catalog_info=dict(info, revision=1),
                                      bindings=bindings)
        other = self.dir / "other.sqlite3"
        build_spool(other, self.rows)  # another spool generation (another cache_id)
        code, refused = run_cli(["bars", "--sqlite", str(other), "--environment", self.ENV, "--cache-url",
                                 "redis://unused", "--out", str(self.dir / "o.json"), "--import-receipt",
                                 str(export)], client=client)
        self.assertEqual((code, refused["verdict"]), (4, "REFUSED"))
        self.assertIn("cache_id", refused["failures"][0]["error"])


class LatestAndReadyTests(unittest.TestCase):
    ENV = "paper"

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="kn3-k36-flow-")
        self.dir = Path(self._tmp.name)
        self.bundle_path = self.dir / "bundle.json"
        self.bundle = write_bundle(self.bundle_path, self.ENV)
        _doc, products = CHECK.load_bundle(self.bundle_path, self.ENV)
        self.by_pair = {(item.physical_key, item.feed): item.lpk for item in products}

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def product_of(self, key, feed):
        return self.by_pair.get((key, feed))

    def test_the_oracle_keeps_the_last_record_per_product_and_excludes_bars(self):
        records = []
        offset = 0
        for key, value in golden_records():
            records.append((0, offset, key.encode(), value))
            offset += 1
        latest = golden_latest_records()
        records.append((0, offset, latest[0][0].encode(), latest[0][1]))  # a later copy wins
        records.append((0, offset + 1, b"unknown/trade/x", latest[0][1]))
        oracle, stats = CHECK.latest_oracle(records, self.product_of)
        self.assertEqual(stats["bar_records"], 15)
        self.assertEqual(stats["unmapped"], 1)
        self.assertEqual(len(oracle), len(latest))  # book snapshot and delta of one key are two products
        feeds = sorted(CHECK.payload_feed(value) for _k, value in latest)
        self.assertEqual(feeds.count("BOOK_DELTA") + feeds.count("BOOK_SNAPSHOT"), 3)
        first_lpk = self.product_of(latest[0][0], CHECK.payload_feed(latest[0][1]))
        self.assertEqual(oracle[first_lpk].offset, offset)
        _o, stats = CHECK.latest_oracle(records + [(1, 99, latest[0][0].encode(), latest[0][1])], self.product_of)
        self.assertEqual(stats["ambiguous"], [first_lpk])

    def test_latest_comparison_categories(self):
        _key, canonical = golden_latest_records()[0]
        oracle = CHECK.OracleRecord(2, 40, canonical)
        ready = CHECK.Pointer(7, None, 1)

        def fields(offset, *, partition=2, topic="tid", value=None):
            return [value or encode_latest_value(canonical, offset, EPOCH), topic.encode(), str(partition).encode(),
                    str(offset).encode()]

        cases = [
            (oracle, ready, fields(40), "equal"),
            (oracle, ready, fields(39), "stale"),
            (oracle, ready, fields(41), "ahead"),
            (oracle, ready, None, "missing"),
            (oracle, CHECK.Pointer(None, 3, 0), None, "not_ready"),
            (oracle, ready, fields(40, partition=1), "mismatch"),
            (oracle, ready, fields(40, topic="other"), "mismatch"),
            (oracle, ready, fields(40, value=encode_latest_value(golden_latest_records()[1][1], 40, EPOCH)),
             "mismatch"),
            (None, ready, fields(40), "unexpected"),
            (None, CHECK.Pointer(None, None, 0), None, "absent"),
        ]
        for oracle_record, pointer, cache_fields, expected in cases:
            with self.subTest(expected):
                result, _info = CHECK.compare_latest(oracle_record, pointer, cache_fields, "tid")
                self.assertEqual(result, expected)
        self.assertEqual(CHECK.compare_latest(oracle, ready, fields(40, topic="other"), None)[0], "equal")

    def test_check_latest_and_ready_over_the_fake_cache(self):
        client = FakeRedis()
        records, lpks = [], []
        for offset, (key, value) in enumerate(golden_records()):
            records.append((0, offset, key.encode(), value))
            lpk = self.product_of(key, CHECK.payload_feed(value))
            if CHECK.payload_feed(value) != "BAR":
                set_pointer(client, self.ENV, lpk, ready=2)
                write_latest(client, self.ENV, 2, lpk, value, topic_id="tid", partition=0, offset=offset)
                lpks.append(lpk)
        receipt = CHECK.check_latest(bundle_path=self.bundle_path, environment=self.ENV, client=client,
                                     records=records, cutoff={}, topic_id="tid")
        self.assertEqual(receipt["verdict"], "PASS", receipt["failures"])
        self.assertEqual(receipt["totals"]["equal"], len(lpks))
        self.assertEqual(set(client.commands), {"HMGET"})
        write_latest(client, self.ENV, 2, lpks[0], golden_latest_records()[0][1], topic_id="tid", partition=0,
                     offset=0)
        receipt = CHECK.check_latest(bundle_path=self.bundle_path, environment=self.ENV, client=client,
                                     records=records, cutoff={}, topic_id="tid")
        self.assertEqual(receipt["verdict"], "FAIL")
        self.assertEqual(receipt["totals"]["stale"] + receipt["totals"]["mismatch"], 1)

        set_pointer(client, self.ENV, lpks[1], ready=2, staging=9)
        set_pointer(client, "paper", "lpk1|paper|X|Y|z|TRADE|-", staging=3)  # not in the bundle: ignored
        ready = CHECK.check_ready(bundle_path=self.bundle_path, environment=self.ENV, client=client)
        self.assertEqual(ready["verdict"], "FAIL")
        self.assertEqual(ready["totals"]["READY"] + ready["totals"]["REBUILDING"], len(lpks))
        self.assertEqual(ready["totals"]["REBUILDING"], 1)
        self.assertEqual(ready["totals"]["products"], len(self.bundle["catalog"]["bindings"]))
        self.assertEqual(sum(sum(c.values()) for c in ready["per_feed"].values()), ready["totals"]["products"])

    def test_a_tampered_or_foreign_bundle_is_refused(self):
        tampered = dict(self.bundle, environment="other")
        path = self.dir / "tampered.json"
        path.write_text(json.dumps(tampered), encoding="utf-8")
        with self.assertRaisesRegex(CHECK.Refused, "hash"):
            CHECK.load_bundle(path, "other")
        with self.assertRaisesRegex(CHECK.Refused, "environment"):
            CHECK.load_bundle(self.bundle_path, "live")
        code, receipt = run_cli(["ready", "--bundle", str(path), "--environment", "other", "--cache-url",
                                 "redis://unused", "--out", str(self.dir / "r.json")], client=FakeRedis())
        self.assertEqual((code, receipt["verdict"]), (4, "REFUSED"))


# ------------------------------------------------------------------ integration

@unittest.skipUnless(os.environ.get("QDL_KN_TEST_KAFKA") and os.environ.get("QDL_KN_TEST_REDIS"),
                     "needs QDL_KN_TEST_KAFKA and QDL_KN_TEST_REDIS (disposable broker and Redis)")
class FlowCheckIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        import redis
        from confluent_kafka.admin import AdminClient, NewTopic

        self.bootstrap = os.environ["QDL_KN_TEST_KAFKA"]
        self.url = os.environ["QDL_KN_TEST_REDIS"]
        self.redis = redis.Redis.from_url(self.url)
        run = uuid.uuid4().hex[:8]
        self.env = f"k36{run}"
        self.topic = f"kn3-k36-canonical-{run}"
        self.written: list[str] = []
        self._tmp = tempfile.TemporaryDirectory(prefix="kn3-k36-flow-")
        self.dir = Path(self._tmp.name)
        self.admin = AdminClient({"bootstrap.servers": self.bootstrap})
        for future in self.admin.create_topics([NewTopic(self.topic, 3, 1)]).values():
            future.result(60)

    def tearDown(self) -> None:
        for future in self.admin.delete_topics([self.topic]).values():
            future.result(60)
        if self.written:
            self.redis.delete(*set(self.written))
        self.redis.close()
        self._tmp.cleanup()

    def produce(self) -> dict[tuple[str, str], tuple[int, int, bytes]]:
        """Two committed copies of every golden record, then an aborted newer copy."""

        from confluent_kafka import Producer

        producer = Producer({"bootstrap.servers": self.bootstrap, "transactional.id": f"kn3-k36-test-{self.env}",
                             "enable.idempotence": True})
        producer.init_transactions(60)
        keys = sorted({key for key, _v in golden_records()})
        partition_of = {key: index % 3 for index, key in enumerate(keys)}
        delivered: dict[tuple[str, str], tuple[int, int, bytes]] = {}

        def remember(error, message):
            assert error is None, error
            key = message.key().decode()
            delivered[(key, CHECK.payload_feed(message.value()))] = (message.partition(), message.offset(),
                                                                      message.value())

        for _round in range(2):
            producer.begin_transaction()
            for key, value in golden_records():
                producer.produce(self.topic, key=key.encode(), value=value, partition=partition_of[key],
                                 on_delivery=remember)
            producer.commit_transaction(60)
        committed = dict(delivered)
        producer.begin_transaction()
        key, value = golden_latest_records()[0]
        newer = market_data_pb2.EventEnvelope.FromString(value)
        newer.event_id = hashlib.sha256(b"k36-test|aborted").digest()[:16]
        producer.produce(self.topic, key=key.encode(), value=newer.SerializeToString(), partition=partition_of[key])
        producer.flush(30)
        producer.abort_transaction(60)
        return committed

    def test_latest_bars_and_ready_against_real_services(self):
        bundle_path = self.dir / "bundle.json"
        write_bundle(bundle_path, self.env)
        _doc, products = CHECK.load_bundle(bundle_path, self.env)
        by_pair = {(item.physical_key, item.feed): item.lpk for item in products}
        committed = self.produce()
        latest_lpks = []
        for (key, feed), (partition, offset, value) in committed.items():
            if feed == "BAR":
                continue
            lpk = by_pair[(key, feed)]
            set_pointer(self.redis, self.env, lpk, ready=3, written=self.written)
            write_latest(self.redis, self.env, 3, lpk, value, topic_id="tid-k36", partition=partition,
                         offset=offset, written=self.written)
            latest_lpks.append(lpk)
        latest_args = ["latest", "--bootstrap", self.bootstrap, "--canonical-topic", self.topic, "--bundle",
                       str(bundle_path), "--environment", self.env, "--cache-url", self.url, "--topic-id", "tid-k36",
                       "--timeout-s", "120", "--out", str(self.dir / "latest.json")]
        code, receipt = run_cli(latest_args)
        self.assertEqual((code, receipt["verdict"]), (0, "PASS"), receipt["failures"])
        self.assertEqual(receipt["totals"]["equal"], len(latest_lpks))
        self.assertEqual(receipt["totals"]["canonical_records"], 2 * len(golden_records()))  # aborted excluded
        stale_key = f"kn3:{self.env}:l:3:{latest_lpks[0]}"
        good = self.redis.hgetall(stale_key)
        partition, offset, value = next(v for (k, f), v in committed.items() if by_pair.get((k, f)) == latest_lpks[0])
        write_latest(self.redis, self.env, 3, latest_lpks[0], value, topic_id="tid-k36", partition=partition,
                     offset=offset - 1)
        code, receipt = run_cli(latest_args)
        self.assertEqual((code, receipt["verdict"], receipt["totals"]["stale"]), (1, "FAIL", 1))
        self.redis.hset(stale_key, mapping=good)

        sqlite = self.dir / "canonical-cache.sqlite3"
        rows = standard_rows()
        build_spool(sqlite, rows)
        materialize_spool_bars(self.redis, self.env, rows, written=self.written)
        code, receipt = run_cli(["bars", "--sqlite", str(sqlite), "--catalog", str(CATALOG), "--environment",
                                 self.env, "--cache-url", self.url, "--out", str(self.dir / "bars.json")])
        self.assertEqual((code, receipt["verdict"]), (0, "PASS"), receipt["failures"])
        self.assertEqual(receipt["totals"]["content_equal"], 45)

        code, receipt = run_cli(["ready", "--bundle", str(bundle_path), "--environment", self.env, "--cache-url",
                                 self.url, "--out", str(self.dir / "ready.json")])
        self.assertEqual(code, 1)  # only the fixture products are materialized
        self.assertEqual(receipt["totals"]["READY"], len(latest_lpks) + 15)
        out = os.environ.get("QDL_KN_TEST_EVIDENCE_OUT")
        if out:
            Path(out).write_text(json.dumps({"ready_totals": receipt["totals"]}, sort_keys=True) + "\n",
                                 encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
