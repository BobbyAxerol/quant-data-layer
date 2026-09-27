"""KN-1 review F5/F6: the sizing helper's safety and the contract-complete row.

F6: ``redis_memory_with`` refuses a non-empty target, writes only under its
per-run namespace, deletes exactly what it wrote (also on error) and never
sends FLUSHALL/FLUSHDB. F5 (R2): the BAR row keeps every field except the
LPK-derived ones, and one product key restores every row of a mixed history
byte-for-byte - schema/source/provider transitions, correction, rebuild.
"""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
import sys
import unittest
from unittest import mock

from qdl.marketdata.v2 import market_data_pb2
from qdl.projection.state_contract import LogicalProductKey

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location("kn_resource_sizing", ROOT / "scripts/kn_resource_sizing.py")
assert _SPEC is not None and _SPEC.loader is not None
SIZING = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = SIZING
_SPEC.loader.exec_module(SIZING)


class _Pipeline:
    def __init__(self, client: "FakeRedis") -> None:
        self.client = client
        self.calls: list[tuple] = []

    def zadd(self, key, mapping):
        self.calls.append(("zadd", key, mapping))

    def hset(self, key, field, value):
        self.calls.append(("hset", key, field, value))

    def execute(self):
        for call in self.calls:
            self.client.apply(call)
        self.calls.clear()


class FakeRedis:
    """Records every command; refuses the flush commands outright."""

    def __init__(self, keys: dict | None = None, fail_on_set: bool = False) -> None:
        self.data: dict[str, object] = dict(keys or {})
        self.commands: list[str] = []
        self.fail_on_set = fail_on_set

    def apply(self, call):
        name, key = call[0], call[1]
        self.commands.append(name)
        if name == "zadd":
            self.data.setdefault(key, {}).update(call[2])
        else:
            self.data.setdefault(key, {})[call[2]] = call[3]

    def dbsize(self):
        return len(self.data)

    def info(self, section):
        return {"used_memory": 1000 + 10 * len(self.data)} if section == "memory" else {"redis_version": "fake"}

    def pipeline(self, transaction=False):
        return _Pipeline(self)

    def memory_usage(self, key):
        return 64

    def object(self, what, key):
        return b"listpack"

    def set(self, key, value):
        if self.fail_on_set:
            raise ConnectionError("simulated failure mid-run")
        self.commands.append("set")
        self.data[key] = value

    def delete(self, *keys):
        self.commands.append("delete")
        for key in keys:
            self.data.pop(key, None)

    def flushall(self, *args, **kwargs):  # pragma: no cover - must never be called
        raise AssertionError("FLUSHALL sent")

    flushdb = flushall


def _sample() -> dict:
    return {"bar": {"open_ms": [60_000, 120_000, 180_000], "final_open_ms": [60_000, 120_000],
                    "canonical": ["0a01", "0a02", "0a03"], "public": ['{"a":1}', '{"a":2}', '{"a":3}'],
                    "compact": ['{"c":1}', '{"c":2}'], "complete": ['{"x":1}', '{"x":2}'],
                    "canonical_state": ["00" * 49, "01" * 49],
                    "lpk_state": ["00" * 49, "02" * 49]},
            "latest": {"TRADE": {"canonical": "0a0b", "public": '{"t":1}'}}}


class RedisSizingSafetyTests(unittest.TestCase):
    def test_a_non_empty_target_is_refused_before_any_write(self):
        client = FakeRedis({"production:key": "value"})
        with self.assertRaises(SIZING.NonEmptyTarget):
            SIZING.redis_memory_with(client, _sample(), run_id="t1")
        self.assertEqual(client.commands, [])
        self.assertEqual(client.data, {"production:key": "value"})

    def test_only_namespaced_keys_are_written_and_all_are_deleted(self):
        client = FakeRedis()
        written: set[str] = set()
        original = client.apply

        def spy(call):
            written.add(call[1])
            original(call)

        client.apply = spy
        result = SIZING.redis_memory_with(client, _sample(), run_id="t2")
        self.assertTrue(written)
        self.assertTrue(all(key.startswith("kn-sizing:t2:") for key in written), written)
        self.assertEqual(client.data, {})
        self.assertEqual(result["keys_left_after_cleanup"], 0)
        self.assertGreaterEqual(result["keys_written"], len(written))
        self.assertNotIn("flushall", client.commands)
        # Final rows stay aligned with their own open times.
        self.assertEqual(result["bar_complete"]["rows"], 2)
        self.assertEqual(result["bar_canonical"]["rows"], 3)

    def test_cleanup_runs_when_the_measurement_fails(self):
        client = FakeRedis(fail_on_set=True)
        with self.assertRaises(ConnectionError):
            SIZING.redis_memory_with(client, _sample(), run_id="t3")
        self.assertEqual(client.data, {})

    def test_string_key_sizing_is_guarded_and_cleans_up(self):
        with self.assertRaises(SIZING.NonEmptyTarget):
            SIZING.string_key_usage_with(FakeRedis({"x": 1}), {"TRADE": 10}, run_id="t4")
        client = FakeRedis()
        result = SIZING.string_key_usage_with(client, {"TRADE": 10, "BOOK": 20}, run_id="t5")
        self.assertEqual(set(result["usage_bytes"]), {"TRADE", "BOOK"})
        self.assertEqual(client.data, {})
        self.assertEqual(result["keys_left_after_cleanup"], 0)

    def test_the_script_never_sends_a_flush(self):
        source = (ROOT / "scripts/kn_resource_sizing.py").read_text(encoding="utf-8")
        self.assertNotIn(".flushall(", source)
        self.assertNotIn(".flushdb(", source)


def _envelope() -> tuple[market_data_pb2.EventEnvelope, bytes]:
    envelope = market_data_pb2.EventEnvelope(
        schema_name="qdl.marketdata.v2", schema_major=2, event_id=bytes(range(16)),
        instrument_uid="fb26214c-7b9b-5961-95b2-55154755af0f", venue="OKX", market="SWAP",
        source_event_time_ns=1, received_at_ns=2, normalized_at_ns=3, published_at_ns=4,
        source_sequence="s1", partition_sequence=5, quality_flags=[1],
        canonical_payload_hash=b"\x01" * 32)
    bar = envelope.bar
    bar.interval = "1m"
    bar.open_time_ns = 60_000_000_000
    bar.close_time_ns = 119_999_999_999
    for name, text in (("open", "1.5"), ("high", "2"), ("low", "1"), ("close", "1.75"), ("volume", "10")):
        field = getattr(bar, name)
        field.source_text = text
        digits = text.replace(".", "")
        field.mantissa = int(digits)
        field.scale = len(text.split(".")[1]) if "." in text else 0
    bar.is_final = True
    bar.revision = 1
    bar.supersedes_event_id = b"\x02" * 16
    return envelope, envelope.SerializeToString()


LPK = LogicalProductKey.for_product(
    environment="paper", venue="OKX", market="SWAP", instrument_uid="fb26214c-7b9b-5961-95b2-55154755af0f",
    feed="BAR", interval="1m")


def _history() -> list[tuple[str, bytes]]:
    """One product's history across the transitions Astra reproduced (R2):
    schema minor, source id, provider - plus source role, native symbol,
    instrument id/revision, schema name and a repair revision."""

    base, _ = _envelope()
    base.schema_minor, base.provider, base.source_id = 1, "okx", "okx-business-001"
    variants = [("base", {}), ("schema_minor", {"schema_minor": 2}),
                ("source_id", {"source_id": "okx-business-002"}), ("provider", {"provider": "okx-backup"}),
                ("source_role", {"source_role": 2}), ("native_symbol", {"native_symbol": "DOGE-USDT-SWAP-v2"}),
                ("instrument", {"instrument_id": "okx:DOGE-USDT-SWAP:2", "instrument_revision": 2}),
                ("schema_name", {"schema_name": "qdl.marketdata.v2.next"})]
    history = []
    for index, (name, changes) in enumerate(variants):
        envelope = market_data_pb2.EventEnvelope()
        envelope.CopyFrom(base)
        envelope.event_id = bytes([index]) * 16
        envelope.bar.open_time_ns += index * 60_000_000_000
        for field, value in changes.items():
            setattr(envelope, field, value)
        history.append((name, envelope.SerializeToString()))
    repair = market_data_pb2.EventEnvelope()
    repair.CopyFrom(base)
    repair.event_id = b"\x77" * 16
    repair.bar.revision = 2
    repair.bar.supersedes_event_id = bytes(16)
    repair.bar.close.source_text, repair.bar.close.mantissa, repair.bar.close.scale = "1.80", 180, 2
    history.append(("correction", repair.SerializeToString()))
    return history


class ContractCompleteRowTests(unittest.TestCase):
    def test_every_non_identity_field_is_carried(self):
        import json

        envelope, payload = _envelope()
        row = json.loads(SIZING._complete_bar(envelope, payload))
        self.assertEqual(row["b"]["revision"], 1)
        self.assertTrue(row["b"]["is_final"])
        self.assertEqual(row["b"]["supersedes_event_id"], "02" * 16)
        self.assertEqual(row["e"]["event_id"], bytes(range(16)).hex())
        self.assertEqual(row["e"]["quality_flags"], [1])
        self.assertEqual(row["s"]["content_sha256"], hashlib.sha256(payload).hexdigest())
        self.assertEqual(row["s"]["source_offset"], 2**63 - 1)
        self.assertEqual(row["s"]["materializer_epoch"], 2**63 - 1)

    def test_an_unmapped_proto_field_fails_the_measurement(self):
        envelope, payload = _envelope()
        with mock.patch.object(SIZING, "LPK_DERIVED_ENVELOPE_FIELDS", ("instrument_uid",)):
            with self.assertRaises(ValueError) as raised:
                SIZING._complete_bar(envelope, payload)
        self.assertIn("venue", str(raised.exception))

    def test_mixed_history_decodes_exactly_with_the_one_product_key(self):
        # Every row of one product, across schema/source/provider transitions
        # and a correction, is restored byte-for-byte from the LPK alone.
        for name, payload in _history():
            with self.subTest(name):
                envelope = market_data_pb2.EventEnvelope.FromString(payload)
                row = SIZING.lpk_row(envelope, payload, LPK)
                decoded_bytes = SIZING.lpk_row_decode(row, LPK, market_data_pb2.EventEnvelope)
                self.assertEqual(decoded_bytes, payload)
                self.assertEqual(row[16:48], hashlib.sha256(payload).digest())
                decoded = market_data_pb2.EventEnvelope.FromString(decoded_bytes)
                for field in ("schema_name", "schema_minor", "provider", "source_id", "source_role",
                              "native_symbol", "instrument_id", "instrument_revision"):
                    self.assertEqual(getattr(decoded, field), getattr(envelope, field), field)

    def test_rebuild_from_decoded_rows_is_idempotent(self):
        rows = [SIZING.lpk_row(market_data_pb2.EventEnvelope.FromString(p), p, LPK) for _, p in _history()]
        decoded = [SIZING.lpk_row_decode(r, LPK, market_data_pb2.EventEnvelope) for r in rows]
        rebuilt = [SIZING.lpk_row(market_data_pb2.EventEnvelope.FromString(p), p, LPK) for p in decoded]
        self.assertEqual(rebuilt, rows)
        self.assertEqual(decoded, [p for _, p in _history()])

    def test_a_header_copied_from_one_row_would_not_be_lossless(self):
        # The R2 counterexample, kept as a guard: provider is not part of the
        # LPK, so the row must carry it; restoring the first row's provider
        # into the second row changes its bytes.
        history = dict(_history())
        first = market_data_pb2.EventEnvelope.FromString(history["base"])
        second = market_data_pb2.EventEnvelope.FromString(history["provider"])
        second.provider = first.provider
        self.assertNotEqual(second.SerializeToString(), history["provider"])
        row = SIZING.lpk_row(market_data_pb2.EventEnvelope.FromString(history["provider"]),
                             history["provider"], LPK)
        self.assertEqual(market_data_pb2.EventEnvelope.FromString(row[48:]).provider, "okx-backup")

    def test_a_row_of_another_product_is_refused(self):
        _, payload = _history()[0]
        envelope = market_data_pb2.EventEnvelope.FromString(payload)
        for field, value in (("venue", "BINANCE"), ("market", "USDM"), ("instrument_uid", "x")):
            other = market_data_pb2.EventEnvelope()
            other.CopyFrom(envelope)
            setattr(other, field, value)
            with self.subTest(field), self.assertRaises(ValueError):
                SIZING.lpk_row(other, other.SerializeToString(), LPK)
        other = market_data_pb2.EventEnvelope()
        other.CopyFrom(envelope)
        other.bar.interval = "5m"
        with self.assertRaises(ValueError):
            SIZING.lpk_row(other, other.SerializeToString(), LPK)

    def test_a_tampered_row_or_wrong_key_fails_the_hash(self):
        _, payload = _history()[0]
        row = SIZING.lpk_row(market_data_pb2.EventEnvelope.FromString(payload), payload, LPK)
        other_key = LogicalProductKey.for_product(
            environment="paper", venue="BINANCE", market="USDM", instrument_uid=LPK.instrument_uid,
            feed="BAR", interval="1m")
        with self.assertRaises(ValueError):
            SIZING.lpk_row_decode(row, other_key, market_data_pb2.EventEnvelope)
        tampered = bytearray(row)
        tampered[20] ^= 1  # inside the stored content hash
        with self.assertRaises(ValueError):
            SIZING.lpk_row_decode(bytes(tampered), LPK, market_data_pb2.EventEnvelope)

    def test_canonical_state_row_is_lossless(self):
        _, payload = _envelope()
        row = SIZING._canonical_state(payload)
        self.assertEqual(row[48:], payload)
        self.assertEqual(row[16:48], hashlib.sha256(payload).digest())
        self.assertEqual(int.from_bytes(row[:8], "big"), 2**63 - 1)


if __name__ == "__main__":
    unittest.main()
