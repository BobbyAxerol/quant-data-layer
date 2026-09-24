"""KN-1 review F5/F6: the sizing helper's safety and the contract-complete row.

F6: ``redis_memory_with`` refuses a non-empty target, writes only under its
per-run namespace, deletes exactly what it wrote (also on error) and never
sends FLUSHALL/FLUSHDB. F5: the BAR row measured for the budget maps every
envelope and Bar field (identity excepted), and the canonical alternative is
lossless.
"""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
import sys
import unittest
from unittest import mock

from qdl.marketdata.v2 import market_data_pb2

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
                    "identity_stripped_state": ["00" * 49, "02" * 49]},
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
        with mock.patch.object(SIZING, "PRODUCT_IDENTITY_FIELDS", frozenset({"schema_name"})):
            with self.assertRaises(ValueError) as raised:
                SIZING._complete_bar(envelope, payload)
        self.assertIn("instrument_uid", str(raised.exception))

    def test_identity_stripped_row_round_trips_exactly(self):
        envelope, payload = _envelope()
        row, exact = SIZING._identity_stripped_state(envelope, payload)
        self.assertTrue(exact)
        self.assertLess(len(row), len(SIZING._canonical_state(payload)))
        stripped = market_data_pb2.EventEnvelope.FromString(row[48:])
        self.assertEqual(stripped.instrument_uid, "")
        self.assertEqual(stripped.bar.interval, "")
        self.assertEqual(stripped.bar.revision, 1)

    def test_canonical_state_row_is_lossless(self):
        _, payload = _envelope()
        row = SIZING._canonical_state(payload)
        self.assertEqual(row[48:], payload)
        self.assertEqual(row[16:48], hashlib.sha256(payload).digest())
        self.assertEqual(int.from_bytes(row[:8], "big"), 2**63 - 1)


if __name__ == "__main__":
    unittest.main()
