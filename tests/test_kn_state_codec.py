"""KN-3 state codecs: Python side of ``contracts/golden/kn_v220/state_codec.json``.

The golden is built from real canonical records by
``scripts/kn_state_codec_golden.py``; ``rust/qdl-contracts/src/state_codec.rs``
reads the same file, so every byte and every refusal reason asserted here is
asserted identically in Rust. The golden tests never skip. The full-sample
check runs over the scratch sample (``QDL_KN3_SAMPLE`` or
``/kn3/canonical-sample.json``) and skips with an explicit reason when absent.
"""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import unittest

from qdl.marketdata.v2 import market_data_pb2
from qdl.projection import kn_state_codec as codec
from qdl.projection.state_contract import MAX_OFFSET, LogicalProductKey, SourceCoordinate
from scripts.kn_resource_sizing import lpk_row, lpk_row_decode
from scripts.kn_state_codec_golden import (
    _coordinates,
    _run_encode,
    load_sample,
    product_key,
    resolve_bytes,
)

GOLDEN = Path(__file__).resolve().parents[1] / "contracts/golden/kn_v220/state_codec.json"
SAMPLE_PATHS = tuple(Path(p) for p in (os.environ.get("QDL_KN3_SAMPLE", ""), "/kn3/canonical-sample.json") if p)


def _b64(value: str) -> bytes:
    return base64.b64decode(value)


def _frame(kind: str, header: str, body: bytes) -> bytes:
    encoded = header.encode()
    return codec.FRAME_MAGIC + bytes([codec.FrameKind[kind]]) + len(encoded).to_bytes(4, "big") + encoded + body


class StateCodecGoldenTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.doc = json.loads(GOLDEN.read_text(encoding="utf-8"))
        cls.bodies = {record["name"]: _b64(record["canonical_b64"]) for record in cls.doc["records"]}

    def _frames(self, record: dict, canonical: bytes) -> dict[str, codec.StateFrame]:
        lpk = LogicalProductKey.parse(record["lpk"])
        source = SourceCoordinate(**record["source"])
        epoch = record["materializer_epoch"]
        lineage = codec.LegacyLineage(record["spool_stream"], record["spool_partition_key"], record["spool_offset"])
        factories = {
            "LATEST": lambda: codec.latest_frame(canonical, lpk, source, epoch),
            "BAR_REVISION": lambda: codec.bar_revision_frame(canonical, lpk, source, epoch),
            "LEGACY_BAR": lambda: codec.legacy_bar_frame(canonical, lpk, lineage, epoch),
        }
        return {kind: factories[kind]() for kind in record["headers"]}

    def test_golden_covers_every_feed_interval_and_venue(self):
        records = [record for record in self.doc["records"] if not record["synthetic"]]
        feeds = {LogicalProductKey.parse(record["lpk"]).feed for record in records}
        self.assertEqual(feeds, {"BAR", "TRADE", "QUOTE", "MARK_INDEX_PRICE", "BOOK_DELTA", "BOOK_SNAPSHOT"})
        intervals = {LogicalProductKey.parse(r["lpk"]).qualifier for r in records if "|BAR|" in r["lpk"]}
        self.assertEqual(len(intervals), 15)
        self.assertEqual({record["venue"] for record in records}, {"BINANCE", "OKX"})
        synthetic = {record["tag"] for record in self.doc["records"] if record["synthetic"]}
        self.assertEqual(synthetic, {"bar|1m|in-progress", "bar|1m|revised"})
        self.assertEqual(self.doc["reasons"], list(codec.REASONS))

    def test_records_encode_and_decode_byte_for_byte(self):
        for record in self.doc["records"]:
            with self.subTest(record["name"]):
                canonical = _b64(record["canonical_b64"])
                lpk = LogicalProductKey.parse(record["lpk"])
                self.assertEqual(product_key(canonical), lpk)
                offset, epoch = record["source"]["offset"], record["materializer_epoch"]
                value = codec.encode_latest_value(canonical, offset, epoch)
                self.assertEqual(value, bytes.fromhex(record["latest_value_trailer_hex"]) + canonical)
                self.assertEqual(codec.decode_latest_value(value), codec.DecodedState(canonical, offset, epoch))
                if record["bar_row_b64"] is not None:
                    row = _b64(record["bar_row_b64"])
                    self.assertEqual(codec.encode_bar_row(canonical, lpk, offset, epoch), row)
                    self.assertEqual(codec.decode_bar_row(row, lpk), codec.DecodedState(canonical, offset, epoch))
                    # Byte-identical to the frozen KN-1 reference encoding (same trailer
                    # layout, same stripped body), apart from the trailer numbers.
                    envelope = market_data_pb2.EventEnvelope.FromString(canonical)
                    reference = lpk_row(envelope, canonical, lpk)
                    self.assertEqual(reference[16:], row[16:])
                    self.assertEqual(lpk_row_decode(row, lpk, market_data_pb2.EventEnvelope), canonical)
                else:
                    self.assertIsNone(record["keys"].get("bar"))
                for kind, frame in self._frames(record, canonical).items():
                    encoded = frame.encode()
                    self.assertEqual(encoded, _frame(kind, record["headers"][kind], canonical), kind)
                    self.assertEqual(codec.decode_frame(encoded), frame, kind)
                    self.assertEqual(frame.key(), record["keys"]["latest" if kind == "LATEST" else "bar"], kind)
                self.assertEqual(codec.floor_key(lpk), record["keys"]["floor"])
                for count, partition in record["partitions"].items():
                    self.assertEqual(codec.state_partition(lpk, int(count)), partition)

    def test_bar_keys_keep_in_progress_apart_from_final_revisions(self):
        keys = {record["tag"]: record["keys"]["bar"] for record in self.doc["records"]
                if record["tag"].startswith("bar|1m|") and record["venue"] == "OKX"}
        final, in_progress, revised = keys["bar|1m|final"], keys["bar|1m|in-progress"], keys["bar|1m|revised"]
        self.assertTrue(in_progress.endswith("|p"))
        self.assertRegex(final, r"\|f0\|[0-9a-f]{16}$")
        self.assertRegex(revised, r"\|f1\|[0-9a-f]{16}$")
        self.assertEqual(len({final, in_progress, revised}), 3)
        self.assertEqual({key.rsplit("|", 2)[0] for key in (final, revised)}, {in_progress.rsplit("|", 1)[0]})

    def test_full_frames_and_floors(self):
        for case in self.doc["full_frames"]:
            with self.subTest(case["record"] + case["kind"]):
                data = _b64(case["frame_b64"])
                frame = codec.decode_frame(data)
                self.assertEqual(frame.kind.name, case["kind"])
                self.assertEqual(frame.envelope, self.bodies[case["record"]])
                self.assertEqual(frame.encode(), data)
        for case in self.doc["floor_frames"]:
            frame = codec.retention_floor_frame(LogicalProductKey.parse(case["lpk"]), case["floor_open_time_ms"],
                                                case["materializer_epoch"])
            self.assertEqual(frame.encode(), _b64(case["frame_b64"]))
            self.assertEqual(codec.decode_frame(frame.encode()), frame)
            self.assertEqual(frame.key(), case["key"])

    def test_invalid_vectors_fail_with_the_shared_reason(self):
        seen = set()
        for group, field, decode in (
            ("invalid_frames", "frame_b64", lambda case, data: codec.decode_frame(data)),
            ("invalid_bar_rows", "row_b64",
             lambda case, data: codec.decode_bar_row(data, LogicalProductKey.parse(case["lpk"]))),
            ("invalid_latest_values", "value_b64", lambda case, data: codec.decode_latest_value(data)),
        ):
            for case in self.doc[group]:
                with self.subTest(case["name"]):
                    self.assertTrue(case["synthetic"])
                    with self.assertRaises(codec.StateCodecError) as caught:
                        decode(case, resolve_bytes(case, field, self.bodies))
                    self.assertEqual((caught.exception.reason, caught.exception.detail),
                                     (case["reason"], case["detail"]))
                    seen.add(case["reason"])
        for case in self.doc["refused_encodes"]:
            with self.subTest(case["name"]):
                with self.assertRaises(codec.StateCodecError) as caught:
                    _run_encode(case, self.bodies)
                self.assertEqual((caught.exception.reason, caught.exception.detail), (case["reason"], case["detail"]))
                seen.add(case["reason"])
        self.assertEqual(seen, set(codec.REASONS))

    def test_kafka_murmur2(self):
        cases = self.doc["murmur2"]
        self.assertGreaterEqual(sum(case["source"].startswith("kafka") for case in cases), 6)
        for case in cases:
            self.assertEqual(codec.murmur2(bytes.fromhex(case["input_hex"])), case["hash"], case)
        self.assertEqual(codec.murmur2(b"21"), -973932308)
        self.assertEqual(codec.murmur2(b"abc"), 479470107)


class StateCodecUnitTests(unittest.TestCase):
    LPK = LogicalProductKey.parse("lpk1|paper|OKX|SWAP|6c7c9256-2905-5c75-a149-fa0ac36bbbc7|BAR|1m")

    def test_trailer_numbers_are_typed(self):
        for offset, epoch in ((-1, 1), (0, 0), (MAX_OFFSET + 1, 1), (0, MAX_OFFSET + 1), (True, 1), (0, 1.0)):
            with self.subTest((offset, epoch)), self.assertRaises(codec.StateCodecError) as caught:
                codec.state_trailer(b"x", offset, epoch)
            self.assertEqual(caught.exception.reason, "TRAILER")
        trailer = codec.state_trailer(b"x", MAX_OFFSET, MAX_OFFSET)
        self.assertEqual(len(trailer), codec.TRAILER_BYTES)
        self.assertEqual(trailer[:8], MAX_OFFSET.to_bytes(8, "big"))

    def test_header_json_is_parsed_like_serde_json(self):
        floor = codec.retention_floor_frame(self.LPK, 5, 1)
        header = codec.canonical_json(floor.header()).decode()
        cases = {
            "5.0": ("FIELD", "floor_open_time_ms"), "true": ("FIELD", "floor_open_time_ms"),
            '"5"': ("FIELD", "floor_open_time_ms"), "-1": ("FIELD", "floor_open_time_ms"),
            "-0": ("FIELD", "floor_open_time_ms"), "05": ("HEADER_JSON", ""),
            "18446744073709551616": ("FIELD", "floor_open_time_ms"), "1e400": ("HEADER_JSON", ""),
            "Infinity": ("HEADER_JSON", ""), "null": ("FIELD", "floor_open_time_ms"),
        }
        for literal, expected in cases.items():
            edited = header.replace('"floor_open_time_ms":5', f'"floor_open_time_ms":{literal}')
            with self.subTest(literal), self.assertRaises(codec.StateCodecError) as caught:
                codec.decode_frame(_frame("RETENTION_FLOOR", edited, b""))
            self.assertEqual((caught.exception.reason, caught.exception.detail), expected)

    def test_bar_key_and_partition_edges(self):
        digest = "ab" * 32
        self.assertEqual(codec.bar_key(self.LPK, 60_000, False, 3, ""), f"{self.LPK.encode()}|60000|p")
        self.assertEqual(codec.bar_key(self.LPK, 60_000, True, 3, digest),
                         f"{self.LPK.encode()}|60000|f3|abababababababab")
        for args in ((-1, True, 0, digest), (0, True, -1, digest), (0, True, 1 << 32, digest), (0, True, 0, "AB")):
            with self.subTest(args), self.assertRaises(codec.StateCodecError):
                codec.bar_key(self.LPK, *args)
        for partitions in (0, -1, 1 << 31, True):
            with self.subTest(partitions), self.assertRaises(codec.StateCodecError) as caught:
                codec.state_partition(self.LPK, partitions)
            self.assertEqual(caught.exception.reason, "PARTITIONS")
        self.assertEqual(codec.state_partition(self.LPK, 1), 0)

    def test_frame_decode_is_idempotent_on_its_own_output(self):
        frame = codec.retention_floor_frame(self.LPK, 0, MAX_OFFSET)
        self.assertEqual(codec.decode_frame(frame.encode()).encode(), frame.encode())
        self.assertIsNone(frame.source)


class StateCodecFullSampleTests(unittest.TestCase):
    """Every real record of the scratch sample: encode, decode, KN-1 identity, keys."""

    def test_every_sample_record_round_trips(self):
        sample = next((path for path in SAMPLE_PATHS if path.is_file()), None)
        if sample is None:
            self.skipTest("KN-3 canonical sample not present (set QDL_KN3_SAMPLE or mount /kn3); "
                          "the golden tests above cover the committed subset")
        records = load_sample(sample)
        self.assertEqual(len(records), 114)
        counts = {"records": 0, "bar_rows": 0, "frames": 0}
        for index, record in enumerate(records):
            canonical = record["canonical"]
            with self.subTest(index=index, tag=record["tag"], offset=record["spool_offset"]):
                lpk = product_key(canonical)
                source, epoch = _coordinates(index)
                value = codec.encode_latest_value(canonical, source.offset, epoch)
                self.assertEqual(codec.decode_latest_value(value).canonical, canonical)
                frames = [codec.latest_frame(canonical, lpk, source, epoch)]
                if lpk.feed == "BAR":
                    row = codec.encode_bar_row(canonical, lpk, source.offset, epoch)
                    self.assertEqual(codec.decode_bar_row(row, lpk).canonical, canonical)
                    envelope = market_data_pb2.EventEnvelope.FromString(canonical)
                    self.assertEqual(lpk_row(envelope, canonical, lpk)[16:], row[16:])
                    counts["bar_rows"] += 1
                    lineage = codec.LegacyLineage("md.canonical.v2", record["physical_key"], record["spool_offset"])
                    frames += [codec.bar_revision_frame(canonical, lpk, source, epoch),
                               codec.legacy_bar_frame(canonical, lpk, lineage, epoch)]
                for frame in frames:
                    self.assertEqual(codec.decode_frame(frame.encode()), frame)
                    self.assertEqual(codec.state_partition(frame.lpk, 12),
                                     (codec.murmur2(lpk.encode().encode()) & 0x7FFFFFFF) % 12)
                    counts["frames"] += 1
                counts["records"] += 1
        self.assertEqual(counts, {"records": 114, "bar_rows": 84, "frames": 282})


if __name__ == "__main__":
    unittest.main()
