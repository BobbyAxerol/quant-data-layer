"""KN-4 review F2: a heartbeat rewritten during the read is not a clock skew.

Query samples its clock, then reads the ingestor's session record. The
ingestor rewrites that record continuously, so the record read can carry a
transport time just after the caller's sample; judged at the sample it looked
like ``SOURCE_SESSION_CLOCK_SKEW`` (fail-closed UNKNOWN -> STALE -> DATA_STALE)
although no clock was wrong. With the caller's clock injected the reader
judges such a record at a clock sampled after the read. These tests pin that,
and that a real skew, a disconnect, a generation or a config mismatch still
fail closed.

The reader also keeps each file's parsed content keyed by (inode, mtime,
size), so an unchanged record (most lanes of a venue are long DISCONNECTED) is
not re-read and re-parsed per item; the second class pins that every kind of
change is still seen on the next call and that no verdict is ever cached.
"""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from qdl.runtime.session_liveness import SESSION_LIVENESS_SCHEMA, StableSessionLivenessReader

SESSION = "binance-public-quote-001"


def write(root: Path, *, transport_ns: int, state: str = "LIVE", generation: int = 5,
          config_revision: int = 17) -> None:
    directory = root / "binance-usdm"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{SESSION}.json").write_text(json.dumps({
        "schema": SESSION_LIVENESS_SCHEMA, "source_session_id": SESSION,
        "connection_generation": generation, "state": state,
        "last_transport_at_ns": transport_ns, "updated_at_ns": transport_ns,
        "config_revision": config_revision}), encoding="utf-8")


class HeartbeatReadClockTests(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.root = Path(self._dir.name)
        self.clock = [0]

    def tearDown(self) -> None:
        self._dir.cleanup()

    def reader(self, injected: bool = True) -> StableSessionLivenessReader:
        return StableSessionLivenessReader(self.root, clock_ns=(lambda: self.clock[0]) if injected else None)

    def status(self, reader, *, now_ns: int, generation: int = 5, config_revision: int = 17):
        return reader.status(venue="BINANCE", market="USDM", source_session_id=SESSION,
                             connection_generation=generation, config_revision=config_revision, now_ns=now_ns)

    def test_a_heartbeat_rewritten_between_the_sample_and_the_read_is_live(self) -> None:
        # Caller sampled at 1_000_000_000; the ingestor wrote 1_000_000_700
        # before the read finished; the post-read clock is 1_000_001_000.
        write(self.root, transport_ns=1_000_000_700)
        self.clock[0] = 1_000_001_000
        result = self.status(self.reader(), now_ns=1_000_000_000)
        self.assertEqual((result.state, result.liveness_ms, result.flags), ("LIVE", 0, ()))

    def test_without_the_caller_clock_the_sample_stays_the_only_clock(self) -> None:
        write(self.root, transport_ns=1_000_000_700)
        result = self.status(self.reader(injected=False), now_ns=1_000_000_000)
        self.assertEqual((result.state, result.flags), ("UNKNOWN", ("SOURCE_SESSION_CLOCK_SKEW",)))

    def test_a_transport_time_still_in_the_future_after_the_read_is_a_real_skew(self) -> None:
        write(self.root, transport_ns=5_000_000_000)
        self.clock[0] = 1_000_001_000
        result = self.status(self.reader(), now_ns=1_000_000_000)
        self.assertEqual((result.state, result.flags), ("UNKNOWN", ("SOURCE_SESSION_CLOCK_SKEW",)))

    def test_the_post_read_clock_never_moves_the_evaluation_back(self) -> None:
        write(self.root, transport_ns=1_000_000_700)
        self.clock[0] = 900_000_000  # a clock behind the caller's own sample
        result = self.status(self.reader(), now_ns=1_000_000_000)
        self.assertEqual(result.flags, ("SOURCE_SESSION_CLOCK_SKEW",))

    def test_an_ordinary_heartbeat_is_judged_at_the_callers_sample(self) -> None:
        write(self.root, transport_ns=1_000_000_000)
        self.clock[0] = 99_000_000_000  # never consulted: the record is not ahead
        result = self.status(self.reader(), now_ns=3_000_000_000)
        self.assertEqual((result.state, result.liveness_ms), ("LIVE", 2_000))

    def test_disconnect_generation_and_config_mismatch_still_fail_closed(self) -> None:
        self.clock[0] = 1_000_001_000
        write(self.root, transport_ns=1_000_000_700, state="DISCONNECTED")
        self.assertEqual(self.status(self.reader(), now_ns=1_000_000_000).state, "DISCONNECTED")
        write(self.root, transport_ns=1_000_000_700)
        other_generation = self.status(self.reader(), now_ns=1_000_000_000, generation=6)
        self.assertEqual((other_generation.state, other_generation.flags),
                         ("UNKNOWN", ("SOURCE_SESSION_UNAVAILABLE",)))
        other_config = self.status(self.reader(), now_ns=1_000_000_000, config_revision=16)
        self.assertEqual((other_config.state, other_config.flags),
                         ("UNKNOWN", ("SOURCE_SESSION_CONFIG_MISMATCH",)))


class ParsedContentCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.TemporaryDirectory()
        self.root = Path(self._dir.name)
        self.reader = StableSessionLivenessReader(self.root, clock_ns=lambda: 10_000_000_000)

    def tearDown(self) -> None:
        self._dir.cleanup()

    def status(self, now_ns: int = 10_000_000_000):
        return self.reader.status(venue="BINANCE", market="USDM", source_session_id=SESSION,
                                  connection_generation=5, config_revision=17, now_ns=now_ns)

    def test_a_record_replaced_by_rename_is_read_again(self) -> None:
        write(self.root, transport_ns=9_000_000_000)
        self.assertEqual(self.status().liveness_ms, 1_000)
        path = self.root / "binance-usdm" / f"{SESSION}.json"
        temporary = path.with_suffix(".tmp")
        temporary.write_text(path.read_text().replace("9000000000", "9900000000"), encoding="utf-8")
        temporary.replace(path)  # the ingestor's write: a new inode
        self.assertEqual(self.status().liveness_ms, 100)

    def test_an_in_place_rewrite_and_a_disconnect_are_seen(self) -> None:
        write(self.root, transport_ns=9_000_000_000)
        self.assertEqual(self.status().state, "LIVE")
        write(self.root, transport_ns=9_500_000_000, state="DISCONNECTED")
        self.assertEqual(self.status().state, "DISCONNECTED")

    def test_malformed_then_repaired_and_removed_records(self) -> None:
        directory = self.root / "binance-usdm"
        directory.mkdir(parents=True)
        (directory / f"{SESSION}.json").write_text("{", encoding="utf-8")
        self.assertEqual(self.status().flags, ("SOURCE_SESSION_MALFORMED",))
        write(self.root, transport_ns=9_000_000_000)
        self.assertEqual(self.status().state, "LIVE")
        (directory / f"{SESSION}.json").unlink()
        self.assertEqual(self.status().flags, ("SOURCE_SESSION_UNAVAILABLE",))
        self.assertNotIn(str(directory), self.reader._parsed)

    def test_a_second_record_of_the_same_session_is_still_ambiguous(self) -> None:
        write(self.root, transport_ns=9_000_000_000)
        self.assertEqual(self.status().state, "LIVE")
        directory = self.root / "binance-usdm"
        (directory / "lane-copy.json").write_text((directory / f"{SESSION}.json").read_text(), encoding="utf-8")
        self.assertEqual(self.status().flags, ("SOURCE_SESSION_AMBIGUOUS",))

    def test_age_is_judged_per_call_never_cached(self) -> None:
        write(self.root, transport_ns=9_000_000_000)
        self.assertEqual(self.status(now_ns=10_000_000_000).liveness_ms, 1_000)
        self.assertEqual(self.status(now_ns=60_000_000_000).liveness_ms, 51_000)


if __name__ == "__main__":
    unittest.main()
