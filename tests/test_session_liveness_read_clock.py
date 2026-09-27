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

    def test_an_old_heartbeat_is_aged_by_the_whole_read(self) -> None:
        """Astra KN-4 re-review F1: 1,999 ms old at the caller's sample,
        2,019 ms when the read ends - the SLA of 2,000 ms is already passed."""
        from qdl.data_quality.binding_decision import BindingQualityInput, evaluate_binding_quality

        write(self.root, transport_ns=1_000_000_000)
        self.clock[0] = 3_019_000_000  # the clock after the read
        result = self.status(self.reader(), now_ns=2_999_000_000)
        self.assertEqual((result.state, result.liveness_ms), ("LIVE", 2_019))
        decision = evaluate_binding_quality(BindingQualityInput(
            binding_id="b", instrument_uid="u", feed="QUOTE", source_role="PRIMARY", authoritative=True,
            acquisition_enabled=True, acquisition_mode="RUST_NATIVE", market_open=True, event_present=True,
            event_age_ms=10, event_limit_ms=2_000, event_recency_policy="OBSERVE",
            session_state=result.state, session_liveness_ms=result.liveness_ms, session_limit_ms=2_000,
            delivery_semantics="ON_CHANGE", allow_quiet_execution=True))
        self.assertEqual(decision.state, "STALE")
        self.assertIn("SOURCE_SESSION_HEARTBEAT_EXPIRED", decision.reason_codes)
        # The same record judged at the caller's sample would have passed.
        strict = self.status(self.reader(injected=False), now_ns=2_999_000_000)
        self.assertEqual(strict.liveness_ms, 1_999)

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


class QueryQualityClockOrderTests(unittest.TestCase):
    """Query's event age is taken after the session read, like the session's."""

    def test_event_and_session_age_are_judged_after_the_read(self) -> None:
        from types import SimpleNamespace

        from qdl.marketdata.v2 import market_data_pb2
        from qdl.query.contracts import StalePolicy
        from qdl.runtime.stable_source import StableSpoolQueryBackend

        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            write(root, transport_ns=1_000_000_000)
            ticks = iter(range(3_000_000_000, 4_000_000_000, 20_000_000))  # +20 ms per clock read
            samples = []

            def clock():
                samples.append(next(ticks))
                return samples[-1]

            backend = SimpleNamespace(
                _clock_ns=clock, config_revision=17,
                _session_liveness=StableSessionLivenessReader(root, clock_ns=clock))
            envelope = market_data_pb2.EventEnvelope(
                source_event_time_ns=1_000_000_000, received_at_ns=1_000_000_000, venue="BINANCE", market="USDM",
                source_session_id=SESSION, connection_generation=5, config_revision=17)
            envelope.quote.level = 1  # selects the quote payload
            binding = SimpleNamespace(
                freshness_basis="SOURCE_EVENT", continuous_calendar=True, stale_after_ms=5_000,
                binding_id="b", instrument=SimpleNamespace(instrument_uid="u", session_calendar_id=None),
                feed=SimpleNamespace(value="QUOTE"), source_role="PRIMARY", authoritative=True,
                delivery_semantics="ON_CHANGE", require_final_bar=False, source_policy_id="p")
            requirement = SimpleNamespace(max_freshness_ms=5_000, max_session_liveness_ms=45_000,
                                          effective_event_recency_policy=StalePolicy.OBSERVE)
            quality = StableSpoolQueryBackend._quality(backend, requirement, binding, envelope,
                                                       gap_open=False, watermark_offset=1)
            # Samples in order: the caller's (before the read), the reader's
            # (after it), then the event-age sample; each age uses a sample
            # taken after the read, never the caller's.
            self.assertGreaterEqual(len(samples), 3)
            self.assertEqual(quality.provider_session_liveness_ms, (samples[1] - 1_000_000_000) // 1_000_000)
            self.assertEqual(quality.freshness_ms, (samples[2] - 1_000_000_000) // 1_000_000)


if __name__ == "__main__":
    unittest.main()
